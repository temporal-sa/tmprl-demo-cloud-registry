"""Legacy pre-prod scanner, for services that predate the Security team's platform.

A handful of services owned by the Waypoint team still gate through this old
scanner rather than
the workflow next door. It is on the list to be migrated and it is not what the
Security team runs today; it is here because comparing the two at the pause is
the entire point of this file.

This script has no memory. It runs, it scans, and it either finishes or it does
not. There is no workflow, no Event History, no task queue, no worker, and no
supervisor that brings it back. Nothing here imports temporalio, and that is the
whole point: it is the requirements document's uncontrolled tool, the kind that
"cannot assume that the upstream tool can preserve stack state, hold open
execution, retry safely, or accept a later resumed result".

If Agent Gateway pauses the promotion this script triggers, the script cannot
wait for a human. It prints the operation id it was given and exits non-zero.
Somebody has to come back later and finish the promotion by hand. Compare
security_scan/security_scan_workflow.py, which checkpoints and resumes, and which
is the same job done by a system that can hold its own state.

    python -m security_scan.legacy_security_scan_script \\
        --service delivery-matching-service --version 2.3.0 \\
        --gateway-workflow-id wf-abc123

It speaks MCP to the gateway rather than posting raw JSON, because that is the
only interface the gateway has -- streamable HTTP MCP needs an initialize
handshake and a session id before a tool call, so a plain curl cannot do it. An
MCP client is still just a client. It buys this script no durability whatsoever.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import threading
import time

import requests
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

GATEWAY_MCP_URL = os.getenv("GATEWAY_MCP_URL", "http://localhost:8080/mcp")
# This scanner's own bearer token, not a person's. The gateway maps it to
# `legacy-scanner@security` and from there to caller_service=security, which is
# what makes the promotion this script asks for the Security team's to approve and
# exempts it from having another scan inserted in front of it. Resolving that
# server-side from the token, rather than letting the request assert it, is the
# whole point: "I am a scanner, do not scan me" is not a claim a caller gets to
# make about itself.
GATEWAY_TOKEN = os.getenv("GATEWAY_TOKEN", "tok_legacy_scanner")
# Where progress goes so the dashboard can draw this scan at all. Publishing
# telemetry buys this script exactly nothing in durability -- it already speaks
# MCP to the gateway, and an HTTP client is still just a client. What it buys is
# an audience that can see the scan running, and then see what happens to it.
STATE_URL = os.getenv("SECURITY_SCAN_BACKEND_URL", "http://localhost:9000/invoke")
# The record this script publishes expires on its own. While the script runs it
# refreshes; when the script dies at the pause, nothing refreshes it and the card
# goes stale by itself. Nobody reports the death, because there is nobody left to
# report it -- which is the accurate failure mode for a process, and precisely
# what the Temporal scan survives.
STATE_TTL_SECONDS = float(os.getenv("LEGACY_SCAN_TTL_SECONDS", "6"))

SEVERITY_ORDER = ["none", "low", "medium", "high", "critical"]
SEVERITY_THRESHOLD = "medium"
STAGE_NAMES = [
    "dependency_scan",
    "container_image_scan",
    "secret_detection",
    "static_analysis",
]
# The same scripted sequences the platform scan uses, so the two paths are
# compared on how they behave at the pause and nothing else.
SCHEDULE = {
    "pass": ["none", "low", "none", "none"],
    "fail": ["high", "none", "none", "none"],
}


class _Tail:
    """Everything this scanner can tell the platform about itself: its stdout.

    Deliberately thin, because that is honest. A workflow publishes stages, a
    verdict, and a workflow id that can be opened and inspected. A script has the
    lines it printed, and if it stops printing there is no way to tell the
    difference between slow and gone -- which is why the record it publishes
    carries a TTL and the dashboard treats silence as abandonment.

    Every failure here is swallowed. A scanner whose telemetry endpoint is down
    still has a scan to run, and the run is the part that matters.
    """

    def __init__(self, service: str, version: str, environment: str,
                 stage_count: int) -> None:
        self._service = service
        self._version = version
        self._environment = environment
        self._stage_count = stage_count
        self._lines: list[str] = []
        self._stages_completed = 0
        self._results: list[dict] = []

    def say(self, line: str, *, echo: bool = True) -> None:
        """Print a line and publish the tail. The two never diverge."""
        if echo:
            print(line, flush=True)
        self._lines.append(line)
        self.publish("running_scan")

    def stage_done(self, name: str, severity: str, passed: bool) -> None:
        self._stages_completed += 1
        self._results.append(
            {
                "stage_name": name,
                "max_severity": severity,
                "passed": passed,
                "findings": 0 if passed else 1,
            }
        )

    def publish(self, phase: str) -> None:
        try:
            requests.post(
                STATE_URL,
                json={
                    "tool_name": "record_scan_state",
                    "arguments": {
                        # No scan_workflow_id. There is no workflow. The dashboard
                        # renders that absence rather than papering over it.
                        "mode": "legacy",
                        "service": self._service,
                        "version": self._version,
                        "environment": self._environment,
                        "phase": phase,
                        "stages_completed": self._stages_completed,
                        "stage_count": self._stage_count,
                        "severity_threshold": SEVERITY_THRESHOLD,
                        "stage_results": self._results,
                        "lines": self._lines[-6:],
                        "ttl_seconds": STATE_TTL_SECONDS,
                    },
                },
                timeout=3,
            )
        except Exception:  # noqa: BLE001 - telemetry is best effort, always
            pass


def _heartbeat_forever(tail: _Tail, phase: dict) -> None:
    """Keep the published record fresh while this process is alive.

    A daemon thread, so it dies with the process and cannot keep the record alive
    past the run. That is the mechanism: the card stays current for exactly as
    long as there is a process, and not one second longer. No cooperative
    shutdown, no goodbye message -- a ctrl-C, a kill -9 and an orderly exit at the
    approval pause all strand it identically, which is the point.
    """
    while True:
        time.sleep(STATE_TTL_SECONDS / 3.0)
        tail.publish(phase["name"])


def _blocking(severity: str) -> bool:
    try:
        return SEVERITY_ORDER.index(severity) >= SEVERITY_ORDER.index(
            SEVERITY_THRESHOLD
        )
    except ValueError:
        return True


def run_scan(
    scripted_outcome: str,
    check_seconds: int,
    stage_count: int,
    tail: _Tail,
) -> bool:
    severities = SCHEDULE[scripted_outcome]
    for stage in range(1, stage_count + 1):
        name = STAGE_NAMES[min(stage - 1, len(STAGE_NAMES) - 1)]
        max_severity = severities[min(stage - 1, len(severities) - 1)]
        blocked = _blocking(max_severity)
        tail.stage_done(name, max_severity, not blocked)
        tail.say(
            f"stage {stage}/{stage_count} {name}: max_severity={max_severity} "
            f"threshold={SEVERITY_THRESHOLD}"
        )
        if blocked:
            tail.say(
                f"SECURITY SCAN FAILED at {name} (1 finding, {max_severity}). "
                f"Stopping."
            )
            tail.publish("scan_failed")
            return False
        if stage < stage_count:
            time.sleep(check_seconds)
    return True


async def request_promotion(
    url: str,
    token: str,
    service: str,
    version: str,
    environment: str,
    gateway_workflow_id: str,
    replay_safe: bool,
) -> dict:
    async with streamablehttp_client(
        url, headers={"Authorization": f"Bearer {token}"}
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "run_nested_release",
                {
                    "service": service,
                    "version": version,
                    "environment": environment,
                    # Honest self-description. This caller cannot hold a pause,
                    # so it says so, and Agent Gateway fails closed accordingly.
                    "tool1_mode": "uncontrolled",
                    # Also honest, and a separate question from the first. A scan
                    # is a read: running it twice is safe, it just costs all four
                    # stages again. Saying so is what lets a human finish the
                    # promotion by hand after the approval, which is the only way
                    # this run can ever finish once the process is gone.
                    "replay_safe": replay_safe,
                    "workflow_id": gateway_workflow_id,
                    "justification": (
                        f"Legacy security scan passed for {service} {version}"
                    ),
                },
            )
            if result.isError:
                text = result.content[0].text if result.content else "tool error"
                raise SystemExit(f"gateway call failed: {text}")
            return result.structuredContent or {}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--environment", default="prod")
    parser.add_argument("--scripted-outcome", default="pass", choices=sorted(SCHEDULE))
    parser.add_argument("--gateway-workflow-id", required=True)
    parser.add_argument(
        "--replay-safe",
        dest="replay_safe",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "whether re-running this scan is safe. True by default, because it "
            "is: pass --no-replay-safe to watch the gateway refuse the manual "
            "retry outright."
        ),
    )
    parser.add_argument("--check-seconds", type=int, default=5)
    parser.add_argument("--stage-count", type=int, default=4)
    parser.add_argument("--url", default=GATEWAY_MCP_URL)
    parser.add_argument("--token", default=GATEWAY_TOKEN)
    args = parser.parse_args()

    tail = _Tail(args.service, args.version, args.environment, args.stage_count)
    # Named so the heartbeat thread can follow the phase without being restarted.
    phase = {"name": "running_scan"}
    tail.publish("running_scan")
    heartbeat = threading.Thread(
        target=_heartbeat_forever, args=(tail, phase), daemon=True
    )
    heartbeat.start()

    if not run_scan(
        args.scripted_outcome, args.check_seconds, args.stage_count, tail
    ):
        sys.exit(1)

    tail.say(
        "Security scan passed. Requesting the production promotion via Agent "
        "Gateway."
    )
    body = asyncio.run(
        request_promotion(
            args.url,
            args.token,
            args.service,
            args.version,
            args.environment,
            args.gateway_workflow_id,
            args.replay_safe,
        )
    )

    status = body.get("status")
    if status in ("waiting_for_approval", "blocked_nested_approval"):
        # The last thing this record will ever say. Note what is NOT done here:
        # the phase is not set to "abandoned" and no farewell is published. The
        # script has no idea whether it is about to be approved, rejected, or
        # forgotten -- all it knows is that it cannot wait. So it says the true
        # thing, and then stops refreshing. The card goes stale on its own a few
        # seconds later, and it would do so identically if this process were
        # killed here instead of exiting.
        phase["name"] = "awaiting_prod_approval"
        tail.say(
            f"promotion requested \u00b7 operation {body.get('operation_id')} "
            "\u00b7 approval required",
            echo=False,
        )
        tail.publish("awaiting_prod_approval")
        # Everything this process knows is about to stop existing. All it can do
        # is print the identifiers and hope somebody writes them down.
        print(
            "\nApproval is required and this script cannot wait for it.\n"
            "Record these to finish the promotion by hand once it is approved:\n"
            f"  workflow_id:  {body.get('workflow_id')}\n"
            f"  operation_id: {body.get('operation_id')}\n\n"
            "  python gateway_call.py resume_nested_release "
            f"workflow_id={body.get('workflow_id')} "
            f"operation_id={body.get('operation_id')}\n",
            flush=True,
        )
        sys.exit(2)

    print(json.dumps(body, indent=2, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
