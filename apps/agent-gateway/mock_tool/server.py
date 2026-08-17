from __future__ import annotations

import asyncio
import os
import time

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

DEFAULT_SERVICE = os.getenv("DEMO_SERVICE", "delivery-matching-service")
ENVIRONMENTS = ("staging", "prod")

_BOOT = time.time()

# Pretend deployment backend. Holds just enough in-memory state to keep the demo
# story internally consistent: cut releases are remembered, and a promotion moves
# the target environment to that version so a later read reflects it. Each record
# also keeps the version it replaced and when it landed, which is what the fleet
# panel needs to render a promotion as a transition rather than a bare value.
# Seed times are backdated so a fresh boot reads as an existing fleet rather than
# deployments that all landed the moment the demo started.
_deployed: dict[str, dict] = {
    env: {
        "service": DEFAULT_SERVICE,
        "version": version,
        "previous_version": previous,
        "updated_at": _BOOT - age,
        "promotions": 0,
    }
    for env, version, previous, age in (
        ("staging", "2.2.0", None, 3300.0),
        ("prod", "2.2.0", "2.1.0", 2700.0),
    )
}
# Insertion ordered, so the fleet rail can show newest releases first. Seeded with
# the version running everywhere and the one production moved past. seen_in records
# every environment a release has ever reached, in promotion order, so the rail can
# tell a release that is ready to go from one already superseded.
_releases: dict[tuple[str, str], dict] = {
    (DEFAULT_SERVICE, version): {
        "service": DEFAULT_SERVICE,
        "version": version,
        "cut_at": _BOOT - offset,
        "seen_in": list(seen_in),
    }
    for version, offset, seen_in in (
        ("2.1.0", 5400.0, ("prod",)),
        ("2.2.0", 3600.0, ("staging", "prod")),
    )
}

# The quality gate card's state, and it is never absent. Gates are not a CASE-2
# capability that appears partway through the demo any more: they are ambient
# infrastructure that reacts to anything reaching staging, so the card is on the
# dashboard from container startup, honestly reporting that nothing has been
# staged yet. It moves to running the first time any promotion reaches staging,
# by any route, including a CASE-1 manual one.
#
# Contrast the security scan below, which reports nothing until it first runs.
# Its card still arrives mid-demo, but on the security mandate being switched on
# rather than on this state existing: the mandate is what puts a scan in the
# pipeline, so the card is drawn from the toggle and filled in from here.
_IDLE_GATES = {
    "status": "idle",
    "service": None,
    "version": None,
    "environment": None,
    "checks": [],
    "checks_completed": 0,
    "check_count": 4,
    "gate_workflow_id": "",
    "updated_at": _BOOT,
}
_quality_gates: dict = dict(_IDLE_GATES)

# Last security scan, or None if a scan has never run. Same contract as
# _quality_gates above: a projection for the dashboard, never the verdict itself.
# None means no scan has reported here, which the panel draws as an idle scan
# card while the mandate is on and as no card at all while it is off. Restarting
# this service restores the "before" picture for both cards.
_scan: dict | None = None

# Who is currently offering pre-prod security scanning, and when they last said
# so. The Security team's worker heartbeats into this while it is running; the
# entry goes stale on its own once it stops, so the capability disappears when
# their platform goes away rather than lingering as an advertisement for something
# nobody is serving. This is the shared platform's service registry, and it is
# what makes "the Security team onboarded the Waypoint team" a thing that happens by
# starting their container rather than by editing a config flag.
_scan_provider: dict | None = None

# How long a gate run takes is no longer this backend's business. The four
# checks are Activities inside QualityGateChildWorkflow, each with its own fixed
# duration, and QUALITY_GATE_FAIL_VERSIONS is read there too. All this service
# does now is hold the latest published state so the dashboard can draw it.

# Idempotency store keyed by the Idempotency-Key header. A replayed activity
# attempt with the same key returns the original result and does not apply the
# effect a second time.
_seen: dict[str, dict] = {}

# Demo knob: cutting this release runs long, which trips the gateway's
# monitor-and-convert budget and demonstrates the sync-to-async conversion. The
# delay stays under the invoke Activity's request timeout.
SLOW_CUT_VERSION = os.getenv("SLOW_CUT_VERSION", "2.3.1")
SLOW_CUT_DELAY_SECONDS = float(os.getenv("SLOW_CUT_DELAY_SECONDS", "8"))


async def _maybe_delay(tool_name: str, arguments: dict) -> None:
    if (
        tool_name == "cut_release"
        and str(arguments.get("version", "")) == SLOW_CUT_VERSION
    ):
        await asyncio.sleep(SLOW_CUT_DELAY_SECONDS)


def _handle(tool_name: str, arguments: dict) -> dict:
    if tool_name == "get_deployed_version":
        env = str(arguments.get("environment", ""))
        record = _deployed.get(env)
        version = record["version"] if record else None
        return {
            "environment": env,
            "deployed_version": version,
            "message": (
                f"{version} is currently deployed in {env}"
                if version
                else f"no known deployment in {env}"
            ),
        }

    if tool_name == "cut_release":
        service = str(arguments.get("service", ""))
        version = str(arguments.get("version", ""))
        # Cutting a version that is already cut is a no-op that reports the
        # existing release, rather than an error or a second release. The
        # orchestrated CASE-2 path can legitimately reach this twice for the same
        # computed version (a replay-safe retry reruns Tool1's real work), and the
        # gateway's own idempotency key already suppresses a repeat of the same
        # attempt, so this covers the case where the two cuts are genuinely
        # separate calls. cut_at is deliberately left alone: a re-cut must not
        # reorder the release rail or make an old release look new.
        already_cut = (service, version) in _releases
        if not already_cut:
            _releases[(service, version)] = {
                "service": service,
                "version": version,
                "cut_at": time.time(),
                "seen_in": [],
            }
        return {
            "service": service,
            "version": version,
            "release": "cut",
            "already_cut": already_cut,
            "message": f"release {version} of {service} is cut and ready to promote",
        }

    if tool_name == "promote_release":
        service = str(arguments.get("service", ""))
        version = str(arguments.get("version", ""))
        env = str(arguments.get("environment", ""))
        prior = _deployed.get(env)
        _deployed[env] = {
            "service": service,
            "version": version,
            "previous_version": prior["version"] if prior else None,
            "updated_at": time.time(),
            "promotions": (prior["promotions"] + 1) if prior else 1,
        }
        # A version can be promoted without a matching cut_release in this demo
        # backend, so register it here too. Nothing reaches an environment without
        # appearing in the release list.
        release = _releases.get((service, version))
        if release is None:
            release = {
                "service": service,
                "version": version,
                "cut_at": time.time(),
                "seen_in": [],
            }
            _releases[(service, version)] = release
        if env in ENVIRONMENTS and env not in release["seen_in"]:
            release["seen_in"].append(env)
        return {
            "service": service,
            "version": version,
            "environment": env,
            "promoted": True,
            # The version this promotion replaced. Carried on the result as well
            # as in the fleet state because PromoteReleaseChildWorkflow reports
            # it back to the caller, and the dashboard's toast names it.
            "previous_version": prior["version"] if prior else None,
            "message": f"promoted {service} {version} to {env}",
        }

    if tool_name == "record_quality_gate_state":
        # Visibility only, exactly like record_scan_state below. The verdict
        # lives in QualityGateChildWorkflow's own Event History; this is just the
        # projection the dashboard draws, published as the run progresses so the
        # card can honestly say "2 / 4 checks complete" mid-run.
        global _quality_gates
        checks = list(arguments.get("checks") or [])
        _quality_gates = {
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "environment": str(arguments.get("environment", "staging")),
            "status": str(arguments.get("status", "running")),
            "checks": checks,
            "checks_completed": int(
                arguments.get("checks_completed", len(checks))
            ),
            "check_count": int(arguments.get("check_count", 4)),
            "gate_workflow_id": str(arguments.get("gate_workflow_id", "")),
            "updated_at": time.time(),
        }
        return {"recorded": True, "status": _quality_gates["status"]}

    if tool_name == "register_security_scan_capability":
        global _scan_provider
        _scan_provider = {
            "provider": str(arguments.get("provider", "security")),
            "namespace": str(arguments.get("namespace", "security")),
            "task_queue": str(arguments.get("task_queue", "security-tq")),
            "endpoint": str(arguments.get("endpoint", "security")),
            "ttl_seconds": float(arguments.get("ttl_seconds", 20.0)),
            "last_seen": time.time(),
        }
        return {
            "registered": True,
            "provider": _scan_provider["provider"],
            "message": (
                f"{_scan_provider['provider']} is offering pre-prod security "
                f"scanning"
            ),
        }

    if tool_name == "get_security_scan_status":
        # The release pipeline's capability lookup: is anyone offering a pre-prod
        # security scan right now? Available means somebody is currently running a
        # scanning platform, not that somebody once did -- a registration that has
        # not been refreshed inside its TTL is treated as gone, so stopping the
        # Security team's worker takes the capability with it and the pipeline
        # goes back to opening the promotion itself.
        #
        # The answer is who, and nothing about how. Which stages a scan runs, what
        # severity threshold it applies, and which versions it rejects are all
        # decided behind the Security team's Nexus endpoint, where the caller
        # cannot reach them and has no business trying.
        record = _scan_provider
        fresh = bool(
            record
            and (time.time() - record["last_seen"]) <= record["ttl_seconds"]
        )
        if not fresh:
            return {
                "available": False,
                "message": (
                    "no pre-prod security scan provider is currently registered"
                ),
            }
        return {
            "available": True,
            "provider": record["provider"],
            "endpoint": record["endpoint"],
            "message": (
                f"{record['provider']} is offering pre-prod security scanning"
            ),
        }

    if tool_name == "record_scan_state":
        # Visibility only. The Security team publishes scan progress here so the
        # approval dashboard can draw it next to the gate card; nothing about the
        # scan's own verdict depends on this call succeeding.
        global _scan
        _scan = {
            "scan_workflow_id": str(arguments.get("scan_workflow_id", "")),
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "environment": str(arguments.get("environment", "prod")),
            "phase": str(arguments.get("phase", "running_scan")),
            "stages_completed": int(arguments.get("stages_completed", 0)),
            "stage_count": int(arguments.get("stage_count", 4)),
            "severity_threshold": str(
                arguments.get("severity_threshold", "medium")
            ),
            "stage_results": list(arguments.get("stage_results") or []),
            "verdict": arguments.get("verdict"),
            "gateway_operation_id": arguments.get("gateway_operation_id"),
            "error": arguments.get("error"),
            "updated_at": time.time(),
            # Which of the Security team's two scanners published this. The
            # dashboard draws the same slot differently for each, because they are
            # not the same kind of thing: one is a workflow you can open in the
            # Temporal UI, the other is a process on somebody's host.
            "mode": (
                "legacy"
                if str(arguments.get("mode", "platform")).lower() == "legacy"
                else "platform"
            ),
            # stdout, for the legacy scanner only. A workflow does not need this:
            # its stages, verdict and history are all durable objects the card can
            # read properly. A script has nothing but what it printed.
            "lines": [str(line) for line in (arguments.get("lines") or [])],
            # How long this record stays believable without a refresh. The legacy
            # scanner heartbeats while it runs and stops when it dies, so the
            # record ages out on its own and the card goes stale without anybody
            # reporting the death -- which is the honest failure mode for a
            # process, and the one the platform scan survives. Absent for the
            # workflow, whose state is authoritative until it says otherwise.
            "ttl_seconds": (
                float(arguments["ttl_seconds"])
                if arguments.get("ttl_seconds") is not None
                else None
            ),
        }
        return {"recorded": True, "phase": _scan["phase"]}

    if tool_name == "security_scan_prepare":
        return {
            "checkpointed": True,
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "scan_workflow_id": str(arguments.get("scan_workflow_id", "")),
            "next_tool": "promote_release",
            "message": (
                "The pre-prod security scan cleared and is requesting the "
                "production promotion"
            ),
        }

    if tool_name == "security_scan_resume":
        return {
            "resumed": True,
            "nested_result_received": arguments.get("nested_result") is not None,
            "message": (
                "The security scan recorded the approved promotion"
            ),
        }

    if tool_name == "release_orchestrator_prepare":
        return {
            "checkpointed": True,
            "service": str(arguments.get("service", "")),
            "version": str(arguments.get("version", "")),
            "next_tool": "promote_release",
            "message": "Tool1 checkpointed before its nested Tool2 call",
        }

    if tool_name == "release_orchestrator_resume":
        nested_result = arguments.get("nested_result")
        return {
            "resumed": True,
            "replayed": bool(arguments.get("replay")),
            "nested_result_received": nested_result is not None,
            "message": "Tool1 resumed with the durable Tool2 result",
        }

    if tool_name == "record_autonomous_followup":
        run_id = str(arguments.get("agent_run_id", ""))
        return {
            "agent_run_id": run_id,
            "followup_recorded": True,
            "message": "Autonomous dependent action executed after approval",
        }

    return {"tool_name": tool_name, "message": f"executed {tool_name}"}


async def invoke(request: Request) -> JSONResponse:
    body = await request.json()
    key = request.headers.get("Idempotency-Key", "")

    if key and key in _seen:
        prior = dict(_seen[key])
        prior["idempotent_replay"] = True
        return JSONResponse(prior)

    tool_name = body.get("tool_name", "")
    arguments = body.get("arguments", {}) or {}
    await _maybe_delay(tool_name, arguments)

    result = _handle(tool_name, arguments)
    result["executed_at"] = time.time()
    result["idempotent_replay"] = False
    if key:
        _seen[key] = result
    return JSONResponse(result)


def _scan_projection() -> dict | None:
    """The last scan record, with staleness decided here rather than by the caller.

    A legacy scan record carries a TTL because the thing that published it is a
    process that can simply stop existing. Nobody sends a "the script died"
    message -- there is nobody left to send it. So the record ages: while the
    script is running it keeps refreshing, and the moment it stops, the record
    stops being believable and the dashboard says so.

    That is the entire difference the CASE-2 contrast rests on. The platform scan
    publishes no TTL because it does not need one: its state lives in its own
    Event History, a Worker restart replays it, and it is authoritative until the
    workflow itself says otherwise. A record that never goes stale and a record
    that ages out on its own are the two behaviours, side by side, in one field.
    """
    if not _scan:
        return None
    record = dict(_scan)
    ttl = record.get("ttl_seconds")
    record["stale"] = bool(
        ttl
        and record.get("phase") not in _SCAN_TERMINAL_PHASES
        and (time.time() - float(record.get("updated_at", 0))) > float(ttl)
    )
    return record


# A scan that reached one of these is finished, and a finished record does not go
# stale: there is nothing left to heartbeat. Only a scan that stopped mid-flight
# is abandoned, which is exactly the state the walkthrough's Act One produces.
_SCAN_TERMINAL_PHASES = {"completed", "scan_failed", "rejected", "expired", "failed"}


async def state(request: Request) -> JSONResponse:
    """Read only view of the fleet: what is running where, and what is promotable.

    The approval dashboard renders this next to the approval queue so an approver
    can see the effect of a decision, not just the decision. It is a projection of
    the same in-memory state the tools mutate, so it can never disagree with what
    get_deployed_version returns.
    """
    return JSONResponse(
        {
            "generated_at": time.time(),
            # Always present, in whichever of idle/running/passed/failed is
            # honestly true. Idle at startup means no candidate has ever reached
            # staging, which is a state worth drawing rather than a card worth
            # hiding: the gate is standing infrastructure, not a capability that
            # gets built partway through the demo.
            "quality_gates": dict(_quality_gates),
            # Absent until the Security team has run a scan at least once. What
            # decides whether the card is on the row is the gateway's mandate
            # toggle, not this: absent here only means there is no verdict yet.
            "security_scan": _scan_projection(),
            "environments": [
                {"environment": env, **_deployed[env]}
                for env in ENVIRONMENTS
                if env in _deployed
            ],
            "releases": [
                dict(record)
                for record in sorted(
                    _releases.values(),
                    key=lambda item: item["cut_at"],
                    reverse=True,
                )
            ],
        }
    )


async def healthz(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True})


app = Starlette(
    routes=[
        Route("/invoke", invoke, methods=["POST"]),
        Route("/state", state, methods=["GET"]),
        Route("/healthz", healthz, methods=["GET"]),
    ]
)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("MOCK_TOOL_PORT", "9000")))
