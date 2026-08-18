from __future__ import annotations

import hashlib
import os
import time
from typing import Any

import requests
from temporalio import activity
from temporalio.exceptions import ApplicationError

from security_scan.models import (
    SCAN_FINDING_SEVERITY_THRESHOLD,
    CheckCveInput,
    CheckLicenseInput,
    GenerateSbomInput,
    PublishScanStateInput,
    ScanCheckInput,
    severity_at_or_above,
    stage_name,
)

# Shared observability backend. The Security team reports scan progress into the
# same platform the deployment tooling reports into.
BACKEND_URL = os.getenv("SECURITY_SCAN_BACKEND_URL", "http://mock-tool:9000/invoke")

# Both Activities here are this team's own business: running a scan stage, and
# reporting progress for a dashboard. Nothing in this file talks to Agent
# Gateway. The two Activities that used to -- one holding a Temporal client for
# their namespace, one sending them a Signal -- are gone, replaced by Nexus calls
# the workflow makes directly. There is no longer any code in this package that
# needs credentials for someone else's cluster.

# Scripted per-stage findings, indexed by stage number, expressed as the highest
# severity that stage turned up. Fixed sequences rather than a real scanner's
# output, so the same demo run produces the same result every time and "watch the
# secret detection stage" is a thing a presenter can actually say.
#
#   pass            every stage clears; one low finding, which is under the
#                   threshold and therefore not a blocker
#   fail            the dependency scan turns up a high straight away, so the
#                   scan dies at stage 1 and Agent Gateway is never called at all
#   flaky_then_pass the first stage is over threshold and the rest are clean. The
#                   scan still FAILS, at stage 1, despite the name: one finding
#                   at or above the threshold fails the scan, it is not averaged
#                   out or rescanned past. Use it to show exactly that, not to
#                   show a recovery.
SCHEDULE: dict[str, list[str]] = {
    "pass": ["none", "low", "none", "none"],
    "fail": ["high", "none", "none", "none"],
    "flaky_then_pass": ["high", "low", "none", "none"],
}


@activity.defn
def run_scan_check(input: ScanCheckInput) -> dict[str, Any]:
    """Run one scan stage and judge it against the severity threshold.

    Deterministic and scripted, because a demo that runs a real scanner cannot be
    rehearsed. In a real Security platform this is where the image, secret, and
    SAST scanners get invoked; the shape of the result is the same either way,
    and so is everything the workflow does with it.

    Stage 1, dependency_scan, does not come through here: it has internal
    structure of its own (see generate_sbom, check_cve_database and
    check_license_compliance) and is sequenced directly by the workflow.
    """
    max_severity = _scripted_severity(input.scripted_outcome, input.stage_number)
    blocking = severity_at_or_above(max_severity, SCAN_FINDING_SEVERITY_THRESHOLD)
    result = {
        "stage": input.stage_number,
        "stage_name": stage_name(input.stage_number),
        # Findings at or above the threshold, which is the only count that
        # decides anything. A low-severity note is recorded in max_severity and
        # deliberately not counted here, so `findings == 0` is exactly the pass
        # condition rather than something close to it.
        "findings": 1 if blocking else 0,
        "max_severity": max_severity,
        "passed": not blocking,
    }
    activity.logger.info(
        "scan stage %s/%s %s max_severity=%s passed=%s",
        input.stage_number,
        input.stage_count,
        result["stage_name"],
        max_severity,
        result["passed"],
    )
    return result


def _scripted_severity(scripted_outcome: str, stage_number: int) -> str:
    severities = SCHEDULE.get(scripted_outcome)
    if severities is None:
        raise ApplicationError(
            f"unknown scripted_outcome {scripted_outcome!r}; expected one "
            f"of {sorted(SCHEDULE)}",
            non_retryable=True,
        )
    return severities[min(stage_number - 1, len(severities) - 1)]


# ------------------------------------------------- dependency_scan sub-stages
#
# The three steps stage 1 is actually made of. Each is its own Activity so a
# worker restart part-way through resumes at the next un-run step rather than
# re-resolving the whole dependency tree, and so Event History says which part
# of the dependency scan was running when something went wrong.


@activity.defn
def generate_sbom(input: GenerateSbomInput) -> dict[str, Any]:
    """Resolve the transitive dependency tree into an SBOM.

    The longest of the three, because real transitive resolution is: everything
    downstream is a lookup against what this produces.
    """
    time.sleep(1.5)
    digest = hashlib.sha256(
        f"sbom::{input.service}::{input.version}".encode()
    ).hexdigest()
    sbom_ref = f"sbom://{input.service}/{input.version}/{digest[:12]}"
    dependency_count = 180 + int(digest[:4], 16) % 240
    activity.logger.info(
        "resolved %s transitive dependencies into %s",
        dependency_count,
        sbom_ref,
    )
    return {
        "service": input.service,
        "version": input.version,
        "sbom_ref": sbom_ref,
        "dependency_count": dependency_count,
    }


@activity.defn
def check_cve_database(input: CheckCveInput) -> dict[str, Any]:
    """Check the SBOM against the advisory feed.

    The single controllable failure lever for the whole dependency_scan stage:
    the top-level scripted_outcome resolves to a severity here and nowhere else,
    so what fails the stage is exactly what the presenter chose.
    """
    time.sleep(1.0)
    max_severity = _scripted_severity(input.scripted_outcome, input.stage_number)
    blocking = severity_at_or_above(max_severity, SCAN_FINDING_SEVERITY_THRESHOLD)
    return {
        "check": "check_cve_database",
        "sbom_ref": input.sbom_ref,
        "findings": 1 if blocking else 0,
        "max_severity": max_severity,
        "passed": not blocking,
    }


@activity.defn
def check_license_compliance(input: CheckLicenseInput) -> dict[str, Any]:
    """Check the same SBOM's licences against the allowed list.

    Always clean, on purpose and regardless of scripted_outcome. Scripting this
    to fail as well would give dependency_scan two independent reasons to go red
    that could disagree with each other, and there is no narrative in the demo
    that would explain which one the audience is looking at.
    """
    time.sleep(1.0)
    return {
        "check": "check_license_compliance",
        "sbom_ref": input.sbom_ref,
        "findings": 0,
        "max_severity": "none",
        "passed": True,
    }


@activity.defn
def publish_scan_state(input: PublishScanStateInput) -> dict[str, Any]:
    """Report scan progress to the shared observability backend."""
    resp = requests.post(
        BACKEND_URL,
        json={
            "tool_name": "record_scan_state",
            "arguments": {
                "scan_workflow_id": input.scan_workflow_id,
                "service": input.service,
                "version": input.version,
                "environment": input.environment,
                "phase": input.phase,
                "stages_completed": input.stages_completed,
                "stage_count": input.stage_count,
                "severity_threshold": input.severity_threshold,
                "stage_results": input.stage_results,
                "verdict": input.verdict,
                "gateway_operation_id": input.gateway_operation_id,
                "error": input.error,
                # Stated rather than left to a default, because it is what the
                # dashboard keys the whole card treatment off. And no ttl_seconds:
                # this record does not age out. There is a Workflow behind it whose
                # Event History is the truth, so silence here means a Worker is
                # busy or restarting, not that the scan stopped existing. That
                # asymmetry with the legacy scanner is the point, not an oversight.
                "mode": "platform",
            },
        },
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()
