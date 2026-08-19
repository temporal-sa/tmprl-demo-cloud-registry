"""Fast, deterministic stand-ins for the release Child Workflows' own steps.

`cut_release` and `promote_release` are Child Workflows now, so the work that
used to be one `invoke_tool` call is a handful of separate Activities. Two of
those Activities reach the deployment backend over HTTP, and all of them sleep
for a second or two so a live demo has a believable elapsed-time story. Neither
property is wanted in a test suite.

These fakes register under the real Activity names, so the Child Workflows
themselves, the gate trigger, and the version-keyed wait are all genuinely
exercised -- only the leaves are swapped. Each one records what it did into
TOOL_CALLS under the name of the *logical* tool it stands for, so a test can
still assert "the pipeline cut once and promoted twice" without knowing which
Activity inside which child performed it.

Not registered here: `await_quality_gate`. That one is real in every test,
because refusing a verdict for the wrong (service, version) is exactly the
behavior worth testing.
"""

from __future__ import annotations

from typing import Any

from temporalio import activity

from common.models import (
    ArchiveArtifactsInput,
    CalculateHashesInput,
    DeployBinariesInput,
    HealthCheckInput,
    PublishQualityGateInput,
    QualityCheckInput,
    TagCommitInput,
    UpdateReleaseNotesInput,
    UpdateRoutingInput,
)
from mock_tool import server as backend

# (logical tool name, arguments, idempotency key), in the order they happened.
# Shared with each test module's own `invoke_tool` fake, so one list shows the
# whole sequence whether a step ran as an Activity or inside a Child Workflow.
TOOL_CALLS: list[tuple[str, dict, str]] = []

# Demo knobs, as sets rather than environment reads so a test can flip one
# without touching process state. The real Activities read the equivalent
# environment variables.
QUALITY_GATE_FAIL_VERSIONS: set[str] = set()
HEALTH_CHECK_FAIL_VERSIONS: set[str] = set()


def reset() -> None:
    TOOL_CALLS.clear()
    QUALITY_GATE_FAIL_VERSIONS.clear()
    HEALTH_CHECK_FAIL_VERSIONS.clear()


def tool_names() -> list[str]:
    return [name for name, _, _ in TOOL_CALLS]


def backend_call(tool_name: str, arguments: dict, key: str = "") -> dict:
    """Hit the real in-memory backend, keyed the way mock_tool.server.invoke is.

    A repeated Idempotency-Key returns the first result and does not apply the
    effect a second time, which is the guarantee the replay-safe retry path
    leans on, so this has to reproduce it rather than assume it.
    """
    TOOL_CALLS.append((tool_name, dict(arguments), key))
    if key and key in backend._seen:
        prior = dict(backend._seen[key])
        prior["idempotent_replay"] = True
        return prior
    result = backend._handle(tool_name, dict(arguments))
    result["idempotent_replay"] = False
    if key:
        backend._seen[key] = result
    return result


# ------------------------------------------------ CutReleaseChildWorkflow


@activity.defn(name="tag_commit_in_source_control")
async def fake_tag_commit_in_source_control(input: TagCommitInput) -> dict[str, Any]:
    return {
        "service": input.service,
        "version": input.version,
        "commit_sha": input.commit_sha or f"sha-{input.version}",
        "tag": f"v{input.version}",
        "message": f"tagged v{input.version}",
    }


@activity.defn(name="archive_artifacts")
async def fake_archive_artifacts(input: ArchiveArtifactsInput) -> dict[str, Any]:
    # Archiving is where the release becomes promotable, so this is the step
    # that registers it with the backend and therefore the one recorded as the
    # logical cut_release.
    registered = backend_call(
        "cut_release",
        {"service": input.service, "version": input.version},
        input.idempotency_key,
    )
    return {
        "service": input.service,
        "version": input.version,
        "artifact_ref": f"artifact://{input.service}/{input.version}",
        "artifact_bytes": 42_000_000,
        "already_cut": bool(registered.get("already_cut")),
        "idempotent_replay": bool(registered.get("idempotent_replay")),
        "message": f"archived {input.version}",
    }


@activity.defn(name="calculate_integrity_hashes")
async def fake_calculate_integrity_hashes(
    input: CalculateHashesInput,
) -> dict[str, Any]:
    return {
        "service": input.service,
        "version": input.version,
        "artifact_ref": input.artifact_ref,
        "sha256": f"sha256-{input.version}",
        "message": "hashed",
    }


# -------------------------------------------- PromoteReleaseChildWorkflow


@activity.defn(name="deploy_binaries")
async def fake_deploy_binaries(input: DeployBinariesInput) -> dict[str, Any]:
    return {
        "service": input.service,
        "version": input.version,
        "environment": input.environment,
        "instance_ids": ["i-aaa", "i-bbb", "i-ccc"],
        "message": "deployed",
    }


@activity.defn(name="health_check_new_instances")
async def fake_health_check_new_instances(input: HealthCheckInput) -> dict[str, Any]:
    healthy = input.version not in HEALTH_CHECK_FAIL_VERSIONS
    return {
        "service": input.service,
        "version": input.version,
        "environment": input.environment,
        "instance_ids": list(input.instance_ids),
        "healthy": healthy,
        "message": "healthy" if healthy else "instances failed their health check",
    }


@activity.defn(name="update_traffic_routing")
async def fake_update_traffic_routing(input: UpdateRoutingInput) -> dict[str, Any]:
    # The step that actually makes a version live, so this is the one recorded
    # as the logical promote_release.
    return backend_call(
        "promote_release",
        {
            "service": input.service,
            "version": input.version,
            "environment": input.environment,
        },
        input.idempotency_key,
    )


@activity.defn(name="update_release_notes")
async def fake_update_release_notes(
    input: UpdateReleaseNotesInput,
) -> dict[str, Any]:
    return {
        "service": input.service,
        "version": input.version,
        "environment": input.environment,
        "message": "notes updated",
    }


# ----------------------------------------------- QualityGateChildWorkflow


@activity.defn(name="run_quality_check")
async def fake_run_quality_check(input: QualityCheckInput) -> dict[str, Any]:
    passed = (
        input.scripted_outcome != "fail"
        and input.version not in QUALITY_GATE_FAIL_VERSIONS
    )
    return {
        "check_name": input.check_name,
        "passed": passed,
        "detail": "ok" if passed else "failed",
    }


@activity.defn(name="publish_quality_gate_state")
async def fake_publish_quality_gate_state(
    input: PublishQualityGateInput,
) -> dict[str, Any]:
    # Straight into the backend's module state, so a test can read the same
    # projection the dashboard would render.
    return backend._handle(
        "record_quality_gate_state",
        {
            "service": input.service,
            "version": input.version,
            "environment": input.environment,
            "status": input.status,
            "checks_completed": input.checks_completed,
            "check_count": input.check_count,
            "checks": input.checks,
            "gate_workflow_id": input.gate_workflow_id,
        },
    )


RELEASE_STEP_ACTIVITIES = [
    fake_tag_commit_in_source_control,
    fake_archive_artifacts,
    fake_calculate_integrity_hashes,
    fake_deploy_binaries,
    fake_health_check_new_instances,
    fake_update_traffic_routing,
    fake_update_release_notes,
    fake_run_quality_check,
    fake_publish_quality_gate_state,
]
