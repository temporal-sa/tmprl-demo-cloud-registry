from __future__ import annotations

import dataclasses
import hashlib
import os
import time
from typing import Any

import requests
from temporalio import activity
from temporalio.client import (
    WithStartWorkflowOperation,
    WorkflowUpdateStage,
)
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError

from common.models import (
    ArchiveArtifactsInput,
    AwaitQualityGateInput,
    CalculateHashesInput,
    ChainInput,
    CorrelationContext,
    DeployBinariesInput,
    EvaluatePolicyInput,
    GatewayOperationResolution,
    HealthCheckInput,
    InvokeToolInput,
    NestedToolCallRequest,
    PolicyDecision,
    PublishQualityGateInput,
    QualityCheckInput,
    QualityGateResult,
    SignalOperationCallbackInput,
    SubmitNestedToolCallInput,
    TagCommitInput,
    ToolCallRequest,
    ToolCallResponse,
    UpdateReleaseNotesInput,
    UpdateRoutingInput,
)

TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")
OPERATION_RESOLVED_SIGNAL = "operation_resolved"


def _protected_environments() -> set[str]:
    raw = os.getenv("PROTECTED_ENVIRONMENTS", "prod,production")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


@activity.defn
def evaluate_policy(input: EvaluatePolicyInput) -> PolicyDecision:
    """Represents the Agent Gateway policy engine.

    Kept in an Activity, not in Workflow code, so policy rules can change without
    a Workflow code change and without introducing non-determinism. Policy is a
    function of the tool identity and the requested arguments: promoting a release
    to a protected environment (prod) requires approval; everything else, including
    reads and promotions to staging, runs immediately.
    """
    if input.tool_name == "promote_release":
        environment = str(input.arguments.get("environment", "")).strip().lower()
        if environment in _protected_environments():
            return PolicyDecision(
                requires_approval=True,
                reason=f"Promotion to {environment} requires human approval.",
            )
    return PolicyDecision(requires_approval=False, reason=None)


@activity.defn
def invoke_tool(input: InvokeToolInput) -> dict[str, Any]:
    """Invoke the downstream tool.

    Activities have at-least-once semantics, so the idempotency key is passed
    downstream. A retry after a Worker crash or network blip must not promote a
    release twice. A longer-running tool would also call activity.heartbeat() here;
    this mock tool returns quickly, so it does not.
    """
    url = os.getenv("MOCK_TOOL_URL", "http://mock-tool:9000/invoke")
    try:
        resp = requests.post(
            url,
            json={"tool_name": input.tool_name, "arguments": input.arguments},
            headers={"Idempotency-Key": input.idempotency_key},
            timeout=45,
        )
    except requests.RequestException as err:
        # Transient transport failure: allow Temporal to retry per the retry policy.
        raise RuntimeError(f"tool transport error: {err}") from err

    if 400 <= resp.status_code < 500:
        # A client error will not succeed on retry, so mark it non-retryable.
        raise ApplicationError(
            f"tool rejected request: {resp.status_code} {resp.text}",
            non_retryable=True,
        )

    resp.raise_for_status()
    return resp.json()


# ------------------------------------------------ release steps (CASE-1, 2a)
#
# The individual steps inside CutReleaseChildWorkflow, PromoteReleaseChildWorkflow
# and QualityGateChildWorkflow. Each is its own Activity function, not a shared
# invoke_tool call with a different tool_name, because the point of the Child
# Workflows is that Event History names what actually happened: an operator
# looking at a stalled release sees archive_artifacts, not a fourth invoke_tool.
#
# Every one of them is a mock. Nothing here talks to real source control, a real
# artifact store, or a real test runner; each sleeps for a plausible interval and
# returns a synthetic payload derived deterministically from its input, so the
# same demo run reads the same way twice. The one exception is
# update_traffic_routing, which really does move the deployment backend's state,
# because that is the step that makes a version live.


def _mock_digest(*parts: str) -> str:
    """A stable synthetic digest for a set of inputs.

    Deterministic so the same release reports the same hash on every run and in
    the Web UI, rather than a fresh random value that would make two views of
    the same release disagree.
    """
    return hashlib.sha256("::".join(parts).encode()).hexdigest()


def _post_tool(tool_name: str, arguments: dict, idempotency_key: str = "") -> dict:
    """Call the deployment backend the same way invoke_tool does."""
    url = os.getenv("MOCK_TOOL_URL", "http://mock-tool:9000/invoke")
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
    resp = requests.post(
        url,
        json={"tool_name": tool_name, "arguments": arguments},
        headers=headers,
        timeout=45,
    )
    resp.raise_for_status()
    return resp.json()


def _fail_versions(name: str) -> set[str]:
    return {item.strip() for item in os.getenv(name, "").split(",") if item.strip()}


@activity.defn
def tag_commit_in_source_control(input: TagCommitInput) -> dict[str, Any]:
    """Tag the commit this release is cut from. First step of a cut."""
    time.sleep(1.0)
    commit_sha = input.commit_sha or _mock_digest(
        "commit", input.service, input.version
    )[:10]
    tag = f"v{input.version}"
    activity.logger.info(
        "tagged %s at %s as %s", input.service, commit_sha, tag
    )
    return {
        "service": input.service,
        "version": input.version,
        "commit_sha": commit_sha,
        "tag": tag,
        "message": f"tagged {commit_sha} as {tag}",
    }


@activity.defn
def archive_artifacts(input: ArchiveArtifactsInput) -> dict[str, Any]:
    """Archive the built artifact. The longest step of a cut, as in real life.

    Also the step at which the release becomes a real, promotable thing, so this
    is where the deployment backend learns about it and the dashboard's "Ready
    for release" rail picks it up. Archiving is the honest place for that: before
    it, there is a tag and nothing to deploy.
    """
    time.sleep(2.0)
    artifact_ref = (
        f"s3://quickmeals-artifacts/{input.service}/{input.version}/"
        f"{input.service}-{input.version}.tar.zst"
    )
    # Derived from the digest rather than random, so it does not change between
    # a run and its replay. Roughly 40-90 MB, which is a believable service.
    digest = _mock_digest("size", input.service, input.version)
    artifact_bytes = 40_000_000 + int(digest[:6], 16) % 50_000_000
    registered = _post_tool(
        "cut_release",
        {"service": input.service, "version": input.version},
        input.idempotency_key,
    )
    return {
        "service": input.service,
        "version": input.version,
        "artifact_ref": artifact_ref,
        "artifact_bytes": artifact_bytes,
        "already_cut": bool(registered.get("already_cut")),
        "message": f"archived {artifact_bytes} bytes to {artifact_ref}",
    }


@activity.defn
def calculate_integrity_hashes(input: CalculateHashesInput) -> dict[str, Any]:
    """Hash the archived artifact. Depends on the archive existing first."""
    time.sleep(1.0)
    sha256 = _mock_digest("artifact", input.artifact_ref)
    return {
        "service": input.service,
        "version": input.version,
        "artifact_ref": input.artifact_ref,
        "sha256": sha256,
        "message": f"sha256 {sha256[:16]}...",
    }


@activity.defn
def deploy_binaries(input: DeployBinariesInput) -> dict[str, Any]:
    """Place the new binaries on fresh instances. Nothing is serving yet."""
    time.sleep(2.0)
    seed = _mock_digest("instances", input.service, input.version, input.environment)
    instance_ids = [
        f"i-{seed[index * 8 : index * 8 + 8]}" for index in range(3)
    ]
    return {
        "service": input.service,
        "version": input.version,
        "environment": input.environment,
        "instance_ids": instance_ids,
        "message": (
            f"deployed {input.version} to {len(instance_ids)} instances in "
            f"{input.environment}"
        ),
    }


@activity.defn
def health_check_new_instances(input: HealthCheckInput) -> dict[str, Any]:
    """Check the new instances before any traffic is pointed at them.

    Scripted healthy by default. HEALTH_CHECK_FAIL_VERSIONS is the demo knob for
    the failure branch, which is implemented and correct but deliberately not
    part of the default scripted run: a promotion whose instances fail this must
    not go on to flip traffic onto them.
    """
    time.sleep(1.5)
    healthy = input.version not in _fail_versions("HEALTH_CHECK_FAIL_VERSIONS")
    return {
        "service": input.service,
        "version": input.version,
        "environment": input.environment,
        "instance_ids": list(input.instance_ids),
        "healthy": healthy,
        "message": (
            f"{len(input.instance_ids)} instances healthy"
            if healthy
            else f"{len(input.instance_ids)} instances failed their health check"
        ),
    }


@activity.defn
def update_traffic_routing(input: UpdateRoutingInput) -> dict[str, Any]:
    """Point the environment at the new version. This is the step that goes live.

    The only step in either release Child Workflow that touches real state: it
    moves the deployment backend's pointer for this environment, which is what
    get_deployed_version reads and what the dashboard's fleet panel renders. The
    idempotency key is threaded down from the operation, so a retried promotion
    finds the move already applied instead of applying it twice.
    """
    time.sleep(1.0)
    result = _post_tool(
        "promote_release",
        {
            "service": input.service,
            "version": input.version,
            "environment": input.environment,
        },
        input.idempotency_key,
    )
    activity.logger.info(
        "%s is now serving %s (was %s)",
        input.environment,
        input.version,
        result.get("previous_version"),
    )
    return result


@activity.defn
def update_release_notes(input: UpdateReleaseNotesInput) -> dict[str, Any]:
    """Append a changelog entry. Cosmetic, and after the version is already live."""
    time.sleep(1.0)
    return {
        "service": input.service,
        "version": input.version,
        "environment": input.environment,
        "entry": (
            f"{input.service} {input.version} promoted to {input.environment}"
        ),
        "message": f"release notes updated for {input.version}",
    }


# --------------------------------------------------------------- quality gates


@activity.defn
def run_quality_check(input: QualityCheckInput) -> dict[str, Any]:
    """Run one of the four quality checks against the staged candidate.

    One Activity reused for all four names rather than four near-identical
    functions. The check name is still visible per-attempt in Event History,
    because it is in the input payload.

    Duration is fixed per check name rather than random, so every demo run takes
    the same time and the four cards finish in the same order twice running. The
    values differ slightly between checks, so they visibly complete one at a time
    instead of in lockstep.
    """
    time.sleep(_QUALITY_CHECK_SECONDS.get(input.check_name, 1.75))
    passed = input.scripted_outcome != "fail" and input.version not in _fail_versions(
        "QUALITY_GATE_FAIL_VERSIONS"
    )
    result = {
        "check_name": input.check_name,
        "passed": passed,
        "detail": _QUALITY_CHECK_DETAIL[passed].get(input.check_name, ""),
    }
    activity.logger.info(
        "quality check %s for %s %s: %s",
        input.check_name,
        input.service,
        input.version,
        "passed" if passed else "failed",
    )
    return result


_QUALITY_CHECK_SECONDS = {
    "e2e_tests": 2.0,
    "user_acceptance_tests": 1.5,
    "performance_tests": 1.9,
    "accessibility_tests": 1.6,
}

# Fixed evidence strings. The point is that an approver has something concrete
# in front of them, not that the numbers are measured.
_QUALITY_CHECK_DETAIL = {
    True: {
        "e2e_tests": "412 passed",
        "user_acceptance_tests": "38 scenarios passed",
        "performance_tests": "p99 180ms, within budget",
        "accessibility_tests": "no WCAG AA violations",
    },
    False: {
        "e2e_tests": "9 failed",
        "user_acceptance_tests": "3 scenarios failed",
        "performance_tests": "p99 940ms, over budget",
        "accessibility_tests": "4 WCAG AA violations",
    },
}


@activity.defn
def publish_quality_gate_state(input: PublishQualityGateInput) -> dict[str, Any]:
    """Push gate progress to the shared observability backend.

    Visibility only, the same contract the security scan's publish step has: the
    verdict is already durable in the gate workflow's own history, so a publish
    failure changes nothing about what the gates found.
    """
    return _post_tool(
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


@activity.defn
async def await_quality_gate(input: AwaitQualityGateInput) -> dict[str, Any]:
    """Wait for one specific gate workflow's verdict, and check it is the right one.

    The release pipeline cannot read "the current gate state": a gate workflow
    starts on every staging promotion from any source, including a CASE-1 manual
    one the operator made a moment ago, so the newest verdict is not necessarily
    about the version this pipeline just staged. This waits on the workflow id
    the promotion handed back and then refuses a result that is not for the
    expected (service, version) pair, so a stale or crossed verdict fails the
    pipeline rather than being promoted on.

    An Activity because the gate workflow was started by, and abandoned from,
    PromoteReleaseChildWorkflow, so the chain has no child handle for it and
    workflow code cannot hold a client.
    """
    client = activity.client()
    handle = client.get_workflow_handle(
        input.gate_workflow_id, result_type=QualityGateResult
    )
    result: QualityGateResult = await handle.result()
    if result.service != input.service or result.version != input.version:
        raise ApplicationError(
            f"quality gate {input.gate_workflow_id} reported on "
            f"{result.service} {result.version}, but the pipeline staged "
            f"{input.service} {input.version}",
            non_retryable=True,
        )
    activity.logger.info(
        "quality gate %s for %s %s: %s",
        input.gate_workflow_id,
        result.service,
        result.version,
        result.status,
    )
    return {
        "gate_workflow_id": input.gate_workflow_id,
        "service": result.service,
        "version": result.version,
        "status": result.status,
        "passed": result.status == "passed",
        "checks": list(result.checks),
    }


# ------------------------------------------------- serving an external caller
#
# Every Activity below uses activity.client(), which is the Worker's own client
# and therefore this worker's OWN namespace, by construction rather than by
# convention. There is no address, namespace or credential anywhere in this
# module to point somewhere else by mistake, and no per-attempt TLS connection to
# leak: the cross-team hop is a Nexus Endpoint, and by the time any of these runs
# the call has already crossed it.
#
# Workflow code still never holds a client, because it has to replay
# deterministically and a network call cannot.


@activity.defn
async def submit_tool_call(input: ToolCallRequest) -> dict[str, Any]:
    """Submit a direct call to the same chain entrypoint used by Claude Code.

    Workflow code cannot issue an Update against another workflow, so the
    autonomous ADK checkpoint shell uses this same-namespace Activity as a thin
    adapter. The request is already fully formed at the gateway boundary,
    including its snapshotted security controls and callback target; this
    Activity does not make policy decisions or modify it.

    Update-with-start and the request's application idempotency key make retries
    safe whether the companion chain exists already or is created by this call.
    String workflow/update names avoid a chain -> activities -> chain import
    cycle.
    """
    request = input
    workflow_id = request.correlation.workflow_id
    if not workflow_id:
        raise ApplicationError(
            "governed tool call is missing its chain workflow_id",
            non_retryable=True,
        )

    client = activity.client()
    start_op = WithStartWorkflowOperation(
        "AgenticChainWorkflow",
        ChainInput(
            workflow_id=workflow_id,
            owner_principal=request.correlation.caller_principal or "",
        ),
        id=workflow_id,
        task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    update = await client.start_update_with_start_workflow(
        "request_tool_call",
        request,
        # Scoped to THIS autonomous run, and that scoping is the whole fix for
        # a demo that wedged on its second showing.
        #
        # Every id on this path is derived from the agent run id, which the
        # agent derives as release-<service>-<version>-<environment>. So a
        # second run of the same release produced the same operation id and
        # therefore the same Update ID. Temporal retains a completed update's
        # outcome per Update ID, so the repeat did not reach a workflow at all:
        # it was served the FIRST run's response -- "security is running a
        # pre-prod security scan" -- while that scan had long since finished.
        # No chain started, no scan started, and the caller waited forever on a
        # resolution that had already been delivered to its predecessor.
        #
        # activity.info().workflow_run_id is the calling AutonomousAgentWorkflow's
        # run, so it is identical across retries of this Activity (which is the
        # deduplication that was actually wanted) and different for every new
        # run of the same release (which is the collision that was not).
        #
        # Recovery semantics are unchanged and now actually work: if the earlier
        # chain is still RUNNING, USE_EXISTING attaches to it, the update is new,
        # and the chain's own idempotency_index recognises the request and
        # returns its LIVE state while re-pointing the callback at this caller.
        # If it has closed, a fresh chain starts and the release runs again.
        id=(
            f"tool-call::{activity.info().workflow_run_id}"
            f"::{request.operation_id or request.idempotency_key}"
        ),
        wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        start_workflow_operation=start_op,
        result_type=ToolCallResponse,
    )
    response: ToolCallResponse = await update.result()
    activity.logger.info(
        "chain %s accepted governed direct call %s with status %s",
        workflow_id,
        response.operation_id,
        response.status,
    )
    return dataclasses.asdict(response)


@activity.defn
async def submit_nested_tool_call(
    input: SubmitNestedToolCallInput,
) -> dict[str, Any]:
    """Lodge an external tool's protected-action request with the chain.

    An Update, which workflow code cannot issue directly, so
    ProtectedActionWorkflow goes through here. The chain is addressed by the
    workflow_id the caller carried as correlation context, so the request lands
    on the operator's existing session rather than opening a second one.

    caller_principal is the chain's owner, threaded through from whoever asked
    for the release. The external tool is acting on their behalf, and the chain
    checks a nested call against its owner, so a tool that invented its own
    principal here would simply be refused. Its own identity travels alongside
    in caller_service and in the call path, so the ledger records both who
    authorized the release and which system actually asked.
    """
    client = activity.client()
    handle = client.get_workflow_handle(input.gateway_workflow_id)
    request = NestedToolCallRequest(
        tool1_name=f"{input.caller_service.replace('-', '_')}_scan",
        tool1_arguments={
            **input.arguments,
            "caller_workflow_id": input.caller_workflow_id,
        },
        tool2_name=input.tool_name,
        tool2_arguments=dict(input.arguments),
        idempotency_key=input.idempotency_key,
        correlation=CorrelationContext(
            workflow_id=input.gateway_workflow_id,
            workflow_id_source="explicit_authorized",
            idempotency_key=input.idempotency_key,
            caller_principal=input.caller_principal,
            caller_service=input.caller_service,
            runtime="nexus",
            call_path=[
                input.caller_service,
                f"{input.caller_service.replace('-', '_')}_scan",
            ],
        ),
        requested_action=(
            f"Promote {input.arguments.get('service', '?')} "
            f"{input.arguments.get('version', '?')} to "
            f"{input.arguments.get('environment', '?')}"
        ),
        justification=input.justification or None,
        controlled_tool1=True,
        replay_safe=True,
        callback_workflow_id=input.callback_workflow_id,
        origin_operation_id=input.origin_operation_id,
    )
    response: ToolCallResponse = await handle.execute_update(
        "request_nested_tool_call", request, result_type=ToolCallResponse
    )
    activity.logger.info(
        "chain %s answered %s for %s",
        input.gateway_workflow_id,
        response.status,
        input.caller_service,
    )
    return dataclasses.asdict(response)


@activity.defn
async def signal_operation_callback(input: SignalOperationCallbackInput) -> None:
    """Tell the workflow holding a request open that its operation resolved.

    The other half of the pause. Something is suspended waiting for this --
    in CASE-2b a ProtectedActionWorkflow, which is in turn the handler for a
    Nexus operation another team's security scan is suspended on. Delivering it
    here completes that chain of waits without anyone polling.
    """
    client = activity.client()
    handle = client.get_workflow_handle(input.callback_workflow_id)
    await handle.signal(
        OPERATION_RESOLVED_SIGNAL,
        GatewayOperationResolution(
            operation_id=input.operation_id,
            status=input.status,
            result=input.result,
            reason=input.reason,
        ),
    )
    activity.logger.info(
        "told %s that %s is %s",
        input.callback_workflow_id,
        input.operation_id,
        input.status,
    )
