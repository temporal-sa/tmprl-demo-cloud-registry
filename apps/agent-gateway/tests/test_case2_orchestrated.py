"""CASE-2, orchestrated: Tool1 works out the version instead of being told it.

The explicit-version CASE-2 path is covered by
tests/test_temporal_scenarios.py::test_case2_controlled_resume_and_uncontrolled_fail_closed,
which is deliberately untouched. These tests cover the bump-driven path and use
their own Activity fakes so nothing here can alter the fixtures that suite (or
the CASE-1 and CASE-3 suites in it) depends on.

The invoke_tool fake here delegates to the real deployment backend in
mock_tool.server rather than returning a canned dict, so what the orchestrator
reads back is what the real tool would say: prod really is on 2.2.0, a minor bump
really lands on 2.3.0, and cutting the same version twice really is a no-op.

Cutting and promoting are Child Workflows, so the real steps inside them are
swapped for the fakes in tests/release_step_fakes.py. The children themselves,
the gate run a staging landing triggers, and the pipeline's version-keyed wait on
that gate are all real here; only the leaf Activities are stubbed, to drop the
HTTP calls and the demo-paced sleeps.
"""

from __future__ import annotations

import asyncio
import copy
import json
import shutil
import uuid

from temporalio import activity
from temporalio.client import WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities.gateway_activities import await_quality_gate
from common.models import (
    ApprovalDecision,
    ChainInput,
    CorrelationContext,
    EvaluatePolicyInput,
    InvokeToolInput,
    NestedToolCallRequest,
    PolicyDecision,
    ResumeNestedRequest,
    ToolCallResponse,
)
from mock_tool import server as backend
from tests import release_step_fakes
from tests.release_step_fakes import RELEASE_STEP_ACTIVITIES, TOOL_CALLS
from workflows.chain import AgenticChainWorkflow
from workflows.release_children import (
    CutReleaseChildWorkflow,
    PromoteReleaseChildWorkflow,
    QualityGateChildWorkflow,
)

TASK_QUEUE = "test-agent-gateway-orchestrated"
SERVICE = "delivery-matching-service"
PRINCIPAL = "requester@example.com"

# (tool_name, arguments, idempotency_key) for every downstream tool invocation,
# whether it ran as an Activity or as a step inside a release Child Workflow.
INVOKE_CALLS = TOOL_CALLS

# The seeded fleet, captured before any test mutates it. Both dicts are module
# state in mock_tool.server, so each test restores them.
_PRISTINE_DEPLOYED = copy.deepcopy(backend._deployed)
_PRISTINE_RELEASES = copy.deepcopy(backend._releases)
_IDLE_GATES = copy.deepcopy(backend._quality_gates)


def _reset_backend() -> None:
    release_step_fakes.reset()
    backend._deployed.clear()
    backend._deployed.update(copy.deepcopy(_PRISTINE_DEPLOYED))
    backend._releases.clear()
    backend._releases.update(copy.deepcopy(_PRISTINE_RELEASES))
    backend._seen.clear()
    backend._quality_gates = copy.deepcopy(_IDLE_GATES)


def _tool_names() -> list[str]:
    return [name for name, _, _ in INVOKE_CALLS]


def _call(tool_name: str) -> tuple[str, dict, str]:
    matches = [entry for entry in INVOKE_CALLS if entry[0] == tool_name]
    assert matches, f"{tool_name} was never invoked; calls were {_tool_names()}"
    return matches[0]


async def _wait_for_gate(status: str = "passed", timeout: float = 10) -> dict:
    """Wait for the backend's gate projection to reach a terminal status.

    A gate run is started by the staging promotion and abandoned, so nothing in
    the pipeline path waits on it except the pipeline's own explicit wait. A
    test that wants to assert on the card has to wait for it the way the
    dashboard's poll would.
    """

    async def poll() -> dict:
        while True:
            gate = backend._quality_gates
            if gate and gate.get("status") == status:
                return dict(gate)
            await asyncio.sleep(0.02)

    return await asyncio.wait_for(poll(), timeout=timeout)


@activity.defn(name="evaluate_policy")
async def fake_evaluate_policy(input: EvaluatePolicyInput) -> PolicyDecision:
    """Mirrors activities.gateway_activities.evaluate_policy.

    Only promote_release to prod is protected. Reads and staging promotions run
    immediately, which is what makes Tool1's new internal work invisible to the
    approval surface.
    """
    requires_approval = (
        input.tool_name == "promote_release"
        and str(input.arguments.get("environment", "")).lower() == "prod"
    )
    return PolicyDecision(
        requires_approval=requires_approval,
        reason=(
            "Promotion to prod requires human approval."
            if requires_approval
            else None
        ),
    )


@activity.defn(name="invoke_tool")
async def backend_invoke_tool(input: InvokeToolInput) -> dict:
    """Invoke the real mock deployment backend, keyed the way the real one is.

    Mirrors mock_tool.server.invoke: a repeated Idempotency-Key returns the first
    result and does not apply the effect a second time. That is the guarantee the
    replay-safe retry path leans on, so the fake has to reproduce it rather than
    assume it.
    """
    return release_step_fakes.backend_call(
        input.tool_name, dict(input.arguments), input.idempotency_key
    )


async def _environment() -> WorkflowEnvironment:
    temporal = shutil.which("temporal")
    assert temporal, "Temporal CLI is required for workflow tests"
    env = await WorkflowEnvironment.start_local(
        dev_server_existing_path=temporal,
        dev_server_log_level="error",
    )
    # await_quality_gate is the one release Activity kept real, and it waits on a
    # specific gate workflow through activity.client() -- the Worker's own client,
    # so it lands on this test server with nothing to point at it.
    return env


def _worker(env: WorkflowEnvironment) -> Worker:
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[
            AgenticChainWorkflow,
            CutReleaseChildWorkflow,
            PromoteReleaseChildWorkflow,
            QualityGateChildWorkflow,
        ],
        activities=[
            fake_evaluate_policy,
            backend_invoke_tool,
            await_quality_gate,
            *RELEASE_STEP_ACTIVITIES,
        ],
    )


async def _start_chain(env: WorkflowEnvironment, name: str) -> WorkflowHandle:
    workflow_id = f"wf-{name}-{uuid.uuid4().hex[:8]}"
    return await env.client.start_workflow(
        AgenticChainWorkflow.run,
        ChainInput(workflow_id=workflow_id, owner_principal=PRINCIPAL),
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )


async def _wait_for_status(
    handle: WorkflowHandle,
    operation_id: str,
    expected: str,
    timeout: float = 5,
) -> ToolCallResponse:
    async def poll():
        while True:
            response = await handle.query(
                "get_operation_status",
                operation_id,
                result_type=ToolCallResponse,
            )
            if response.status == expected:
                return response
            await asyncio.sleep(0.02)

    return await asyncio.wait_for(poll(), timeout=timeout)


async def _operation_view(handle: WorkflowHandle, operation_id: str):
    summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
    for op in summary.operations:
        if op.operation_id == operation_id:
            return op
    raise AssertionError(f"{operation_id} not found in workflow status")


def _orchestrated_request(
    workflow_id: str,
    key: str,
    *,
    bump: str,
    environment: str,
    controlled: bool = True,
    replay_safe: bool = False,
) -> NestedToolCallRequest:
    """The request gateway.server._submit_nested_release builds for a bump call.

    Note what is absent: tool2_arguments carries no version, because at this point
    nobody knows it yet. Tool1 resolves it inside the workflow.
    """
    tool1_arguments = {
        "service": SERVICE,
        "bump": bump,
        "environment": environment,
    }
    tool2_arguments = {"service": SERVICE, "environment": environment}
    return NestedToolCallRequest(
        tool1_name="release_orchestrator",
        tool1_arguments=tool1_arguments,
        tool2_name="promote_release",
        tool2_arguments=tool2_arguments,
        safe_tool1_arguments=dict(tool1_arguments),
        safe_tool2_arguments=dict(tool2_arguments),
        idempotency_key=key,
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="explicit_authorized",
            caller_principal=PRINCIPAL,
            runtime="ClaudeCode",
            call_path=["ClaudeCode", "release_orchestrator"],
        ),
        requested_action=(
            f"Promote the next {bump} version of {SERVICE} to {environment}"
        ),
        parent_operation_id=f"{key}-parent",
        nested_operation_id=f"{key}-child",
        bump=bump,
        controlled_tool1=controlled,
        replay_safe=replay_safe,
    )


def _named_version_request(
    workflow_id: str,
    key: str,
    *,
    version: str,
    environment: str,
) -> NestedToolCallRequest:
    """The request gateway.server.start_google_adk_release_run builds.

    The same pipeline as _orchestrated_request above, asked for the other way
    round: the caller names the version instead of a bump type, which is how a
    person talks to an autonomous agent ("ship 2.3.0", not "ship the next
    minor"). orchestrate is what says so -- an explicit version without it is
    CASE-2a, a Tool1 whose only step is to discover the protected promotion.
    """
    arguments = {
        "service": SERVICE,
        "version": version,
        "environment": environment,
    }
    return NestedToolCallRequest(
        tool1_name="release_orchestrator",
        tool1_arguments=dict(arguments),
        tool2_name="promote_release",
        tool2_arguments=dict(arguments),
        safe_tool1_arguments=dict(arguments),
        safe_tool2_arguments=dict(arguments),
        idempotency_key=key,
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="derived_from_autonomous_run",
            caller_principal=PRINCIPAL,
            runtime="GoogleADKAgent",
            call_path=["GoogleADKAgent", "release_orchestrator"],
        ),
        requested_action=(
            f"Promote {SERVICE} {version} to {environment}"
        ),
        parent_operation_id=f"{key}-parent",
        nested_operation_id=f"{key}-child",
        orchestrate=True,
        controlled_tool1=True,
    )


def test_case2_orchestrated_controlled_prod_staging_dedup_and_bad_version() -> None:
    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                await _prod_requires_approval_for_the_computed_version(env)
                await _staging_target_runs_the_first_leg_only(env)
                await _duplicate_call_dedups_without_a_second_cut(env)
                await _unparseable_deployed_version_fails_before_cutting(env)

    asyncio.run(run())


async def _prod_requires_approval_for_the_computed_version(
    env: WorkflowEnvironment,
) -> None:
    _reset_backend()
    handle = await _start_chain(env, "orch-prod")
    request = _orchestrated_request(
        handle.id, "prod-minor", bump="minor", environment="prod"
    )

    waiting = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call, request
    )

    # Tool1 ran the team's whole pipeline: read production (seeded at 2.2.0),
    # computed the next minor, cut it, and put it on staging. Landing on staging
    # started a gate run, which the pipeline then waited on. Then it opened the
    # production promotion itself, which is CASE-2a: the security mandate is off
    # on these requests, so there is no scan step in the flow and the pipeline
    # does not even ask whether one is on offer.
    #
    # Note there is no "run_quality_gates" call. There is no such tool any more:
    # the gates are a Child Workflow the staging promotion starts, not something
    # the pipeline invokes.
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
    ]
    assert "get_security_scan_status" not in _tool_names()
    assert "release_orchestrator_prepare" not in _tool_names()
    # The bump always reads production, whatever the target.
    assert _call("get_deployed_version")[1] == {"environment": "prod"}
    assert _call("cut_release")[1] == {
        "service": SERVICE,
        "version": "2.3.0",
    }
    # The internal promotion goes to staging, not the target.
    assert _call("promote_release")[1] == {
        "service": SERVICE,
        "version": "2.3.0",
        "environment": "staging",
    }
    assert (SERVICE, "2.3.0") in backend._releases
    assert backend._deployed["staging"]["version"] == "2.3.0"
    # The gate card reflects the candidate that was staged, and it is green.
    assert backend._quality_gates["status"] == "passed"
    assert backend._quality_gates["version"] == "2.3.0"
    assert len(backend._quality_gates["checks"]) == 4

    # The protected promotion is the only thing that reached the approver, and it
    # names the version Tool1 computed.
    assert waiting.status == "waiting_for_approval"
    assert waiting.reason == "approval_required"
    assert waiting.parent_operation_id == "prod-minor-parent"
    assert waiting.call_path == [
        "ClaudeCode",
        "release_orchestrator",
        "promote_release",
    ]
    child = await _operation_view(handle, "prod-minor-child")
    assert child.protected is True
    assert child.arguments == {
        "service": SERVICE,
        "environment": "prod",
        "version": "2.3.0",
    }
    assert child.requested_action == (
        f"Promote {SERVICE} 2.3.0 to prod"
    )
    parent = await _operation_view(handle, "prod-minor-parent")
    assert parent.status == "waiting_for_dependency"
    assert parent.protected is False
    assert parent.checkpoint["stage"] == "waiting_for_tool2"
    assert parent.checkpoint["resolved_version"] == "2.3.0"
    assert parent.checkpoint["bump"] == "minor"
    prepare_result = parent.checkpoint["tool1_prepare_result"]
    assert prepare_result["current_version"] == "2.2.0"
    assert prepare_result["version"] == "2.3.0"
    # The cut is a Child Workflow now, so its result carries what the three
    # steps produced rather than a single tool's response.
    cut_result = prepare_result["cut_release_result"]
    assert cut_result["commit_sha"]
    assert cut_result["artifact_ref"]
    assert cut_result["sha256"]
    # The expensive part of the pipeline is in the checkpoint, waiting on a human.
    staged_result = prepare_result["staging_promotion_result"]
    assert staged_result["status"] == "completed"
    assert staged_result["previous_version"] == "2.2.0"
    # The pipeline waited on the gate its OWN staging promotion started, not on
    # whatever gate happened to have run most recently.
    gate_result = prepare_result["quality_gate_result"]
    assert gate_result["passed"] is True
    assert gate_result["version"] == "2.3.0"
    assert gate_result["gate_workflow_id"] == staged_result[
        "quality_gate_workflow_id"
    ]
    assert "2.3.0" in gate_result["gate_workflow_id"]
    # Production is untouched: cut, staged, and gated, but not promoted.
    assert backend._deployed["prod"]["version"] == "2.2.0"

    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id="prod-minor-child",
            approver="approver@example.com",
        ),
    )
    completed = await _wait_for_status(handle, "prod-minor-parent", "completed")

    assert completed.result["resumed_from_checkpoint"] is True
    assert completed.result["tool2_result"]["status"] == "completed"
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
        "promote_release",
        "release_orchestrator_resume",
    ]
    promotions = [e[1] for e in INVOKE_CALLS if e[0] == "promote_release"]
    assert promotions[1] == {
        "service": SERVICE,
        "version": "2.3.0",
        "environment": "prod",
    }
    assert backend._deployed["prod"]["version"] == "2.3.0"
    assert backend._deployed["prod"]["previous_version"] == "2.2.0"

    ledger = await handle.query(AgenticChainWorkflow.get_ledger)
    events = {entry.event for entry in ledger}
    # tool1_quality_gate_verdict, not tool1_quality_gates: the pipeline read a
    # verdict, it did not run a check.
    assert {
        "tool1_version_resolved",
        "tool1_release_cut",
        "tool1_staged",
        "tool1_quality_gate_verdict",
        "tool1_checkpointed",
        "waiting_for_approval",
        "approved",
        "tool1_completed",
    }.issubset(events)
    assert "tool1_quality_gates" not in events
    resolved = next(
        entry for entry in ledger if entry.event == "tool1_version_resolved"
    )
    assert resolved.detail["current_version"] == "2.2.0"
    assert resolved.detail["resolved_version"] == "2.3.0"
    verdict = next(
        entry for entry in ledger if entry.event == "tool1_quality_gate_verdict"
    )
    assert verdict.detail["passed"] is True
    assert verdict.detail["version"] == "2.3.0"
    # The prod promotion carries no team restriction: no Security scan gated it.
    assert child.required_approver_team is None


async def _staging_target_runs_the_first_leg_only(env: WorkflowEnvironment) -> None:
    _reset_backend()
    handle = await _start_chain(env, "orch-staging")

    response = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _orchestrated_request(
            handle.id,
            "staging-bugfix",
            bump="bugfix",
            environment="staging",
        ),
    )

    # No approval anywhere in this flow, and no waiting state to poll.
    assert response.status == "completed"
    # A staging target stops after staging: the pipeline never waits on a gate,
    # because nothing is being qualified for production yet.
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
        "release_orchestrator_resume",
    ]
    ledger = await handle.query(AgenticChainWorkflow.get_ledger)
    assert not [
        entry
        for entry in ledger
        if entry.event == "tool1_quality_gate_verdict"
    ]
    # A gate run still HAPPENS, though, because something reached staging, and
    # that is the point of the redesign: the gate reacts to the landing rather
    # than to the pipeline. It just runs alongside, with nobody waiting on it.
    child_view = await _operation_view(handle, "staging-bugfix-child")
    assert child_view.result["quality_gate_workflow_id"]
    gate = await _wait_for_gate("passed")
    assert gate["version"] == "2.2.1"
    # Still bumped from production, not from staging.
    assert _call("get_deployed_version")[1] == {"environment": "prod"}
    assert _call("cut_release")[1]["version"] == "2.2.1"
    assert _call("promote_release")[1] == {
        "service": SERVICE,
        "version": "2.2.1",
        "environment": "staging",
    }
    child = await _operation_view(handle, "staging-bugfix-child")
    assert child.status == "completed"
    assert child.protected is False
    assert backend._deployed["staging"]["version"] == "2.2.1"
    assert backend._deployed["prod"]["version"] == "2.2.0"


async def _duplicate_call_dedups_without_a_second_cut(
    env: WorkflowEnvironment,
) -> None:
    _reset_backend()
    handle = await _start_chain(env, "orch-dedup")
    request = _orchestrated_request(
        handle.id, "dup-minor", bump="minor", environment="prod"
    )

    first = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call, request
    )
    # Same idempotency key while the first call is still waiting for approval.
    second = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call, request
    )

    assert first.status == second.status == "waiting_for_approval"
    assert first.operation_id == second.operation_id == "dup-minor-child"
    assert (
        first.parent_operation_id
        == second.parent_operation_id
        == "dup-minor-parent"
    )
    # The dedup hit returned the existing pair instead of re-deriving a version
    # and running the pipeline a second time.
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
    ]
    ledger = await handle.query(AgenticChainWorkflow.get_ledger)
    assert any(entry.event == "dedup_hit" for entry in ledger)


async def _unparseable_deployed_version_fails_before_cutting(
    env: WorkflowEnvironment,
) -> None:
    _reset_backend()
    backend._deployed["prod"]["version"] = "2.x-nightly"
    handle = await _start_chain(env, "orch-badversion")

    response = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _orchestrated_request(
            handle.id, "bad-minor", bump="minor", environment="prod"
        ),
    )

    # A version the orchestrator cannot read stops the flow at the read. It does
    # not guess a version, does not cut anything, and never creates the child, so
    # nothing lands on the approval surface.
    assert response.status == "failed"
    assert "2.x-nightly" in response.reason
    assert _tool_names() == ["get_deployed_version"]
    parent = await _operation_view(handle, "bad-minor-parent")
    assert parent.status == "failed"
    assert parent.child_operation_id is None
    summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
    assert not [
        op for op in summary.operations if op.status == "waiting_for_approval"
    ]
    ledger = await handle.query(AgenticChainWorkflow.get_ledger)
    assert any(
        entry.event == "tool1_version_resolution_failed" for entry in ledger
    )


def test_case2_orchestrated_uncontrolled_fail_closed_and_replay_safe_retry() -> None:
    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                await _uncontrolled_without_replay_safety_stays_blocked(env)
                await _replay_safe_retry_reverifies_without_remutating(env)

    asyncio.run(run())


async def _uncontrolled_without_replay_safety_stays_blocked(
    env: WorkflowEnvironment,
) -> None:
    _reset_backend()
    handle = await _start_chain(env, "orch-unsafe")

    blocked = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _orchestrated_request(
            handle.id,
            "unsafe-minor",
            bump="minor",
            environment="prod",
            controlled=False,
            replay_safe=False,
        ),
    )

    assert blocked.status == "blocked_nested_approval"
    assert blocked.retry_required is True
    parent = await _operation_view(handle, "unsafe-minor-parent")
    assert parent.status == "blocked"

    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id="unsafe-minor-child",
            approver="approver@example.com",
        ),
    )
    approved = await _wait_for_status(
        handle, "unsafe-minor-child", "approved_retry_required"
    )
    assert approved.retry_required is True

    refused = await handle.execute_update(
        AgenticChainWorkflow.resume_nested_tool_call,
        ResumeNestedRequest(
            operation_id="unsafe-minor-child",
            caller_principal=PRINCIPAL,
        ),
    )

    assert refused.reason == "uncontrolled_tool_not_replay_safe"
    # The whole pipeline ran once, up to the boundary, and is now stranded: a cut
    # release, a staging deployment, and a completed gate run, with production
    # untouched. That stranded work is the cost of a Tool1 that cannot suspend.
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
    ]
    assert backend._deployed["staging"]["version"] == "2.3.0"
    assert backend._deployed["prod"]["version"] == "2.2.0"
    assert backend._quality_gates["status"] == "passed"


async def _replay_safe_retry_reverifies_without_remutating(
    env: WorkflowEnvironment,
) -> None:
    """The replay pays for the gates again, and only for the gates.

    This is the whole argument for a suspension-aware Tool1, priced. Mutations are
    keyed once, so the cut and the staging promotion are found already done. The
    gate is not keyed at all: the replay's own staging promotion starts a fresh
    gate run and the replay waits on that one, so the candidate is re-qualified
    rather than promoted on a verdict from before the pause.
    """
    _reset_backend()
    handle = await _start_chain(env, "orch-replayable")

    await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _orchestrated_request(
            handle.id,
            "replay-minor",
            bump="minor",
            environment="prod",
            controlled=False,
            replay_safe=True,
        ),
    )
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
    ]
    first_cut_at = backend._releases[(SERVICE, "2.3.0")]["cut_at"]
    releases_after_first_cut = len(backend._releases)
    first_view = await _operation_view(handle, "replay-minor-parent")
    first_gate_id = first_view.checkpoint["tool1_prepare_result"][
        "quality_gate_result"
    ]["gate_workflow_id"]

    await handle.signal(
        AgenticChainWorkflow.approve_operation,
        ApprovalDecision(
            operation_id="replay-minor-child",
            approver="approver@example.com",
        ),
    )
    await _wait_for_status(
        handle, "replay-minor-child", "approved_retry_required"
    )

    resumed = await handle.execute_update(
        AgenticChainWorkflow.resume_nested_tool_call,
        ResumeNestedRequest(
            operation_id="replay-minor-child",
            caller_principal=PRINCIPAL,
        ),
    )

    assert resumed.status == "completed"
    assert resumed.result["replayed"] is True
    # The replay reran the whole pipeline: every step was invoked a second time.
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
        "get_deployed_version",
        "cut_release",
        "promote_release",
        "promote_release",
        "release_orchestrator_resume",
    ]

    cuts = [entry for entry in INVOKE_CALLS if entry[0] == "cut_release"]
    stagings = [
        entry
        for entry in INVOKE_CALLS
        if entry[0] == "promote_release" and entry[1]["environment"] == "staging"
    ]

    # Mutations: same idempotency key on both passes, so the second call is a
    # replay of the first and the effect is applied once.
    assert cuts[0][2] == cuts[1][2]
    assert stagings[0][2] == stagings[1][2]
    assert len(backend._releases) == releases_after_first_cut
    assert backend._releases[(SERVICE, "2.3.0")]["cut_at"] == first_cut_at
    assert backend._deployed["staging"]["promotions"] == 1

    replay_view = await _operation_view(handle, "replay-minor-parent")
    replayed = replay_view.checkpoint["tool1_replay_result"]
    assert replayed["cut_release_result"]["service"] == SERVICE
    assert replayed["staging_promotion_result"]["status"] == "completed"
    # Verification: the replay's own staging promotion started a NEW gate run
    # and the replay waited on that one, so the promotion is not going ahead on
    # a verdict from before the pause. That is the cost of a Tool1 that could
    # not hold it.
    replay_gate_id = replayed["quality_gate_result"]["gate_workflow_id"]
    assert replay_gate_id != first_gate_id
    assert replay_gate_id == replayed["staging_promotion_result"][
        "quality_gate_workflow_id"
    ]
    assert replayed["quality_gate_result"]["passed"] is True

    # Tool2 still ran exactly once, on the approved version.
    promotions = [
        entry
        for entry in INVOKE_CALLS
        if entry[0] == "promote_release" and entry[1]["environment"] == "prod"
    ]
    assert len(promotions) == 1
    assert promotions[0][1]["version"] == "2.3.0"
    assert backend._deployed["prod"]["version"] == "2.3.0"


def test_case2_orchestrated_failing_gates_never_reach_the_approver() -> None:
    """The strongest safety property: the system declines to even ask.

    A candidate that fails its gates produces no child operation, so there is
    nothing on the approval queue for a human to wave through.
    """

    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                _reset_backend()
                # prod 2.2.0, minor bump -> 2.3.0, which this run fails.
                release_step_fakes.QUALITY_GATE_FAIL_VERSIONS.add("2.3.0")
                try:
                    handle = await _start_chain(env, "orch-gatefail")
                    response = await handle.execute_update(
                        AgenticChainWorkflow.request_nested_tool_call,
                        _orchestrated_request(
                            handle.id,
                            "gatefail-minor",
                            bump="minor",
                            environment="prod",
                        ),
                    )
                finally:
                    release_step_fakes.QUALITY_GATE_FAIL_VERSIONS.discard("2.3.0")

                assert response.status == "failed"
                assert "quality gates failed" in response.reason.lower()
                # The pipeline stopped on the verdict. Production was never
                # promoted and the target promotion was never even created.
                assert _tool_names() == [
                    "get_deployed_version",
                    "cut_release",
                    "promote_release",
                ]
                assert backend._deployed["prod"]["version"] == "2.2.0"
                assert backend._quality_gates["status"] == "failed"
                assert backend._quality_gates["version"] == "2.3.0"

                parent = await _operation_view(handle, "gatefail-minor-parent")
                assert parent.status == "failed"
                assert parent.child_operation_id is None
                summary = await handle.query(
                    AgenticChainWorkflow.get_workflow_status
                )
                assert summary.total_operations == 1
                assert not [
                    op
                    for op in summary.operations
                    if op.status == "waiting_for_approval"
                ]
                ledger = await handle.query(AgenticChainWorkflow.get_ledger)
                events = {entry.event for entry in ledger}
                assert "tool1_quality_gates_failed" in events
                # It got far enough to have staged the candidate, which is what
                # the gates were run against.
                assert backend._deployed["staging"]["version"] == "2.3.0"

    asyncio.run(run())


def test_case2_orchestrated_refuses_when_policy_protects_staging() -> None:
    """The pipeline carries one approval gate, at its target.

    If policy also protects the environment the pipeline promotes through, the
    pipeline refuses rather than promoting to a protected environment without an
    approval. This is the hole that a fixed internal flow could otherwise open.
    """

    @activity.defn(name="evaluate_policy")
    async def protect_everything(input: EvaluatePolicyInput) -> PolicyDecision:
        return PolicyDecision(
            requires_approval=input.tool_name == "promote_release",
            reason="every promotion requires approval",
        )

    async def run() -> None:
        async with await _environment() as env:
            async with Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[
                    AgenticChainWorkflow,
                    CutReleaseChildWorkflow,
                    PromoteReleaseChildWorkflow,
                    QualityGateChildWorkflow,
                ],
                activities=[
                    protect_everything,
                    backend_invoke_tool,
                    await_quality_gate,
                    *RELEASE_STEP_ACTIVITIES,
                ],
            ):
                _reset_backend()
                handle = await _start_chain(env, "orch-policyconflict")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    _orchestrated_request(
                        handle.id,
                        "conflict-minor",
                        bump="minor",
                        environment="prod",
                    ),
                )

                assert response.status == "failed"
                assert "pipeline" in response.reason.lower()
                # It refused before promoting anything.
                assert _tool_names() == [
                    "get_deployed_version",
                    "cut_release",
                ]
                assert backend._deployed["staging"]["version"] == "2.2.0"
                assert backend._deployed["prod"]["version"] == "2.2.0"
                ledger = await handle.query(AgenticChainWorkflow.get_ledger)
                assert any(
                    entry.event == "tool1_pipeline_policy_conflict"
                    for entry in ledger
                )

    asyncio.run(run())


# ------------------------------------- the real backend's cut idempotency


class _FakeRequest:
    """Just enough of a Starlette request for mock_tool.server.invoke."""

    def __init__(self, body: dict, headers: dict[str, str]) -> None:
        self._body = body
        self.headers = headers

    async def json(self) -> dict:
        return self._body


async def _invoke(tool_name: str, arguments: dict, key: str = "") -> dict:
    response = await backend.invoke(
        _FakeRequest(
            {"tool_name": tool_name, "arguments": arguments},
            {"Idempotency-Key": key} if key else {},
        )
    )
    return json.loads(response.body)


def test_cut_release_is_a_safe_no_op_for_an_already_cut_version() -> None:
    """Re-cutting the same version must not duplicate or re-date the release.

    This pins the deployment backend itself, not the workflow's use of it: the
    orchestrated path can reach cut_release twice for one computed version, and
    that has to be safe even when the two calls are not the same attempt and so
    are not covered by the idempotency key.
    """

    async def run() -> None:
        _reset_backend()

        first = await _invoke(
            "cut_release", {"service": SERVICE, "version": "2.4.0"}
        )
        assert first["already_cut"] is False
        assert first["release"] == "cut"
        record = backend._releases[(SERVICE, "2.4.0")]
        cut_at = record["cut_at"]
        release_count = len(backend._releases)

        # A distinct call, no idempotency key, same version.
        second = await _invoke(
            "cut_release", {"service": SERVICE, "version": "2.4.0"}
        )
        assert second["already_cut"] is True
        assert second["message"] == first["message"]
        assert len(backend._releases) == release_count
        assert backend._releases[(SERVICE, "2.4.0")]["cut_at"] == cut_at

        # A re-cut also must not lose where the release has already been.
        await _invoke(
            "promote_release",
            {"service": SERVICE, "version": "2.4.0", "environment": "staging"},
        )
        assert backend._releases[(SERVICE, "2.4.0")]["seen_in"] == ["staging"]
        await _invoke(
            "cut_release", {"service": SERVICE, "version": "2.4.0"}
        )
        assert backend._releases[(SERVICE, "2.4.0")]["seen_in"] == ["staging"]

        # And the first cut of a genuinely new version is still a fresh cut.
        third = await _invoke(
            "cut_release", {"service": SERVICE, "version": "2.5.0"}
        )
        assert third["already_cut"] is False
        assert len(backend._releases) == release_count + 1

    asyncio.run(run())


def test_repeated_idempotency_key_replays_the_first_cut_result() -> None:
    """The key path: same key, same result, effect applied once."""

    async def run() -> None:
        _reset_backend()

        first = await _invoke(
            "cut_release",
            {"service": SERVICE, "version": "2.6.0"},
            key="idem-orchestrated-cut",
        )
        assert first["idempotent_replay"] is False
        assert first["already_cut"] is False

        second = await _invoke(
            "cut_release",
            {"service": SERVICE, "version": "2.6.0"},
            key="idem-orchestrated-cut",
        )
        assert second["idempotent_replay"] is True
        assert second["executed_at"] == first["executed_at"]
        assert len([k for k in backend._releases if k[1] == "2.6.0"]) == 1

    asyncio.run(run())


def test_orchestrated_prepare_never_calls_the_canned_prepare_tool() -> None:
    """The mocked Tool1 prepare/resume pair still exists for the explicit path.

    prepare is what the orchestrated path replaces; resume is what it keeps.
    """
    prepared = backend._handle(
        "release_orchestrator_prepare",
        {"service": SERVICE, "version": "2.3.0"},
    )
    assert prepared["checkpointed"] is True
    resumed = backend._handle(
        "release_orchestrator_resume",
        {"nested_result": {"promoted": True}, "replay": True},
    )
    assert resumed["resumed"] is True
    assert resumed["nested_result_received"] is True


# Keep the module importable as a script for a quick manual run.
if __name__ == "__main__":  # pragma: no cover
    test_case2_orchestrated_controlled_prod_staging_dedup_and_bad_version()
    test_case2_orchestrated_uncontrolled_fail_closed_and_replay_safe_retry()
    print("ok")


def test_a_caller_named_version_runs_the_very_same_pipeline() -> None:
    """CASE-3's shape: the version is chosen by the caller, not by a bump.

    This is the difference between an autonomous agent that releases and one
    that merely promotes. Asked to ship a version, it has to cut that version,
    put it on staging, and get a green gate verdict before production is asked
    about anything -- otherwise the promotion moves production to an artifact
    nothing ever built, and the fleet shows staging still holding the old
    release while production is asked to jump past it.
    """

    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                await _a_named_version_is_cut_staged_and_gated(env)
                await _a_version_behind_production_is_refused_before_cutting(env)

    asyncio.run(run())


async def _a_named_version_is_cut_staged_and_gated(
    env: WorkflowEnvironment,
) -> None:
    _reset_backend()
    handle = await _start_chain(env, "named-prod")

    waiting = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _named_version_request(
            handle.id, "named-minor", version="2.3.0", environment="prod"
        ),
    )

    # Identical to the bump run above, step for step. Production is still read,
    # because a named version has to be checked against what customers are on,
    # but the number comes from the caller rather than from arithmetic.
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
    ]
    assert "release_orchestrator_prepare" not in _tool_names()
    assert _call("cut_release")[1] == {"service": SERVICE, "version": "2.3.0"}
    assert _call("promote_release")[1] == {
        "service": SERVICE,
        "version": "2.3.0",
        "environment": "staging",
    }
    assert backend._deployed["staging"]["version"] == "2.3.0"
    assert backend._quality_gates["status"] == "passed"
    assert backend._quality_gates["version"] == "2.3.0"
    # Production has NOT moved. It is the thing being asked about.
    assert backend._deployed["prod"]["version"] == "2.2.0"

    assert waiting.status == "waiting_for_approval"
    assert waiting.call_path == [
        "GoogleADKAgent",
        "release_orchestrator",
        "promote_release",
    ]
    child = await _operation_view(handle, "named-minor-child")
    assert child.arguments == {
        "service": SERVICE,
        "environment": "prod",
        "version": "2.3.0",
    }
    ledger = await handle.query(AgenticChainWorkflow.get_ledger)
    resolved = [e for e in ledger if e.event == "tool1_version_resolved"]
    assert resolved and resolved[0].detail["version_source"] == "caller"
    assert resolved[0].detail["resolved_version"] == "2.3.0"


async def _a_version_behind_production_is_refused_before_cutting(
    env: WorkflowEnvironment,
) -> None:
    _reset_backend()
    handle = await _start_chain(env, "named-backwards")

    response = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _named_version_request(
            handle.id, "named-back", version="2.1.0", environment="prod"
        ),
    )

    # Cutting it would succeed and promoting it would move production backwards.
    # Refused at the read, before anything is cut and before a child exists, so
    # nothing reaches an approver -- the same shape as an unreadable version.
    assert response.status == "failed"
    assert "2.1.0" in response.reason
    assert _tool_names() == ["get_deployed_version"]
    parent = await _operation_view(handle, "named-back-parent")
    assert parent.status == "failed"
    assert parent.child_operation_id is None
    assert backend._deployed["prod"]["version"] == "2.2.0"
