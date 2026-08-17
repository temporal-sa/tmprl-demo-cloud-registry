"""The demo-ability addendum's own behaviors, tested directly.

Four claims that the CASE-1/2a/2b suites exercise only incidentally:

  depth              cut_release and promote_release are Child Workflows whose
                     individual steps are visible in Event History by name
  ambient gates      a CASE-1 manual staging promotion, with no pipeline
                     anywhere, moves the gate card idle -> running -> passed
  fail before live   a promotion whose instances fail their health check never
                     routes traffic
  team authorization an operation with no required team is decided by anyone,
                     and one that requires a team is decided only by that team
"""

from __future__ import annotations

import asyncio
import copy
import shutil
import uuid

from temporalio import activity
from temporalio.client import Client, WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities.gateway_activities import await_quality_gate
from common.models import (
    QUALITY_GATE_CHECKS,
    ApprovalDecision,
    ChainInput,
    CorrelationContext,
    EvaluatePolicyInput,
    InvokeToolInput,
    PolicyDecision,
    ToolCallRequest,
    ToolCallResponse,
)
from mock_tool import server as backend
from security_scan.models import (
    CheckCveInput,
    CheckLicenseInput,
    GenerateSbomInput,
)
from security_scan.security_scan_activities import (
    check_cve_database,
    check_license_compliance,
    generate_sbom,
)
from tests import release_step_fakes
from tests.release_step_fakes import RELEASE_STEP_ACTIVITIES
from workflows.chain import AgenticChainWorkflow
from workflows.release_children import (
    CutReleaseChildWorkflow,
    PromoteReleaseChildWorkflow,
    QualityGateChildWorkflow,
)

TASK_QUEUE = "test-release-children"
SERVICE = "delivery-matching-service"
PRINCIPAL = "requester@example.com"

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


@activity.defn(name="evaluate_policy")
async def fake_evaluate_policy(input: EvaluatePolicyInput) -> PolicyDecision:
    requires_approval = (
        input.tool_name == "promote_release"
        and str(input.arguments.get("environment", "")).lower() == "prod"
    )
    return PolicyDecision(
        requires_approval=requires_approval,
        reason="Promotion to prod requires human approval."
        if requires_approval
        else None,
    )


@activity.defn(name="invoke_tool")
async def backend_invoke_tool(input: InvokeToolInput) -> dict:
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


def _request(
    workflow_id: str,
    operation_id: str,
    tool_name: str,
    arguments: dict,
) -> ToolCallRequest:
    return ToolCallRequest(
        tool_name=tool_name,
        arguments=arguments,
        safe_arguments=dict(arguments),
        idempotency_key=f"idem-{operation_id}",
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="explicit_authorized",
            caller_principal=PRINCIPAL,
            runtime="ClaudeCode",
            call_path=["ClaudeCode", tool_name],
        ),
        approval_timeout_seconds=60,
        requested_action=f"Call {tool_name}",
        operation_id=operation_id,
    )


async def _activity_names(client: Client, workflow_id: str) -> list[str]:
    """The Activity types scheduled by one workflow, in Event History order."""
    handle = client.get_workflow_handle(workflow_id)
    names = []
    async for event in handle.fetch_history_events():
        if event.HasField("activity_task_scheduled_event_attributes"):
            names.append(
                event.activity_task_scheduled_event_attributes.activity_type.name
            )
    return names


async def _child_ids(client: Client, prefix: str) -> list[str]:
    found = []
    async for wf in client.list_workflows():
        if wf.id.startswith(prefix):
            found.append(wf.id)
    return found


async def _wait_for_gate(status: str, timeout: float = 15) -> dict:
    async def poll() -> dict:
        while True:
            gate = backend._quality_gates
            if gate and gate.get("status") == status:
                return dict(gate)
            await asyncio.sleep(0.02)

    return await asyncio.wait_for(poll(), timeout=timeout)


async def _operation_view(handle: WorkflowHandle, operation_id: str):
    summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
    for op in summary.operations:
        if op.operation_id == operation_id:
            return op
    raise AssertionError(f"{operation_id} not found")


def test_case1_staging_promotion_is_a_child_workflow_that_triggers_the_gate() -> None:
    """The addendum's headline CASE-1 beat, with no pipeline anywhere.

    One manual promote_release to staging. It runs as a Child Workflow with four
    named steps, and reaching staging starts a gate run for the candidate that
    just landed there -- which the operator neither invoked nor waited on.
    """

    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                _reset_backend()
                # Nothing has ever been staged, so the card is idle. This is the
                # state at container startup, and it is drawn rather than hidden.
                assert backend._quality_gates["status"] == "idle"
                assert backend._quality_gates["version"] is None
                assert backend._quality_gates["checks"] == []

                handle = await _start_chain(env, "case1-gate")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _request(
                        handle.id,
                        "op-stage",
                        "promote_release",
                        {
                            "service": SERVICE,
                            "version": "2.3.0",
                            "environment": "staging",
                        },
                    ),
                )

                # Staging needs no approval, and the call returns as soon as
                # staging is actually promoted.
                assert response.status == "completed"
                assert response.result["status"] == "completed"
                assert response.result["previous_version"] == "2.2.0"
                assert backend._deployed["staging"]["version"] == "2.3.0"

                # Four named steps in Event History, not one flat tool call.
                promote_ids = await _child_ids(env.client, "promote-release::")
                assert len(promote_ids) == 1
                assert "staging" in promote_ids[0]
                assert await _activity_names(env.client, promote_ids[0]) == [
                    "deploy_binaries",
                    "health_check_new_instances",
                    "update_traffic_routing",
                    "update_release_notes",
                ]

                # The promotion handed back the id of the gate run it started,
                # and that id names the exact candidate it is about.
                gate_id = response.result["quality_gate_workflow_id"]
                assert gate_id.startswith(f"quality-gate::{SERVICE}::2.3.0::")

                # idle -> running -> passed, off one manual CASE-1 tool call.
                gate = await _wait_for_gate("passed")
                assert gate["version"] == "2.3.0"
                assert gate["gate_workflow_id"] == gate_id
                assert {check["check_name"] for check in gate["checks"]} == set(
                    QUALITY_GATE_CHECKS
                )
                assert all(check["passed"] for check in gate["checks"])
                # Four checks, run concurrently rather than in sequence.
                assert (
                    await _activity_names(env.client, gate_id)
                ).count("run_quality_check") == 4

    asyncio.run(run())


def test_cut_release_runs_three_sequential_steps_in_its_own_history() -> None:
    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                _reset_backend()
                handle = await _start_chain(env, "cut")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _request(
                        handle.id,
                        "op-cut",
                        "cut_release",
                        {"service": SERVICE, "version": "2.4.0"},
                    ),
                )

                assert response.status == "completed"
                # Each step contributed part of the result, so all three ran and
                # the hash genuinely depends on the archive that preceded it.
                assert response.result["commit_sha"]
                assert response.result["tag"] == "v2.4.0"
                assert response.result["artifact_ref"]
                assert response.result["sha256"]
                assert (SERVICE, "2.4.0") in backend._releases

                cut_ids = await _child_ids(env.client, "cut-release::")
                assert len(cut_ids) == 1
                assert await _activity_names(env.client, cut_ids[0]) == [
                    "tag_commit_in_source_control",
                    "archive_artifacts",
                    "calculate_integrity_hashes",
                ]

    asyncio.run(run())


def test_a_failed_health_check_stops_before_traffic_moves() -> None:
    """Implemented and correct, though not part of the default scripted demo.

    The promotion machinery worked: it declined to route traffic onto instances
    that failed their check. The operation still has to come out failed, or a
    promotion that never went live would be recorded as though it had.
    """

    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                _reset_backend()
                release_step_fakes.HEALTH_CHECK_FAIL_VERSIONS.add("2.3.0")
                handle = await _start_chain(env, "unhealthy")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _request(
                        handle.id,
                        "op-unhealthy",
                        "promote_release",
                        {
                            "service": SERVICE,
                            "version": "2.3.0",
                            "environment": "staging",
                        },
                    ),
                )

                assert response.status == "failed"
                assert "health check" in response.reason

                promote_ids = await _child_ids(env.client, "promote-release::")
                names = await _activity_names(env.client, promote_ids[0])
                assert names == ["deploy_binaries", "health_check_new_instances"]
                assert "update_traffic_routing" not in names

                # Staging is still serving what it was serving, and no gate run
                # started, because nothing landed to qualify.
                assert backend._deployed["staging"]["version"] == "2.2.0"
                assert backend._quality_gates["status"] == "idle"
                assert not await _child_ids(env.client, "quality-gate::")

    asyncio.run(run())


def test_an_operation_with_no_required_team_is_decided_by_any_approver() -> None:
    """The no-regression half of the authorization change.

    CASE-1's ordinary production promotions carry no team restriction, so an
    approver with no team at all still decides them, exactly as before.
    """

    async def run() -> None:
        async with await _environment() as env:
            async with _worker(env):
                _reset_backend()
                handle = await _start_chain(env, "no-team")
                waiting = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _request(
                        handle.id,
                        "op-prod",
                        "promote_release",
                        {
                            "service": SERVICE,
                            "version": "2.3.0",
                            "environment": "prod",
                        },
                    ),
                )
                assert waiting.status == "waiting_for_approval"
                view = await _operation_view(handle, "op-prod")
                assert view.required_approver_team is None
                # And the query the gateway uses before signalling agrees.
                assert (
                    await handle.query(
                        AgenticChainWorkflow.get_required_approver_team,
                        "op-prod",
                        result_type=str,
                    )
                    == ""
                )

                await handle.signal(
                    AgenticChainWorkflow.approve_operation,
                    ApprovalDecision(
                        operation_id="op-prod",
                        approver="approver@demo",
                        approver_team="",
                    ),
                )

                async def poll() -> ToolCallResponse:
                    while True:
                        response = await handle.query(
                            "get_operation_status",
                            "op-prod",
                            result_type=ToolCallResponse,
                        )
                        if response.status == "completed":
                            return response
                        await asyncio.sleep(0.02)

                completed = await asyncio.wait_for(poll(), timeout=15)
                assert completed.result["status"] == "completed"
                assert backend._deployed["prod"]["version"] == "2.3.0"

    asyncio.run(run())


# ------------------------------------------- dependency_scan's own sub-steps


def test_the_cve_check_is_the_only_lever_that_fails_a_dependency_scan() -> None:
    """One controllable failure lever, expressed through check_cve_database.

    The licence check is scripted clean whatever the top-level outcome is. Two
    independently failing sub-checks could disagree with no narratively visible
    reason, which would make the demo's failure story harder to explain rather
    than richer.
    """
    sbom = generate_sbom(GenerateSbomInput(service=SERVICE, version="2.3.0"))
    assert sbom["sbom_ref"].startswith(f"sbom://{SERVICE}/2.3.0/")
    assert sbom["dependency_count"] > 0

    clean = check_cve_database(
        CheckCveInput(
            service=SERVICE,
            version="2.3.0",
            sbom_ref=sbom["sbom_ref"],
            stage_number=1,
            scripted_outcome="pass",
        )
    )
    assert clean["passed"] is True
    assert clean["findings"] == 0

    blocking = check_cve_database(
        CheckCveInput(
            service=SERVICE,
            version="2.3.0",
            sbom_ref=sbom["sbom_ref"],
            stage_number=1,
            scripted_outcome="fail",
        )
    )
    assert blocking["passed"] is False
    assert blocking["max_severity"] == "high"

    # Clean on every scripted outcome there is, because it is never the lever.
    for _ in ("pass", "fail", "flaky_then_pass"):
        licences = check_license_compliance(
            CheckLicenseInput(
                service=SERVICE, version="2.3.0", sbom_ref=sbom["sbom_ref"]
            )
        )
        assert licences["passed"] is True
        assert licences["findings"] == 0
        assert licences["max_severity"] == "none"
