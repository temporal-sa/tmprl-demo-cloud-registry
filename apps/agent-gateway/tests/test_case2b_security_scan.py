"""CASE-2b: the pre-prod security scan as a genuinely separate, durable Tool1.

Everything here runs against two Temporal namespaces and two Worker processes,
because that is the claim under test. A single-namespace version of this suite
would pass while proving nothing: the point is not that a scan workflow exists,
it is that it exists somewhere Agent Gateway does not own, suspends its own
execution across an approval it does not control, and is woken by a Signal that
crosses the boundary.

What each test pins down:

  the handoff       the pipeline parks itself and starts a workflow in the other
                    namespace, rather than opening the production promotion
  the nested call   the scan calls back into the SAME chain workflow_id, so one
                    user-visible task spans two systems
  the suspension    the scan stops at waiting_for_approval and its state survives
                    a worker being torn down and replaced underneath it
  the wake          approval in the gateway signals the scan back across the
                    namespace boundary, and both sides end up consistent
  fail closed       a scan that fails never calls the gateway at all, so the
                    promotion is never put in front of a human
  before 2b         with no scan provider registered, the pipeline behaves
                    exactly as CASE-2a did
  mandate off       with the mandate off there is no scan step in the flow at
                    all, even with a provider up and advertising
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import logging
import os
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone

from temporalio import activity, workflow
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest
from temporalio.client import Client, WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from activities.gateway_activities import (
    await_quality_gate,
    signal_operation_callback,
    submit_nested_tool_call,
)
from common.models import (
    SCAN_MODE_LEGACY,
    SCAN_MODE_PLATFORM,
    ApprovalDecision,
    ChainInput,
    CorrelationContext,
    EvaluatePolicyInput,
    GatewayOperationResolution,
    InvokeToolInput,
    NestedToolCallRequest,
    Operation,
    OperationStatus,
    PolicyDecision,
    ToolCallRequest,
)
from mock_tool import server as backend
import security_scan.models as security_models
from security_scan.security_scan_activities import (
    check_cve_database,
    check_license_compliance,
    generate_sbom,
    run_scan_check,
)
from security_scan.security_scan_workflow import SecurityScanWorkflow
from security_scan.models import PublishScanStateInput, ScanResult
from security_scan.nexus_contracts import (
    AGENT_GATEWAY_ENDPOINT,
    SECURITY_ENDPOINT,
)
from security_scan.nexus_handlers import SecurityScanServiceHandler
from workflows.nexus_handlers import AgentGatewayServiceHandler
from workflows.protected_action import ProtectedActionWorkflow
from tests import release_step_fakes
from tests.release_step_fakes import RELEASE_STEP_ACTIVITIES
from workflows.chain import (
    SCAN_REPORT_DEADLINE_SECONDS,
    AgenticChainWorkflow,
)
import workflows.chain as chain_module
from workflows.release_children import (
    CutReleaseChildWorkflow,
    PromoteReleaseChildWorkflow,
    QualityGateChildWorkflow,
)

GATEWAY_TASK_QUEUE = "test-agent-gateway-2b"
SECURITY_TASK_QUEUE = "test-security-tq"
SECURITY_NAMESPACE = "security"
SERVICE = "delivery-matching-service"
PRINCIPAL = "requester@example.com"

# A one-second, two-stage scan. The real one is four stages five seconds apart,
# which is right for a live demo and wrong for a test suite. Patched onto the
# Security team's own module, because that is where the scan's shape lives now:
# the caller cannot set it, so a test cannot either without standing where the
# Security team stands.
TEST_CHECK_SECONDS = 1
TEST_STAGE_COUNT = 2

# Shared with the release-step fakes, so one list shows the whole sequence
# whether a step ran as an Activity or inside a release Child Workflow.
INVOKE_CALLS = release_step_fakes.TOOL_CALLS
PUBLISHED: list[PublishScanStateInput] = []


@workflow.defn(sandboxed=False)
class OperationCallbackReceiverWorkflow:
    """Stand-in for the outer autonomous workflow waiting on its chain."""

    def __init__(self) -> None:
        self._resolution: GatewayOperationResolution | None = None

    @workflow.run
    async def run(self) -> GatewayOperationResolution:
        await workflow.wait_condition(lambda: self._resolution is not None)
        assert self._resolution is not None
        return self._resolution

    @workflow.signal(name="operation_resolved")
    def operation_resolved(self, resolution: GatewayOperationResolution) -> None:
        if self._resolution is None:
            self._resolution = resolution

    @workflow.query
    def get_resolution(self) -> GatewayOperationResolution | None:
        return self._resolution

_PRISTINE_DEPLOYED = copy.deepcopy(backend._deployed)
_PRISTINE_RELEASES = copy.deepcopy(backend._releases)
_IDLE_GATES = copy.deepcopy(backend._quality_gates)


def _reset_backend(*, scan_available: bool, fail_versions: set[str] | None = None):
    release_step_fakes.reset()
    PUBLISHED.clear()
    backend._deployed.clear()
    backend._deployed.update(copy.deepcopy(_PRISTINE_DEPLOYED))
    backend._releases.clear()
    backend._releases.update(copy.deepcopy(_PRISTINE_RELEASES))
    backend._seen.clear()
    backend._quality_gates = copy.deepcopy(_IDLE_GATES)
    backend._scan = None
    # The scan's shape and its verdict rule both belong to the Security team, so
    # both are set on their module rather than passed in by anyone.
    security_models.SCAN_CHECK_SECONDS = TEST_CHECK_SECONDS
    security_models.SCAN_CHECK_COUNT = TEST_STAGE_COUNT
    security_models.SECURITY_TASK_QUEUE = SECURITY_TASK_QUEUE
    os.environ["SCAN_FAIL_VERSIONS"] = ",".join(sorted(fail_versions or set()))
    # A registered provider means the Security team's platform is answering. It no
    # longer decides WHETHER a scan happens -- the mandate and the scanner switch
    # do that, and they arrive on the request -- so an empty registry is not
    # "there is no scan step" any more. It is a required scanner that cannot be
    # reached, which stops the release. See
    # test_an_unreachable_scanner_stops_the_release_rather_than_skipping_it.
    #
    # Note what the registration no longer carries: no namespace, no task queue,
    # no workflow type. Just who, and the endpoint to reach them on.
    backend._scan_provider = (
        {
            "provider": "security",
            "endpoint": SECURITY_ENDPOINT,
            "ttl_seconds": 300.0,
            "last_seen": time.time(),
        }
        if scan_available
        else None
    )


def _tool_names() -> list[str]:
    return [name for name, _, _ in INVOKE_CALLS]


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


@activity.defn(name="publish_scan_state")
async def fake_publish_scan_state(input: PublishScanStateInput) -> dict:
    """Stands in for the HTTP post to the observability backend.

    Recorded rather than dropped, because the dashboard card's phases come from
    here and a phase the workflow never publishes is a card that never updates.
    """
    PUBLISHED.append(input)
    backend._handle(
        "record_scan_state",
        {
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
        },
    )
    return {"recorded": True}


async def _environment() -> WorkflowEnvironment:
    """A dev server with both namespaces and both Nexus Endpoints registered.

    The endpoints are the whole topology now. Each names a target namespace and
    task queue, and neither team's code contains either: that indirection is
    what the design buys, so the test has to set it up the same way production
    does rather than by patching constants into the callers.
    """
    temporal = shutil.which("temporal")
    assert temporal, "Temporal CLI is required for workflow tests"
    env = await WorkflowEnvironment.start_local(
        dev_server_existing_path=temporal,
        dev_server_log_level="error",
        dev_server_extra_args=["--namespace", SECURITY_NAMESPACE],
    )
    # The gateway's own Activities reach Temporal through activity.client(), so
    # they follow their Worker onto this ephemeral server automatically.

    await _create_endpoint(
        env, AGENT_GATEWAY_ENDPOINT, env.client.namespace, GATEWAY_TASK_QUEUE
    )
    await _create_endpoint(
        env, SECURITY_ENDPOINT, SECURITY_NAMESPACE, SECURITY_TASK_QUEUE
    )
    return env


async def _create_endpoint(
    env: WorkflowEnvironment, name: str, namespace: str, task_queue: str
) -> None:
    await env.client.operator_service.create_nexus_endpoint(
        CreateNexusEndpointRequest(
            spec=EndpointSpec(
                name=name,
                target=EndpointTarget(
                    worker=EndpointTarget.Worker(
                        namespace=namespace, task_queue=task_queue
                    )
                ),
            )
        )
    )


def _gateway_worker(env: WorkflowEnvironment) -> Worker:
    return Worker(
        env.client,
        task_queue=GATEWAY_TASK_QUEUE,
        workflows=[
            AgenticChainWorkflow,
            ProtectedActionWorkflow,
            OperationCallbackReceiverWorkflow,
            # The Waypoint team's own release steps. Same namespace and task
            # queue as the chain that starts them, which is the deliberate
            # contrast with SecurityScanWorkflow below.
            CutReleaseChildWorkflow,
            PromoteReleaseChildWorkflow,
            QualityGateChildWorkflow,
        ],
        activities=[
            fake_evaluate_policy,
            backend_invoke_tool,
            submit_nested_tool_call,
            signal_operation_callback,
            await_quality_gate,
            *RELEASE_STEP_ACTIVITIES,
        ],
        nexus_service_handlers=[AgentGatewayServiceHandler()],
    )


def _scan_worker(client: Client) -> Worker:
    """The Security team's worker. Different client, namespace, and task queue."""
    return Worker(
        client,
        task_queue=SECURITY_TASK_QUEUE,
        workflows=[SecurityScanWorkflow],
        activities=[
            run_scan_check,
            fake_publish_scan_state,
            # dependency_scan is three sub-steps of its own now.
            generate_sbom,
            check_cve_database,
            check_license_compliance,
        ],
        nexus_service_handlers=[SecurityScanServiceHandler()],
        # run_scan_check is synchronous, exactly as it is in the real worker.
        activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=8),
    )


async def _scan_client(env: WorkflowEnvironment) -> Client:
    return await Client.connect(
        env.client.service_client.config.target_host, namespace=SECURITY_NAMESPACE
    )


async def _start_chain(env: WorkflowEnvironment, name: str) -> WorkflowHandle:
    workflow_id = f"wf-{name}-{uuid.uuid4().hex[:8]}"
    return await env.client.start_workflow(
        AgenticChainWorkflow.run,
        ChainInput(workflow_id=workflow_id, owner_principal=PRINCIPAL),
        id=workflow_id,
        task_queue=GATEWAY_TASK_QUEUE,
    )


def _pipeline_request(
    workflow_id: str,
    key: str,
    *,
    security_mandate: bool = True,
    scan_mode: str = SCAN_MODE_PLATFORM,
) -> NestedToolCallRequest:
    """A CASE-2 pipeline run, made under the security mandate by default.

    Two switches have to be set for a scan to be in the path, and both default to
    the position that puts one there:

      security_mandate -- the rule that a production release is scanned at all.
      scan_mode        -- which of the Security team's two scanners runs it. Only
                          `platform` is something the gateway can hand off to; in
                          `legacy` mode a human runs their host script and the
                          script asks for the promotion itself, so the gateway
                          inserts no step.

    Tests asserting an absence turn one of the two off and say which.
    """
    tool1_arguments = {"service": SERVICE, "bump": "minor", "environment": "prod"}
    tool2_arguments = {"service": SERVICE, "environment": "prod"}
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
        requested_action=f"Promote the next minor version of {SERVICE} to prod",
        parent_operation_id=f"{key}-parent",
        nested_operation_id=f"{key}-child",
        bump="minor",
        controlled_tool1=True,
        security_mandate=security_mandate,
        scan_mode=scan_mode,
    )


async def _operation_view(handle: WorkflowHandle, operation_id: str):
    summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
    for op in summary.operations:
        if op.operation_id == operation_id:
            return op
    raise AssertionError(f"{operation_id} not found in workflow status")


async def _await_condition(check, timeout: float = 20.0):
    async def poll():
        while True:
            value = await check()
            if value:
                return value
            await asyncio.sleep(0.05)

    return await asyncio.wait_for(poll(), timeout=timeout)


async def _waiting_promotion(handle: WorkflowHandle):
    """The production promotion the scan asked for, once it reaches the queue."""

    async def check():
        summary = await handle.query(AgenticChainWorkflow.get_workflow_status)
        for op in summary.operations:
            if (
                op.tool_name == "promote_release"
                and op.status == "waiting_for_approval"
            ):
                return op
        return None

    return await _await_condition(check)


async def _ledger_events(handle: WorkflowHandle) -> list[str]:
    ledger = await handle.query(AgenticChainWorkflow.get_ledger)
    return [entry.event for entry in ledger]


# --------------------------------------------------------------------- tests


def test_the_scan_passes_suspends_and_resumes_across_two_namespaces() -> None:
    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            scan_client = await _scan_client(env)
            async with _gateway_worker(env):
                # The Security team's worker runs the scan and then goes away
                # entirely, mid-pause. A replacement comes up afterwards. Two
                # separate `async with` blocks, not one nested inside the other,
                # because a worker that is still running has not been killed.
                async with _scan_worker(scan_client):
                    handle, promotion, status = await _scan_runs_and_suspends(
                        env, scan_client
                    )
                await _resumes_on_a_fresh_worker(
                    handle, scan_client, promotion, status
                )

    asyncio.run(run())


async def _scan_runs_and_suspends(
    env: WorkflowEnvironment, scan_client: Client
):
    handle = await _start_chain(env, "scan-pass")
    response = await handle.execute_update(
        AgenticChainWorkflow.request_nested_tool_call,
        _pipeline_request(handle.id, "scan-pass"),
    )

    # The pipeline ran its own work and then stopped, because the next
    # checkpoint is not its own work. Nothing has been put in front of an
    # approver yet: at this point production is not even being asked about.
    assert response.status == "processing"
    assert _tool_names() == [
        "get_deployed_version",
        "cut_release",
        "promote_release",
        "get_security_scan_status",
    ]
    parent = await _operation_view(handle, "scan-pass-parent")
    assert parent.status == "waiting_for_dependency"
    assert parent.checkpoint["stage"] == "awaiting_security_scan"
    assert parent.checkpoint["scan_provider"] == "security"
    # An endpoint, not a namespace. The pipeline recorded who it handed to and
    # how to reach them, and that is the entire extent of what it knows.
    assert parent.checkpoint["scan_endpoint"] == SECURITY_ENDPOINT
    # The park is bounded. A scan that started and then said nothing forever
    # would otherwise hold this operation open for the life of the chain.
    parked_at = datetime.fromisoformat(parent.created_iso).timestamp()
    assert (
        parent.checkpoint["scan_deadline_epoch"] - parked_at
        <= SCAN_REPORT_DEADLINE_SECONDS + 60
    )
    assert "scan_namespace" not in parent.checkpoint
    assert "scan_task_queue" not in parent.checkpoint
    scan_workflow_id = parent.checkpoint["scan_workflow_id"]

    # A workflow really did start in the OTHER namespace, on the other team's
    # task queue, under an id that looks nothing like anything Agent Gateway
    # owns -- none of which the gateway chose or could see.
    assert scan_workflow_id.startswith("security::scan::")
    scan = scan_client.get_workflow_handle(scan_workflow_id)
    describe = await scan.describe()
    assert describe.workflow_type == "SecurityScanWorkflow"
    assert describe.task_queue == SECURITY_TASK_QUEUE

    # The scan runs its stages on its own, then makes the nested call. The
    # promotion that lands in the approval queue was requested by the scan, not by
    # the pipeline, and it landed on the SAME chain the operator started.
    promotion = await _waiting_promotion(handle)
    assert promotion.arguments == {
        "service": SERVICE,
        "version": "2.3.0",
        "environment": "prod",
    }
    assert promotion.call_path == [
        "security",
        "security_scan",
        "promote_release",
    ]
    assert "security scan cleared" in (promotion.justification or "")

    # The scan is now genuinely suspended: its own workflow, in its own namespace,
    # holding its own checkpoint, waiting on a decision nobody has made yet.
    status = await _await_condition(
        lambda: scan.query(SecurityScanWorkflow.get_status)
    )
    assert status.phase == "awaiting_prod_approval"
    assert status.verdict == "pass"
    assert status.stages_completed == TEST_STAGE_COUNT
    assert status.gateway_workflow_id == handle.id
    # What the scan holds while it waits is a Nexus operation token, not Agent
    # Gateway's internal operation id. It is suspended on an operation it owns a
    # handle to, rather than polling somebody else's record by id, and the
    # gateway's id only reaches it with the outcome. That is the boundary doing
    # its job: the scan correlates on the thing that is actually its own.
    assert status.gateway_operation_token
    assert status.gateway_operation_id is None
    assert backend._deployed["prod"]["version"] == "2.2.0"
    return handle, promotion, status


async def _resumes_on_a_fresh_worker(
    handle: WorkflowHandle, scan_client: Client, promotion, status
) -> None:
    """The scan worker is gone. Bring up a new one and check what survived.

    This is the property the whole case exists for: the stage results, the
    verdict, and the operation id all come back out of Event History, no stage is
    re-run, and the wait picks up where it was. If the pause lived inside Agent
    Gateway's workflow instead of this one, there would be nothing here to
    survive -- the gateway would simply be resuming its own work.
    """
    scan = scan_client.get_workflow_handle(
        status.scan_workflow_id, result_type=ScanResult
    )
    ticks_before = len([c for c in PUBLISHED if c.phase == "running_scan"])

    async with _scan_worker(scan_client):
        replayed = await scan.query(SecurityScanWorkflow.get_status)
        assert replayed.phase == "awaiting_prod_approval"
        assert replayed.stages_completed == TEST_STAGE_COUNT
        assert replayed.stage_results == status.stage_results
        assert (
            len([c for c in PUBLISHED if c.phase == "running_scan"])
            == ticks_before
        ), "replay must not re-run scan stages"

        # This promotion was requested by the Security team's scan, so it may
        # only be decided by the Security team. An approver from anywhere else
        # is refused outright rather than quietly ignored: the operation stays
        # waiting, the ledger records the attempt, and nothing is promoted.
        #
        # That requirement is what makes the scan the only correct caller here.
        # If any approver would do, the Waypoint pipeline could just as well have
        # waited for the verdict and requested the promotion itself; because a
        # Security-team member has to sign off, the requester identity on the
        # operation has to genuinely be theirs.
        await handle.signal(
            AgenticChainWorkflow.approve_operation,
            ApprovalDecision(
                operation_id=promotion.operation_id,
                approver="approver@example.com",
                approver_team="waypoint",
            ),
        )
        await asyncio.sleep(0.5)
        still_waiting = await _operation_view(handle, promotion.operation_id)
        assert still_waiting.status == "waiting_for_approval"
        assert still_waiting.required_approver_team == "security"
        assert "approval_refused_wrong_team" in await _ledger_events(handle)
        assert backend._deployed["prod"]["version"] == "2.2.0"

        # The Security team's own approver, and now it lands.
        await handle.signal(
            AgenticChainWorkflow.approve_operation,
            ApprovalDecision(
                operation_id=promotion.operation_id,
                approver="Abe Roover <abe.roover@quickmeals.com>",
                approver_team="security",
            ),
        )

        # The gateway promotes, then signals across the namespace boundary, and
        # the scan wakes and finishes. Its result carries the promotion it was
        # waiting for, which it never saw happen and never performed itself.
        result = await asyncio.wait_for(scan.result(), timeout=30)
        assert result.phase == "completed"
        assert result.verdict == "pass"
        assert result.promotion_result["status"] == "completed"
        assert result.promotion_result["environment"] == "prod"
        assert backend._deployed["prod"]["version"] == "2.3.0"

    # And the pipeline operation that parked itself is settled, so the chain the
    # operator is watching ends up complete rather than waiting forever on a
    # handoff that already resolved.
    settled = await _await_condition(lambda: _settled(handle, "scan-pass-parent"))
    assert settled.status == "completed"
    assert settled.checkpoint["stage"] == "security_scan_completed"

    events = await _ledger_events(handle)
    for expected in (
        "scan_capability_discovered",
        "security_scan_handoff",
        "controlled_caller_notified",
        "security_scan_pipeline_settled",
    ):
        assert expected in events, f"{expected} missing from {events}"


def test_rejecting_the_promotion_wakes_the_scan_with_the_bad_news() -> None:
    """The pause resolves the other way, and both systems agree about it.

    Worth its own test because it takes a different branch on both sides: the
    gateway settles the parked pipeline operation from the rejection rather than
    from a result, and the scan wakes into its rejected phase instead of recording
    a promotion. A suspended workflow that only ever gets woken by approvals is a
    workflow that hangs the first time somebody says no.
    """

    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            scan_client = await _scan_client(env)
            async with _gateway_worker(env), _scan_worker(scan_client):
                handle = await _start_chain(env, "scan-reject")
                await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    _pipeline_request(handle.id, "scan-reject"),
                )
                promotion = await _waiting_promotion(handle)

                # Rejecting is a decision too, and it disposes of the operation
                # just as finally as approving does, so the same team
                # restriction applies to it.
                await handle.signal(
                    AgenticChainWorkflow.reject_operation,
                    ApprovalDecision(
                        operation_id=promotion.operation_id,
                        approver="Abe Roover <abe.roover@quickmeals.com>",
                        approver_team="security",
                        reason="holiday change freeze",
                    ),
                )

                parent = await _operation_view(handle, "scan-reject-parent")
                scan = scan_client.get_workflow_handle(
                    parent.checkpoint["scan_workflow_id"],
                    result_type=ScanResult,
                )
                result = await asyncio.wait_for(scan.result(), timeout=30)
                assert result.phase == "rejected"
                # The scan was clean. The release still does not ship, and the
                # scan knows why rather than timing out on a promotion that
                # silently never happened.
                assert result.verdict == "pass"
                assert result.error == "holiday change freeze"
                assert result.promotion_result is None
                assert backend._deployed["prod"]["version"] == "2.2.0"

                settled = await _await_condition(
                    lambda: _settled(handle, "scan-reject-parent")
                )
                assert settled.status == "rejected"
                assert "controlled_caller_notified" in await _ledger_events(handle)

    asyncio.run(run())


async def _settled(handle: WorkflowHandle, operation_id: str):
    op = await _operation_view(handle, operation_id)
    return op if op.status != "waiting_for_dependency" else None


def test_a_failing_scan_never_reaches_the_approver() -> None:
    """A red window stops the release without asking anyone.

    Same fail-closed shape the quality gate already has, one checkpoint later:
    the gateway is never called, so there is no operation to approve, and the
    parked pipeline operation is failed by the verdict rather than left hanging.
    """

    async def run() -> None:
        _reset_backend(scan_available=True, fail_versions={"2.3.0"})
        async with await _environment() as env:
            scan_client = await _scan_client(env)
            async with _gateway_worker(env), _scan_worker(scan_client):
                handle = await _start_chain(env, "scan-fail")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    _pipeline_request(handle.id, "scan-fail"),
                )
                assert response.status == "processing"

                parent = await _await_condition(
                    lambda: _failed_parent(handle, "scan-fail-parent")
                )
                assert parent.status == "failed"
                assert "The pre-prod security scan did not pass" in (
                    parent.decision_reason or ""
                )
                assert parent.checkpoint["stage"] == "security_scan_failed"

                # Nothing was ever asked of a human, and production is untouched.
                summary = await handle.query(
                    AgenticChainWorkflow.get_workflow_status
                )
                assert not [
                    op
                    for op in summary.operations
                    if op.status == "waiting_for_approval"
                ]
                assert not [
                    op
                    for op in summary.operations
                    if op.tool_name == "promote_release"
                    and op.arguments.get("environment") == "prod"
                ]
                assert backend._deployed["prod"]["version"] == "2.2.0"
                assert "security_scan_failed" in await _ledger_events(handle)

                scan = scan_client.get_workflow_handle(
                    parent.checkpoint["scan_workflow_id"],
                    result_type=ScanResult,
                )
                result = await asyncio.wait_for(scan.result(), timeout=30)
                assert result.phase == "scan_failed"
                assert result.verdict == "fail"
                assert result.gateway_operation_id is None

    asyncio.run(run())


async def _failed_parent(handle: WorkflowHandle, operation_id: str):
    parent = await _operation_view(handle, operation_id)
    return parent if parent.status == "failed" else None


def test_an_unreachable_scanner_stops_the_release_rather_than_skipping_it() -> None:
    """The regression this file exists to prevent from coming back.

    The pipeline used to treat "nobody is offering a scan" as "then there is no
    scan step", and promote to production anyway. That made a company-wide security
    mandate evaporate whenever the Security team's worker happened to be down, and
    -- worse -- it reported success while doing it, so the only way to discover the
    checkpoint had been skipped was to go looking in the Temporal UI for a workflow
    that was never started.

    The mandate is on and the scanner switch is on Temporal, so a scan is required.
    Their platform is not answering. The release fails, the failure says which two
    things could fix it, and nothing is put in front of an approver -- because
    approving a production promotion that skipped a mandated checkpoint is exactly
    what the checkpoint exists to prevent.
    """

    async def run() -> None:
        _reset_backend(scan_available=False)
        async with await _environment() as env:
            async with _gateway_worker(env):
                handle = await _start_chain(env, "no-scan")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    _pipeline_request(handle.id, "no-scan"),
                )

                # Not waiting_for_approval. Nobody is asked.
                assert response.status == "failed"
                parent = await _operation_view(handle, "no-scan-parent")
                assert parent.status == "failed"
                assert "not reachable" in (parent.decision_reason or "")
                # Both fixes are named, because they are different fixes.
                assert "worker" in (parent.decision_reason or "")
                assert "legacy script" in (parent.decision_reason or "")
                # It did ask, and the answer stopped the release.
                assert "get_security_scan_status" in _tool_names()
                assert backend._scan is None
                events = await _ledger_events(handle)
                assert "security_scan_unavailable" in events
                assert "security_scan_handoff" not in events
                # And production was never touched.
                assert backend._deployed["prod"]["version"] == "2.2.0"

    asyncio.run(run())


def _direct_promotion_request(
    workflow_id: str,
    key: str,
    *,
    version: str = "2.3.0",
    environment: str = "prod",
    caller_service: str | None = None,
    security_mandate: bool = True,
    scan_mode: str = SCAN_MODE_PLATFORM,
    callback_workflow_id: str = "",
    runtime: str = "ClaudeCode",
) -> ToolCallRequest:
    """A single promote_release call, the way an agent makes one by hand.

    No pipeline, no bump, no orchestrator: this is what arrives when an agent
    decides to call promote_release itself rather than run_release_orchestration.
    It is the shape the mandate used to be blind to.
    """
    arguments = {
        "service": SERVICE,
        "version": version,
        "environment": environment,
    }
    return ToolCallRequest(
        tool_name="promote_release",
        arguments=arguments,
        safe_arguments=dict(arguments),
        idempotency_key=key,
        operation_id=f"{key}-op",
        correlation=CorrelationContext(
            workflow_id=workflow_id,
            workflow_id_source="explicit_authorized",
            caller_principal=PRINCIPAL,
            caller_service=caller_service,
            runtime=runtime,
            call_path=[runtime, "promote_release"],
        ),
        requested_action=f"Promote {SERVICE} {version} to {environment}",
        security_mandate=security_mandate,
        scan_mode=scan_mode,
        callback_workflow_id=callback_workflow_id,
    )


def test_a_hand_rolled_promotion_cannot_step_around_the_mandate() -> None:
    """The bug this whole change exists to fix.

    The scan used to live inside the release pipeline, which meant an agent that
    skipped the pipeline skipped the scan. Ask for a production promotion with
    promote_release directly -- no orchestrator, no bump -- and the release went
    from green quality gates straight to the approval queue with no scan started,
    no scan workflow in the Temporal UI, and nothing anywhere saying a checkpoint
    had been missed. The mandate held only on the path the agent was supposed to
    take, which is not what a mandate is.

    Now the check is at the single-tool boundary. The promotion is parked on the
    Security team's scan exactly as a pipeline run would be, and it is the scan --
    not this call -- that eventually asks a human.
    """

    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            scan_client = await _scan_client(env)
            async with _gateway_worker(env):
                async with _scan_worker(scan_client):
                    handle = await _start_chain(env, "direct-promote")
                    response = await handle.execute_update(
                        AgenticChainWorkflow.request_tool_call,
                        _direct_promotion_request(handle.id, "direct-promote"),
                    )

                    # Not waiting_for_approval. Nobody is asked to approve a
                    # promotion that has not been scanned yet.
                    assert response.status == "processing"
                    op = await _operation_view(handle, "direct-promote-op")
                    assert op.status == "waiting_for_dependency"
                    assert op.checkpoint["stage"] == "awaiting_security_scan"
                    assert op.checkpoint["scan_provider"] == "security"
                    # An endpoint name, not a namespace or a task queue.
                    assert op.checkpoint["scan_endpoint"] == SECURITY_ENDPOINT
                    assert op.checkpoint["scan_workflow_id"]
                    events = await _ledger_events(handle)
                    assert "security_scan_handoff" in events
                    # And a scan really did start, in the other namespace.
                    scan = scan_client.get_workflow_handle(
                        op.checkpoint["scan_workflow_id"]
                    )
                    assert await scan.query("get_status") is not None

    asyncio.run(run())


def test_an_adk_companion_chain_notifies_its_outer_workflow_after_the_scan() -> None:
    """The ADK shell delegates to the same governed operation as Claude Code.

    The callback belongs to the original single-tool operation. It must remain
    unresolved while the mandated scan is running and while the scan's production
    promotion awaits Security approval, then receive the origin operation's final
    result after that nested promotion completes. This is the seam that lets the
    autonomous workflow run its dependent follow-up without owning a second copy
    of gateway policy.
    """

    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            scan_client = await _scan_client(env)
            async with _gateway_worker(env), _scan_worker(scan_client):
                outer_workflow_id = f"wf-adk-shell-{uuid.uuid4().hex[:8]}"
                callback = await env.client.start_workflow(
                    OperationCallbackReceiverWorkflow.run,
                    id=outer_workflow_id,
                    task_queue=GATEWAY_TASK_QUEUE,
                )
                companion_workflow_id = f"{outer_workflow_id}:gateway"
                chain = await env.client.start_workflow(
                    AgenticChainWorkflow.run,
                    ChainInput(
                        workflow_id=companion_workflow_id,
                        owner_principal=PRINCIPAL,
                    ),
                    id=companion_workflow_id,
                    task_queue=GATEWAY_TASK_QUEUE,
                )
                assert chain.id == f"{outer_workflow_id}:gateway"
                response = await chain.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _direct_promotion_request(
                        chain.id,
                        "adk-companion",
                        caller_service="google-adk-agent",
                        callback_workflow_id=outer_workflow_id,
                        runtime="GoogleADKAgent",
                    ),
                )

                assert response.status == "processing"
                assert (
                    await callback.query(
                        OperationCallbackReceiverWorkflow.get_resolution
                    )
                    is None
                )

                promotion = await _waiting_promotion(chain)
                assert promotion.required_approver_team == "security"
                assert (
                    await callback.query(
                        OperationCallbackReceiverWorkflow.get_resolution
                    )
                    is None
                )

                await chain.signal(
                    AgenticChainWorkflow.approve_operation,
                    ApprovalDecision(
                        operation_id=promotion.operation_id,
                        approver="Abe Roover <abe.roover@quickmeals.com>",
                        approver_team="security",
                    ),
                )

                resolution = await asyncio.wait_for(callback.result(), timeout=30)
                assert resolution.operation_id == "adk-companion-op"
                assert resolution.status == "completed"
                assert (
                    resolution.result["scan_result"]["tool2_result"]["environment"]
                    == "prod"
                )
                assert backend._deployed["prod"]["version"] == "2.3.0"

    asyncio.run(run())


def test_a_scanners_own_promotion_is_not_handed_back_to_be_scanned() -> None:
    """The exemption, without which platform mode would never terminate.

    The Security team's scan clears a release and then asks Agent Gateway for the
    production promotion, because promoting is a protected action and they have no
    more right to do it unsupervised than anyone else. If that request were itself
    subject to the mandate's scan insertion, the gateway would hand the Security
    team's promotion back to the Security team to scan again, and again.

    caller_service is what breaks the loop, and it is resolved from the caller's
    bearer token rather than read off the request -- "I am a scanner, do not scan
    me" is not a claim a caller gets to make about itself. Both of their scanners
    identify this way: the workflow states it over Nexus, the legacy host script
    gets it from its own token.
    """

    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            async with _gateway_worker(env):
                handle = await _start_chain(env, "scanner-promote")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _direct_promotion_request(
                        handle.id, "scanner-promote", caller_service="security"
                    ),
                )

                # Straight to a human, with no scan inserted: this request IS the
                # output of a scan.
                assert response.status == "waiting_for_approval"
                op = await _operation_view(handle, "scanner-promote-op")
                assert op.status == "waiting_for_approval"
                assert "scan_workflow_id" not in op.checkpoint
                assert "get_security_scan_status" not in _tool_names()
                # Still the Security team's to approve, on both grounds.
                assert op.required_approver_team == "security"

    asyncio.run(run())


def test_a_hand_rolled_staging_promotion_is_untouched() -> None:
    """The mandate covers production, and only production.

    Worth pinning because the new check runs on every single-tool call now. A
    staging promotion is not a mandate-covered action, was never scanned, and must
    stay exactly as cheap as it was -- the pipeline promotes through staging on its
    way to production and would deadlock on a scan that had no business being
    there.
    """

    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            async with _gateway_worker(env):
                handle = await _start_chain(env, "staging-promote")
                await handle.execute_update(
                    AgenticChainWorkflow.request_tool_call,
                    _direct_promotion_request(
                        handle.id, "staging-promote", environment="staging"
                    ),
                )

                assert "get_security_scan_status" not in _tool_names()
                events = await _ledger_events(handle)
                assert "security_scan_handoff" not in events

    asyncio.run(run())


def test_in_legacy_mode_the_gateway_inserts_no_scan_step() -> None:
    """Act One of the walkthrough, as a contract rather than a container state.

    The mandate is on, and the Security team's Temporal platform is up and
    advertising -- it always is now, because it starts with everything else. What
    decides the shape of the run is the scanner switch, and it is on `legacy`:
    their scanner is a script somebody runs by hand, so there is nothing for the
    gateway to hand off to and it inserts no step. The capability lookup does not
    even happen.

    This is what used to be expressed by not starting a container, which meant the
    demo's central claim depended on operator setup and a release silently lost its
    scan whenever the setup was wrong.
    """

    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            async with _gateway_worker(env):
                handle = await _start_chain(env, "legacy-mode")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    _pipeline_request(
                        handle.id, "legacy-mode", scan_mode=SCAN_MODE_LEGACY
                    ),
                )

                assert response.status == "waiting_for_approval"
                parent = await _operation_view(handle, "legacy-mode-parent")
                assert parent.status == "waiting_for_dependency"
                assert "scan_workflow_id" not in parent.checkpoint
                assert backend._scan is None
                # Never asked. The provider was there and would have said yes.
                assert "get_security_scan_status" not in _tool_names()
                events = await _ledger_events(handle)
                assert "security_scan_handoff" not in events
                assert "security_scan_unavailable" not in events
                # The mandate's other half still applies: the promotion that comes
                # out is the Security team's to approve, whoever asked for it. That
                # is what makes their legacy script hit a wall it cannot wait at.
                child = await _operation_view(handle, "legacy-mode-child")
                assert child.required_approver_team == "security"

    asyncio.run(run())


def test_without_the_mandate_the_scan_is_not_in_the_flow_at_all() -> None:
    """The other half of the same rule, and the one CASE-1 depends on.

    The provider is up and advertising here -- the Security team's platform is
    running, exactly as it is for every other test in this file. The mandate is
    off, so the pipeline does not route through the scan, and it does not even ask
    whether one is on offer: the capability lookup never happens. A run under a
    fresh gateway is the plain cut/stage/gate/promote pipeline, which is what
    makes the CASE-1 scenarios demoable without stopping the Security worker.
    """

    async def run() -> None:
        _reset_backend(scan_available=True)
        async with await _environment() as env:
            async with _gateway_worker(env):
                handle = await _start_chain(env, "mandate-off-scan")
                response = await handle.execute_update(
                    AgenticChainWorkflow.request_nested_tool_call,
                    _pipeline_request(
                        handle.id, "mandate-off-scan", security_mandate=False
                    ),
                )

                assert response.status == "waiting_for_approval"
                parent = await _operation_view(handle, "mandate-off-scan-parent")
                assert parent.status == "waiting_for_dependency"
                assert "scan_workflow_id" not in parent.checkpoint
                assert backend._scan is None
                # Not "asked and was told no" -- never asked. The provider would
                # have said yes.
                assert "get_security_scan_status" not in _tool_names()
                events = await _ledger_events(handle)
                assert "scan_capability_discovered" not in events
                assert "security_scan_handoff" not in events
                # And the promotion it opened instead is nobody's team in
                # particular, so any approver can clear it.
                promotion = await _operation_view(handle, "mandate-off-scan-child")
                assert promotion.status == "waiting_for_approval"
                assert promotion.required_approver_team is None

    asyncio.run(run())


# ------------------------------------------------- the bound on a parked scan
#
# These three run against the chain object directly rather than against a
# Worker, and that is not a shortcut taken to keep them fast. The behaviour
# under test is what happens after fifteen minutes of silence, and there is no
# honest way to reach that point on a real dev server: this suite needs two
# namespaces and real Nexus, so it runs on a real server, and a real server does
# not skip time. Driving the decision function with a clock the test controls
# tests the actual rule. That the rule is wired into the running Workflow, and
# that the deadline it reads is really written at handoff, is pinned by
# _scan_runs_and_suspends above.


def _parked_chain(monkeypatch, *, now: datetime) -> AgenticChainWorkflow:
    """A chain holding one pipeline operation parked on a security scan."""
    monkeypatch.setattr(chain_module.workflow, "now", lambda: now)
    monkeypatch.setattr(
        chain_module.workflow, "logger", logging.getLogger("test-chain")
    )
    chain = AgenticChainWorkflow(
        ChainInput(workflow_id="wf-parked", owner_principal=PRINCIPAL)
    )
    chain._state.operations["op-pipeline"] = Operation(
        operation_id="op-pipeline",
        tool_name="run_nested_release",
        arguments={"service": SERVICE, "environment": "prod"},
        idempotency_key="idem-parked",
        status=OperationStatus.WAITING_FOR_DEPENDENCY,
        requester=PRINCIPAL,
        created_iso=now.isoformat(),
        checkpoint={
            "stage": "awaiting_security_scan",
            "scan_provider": "security",
            "scan_endpoint": SECURITY_ENDPOINT,
            "scan_workflow_id": "security::scan::abc123",
            "scan_deadline_epoch": now.timestamp() + SCAN_REPORT_DEADLINE_SECONDS,
        },
    )
    return chain


def test_a_scan_that_never_reports_fails_the_release_rather_than_hanging(
    monkeypatch,
) -> None:
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    chain = _parked_chain(monkeypatch, now=start)
    parked = chain._state.operations["op-pipeline"]

    # A minute before the deadline nothing happens, and the loop is still
    # planning to wake up for it.
    monkeypatch.setattr(
        chain_module.workflow,
        "now",
        lambda: start + timedelta(seconds=SCAN_REPORT_DEADLINE_SECONDS - 60),
    )
    chain._expire_stalled_scans()
    assert parked.status == OperationStatus.WAITING_FOR_DEPENDENCY
    assert chain._scan_deadline(parked) is not None

    monkeypatch.setattr(
        chain_module.workflow,
        "now",
        lambda: start + timedelta(seconds=SCAN_REPORT_DEADLINE_SECONDS + 1),
    )
    chain._expire_stalled_scans()

    # Failed, not completed and not quietly abandoned: the mandate asked for a
    # scan, no scan was produced, and production does not move.
    assert parked.status == OperationStatus.FAILED
    assert parked.checkpoint["stage"] == "security_scan_timed_out"
    # The message has to be usable by someone watching a demo go wrong, so it
    # names the scan that vanished and the switch that brings it back.
    assert "security::scan::abc123" in parked.error
    assert "Security scanner worker" in parked.error
    assert [entry.event for entry in chain._state.ledger] == [
        "security_scan_timed_out"
    ]
    # And the operation is terminal, which is what makes the rest of the machine
    # notice: the caller gets a callback and the dashboard stops showing a
    # pipeline that is going nowhere.
    assert parked.status in chain_module.TERMINAL
    assert chain._scan_deadline(parked) is None


def test_the_scan_clock_stops_once_the_scan_has_reported(monkeypatch) -> None:
    """A slow approver is not a stalled scan.

    A scan that clears reports by opening a nested production promotion, and the
    pipeline operation stays parked on the same checkpoint stage while that
    promotion sits in front of a human. The approver has their own clock; this
    one must be off by then, or a long approval would fail the release.
    """
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    chain = _parked_chain(monkeypatch, now=start)
    parked = chain._state.operations["op-pipeline"]
    chain._state.operations["op-promotion"] = Operation(
        operation_id="op-promotion",
        tool_name="promote_release",
        arguments={"service": SERVICE, "environment": "prod"},
        idempotency_key="idem-promotion",
        status=OperationStatus.WAITING_FOR_APPROVAL,
        requester=PRINCIPAL,
        created_iso=start.isoformat(),
        origin_operation_id="op-pipeline",
    )

    assert chain._scan_deadline(parked) is None
    monkeypatch.setattr(
        chain_module.workflow,
        "now",
        lambda: start + timedelta(seconds=SCAN_REPORT_DEADLINE_SECONDS * 4),
    )
    chain._expire_stalled_scans()

    assert parked.status == OperationStatus.WAITING_FOR_DEPENDENCY
    assert chain._state.ledger == []


def test_a_chain_parked_by_an_older_build_carries_no_scan_deadline(
    monkeypatch,
) -> None:
    """Rollout safety: chains already parked when this code ships stay parked.

    Their checkpoint has no deadline in it, and inventing one for them would
    mean failing releases that were mid-flight during a deploy.
    """
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    chain = _parked_chain(monkeypatch, now=start)
    parked = chain._state.operations["op-pipeline"]
    del parked.checkpoint["scan_deadline_epoch"]

    monkeypatch.setattr(
        chain_module.workflow,
        "now",
        lambda: start + timedelta(days=1),
    )
    assert chain._scan_deadline(parked) is None
    chain._expire_stalled_scans()
    assert parked.status == OperationStatus.WAITING_FOR_DEPENDENCY
