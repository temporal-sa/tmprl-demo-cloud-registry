from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta, timezone

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from common.models import (
        SCAN_MODE_PLATFORM,
        ApprovalDecision,
        AwaitQualityGateInput,
        CancelOperation,
        ChainInput,
        ChainState,
        ChainSummary,
        CutReleaseInput,
        EvaluatePolicyInput,
        InvokeToolInput,
        LedgerEntry,
        Operation,
        OperationStatus,
        OperationView,
        NestedToolCallRequest,
        PromoteReleaseInput,
        ResumeNestedRequest,
        ScanVerdict,
        SignalOperationCallbackInput,
        ToolCallRequest,
        ToolCallResponse,
    )
    from common.nexus_contracts import (
        SECURITY_ENDPOINT,
        SecurityScanService,
        StartSecurityScanInput,
    )
    from common.semver import InvalidVersionError, next_version, normalize_bump
    from activities.gateway_activities import (
        await_quality_gate,
        evaluate_policy,
        invoke_tool,
        signal_operation_callback,
    )
    from workflows.release_children import (
        CutReleaseChildWorkflow,
        PromoteReleaseChildWorkflow,
    )


POLICY_TIMEOUT = timedelta(seconds=10)
INVOKE_TIMEOUT = timedelta(seconds=60)
# A release step that is a Child Workflow rather than a single Activity. Both
# are a handful of short mock steps, so the ceiling is generous rather than
# tight; the per-step timeouts inside the child are what actually bound them.
RELEASE_CHILD_TIMEOUT = timedelta(minutes=5)
# Waiting on the verdict for a candidate that was just staged. The gate runs
# four checks concurrently, so it is seconds of work, but the Activity is a
# genuine wait on another workflow's completion and is given room for a Worker
# restart mid-run.
QUALITY_GATE_WAIT_TIMEOUT = timedelta(minutes=5)
# Starting a pre-prod security scan with the Security team. A Nexus call to
# another team's endpoint, and a short one: their handler starts a workflow and
# hands back its id rather than waiting for the scan to finish. See
# _hand_off_to_scan for why it must not be the other way round.
SCAN_HANDOFF_TIMEOUT = timedelta(seconds=30)
# Delivering a decision to whatever is holding a request open. Same namespace,
# but worth retrying hard: it is what wakes a system that is deliberately asleep.
CALLBACK_TIMEOUT = timedelta(seconds=30)

# Idle timeout for an agentic chain. A chain that has no pending work and receives
# no new tool call or decision for this long completes on its own. This is a fixed
# constant rather than an environment read, because Workflow code must not read
# non-deterministic sources: every Worker replaying this Workflow must agree on the
# value. To demo the idle close live, lower this and rebuild the worker image. To
# make it per-chain configurable, carry it on ChainInput instead of here.
IDLE_TIMEOUT_SECONDS = 24 * 60 * 60

TERMINAL = {
    OperationStatus.COMPLETED,
    OperationStatus.REJECTED,
    OperationStatus.EXPIRED,
    OperationStatus.CANCELED,
    OperationStatus.BLOCKED,
    OperationStatus.FAILED,
}
PENDING = {
    OperationStatus.EVALUATING,
    OperationStatus.WAITING_FOR_APPROVAL,
    OperationStatus.APPROVED,
    OperationStatus.APPROVED_AWAITING_RETRY,
    OperationStatus.WAITING_FOR_DEPENDENCY,
    OperationStatus.INVOKING,
}

# Continue-As-New input is subject to the same 2 MB payload limit as any other
# Workflow input. Keep every pending operation plus the most recent terminal
# operations, and drop older terminal records. A production system offloads
# older records to external storage. See README, "Continue-As-New".
TERMINAL_RETENTION = 50
LEDGER_RETENTION = 500

# The release pipeline the orchestrated CASE-2 path runs: a candidate reaches
# staging, is qualified by quality gates there, and only then goes to its target.
# These are constants rather than environment reads because the pipeline is a fixed
# flow, which is the entire reason it exists, and because every Worker replaying
# this Workflow must agree on the shape it took.
PIPELINE_STAGING_ENVIRONMENT = "staging"
PIPELINE_PRODUCTION_ENVIRONMENT = "prod"

# The capability lookup the pipeline does after its gates pass, once the mandate
# has put a scan in its path at all. Pre-prod security scanning belongs to the
# Security team, so the pipeline does not have it configured; it asks the shared
# platform whether that team is currently offering it, and routes through it when
# they are. Any time their worker is not running the answer is no and the pipeline
# opens the production promotion itself, exactly as it always did.
SCAN_CAPABILITY_TOOL = "get_security_scan_status"

# Which QuickMeals engineering team must approve a promotion, keyed by the
# service that asked for it. A promotion the Security team's scan requested is
# the Security team's call to stand behind, so a member of that team has to be
# the one who approves it -- that authorization requirement is what makes the
# scan the only correct caller, rather than something the Waypoint pipeline
# could equally well have relayed on their behalf.
#
# Anything not named here has no team restriction, which is every CASE-1 call,
# every CASE-2a run with no scan in play, and every CASE-3 run.
CALLER_SERVICE_APPROVER_TEAMS = {"security": "security"}

# The company-wide mandate: while it is in effect, no engineering team promotes
# its own service to production on its own say-so. That has two consequences, and
# the mandate is the single thing that produces both:
#
#   1. The release pipeline routes a qualified candidate through the Security
#      team's pre-prod scan before it opens the production promotion. While the
#      mandate is off there is no scan step in the flow at all -- the pipeline does
#      not even ask whether one is on offer -- so a run is the plain
#      cut/stage/gate/promote pipeline it was before the Security team onboarded.
#   2. The production promotion that comes out the far end is the Security team's
#      to approve, whoever asked for it.
#
# The toggle itself lives in the gateway process and rides in on the request (see
# ToolCallRequest.security_mandate); these two constants are the only thing the
# Workflow knows about it.
#
# "production" is accepted alongside "prod" because PROTECTED_ENVIRONMENTS names
# both, and a mandate that silently did not apply to one spelling would be worse
# than useless. Listed as a constant rather than read from the environment for
# the usual reason: every Worker replaying this Workflow must agree.
MANDATE_ENVIRONMENTS = ("prod", "production")
MANDATE_APPROVER_TEAM = "security"


def _required_approver_team_for(
    caller_service: str | None,
    environment: str,
    security_mandate: bool,
) -> str | None:
    """Which QuickMeals team, if any, must be the one to approve this operation.

    Two independent reasons an operation can be team-restricted, checked in the
    order they were introduced:

    1. Which team's check produced it. A promotion the Security team's own
       pre-prod scan asked for is theirs to stand behind, and that is true
       whether or not any mandate is in force.
    2. The company-wide security mandate. While it is on, a production promotion
       is Security's call no matter who asked, including the Waypoint team asking
       for its own service.

    Everything else is unrestricted and stays approvable by anyone in
    GATEWAY_APPROVERS, which is every staging promotion, every read, and every
    production promotion made before the mandate landed.
    """
    team = CALLER_SERVICE_APPROVER_TEAMS.get(str(caller_service or "").lower())
    if team:
        return team
    if security_mandate and environment.strip().lower() in MANDATE_ENVIRONMENTS:
        return MANDATE_APPROVER_TEAM
    return None


# A caller that is itself a scanner. Both of the Security team's scanners identify
# as this -- their Temporal workflow states it over Nexus, their legacy host script
# gets it from its own bearer token -- and it is the one thing that exempts a
# promotion from having a scan put in front of it.
#
# Without the exemption the platform mode would scan the output of a scan: the
# Security team's own promotion request would be handed back to the Security team
# to scan again, forever. Keyed on who is calling rather than on which mode is
# current, so it holds even if somebody flips the scanner switch to Temporal while
# the legacy script is mid-run.
SCANNER_CALLER_SERVICE = "security"


def _scan_required_for(
    caller_service: str | None,
    environment: str,
    security_mandate: bool,
    scan_mode: str,
) -> bool:
    """Whether the gateway inserts a pre-prod security scan ahead of this action.

    Three conditions, all of them about the request rather than about what happens
    to be deployed:

    1. The mandate is on. It is the rule that puts a scan in the path at all.
    2. The action targets a mandate-covered environment. Staging promotions and
       reads are not covered and never were.
    3. The Security team is running their Temporal workflow. In legacy mode there
       is nothing for the gateway to hand off TO -- their scanner is a script on
       somebody's host, started by a person, and the gateway inserting a step it
       cannot start would park the release on a dependency that never arrives.
       Legacy mode is the mode where the human runs the scan and the scan asks for
       the promotion itself.

    And one exemption: a request that came from a scanner is already the output of
    a scan.

    Deliberately not a condition: which tool the caller used. This check runs on
    the single-tool path and the orchestrated path alike, because a company-wide
    mandate that an agent sidesteps by calling promote_release instead of
    run_release_orchestration is not a mandate -- and an agent hand-rolling its own
    steps is exactly the case the boundary exists for.

    Also deliberately not a condition: whether anyone is currently answering on
    the Security team's task queue. That used to decide this, which meant a
    release quietly skipped its scan whenever their worker was down, reporting
    success. Liveness is checked separately and failure is loud; see
    _scan_capability.
    """
    if not security_mandate:
        return False
    if scan_mode != SCAN_MODE_PLATFORM:
        return False
    if environment.strip().lower() not in MANDATE_ENVIRONMENTS:
        return False
    return str(caller_service or "").lower() != SCANNER_CALLER_SERVICE

# Executing a tool step raises one of these depending on whether the step is an
# Activity or a Child Workflow. Callers care that the step failed, not which
# shape it had, so they catch the pair.
TOOL_STEP_ERRORS = (ActivityError, ChildWorkflowError)


class QualityGateFailure(Exception):
    """Quality gates did not pass, so the pipeline stops before its target.

    Raised inside the Workflow and caught by the calling handler, which fails the
    parent operation without ever creating the child. A candidate that failed its
    gates does not reach the approval queue: the system declines to ask.
    """


class SecurityScanUnavailable(Exception):
    """The mandated security scan could not be reached, so the release stops.

    Raised instead of quietly degrading. The gateway used to treat an unreachable
    scanner as "there is no scan step", which meant the mandate evaporated
    whenever the Security team's worker was down and the release reported success
    with a checkpoint silently missing. Failing is the safety property: an
    unreachable checkpoint is an unmet one.
    """


class PipelinePolicyConflict(Exception):
    """Policy protects an environment the pipeline promotes through internally.

    The pipeline carries one approval gate, at its target. If policy also protects
    an intermediate environment, the pipeline refuses rather than promoting to a
    protected environment without an approval.
    """


class PipelineVersionDrift(Exception):
    """A replay recomputed a different version than the one already approved.

    The approver approved one specific version. If production moved between the
    approval and the retry, recomputing the bump lands somewhere else, and
    promoting that would execute something nobody approved. Raised before the
    replay mutates anything.
    """


@workflow.defn
class AgenticChainWorkflow:
    """One durable, long-lived workflow per agentic chain (workflow_id).

    The workflow holds the approval pause. A tool call arrives as an Update and
    returns fast with either a completed result or a waiting payload. Approve and
    reject decisions arrive as Signals that only mutate state; the main loop reacts
    to those state changes and performs the downstream invocation. This mirrors the
    Temporal best practice that message handlers should not drive Activities
    directly. The workflow rolls over with Continue-As-New to keep history bounded,
    and completes on its own once it has been idle with no pending work.
    """

    @workflow.init
    def __init__(self, input: ChainInput) -> None:
        # State is initialized before any handler runs, which matters because a
        # tool-call Update can arrive together with workflow start (update-with-start).
        self._state: ChainState = input.state or ChainState(
            workflow_id=input.workflow_id,
            owner_principal=input.owner_principal,
        )
        self._lock = asyncio.Lock()
        self._closing = False
        # Transient wake flag so the main loop recomputes its timeout when a new
        # waiting operation with a fresh deadline is created. Not carried across CAN.
        self._reschedule = False

    @workflow.run
    async def run(self, input: ChainInput) -> ChainSummary:
        # Seed the idle clock on first run only. On Continue-As-New the carried value
        # is preserved so a rollover does not reset the idle window.
        if self._state.last_activity_epoch == 0.0:
            self._state.last_activity_epoch = workflow.now().timestamp()

        while True:
            try:
                await workflow.wait_condition(
                    self._loop_should_wake,
                    timeout=self._next_wait(),
                )
            except asyncio.TimeoutError:
                # Timeout means an approval deadline or the idle window may have elapsed.
                pass

            self._reschedule = False
            self._expire_overdue()
            await self._process_approved()
            await self._settle_handoffs()

            if not self._closing and workflow.info().is_continue_as_new_suggested():
                await self._do_continue_as_new()  # does not return

            # Graceful completion: an explicit close signal, or the chain has been
            # idle with nothing pending. Drain handlers first, then re-check that no
            # work arrived during the drain before completing.
            if not self._has_pending_work() and (self._closing or self._is_idle()):
                await workflow.wait_condition(workflow.all_handlers_finished)
                if not self._has_pending_work():
                    if self._closing:
                        self._log("closed", None)
                    else:
                        self._log("idle_timeout", None, {"idle_seconds": IDLE_TIMEOUT_SECONDS})
                    return self._summary()

    # ----------------------------------------------------------------- handlers

    @workflow.update
    async def request_tool_call(self, req: ToolCallRequest) -> ToolCallResponse:
        """Idempotent, deterministic tool-call entry point.

        Returns quickly with a completed result (no approval needed) or a waiting
        payload (approval needed). The downstream tool is never invoked here on the
        approval path; the main loop performs it after approval.
        """
        async with self._lock:
            self._touch()
            existing_id = self._state.idempotency_index.get(req.idempotency_key)
            if existing_id is not None:
                op = self._state.operations[existing_id]
                if req.callback_workflow_id:
                    if (
                        op.callback_workflow_id
                        and op.callback_workflow_id != req.callback_workflow_id
                    ):
                        raise ValueError(
                            "idempotent tool call already has a different "
                            "callback workflow"
                        )
                    # Covers an Activity retry after the Update committed but
                    # before its response reached the autonomous caller. A
                    # terminal operation becomes callback-eligible immediately.
                    op.callback_workflow_id = req.callback_workflow_id
                self._log("dedup_hit", op)
                return self._response_for(op)

            op = self._new_operation(req)
            self._state.operations[op.operation_id] = op
            self._state.idempotency_index[req.idempotency_key] = op.operation_id
            self._log(
                "created",
                op,
                {
                    "tool_name": op.tool_name,
                    "requester": op.requester,
                    "workflow_id_source": op.workflow_id_source,
                    "call_path": op.call_path,
                    "arguments": op.safe_arguments,
                },
            )

            try:
                decision = await workflow.execute_activity(
                    evaluate_policy,
                    EvaluatePolicyInput(
                        tool_name=req.tool_name,
                        arguments=req.arguments,
                        caller_principal=req.correlation.caller_principal,
                    ),
                    start_to_close_timeout=POLICY_TIMEOUT,
                )
            except ActivityError as err:
                op.status = OperationStatus.FAILED
                op.error = f"policy evaluation failed: {err}"
                op.decided_iso = workflow.now().isoformat()
                self._log("policy_failed", op, {"error": str(err)})
                return self._response_for(op)
            op.risk_reason = decision.reason
            op.protected = decision.requires_approval
            self._log(
                "policy_evaluated",
                op,
                {"requires_approval": decision.requires_approval},
            )

            if not decision.requires_approval:
                op.status = OperationStatus.INVOKING
                await self._invoke(op)
                return self._response_for(op)

            # The mandate applies here too, and this is the path that matters
            # most. An agent that decides to call promote_release itself rather
            # than run_release_orchestration used to reach production having
            # skipped the scan entirely, with the approval gate as the only thing
            # left standing -- the pipeline was where the scan lived, so stepping
            # outside the pipeline stepped outside the scan. A company-wide rule
            # that holds only on the path the agent was supposed to take is not a
            # rule, so the check is here, at the single-tool boundary, and not
            # only in the orchestrator.
            #
            # The operation is parked on the scan rather than sent for approval.
            # Nobody is asked to approve a promotion that has not been scanned
            # yet; the scan asks for it once it clears, and that request is the
            # one that reaches a human.
            if _scan_required_for(
                req.correlation.caller_service,
                str(req.arguments.get("environment", "")),
                req.security_mandate,
                req.scan_mode,
            ):
                try:
                    handoff = await self._scan_capability(
                        op,
                        str(req.arguments.get("service", "")),
                        str(req.arguments.get("version", "")),
                        str(req.arguments.get("environment", "")),
                        req.idempotency_key,
                        "direct",
                    )
                except SecurityScanUnavailable:
                    # Failed, with the reason and both fixes already on the
                    # operation. The promotion is not offered for approval:
                    # nobody approves a production release that skipped a
                    # checkpoint the mandate says it needs.
                    return self._response_for(op)
                return await self._hand_off_to_scan(
                    op,
                    req.idempotency_key,
                    handoff,
                    str(req.arguments.get("version", "")),
                )

            op.status = OperationStatus.WAITING_FOR_APPROVAL
            op.deadline_epoch = workflow.now().timestamp() + req.approval_timeout_seconds
            op.deadline_iso = self._iso(op.deadline_epoch)
            self._reschedule = True
            self._log("waiting_for_approval", op)
            return self._response_for(op)

    @request_tool_call.validator
    def _validate_request(self, req: ToolCallRequest) -> None:
        # Rejected Updates leave no trace in Event History.
        if not req.tool_name:
            raise ValueError("tool_name is required")
        if not req.idempotency_key:
            raise ValueError("idempotency_key is required")
        self._validate_owner(req.correlation.caller_principal)

    @workflow.update
    async def request_nested_tool_call(
        self, req: NestedToolCallRequest
    ) -> ToolCallResponse:
        """Run Tool1 until it reaches Tool2, then suspend safely at Tool2.

        Controlled Tool1 calls are checkpointed in workflow state and resume
        automatically after Tool2 approval. Uncontrolled Tool1 calls fail closed:
        approval is recorded, but Tool2 is not invoked until the caller explicitly
        retries and Tool1 advertised replay safety.
        """
        async with self._lock:
            self._touch()
            existing_id = self._state.idempotency_index.get(req.idempotency_key)
            if existing_id is not None:
                parent = self._state.operations[existing_id]
                child = (
                    self._state.operations.get(parent.child_operation_id)
                    if parent.child_operation_id
                    else None
                )
                self._log("dedup_hit", parent)
                return self._nested_response(parent, child)

            if req.origin_operation_id:
                origin = self._state.operations.get(req.origin_operation_id)
                if (
                    origin is None
                    or origin.status != OperationStatus.WAITING_FOR_DEPENDENCY
                ):
                    # The scan can finish after its caller has canceled or
                    # otherwise settled the original promotion. Do not create a
                    # fresh nested promotion after that durable decision.
                    if origin is None:
                        return ToolCallResponse(
                            status="failed",
                            workflow_id=self._state.workflow_id,
                            operation_id=req.nested_operation_id or None,
                            reason="origin_operation_not_found",
                            message=(
                                "The operation waiting on this security scan no "
                                "longer exists; no promotion was created."
                            ),
                        )
                    self._log(
                        "nested_request_refused_origin_settled",
                        origin,
                        {"origin_status": origin.status.value},
                    )
                    return self._response_for(origin)

            parent = self._new_nested_parent(req)
            self._state.operations[parent.operation_id] = parent
            self._state.idempotency_index[req.idempotency_key] = parent.operation_id
            self._log(
                "nested_tool1_started",
                parent,
                {
                    "controlled": req.controlled_tool1,
                    "replay_safe": req.replay_safe,
                    "call_path": parent.call_path,
                    "arguments": parent.safe_arguments,
                },
            )

            # Tool1 is safe, so it can run up to the protected Tool2 boundary.
            # A bump-driven Tool1 does real work here (read the deployed version,
            # compute the next one, cut it); a Tool1 given an explicit version
            # runs its single prepare step exactly as it always has.
            resolved_version = ""
            resolved_tool2_arguments: dict | None = None
            resolved_requested_action: str | None = None
            try:
                if req.bump:
                    prepare, resolved_version = await self._orchestrate_tool1(
                        parent,
                        req.bump,
                        req.idempotency_key,
                        allow_scan=_scan_required_for(
                            req.correlation.caller_service,
                            str(req.tool2_arguments.get("environment", "")),
                            req.security_mandate,
                            req.scan_mode,
                        ),
                    )
                    resolved_tool2_arguments = {
                        **req.tool2_arguments,
                        "version": resolved_version,
                    }
                    resolved_requested_action = self._promotion_sentence(
                        resolved_tool2_arguments
                    )
                else:
                    prepare = await self._execute_tool_step(
                        tool_name=f"{req.tool1_name}_prepare",
                        arguments=req.tool1_arguments,
                        idempotency_key=f"{req.idempotency_key}:prepare",
                    )
            except TOOL_STEP_ERRORS as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 preparation failed: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_prepare_failed", parent, {"error": str(err)})
                return self._response_for(parent)
            except InvalidVersionError as err:
                # A version the orchestrator cannot interpret stops the flow. It
                # never falls back to a guessed version, and Tool2 is never
                # created, so nothing reaches the approval surface.
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 could not resolve a version: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_version_resolution_failed",
                    parent,
                    {"error": str(err), "bump": req.bump},
                )
                return self._response_for(parent)
            except QualityGateFailure as err:
                # The candidate did not qualify. The pipeline stops here, so the
                # child is never created and nothing is ever put in front of an
                # approver. Refusing to ask is the safety property.
                parent.status = OperationStatus.FAILED
                parent.error = f"Quality gates did not pass: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_quality_gates_failed",
                    parent,
                    {"error": str(err)},
                )
                return self._response_for(parent)
            except PipelinePolicyConflict as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Release pipeline refused to run: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_pipeline_policy_conflict",
                    parent,
                    {"error": str(err)},
                )
                return self._response_for(parent)
            except SecurityScanUnavailable:
                # _scan_capability already failed the operation and wrote the
                # reason, which names both fixes. Nothing to add.
                return self._response_for(parent)
            parent.checkpoint = {
                "stage": "waiting_for_tool2",
                "tool1_prepare_result": prepare,
                "nested_tool": req.tool2_name,
            }
            if resolved_version:
                # Recorded so a Worker or gateway restart resumes against the
                # version this run actually cut, and so an explicit replay can
                # detect that the fleet moved underneath it.
                parent.checkpoint["bump"] = req.bump
                parent.checkpoint["resolved_version"] = resolved_version
                parent.arguments = {
                    **parent.arguments,
                    "version": resolved_version,
                }
                parent.safe_arguments = {
                    **parent.safe_arguments,
                    "version": resolved_version,
                }
            self._log("tool1_checkpointed", parent, {"stage": "waiting_for_tool2"})

            # A cancellation Signal may arrive while the safe Tool1 preparation
            # Activity is awaited. Honor it before creating the protected Tool2.
            if parent.status in TERMINAL:
                return self._response_for(parent)
            if req.origin_operation_id:
                origin = self._state.operations.get(req.origin_operation_id)
                if (
                    origin is None
                    or origin.status != OperationStatus.WAITING_FOR_DEPENDENCY
                ):
                    parent.status = (
                        origin.status
                        if origin is not None and origin.status in TERMINAL
                        else OperationStatus.CANCELED
                    )
                    parent.error = (
                        origin.error
                        if origin is not None
                        else "The originating operation no longer exists."
                    )
                    parent.decided_iso = workflow.now().isoformat()
                    self._log("nested_request_stopped_with_origin", parent)
                    return self._response_for(parent)

            handoff = prepare.get("scan_handoff") if req.bump else None
            if handoff:
                return await self._hand_off_to_scan(
                    parent, req.idempotency_key, handoff, resolved_version
                )

            child = self._new_nested_child(
                req,
                parent,
                arguments=resolved_tool2_arguments,
                requested_action=resolved_requested_action,
            )
            parent.child_operation_id = child.operation_id
            self._state.operations[child.operation_id] = child
            self._state.idempotency_index[
                f"{req.idempotency_key}:nested"
            ] = child.operation_id
            self._log(
                "nested_tool2_created",
                child,
                {
                    "parent_operation_id": parent.operation_id,
                    "call_path": child.call_path,
                    "arguments": child.safe_arguments,
                },
            )

            try:
                decision = await workflow.execute_activity(
                    evaluate_policy,
                    EvaluatePolicyInput(
                        tool_name=child.tool_name,
                        arguments=child.arguments,
                        caller_principal=child.requester,
                    ),
                    start_to_close_timeout=POLICY_TIMEOUT,
                )
            except ActivityError as err:
                child.status = OperationStatus.FAILED
                child.error = f"policy evaluation failed: {err}"
                child.decided_iso = workflow.now().isoformat()
                self._log("policy_failed", child, {"error": str(err)})
                self._propagate_child_terminal(child)
                return self._response_for(parent)
            child.risk_reason = decision.reason
            child.protected = decision.requires_approval
            self._log(
                "policy_evaluated",
                child,
                {"requires_approval": decision.requires_approval},
            )

            if not decision.requires_approval:
                child.status = OperationStatus.INVOKING
                await self._invoke(child)
                await self._resume_tool1(parent, child)
                return self._response_for(parent)

            child.status = OperationStatus.WAITING_FOR_APPROVAL
            child.deadline_epoch = (
                workflow.now().timestamp() + req.approval_timeout_seconds
            )
            child.deadline_iso = self._iso(child.deadline_epoch)
            parent.status = (
                OperationStatus.WAITING_FOR_DEPENDENCY
                if req.controlled_tool1
                else OperationStatus.BLOCKED
            )
            if not req.controlled_tool1:
                parent.error = (
                    "Tool1 is not suspension-aware. Approval can be recorded, but "
                    "the original call must be retried explicitly."
                )
            self._reschedule = True
            self._log(
                "waiting_for_approval",
                child,
                {
                    "controlled_parent": req.controlled_tool1,
                    "retry_required": not req.controlled_tool1,
                },
            )
            return self._nested_response(parent, child)

    @request_nested_tool_call.validator
    def _validate_nested_request(self, req: NestedToolCallRequest) -> None:
        if not req.tool1_name or not req.tool2_name:
            raise ValueError("tool1_name and tool2_name are required")
        if not req.idempotency_key:
            raise ValueError("idempotency_key is required")
        self._validate_owner(req.correlation.caller_principal)

    @workflow.update
    async def resume_nested_tool_call(
        self, req: ResumeNestedRequest
    ) -> ToolCallResponse:
        """Explicitly replay an approved uncontrolled nested call when safe."""
        async with self._lock:
            self._touch()
            self._validate_owner(req.caller_principal)
            child = self._state.operations.get(req.operation_id)
            if child is None or not child.parent_operation_id:
                return ToolCallResponse(
                    status="not_found",
                    workflow_id=self._state.workflow_id,
                    operation_id=req.operation_id,
                )
            parent = self._state.operations[child.parent_operation_id]
            if child.status != OperationStatus.APPROVED_AWAITING_RETRY:
                return self._nested_response(parent, child)
            if not parent.replay_safe:
                self._log(
                    "unsafe_replay_refused",
                    child,
                    {"parent_operation_id": parent.operation_id},
                )
                return self._nested_response(parent, child)

            parent.status = OperationStatus.INVOKING
            self._log(
                "explicit_nested_retry",
                child,
                {"parent_operation_id": parent.operation_id},
            )
            bump = str(parent.arguments.get("bump", ""))
            try:
                if bump:
                    # Replaying a bump-driven Tool1 reruns the whole pipeline. The
                    # mutating steps carry their original idempotency keys, so the
                    # cut and the staging promotion are found already done rather
                    # than repeated. The read and the quality gates are keyed to
                    # this pass, so production is re-read and the candidate is
                    # re-verified: that is the price of a Tool1 that could not hold
                    # the pause, and it is paid in gate minutes.
                    replay_prepare, _ = await self._orchestrate_tool1(
                        parent,
                        bump,
                        parent.idempotency_key,
                        pass_label="replay",
                        expect_version=str(child.arguments.get("version", "")),
                    )
                else:
                    replay_prepare = await self._execute_tool_step(
                        tool_name=f"{parent.tool_name}_prepare",
                        arguments=parent.arguments,
                        idempotency_key=f"{parent.idempotency_key}:prepare",
                    )
            except TOOL_STEP_ERRORS as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 replay failed: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_replay_failed", parent, {"error": str(err)})
                return self._response_for(parent)
            except InvalidVersionError as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Tool1 could not resolve a version: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_version_resolution_failed",
                    parent,
                    {"error": str(err), "bump": bump},
                )
                return self._response_for(parent)
            except PipelineVersionDrift as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Replay refused: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_replay_version_drift", parent, {"error": str(err)})
                return self._response_for(parent)
            except QualityGateFailure as err:
                # Re-verification failed on the retry. The candidate was green
                # when the approver saw it and is not green now, so the approved
                # promotion does not run.
                parent.status = OperationStatus.FAILED
                parent.error = f"Quality gates did not pass on replay: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log("tool1_quality_gates_failed", parent, {"error": str(err)})
                return self._response_for(parent)
            except PipelinePolicyConflict as err:
                parent.status = OperationStatus.FAILED
                parent.error = f"Release pipeline refused to run: {err}"
                parent.decided_iso = workflow.now().isoformat()
                self._log(
                    "tool1_pipeline_policy_conflict", parent, {"error": str(err)}
                )
                return self._response_for(parent)
            except SecurityScanUnavailable:
                # The scanner went away between the approval and the retry. Same
                # answer as everywhere else: the release stops rather than
                # promoting past a checkpoint it cannot reach.
                return self._response_for(parent)
            parent.checkpoint = {
                **parent.checkpoint,
                "stage": "replayed_to_tool2_boundary",
                "tool1_replay_result": replay_prepare,
            }
            self._log("tool1_replayed", parent)
            child.status = OperationStatus.INVOKING
            await self._invoke(child)
            await self._resume_tool1(parent, child, replay=True)
            return self._response_for(parent)

    @workflow.signal
    def approve_operation(self, decision: ApprovalDecision) -> None:
        # Signal handlers only mutate state. They are synchronous, so they run to
        # completion without interleaving, and they are idempotent: a duplicate or
        # late decision on an operation that is no longer waiting is ignored.
        op = self._state.operations.get(decision.operation_id)
        if op is None or op.status != OperationStatus.WAITING_FOR_APPROVAL:
            return
        self._touch()
        if (
            op.deadline_epoch is not None
            and workflow.now().timestamp() >= op.deadline_epoch
        ):
            op.status = OperationStatus.EXPIRED
            op.decided_iso = workflow.now().isoformat()
            self._log(
                "late_approval_ignored",
                op,
                {"approver": decision.approver},
            )
            self._propagate_child_terminal(op)
            return
        if not self._approver_authorized(op, decision):
            # Refused, not ignored. The operation stays waiting, so the right
            # approver can still act on it, and the ledger records the attempt.
            # The gateway rejects this at the HTTP boundary before it ever gets
            # here (see gateway.server._authorize_approver), which is what gives
            # the approver an immediate error instead of a decision that appears
            # to land and does nothing. This is the same check enforced a second
            # time on the durable side, so a Signal sent straight to the workflow
            # cannot walk around the UI.
            self._log(
                "approval_refused_wrong_team",
                op,
                {
                    "approver": decision.approver,
                    "approver_team": decision.approver_team or None,
                    "required_approver_team": op.required_approver_team,
                },
            )
            return
        parent = (
            self._state.operations.get(op.parent_operation_id)
            if op.parent_operation_id
            else None
        )
        if parent is not None and not parent.controlled_tool:
            # Do not execute a protected nested action after an uncontrolled
            # Tool1 stack has unwound. Store the decision and require an explicit,
            # replay-safe retry.
            op.status = OperationStatus.APPROVED_AWAITING_RETRY
        else:
            op.status = OperationStatus.APPROVED
        op.approver = decision.approver
        op.decided_iso = workflow.now().isoformat()
        self._log(
            (
                "approved_awaiting_retry"
                if op.status == OperationStatus.APPROVED_AWAITING_RETRY
                else "approved"
            ),
            op,
            {"approver": decision.approver},
        )

    @workflow.signal
    def reject_operation(self, decision: ApprovalDecision) -> None:
        op = self._state.operations.get(decision.operation_id)
        if op is None or op.status != OperationStatus.WAITING_FOR_APPROVAL:
            return
        self._touch()
        # Rejecting is a decision too, and it takes the operation out of the
        # queue just as finally as approving does. If the Security team owns the
        # call, they own both halves of it: someone outside the team cannot
        # dispose of a Security-gated promotion either way.
        if not self._approver_authorized(op, decision):
            self._log(
                "rejection_refused_wrong_team",
                op,
                {
                    "approver": decision.approver,
                    "approver_team": decision.approver_team or None,
                    "required_approver_team": op.required_approver_team,
                },
            )
            return
        op.status = OperationStatus.REJECTED
        op.approver = decision.approver
        op.error = decision.reason
        op.decided_iso = workflow.now().isoformat()
        self._log(
            "rejected",
            op,
            {"approver": decision.approver, "reason": decision.reason},
        )
        self._propagate_child_terminal(op)

    @workflow.signal
    def cancel_operation(self, cancellation: CancelOperation) -> None:
        op = self._state.operations.get(cancellation.operation_id)
        if op is None or op.status in TERMINAL or op.status == OperationStatus.INVOKING:
            return

        # If this is an operation parked on a scan, the scan may already have
        # created its nested promotion. Cancellation is still safe until that
        # protected child begins invoking. Once invocation starts (or finishes),
        # the eventual result is authoritative and we refuse to claim it stopped.
        dependents = [
            candidate
            for candidate in self._state.operations.values()
            if candidate.origin_operation_id == op.operation_id
        ]
        for dependent in dependents:
            child = (
                self._state.operations.get(dependent.child_operation_id)
                if dependent.child_operation_id
                else None
            )
            if dependent.status == OperationStatus.COMPLETED or (
                child is not None
                and child.status
                in {OperationStatus.INVOKING, OperationStatus.COMPLETED}
            ):
                self._log(
                    "cancellation_refused_dependency_committed",
                    op,
                    {
                        "dependent_operation_id": dependent.operation_id,
                        "child_operation_id": (
                            child.operation_id if child is not None else None
                        ),
                    },
                )
                return
        self._touch()
        op.status = OperationStatus.CANCELED
        op.approver = cancellation.canceled_by
        op.error = cancellation.reason
        op.decided_iso = workflow.now().isoformat()
        self._log(
            "canceled",
            op,
            {
                "canceled_by": cancellation.canceled_by,
                "reason": cancellation.reason,
            },
        )
        if op.child_operation_id:
            child = self._state.operations.get(op.child_operation_id)
            if child is not None and child.status not in TERMINAL:
                child.status = OperationStatus.CANCELED
                child.approver = cancellation.canceled_by
                child.error = cancellation.reason
                child.decided_iso = workflow.now().isoformat()
                self._log(
                    "canceled",
                    child,
                    {
                        "canceled_by": cancellation.canceled_by,
                        "reason": cancellation.reason,
                        "parent_canceled": True,
                    },
                )
        for dependent in dependents:
            if dependent.status not in TERMINAL:
                dependent.status = OperationStatus.CANCELED
                dependent.approver = cancellation.canceled_by
                dependent.error = cancellation.reason
                dependent.decided_iso = workflow.now().isoformat()
                self._log(
                    "canceled",
                    dependent,
                    {
                        "canceled_by": cancellation.canceled_by,
                        "reason": cancellation.reason,
                        "origin_canceled": True,
                    },
                )
            if dependent.child_operation_id:
                child = self._state.operations.get(dependent.child_operation_id)
                if child is not None and child.status not in TERMINAL:
                    child.status = OperationStatus.CANCELED
                    child.approver = cancellation.canceled_by
                    child.error = cancellation.reason
                    child.decided_iso = workflow.now().isoformat()
                    self._log(
                        "canceled",
                        child,
                        {
                            "canceled_by": cancellation.canceled_by,
                            "reason": cancellation.reason,
                            "origin_canceled": True,
                        },
                    )
        self._propagate_child_terminal(op)

    @workflow.signal
    def scan_verdict_reported(self, verdict: ScanVerdict) -> None:
        """The Security team reporting that no promotion request is coming.

        Only the failing verdict arrives this way. A scan that clears reports
        itself by making the nested promote_release call, which is a request the
        gateway governs like any other. A scan that fails has nothing to request,
        and without this the pipeline operation that handed off would wait
        indefinitely on a call that will never be made.

        A failed scan means production was never touched, which is the same
        fail-closed shape as a failed quality gate, one checkpoint later.
        """
        op = self._state.operations.get(verdict.origin_operation_id)
        if op is None or op.status != OperationStatus.WAITING_FOR_DEPENDENCY:
            return
        self._touch()
        if verdict.verdict == "pass":
            # Nothing to do: the promotion request is the report. Recorded so the
            # ledger shows the verdict arriving even on the path that does not
            # need it.
            self._log(
                "security_scan_passed",
                op,
                {"scan_workflow_id": verdict.scan_workflow_id},
            )
            return
        op.status = OperationStatus.FAILED
        op.error = (
            f"The pre-prod security scan did not pass: {verdict.reason}"
            if verdict.reason
            else "The pre-prod security scan did not pass."
        )
        op.checkpoint = {
            **op.checkpoint,
            "stage": "security_scan_failed",
            "scan_verdict": verdict.verdict,
            "scan_detail": verdict.detail,
        }
        op.decided_iso = workflow.now().isoformat()
        self._log(
            "security_scan_failed",
            op,
            {
                "scan_workflow_id": verdict.scan_workflow_id,
                "verdict": verdict.verdict,
                "reason": verdict.reason,
            },
        )

    @workflow.signal
    def close_chain(self) -> None:
        # Lets the chain complete gracefully once no work is pending, so it can be
        # queried afterward as a completed (not terminated) workflow.
        self._closing = True

    @workflow.query
    def get_operation_status(self, operation_id: str) -> ToolCallResponse:
        op = self._state.operations.get(operation_id)
        if op is None:
            return ToolCallResponse(
                status="not_found",
                workflow_id=self._state.workflow_id,
                operation_id=operation_id,
            )
        return self._response_for(op)

    @workflow.query
    def get_operation_result(self, operation_id: str) -> ToolCallResponse:
        return self.get_operation_status(operation_id)

    @workflow.query
    def get_workflow_status(self) -> ChainSummary:
        return self._summary()

    @workflow.query
    def get_ledger(self) -> list[LedgerEntry]:
        return list(self._state.ledger)

    @workflow.query
    def get_workflow_owner(self) -> str:
        return self._state.owner_principal

    @workflow.query
    def get_required_approver_team(self, operation_id: str) -> str:
        """Which team must decide this operation, or "" for no restriction.

        Queried by the gateway before it signals a decision, so an approver who
        is not allowed to act on an entry is told so immediately instead of
        watching a decision vanish. The Signal handlers enforce the same rule
        again on the durable side.
        """
        op = self._state.operations.get(operation_id)
        if op is None or not op.required_approver_team:
            return ""
        return op.required_approver_team

    @staticmethod
    def _approver_authorized(op: Operation, decision: ApprovalDecision) -> bool:
        """Is this decider on the team this operation requires?

        No restriction means anyone the gateway already accepted as an approver
        may decide, which is every operation except the ones a Security scan
        asked for. The team on the decision was resolved by the gateway from the
        validated bearer token, not supplied by whoever sent the Signal.
        """
        if not op.required_approver_team:
            return True
        return decision.approver_team == op.required_approver_team

    # ------------------------------------------------------------- loop helpers

    def _loop_should_wake(self) -> bool:
        if self._closing or self._reschedule:
            return True
        if workflow.info().is_continue_as_new_suggested():
            return True
        if not self._has_pending_work() and self._is_idle():
            return True
        now = workflow.now().timestamp()
        for op in self._state.operations.values():
            if op.status == OperationStatus.APPROVED:
                return True
            if (
                op.status == OperationStatus.WAITING_FOR_APPROVAL
                and op.deadline_epoch is not None
                and now >= op.deadline_epoch
            ):
                return True
            # A rejection or a cancellation lands in a Signal handler, which only
            # mutates state. Waking here is what turns that into a delivered
            # callback and a settled pipeline operation.
            if self._needs_callback(op) or self._settles_origin(op) is not None:
                return True
        return False

    def _next_wait(self) -> timedelta:
        now = workflow.now().timestamp()
        candidates = [
            op.deadline_epoch
            for op in self._state.operations.values()
            if op.status == OperationStatus.WAITING_FOR_APPROVAL
            and op.deadline_epoch is not None
        ]
        # Idle completion only applies when no work is pending. A pending
        # approved-awaiting-retry operation has no timer and should sleep until an
        # Update or Signal arrives, rather than spin after the idle deadline.
        if not self._has_pending_work():
            candidates.append(self._state.last_activity_epoch + IDLE_TIMEOUT_SECONDS)
        if not candidates:
            candidates.append(now + IDLE_TIMEOUT_SECONDS)
        return timedelta(seconds=max(0.0, min(candidates) - now))

    def _is_idle(self) -> bool:
        return (
            workflow.now().timestamp()
            >= self._state.last_activity_epoch + IDLE_TIMEOUT_SECONDS
        )

    def _touch(self) -> None:
        self._state.last_activity_epoch = workflow.now().timestamp()

    def _expire_overdue(self) -> None:
        now = workflow.now().timestamp()
        for op in self._state.operations.values():
            if (
                op.status == OperationStatus.WAITING_FOR_APPROVAL
                and op.deadline_epoch is not None
                and now >= op.deadline_epoch
            ):
                op.status = OperationStatus.EXPIRED
                op.decided_iso = workflow.now().isoformat()
                self._log("expired", op)
                self._propagate_child_terminal(op)

    async def _process_approved(self) -> None:
        approved_ids = [
            op.operation_id
            for op in self._state.operations.values()
            if op.status == OperationStatus.APPROVED
        ]
        for op_id in approved_ids:
            async with self._lock:
                op = self._state.operations[op_id]
                if op.status != OperationStatus.APPROVED:
                    continue
                op.status = OperationStatus.INVOKING
                self._log("invoking", op)
            await self._invoke(op)
            if op.parent_operation_id:
                parent = self._state.operations.get(op.parent_operation_id)
                if parent is not None and parent.controlled_tool:
                    await self._resume_tool1(parent, op)
            self._touch()

    def _needs_callback(self, op: Operation) -> bool:
        return (
            bool(op.callback_workflow_id)
            and not op.callback_notified
            and op.status in TERMINAL
        )

    def _settles_origin(self, op: Operation) -> Operation | None:
        """The parked operation this one has just settled, if any.

        Returns the pipeline operation waiting on op, but only once op itself is
        terminal and only while the pipeline operation is still parked, so a
        second sweep over the same pair finds nothing to do.
        """
        if not op.origin_operation_id or op.status not in TERMINAL:
            return None
        origin = self._state.operations.get(op.origin_operation_id)
        if origin is None or origin.status != OperationStatus.WAITING_FOR_DEPENDENCY:
            return None
        return origin

    async def _settle_handoffs(self) -> None:
        """Close the loop with anything that handed work to, or through, the scan.

        Two separate obligations, both driven from the main loop rather than from
        the Signal handlers that create them, because handlers here only mutate
        state and never drive Activities:

        1. Deliver terminal results back to a durable external Tool1 that is
           suspended waiting for one. This is the other half of the pause: the
           scan genuinely stopped executing, and this is what starts it again.
        2. Settle the release pipeline operation that handed off to the scan, once
           the promotion the scan requested has actually resolved.

        Both are idempotent, and both are driven off durable operation state
        rather than an in-memory queue, so a Worker restart mid-notification
        picks the work back up instead of dropping it.
        """
        for op_id in list(self._state.operations):
            # Re-fetched rather than iterated directly: this awaits Activities,
            # so handlers can run between passes and the map can change.
            op = self._state.operations.get(op_id)
            if op is None:
                continue
            if self._needs_callback(op):
                # Marked before the call, not after. The Activity retries hard on
                # its own, and a duplicate Signal is harmless (the scan ignores a
                # resolution it has already taken), but a loop that re-queued the
                # same notification on every pass would not be.
                op.callback_notified = True
                try:
                    await workflow.execute_activity(
                        signal_operation_callback,
                        SignalOperationCallbackInput(
                            callback_workflow_id=op.callback_workflow_id,
                            operation_id=op.operation_id,
                            status=op.status.value,
                            result=op.result,
                            reason=op.error,
                        ),
                        start_to_close_timeout=CALLBACK_TIMEOUT,
                        retry_policy=RetryPolicy(maximum_attempts=5),
                    )
                    self._log(
                        "controlled_caller_notified",
                        op,
                        {
                            "callback_workflow_id": op.callback_workflow_id,
                            "status": op.status.value,
                        },
                    )
                except ActivityError as err:
                    # The decision is recorded and production already reflects
                    # it. What failed is telling the other system, so say so
                    # loudly in the ledger rather than pretending it landed.
                    self._log(
                        "controlled_caller_unreachable",
                        op,
                        {
                            "callback_workflow_id": op.callback_workflow_id,
                            "error": str(err),
                        },
                    )

            origin = self._settles_origin(op)
            if origin is not None:
                self._settle_origin(origin, op)

    def _settle_origin(self, origin: Operation, op: Operation) -> None:
        """Finish the pipeline operation that handed off to the security scan."""
        origin.checkpoint = {
            **origin.checkpoint,
            "stage": "security_scan_completed",
            "scan_operation_id": op.operation_id,
        }
        if op.status == OperationStatus.COMPLETED:
            origin.status = OperationStatus.COMPLETED
            origin.result = {
                "pipeline_result": origin.checkpoint.get("tool1_prepare_result"),
                "scan_result": op.result,
                "resumed_from_checkpoint": True,
            }
        else:
            origin.status = op.status
            origin.error = op.error or (
                f"The security scan ended {op.status.value} before the promotion."
            )
        origin.decided_iso = workflow.now().isoformat()
        self._log(
            "security_scan_pipeline_settled",
            origin,
            {
                "scan_operation_id": op.operation_id,
                "status": origin.status.value,
            },
        )

    async def _invoke(self, op: Operation) -> None:
        try:
            result = await self._execute_tool_step(
                tool_name=op.tool_name,
                arguments=op.arguments,
                idempotency_key=op.idempotency_key,
            )
            # A promotion whose new instances failed their health check reports
            # itself as a completed workflow with a failed status rather than by
            # throwing, because nothing went wrong with the promotion machinery:
            # it correctly declined to route traffic. The operation still has to
            # come out failed, or an approved production promotion that never
            # actually went live would be recorded as having done so.
            if isinstance(result, dict) and result.get("status") == "failed":
                op.status = OperationStatus.FAILED
                op.result = result
                op.error = str(
                    result.get("reason") or result.get("message") or "step failed"
                )
                op.decided_iso = workflow.now().isoformat()
                self._log("failed", op, {"error": op.error})
                self._propagate_child_terminal(op)
                return
            op.status = OperationStatus.COMPLETED
            op.result = result
            op.decided_iso = workflow.now().isoformat()
            self._log("completed", op)
        except TOOL_STEP_ERRORS as err:
            op.status = OperationStatus.FAILED
            op.error = str(err)
            op.decided_iso = workflow.now().isoformat()
            self._log("failed", op, {"error": str(err)})
            self._propagate_child_terminal(op)

    async def _execute_tool_step(
        self, tool_name: str, arguments: dict, idempotency_key: str
    ) -> dict:
        """Run one tool step, whatever shape that step happens to have.

        Every caller in this file goes through here, so cutting a release means
        the same thing whether CASE-1's operator asked for it directly or the
        CASE-2a pipeline did it as part of a longer sequence.

        Two shapes:

        - cut_release and promote_release are Child Workflows. Each is several
          real steps -- tag, archive, hash; deploy, health check, route, note --
          and running them as children gives each its own Event History, its own
          independently retryable steps, and its own id in the Web UI. Same
          namespace, same task queue, same team: this is depth, not a boundary.
        - everything else is a single Activity against the deployment backend,
          which is all those tools are.

        Both are awaited the same way, and the result is normalized to a dict so
        the operation record, the ledger, and the dashboard do not have to know
        or care which shape ran.
        """
        if tool_name == "cut_release":
            cut = await workflow.execute_child_workflow(
                CutReleaseChildWorkflow.run,
                CutReleaseInput(
                    service=str(arguments.get("service", "")),
                    version=str(arguments.get("version", "")),
                    idempotency_key=idempotency_key,
                ),
                id=self._child_id("cut-release", arguments),
                execution_timeout=RELEASE_CHILD_TIMEOUT,
            )
            return dataclasses.asdict(cut)

        if tool_name == "promote_release":
            promotion = await workflow.execute_child_workflow(
                PromoteReleaseChildWorkflow.run,
                PromoteReleaseInput(
                    service=str(arguments.get("service", "")),
                    version=str(arguments.get("version", "")),
                    environment=str(arguments.get("environment", "")),
                    idempotency_key=idempotency_key,
                ),
                id=self._child_id("promote-release", arguments),
                execution_timeout=RELEASE_CHILD_TIMEOUT,
            )
            return dataclasses.asdict(promotion)

        return await workflow.execute_activity(
            invoke_tool,
            InvokeToolInput(
                tool_name=tool_name,
                arguments=arguments,
                idempotency_key=idempotency_key,
            ),
            start_to_close_timeout=INVOKE_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

    @staticmethod
    def _child_id(prefix: str, arguments: dict) -> str:
        """A legible, unique id for a release Child Workflow.

        Named after what it is doing so the Web UI's workflow list reads as a
        release history rather than a column of hashes, and suffixed with a
        workflow-safe random value so a genuine re-run (an explicit replay after
        an approval) starts its own execution instead of colliding with the one
        that already ran. Repeating the underlying mutation is prevented by the
        idempotency key the child threads down to the deployment backend, not by
        the id.
        """
        parts = [
            prefix,
            str(arguments.get("service", "")),
            str(arguments.get("version", "")),
        ]
        environment = str(arguments.get("environment", ""))
        if environment:
            parts.append(environment)
        parts.append(workflow.uuid4().hex[:8])
        return "::".join(parts)

    async def _orchestrate_tool1(
        self,
        parent: Operation,
        bump: str,
        idempotency_key: str,
        pass_label: str = "prepare",
        expect_version: str = "",
        allow_scan: bool = False,
    ) -> tuple[dict, str]:
        """Run the release pipeline up to the target promotion, and report it.

        This is the CASE-2 story: one instruction ("deploy the next minor
        version") turns into the team's whole release pipeline, with no further
        prompting. The pipeline is fixed, which is the point of it existing at
        all. An LLM holding these tools could sequence the happy path itself; what
        it cannot do is guarantee the sequence, and in particular it cannot be
        trusted to enforce "never promote to production without green quality
        gates on staging" when a caller asks it to hurry.

            read the deployed production version
            compute the next version from the bump type
            cut that release                       (CutReleaseChildWorkflow)
            promote it to staging                  (PromoteReleaseChildWorkflow;
                                                    skipped for a staging target)
            wait for THAT candidate's gate verdict (skipped for a staging target)
            look for a security scan provider      (allow_scan only, which needs
                                                    the security mandate to be on)
            -- caller either hands off to the scan, or creates the target
               promotion as the child operation --

        A staging target gets the first leg only: the staging promotion is then
        the child, and there is nothing to qualify a candidate for.

        Note what the gate step is and is not. The pipeline does not run the
        gates; reaching staging is what starts a gate run, whoever caused it,
        including a CASE-1 operator promoting by hand. What the pipeline does is
        wait for the verdict belonging to the candidate it just staged, and stop
        if it is not green. The policy is unchanged from when this ran the gates
        itself -- a candidate that fails never reaches the approval queue -- only
        the mechanism for obtaining the verdict is.

        allow_scan is set only on the first pass of a controlled, bump-driven run
        made while the security mandate is in effect. The mandate is what puts a
        scan in the pipeline's path at all, so with it off this stops at the
        production promotion and no scan lookup happens. A replay is finishing an
        approval that was already granted against a promotion that already exists,
        so there is nothing left to hand off; and an uncontrolled caller cannot be
        parked waiting on another system's verdict, since not being able to wait is
        what makes it uncontrolled.

        Every step is an ordinary Activity, so the work is in Event History and a
        Worker or gateway restart resumes rather than starting over. That matters
        far more now than when this only cut a release: the gate run is minutes of
        work sitting in the checkpoint while a human decides.

        Returns the aggregate result to checkpoint, plus the version it resolved.
        Raises InvalidVersionError if the deployed version or bump type cannot be
        interpreted, and QualityGateFailure if the gates do not pass. The caller
        turns both into a failed operation with no child, so a candidate that
        failed its gates never reaches the approval queue at all.

        Idempotency keys are split by what the step does, not by which pass it is.
        The mutations (the cut, the staging promotion) are keyed once, so a replay
        finds them already done and does not repeat them. The read and the quality
        gates are keyed per pass, so a replay re-reads production and re-verifies
        rather than trusting a verdict from before the pause. expect_version is set
        on a replay: if re-reading production yields a different next version than
        the one already approved, this raises PipelineVersionDrift before touching
        anything.

        Waiting for each step is just awaiting the Activity. There is no async
        handle to poll at this layer: the sync/async conversion is a gateway
        HTTP-boundary behavior (a client-side wait budget in gateway.server, see
        _TOOL_STRATEGY), not something a tool returns, and inside the Workflow the
        Activity await already is the durable wait for the real result.
        """
        kind = normalize_bump(bump)
        service = str(parent.arguments.get("service", ""))
        target = str(parent.arguments.get("environment", ""))

        # The bump is always computed from production, whatever the target. "The
        # next minor version" means the next one after what customers are running,
        # not the next one after whatever happens to be sitting on staging.
        deployed = await self._execute_tool_step(
            tool_name="get_deployed_version",
            arguments={"environment": PIPELINE_PRODUCTION_ENVIRONMENT},
            idempotency_key=f"{idempotency_key}:{pass_label}:deployed",
        )
        current = deployed.get("deployed_version")
        if not current:
            raise InvalidVersionError(
                f"get_deployed_version reported no deployment in "
                f"{PIPELINE_PRODUCTION_ENVIRONMENT!r}, so there is no version "
                "to bump"
            )
        resolved = next_version(str(current), kind)
        if expect_version and resolved != expect_version:
            raise PipelineVersionDrift(
                f"production is now on {current}, so a {kind} bump resolves to "
                f"{resolved}, but {expect_version} is the approved version. "
                "Start a new release rather than promoting an unapproved version."
            )
        self._log(
            "tool1_version_resolved",
            parent,
            {
                "source_environment": PIPELINE_PRODUCTION_ENVIRONMENT,
                "current_version": str(current),
                "bump": kind,
                "resolved_version": resolved,
                "target_environment": target,
            },
        )

        # Two orchestrations started concurrently under different idempotency
        # keys both read the same deployed version and so both compute the same
        # next one. Cutting it twice is safe (the second cut is a no-op that
        # returns the existing release), but each still opens its own promotion
        # for approval, and an approver would see two cards for the same
        # promotion. Deliberately out of scope here: the dedup that prevents this
        # for a repeated call is idempotency-key based, and collapsing two
        # distinct callers would need a per-(service, environment) lease, which is
        # a larger design decision than this change. The gateway's own dedup
        # already covers the case the demo exercises, a caller retrying.
        cut = await self._execute_tool_step(
            tool_name="cut_release",
            arguments={"service": service, "version": resolved},
            idempotency_key=f"{idempotency_key}:cut",
        )
        self._log(
            "tool1_release_cut",
            parent,
            {
                "version": resolved,
                "commit_sha": cut.get("commit_sha"),
                "artifact_ref": cut.get("artifact_ref"),
            },
        )

        result = {
            "orchestrated": True,
            "service": service,
            "environment": target,
            "bump": kind,
            "current_version": str(current),
            "version": resolved,
            "get_deployed_version_result": deployed,
            "cut_release_result": cut,
            "staging_promotion_result": None,
            "quality_gate_result": None,
            "next_tool": "promote_release",
        }

        if target == PIPELINE_STAGING_ENVIRONMENT:
            result["message"] = (
                f"Tool1 read {current} from production, computed {resolved} for "
                f"a {kind} bump, and cut it"
            )
            return result, resolved

        staging_arguments = {
            "service": service,
            "version": resolved,
            "environment": PIPELINE_STAGING_ENVIRONMENT,
        }
        # The pipeline's own staging promotion is still a governed action. Policy
        # is not consulted to decide the shape of the flow, but it is consulted
        # before the flow mutates an environment, so this cannot become a way to
        # reach a protected environment without an approval. The pipeline carries
        # exactly one approval gate, at its target, so a configuration that
        # protects staging too is refused rather than quietly executed.
        staging_decision = await workflow.execute_activity(
            evaluate_policy,
            EvaluatePolicyInput(
                tool_name="promote_release",
                arguments=staging_arguments,
                caller_principal=parent.requester,
            ),
            start_to_close_timeout=POLICY_TIMEOUT,
        )
        if staging_decision.requires_approval:
            raise PipelinePolicyConflict(
                f"policy protects {PIPELINE_STAGING_ENVIRONMENT}, which the "
                "release pipeline promotes to on the way to "
                f"{target}. The pipeline supports one approval gate, at its "
                "target. Promote through the single-step tools instead."
            )
        staged = await self._execute_tool_step(
            tool_name="promote_release",
            arguments=staging_arguments,
            idempotency_key=f"{idempotency_key}:staging",
        )
        if staged.get("status") == "failed":
            # The staging promotion declined to route traffic, so there is
            # nothing on staging to qualify and nothing to promote onward.
            raise QualityGateFailure(
                str(staged.get("reason") or staged.get("message"))
                or f"the staging promotion of {resolved} did not complete"
            )
        result["staging_promotion_result"] = staged
        self._log(
            "tool1_staged",
            parent,
            {
                "version": resolved,
                "environment": PIPELINE_STAGING_ENVIRONMENT,
                "previous_version": staged.get("previous_version"),
                "quality_gate_workflow_id": staged.get("quality_gate_workflow_id"),
            },
        )

        # The pipeline does not run the gates. Reaching staging is what starts a
        # gate run, wherever the promotion came from, and the pipeline's job here
        # is to wait for the verdict on the exact candidate it just staged and
        # then act on it.
        #
        # Waiting on that SPECIFIC run matters. Gate runs start on every staging
        # promotion by any route -- an operator doing a CASE-1 manual promotion
        # in the next tab starts one too -- so "the current gate state" is not a
        # safe thing to read. The promotion handed back the id of its own gate
        # workflow, and await_quality_gate refuses a verdict whose
        # (service, version) does not match, so a crossed or stale result stops
        # the pipeline rather than being promoted on.
        #
        # Deliberately not keyed by idempotency the way the mutations are: the
        # cut and the staging promotion must not be repeated on a replay, but a
        # verdict must not be trusted from before the pause, so a replay waits on
        # the gate run its own staging promotion started.
        gate_workflow_id = str(staged.get("quality_gate_workflow_id") or "")
        if not gate_workflow_id:
            raise QualityGateFailure(
                f"{resolved} reached {PIPELINE_STAGING_ENVIRONMENT} without a "
                "quality gate run, so the candidate is unqualified"
            )
        gates = await workflow.execute_activity(
            await_quality_gate,
            AwaitQualityGateInput(
                gate_workflow_id=gate_workflow_id,
                service=service,
                version=resolved,
            ),
            start_to_close_timeout=QUALITY_GATE_WAIT_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        result["quality_gate_result"] = gates
        self._log(
            "tool1_quality_gate_verdict",
            parent,
            {
                "version": resolved,
                "gate_workflow_id": gate_workflow_id,
                "status": gates.get("status"),
                "passed": gates.get("passed"),
            },
        )
        if not gates.get("passed"):
            failed = [
                str(check.get("check_name"))
                for check in (gates.get("checks") or [])
                if not check.get("passed")
            ]
            raise QualityGateFailure(
                f"quality gates failed for {resolved}"
                + (": " + ", ".join(failed) if failed else "")
            )

        # The candidate is qualified. Whether a scan stands between it and
        # production was decided before this pipeline started running, by the
        # mandate and the scanner switch, and arrived on the request -- see
        # _scan_required_for. allow_scan is that answer, already computed.
        #
        # So this is no longer a question about whether the step exists. It is a
        # check that the scanner is answering before the release is parked on it,
        # and an unreachable one stops the release rather than being treated as an
        # absent one. With allow_scan false -- no mandate, or the Security team on
        # their legacy script -- nothing is asked and the next line is the
        # production promotion, exactly as the pipeline behaved before the mandate.
        handoff = None
        if allow_scan:
            handoff = await self._scan_capability(
                parent, service, resolved, target, idempotency_key, pass_label
            )
        result["scan_handoff"] = handoff

        if handoff:
            result["message"] = (
                f"Tool1 read {current} from production, computed {resolved} for "
                f"a {kind} bump, cut it, promoted it to "
                f"{PIPELINE_STAGING_ENVIRONMENT}, passed quality gates, and "
                f"handed off to {handoff['provider']} for a pre-prod "
                f"security scan before the protected {target} promotion"
            )
            return result, resolved

        result["message"] = (
            f"Tool1 read {current} from production, computed {resolved} for a "
            f"{kind} bump, cut it, promoted it to "
            f"{PIPELINE_STAGING_ENVIRONMENT}, passed quality gates, and stopped "
            f"at the protected {target} promotion"
        )
        return result, resolved

    async def _scan_capability(
        self,
        parent: Operation,
        service: str,
        version: str,
        target: str,
        idempotency_key: str,
        pass_label: str,
    ) -> dict:
        """Check that the scanner we are about to hand off to is actually there.

        A health check, not a decision. Whether a scan happens was already settled
        before this is called -- by the mandate and the scanner switch, both of
        which are stated on the request -- and all this does is confirm the
        Security team's platform is answering before the release is parked on it.

        It used to be the decision, and that was the bug. The pipeline asked a
        service registry "is anyone offering pre-prod scanning?" and treated no as
        "then there is no scan step", which meant a company-wide security mandate
        silently evaporated whenever the Security team's worker was down, restarting,
        or simply had not been started -- and the release reported success. A
        checkpoint that disappears when the thing behind it is unavailable is not a
        checkpoint. So: unavailable now stops the release, loudly, with an error
        that names what was unreachable.

        Keyed per pass, like the gate run and the production read, because it is a
        read of something that moves.

        Returns the provider on success. On failure it fails `parent` with the
        reason and raises SecurityScanUnavailable, so callers on both paths report
        the same thing and neither can fall through to promoting unscanned.
        """
        try:
            status = await self._execute_tool_step(
                tool_name=SCAN_CAPABILITY_TOOL,
                arguments={
                    "service": service,
                    "version": version,
                    "environment": target,
                },
                idempotency_key=f"{idempotency_key}:{pass_label}:scan-status",
            )
        except ActivityError as err:
            return self._scan_unavailable(
                parent,
                version,
                f"the capability registry could not be reached ({err})",
            )
        if not status.get("available"):
            return self._scan_unavailable(
                parent,
                version,
                str(
                    status.get("message")
                    or "no pre-prod security scan provider is currently registered"
                ),
            )
        # Just who is offering, and nothing about how they do it. Which stages
        # run, the severity threshold, and which versions it rejects all used to be
        # relayed through here, which meant the caller was telling the scan what
        # verdict to reach. They live behind the endpoint now.
        handoff = {"provider": str(status.get("provider", "security"))}
        self._log(
            "scan_capability_discovered",
            parent,
            {"version": version, **handoff},
        )
        return handoff

    def _scan_unavailable(
        self, parent: Operation, version: str, detail: str
    ) -> None:  # always raises
        """Stop the release because the required scan could not be reached.

        Fails loudly and says which of the two things went wrong, because the fix
        is different for each: the Security team's Temporal worker is not running,
        or the scanner switch is on Temporal when their platform is not up. Neither
        is a reason to promote to production anyway -- the mandate is in force, and
        an unreachable checkpoint is an unmet one.
        """
        parent.status = OperationStatus.FAILED
        parent.error = (
            f"The security mandate requires a pre-prod security scan for "
            f"{version or 'this release'} and the Security team's scanning "
            f"platform is not reachable: {detail}. The release stops here rather "
            "than promoting to production without a scan. Either start the "
            "Security team's worker, or switch the scanner to their legacy script "
            "and run it by hand."
        )
        parent.decided_iso = workflow.now().isoformat()
        self._log(
            "security_scan_unavailable",
            parent,
            {"version": version, "detail": detail},
        )
        raise SecurityScanUnavailable(parent.error)

    async def _hand_off_to_scan(
        self,
        parent: Operation,
        idempotency_key: str,
        handoff: dict,
        version: str,
    ) -> ToolCallResponse:
        """Pass a qualified release to the Security team, and park the pipeline.

        This is where CASE-2b stops being the same shape as CASE-2a. Up to here
        every step was the Waypoint team's own work, correctly modeled as ordinary
        Activities inside this workflow, because one team genuinely owns cutting
        and staging and gating a release. A pre-prod security scan is not that. It
        is another team's system, and what happens next is a real handoff to it.

        The handoff is a Nexus call, straight from workflow code, to an endpoint
        the Security team owns. Notice everything this workflow does not know: not
        their namespace, not their task queue, not their workflow type, not which
        stages a scan runs, and not what would make one fail. It knows an endpoint
        name and an operation contract, and they can change everything behind it
        without this file being edited.

        The operation is deliberately synchronous, returning as soon as the scan
        has started. An asynchronous operation awaited for the whole scan would
        read better right up until this workflow continued-as-new, which it does
        on Temporal's suggestion and will certainly do across an open-ended human
        approval. Nexus operation handles do not survive Continue-As-New; a
        workflow id does, because it is data. So the pipeline takes the id and
        parks on its own state, which is the same reason approval deadlines here
        are absolute timestamps rather than Timers.

        The pipeline operation is parked, not finished. Nothing else in this
        workflow is blocked meanwhile: the chain keeps taking tool calls and
        approvals for anything else the operator is doing.

        A scan that cannot be started stops the release. The alternative is
        promoting to production having skipped a checkpoint the pipeline just
        confirmed was in force, which is exactly the thing the gate machinery
        exists to refuse.
        """
        security = workflow.create_nexus_client(
            service=SecurityScanService, endpoint=SECURITY_ENDPOINT
        )
        try:
            opened = await security.execute_operation(
                SecurityScanService.start_security_scan,
                StartSecurityScanInput(
                    gateway_workflow_id=self._state.workflow_id,
                    origin_operation_id=parent.operation_id,
                    service=str(parent.arguments.get("service", "")),
                    version=version,
                    environment=str(parent.arguments.get("environment", "")),
                    idempotency_key=f"{idempotency_key}:security-scan",
                    requester=parent.requester or "",
                ),
                schedule_to_close_timeout=SCAN_HANDOFF_TIMEOUT,
            )
        except Exception as err:
            parent.status = OperationStatus.FAILED
            parent.error = (
                f"Could not start a pre-prod security scan with "
                f"{handoff['provider']}: {err}. The release stops here rather "
                "than promoting to production without one."
            )
            parent.decided_iso = workflow.now().isoformat()
            self._log("security_scan_handoff_failed", parent, {"error": str(err)})
            return self._response_for(parent)

        parent.status = OperationStatus.WAITING_FOR_DEPENDENCY
        parent.checkpoint = {
            **parent.checkpoint,
            "stage": "awaiting_security_scan",
            "scan_provider": handoff["provider"],
            "scan_endpoint": SECURITY_ENDPOINT,
            "scan_workflow_id": opened.scan_workflow_id,
        }
        self._log(
            "security_scan_handoff",
            parent,
            {
                "version": version,
                "provider": handoff["provider"],
                "endpoint": SECURITY_ENDPOINT,
                "scan_workflow_id": opened.scan_workflow_id,
            },
        )
        response = self._response_for(parent)
        response.message = (
            f"Quality gates passed. {handoff['provider']} is running a "
            f"pre-prod security scan for {version}; the production promotion "
            "will be requested for approval once the scan clears."
        )
        return response

    @staticmethod
    def _promotion_sentence(arguments: dict) -> str:
        """The approver-facing sentence for a promotion resolved in-Workflow.

        The gateway builds this sentence for every other call path, but it cannot
        build this one: the version does not exist until Tool1 computes it. The
        wording is kept identical to gateway.server._requested_action so the two
        paths read the same on the approval card. That helper is CASE-1 and CASE-3
        surface, so it is duplicated here rather than moved and shared.
        """
        return (
            f"Promote {arguments.get('service', '?')} "
            f"{arguments.get('version', '?')} to "
            f"{arguments.get('environment', '?')}"
        )

    async def _resume_tool1(
        self, parent: Operation, child: Operation, replay: bool = False
    ) -> None:
        if child.status != OperationStatus.COMPLETED:
            self._propagate_child_terminal(child)
            return
        try:
            parent.status = OperationStatus.INVOKING
            self._log(
                "tool1_resuming",
                parent,
                {
                    "child_operation_id": child.operation_id,
                    "replay": replay,
                },
            )
            resume_result = await self._execute_tool_step(
                tool_name=f"{parent.tool_name}_resume",
                arguments={
                    **parent.arguments,
                    "nested_result": child.result,
                    "replay": replay,
                },
                idempotency_key=f"{parent.idempotency_key}:resume",
            )
            parent.status = OperationStatus.COMPLETED
            parent.result = {
                "tool1_result": resume_result,
                "tool2_result": child.result,
                "resumed_from_checkpoint": True,
                "replayed": replay,
            }
            parent.checkpoint = {
                **parent.checkpoint,
                "stage": "completed",
                "nested_operation_id": child.operation_id,
            }
            parent.decided_iso = workflow.now().isoformat()
            self._log("tool1_completed", parent)
        except TOOL_STEP_ERRORS as err:
            parent.status = OperationStatus.FAILED
            parent.error = str(err)
            parent.decided_iso = workflow.now().isoformat()
            self._log("tool1_resume_failed", parent, {"error": str(err)})

    def _propagate_child_terminal(self, child: Operation) -> None:
        if not child.parent_operation_id:
            return
        parent = self._state.operations.get(child.parent_operation_id)
        if parent is None or parent.status == OperationStatus.COMPLETED:
            return
        if child.status == OperationStatus.REJECTED:
            parent.status = OperationStatus.REJECTED
            parent.error = child.error or "Nested action was rejected."
        elif child.status == OperationStatus.EXPIRED:
            parent.status = OperationStatus.EXPIRED
            parent.error = "Nested approval timed out."
        elif child.status == OperationStatus.CANCELED:
            parent.status = OperationStatus.CANCELED
            parent.error = child.error or "Nested action was canceled."
        elif child.status == OperationStatus.FAILED:
            parent.status = OperationStatus.FAILED
            parent.error = child.error or "Nested action failed."
        else:
            return
        parent.decided_iso = workflow.now().isoformat()
        self._log(
            "nested_terminal_propagated",
            parent,
            {
                "child_operation_id": child.operation_id,
                "child_status": child.status.value,
            },
        )

    async def _do_continue_as_new(self) -> None:
        # Continue-As-New must be called from the main method, never a handler.
        # Drain in-flight Update and Signal handlers first so their work is not
        # interrupted by the transition.
        await workflow.wait_condition(workflow.all_handlers_finished)
        self._prune_terminal()
        self._log("continue_as_new", None, {"operations": len(self._state.operations)})
        workflow.continue_as_new(
            ChainInput(workflow_id=self._state.workflow_id, state=self._state)
        )

    def _prune_terminal(self) -> None:
        required_parent_ids = {
            op.parent_operation_id
            for op in self._state.operations.values()
            if op.status in PENDING and op.parent_operation_id
        }
        terminal_ids = [
            op_id
            for op_id, op in self._state.operations.items()
            if op.status in TERMINAL and op_id not in required_parent_ids
        ]
        excess = len(terminal_ids) - TERMINAL_RETENTION
        if excess > 0:
            for op_id in terminal_ids[:excess]:
                op = self._state.operations.pop(op_id)
                self._state.idempotency_index.pop(op.idempotency_key, None)
        if len(self._state.ledger) > LEDGER_RETENTION:
            self._state.ledger = self._state.ledger[-LEDGER_RETENTION:]

    def _has_pending_work(self) -> bool:
        return any(op.status in PENDING for op in self._state.operations.values())

    # -------------------------------------------------------------- projections

    def _new_operation(self, req: ToolCallRequest) -> Operation:
        self._state.op_seq += 1
        op_id = req.operation_id or ("op-" + workflow.uuid4().hex[:6])
        return Operation(
            operation_id=op_id,
            tool_name=req.tool_name,
            arguments=req.arguments,
            idempotency_key=req.idempotency_key,
            status=OperationStatus.EVALUATING,
            requester=req.correlation.caller_principal,
            requested_action=req.requested_action,
            justification=req.justification,
            approval_timeout_seconds=req.approval_timeout_seconds,
            created_iso=workflow.now().isoformat(),
            call_path=req.correlation.call_path
            or [req.correlation.runtime, req.tool_name],
            safe_arguments=req.safe_arguments or req.arguments,
            workflow_id_source=req.correlation.workflow_id_source,
            callback_workflow_id=req.callback_workflow_id,
            # A single-step call has no Tool1 above it, so the only thing that
            # can restrict it is the mandate. Before the mandate lands this is
            # None for every CASE-1 call, which is exactly how CASE-1 behaved
            # when it was demoed.
            required_approver_team=_required_approver_team_for(
                req.correlation.caller_service,
                str(req.arguments.get("environment", "")),
                req.security_mandate,
            ),
        )

    def _new_nested_parent(self, req: NestedToolCallRequest) -> Operation:
        self._state.op_seq += 1
        return Operation(
            operation_id=req.parent_operation_id
            or ("op-" + workflow.uuid4().hex[:6]),
            tool_name=req.tool1_name,
            arguments=req.tool1_arguments,
            idempotency_key=req.idempotency_key,
            status=OperationStatus.EVALUATING,
            requester=req.correlation.caller_principal,
            requested_action=f"Run {req.tool1_name}",
            justification=req.justification,
            approval_timeout_seconds=req.approval_timeout_seconds,
            created_iso=workflow.now().isoformat(),
            call_path=req.correlation.call_path
            or [req.correlation.runtime, req.tool1_name],
            safe_arguments=req.safe_tool1_arguments or req.tool1_arguments,
            workflow_id_source=req.correlation.workflow_id_source,
            controlled_tool=req.controlled_tool1,
            replay_safe=req.replay_safe,
            # Carried on the parent only. The parent is what completes once the
            # whole nested unit is done, so it is the right thing to key the
            # waiting pipeline operation off.
            origin_operation_id=req.origin_operation_id,
        )

    def _new_nested_child(
        self,
        req: NestedToolCallRequest,
        parent: Operation,
        arguments: dict | None = None,
        requested_action: str | None = None,
    ) -> Operation:
        """Create the Tool2 operation.

        arguments and requested_action override the request only on the
        orchestrated path, where Tool1 resolved the version the caller did not
        supply. Both default to None, so an explicit-version call is created from
        exactly what the gateway sent.
        """
        self._state.op_seq += 1
        call_path = list(parent.call_path)
        if not call_path or call_path[-1] != req.tool1_name:
            call_path.append(req.tool1_name)
        call_path.append(req.tool2_name)
        tool2_arguments = (
            arguments if arguments is not None else req.tool2_arguments
        )
        safe_tool2_arguments = req.safe_tool2_arguments or req.tool2_arguments
        if arguments is not None:
            # Keep the redacted projection in step with the resolved arguments,
            # or the approval card and the ledger would show a promotion with no
            # version while the operation carries one.
            safe_tool2_arguments = {**safe_tool2_arguments, **arguments}
        return Operation(
            operation_id=req.nested_operation_id
            or ("op-" + workflow.uuid4().hex[:6]),
            tool_name=req.tool2_name,
            arguments=tool2_arguments,
            idempotency_key=f"{req.idempotency_key}:nested",
            status=OperationStatus.EVALUATING,
            requester=req.correlation.caller_principal,
            requested_action=(
                requested_action
                if requested_action is not None
                else req.requested_action
            ),
            justification=req.justification,
            approval_timeout_seconds=req.approval_timeout_seconds,
            created_iso=workflow.now().isoformat(),
            parent_operation_id=parent.operation_id,
            call_path=call_path,
            safe_arguments=safe_tool2_arguments,
            workflow_id_source=req.correlation.workflow_id_source,
            controlled_tool=req.controlled_tool1,
            replay_safe=req.replay_safe,
            # The child is the operation the external Tool1 was told to wait on,
            # so its terminal status is what gets delivered back.
            callback_workflow_id=req.callback_workflow_id,
            # Which team's check gated this promotion decides who is allowed to
            # approve it. A promotion the Security team's scan asked for may only
            # be approved by the Security team; a pipeline the Waypoint team ran
            # for itself carries no restriction and stays approvable by anyone in
            # GATEWAY_APPROVERS -- unless the security mandate is in force, which
            # makes every production promotion Security's call whoever asked.
            #
            # The environment comes from Tool2's own arguments, because Tool2 is
            # the operation being restricted: the pipeline's earlier staging
            # promotion is not a mandate-covered action and never appears here.
            required_approver_team=_required_approver_team_for(
                req.correlation.caller_service,
                str(tool2_arguments.get("environment", "")),
                req.security_mandate,
            ),
        )

    def _response_for(self, op: Operation) -> ToolCallResponse:
        wf_id = self._state.workflow_id
        if op.status == OperationStatus.WAITING_FOR_APPROVAL:
            return ToolCallResponse(
                status="waiting_for_approval",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason="approval_required",
                message="Approval is required before this tool call can continue.",
                poll_after_seconds=self._state.poll_after_seconds,
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status in (
            OperationStatus.EVALUATING,
            OperationStatus.APPROVED,
            OperationStatus.WAITING_FOR_DEPENDENCY,
            OperationStatus.INVOKING,
        ):
            return ToolCallResponse(
                status="processing",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                message="The tool call is still running.",
                poll_after_seconds=self._state.poll_after_seconds,
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.APPROVED_AWAITING_RETRY:
            return ToolCallResponse(
                status="approved_retry_required",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason="upstream_context_not_suspended",
                message=(
                    "Approval was granted, but Tool1 was not suspension-aware. "
                    "Retry explicitly; execution remains blocked until then."
                ),
                poll_after_seconds=self._state.poll_after_seconds,
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
                retry_required=True,
            )
        if op.status == OperationStatus.COMPLETED:
            return ToolCallResponse(
                status="completed",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                result=op.result,
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.REJECTED:
            return ToolCallResponse(
                status="rejected",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason=op.error,
                message="The approver rejected this operation.",
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.EXPIRED:
            return ToolCallResponse(
                status="expired",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason="approval_timeout",
                message="Approval timed out; the downstream tool was not invoked.",
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.CANCELED:
            return ToolCallResponse(
                status="canceled",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason=op.error,
                message="The operation was canceled before completion.",
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
            )
        if op.status == OperationStatus.BLOCKED:
            return ToolCallResponse(
                status="blocked_nested_approval",
                workflow_id=wf_id,
                operation_id=op.operation_id,
                reason=op.error,
                message=(
                    "A nested approval was reached in a Tool1 that cannot suspend."
                ),
                parent_operation_id=op.parent_operation_id,
                call_path=op.call_path,
                retry_required=True,
            )
        return ToolCallResponse(
            status="failed",
            workflow_id=wf_id,
            operation_id=op.operation_id,
            reason=op.error,
            # The operation's own error, when it has one, rather than a canned
            # sentence. The caller here is usually an agent, and "The downstream
            # tool invocation failed." tells it nothing it can act on -- it reads
            # as transient, so the agent retries, and retries the same way. A
            # release stopped because a mandated security scan was unreachable
            # needs to say exactly that, and say what would fix it, or the agent
            # will keep trying to get to production by other means.
            message=op.error or "The downstream tool invocation failed.",
            parent_operation_id=op.parent_operation_id,
            call_path=op.call_path,
        )

    def _nested_response(
        self, parent: Operation, child: Operation | None
    ) -> ToolCallResponse:
        if child is None:
            return self._response_for(parent)
        if child.status == OperationStatus.APPROVED_AWAITING_RETRY:
            response = self._response_for(child)
            if not parent.replay_safe:
                response.reason = "uncontrolled_tool_not_replay_safe"
                response.message = (
                    "Approval is recorded, but Tool1 did not advertise replay "
                    "safety. Agent Gateway will not execute Tool2 automatically."
                )
            return response
        if parent.status == OperationStatus.BLOCKED:
            return ToolCallResponse(
                status="blocked_nested_approval",
                workflow_id=self._state.workflow_id,
                operation_id=child.operation_id,
                parent_operation_id=parent.operation_id,
                reason="uncontrolled_tool_requires_retry",
                message=(
                    "Tool2 requires approval, but Tool1 is not suspension-aware. "
                    "The protected action remains blocked and the original call "
                    "must be retried after approval."
                ),
                poll_after_seconds=self._state.poll_after_seconds,
                call_path=child.call_path,
                retry_required=True,
            )
        if parent.status == OperationStatus.COMPLETED:
            return self._response_for(parent)
        return self._response_for(child)

    def _summary(self) -> ChainSummary:
        counts: dict[str, int] = {}
        views: list[OperationView] = []
        for op in self._state.operations.values():
            counts[op.status.value] = counts.get(op.status.value, 0) + 1
            views.append(
                OperationView(
                    operation_id=op.operation_id,
                    tool_name=op.tool_name,
                    status=op.status.value,
                    requester=op.requester,
                    requested_action=op.requested_action,
                    arguments=op.safe_arguments,
                    justification=op.justification,
                    risk_reason=op.risk_reason,
                    protected=op.protected,
                    created_iso=op.created_iso,
                    approval_timeout_seconds=op.approval_timeout_seconds,
                    deadline_epoch=op.deadline_epoch,
                    deadline_iso=op.deadline_iso,
                    approver=op.approver,
                    decided_iso=op.decided_iso,
                    decision_reason=op.error,
                    result=op.result,
                    parent_operation_id=op.parent_operation_id,
                    child_operation_id=op.child_operation_id,
                    call_path=op.call_path,
                    workflow_id_source=op.workflow_id_source,
                    controlled_tool=op.controlled_tool,
                    replay_safe=op.replay_safe,
                    checkpoint=op.checkpoint,
                    required_approver_team=op.required_approver_team,
                )
            )
        return ChainSummary(
            workflow_id=self._state.workflow_id,
            run_id=workflow.info().run_id,
            total_operations=len(self._state.operations),
            status_counts=counts,
            operations=views,
            closing=self._closing,
        )

    def _validate_owner(self, caller_principal: str | None) -> None:
        if (
            self._state.owner_principal
            and caller_principal != self._state.owner_principal
        ):
            raise PermissionError("workflow_id is owned by a different principal")

    def _log(self, event: str, op, detail: dict | None = None) -> None:
        entry = LedgerEntry(
            ts=workflow.now().isoformat(),
            event=event,
            operation_id=op.operation_id if op is not None else None,
            detail=detail or {},
        )
        self._state.ledger.append(entry)
        workflow.logger.info(
            "ledger event",
            extra={"event": event, "operation_id": entry.operation_id},
        )

    @staticmethod
    def _iso(epoch: float) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
