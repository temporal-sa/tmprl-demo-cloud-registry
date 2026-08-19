from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


AGENT_GATEWAY_APPROVAL_SIGNAL = "agent_gateway_approval_resolved"
CALLBACK_WORKFLOW_ID_HEADER = "x-agent-gateway-callback-workflow-id"
CALLBACK_RUN_ID_HEADER = "x-agent-gateway-callback-run-id"
ADK_SESSION_ID_HEADER = "x-agent-gateway-adk-session-id"

# The Waypoint team's Temporal namespace: Agent Gateway, AgenticChainWorkflow, the
# release children, and the `agent-gateway` Nexus Endpoint all live here. Named
# after the team that operates it, exactly as SECURITY_NAMESPACE is in
# security_scan/models.py, so the Web UI's namespace selector reads as two teams
# rather than as one team and a leftover.
#
# It is the default for TEMPORAL_NAMESPACE everywhere rather than a bare
# Client.connect() -- the SDK's own fallback is "default", so a client that forgets
# to pass a namespace silently connects to an empty one and simply never receives
# tasks. Nothing errors; work just stops arriving. Every connect site in this repo
# passes a namespace explicitly for that reason.
WAYPOINT_NAMESPACE = "waypoint"

# How the Security team runs a pre-prod scan today. Two ways of doing the same
# job, and the distinction the whole of CASE-2 turns on:
#
#   legacy   -- the host script they have run for years. No Temporal, no Event
#               History, no worker. The gateway inserts no scan step of its own,
#               because there is nothing durable to insert; somebody runs the
#               script by hand and the script asks for the promotion itself.
#   platform -- SecurityScanWorkflow, in the Security team's own namespace,
#               reached over their Nexus Endpoint. The gateway routes every
#               protected promotion through it.
#
# Which one is current is a dashboard switch, not a deployed state. Both
# scanners exist and both are reachable at all times; the switch says which one
# the company is using. Inferring it from whether a container happened to be
# running made the demo's central claim depend on operator setup, and made a
# release quietly skip its scan when the answer came back "nobody is home".
SCAN_MODE_LEGACY = "legacy"
SCAN_MODE_PLATFORM = "platform"
SCAN_MODES = (SCAN_MODE_LEGACY, SCAN_MODE_PLATFORM)


class OperationStatus(str, Enum):
    """Lifecycle states for a single approval-gated operation."""

    EVALUATING = "evaluating"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    APPROVED = "approved"
    APPROVED_AWAITING_RETRY = "approved_awaiting_retry"
    WAITING_FOR_DEPENDENCY = "waiting_for_dependency"
    INVOKING = "invoking"
    COMPLETED = "completed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELED = "canceled"
    BLOCKED = "blocked"
    FAILED = "failed"


@dataclass
class CorrelationContext:
    """Carries enough context for the gateway to resolve the chain workflow_id.

    workflow_id_source records how the ID was resolved (explicit propagation is
    the preferred contract for controlled agents such as Claude Code; the gateway
    derives one from the MCP session when no explicit id is supplied).
    """

    workflow_id: str
    workflow_id_source: str = "explicit"
    operation_id: Optional[str] = None
    parent_operation_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    agent_session_id: Optional[str] = None
    agent_run_id: Optional[str] = None
    tool_call_id: Optional[str] = None
    parent_call_id: Optional[str] = None
    request_id: Optional[str] = None
    traceparent: Optional[str] = None
    caller_service: Optional[str] = None
    caller_principal: Optional[str] = None
    end_user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    operation_type: Optional[str] = None
    target_resource: Optional[str] = None
    runtime: str = "mcp"
    call_path: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AdkTemporalSessionCallback:
    """Durable callback target for an ADK session running in Temporal."""

    workflow_id: str
    run_id: Optional[str]
    session_id: str


@dataclass
class ApprovalResolution:
    """Terminal Agent Gateway result signaled back to an ADK session workflow."""

    gateway_workflow_id: str
    operation_id: str
    status: str
    adk_session_id: str
    agent_run_id: Optional[str] = None
    result: Optional[Any] = None
    reason: Optional[str] = None
    message: Optional[str] = None


@dataclass
class ToolCallRequest:
    """A single tool call delivered to the chain workflow as an Update."""

    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str
    correlation: CorrelationContext
    approval_timeout_seconds: int = 300
    # Human sentence describing what will happen, built by the gateway.
    requested_action: str = ""
    # Optional requester-supplied reason for the request, distinct from the
    # policy's risk reason.
    justification: Optional[str] = None
    # Gateway-supplied operation id, derived from the idempotency key so the
    # gateway knows it up front (needed when a call converts to async before the
    # workflow responds).
    operation_id: str = ""
    safe_arguments: dict[str, Any] = field(default_factory=dict)
    # Whether the company-wide security mandate was in effect when this call
    # arrived. Read from the gateway's runtime toggle at the trust boundary and
    # carried on the request, for the same reason approver_team is: it is
    # process state outside the Workflow, and Workflow code must not read it.
    # Carrying it makes the value part of Event History, so a replay reaches the
    # same answer even if the toggle has been flipped since.
    security_mandate: bool = False
    # How the Security team performs a scan right now: "legacy" for the host
    # script they have always run, "platform" for their Temporal workflow. Read
    # from the gateway's second toggle at the same boundary and carried for the
    # same reason as security_mandate.
    #
    # This is the only thing that decides whether the gateway inserts a scan step
    # of its own. In legacy mode it does not: there is nothing durable to hand
    # off to, so a human runs the script and the script asks for the promotion
    # itself. In platform mode it does, on every path to a protected environment
    # rather than only inside the orchestrated pipeline -- a company-wide mandate
    # an agent can step around by calling promote_release directly instead of
    # run_release_orchestration is not a mandate.
    scan_mode: str = SCAN_MODE_LEGACY
    # A workflow in this namespace that is waiting for the terminal result of
    # this direct call. The Google ADK autonomous workflow uses this to delegate
    # governance to AgenticChainWorkflow without polling or reimplementing policy.
    # Empty for ordinary Claude Code/MCP calls, whose caller polls the chain.
    callback_workflow_id: str = ""


@dataclass
class ToolCallResponse:
    """The structured response the gateway returns to the caller.

    A waiting response mirrors the requirements document shape: status,
    workflow_id, operation_id, reason, message, and poll_after_seconds.
    """

    status: str
    workflow_id: str
    operation_id: Optional[str] = None
    result: Optional[Any] = None
    reason: Optional[str] = None
    message: Optional[str] = None
    poll_after_seconds: Optional[int] = None
    parent_operation_id: Optional[str] = None
    call_path: list[str] = field(default_factory=list)
    retry_required: bool = False


@dataclass
class ApprovalDecision:
    """An approve or reject decision delivered to the chain workflow as a Signal.

    approver_team is resolved by the gateway from the validated bearer token at
    the same moment the approver identity is, and carried here rather than
    looked up in the Workflow, because team membership is environment-driven
    configuration and Workflow code must not read it. The Workflow's job is to
    enforce the match against Operation.required_approver_team, not to decide
    who is on which team.
    """

    operation_id: str
    approver: str
    reason: Optional[str] = None
    approver_team: str = ""


@dataclass
class CancelOperation:
    """A caller or operator cancellation delivered as a Signal."""

    operation_id: str
    canceled_by: str
    reason: Optional[str] = None


@dataclass
class NestedToolCallRequest:
    """A Tool1 call that discovers a nested Tool2 call through the gateway."""

    tool1_name: str
    tool1_arguments: dict[str, Any]
    tool2_name: str
    tool2_arguments: dict[str, Any]
    idempotency_key: str
    correlation: CorrelationContext
    approval_timeout_seconds: int = 300
    requested_action: str = ""
    justification: Optional[str] = None
    parent_operation_id: str = ""
    nested_operation_id: str = ""
    # Set for the orchestrated CASE-2 path: Tool1 reads the deployed version and
    # computes the version to cut and promote from this bump type, instead of the
    # caller naming a version up front. Empty means the caller supplied an
    # explicit version in tool1_arguments/tool2_arguments.
    bump: str = ""
    controlled_tool1: bool = True
    replay_safe: bool = False
    safe_tool1_arguments: dict[str, Any] = field(default_factory=dict)
    safe_tool2_arguments: dict[str, Any] = field(default_factory=dict)
    # Set when something is holding this request open and needs the terminal
    # result delivered rather than polled for. In CASE-2b that is
    # ProtectedActionWorkflow, which is servicing a Nexus operation another team
    # is suspended on. Empty for every caller that waits inline, which is every
    # CASE-2a caller.
    callback_workflow_id: str = ""
    # An operation elsewhere in this chain that is waiting on this nested call to
    # resolve. CASE-2b sets it to the release pipeline's own operation, which
    # handed off to the security scan and is parked until the promotion lands.
    origin_operation_id: str = ""
    # As on ToolCallRequest: the gateway's security-mandate toggle, snapshotted
    # at the boundary. Only the gateway's own MCP surface sets it; a nested call
    # that arrives over Nexus from the Security team already carries its own team
    # restriction through caller_service and does not need it.
    security_mandate: bool = False
    # As on ToolCallRequest: which of the Security team's two scanners is current.
    scan_mode: str = SCAN_MODE_LEGACY


@dataclass
class ResumeNestedRequest:
    """Explicit retry/resume request for an uncontrolled Tool1."""

    operation_id: str
    caller_principal: str


# --------------------------------------------------------------------------
# Serving other teams' tools (CASE-2b).
#
# Everything below is internal to the gateway. The cross-team boundary is a
# Nexus Endpoint and lives in common/nexus_contracts.py; by the time any of
# these types are in play, the call has already crossed it and is being handled
# by the gateway's own workflows in the gateway's own namespace.
#
# That split is the whole design. There used to be a set of types here that were
# hand-mirrored in security_scan/models.py, because both sides had to agree on
# an internal request struct. They do not any more.
# --------------------------------------------------------------------------


@dataclass
class SubmitNestedToolCallInput:
    """Input for the Activity that lodges an external tool's request.

    ProtectedActionWorkflow cannot issue an Update from workflow code, so it goes
    through this. Same namespace, same worker, same team: this is the gateway
    calling its own chain workflow, not a cross-cluster reach.
    """

    gateway_workflow_id: str
    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str
    caller_service: str
    caller_principal: str
    caller_workflow_id: str
    callback_workflow_id: str
    origin_operation_id: str = ""
    justification: str = ""


@dataclass
class GatewayOperationResolution:
    """A terminal operation, reported to whatever is holding the request open.

    Delivered as a Signal to ProtectedActionWorkflow, in this namespace. It is
    not a cross-team payload: the external caller never sees this, it sees the
    Nexus operation complete.
    """

    operation_id: str
    status: str
    result: Optional[Any] = None
    reason: Optional[str] = None


@dataclass
class SignalOperationCallbackInput:
    """Input for the Activity that delivers a resolution to a waiting workflow."""

    callback_workflow_id: str
    operation_id: str
    status: str
    result: Optional[Any] = None
    reason: Optional[str] = None


@dataclass
class ScanVerdict:
    """A checkpoint's verdict, signaled to the chain workflow.

    Only a failing verdict needs this: a clean scan reports itself by requesting
    the promotion. A failing one never asks for anything, so without it the
    pipeline operation parked on the handoff would wait forever for a request
    that is not coming.

    Raised into the chain by the gateway's own Nexus handler, not by the caller.
    """

    origin_operation_id: str
    scan_workflow_id: str
    verdict: str
    reason: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------
# The Waypoint team's own release steps (CASE-1 and CASE-2a).
#
# cut_release and promote_release are Child Workflows rather than single
# Activities, and the reason is not "organize code" or "reduce cost", both of
# which would be bad reasons. Each is a distinct unit of work with several
# genuinely separate steps, and running it as a child gives it its own Event
# History, its own independently retryable steps, and its own workflow id in
# the Web UI. That is what an operator actually wants to look at when a release
# stalls: which step, not which tool call.
#
# These are the OPPOSITE case from SecurityScanWorkflow. That one is a peer in
# another namespace because another QuickMeals team owns it. These are the
# Waypoint team's own tooling, invoked by the Waypoint team's own gateway
# workflow, in the same namespace on the same task queue. Same team, same
# infrastructure, more depth.
# --------------------------------------------------------------------------


@dataclass
class CutReleaseInput:
    service: str
    version: str
    commit_sha: str = ""
    # Threaded down to the sub-steps so a retried cut is deduplicated by the
    # deployment backend rather than repeated.
    idempotency_key: str = ""


@dataclass
class CutReleaseResult:
    service: str
    version: str
    commit_sha: str = ""
    tag: str = ""
    artifact_ref: str = ""
    artifact_bytes: int = 0
    sha256: str = ""
    message: str = ""


@dataclass
class TagCommitInput:
    service: str
    version: str
    commit_sha: str = ""


@dataclass
class ArchiveArtifactsInput:
    service: str
    version: str
    idempotency_key: str = ""


@dataclass
class CalculateHashesInput:
    service: str
    version: str
    artifact_ref: str


@dataclass
class PromoteReleaseInput:
    service: str
    version: str
    environment: str
    idempotency_key: str = ""


@dataclass
class PromoteReleaseResult:
    service: str
    version: str
    environment: str
    status: str = "completed"
    previous_version: Optional[str] = None
    instance_ids: list[str] = field(default_factory=list)
    reason: Optional[str] = None
    message: str = ""
    # Set only for a successful staging promotion. Whatever reached staging is
    # evaluated by a quality gate workflow started from inside the promotion,
    # and the caller needs its id to be able to wait on that specific
    # (service, version) verdict rather than reading whatever the gate card
    # happens to show. See QualityGateChildWorkflow.
    quality_gate_workflow_id: str = ""


@dataclass
class DeployBinariesInput:
    service: str
    version: str
    environment: str


@dataclass
class HealthCheckInput:
    service: str
    version: str
    environment: str
    instance_ids: list[str] = field(default_factory=list)


@dataclass
class UpdateRoutingInput:
    service: str
    version: str
    environment: str
    instance_ids: list[str] = field(default_factory=list)
    idempotency_key: str = ""


@dataclass
class UpdateReleaseNotesInput:
    service: str
    version: str
    environment: str


# --------------------------------------------------------------------------
# Quality gates (Section 4 of the demo-ability addendum).
#
# A gate run is not part of the release pipeline's own sequence any more. It is
# ambient infrastructure: whenever anything reaches staging, by any route, the
# candidate that just landed there gets evaluated. Closer to a CI system
# kicking off a test run on a push than to a pipeline step. CASE-1 benefits
# from it without invoking, sequencing, or waiting on it; CASE-2a's pipeline
# explicitly waits on the verdict for the exact version it staged.
# --------------------------------------------------------------------------

QUALITY_GATE_CHECKS = [
    "e2e_tests",
    "user_acceptance_tests",
    "performance_tests",
    "accessibility_tests",
]


@dataclass
class QualityGateInput:
    service: str
    version: str
    environment: str = "staging"
    # "pass" | "fail". No third variant: the four checks run concurrently, so
    # there is no meaningful "first stage versus later stage" distinction of the
    # kind the security scan's sequential stages have.
    scripted_outcome: str = "pass"


@dataclass
class QualityCheckInput:
    service: str
    version: str
    check_name: str
    scripted_outcome: str = "pass"


@dataclass
class QualityGateState:
    status: str = "running"
    service: str = ""
    version: str = ""
    environment: str = "staging"
    checks: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class QualityGateResult:
    service: str
    version: str
    status: str
    environment: str = "staging"
    checks: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PublishQualityGateInput:
    """Publish gate progress to the shared observability backend.

    Visibility only, exactly like the security scan's own publish step: the
    verdict does not depend on this landing, so a publish failure must never
    fail a gate run.
    """

    service: str
    version: str
    environment: str
    status: str
    checks_completed: int
    check_count: int
    checks: list[dict[str, Any]] = field(default_factory=list)
    gate_workflow_id: str = ""


@dataclass
class AwaitQualityGateInput:
    """Wait for one specific (service, version) gate workflow's verdict.

    The pipeline cannot read "whatever the gate card currently shows": a gate
    workflow starts on every staging promotion from any source, so the card may
    be showing an older candidate, or a newer one. This carries the workflow id
    the promotion handed back plus the pair it must be about, and the Activity
    refuses a verdict that does not match.
    """

    gate_workflow_id: str
    service: str
    version: str


@dataclass
class EvaluatePolicyInput:
    tool_name: str
    arguments: dict[str, Any]
    caller_principal: Optional[str] = None


@dataclass
class PolicyDecision:
    requires_approval: bool
    reason: Optional[str] = None


@dataclass
class InvokeToolInput:
    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str


@dataclass
class LedgerEntry:
    """One application-level audit entry stored in Workflow state.

    Temporal Event History remains the underlying execution record.
    """

    ts: str
    event: str
    operation_id: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class Operation:
    operation_id: str
    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str
    status: OperationStatus
    requester: Optional[str] = None
    requested_action: str = ""
    justification: Optional[str] = None
    risk_reason: Optional[str] = None
    protected: bool = False
    created_iso: str = ""
    approval_timeout_seconds: Optional[int] = None
    deadline_epoch: Optional[float] = None
    deadline_iso: Optional[str] = None
    approver: Optional[str] = None
    decided_iso: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    parent_operation_id: Optional[str] = None
    child_operation_id: Optional[str] = None
    call_path: list[str] = field(default_factory=list)
    safe_arguments: dict[str, Any] = field(default_factory=dict)
    workflow_id_source: str = ""
    controlled_tool: bool = True
    replay_safe: bool = False
    checkpoint: dict[str, Any] = field(default_factory=dict)
    # A workflow in this namespace holding a request open on someone's behalf,
    # to be signaled when this operation reaches a terminal state. See
    # NestedToolCallRequest.callback_workflow_id.
    callback_workflow_id: str = ""
    callback_notified: bool = False
    origin_operation_id: str = ""
    # The QuickMeals engineering team a human must belong to in order to approve
    # this operation, or None for no team restriction. Set at creation time from
    # which check gated the promotion: an Operation the Security team's scan
    # asked for may only be approved by the Security team. Everything else --
    # CASE-1's ordinary prod promotions, CASE-2a with no scan in play, CASE-3 --
    # leaves this None and is approvable by any principal in GATEWAY_APPROVERS,
    # exactly as before.
    required_approver_team: Optional[str] = None


@dataclass
class OperationView:
    """A query-friendly projection of an Operation, with everything an approver
    or an auditor needs to see for both the pending and the decided views."""

    operation_id: str
    tool_name: str
    status: str
    requester: Optional[str] = None
    requested_action: str = ""
    arguments: dict[str, Any] = field(default_factory=dict)
    justification: Optional[str] = None
    risk_reason: Optional[str] = None
    protected: bool = False
    created_iso: str = ""
    approval_timeout_seconds: Optional[int] = None
    deadline_epoch: Optional[float] = None
    deadline_iso: Optional[str] = None
    approver: Optional[str] = None
    decided_iso: Optional[str] = None
    decision_reason: Optional[str] = None
    result: Optional[Any] = None
    parent_operation_id: Optional[str] = None
    child_operation_id: Optional[str] = None
    call_path: list[str] = field(default_factory=list)
    workflow_id_source: str = ""
    controlled_tool: bool = True
    replay_safe: bool = False
    checkpoint: dict[str, Any] = field(default_factory=dict)
    # Surfaced so the approval queue can label an entry a non-Security approver
    # is not allowed to act on, rather than letting them find out by being
    # refused.
    required_approver_team: Optional[str] = None


@dataclass
class ChainState:
    """All chain state. This is what gets forwarded across Continue-As-New."""

    workflow_id: str
    owner_principal: str = ""
    operations: dict[str, Operation] = field(default_factory=dict)
    # idempotency_key -> operation_id. Carried across CAN because Update IDs are
    # scoped to a single Execution and reset after Continue-As-New.
    idempotency_index: dict[str, str] = field(default_factory=dict)
    ledger: list[LedgerEntry] = field(default_factory=list)
    op_seq: int = 0
    poll_after_seconds: int = 5
    # Absolute workflow time of the last session activity. Seeded on first run and
    # updated on each tool call and decision. Drives the idle timeout. Carried
    # across Continue-As-New.
    last_activity_epoch: float = 0.0


@dataclass
class ChainInput:
    """Workflow input. state is None on first start and populated on CAN."""

    workflow_id: str
    owner_principal: str = ""
    state: Optional[ChainState] = None


@dataclass
class ChainSummary:
    workflow_id: str
    run_id: str
    total_operations: int
    status_counts: dict[str, int]
    operations: list[OperationView]
    closing: bool = False


@dataclass
class AutonomousAgentInput:
    """Input for a durable Google ADK-style autonomous agent run."""

    workflow_id: str
    operation_id: str
    idempotency_key: str
    owner_principal: str
    agent_run_id: str
    agent_identity: str
    tool_name: str
    arguments: dict[str, Any]
    safe_arguments: dict[str, Any]
    requested_action: str
    justification: Optional[str]
    correlation: CorrelationContext
    approval_timeout_seconds: int = 300
    callback: Optional[AdkTemporalSessionCallback] = None
    # New CASE-3 runs carry the exact ToolCallRequest used by the Claude Code
    # direct-call path. Its presence is also the replay guard: histories created
    # before delegation leave this None and retain the original workflow command
    # sequence, while new runs submit it to a companion AgenticChainWorkflow.
    governed_request: Optional[ToolCallRequest] = None


@dataclass
class AdkSessionWorkflowInput:
    """Configuration for one long-lived Temporal-backed ADK session."""

    user_id: str = "user"
    session_id: str = ""
    model: str = "gemini-3.6-flash"


@dataclass
class AdkSessionTurnInput:
    """One user turn submitted to a running ADK session workflow."""

    turn_id: str
    prompt: str


@dataclass
class AdkSessionWorkflowResult:
    """Current or completed result for one turn in the ADK session."""

    workflow_id: str
    session_id: str
    turn_id: str = ""
    initial_response: str = ""
    resumed_response: Optional[str] = None
    approval: Optional[ApprovalResolution] = None
    complete: bool = False
    error: Optional[str] = None


@dataclass
class AgentRunSummary:
    workflow_id: str
    run_id: str
    total_operations: int
    status_counts: dict[str, int]
    operations: list[OperationView]
    agent_run_id: str
    agent_identity: str
    checkpoint: dict[str, Any] = field(default_factory=dict)
    dependent_action_executed: bool = False


@dataclass
class WorkflowStatusResponse:
    """Stable MCP output schema shared by chain and autonomous workflows."""

    workflow_id: str
    run_id: str
    total_operations: int
    status_counts: dict[str, int]
    operations: list[OperationView]
    closing: bool = False
    agent_run_id: Optional[str] = None
    agent_identity: Optional[str] = None
    checkpoint: dict[str, Any] = field(default_factory=dict)
    dependent_action_executed: bool = False


@dataclass
class WorkflowLedgerResponse:
    """Stable MCP output schema for the durable human-input ledger."""

    workflow_id: str
    entries: list[LedgerEntry]
