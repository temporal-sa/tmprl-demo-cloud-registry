"""Agent Gateway's view of the Nexus boundary with the Security team.

Two services cross this boundary, in opposite directions:

  AgentGatewayService    handled here, called by the Security team. How another
                         team's tool asks the gateway to perform a protected
                         action, and how it learns what a human decided.
  SecurityScanService    handled by the Security team, called from here. How the
                         release pipeline starts a pre-prod security scan.

What is deliberately absent from this file is as important as what is in it:
there is no namespace, no task queue, and no workflow type name belonging to the
other team. A Nexus Endpoint is the only thing either side addresses, and the
team that owns an endpoint can re-point it, rename their workflows, or move
namespaces without anyone on the other side editing code.

security_scan/nexus_contracts.py declares the mirror image of this file. The
two are duplicated on purpose -- they are the published interface between two
services, which is exactly the thing that is legitimate to state twice, unlike
an internal request struct copied field by field.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import nexusrpc

# Endpoint names, registered with the cluster at startup. These, plus the
# operation names below, are the entire integration surface.
#
# Both are prefixed with the project name because Temporal Cloud scopes Endpoint
# names to the whole ACCOUNT, not to a namespace: a bare "security" would claim
# that name against every other demo in the account, and a leaked one blocks it
# for good. The prefix is the price of sharing an account.
#
# Literal constants, deliberately not read from the environment. An endpoint name
# is an attribute of the ScheduleNexusOperation command and is written into
# workflow state and the ledger (workflows/chain.py), so an env-driven value would
# put configuration into Event History -- and a rolling deploy holding two values
# would produce two endpoint names with no code change to blame. This is the same
# class of thing as a workflow type name, and it gets versioned and reviewed like
# one. Renaming either is a drain-first migration.
AGENT_GATEWAY_ENDPOINT = "agent-gateway-waypoint"
SECURITY_ENDPOINT = "agent-gateway-security"


# ------------------------------------------------- gateway's inbound service


@dataclass
class ProtectedActionRequest:
    """Another team's tool asking Agent Gateway to perform a governed action.

    gateway_workflow_id is the correlation context the requirements document
    calls for: the chain the operator started. It is a token the gateway handed
    out, not knowledge of gateway internals, and it is what keeps one
    user-visible task on one workflow_id across two systems.
    """

    gateway_workflow_id: str
    tool_name: str
    arguments: dict[str, Any]
    idempotency_key: str
    caller_service: str
    caller_principal: str
    caller_workflow_id: str
    origin_operation_id: str = ""
    justification: str = ""


@dataclass
class ProtectedActionOutcome:
    """What a human decided, and what happened as a result.

    Delivered as the Nexus operation's result. The caller does not poll for
    this and does not receive a Signal: it suspends on the operation and is
    resumed when the operation completes, however long that takes.
    """

    operation_id: str
    status: str
    result: Optional[Any] = None
    reason: Optional[str] = None


@dataclass
class ToolOutcomeReport:
    """A tool reporting that the action it was going to request is not coming.

    Needed because a checkpoint that fails never asks for anything, and the
    pipeline operation waiting on it would otherwise wait forever.
    """

    gateway_workflow_id: str
    origin_operation_id: str
    caller_workflow_id: str
    outcome: str
    reason: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolOutcomeAck:
    accepted: bool


@nexusrpc.service
class AgentGatewayService:
    """Handled by the Agent Gateway team, in their namespace."""

    request_protected_action: nexusrpc.Operation[
        ProtectedActionRequest, ProtectedActionOutcome
    ]
    report_tool_outcome: nexusrpc.Operation[ToolOutcomeReport, ToolOutcomeAck]


# ------------------------------------------- the Security team's service, ours
#                                              to call and theirs to change


@dataclass
class StartSecurityScanInput:
    """Ask the Security team to scan a staged release before it reaches prod.

    Note what this does not carry: which stages run, how long they take, or what
    severity threshold blocks a release. Those are the Security team's decisions
    about their own product, and the pipeline has no business relaying them.
    """

    gateway_workflow_id: str
    origin_operation_id: str
    service: str
    version: str
    environment: str
    idempotency_key: str
    requester: str


@dataclass
class SecurityScanStarted:
    """The handle the pipeline records so an operator can find the scan."""

    scan_workflow_id: str


@nexusrpc.service
class SecurityScanService:
    """Handled by the Security team, in their namespace."""

    start_security_scan: nexusrpc.Operation[
        StartSecurityScanInput, SecurityScanStarted
    ]
