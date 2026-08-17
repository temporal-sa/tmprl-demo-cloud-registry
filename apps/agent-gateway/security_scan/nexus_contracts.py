"""The Security team's view of the Nexus boundary with Agent Gateway.

The mirror image of common/nexus_contracts.py, declared independently because
this team does not have Agent Gateway's Python package on its path and does not
want it. What is shared between the two teams is an interface -- endpoint names,
operation names, and payload field names -- and each side states it once.

That duplication is the legitimate kind. Copying another service's internal
request struct field by field is not the same thing, and this file replaces
exactly that.

Nothing here names a namespace, a task queue, or a workflow type on the other
side. Agent Gateway can restructure everything behind their endpoint without
this team noticing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import nexusrpc

# Prefixed because Temporal Cloud scopes Endpoint names to the account rather
# than the namespace, so a bare "security" would claim that name against every
# other team in the account. Literal constants rather than environment reads:
# workflow code passes these to Nexus calls, so the value lands in Event History
# and has to be a code fact. See the fuller note on the gateway's own copy.
AGENT_GATEWAY_ENDPOINT = "agent-gateway-waypoint"
SECURITY_ENDPOINT = "agent-gateway-security"


# ------------------------------------------ Agent Gateway's service, ours to
#                                             call and theirs to change


@dataclass
class ProtectedActionRequest:
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
    operation_id: str
    status: str
    result: Optional[Any] = None
    reason: Optional[str] = None


@dataclass
class ToolOutcomeReport:
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
    request_protected_action: nexusrpc.Operation[
        ProtectedActionRequest, ProtectedActionOutcome
    ]
    report_tool_outcome: nexusrpc.Operation[ToolOutcomeReport, ToolOutcomeAck]


# -------------------------------------------------- this team's own service


@dataclass
class StartSecurityScanInput:
    gateway_workflow_id: str
    origin_operation_id: str
    service: str
    version: str
    environment: str
    idempotency_key: str
    requester: str


@dataclass
class SecurityScanStarted:
    scan_workflow_id: str


@nexusrpc.service
class SecurityScanService:
    start_security_scan: nexusrpc.Operation[
        StartSecurityScanInput, SecurityScanStarted
    ]
