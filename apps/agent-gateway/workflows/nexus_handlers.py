"""Agent Gateway's Nexus service handler.

The gateway's front door for other teams' tools. Registered on the gateway's own
worker, in the gateway's own namespace, behind the `agent-gateway` Nexus
Endpoint. A caller needs the endpoint name and the operation contract; it needs
no credentials for this namespace, no workflow id belonging to the gateway's
internals, and no knowledge that AgenticChainWorkflow exists.
"""

from __future__ import annotations

import uuid

import nexusrpc
from nexusrpc.handler import service_handler, sync_operation
from temporalio import nexus
from temporalio.nexus import WorkflowRunOperationContext, workflow_run_operation

from common.models import ScanVerdict
from common.nexus_contracts import (
    AgentGatewayService,
    ProtectedActionOutcome,
    ProtectedActionRequest,
    ToolOutcomeAck,
    ToolOutcomeReport,
)
from workflows.protected_action import ProtectedActionWorkflow

CHAIN_SCAN_VERDICT_SIGNAL = "scan_verdict_reported"


@service_handler(service=AgentGatewayService)
class AgentGatewayServiceHandler:
    @workflow_run_operation
    async def request_protected_action(
        self, ctx: WorkflowRunOperationContext, input: ProtectedActionRequest
    ) -> nexus.WorkflowHandle[ProtectedActionOutcome]:
        """Accept a governed action request and hold it open until it resolves.

        Asynchronous because the answer involves a human. The caller gets an
        operation handle immediately and suspends on it; the completion callback
        Temporal delivers when ProtectedActionWorkflow returns is what resumes
        them. No polling, no signal plumbing, and nothing held open in between.
        """
        return await ctx.start_workflow(
            ProtectedActionWorkflow.run,
            input,
            id=(
                f"protected-action::{input.caller_service}::"
                f"{input.tool_name}::{uuid.uuid4().hex[:8]}"
            ),
        )

    @sync_operation
    async def report_tool_outcome(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ToolOutcomeReport
    ) -> ToolOutcomeAck:
        """Accept a tool's report that no request is coming after all.

        Synchronous: there is nothing to wait for. A checkpoint that failed will
        never ask for a protected action, and the pipeline operation parked on it
        needs to be told so rather than waiting for a call that is not coming.
        """
        if not input.origin_operation_id:
            return ToolOutcomeAck(accepted=False)
        # The worker's own client, so this stays inside the gateway's namespace.
        client = nexus.client()
        handle = client.get_workflow_handle(input.gateway_workflow_id)
        await handle.signal(
            CHAIN_SCAN_VERDICT_SIGNAL,
            ScanVerdict(
                origin_operation_id=input.origin_operation_id,
                scan_workflow_id=input.caller_workflow_id,
                verdict=input.outcome,
                reason=input.reason,
                detail=input.detail,
            ),
        )
        return ToolOutcomeAck(accepted=True)
