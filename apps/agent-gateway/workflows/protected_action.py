from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from common.models import (
        GatewayOperationResolution,
        SubmitNestedToolCallInput,
    )
    from common.nexus_contracts import (
        ProtectedActionOutcome,
        ProtectedActionRequest,
    )
    from activities.gateway_activities import submit_nested_tool_call


SUBMIT_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class ProtectedActionWorkflow:
    """Holds one external tool's protected-action request open until it resolves.

    This is the Nexus handler for AgentGatewayService.request_protected_action,
    and it exists because of a specific mismatch. A Nexus operation backed by a
    workflow starts a *new* workflow, but the thing that has to service this
    request is AgenticChainWorkflow, which is long-lived, already running, and
    owns the operator's whole session. There is no Nexus primitive for "attach
    to that execution and complete when one operation inside it resolves", so
    this workflow is that adapter.

    It is short-lived and entirely the gateway team's own code. Everything it
    does to the chain is an ordinary same-namespace call, which is the point:
    the cross-team hop ended at the Nexus boundary, and what happens after it is
    internal.

    The caller does not poll and is not signaled. It suspends on the Nexus
    operation, and this workflow's return value is what wakes it, whether that
    is minutes or hours later.
    """

    @workflow.init
    def __init__(self, request: ProtectedActionRequest) -> None:
        self._resolution: GatewayOperationResolution | None = None
        self._operation_id: str = ""

    @workflow.run
    async def run(self, request: ProtectedActionRequest) -> ProtectedActionOutcome:
        # Submitting is an Activity because it is an Update against another
        # workflow, which workflow code cannot issue directly. Same namespace,
        # same team, same deployment.
        try:
            response = await workflow.execute_activity(
                submit_nested_tool_call,
                SubmitNestedToolCallInput(
                    gateway_workflow_id=request.gateway_workflow_id,
                    tool_name=request.tool_name,
                    arguments=request.arguments,
                    idempotency_key=request.idempotency_key,
                    caller_service=request.caller_service,
                    caller_principal=request.caller_principal,
                    caller_workflow_id=request.caller_workflow_id,
                    origin_operation_id=request.origin_operation_id,
                    justification=request.justification,
                    # The chain signals this workflow once the operation
                    # resolves. Same namespace, so no address and no namespace
                    # travels with it.
                    callback_workflow_id=workflow.info().workflow_id,
                ),
                start_to_close_timeout=SUBMIT_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
        except ActivityError as err:
            return ProtectedActionOutcome(
                operation_id="",
                status="failed",
                reason=f"Agent Gateway did not accept the request: {err}",
            )

        self._operation_id = response.get("operation_id") or ""
        status = response.get("status")

        if status not in ("waiting_for_approval", "processing"):
            # Resolved without a human: policy did not protect it, it deduped
            # onto a finished operation, or it was refused outright.
            return ProtectedActionOutcome(
                operation_id=self._operation_id,
                status="completed" if status == "completed" else str(status),
                result=response.get("result"),
                reason=response.get("reason") or response.get("message"),
            )

        # The pause. This workflow stops here, and so does the Nexus operation
        # the caller is suspended on, which is how a tool in another namespace
        # ends up waiting on a human without holding anything open.
        await workflow.wait_condition(lambda: self._resolution is not None)
        resolution = self._resolution
        assert resolution is not None
        return ProtectedActionOutcome(
            operation_id=resolution.operation_id,
            status=resolution.status,
            result=resolution.result,
            reason=resolution.reason,
        )

    @workflow.signal
    def operation_resolved(self, resolution: GatewayOperationResolution) -> None:
        """The chain reporting a terminal operation. Mutates state only."""
        if self._resolution is not None:
            return
        if self._operation_id and resolution.operation_id != self._operation_id:
            workflow.logger.warning(
                "ignoring a resolution for an unrelated operation",
                extra={
                    "received": resolution.operation_id,
                    "expected": self._operation_id,
                },
            )
            return
        self._resolution = resolution

    @workflow.query
    def get_operation_id(self) -> str:
        return self._operation_id
