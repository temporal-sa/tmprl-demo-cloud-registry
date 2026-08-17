"""The Security team's Nexus service handler.

This team's front door. Behind the `security` Nexus Endpoint, registered on this
team's worker, in this team's namespace. Agent Gateway addresses the endpoint and
nothing else -- it cannot see the namespace, the task queue, or the workflow
type, so all three can change here without anyone else's deploy.
"""

from __future__ import annotations

import uuid

from nexusrpc.handler import service_handler, sync_operation
from nexusrpc.handler import StartOperationContext
from temporalio import nexus

from security_scan import models
from security_scan.security_scan_workflow import SecurityScanWorkflow
from security_scan.models import SecurityScanInput
from security_scan.nexus_contracts import (
    SecurityScanService,
    SecurityScanStarted,
    StartSecurityScanInput,
)


@service_handler(service=SecurityScanService)
class SecurityScanServiceHandler:
    @sync_operation
    async def start_security_scan(
        self, ctx: StartOperationContext, input: StartSecurityScanInput
    ) -> SecurityScanStarted:
        """Start a pre-prod security scan and hand back its id.

        Synchronous on purpose, and this is the one design decision in the file
        worth reading twice. The obvious alternative -- an asynchronous operation
        the pipeline awaits for the whole scan -- would put a Nexus operation
        handle inside AgenticChainWorkflow, which is a long-lived entity workflow
        that continues-as-new. Handles do not survive Continue-As-New, so the
        scan would be orphaned mid-approval. Returning immediately means the
        pipeline holds an id, which is plain data and carries across CAN like any
        other state.

        The scan's shape -- which stages run, how far apart, at what severity
        threshold -- is decided here, from this team's own constants. It is not an
        argument, because it is not the caller's decision.
        """
        client = nexus.client()
        scan_workflow_id = (
            f"security::scan::{input.service}::{input.version}::"
            f"{uuid.uuid4().hex[:8]}"
        )
        await client.start_workflow(
            SecurityScanWorkflow.run,
            SecurityScanInput(
                workflow_id=input.gateway_workflow_id,
                service=input.service,
                version=input.version,
                environment=input.environment,
                idempotency_key=input.idempotency_key,
                origin_operation_id=input.origin_operation_id,
                requester=input.requester,
                # Read at call time rather than bound as dataclass defaults, so
                # a test can shorten the scan through the same path the real
                # worker uses instead of a test-only branch.
                check_seconds=models.SCAN_CHECK_SECONDS,
                stage_count=models.SCAN_CHECK_COUNT,
                scripted_outcome=_scripted_outcome(input.version),
            ),
            id=scan_workflow_id,
            task_queue=models.SECURITY_TASK_QUEUE,
        )
        return SecurityScanStarted(scan_workflow_id=scan_workflow_id)


def _scripted_outcome(version: str) -> str:
    """Demo knob, and this team's business rather than the caller's.

    Which versions fail the scan used to be relayed from the shared registry
    through the pipeline, which meant the caller was telling the scan what
    verdict to reach. It is read here now, where it belongs.
    """
    return "fail" if version in models.scan_fail_versions() else "pass"
