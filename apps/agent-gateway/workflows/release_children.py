"""The Waypoint engineering team's own release steps, as Child Workflows.

Three workflows live here, all owned by the Waypoint team, all running in the
gateway's own `agent-gateway` namespace on the existing `agentic-gateway` task
queue, started from AgenticChainWorkflow. None of them is a new namespace, a new
worker, or a new task queue.

That is the whole point of the distinction they draw against
SecurityScanWorkflow. That one is a peer in another namespace because a
different QuickMeals engineering team genuinely owns it, and the boundary between
them is a Nexus Endpoint. These three are the Waypoint team's own tooling,
invoked by the Waypoint team's own gateway workflow, and a Child Workflow is the
right primitive for exactly the reasons the SA best-practices guide lists as
legitimate: each is a unit of work worth giving its own Event History, its own
independently retryable steps, and its own workflow id in the Web UI. Not "to
organize code" and not "to reduce cost", both of which would be bad reasons.

The naming convention carries the distinction. Anything ending in
`ChildWorkflow` is same-namespace, same-team, started as a child of the chain.
`SecurityScanWorkflow` deliberately has no such suffix, because it is not one.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from common.models import (
        QUALITY_GATE_CHECKS,
        ArchiveArtifactsInput,
        CalculateHashesInput,
        CutReleaseInput,
        CutReleaseResult,
        DeployBinariesInput,
        HealthCheckInput,
        PromoteReleaseInput,
        PromoteReleaseResult,
        PublishQualityGateInput,
        QualityCheckInput,
        QualityGateInput,
        QualityGateResult,
        QualityGateState,
        TagCommitInput,
        UpdateReleaseNotesInput,
        UpdateRoutingInput,
    )
    from activities.gateway_activities import (
        archive_artifacts,
        calculate_integrity_hashes,
        deploy_binaries,
        health_check_new_instances,
        publish_quality_gate_state,
        run_quality_check,
        tag_commit_in_source_control,
        update_release_notes,
        update_traffic_routing,
    )


STEP_TIMEOUT = timedelta(seconds=10)
PUBLISH_TIMEOUT = timedelta(seconds=10)
STEP_RETRY = RetryPolicy(maximum_attempts=3)
CHECK_RETRY = RetryPolicy(maximum_attempts=2)

# The environment a candidate is qualified on. A promotion that lands here
# triggers a gate run for whatever just landed; a promotion anywhere else does
# not, because there is nothing to qualify a candidate for beyond production.
GATE_TRIGGER_ENVIRONMENT = "staging"


@workflow.defn
class CutReleaseChildWorkflow:
    """Cut a release: tag it, archive it, hash it.

    Sequential because each step genuinely depends on the last -- an artifact
    has to exist before it can be hashed -- and because sequencing it this way
    gives Event History a believable elapsed-time story instead of three
    identical timestamps.
    """

    @workflow.run
    async def run(self, input: CutReleaseInput) -> CutReleaseResult:
        tag = await workflow.execute_activity(
            tag_commit_in_source_control,
            TagCommitInput(
                service=input.service,
                version=input.version,
                commit_sha=input.commit_sha,
            ),
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=STEP_RETRY,
        )
        archive = await workflow.execute_activity(
            archive_artifacts,
            ArchiveArtifactsInput(
                service=input.service,
                version=input.version,
                idempotency_key=input.idempotency_key,
            ),
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=STEP_RETRY,
        )
        hashes = await workflow.execute_activity(
            calculate_integrity_hashes,
            CalculateHashesInput(
                service=input.service,
                version=input.version,
                artifact_ref=archive["artifact_ref"],
            ),
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=STEP_RETRY,
        )
        return CutReleaseResult(
            service=input.service,
            version=input.version,
            commit_sha=tag["commit_sha"],
            tag=tag["tag"],
            artifact_ref=archive["artifact_ref"],
            artifact_bytes=archive["artifact_bytes"],
            sha256=hashes["sha256"],
            message=(
                f"release {input.version} of {input.service} is cut from "
                f"{tag['commit_sha']} and ready to promote"
            ),
        )


@workflow.defn
class PromoteReleaseChildWorkflow:
    """Promote a release to an environment: deploy, health check, route, note it.

    Environment-agnostic on purpose. This runs identically for staging and for
    production, and there is deliberately no approval-flavored step anywhere
    inside it. Whether a promotion needs a human, and which human, is a policy
    question that belongs at the gateway layer above this workflow; mixing it in
    here would blur the line between what a promotion mechanically does and who
    is allowed to ask for one.

    The one thing it does do beyond promoting is start a quality gate run when
    the candidate lands on staging -- see _start_quality_gate for why that
    belongs here and not in either caller.
    """

    @workflow.run
    async def run(self, input: PromoteReleaseInput) -> PromoteReleaseResult:
        deploy = await workflow.execute_activity(
            deploy_binaries,
            DeployBinariesInput(
                service=input.service,
                version=input.version,
                environment=input.environment,
            ),
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=STEP_RETRY,
        )
        health = await workflow.execute_activity(
            health_check_new_instances,
            HealthCheckInput(
                service=input.service,
                version=input.version,
                environment=input.environment,
                instance_ids=deploy["instance_ids"],
            ),
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=STEP_RETRY,
        )
        if not health["healthy"]:
            # A real promotion stops here rather than flipping traffic onto
            # instances that just failed their health check. Not part of the
            # default scripted demo run (set HEALTH_CHECK_FAIL_VERSIONS to reach
            # it), but the branch is real: nothing below this line runs, so the
            # environment keeps serving whatever it was serving.
            workflow.logger.warning(
                "health check failed; not routing traffic",
                extra={"service": input.service, "version": input.version},
            )
            return PromoteReleaseResult(
                service=input.service,
                version=input.version,
                environment=input.environment,
                status="failed",
                instance_ids=list(deploy["instance_ids"]),
                reason="new instances failed health check",
                message=(
                    f"{input.version} was not promoted to {input.environment}: "
                    f"{health['message']}"
                ),
            )

        routing = await workflow.execute_activity(
            update_traffic_routing,
            UpdateRoutingInput(
                service=input.service,
                version=input.version,
                environment=input.environment,
                instance_ids=deploy["instance_ids"],
                idempotency_key=input.idempotency_key,
            ),
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=STEP_RETRY,
        )
        await workflow.execute_activity(
            update_release_notes,
            UpdateReleaseNotesInput(
                service=input.service,
                version=input.version,
                environment=input.environment,
            ),
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=STEP_RETRY,
        )

        gate_workflow_id = ""
        if input.environment == GATE_TRIGGER_ENVIRONMENT:
            gate_workflow_id = await self._start_quality_gate(input)

        return PromoteReleaseResult(
            service=input.service,
            version=input.version,
            environment=input.environment,
            status="completed",
            previous_version=routing.get("previous_version"),
            instance_ids=list(deploy["instance_ids"]),
            quality_gate_workflow_id=gate_workflow_id,
            message=f"promoted {input.service} {input.version} to {input.environment}",
        )

    async def _start_quality_gate(self, input: PromoteReleaseInput) -> str:
        """Start a gate run for whatever just landed on staging.

        Started from inside the promotion, and from nowhere else. Both CASE-1's
        manual promote_release and CASE-2a's pipeline reach staging through this
        same workflow, so this is the single place that guarantees exactly one
        gate run per staging promotion regardless of which phase's tool caused
        it -- duplicating the trigger in both callers would race, and putting it
        in only one of them would mean CASE-1 never gets a gate.

        ABANDON, and only the start is awaited. The gate outlives this workflow
        deliberately: a staging promotion returns to its caller as soon as
        staging is actually promoted, and the gate runs alongside for whoever
        wants to look. CASE-2a's pipeline waits on the verdict afterwards, using
        the id returned here; CASE-1 never waits on it at all.
        """
        gate_id = (
            f"quality-gate::{input.service}::{input.version}::"
            f"{workflow.uuid4().hex[:8]}"
        )
        await workflow.start_child_workflow(
            QualityGateChildWorkflow.run,
            QualityGateInput(
                service=input.service,
                version=input.version,
                environment=input.environment,
            ),
            id=gate_id,
            parent_close_policy=ParentClosePolicy.ABANDON,
        )
        workflow.logger.info(
            "started quality gate %s for %s %s",
            gate_id,
            input.service,
            input.version,
        )
        return gate_id


@workflow.defn
class QualityGateChildWorkflow:
    """Qualify one staged candidate against four checks, run concurrently.

    Version-keyed by its workflow id, which is what makes the verdict safe to
    consume. A gate run starts every time anything reaches staging, so "the
    latest gate result" is not a safe thing for a pipeline to read: it may
    belong to a different candidate entirely. Waiting on a specific workflow id
    for a specific (service, version) is, and the id encodes both so a
    mismatched pair is visible rather than silently promoted on.

    Nothing blocks on this in CASE-1. The card on the dashboard updates and an
    approver can look at it before deciding, which is a very different thing
    from the pipeline sequencing that CASE-1 deliberately does not have.
    """

    @workflow.init
    def __init__(self, input: QualityGateInput) -> None:
        self._state = QualityGateState(
            status="running",
            service=input.service,
            version=input.version,
            environment=input.environment,
        )

    @workflow.run
    async def run(self, input: QualityGateInput) -> QualityGateResult:
        await self._publish()

        # All four started before any is awaited, so they run concurrently: the
        # gate takes about as long as its slowest check rather than the sum of
        # all four. start_activity rather than execute_activity because these
        # are collected as handles and drained as they finish.
        handles = [
            workflow.start_activity(
                run_quality_check,
                QualityCheckInput(
                    service=input.service,
                    version=input.version,
                    check_name=name,
                    scripted_outcome=input.scripted_outcome,
                ),
                start_to_close_timeout=STEP_TIMEOUT,
                retry_policy=CHECK_RETRY,
            )
            for name in QUALITY_GATE_CHECKS
        ]

        # Drained as they complete rather than gathered at the end, so the card
        # can honestly say "2 / 4 checks complete" while the run is in flight.
        # Results are collected in the original check order on every pass, never
        # in whatever order asyncio hands the completed set back, so replay
        # produces the same list.
        pending = list(handles)
        while pending:
            await workflow.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            still_pending = []
            for handle in pending:
                if handle.done():
                    self._state.checks.append(handle.result())
                else:
                    still_pending.append(handle)
            pending = still_pending
            if pending:
                await self._publish()

        self._state.status = (
            "passed"
            if all(check["passed"] for check in self._state.checks)
            else "failed"
        )
        await self._publish()
        workflow.logger.info(
            "quality gate for %s %s: %s",
            input.service,
            input.version,
            self._state.status,
        )
        return QualityGateResult(
            service=input.service,
            version=input.version,
            environment=input.environment,
            status=self._state.status,
            checks=list(self._state.checks),
        )

    @workflow.query
    def get_status(self) -> QualityGateState:
        return self._state

    async def _publish(self) -> None:
        try:
            await workflow.execute_activity(
                publish_quality_gate_state,
                PublishQualityGateInput(
                    service=self._state.service,
                    version=self._state.version,
                    environment=self._state.environment,
                    status=self._state.status,
                    checks_completed=len(self._state.checks),
                    check_count=len(QUALITY_GATE_CHECKS),
                    checks=list(self._state.checks),
                    gate_workflow_id=workflow.info().workflow_id,
                ),
                start_to_close_timeout=PUBLISH_TIMEOUT,
                retry_policy=CHECK_RETRY,
            )
        except ActivityError as err:
            # Whether the dashboard can draw a card has nothing to do with what
            # the gates found, and the verdict is already durable here.
            workflow.logger.warning(
                "could not publish quality gate state", extra={"error": str(err)}
            )
