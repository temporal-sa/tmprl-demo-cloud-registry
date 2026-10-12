"""Per-(user, demo) entity workflow for a crashable, time-limited environment.

One workflow instance represents one user's ephemeral clone of one demo's
Kubernetes namespace. It provisions the clone with a saga (compensating by
deleting the namespace, which cascades to everything in it), serves its lifecycle
through updates/queries, holds an updatable-timer TTL, continues-as-new to bound
history, and tears the namespace down on expiry or explicit delete.

Only Kubernetes resources are created; the clone reuses the demo's existing
Temporal Cloud namespace and Secrets Manager secrets.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, is_cancelled_exception

from demo_registry_operator import derive, errors
from demo_registry_operator.activities import crashable_workspace as activities
from demo_registry_operator.workflows.crashable_workspace_messages import (
    ApplyWorkspaceResourceRequest,
    BuildWorkspacePlanRequest,
    CrashWorkspacePodsRequest,
    DeleteWorkspaceNamespaceRequest,
    EnsureWorkspaceRequest,
    ResolveSourceImagesRequest,
    ScaleWorkspaceWorkloadsRequest,
    TerminateWorkspaceWorkflowsRequest,
    WaitWorkspaceReadyRequest,
    WorkspaceStatus,
)

LIVE_PHASES = frozenset(
    {"Active", "Crashing", "ScalingDown", "ScaledDown", "ScalingUp"}
)
SETTLED_PHASES = frozenset({"Active", "ScaledDown"})
EXTEND_INTERVAL = timedelta(hours=1)


@workflow.defn
class CrashableWorkspaceWorkflow:
    """Entity workflow for one user's crashable clone of one demo."""

    @workflow.init
    def __init__(self, request: EnsureWorkspaceRequest) -> None:
        workspace_id = request.carried_workspace_id or derive.crashable_workspace_id(
            request.owner_email, request.demo_name
        )
        namespace = derive.crashable_workspace_namespace(workspace_id)
        host = derive.crashable_workspace_host(workspace_id)
        self._workspace_id = workspace_id
        self._namespace = namespace
        self._crash_requested = False
        self._scale_target_replicas: int | None = None
        self._delete_requested = False
        self._deadline_version = 0
        self._expires_at: datetime | None = None
        self._status = WorkspaceStatus(
            phase="Provisioning",
            step="Queued",
            demo=request.demo_name,
            workspace_id=workspace_id,
            namespace=namespace,
            host=host,
            app_url=f"https://{host}",
            task_queue="",
            created_at=None,
            expires_at=None,
            last_error=None,
        )

    @workflow.run
    async def run(self, request: EnsureWorkspaceRequest) -> None:
        if request.carried_already_provisioned:
            # Continue-as-new carryover: the namespace already exists, so restore
            # state and skip straight to the TTL loop — do NOT re-provision.
            self._status.task_queue = request.carried_task_queue or ""
            self._status.created_at = request.carried_created_at
            self._status.expires_at = request.carried_expires_at
            self._expires_at = _parse(request.carried_expires_at)
            self._status.phase = (
                "ScaledDown" if request.carried_scaled_down else "Active"
            )
            self._status.step = (
                "Workers scaled to zero"
                if request.carried_scaled_down
                else "Ready"
            )
        else:
            await self._provision(request)

        await self._run_ttl_loop(request)

        # Teardown (TTL expired or explicit delete): terminate what is left on
        # the workspace queue, then drop the namespace, which cascades to
        # everything in it.
        self._status.phase = "Deleting"
        try:
            await self._terminate_task_queue_workflows()
        except ActivityError as error:
            # Best effort: never let it strand the workspace's own resources.
            if is_cancelled_exception(error):
                raise
            self._status.failure = errors.failure_details(error)
            self._status.last_error = (
                "Failed to terminate workspace workflows: "
                f"{self._status.failure['message']}"
            )
            workflow.logger.error(
                "crashable workspace workflow termination failed: %s", error
            )
        self._status.step = "Deleting environment"
        try:
            await workflow.execute_activity(
                activities.delete_workspace_namespace,
                DeleteWorkspaceNamespaceRequest(namespace=self._namespace),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=activities.CLEANUP_RETRY_POLICY,
            )
        except ActivityError as error:
            if is_cancelled_exception(error):
                raise
            self._status.phase = "Failed"
            self._status.step = "Environment cleanup failed"
            self._status.failure = errors.failure_details(error)
            self._status.last_error = self._status.failure["message"]
            raise

    async def _provision(self, request: EnsureWorkspaceRequest) -> None:
        compensations: list[Callable[[], Awaitable[Any]]] = []
        try:
            self._status.step = "Resolving images"
            images = await workflow.execute_activity(
                activities.resolve_source_images,
                ResolveSourceImagesRequest(demo_name=request.demo_name),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=activities.PROVISIONING_RETRY_POLICY,
            )

            self._status.step = "Building plan"
            plan = await workflow.execute_activity(
                activities.build_workspace_plan,
                BuildWorkspacePlanRequest(
                    demo_name=request.demo_name,
                    owner_email=request.owner_email,
                    workspace_id=self._workspace_id,
                    images=images.images,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=activities.PROVISIONING_RETRY_POLICY,
            )
            self._status.task_queue = plan.task_queue

            now = workflow.now()
            self._expires_at = now + timedelta(seconds=request.ttl_seconds)
            self._status.created_at = now.astimezone(UTC).isoformat()
            self._status.expires_at = self._expires_at.astimezone(UTC).isoformat()

            # Register the cascading compensation BEFORE the first apply so a
            # namespace created-but-not-reported is still cleaned up. Deleting the
            # namespace cascades all children, so this single compensation suffices.
            compensations.append(
                lambda: workflow.execute_activity(
                    activities.delete_workspace_namespace,
                    DeleteWorkspaceNamespaceRequest(namespace=self._namespace),
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=activities.CLEANUP_RETRY_POLICY,
                    summary="Compensate failed workspace provision: delete namespace",
                )
            )
            # Runs first (compensations run in reverse); the app may already
            # have started executions on the workspace queue.
            compensations.append(self._terminate_task_queue_workflows)

            for manifest in plan.resources:
                metadata = manifest.get("metadata", {})
                name = metadata.get("name") if isinstance(metadata, dict) else None
                self._status.step = f"Applying {manifest.get('kind')}/{name}"
                await workflow.execute_activity(
                    activities.apply_workspace_resource,
                    ApplyWorkspaceResourceRequest(manifest=manifest),
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=activities.PROVISIONING_RETRY_POLICY,
                )

            self._status.step = "Waiting for pods"
            await workflow.execute_activity(
                activities.wait_workspace_ready,
                WaitWorkspaceReadyRequest(
                    namespace=self._namespace,
                    deployment_names=plan.deployment_names,
                    worker_deployment_names=plan.worker_deployment_names,
                ),
                start_to_close_timeout=timedelta(minutes=6),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=activities.WAIT_READY_RETRY_POLICY,
            )

            self._status.phase = "Active"
            self._status.step = "Ready"
        except (Exception, asyncio.CancelledError) as error:
            cancelled = isinstance(
                error, asyncio.CancelledError
            ) or is_cancelled_exception(error)
            self._status.phase = "Deleting" if cancelled else "Failed"
            self._status.failure = errors.failure_details(error)
            self._status.last_error = self._status.failure["message"]
            cleanup = asyncio.create_task(self._compensate_provision(compensations))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                # Cancellation arriving during cleanup must also wait for it.
                await asyncio.shield(cleanup)
                raise
            raise

    async def _terminate_task_queue_workflows(self) -> None:
        """Terminate executions left open on the workspace task queue.

        The workspace shares the demo's Temporal namespace, so nothing else
        cleans these up once its workers are gone.
        """

        if not self._status.task_queue:
            return
        self._status.step = "Terminating workspace workflows"
        await workflow.execute_activity(
            activities.terminate_workspace_workflows,
            TerminateWorkspaceWorkflowsRequest(
                demo_name=self._status.demo,
                workspace_id=self._workspace_id,
                task_queue=self._status.task_queue,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=activities.CLEANUP_RETRY_POLICY,
            summary="Terminate workflows left on the workspace task queue",
        )

    async def _compensate_provision(
        self,
        compensations: list[Callable[[], Awaitable[Any]]],
    ) -> None:
        if not compensations:
            self._status.compensation_status = {"phase": "NotRequired"}
            return
        self._status.compensation_status = {"phase": "Running"}
        failures = await _run_compensations(compensations)
        self._status.compensation_status = {
            "phase": "Error" if failures else "Completed",
            "failures": failures,
        }

    async def _run_ttl_loop(self, request: EnsureWorkspaceRequest) -> None:
        while self._status.phase in LIVE_PHASES:
            if self._expires_at is None:
                return
            remaining = self._expires_at - workflow.now()
            if remaining <= timedelta(0):
                return

            # Bound history growth for a long-lived workspace (repeated extends
            # and/or outage actions — extends have no cap) by continuing-as-new
            # when the server suggests it, carrying enough state to resume the TTL
            # loop without re-provisioning. Only when settled (no pending action).
            if (
                self._status.phase in SETTLED_PHASES
                and not self._crash_requested
                and self._scale_target_replicas is None
                and not self._delete_requested
                and workflow.info().is_continue_as_new_suggested()
            ):
                workflow.continue_as_new(
                    EnsureWorkspaceRequest(
                        demo_name=self._status.demo,
                        owner_email=request.owner_email,
                        ttl_seconds=request.ttl_seconds,
                        carried_already_provisioned=True,
                        carried_expires_at=self._status.expires_at,
                        carried_created_at=self._status.created_at,
                        carried_task_queue=self._status.task_queue,
                        carried_workspace_id=self._workspace_id,
                        carried_scaled_down=self._status.phase == "ScaledDown",
                    )
                )

            deadline_snapshot = self._deadline_version
            try:
                await workflow.wait_condition(
                    lambda: (
                        self._crash_requested
                        or self._scale_target_replicas is not None
                        or self._delete_requested
                        or self._deadline_version != deadline_snapshot
                    ),
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                return  # TTL expired

            if self._delete_requested:
                return
            if self._crash_requested:
                self._crash_requested = False
                self._status.phase = "Crashing"
                self._status.step = "Crashing workers"
                self._status.last_error = None
                self._status.failure = {}
                try:
                    await workflow.execute_activity(
                        activities.crash_workspace_pods,
                        CrashWorkspacePodsRequest(
                            namespace=self._namespace,
                            demo_name=self._status.demo,
                        ),
                        start_to_close_timeout=timedelta(minutes=2),
                        retry_policy=activities.CRASH_RETRY_POLICY,
                    )
                except ActivityError as error:
                    if is_cancelled_exception(error):
                        raise
                    self._status.failure = errors.failure_details(error)
                    self._status.last_error = (
                        f"Failed to crash workers: {self._status.failure['message']}"
                    )
                    workflow.logger.error(
                        "crashable workspace pod crash failed: %s", error
                    )
                if self._delete_requested:
                    return
                self._status.phase = "Active"
                self._status.step = (
                    "Crash failed; environment remains active"
                    if self._status.last_error
                    else "Ready"
                )
                # Loops back to the top, where the continue-as-new check runs.
            elif self._scale_target_replicas is not None:
                target_replicas = self._scale_target_replicas
                self._scale_target_replicas = None
                scaling_down = target_replicas == 0
                fallback_phase = "Active" if scaling_down else "ScaledDown"
                action = "scale down" if scaling_down else "scale up"
                self._status.phase = "ScalingDown" if scaling_down else "ScalingUp"
                self._status.step = (
                    "Scaling workers to zero"
                    if scaling_down
                    else "Scaling workers to one"
                )
                self._status.last_error = None
                self._status.failure = {}
                try:
                    await workflow.execute_activity(
                        activities.scale_workspace_workloads,
                        ScaleWorkspaceWorkloadsRequest(
                            namespace=self._namespace,
                            replicas=target_replicas,
                            demo_name=self._status.demo,
                        ),
                        start_to_close_timeout=timedelta(minutes=2),
                        retry_policy=activities.SCALE_RETRY_POLICY,
                    )
                except ActivityError as error:
                    if is_cancelled_exception(error):
                        raise
                    self._status.failure = errors.failure_details(error)
                    self._status.last_error = (
                        f"Failed to {action} workers: {self._status.failure['message']}"
                    )
                    workflow.logger.error(
                        "crashable workspace %s failed: %s", action, error
                    )
                if self._delete_requested:
                    return
                if self._status.last_error:
                    self._status.phase = fallback_phase
                    self._status.step = (
                        "Scale down failed; worker state may be mixed"
                        if scaling_down
                        else "Scale up failed; worker state may be mixed"
                    )
                else:
                    self._status.phase = "ScaledDown" if scaling_down else "Active"
                    self._status.step = (
                        "Workers scaled to zero" if scaling_down else "Ready"
                    )
            # Otherwise the deadline changed (extend): recompute and wait again.

    @workflow.update
    def ensure(self) -> WorkspaceStatus:
        """Idempotent handle: returns current status. Paired with update-with-start
        so the first click provisions and repeat clicks just report status."""

        return self._snapshot()

    @workflow.update
    def extend(self) -> WorkspaceStatus:
        if self._status.phase not in LIVE_PHASES:
            return self._snapshot()
        base = self._expires_at or workflow.now()
        self._expires_at = base + EXTEND_INTERVAL
        self._status.expires_at = self._expires_at.astimezone(UTC).isoformat()
        self._deadline_version += 1
        return self._snapshot()

    @workflow.update
    def crash(self) -> WorkspaceStatus:
        if self._status.phase != "Active":
            return self._snapshot()
        self._crash_requested = True
        self._status.phase = "Crashing"
        return self._snapshot()

    @workflow.update
    def scale_down(self) -> WorkspaceStatus:
        if self._status.phase != "Active":
            return self._snapshot()
        self._scale_target_replicas = 0
        self._status.phase = "ScalingDown"
        self._status.step = "Scaling workers to zero"
        self._status.last_error = None
        return self._snapshot()

    @workflow.update
    def scale_up(self) -> WorkspaceStatus:
        if self._status.phase != "ScaledDown":
            return self._snapshot()
        self._scale_target_replicas = 1
        self._status.phase = "ScalingUp"
        self._status.step = "Scaling workers to one"
        self._status.last_error = None
        return self._snapshot()

    @workflow.update
    def delete(self) -> WorkspaceStatus:
        self._delete_requested = True
        self._status.phase = "Deleting"
        return self._snapshot()

    @workflow.query
    def status(self) -> dict[str, Any]:
        return dataclasses.asdict(self._status)

    def _snapshot(self) -> WorkspaceStatus:
        return dataclasses.replace(self._status)


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


async def _run_compensations(
    compensations: list[Callable[[], Awaitable[Any]]],
) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for compensate in reversed(compensations):
        try:
            await compensate()
        except Exception as error:  # noqa: BLE001 - best-effort compensation
            failure = errors.failure_details(error)
            failures.append(failure)
            workflow.logger.error(
                "crashable workspace compensation failed: %s", failure["message"]
            )
    return failures
