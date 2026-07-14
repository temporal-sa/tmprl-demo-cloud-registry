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

from demo_registry_operator import derive
from demo_registry_operator.activities import crashable_workspace as activities
from demo_registry_operator.workflows.crashable_workspace_messages import (
    ApplyWorkspaceResourceRequest,
    BuildWorkspacePlanRequest,
    CrashWorkspacePodsRequest,
    DeleteWorkspaceNamespaceRequest,
    EnsureWorkspaceRequest,
    ResolveSourceImagesRequest,
    WaitWorkspaceReadyRequest,
    WorkspaceStatus,
)

LIVE_PHASES = frozenset({"Active", "Crashing"})
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
            self._status.phase = "Active"
            self._status.step = "Ready"
        else:
            await self._provision(request)

        await self._run_ttl_loop(request)

        # Teardown (TTL expired or explicit delete): drop the namespace, which
        # cascades to everything in it.
        self._status.phase = "Deleting"
        self._status.step = "Deleting environment"
        await workflow.execute_activity(
            activities.delete_workspace_namespace,
            DeleteWorkspaceNamespaceRequest(namespace=self._namespace),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=activities.CLEANUP_RETRY_POLICY,
        )

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
                )
            )

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
                ),
                start_to_close_timeout=timedelta(minutes=6),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=activities.WAIT_READY_RETRY_POLICY,
            )

            self._status.phase = "Active"
            self._status.step = "Ready"
        except asyncio.CancelledError:
            self._status.phase = "Deleting"
            # Shield so compensation survives workflow cancellation.
            await asyncio.shield(
                asyncio.ensure_future(_run_compensations(compensations))
            )
            raise
        except Exception as error:  # noqa: BLE001 - saga compensation boundary
            self._status.phase = "Failed"
            self._status.last_error = str(error)
            await _run_compensations(compensations)
            raise

    async def _run_ttl_loop(self, request: EnsureWorkspaceRequest) -> None:
        while self._status.phase in LIVE_PHASES:
            if self._expires_at is None:
                return
            remaining = self._expires_at - workflow.now()
            if remaining <= timedelta(0):
                return

            # Bound history growth for a long-lived workspace (repeated extends
            # and/or crashes — extends have no cap) by continuing-as-new when the
            # server suggests it, carrying enough state to resume the TTL loop
            # without re-provisioning. Only when settled (no pending crash/delete).
            if (
                self._status.phase == "Active"
                and not self._crash_requested
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
                    )
                )

            deadline_snapshot = self._deadline_version
            try:
                await workflow.wait_condition(
                    lambda: (
                        self._crash_requested
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
                await workflow.execute_activity(
                    activities.crash_workspace_pods,
                    CrashWorkspacePodsRequest(namespace=self._namespace),
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=activities.CRASH_RETRY_POLICY,
                )
                self._status.phase = "Active"
                self._status.step = "Ready"
                # Loops back to the top, where the continue-as-new check runs.
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
) -> None:
    for compensate in reversed(compensations):
        try:
            await compensate()
        except Exception as error:  # noqa: BLE001 - best-effort compensation
            workflow.logger.error(
                "crashable workspace compensation failed: %s", error
            )
