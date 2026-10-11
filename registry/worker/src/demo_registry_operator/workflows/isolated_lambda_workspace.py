"""Per-(user, demo) workspace whose order workers are temporary Lambda functions.

This is a separate workflow from CrashableWorkspaceWorkflow. The two modes
schedule different activities and accept different updates, and an in-flight
crashable history must keep its existing command sequence. They share the
workflow id ``crashable-workspace/{email}/{demo}`` so a user still has one
environment per demo. The catalog starts this type only when the demo opts in
through ``spec.registry.isolatedLambda``.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, is_cancelled_exception

from demo_registry_operator import derive, errors
from demo_registry_operator.activities import crashable_workspace as crashable_activities
from demo_registry_operator.activities import external_resources
from demo_registry_operator.activities import isolated_lambda_workspace as activities
from demo_registry_operator.workflows.crashable_workspace_messages import (
    ApplyWorkspaceResourceRequest,
    DeleteWorkspaceNamespaceRequest,
    ResolveSourceImagesRequest,
    WaitWorkspaceReadyRequest,
)
from demo_registry_operator.workflows.isolated_lambda_workspace_messages import (
    BuildIsolatedWorkspacePlanRequest,
    DeleteWorkspaceLambdaFunctionsRequest,
    EnsureIsolatedWorkspaceRequest,
    IsolatedLambdaComponentPlan,
    IsolatedWorkspaceStatus,
    ResolveSharedLambdaRolesRequest,
    UpdateWorkspaceLambdaInvokePolicyRequest,
    WorkspaceLambdaWorker,
)

LIVE_PHASES = frozenset({"Active"})
SETTLED_PHASES = frozenset({"Active"})
EXTEND_INTERVAL = timedelta(hours=1)
LAMBDA_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
SERVERLESS_WORKER_DEPLOYMENT_ACTIVITY_TIMEOUT = timedelta(minutes=6)
# An already-running Lambda invocation can poll until its timeout (up to 15m),
# followed by Matching's default 5m poller history. Allow 20m15s of retry delays
# for deployment deletion; the rollout gate reserves 30m for cleanup.
WORKER_DEPLOYMENT_CLEANUP_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=24,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


@workflow.defn
class IsolatedLambdaWorkspaceWorkflow:
    """Entity workflow for one user's isolated Lambda copy of one demo."""

    @workflow.init
    def __init__(self, request: EnsureIsolatedWorkspaceRequest) -> None:
        workspace_id = request.carried_workspace_id or derive.crashable_workspace_id(
            request.owner_email, request.demo_name
        )
        namespace = derive.crashable_workspace_namespace(workspace_id)
        host = derive.crashable_workspace_host(workspace_id)
        self._workspace_id = workspace_id
        self._namespace = namespace
        self._delete_requested = False
        self._deadline_version = 0
        self._expires_at: datetime | None = None
        self._workers: list[WorkspaceLambdaWorker] = list(request.carried_lambda_workers)
        self._status = IsolatedWorkspaceStatus(
            phase="Provisioning",
            step="Queued",
            demo=request.demo_name,
            workspace_id=workspace_id,
            namespace=namespace,
            host=host,
            app_url=f"https://{host}",
            task_queue=request.carried_task_queue or "",
            lambda_workers=[dataclasses.asdict(worker) for worker in self._workers],
            initial_current=dict(request.carried_initial_current),
        )

    @workflow.run
    async def run(self, request: EnsureIsolatedWorkspaceRequest) -> None:
        if request.carried_already_provisioned:
            self._status.created_at = request.carried_created_at
            self._status.expires_at = request.carried_expires_at
            self._expires_at = _parse(request.carried_expires_at)
            self._status.phase = "Active"
            self._status.step = "Ready"
        else:
            await self._provision(request)

        await self._run_ttl_loop(request)

        self._status.phase = "Deleting"
        self._status.step = "Deleting environment"
        try:
            await self._cleanup_lambda()
            await workflow.execute_activity(
                crashable_activities.delete_workspace_namespace,
                DeleteWorkspaceNamespaceRequest(namespace=self._namespace),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=crashable_activities.CLEANUP_RETRY_POLICY,
            )
        except ActivityError as error:
            if is_cancelled_exception(error):
                raise
            self._status.phase = "Failed"
            self._status.step = "Environment cleanup failed"
            self._status.failure = errors.failure_details(error)
            self._status.last_error = self._status.failure["message"]
            raise

    async def _provision(self, request: EnsureIsolatedWorkspaceRequest) -> None:
        compensations: list[Callable[[], Awaitable[Any]]] = []
        try:
            self._status.step = "Resolving images"
            images = await workflow.execute_activity(
                crashable_activities.resolve_source_images,
                ResolveSourceImagesRequest(demo_name=request.demo_name),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=crashable_activities.PROVISIONING_RETRY_POLICY,
            )
            self._status.step = "Building plan"
            plan = await workflow.execute_activity(
                activities.build_isolated_workspace_plan_activity,
                BuildIsolatedWorkspacePlanRequest(
                    demo_name=request.demo_name,
                    owner_email=request.owner_email,
                    workspace_id=self._workspace_id,
                    images=images.images,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=crashable_activities.PROVISIONING_RETRY_POLICY,
            )
            self._status.task_queue = plan.task_queue
            self._status.initial_current = dict(plan.initial_current)
            now = workflow.now()
            self._expires_at = now + timedelta(seconds=request.ttl_seconds)
            self._status.created_at = now.astimezone(UTC).isoformat()
            self._status.expires_at = self._expires_at.astimezone(UTC).isoformat()

            # Namespace compensation is registered first. Compensations run in
            # reverse, so Lambda cleanup still happens before the namespace delete.
            compensations.append(
                lambda: workflow.execute_activity(
                    crashable_activities.delete_workspace_namespace,
                    DeleteWorkspaceNamespaceRequest(namespace=self._namespace),
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=crashable_activities.CLEANUP_RETRY_POLICY,
                    summary="Compensate failed workspace provision: delete namespace",
                )
            )
            compensations.append(self._cleanup_lambda)

            for manifest in plan.resources:
                metadata = manifest.get("metadata", {})
                name = metadata.get("name") if isinstance(metadata, dict) else None
                self._status.step = f"Applying {manifest.get('kind')}/{name}"
                await workflow.execute_activity(
                    crashable_activities.apply_workspace_resource,
                    ApplyWorkspaceResourceRequest(manifest=manifest),
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=crashable_activities.PROVISIONING_RETRY_POLICY,
                )

            for component in plan.components:
                await self._provision_lambda_component(component)

            self._status.step = "Selecting current worker versions"
            for component in plan.components:
                if not component.set_current:
                    continue
                await workflow.execute_activity(
                    external_resources.set_serverless_worker_deployment_current,
                    external_resources.SetServerlessWorkerDeploymentCurrentRequest(
                        project=component.project,
                        component=component.component,
                        temporal_secret_path=component.temporal_secret_path,
                        deployment_name=component.deployment_name,
                        build_id=component.build_id,
                    ),
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=LAMBDA_RETRY_POLICY,
                )

            self._status.step = "Waiting for workloads"
            await workflow.execute_activity(
                crashable_activities.wait_workspace_ready,
                WaitWorkspaceReadyRequest(
                    namespace=self._namespace,
                    deployment_names=plan.deployment_names,
                ),
                start_to_close_timeout=timedelta(minutes=6),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=crashable_activities.WAIT_READY_RETRY_POLICY,
            )
            for component in plan.components:
                await workflow.execute_activity(
                    external_resources.describe_serverless_worker_deployment_version,
                    external_resources.DescribeServerlessWorkerDeploymentVersionRequest(
                        project=component.project,
                        component=component.component,
                        temporal_secret_path=component.temporal_secret_path,
                        deployment_name=component.deployment_name,
                        build_id=component.build_id,
                    ),
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=LAMBDA_RETRY_POLICY,
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
                await asyncio.shield(cleanup)
                raise
            raise

    async def _provision_lambda_component(
        self,
        component: IsolatedLambdaComponentPlan,
    ) -> None:
        self._status.step = f"Creating Lambda {component.function_name}"
        roles = await workflow.execute_activity(
            activities.resolve_shared_lambda_roles,
            ResolveSharedLambdaRolesRequest(
                execution_role_name=component.execution_role_name,
                invoke_role_name=component.invoke_role_name,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=crashable_activities.PROVISIONING_RETRY_POLICY,
        )
        function = await workflow.execute_activity(
            external_resources.ensure_lambda_function,
            external_resources.EnsureLambdaFunctionRequest(
                project=component.project,
                component=component.component,
                function_name=component.function_name,
                execution_role_arn=roles.execution_role_arn,
                image_uri=component.image_uri,
                architecture=component.architecture,
                memory_mb=component.memory_mb,
                timeout_seconds=component.timeout_seconds,
                deployment_name=component.deployment_name,
                build_id=component.build_id,
                temporal_secret_path=component.temporal_secret_path,
                env=component.env,
                temporal_auth_mode=component.temporal_auth_mode,
                secret_env=component.secret_env,
                mtls_file_path_env=component.mtls_file_path_env,
                worker_deployment_env=component.worker_deployment_env,
                tags={"workspace-id": self._workspace_id},
            ),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=LAMBDA_RETRY_POLICY,
        )
        worker = WorkspaceLambdaWorker(
            component=component.component,
            function_name=function.function_name,
            function_arn=function.function_arn,
            function_version_arn=function.function_version_arn,
            deployment_name=component.deployment_name,
            build_id=component.build_id,
            invoke_role_name=component.invoke_role_name,
            temporal_secret_path=component.temporal_secret_path,
        )
        self._workers.append(worker)
        self._status.lambda_workers = [
            dataclasses.asdict(item) for item in self._workers
        ]
        await workflow.execute_activity(
            activities.update_workspace_lambda_invoke_policy,
            UpdateWorkspaceLambdaInvokePolicyRequest(
                role_name=component.invoke_role_name,
                function_arn=function.function_arn,
                function_version_arn=function.function_version_arn,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=LAMBDA_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.ensure_serverless_worker_deployment,
            external_resources.EnsureServerlessWorkerDeploymentRequest(
                project=component.project,
                component=component.component,
                temporal_secret_path=component.temporal_secret_path,
                deployment_name=component.deployment_name,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=LAMBDA_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.upsert_serverless_worker_deployment_version,
            external_resources.UpsertServerlessWorkerDeploymentVersionRequest(
                project=component.project,
                component=component.component,
                temporal_secret_path=component.temporal_secret_path,
                deployment_name=component.deployment_name,
                build_id=component.build_id,
                function_version_arn=function.function_version_arn,
                invoke_role_arn=roles.invoke_role_arn,
                invoke_role_external_id=component.external_id,
            ),
            start_to_close_timeout=SERVERLESS_WORKER_DEPLOYMENT_ACTIVITY_TIMEOUT,
            retry_policy=LAMBDA_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.validate_serverless_worker_compute_config,
            external_resources.ValidateServerlessWorkerComputeConfigRequest(
                project=component.project,
                component=component.component,
                temporal_secret_path=component.temporal_secret_path,
                deployment_name=component.deployment_name,
                build_id=component.build_id,
                function_version_arn=function.function_version_arn,
                invoke_role_arn=roles.invoke_role_arn,
                invoke_role_external_id=component.external_id,
            ),
            start_to_close_timeout=SERVERLESS_WORKER_DEPLOYMENT_ACTIVITY_TIMEOUT,
            retry_policy=LAMBDA_RETRY_POLICY,
        )

    async def _cleanup_lambda(self) -> None:
        if not self._workers:
            return
        self._status.step = "Deleting Lambda workers"
        await workflow.execute_activity(
            activities.delete_workspace_lambda_functions,
            DeleteWorkspaceLambdaFunctionsRequest(
                function_names=tuple(
                    worker.function_name for worker in self._workers
                )
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=crashable_activities.CLEANUP_RETRY_POLICY,
            summary="Delete workspace Lambda functions",
        )
        by_deployment: dict[str, list[WorkspaceLambdaWorker]] = defaultdict(list)
        for worker in self._workers:
            by_deployment[worker.deployment_name].append(worker)
        for deployment_name in sorted(by_deployment):
            versions = by_deployment[deployment_name]
            await workflow.execute_activity(
                external_resources.delete_serverless_worker_deployment,
                external_resources.DeleteServerlessWorkerDeploymentRequest(
                    temporal_secret_path=versions[0].temporal_secret_path,
                    deployment_name=deployment_name,
                    build_ids=tuple(version.build_id for version in versions),
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=WORKER_DEPLOYMENT_CLEANUP_RETRY_POLICY,
                summary=f"Delete workspace Worker Deployment {deployment_name}",
            )
        for worker in self._workers:
            await workflow.execute_activity(
                activities.update_workspace_lambda_invoke_policy,
                UpdateWorkspaceLambdaInvokePolicyRequest(
                    role_name=worker.invoke_role_name,
                    function_arn=worker.function_arn,
                    function_version_arn=worker.function_version_arn,
                    remove=True,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=crashable_activities.CLEANUP_RETRY_POLICY,
                summary=(
                    "Remove workspace Lambda ARNs from shared invoke policy "
                    f"{worker.invoke_role_name}"
                ),
            )

    async def _compensate_provision(
        self,
        compensations: list[Callable[[], Awaitable[Any]]],
    ) -> None:
        if not compensations:
            self._status.compensation_status = {"phase": "NotRequired"}
            return
        self._status.compensation_status = {"phase": "Running"}
        failures: list[dict[str, Any]] = []
        for compensate in reversed(compensations):
            try:
                await compensate()
            except Exception as error:  # noqa: BLE001 - best-effort compensation
                failure = errors.failure_details(error)
                failures.append(failure)
                workflow.logger.error(
                    "isolated lambda workspace compensation failed: %s",
                    failure["message"],
                )
        self._status.compensation_status = {
            "phase": "Error" if failures else "Completed",
            "failures": failures,
        }

    async def _run_ttl_loop(self, request: EnsureIsolatedWorkspaceRequest) -> None:
        while self._status.phase in LIVE_PHASES:
            if self._expires_at is None:
                return
            remaining = self._expires_at - workflow.now()
            if remaining <= timedelta(0):
                return
            if (
                self._status.phase in SETTLED_PHASES
                and not self._delete_requested
                and workflow.info().is_continue_as_new_suggested()
            ):
                workflow.continue_as_new(
                    EnsureIsolatedWorkspaceRequest(
                        demo_name=self._status.demo,
                        owner_email=request.owner_email,
                        ttl_seconds=request.ttl_seconds,
                        carried_already_provisioned=True,
                        carried_expires_at=self._status.expires_at,
                        carried_created_at=self._status.created_at,
                        carried_task_queue=self._status.task_queue,
                        carried_workspace_id=self._workspace_id,
                        carried_lambda_workers=tuple(self._workers),
                        carried_initial_current=dict(self._status.initial_current),
                    )
                )
            deadline_snapshot = self._deadline_version
            try:
                await workflow.wait_condition(
                    lambda: (
                        self._delete_requested
                        or self._deadline_version != deadline_snapshot
                    ),
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                return
            if self._delete_requested:
                return

    @workflow.update
    def ensure(self) -> IsolatedWorkspaceStatus:
        return self._snapshot()

    @workflow.update
    def extend(self) -> IsolatedWorkspaceStatus:
        if self._status.phase not in LIVE_PHASES:
            return self._snapshot()
        base = self._expires_at or workflow.now()
        self._expires_at = base + EXTEND_INTERVAL
        self._status.expires_at = self._expires_at.astimezone(UTC).isoformat()
        self._deadline_version += 1
        return self._snapshot()

    @workflow.update
    def delete(self) -> IsolatedWorkspaceStatus:
        self._delete_requested = True
        self._status.phase = "Deleting"
        return self._snapshot()

    @workflow.query
    def status(self) -> dict[str, Any]:
        return dataclasses.asdict(self._status)

    def _snapshot(self) -> IsolatedWorkspaceStatus:
        return dataclasses.replace(self._status)


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
