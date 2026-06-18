from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.external_resources import (
        EnsureLambdaExecutionRoleRequest,
        EnsureLambdaFunctionRequest,
        EnsureTemporalLambdaInvokeRoleRequest,
        ReconcileServerlessWorkerDeploymentRequest,
        ensure_lambda_execution_role,
        ensure_lambda_function,
        ensure_temporal_lambda_invoke_role,
        reconcile_serverless_worker_deployment,
    )
    from demo_registry_operator.activities.external_resources.serverless_names import (
        lambda_execution_role_name,
        lambda_function_name,
        temporal_invoke_external_id,
        temporal_lambda_invoke_role_name,
    )
    from demo_registry_operator.errors import NON_RETRYABLE_ERROR_TYPES
    from demo_registry_operator.resources import (
        candidate_version,
        project_name,
        serverless_lambda_components,
        serverless_config,
        temporal_secret_remote_path,
        worker_deployment_name,
        worker_versioning,
    )
    from demo_registry_operator.workflows.messages import (
        DemoProjectServerlessLambdaRequest,
        DemoProjectServerlessLambdaResult,
    )


COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
LAMBDA_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)


@workflow.defn
class DemoProjectServerlessLambdaWorkflow:
    """Deploy AWS Lambda-backed Temporal Serverless Workers for one reconcile."""

    @workflow.run
    async def run(
        self,
        request: DemoProjectServerlessLambdaRequest,
    ) -> DemoProjectServerlessLambdaResult:
        demo_project = request.demoProject
        serverless_components = serverless_lambda_components(demo_project)
        if not serverless_components:
            return DemoProjectServerlessLambdaResult(
                status={
                    "phase": "NotRequired",
                    "message": "No serverless Lambda workers configured.",
                    "components": {},
                }
            )

        images = build_images(request.buildStatus)
        build_id = candidate_version(demo_project, images)
        if not build_id:
            return DemoProjectServerlessLambdaResult(
                status={
                    "phase": "ImagesMissing",
                    "message": "Serverless Lambda workers require resolved images.",
                    "components": sorted(serverless_components),
                }
            )

        component_statuses: dict[str, Any] = {}
        for component_name, component in sorted(serverless_components.items()):
            component_statuses[component_name] = await self._reconcile_component(
                demo_project,
                component_name,
                component,
                images,
                build_id,
            )

        return DemoProjectServerlessLambdaResult(
            status={
                "phase": aggregate_phase(component_statuses),
                "message": "Serverless Lambda workers reconciled.",
                "components": component_statuses,
            }
        )

    async def _reconcile_component(
        self,
        demo_project: dict[str, Any],
        component_name: str,
        component: dict[str, Any],
        images: dict[str, str],
        build_id: str,
    ) -> dict[str, Any]:
        build_name = component.get("build")
        image_uri = images.get(build_name) if isinstance(build_name, str) else None
        if not isinstance(image_uri, str) or not image_uri:
            return {
                "phase": "ImagesMissing",
                "message": f"Build {build_name!r} does not have a resolved image.",
            }

        versioning = worker_versioning(component)
        task_queue = versioning.get("taskQueue") if isinstance(versioning, dict) else None
        if not isinstance(task_queue, str) or not task_queue:
            return {
                "phase": "InvalidConfiguration",
                "message": "Serverless Lambda worker requires worker.versioning.taskQueue.",
            }

        project = project_name(demo_project)
        deployment_name = worker_deployment_name(component_name, component)
        function_name = lambda_function_name(project, component_name)
        execution_role_name = lambda_execution_role_name(project, component_name)
        invoke_role_name = temporal_lambda_invoke_role_name(project, component_name)
        external_id = temporal_invoke_external_id(project, component_name)
        serverless = serverless_config(component)

        execution_role = await workflow.execute_activity(
            ensure_lambda_execution_role,
            EnsureLambdaExecutionRoleRequest(
                project=project,
                component=component_name,
                role_name=execution_role_name,
            ),
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=COMMON_RETRY_POLICY,
        )
        function = await workflow.execute_activity(
            ensure_lambda_function,
            EnsureLambdaFunctionRequest(
                project=project,
                component=component_name,
                function_name=function_name,
                execution_role_arn=execution_role.role_arn,
                image_uri=image_uri,
                architecture=str(serverless.get("architecture", "x86_64")),
                memory_mb=int(serverless.get("memoryMb", 512)),
                timeout_seconds=int(serverless.get("timeoutSeconds", 600)),
                deployment_name=deployment_name,
                build_id=build_id,
                task_queue=task_queue,
                temporal_secret_path=temporal_secret_remote_path(demo_project),
                env=literal_env(component),
            ),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=LAMBDA_RETRY_POLICY,
        )
        invoke_role = await workflow.execute_activity(
            ensure_temporal_lambda_invoke_role,
            EnsureTemporalLambdaInvokeRoleRequest(
                project=project,
                component=component_name,
                role_name=invoke_role_name,
                function_arn=function.function_arn,
                function_version_arn=function.function_version_arn,
                external_id=external_id,
            ),
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=COMMON_RETRY_POLICY,
        )
        worker_deployment = await workflow.execute_activity(
            reconcile_serverless_worker_deployment,
            ReconcileServerlessWorkerDeploymentRequest(
                project=project,
                component=component_name,
                temporal_secret_path=temporal_secret_remote_path(demo_project),
                deployment_name=deployment_name,
                build_id=build_id,
                function_version_arn=function.function_version_arn,
                invoke_role_arn=invoke_role.role_arn,
                invoke_role_external_id=invoke_role.external_id,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return {
            "phase": aggregate_phase(
                {
                    "executionRole": execution_role.status,
                    "function": function.status,
                    "invokeRole": invoke_role.status,
                    "workerDeployment": worker_deployment.status,
                }
            ),
            "deploymentName": deployment_name,
            "buildId": build_id,
            "taskQueue": task_queue,
            "image": image_uri,
            "steps": {
                "executionRole": execution_role.status,
                "function": function.status,
                "invokeRole": invoke_role.status,
                "workerDeployment": worker_deployment.status,
            },
        }


def build_images(build_status: dict[str, Any]) -> dict[str, str]:
    images = build_status.get("images")
    if not isinstance(images, dict):
        return {}
    return {
        str(build_name): str(image)
        for build_name, image in images.items()
        if isinstance(build_name, str) and isinstance(image, str) and image
    }


def literal_env(component: dict[str, Any]) -> dict[str, str]:
    env = component.get("env")
    if not isinstance(env, list):
        return {}
    rendered: dict[str, str] = {}
    for item in env:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if isinstance(name, str) and "value" in item:
            rendered[name] = str(item["value"])
    return rendered


def aggregate_phase(statuses: dict[str, Any]) -> str:
    phases = {
        status.get("phase")
        for status in statuses.values()
        if isinstance(status, dict)
    }
    if "Error" in phases:
        return "Error"
    if "ImagesMissing" in phases or "InvalidConfiguration" in phases:
        return "Error"
    if phases and phases <= {"Ready", "NotRequired"}:
        return "Ready"
    return "Unknown"

