from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors, resources
from demo_registry_operator.activities import external_resources
from demo_registry_operator.activities.external_resources import serverless_names
from demo_registry_operator.workflows import messages


COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
LAMBDA_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


@workflow.defn
class DemoProjectServerlessLambdaWorkflow:
    """Deploy AWS Lambda-backed Temporal Serverless Workers for one reconcile."""

    @workflow.run
    async def run(
        self,
        request: messages.DemoProjectServerlessLambdaRequest,
    ) -> messages.DemoProjectServerlessLambdaResult:
        demo_project = request.demoProject
        serverless_components = resources.serverless_lambda_components(demo_project)
        if not serverless_components:
            return messages.DemoProjectServerlessLambdaResult(
                status={
                    "phase": "NotRequired",
                    "message": "No serverless Lambda workers configured.",
                    "components": {},
                }
            )

        images = build_images(request.buildStatus)
        build_id = resources.candidate_version(demo_project, images)
        if not build_id:
            return messages.DemoProjectServerlessLambdaResult(
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

        return messages.DemoProjectServerlessLambdaResult(
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

        project = resources.project_name(demo_project)
        deployment_name = resources.worker_deployment_name(component_name, component)
        function_name = serverless_names.lambda_function_name(project, component_name)
        execution_role_name = serverless_names.lambda_execution_role_name(
            project, component_name
        )
        invoke_role_name = serverless_names.temporal_lambda_invoke_role_name(
            project, component_name
        )
        external_id = serverless_names.temporal_invoke_external_id(
            project, component_name
        )
        serverless = resources.serverless_config(component)

        execution_role = await workflow.execute_activity(
            external_resources.ensure_lambda_execution_role,
            external_resources.EnsureLambdaExecutionRoleRequest(
                project=project,
                component=component_name,
                role_name=execution_role_name,
            ),
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=COMMON_RETRY_POLICY,
        )
        function = await workflow.execute_activity(
            external_resources.ensure_lambda_function,
            external_resources.EnsureLambdaFunctionRequest(
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
                temporal_secret_path=resources.temporal_secret_remote_path(
                    demo_project
                ),
                env=literal_env(component),
            ),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=LAMBDA_RETRY_POLICY,
        )
        invoke_role = await workflow.execute_activity(
            external_resources.ensure_temporal_lambda_invoke_role,
            external_resources.EnsureTemporalLambdaInvokeRoleRequest(
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
            external_resources.reconcile_serverless_worker_deployment,
            external_resources.ReconcileServerlessWorkerDeploymentRequest(
                project=project,
                component=component_name,
                temporal_secret_path=resources.temporal_secret_remote_path(
                    demo_project
                ),
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
