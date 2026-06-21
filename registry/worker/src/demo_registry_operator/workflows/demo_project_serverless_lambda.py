from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, timedelta
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
SERVERLESS_WORKER_DEPLOYMENT_ACTIVITY_TIMEOUT = timedelta(minutes=6)


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
        default_build_id = resources.candidate_version(demo_project, images)
        if not default_build_id:
            return messages.DemoProjectServerlessLambdaResult(
                status={
                    "phase": "ImagesMissing",
                    "message": "Serverless Lambda workers require resolved images.",
                    "components": sorted(serverless_components),
                }
            )

        compensations: list[Callable[[], Awaitable[Any]]] = []
        if should_compensate_serverless_lambda(demo_project):
            register_serverless_lambda_compensation(compensations, demo_project)

        component_statuses: dict[str, Any] = {}
        try:
            for component_name, component in sorted(serverless_components.items()):
                component_statuses[component_name] = await self._reconcile_component(
                    demo_project,
                    component_name,
                    component,
                    images,
                    default_build_id,
                )
        except asyncio.CancelledError:
            await asyncio.shield(
                asyncio.ensure_future(
                    run_serverless_lambda_compensations(compensations)
                )
            )
            raise
        except Exception:
            await run_serverless_lambda_compensations(compensations)
            raise

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
        default_build_id: str,
    ) -> dict[str, Any]:
        build_name = component.get("build")
        image_uri = images.get(build_name) if isinstance(build_name, str) else None
        if not isinstance(image_uri, str) or not image_uri:
            return {
                "phase": "ImagesMissing",
                "message": f"Build {build_name!r} does not have a resolved image.",
            }

        project = resources.project_name(demo_project)
        deployment_name = resources.serverless_worker_deployment_name(
            component_name,
            component,
        )
        build_id = resources.serverless_worker_build_id(component, default_build_id)
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
                secret_paths=secret_paths_for_lambda(demo_project),
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
                temporal_auth_mode=resources.temporal_auth_mode(demo_project),
                secret_env=secret_env(component, demo_project),
                mtls_file_path_env=mtls_file_path_env(component),
                worker_deployment_env=worker_deployment_env(
                    component,
                    deployment_name,
                    build_id,
                ),
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
            external_resources.ensure_serverless_worker_deployment,
            external_resources.EnsureServerlessWorkerDeploymentRequest(
                project=project,
                component=component_name,
                temporal_secret_path=resources.temporal_secret_remote_path(
                    demo_project
                ),
                deployment_name=deployment_name,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        ensure_status = worker_deployment.status

        worker_version = await workflow.execute_activity(
            external_resources.upsert_serverless_worker_deployment_version,
            external_resources.UpsertServerlessWorkerDeploymentVersionRequest(
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
            start_to_close_timeout=SERVERLESS_WORKER_DEPLOYMENT_ACTIVITY_TIMEOUT,
            retry_policy=COMMON_RETRY_POLICY,
        )
        compute_validation = await workflow.execute_activity(
            external_resources.validate_serverless_worker_compute_config,
            external_resources.ValidateServerlessWorkerComputeConfigRequest(
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
            start_to_close_timeout=SERVERLESS_WORKER_DEPLOYMENT_ACTIVITY_TIMEOUT,
            retry_policy=COMMON_RETRY_POLICY,
        )
        current_version = await workflow.execute_activity(
            external_resources.set_serverless_worker_deployment_current,
            external_resources.SetServerlessWorkerDeploymentCurrentRequest(
                project=project,
                component=component_name,
                temporal_secret_path=resources.temporal_secret_remote_path(
                    demo_project
                ),
                deployment_name=deployment_name,
                build_id=build_id,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        version_status = await workflow.execute_activity(
            external_resources.describe_serverless_worker_deployment_version,
            external_resources.DescribeServerlessWorkerDeploymentVersionRequest(
                project=project,
                component=component_name,
                temporal_secret_path=resources.temporal_secret_remote_path(
                    demo_project
                ),
                deployment_name=deployment_name,
                build_id=build_id,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        worker_deployment_status = serverless_worker_deployment_status(
            worker_version.status,
            compute_validation.status,
            current_version.status,
            version_status.status,
            ensure_status=ensure_status,
        )
        return {
            "phase": aggregate_phase(
                {
                    "executionRole": execution_role.status,
                    "function": function.status,
                    "invokeRole": invoke_role.status,
                    "workerDeployment": worker_deployment_status,
                }
            ),
            "deploymentName": deployment_name,
            "buildId": build_id,
            "image": image_uri,
            "steps": {
                "executionRole": execution_role.status,
                "function": function.status,
                "invokeRole": invoke_role.status,
                "workerDeployment": worker_deployment_status,
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


def secret_env(
    component: dict[str, Any],
    demo_project: dict[str, Any],
) -> tuple[external_resources.LambdaSecretEnvironmentVariable, ...]:
    refs: list[external_resources.LambdaSecretEnvironmentVariable] = []
    for item in resources.component_env_items(component):
        name = item.get("name")
        input_ref = resources.env_input_ref(item)
        if not isinstance(name, str) or not input_ref:
            continue

        temporal_key = temporal_secret_property(input_ref)
        if temporal_key:
            refs.append(
                external_resources.LambdaSecretEnvironmentVariable(
                    name=name,
                    secret_path=resources.temporal_secret_remote_path(demo_project),
                    property_name=temporal_key,
                )
            )
            continue

        user_secret_ref = resources.parse_user_secret_input_ref(input_ref)
        if user_secret_ref:
            secret_name, property_name = user_secret_ref
            refs.append(
                external_resources.LambdaSecretEnvironmentVariable(
                    name=name,
                    secret_path=resources.user_secret_remote_path(
                        demo_project,
                        secret_name,
                    ),
                    property_name=property_name,
                )
            )

    return tuple(refs)


def temporal_secret_property(input_ref: str) -> str | None:
    if not input_ref.startswith("temporal."):
        return None
    property_name = input_ref.removeprefix("temporal.")
    return property_name or None


def secret_paths_for_lambda(demo_project: dict[str, Any]) -> tuple[str, ...]:
    paths = {
        resources.project_secret_prefix(demo_project),
        resources.temporal_secret_remote_path(demo_project),
    }
    for secret_name in resources.user_secret_names(demo_project):
        paths.add(resources.user_secret_remote_path(demo_project, secret_name))
    return tuple(sorted(paths))


def mtls_file_path_env(
    component: dict[str, Any],
) -> tuple[external_resources.LambdaEnvironmentVariable, ...]:
    paths = {
        "certDir": "/tmp/tmprl-demo-cloud/temporal",
        "caCert": "/tmp/tmprl-demo-cloud/temporal/ca.crt",
        "clientCert": "/tmp/tmprl-demo-cloud/temporal/tls.crt",
        "clientKey": "/tmp/tmprl-demo-cloud/temporal/tls.key",
    }
    configured = resources.temporal_mtls_cert_files_config(component).get("pathEnv")
    path_envs = list(resources.TEMPORAL_MTLS_CERT_FILE_PATH_ENVS.items())
    if isinstance(configured, dict):
        path_envs.extend(
            (key, value)
            for key, value in configured.items()
            if key in paths and isinstance(value, str) and value
        )

    rendered: list[external_resources.LambdaEnvironmentVariable] = []
    seen: set[str] = set()
    for key, env_name in path_envs:
        if key not in paths or env_name in seen:
            continue
        rendered.append(
            external_resources.LambdaEnvironmentVariable(
                name=env_name,
                value=paths[key],
            )
        )
        seen.add(env_name)
    return tuple(rendered)


def worker_deployment_env(
    component: dict[str, Any],
    deployment_name: str,
    build_id: str,
) -> tuple[external_resources.LambdaEnvironmentVariable, ...]:
    configured = resources.serverless_worker_deployment_env_names(component)
    rendered: list[external_resources.LambdaEnvironmentVariable] = []
    seen: set[str] = set()
    for env_name in configured["deploymentName"]:
        if env_name in seen:
            continue
        rendered.append(
            external_resources.LambdaEnvironmentVariable(
                name=env_name,
                value=deployment_name,
            )
        )
        seen.add(env_name)
    for env_name in configured["buildId"]:
        if env_name in seen:
            continue
        rendered.append(
            external_resources.LambdaEnvironmentVariable(
                name=env_name,
                value=build_id,
            )
        )
        seen.add(env_name)
    return tuple(rendered)


def serverless_worker_deployment_status(
    version_status: dict[str, Any],
    validation_status: dict[str, Any],
    current_status: dict[str, Any],
    describe_status: dict[str, Any],
    *,
    ensure_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    steps = {
        "upsertVersion": version_status,
        "validateComputeConfig": validation_status,
        "setCurrent": current_status,
        "describeVersion": describe_status,
    }
    if ensure_status is not None:
        steps = {"ensureDeployment": ensure_status, **steps}
    status = {
        "phase": aggregate_phase(steps),
        "steps": steps,
    }
    for key in (
        "namespace",
        "deploymentName",
        "buildId",
        "functionVersionArn",
        "invokeRoleArn",
        "externalId",
        "computeProvider",
        "scaler",
        "taskQueues",
    ):
        for step_status in steps.values():
            value = step_status.get(key)
            if value is not None:
                status[key] = value
                break
    status["validation"] = validation_status
    return status


def should_compensate_serverless_lambda(demo_project: dict[str, Any]) -> bool:
    status = demo_project.get("status")
    if not isinstance(status, dict):
        return True
    if status.get("activeImageVersion"):
        return False
    serverless_status = status.get("serverlessWorkerStatus")
    return not (
        isinstance(serverless_status, dict)
        and serverless_status.get("phase") in {"Ready", "Promoted"}
    )


def register_serverless_lambda_compensation(
    compensations: list[Callable[[], Awaitable[Any]]],
    demo_project: dict[str, Any],
) -> None:
    compensations.append(
        lambda: workflow.execute_activity(
            external_resources.delete_serverless_lambda_resources,
            external_resources.DeleteServerlessLambdaResourcesRequest(
                checked_at=workflow.now().astimezone(UTC).isoformat(),
                demo_project=demo_project,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
    )


async def run_serverless_lambda_compensations(
    compensations: list[Callable[[], Awaitable[Any]]],
) -> None:
    for compensate in reversed(compensations):
        try:
            await compensate()
        except Exception as error:  # noqa: BLE001 - best-effort compensation
            workflow.logger.error("Serverless Lambda compensation failed: %s", error)


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
