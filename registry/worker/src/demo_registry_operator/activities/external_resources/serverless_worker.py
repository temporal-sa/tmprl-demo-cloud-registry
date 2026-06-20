from __future__ import annotations

import json
import uuid
from typing import Any, Mapping

from google.protobuf import field_mask_pb2
from temporalio import activity
from temporalio.api.compute.v1 import config_pb2, provider_pb2, scaler_pb2
from temporalio.api.deployment.v1 import message_pb2 as deployment_pb2
from temporalio.api.workflowservice.v1 import request_response_pb2 as workflowservice_pb2
from temporalio.client import Client, TLSConfig
from temporalio.converter import default
from temporalio.service import RPCError, RPCStatusCode

from demo_registry_operator import errors
from demo_registry_operator.activities.external_resources import aws_lambda, models
from demo_registry_operator.config import WorkerConfig


@activity.defn
async def ensure_serverless_worker_deployment(
    request: models.EnsureServerlessWorkerDeploymentRequest,
) -> models.EnsureServerlessWorkerDeploymentResult:
    client, namespace = await connect_to_project_namespace_from_secret_path(
        request.temporal_secret_path,
    )
    identity = "tmprl-demo-cloud-registry"
    try:
        action = await ensure_worker_deployment(
            client,
            namespace=namespace,
            deployment_name=request.deployment_name,
            identity=identity,
        )
    except RPCError as error:
        raise temporal_service_application_error(
            error,
            operation=(
                "Ensure Temporal Serverless Worker Deployment "
                f"{request.deployment_name}"
            ),
        ) from error

    return models.EnsureServerlessWorkerDeploymentResult(
        status={
            "phase": "Ready",
            "action": action,
            "namespace": namespace,
            "deploymentName": request.deployment_name,
        }
    )


@activity.defn
async def upsert_serverless_worker_deployment_version(
    request: models.UpsertServerlessWorkerDeploymentVersionRequest,
) -> models.UpsertServerlessWorkerDeploymentVersionResult:
    client, namespace = await connect_to_project_namespace_from_secret_path(
        request.temporal_secret_path,
    )
    identity = "tmprl-demo-cloud-registry"
    deployment_version = deployment_pb2.WorkerDeploymentVersion(
        deployment_name=request.deployment_name,
        build_id=request.build_id,
    )
    scaling_group = compute_scaling_group(request)

    try:
        action = await create_or_update_worker_deployment_version(
            client,
            namespace=namespace,
            deployment_version=deployment_version,
            scaling_group=scaling_group,
            identity=identity,
        )
    except RPCError as error:
        raise temporal_service_application_error(
            error,
            operation=(
                "Upsert Temporal Serverless Worker Deployment Version "
                f"{request.deployment_name}.{request.build_id}"
            ),
        ) from error

    return models.UpsertServerlessWorkerDeploymentVersionResult(
        status={
            "phase": "Ready",
            "action": action,
            "namespace": namespace,
            "deploymentName": request.deployment_name,
            "buildId": request.build_id,
            "functionVersionArn": request.function_version_arn,
            "invokeRoleArn": request.invoke_role_arn,
            "externalId": request.invoke_role_external_id,
            "computeProvider": "aws-lambda",
            "scaler": "no-sync",
        }
    )


@activity.defn
async def validate_serverless_worker_compute_config(
    request: models.ValidateServerlessWorkerComputeConfigRequest,
) -> models.ValidateServerlessWorkerComputeConfigResult:
    client, namespace = await connect_to_project_namespace_from_secret_path(
        request.temporal_secret_path,
    )
    identity = "tmprl-demo-cloud-registry"
    deployment_version = deployment_pb2.WorkerDeploymentVersion(
        deployment_name=request.deployment_name,
        build_id=request.build_id,
    )
    scaling_group = compute_scaling_group(request)

    try:
        await validate_worker_deployment_compute_config(
            client,
            namespace=namespace,
            deployment_version=deployment_version,
            scaling_group=scaling_group,
            identity=identity,
        )
    except RPCError as error:
        raise temporal_service_application_error(
            error,
            operation=(
                "Validate Temporal Serverless Worker Deployment Version "
                f"{request.deployment_name}.{request.build_id}"
            ),
        ) from error

    return models.ValidateServerlessWorkerComputeConfigResult(
        status={
            "phase": "Ready",
            "namespace": namespace,
            "deploymentName": request.deployment_name,
            "buildId": request.build_id,
            "functionVersionArn": request.function_version_arn,
            "invokeRoleArn": request.invoke_role_arn,
            "externalId": request.invoke_role_external_id,
            "computeProvider": "aws-lambda",
            "scaler": "no-sync",
            "message": "Temporal validated the Lambda compute configuration.",
        }
    )


@activity.defn
async def set_serverless_worker_deployment_current(
    request: models.SetServerlessWorkerDeploymentCurrentRequest,
) -> models.SetServerlessWorkerDeploymentCurrentResult:
    client, namespace = await connect_to_project_namespace_from_secret_path(
        request.temporal_secret_path,
    )
    identity = "tmprl-demo-cloud-registry"
    try:
        await set_worker_deployment_current(
            client,
            namespace=namespace,
            deployment_name=request.deployment_name,
            build_id=request.build_id,
            identity=identity,
        )
    except RPCError as error:
        raise temporal_service_application_error(
            error,
            operation=(
                "Set Temporal Serverless Worker Deployment Current Version "
                f"{request.deployment_name}.{request.build_id}"
            ),
        ) from error

    return models.SetServerlessWorkerDeploymentCurrentResult(
        status={
            "phase": "Ready",
            "action": "set-current",
            "namespace": namespace,
            "deploymentName": request.deployment_name,
            "buildId": request.build_id,
        }
    )


@activity.defn
async def describe_serverless_worker_deployment_version(
    request: models.DescribeServerlessWorkerDeploymentVersionRequest,
) -> models.DescribeServerlessWorkerDeploymentVersionResult:
    client, namespace = await connect_to_project_namespace_from_secret_path(
        request.temporal_secret_path,
    )
    deployment_version = deployment_pb2.WorkerDeploymentVersion(
        deployment_name=request.deployment_name,
        build_id=request.build_id,
    )
    try:
        version_status = await describe_worker_deployment_version_status(
            client,
            namespace=namespace,
            deployment_version=deployment_version,
        )
    except RPCError as error:
        raise temporal_service_application_error(
            error,
            operation=(
                "Describe Temporal Serverless Worker Deployment Version "
                f"{request.deployment_name}.{request.build_id}"
            ),
        ) from error

    return models.DescribeServerlessWorkerDeploymentVersionResult(
        status={
            "phase": "Ready",
            "namespace": namespace,
            "deploymentName": request.deployment_name,
            "buildId": request.build_id,
            **version_status,
        }
    )


async def connect_to_project_namespace_from_secret_path(
    temporal_secret_path: str,
) -> tuple[Client, str]:
    config = WorkerConfig.from_env()
    try:
        import boto3
    except ImportError as error:
        raise errors.non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error

    secretsmanager = boto3.session.Session(region_name=config.aws_region).client(
        "secretsmanager"
    )
    temporal_secret = aws_lambda.read_json_secret(secretsmanager, temporal_secret_path)
    client = await connect_to_project_namespace(temporal_secret)
    return client, str(temporal_secret["namespace"])


async def ensure_worker_deployment(
    client: Client,
    *,
    namespace: str,
    deployment_name: str,
    identity: str,
) -> str:
    try:
        await client.workflow_service.create_worker_deployment(
            workflowservice_pb2.CreateWorkerDeploymentRequest(
                namespace=namespace,
                deployment_name=deployment_name,
                identity=identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
        )
        return "created"
    except RPCError as error:
        if error.status != RPCStatusCode.ALREADY_EXISTS:
            raise
    return "verified"


async def create_or_update_worker_deployment_version(
    client: Client,
    *,
    namespace: str,
    deployment_version: deployment_pb2.WorkerDeploymentVersion,
    scaling_group: config_pb2.ComputeConfigScalingGroup,
    identity: str,
) -> str:
    compute_config = config_pb2.ComputeConfig(
        scaling_groups={"default": scaling_group},
    )
    try:
        await client.workflow_service.create_worker_deployment_version(
            workflowservice_pb2.CreateWorkerDeploymentVersionRequest(
                namespace=namespace,
                deployment_version=deployment_version,
                compute_config=compute_config,
                identity=identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
        )
        return "created"
    except RPCError as error:
        if error.status != RPCStatusCode.ALREADY_EXISTS:
            raise

    await client.workflow_service.update_worker_deployment_version_compute_config(
        workflowservice_pb2.UpdateWorkerDeploymentVersionComputeConfigRequest(
            namespace=namespace,
            deployment_version=deployment_version,
            compute_config_scaling_groups={
                "default": compute_scaling_group_update(scaling_group)
            },
            identity=identity,
            request_id=str(uuid.uuid4()),
        ),
        retry=True,
    )
    return "updated"


async def validate_worker_deployment_compute_config(
    client: Client,
    *,
    namespace: str,
    deployment_version: deployment_pb2.WorkerDeploymentVersion,
    scaling_group: config_pb2.ComputeConfigScalingGroup,
    identity: str,
) -> None:
    await client.workflow_service.validate_worker_deployment_version_compute_config(
        workflowservice_pb2.ValidateWorkerDeploymentVersionComputeConfigRequest(
            namespace=namespace,
            deployment_version=deployment_version,
            compute_config_scaling_groups={
                "default": compute_scaling_group_update(scaling_group)
            },
            identity=identity,
        ),
        retry=True,
    )


async def set_worker_deployment_current(
    client: Client,
    *,
    namespace: str,
    deployment_name: str,
    build_id: str,
    identity: str,
) -> None:
    conflict_token = ""
    try:
        response = await client.workflow_service.describe_worker_deployment(
            workflowservice_pb2.DescribeWorkerDeploymentRequest(
                namespace=namespace,
                deployment_name=deployment_name,
            ),
            retry=True,
        )
        conflict_token = response.conflict_token
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:
            raise

    await client.workflow_service.set_worker_deployment_current_version(
        workflowservice_pb2.SetWorkerDeploymentCurrentVersionRequest(
            namespace=namespace,
            deployment_name=deployment_name,
            build_id=build_id,
            conflict_token=conflict_token,
            identity=identity,
            ignore_missing_task_queues=True,
            allow_no_pollers=True,
        ),
        retry=True,
    )


async def describe_worker_deployment_version_status(
    client: Client,
    *,
    namespace: str,
    deployment_version: deployment_pb2.WorkerDeploymentVersion,
) -> dict[str, Any]:
    response = await client.workflow_service.describe_worker_deployment_version(
        workflowservice_pb2.DescribeWorkerDeploymentVersionRequest(
            namespace=namespace,
            deployment_version=deployment_version,
            report_task_queue_stats=True,
        ),
        retry=True,
    )
    task_queues = [
        {
            "name": task_queue.name,
            "type": task_queue_type(task_queue.type),
        }
        for task_queue in response.version_task_queues
    ]
    return {
        "taskQueues": task_queues,
    }


def compute_scaling_group(
    request: (
        models.UpsertServerlessWorkerDeploymentVersionRequest
        | models.ValidateServerlessWorkerComputeConfigRequest
    ),
) -> config_pb2.ComputeConfigScalingGroup:
    return config_pb2.ComputeConfigScalingGroup(
        provider=provider_pb2.ComputeProvider(
            type="aws-lambda",
            details=aws_lambda_provider_details_payload(request),
        ),
        scaler=scaler_pb2.ComputeScaler(type="no-sync"),
    )


def compute_scaling_group_update(
    scaling_group: config_pb2.ComputeConfigScalingGroup,
) -> config_pb2.ComputeConfigScalingGroupUpdate:
    return config_pb2.ComputeConfigScalingGroupUpdate(
        scaling_group=scaling_group,
        update_mask=field_mask_pb2.FieldMask(paths=["provider.details"]),
    )


def aws_lambda_provider_details_payload(
    request: (
        models.UpsertServerlessWorkerDeploymentVersionRequest
        | models.ValidateServerlessWorkerComputeConfigRequest
    ),
) -> Any:
    details = {
        "arn": request.function_version_arn,
        "role": request.invoke_role_arn,
        "role_external_id": request.invoke_role_external_id,
    }
    payloads = default().payload_converter.to_payloads([details])
    return payloads[0]


async def connect_to_project_namespace(secret: dict[str, Any]) -> Client:
    required = ("address", "namespace", "tls")
    missing = [key for key in required if key not in secret]
    if missing:
        raise errors.non_retryable_error(
            "Temporal runtime secret is missing required properties: "
            + ", ".join(missing),
            error_type=errors.CONFIGURATION_ERROR,
        )

    rpc_metadata: Mapping[str, str] | None = None
    tls: bool | TLSConfig = secret_bool(secret.get("tls"), default=True)
    if secret.get("apiKey"):
        rpc_metadata = {"authorization": f"Bearer {secret['apiKey']}"}
    else:
        mtls_missing = [key for key in ("clientCert", "clientKey") if not secret.get(key)]
        if mtls_missing:
            raise errors.non_retryable_error(
                "Temporal mTLS runtime secret is missing required properties: "
                + ", ".join(mtls_missing),
                error_type=errors.CONFIGURATION_ERROR,
            )
        tls = TLSConfig(
            client_cert=str(secret["clientCert"]).encode("utf-8"),
            client_private_key=str(secret["clientKey"]).encode("utf-8"),
        )

    return await Client.connect(
        str(secret["address"]),
        namespace=str(secret["namespace"]),
        tls=tls,
        rpc_metadata=rpc_metadata,
    )


def secret_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    return default


def task_queue_type(value: int) -> str:
    return {
        1: "workflow",
        2: "activity",
        3: "nexus",
    }.get(value, "unspecified")


def temporal_service_application_error(
    error: RPCError,
    *,
    operation: str,
) -> Exception:
    message = f"{operation} failed: {error}"
    if error.status in {
        RPCStatusCode.UNAUTHENTICATED,
        RPCStatusCode.PERMISSION_DENIED,
    }:
        return errors.non_retryable_error(
            message,
            error_type=errors.PROVIDER_AUTHENTICATION_ERROR,
        )
    if error.status in {
        RPCStatusCode.INVALID_ARGUMENT,
        RPCStatusCode.FAILED_PRECONDITION,
        RPCStatusCode.NOT_FOUND,
        RPCStatusCode.UNIMPLEMENTED,
    }:
        return errors.non_retryable_error(
            message,
            error_type=errors.PROVIDER_CONFIGURATION_ERROR,
        )
    if error.status in {
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.RESOURCE_EXHAUSTED,
        RPCStatusCode.ABORTED,
        RPCStatusCode.UNAVAILABLE,
    }:
        return errors.retryable_provider_error(message)
    return errors.non_retryable_error(
        message,
        error_type=errors.PROVIDER_CONFIGURATION_ERROR,
        details=json.dumps({"status": str(error.status)}),
    )
