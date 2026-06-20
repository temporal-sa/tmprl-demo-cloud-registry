from __future__ import annotations

import json
from typing import Any

from temporalio import activity

from demo_registry_operator import errors, resources
from demo_registry_operator.activities.external_resources import common, models
from demo_registry_operator.activities.external_resources import serverless_names
from demo_registry_operator.config import WorkerConfig


TEMPORAL_RUNTIME_SECRET_ARN_ENV = "TEMPORAL_RUNTIME_SECRET_ARN"
TEMPORAL_RUNTIME_SECRET_ID_ENV = "TEMPORAL_RUNTIME_SECRET_ID"
SECRET_ENV_MANIFEST_ENV = "TMPRL_DEMO_CLOUD_SECRET_ENV"
SECRET_FILE_MANIFEST_ENV = "TMPRL_DEMO_CLOUD_SECRET_FILES"
TEMPORAL_MTLS_TMP_DIR = "/tmp/tmprl-demo-cloud/temporal"
TEMPORAL_MTLS_CA_PATH = f"{TEMPORAL_MTLS_TMP_DIR}/ca.crt"
TEMPORAL_MTLS_CERT_PATH = f"{TEMPORAL_MTLS_TMP_DIR}/tls.crt"
TEMPORAL_MTLS_KEY_PATH = f"{TEMPORAL_MTLS_TMP_DIR}/tls.key"


@activity.defn
def ensure_lambda_function(
    request: models.EnsureLambdaFunctionRequest,
) -> models.EnsureLambdaFunctionResult:
    config = WorkerConfig.from_env()
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise errors.non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error

    session = boto3.session.Session(region_name=config.aws_region)
    lambda_client = session.client("lambda")
    secretsmanager = session.client("secretsmanager")
    try:
        temporal_secret = describe_secret_reference(
            secretsmanager,
            request.temporal_secret_path,
        )
        secret_env = describe_secret_environment(secretsmanager, request.secret_env)
        environment = lambda_environment(request, temporal_secret, secret_env)
        result = ensure_image_lambda(lambda_client, request, environment)
    except (BotoCoreError, ClientError) as error:
        raise lambda_application_error(
            error,
            operation=f"Create or update Lambda function {request.function_name}",
        ) from error

    status = {
        "phase": "Ready",
        "action": result["action"],
        "functionName": request.function_name,
        "functionArn": result["functionArn"],
        "functionVersionArn": result["functionVersionArn"],
        "version": result["version"],
        "imageUri": request.image_uri,
        "architecture": request.architecture,
        "memoryMb": request.memory_mb,
        "timeoutSeconds": request.timeout_seconds,
        "deploymentName": request.deployment_name,
        "buildId": request.build_id,
        "temporalAuthMode": request.temporal_auth_mode,
        "temporalSecretArn": temporal_secret["arn"],
    }
    return models.EnsureLambdaFunctionResult(
        status=status,
        function_name=request.function_name,
        function_arn=str(result["functionArn"]),
        function_version_arn=str(result["functionVersionArn"]),
        version=str(result["version"]),
    )


@activity.defn
def delete_serverless_lambda_resources(
    request: models.DeleteServerlessLambdaResourcesRequest,
) -> models.DeleteServerlessLambdaResourcesResult:
    config = WorkerConfig.from_env()
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return models.DeleteServerlessLambdaResourcesResult(
            {"phase": "Error", "message": f"boto3 is required: {error}"}
        )

    session = boto3.session.Session(region_name=config.aws_region)
    lambda_client = session.client("lambda")
    iam = session.client("iam")
    project = resources.project_name(request.demo_project)
    components = resources.serverless_lambda_components(request.demo_project)
    statuses: dict[str, Any] = {}
    for component_name in sorted(components):
        try:
            statuses[component_name] = delete_component_lambda_resources(
                lambda_client,
                iam,
                project=project,
                component=component_name,
            )
        except (BotoCoreError, ClientError) as error:
            statuses[component_name] = {
                "phase": "Error",
                "message": str(error),
            }

    if not statuses:
        phase = "NotRequired"
    elif any(status.get("phase") == "Error" for status in statuses.values()):
        phase = "Error"
    else:
        phase = "Deleted"
    return models.DeleteServerlessLambdaResourcesResult(
        {
            "phase": phase,
            "checkedAt": request.checked_at,
            "components": statuses,
        }
    )


def delete_component_lambda_resources(
    lambda_client: Any,
    iam: Any,
    *,
    project: str,
    component: str,
) -> dict[str, Any]:
    function = delete_lambda_function(
        lambda_client,
        serverless_names.lambda_function_name(project, component),
    )
    execution_role = delete_iam_role(
        iam,
        serverless_names.lambda_execution_role_name(project, component),
    )
    invoke_role = delete_iam_role(
        iam,
        serverless_names.temporal_lambda_invoke_role_name(project, component),
    )
    phase = (
        "Error"
        if "error"
        in {
            function.get("action"),
            execution_role.get("action"),
            invoke_role.get("action"),
        }
        else "Deleted"
    )
    return {
        "phase": phase,
        "function": function,
        "executionRole": execution_role,
        "invokeRole": invoke_role,
    }


def delete_lambda_function(lambda_client: Any, function_name: str) -> dict[str, Any]:
    try:
        lambda_client.delete_function(FunctionName=function_name)
        return {"name": function_name, "action": "deleted"}
    except Exception as error:
        if common.error_code(error) == "ResourceNotFoundException":
            return {"name": function_name, "action": "not-found"}
        return {
            "name": function_name,
            "action": "error",
            "message": str(error),
        }


def delete_iam_role(iam: Any, role_name: str) -> dict[str, Any]:
    try:
        for policy_name in inline_role_policy_names(iam, role_name):
            iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
        iam.delete_role(RoleName=role_name)
        return {"name": role_name, "action": "deleted"}
    except Exception as error:
        if common.error_code(error) == "NoSuchEntity":
            return {"name": role_name, "action": "not-found"}
        return {
            "name": role_name,
            "action": "error",
            "message": str(error),
        }


def inline_role_policy_names(iam: Any, role_name: str) -> list[str]:
    names: list[str] = []
    paginator = iam.get_paginator("list_role_policies")
    for page in paginator.paginate(RoleName=role_name):
        names.extend(
            name
            for name in page.get("PolicyNames", [])
            if isinstance(name, str)
        )
    return names


def ensure_image_lambda(
    lambda_client: Any,
    request: models.EnsureLambdaFunctionRequest,
    environment: dict[str, str],
) -> dict[str, str]:
    action = "verified"
    existing = get_function(lambda_client, request.function_name)
    if existing is None:
        lambda_client.create_function(
            FunctionName=request.function_name,
            PackageType="Image",
            Code={"ImageUri": request.image_uri},
            Role=request.execution_role_arn,
            Architectures=[request.architecture],
            MemorySize=request.memory_mb,
            Timeout=request.timeout_seconds,
            Environment={"Variables": environment},
            Description=lambda_description(request),
            Tags={
                "managed-by": "demo-registry",
                "project": request.project,
                "component": request.component,
            },
        )
        wait_for_function_active(lambda_client, request.function_name)
        action = "created"
    else:
        configuration = existing.get("Configuration", {})
        code = existing.get("Code", {})
        if lambda_configuration_needs_update(configuration, request, environment):
            lambda_client.update_function_configuration(
                FunctionName=request.function_name,
                Role=request.execution_role_arn,
                Architectures=[request.architecture],
                MemorySize=request.memory_mb,
                Timeout=request.timeout_seconds,
                Environment={"Variables": environment},
                Description=lambda_description(request),
            )
            wait_for_function_updated(lambda_client, request.function_name)
            action = "updated"

        if code.get("ImageUri") != request.image_uri:
            lambda_client.update_function_code(
                FunctionName=request.function_name,
                ImageUri=request.image_uri,
            )
            wait_for_function_updated(lambda_client, request.function_name)
            action = "updated"

    latest = get_function_configuration(lambda_client, request.function_name)
    published = matching_published_version(lambda_client, request.function_name, latest)
    if published is None:
        published = lambda_client.publish_version(
            FunctionName=request.function_name,
            Description=lambda_description(request),
        )
        if action == "verified":
            action = "published"

    return {
        "action": action,
        "functionArn": unqualified_function_arn(str(latest["FunctionArn"])),
        "functionVersionArn": str(published["FunctionArn"]),
        "version": str(published["Version"]),
    }


def get_function(lambda_client: Any, function_name: str) -> dict[str, Any] | None:
    try:
        return lambda_client.get_function(FunctionName=function_name)
    except Exception as error:
        if common.error_code(error) == "ResourceNotFoundException":
            return None
        raise


def get_function_configuration(
    lambda_client: Any,
    function_name: str,
) -> dict[str, Any]:
    return lambda_client.get_function_configuration(FunctionName=function_name)


def lambda_configuration_needs_update(
    configuration: dict[str, Any],
    request: models.EnsureLambdaFunctionRequest,
    environment: dict[str, str],
) -> bool:
    return any(
        (
            configuration.get("Role") != request.execution_role_arn,
            configuration.get("Timeout") != request.timeout_seconds,
            configuration.get("MemorySize") != request.memory_mb,
            configuration.get("Architectures") != [request.architecture],
            configuration.get("Description") != lambda_description(request),
            configuration.get("Environment", {}).get("Variables", {}) != environment,
        )
    )


def matching_published_version(
    lambda_client: Any,
    function_name: str,
    latest: dict[str, Any],
) -> dict[str, Any] | None:
    paginator = lambda_client.get_paginator("list_versions_by_function")
    matches: list[dict[str, Any]] = []
    for page in paginator.paginate(FunctionName=function_name):
        for version in page.get("Versions", []):
            if version.get("Version") == "$LATEST":
                continue
            if published_version_matches_latest(version, latest):
                matches.append(version)
    if not matches:
        return None
    return sorted(matches, key=lambda item: int(item["Version"]))[-1]


def published_version_matches_latest(
    version: dict[str, Any],
    latest: dict[str, Any],
) -> bool:
    compared_fields = (
        "CodeSha256",
        "Role",
        "Timeout",
        "MemorySize",
        "Description",
    )
    if any(version.get(field) != latest.get(field) for field in compared_fields):
        return False
    if version.get("Architectures") != latest.get("Architectures"):
        return False
    return (
        version.get("Environment", {}).get("Variables", {})
        == latest.get("Environment", {}).get("Variables", {})
    )


def wait_for_function_active(lambda_client: Any, function_name: str) -> None:
    lambda_client.get_waiter("function_active_v2").wait(FunctionName=function_name)


def wait_for_function_updated(lambda_client: Any, function_name: str) -> None:
    lambda_client.get_waiter("function_updated_v2").wait(FunctionName=function_name)


def read_json_secret(secretsmanager: Any, secret_id: str) -> dict[str, Any]:
    try:
        response = secretsmanager.get_secret_value(SecretId=secret_id)
    except Exception as error:
        if common.error_code(error) == "ResourceNotFoundException":
            raise errors.non_retryable_error(
                f"Temporal runtime secret {secret_id!r} does not exist",
                error_type=errors.CONFIGURATION_ERROR,
            ) from error
        raise
    secret_string = response.get("SecretString")
    if not isinstance(secret_string, str):
        raise errors.non_retryable_error(
            f"Temporal runtime secret {secret_id!r} must be a JSON object",
            error_type=errors.CONFIGURATION_ERROR,
        )
    try:
        decoded = json.loads(secret_string)
    except json.JSONDecodeError as error:
        raise errors.non_retryable_error(
            f"Temporal runtime secret {secret_id!r} is not valid JSON: {error}",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error
    if not isinstance(decoded, dict):
        raise errors.non_retryable_error(
            f"Temporal runtime secret {secret_id!r} must be a JSON object",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return decoded


def describe_secret_reference(secretsmanager: Any, secret_id: str) -> dict[str, str]:
    try:
        response = secretsmanager.describe_secret(SecretId=secret_id)
    except Exception as error:
        if common.error_code(error) == "ResourceNotFoundException":
            raise errors.non_retryable_error(
                f"Secrets Manager secret {secret_id!r} does not exist",
                error_type=errors.CONFIGURATION_ERROR,
            ) from error
        raise
    arn = response.get("ARN")
    name = response.get("Name")
    if not isinstance(arn, str) or not arn:
        raise errors.non_retryable_error(
            f"Secrets Manager secret {secret_id!r} is missing an ARN",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return {
        "arn": arn,
        "name": str(name or secret_id),
    }


def describe_secret_environment(
    secretsmanager: Any,
    refs: tuple[models.LambdaSecretEnvironmentVariable, ...],
) -> tuple[dict[str, str], ...]:
    cache: dict[str, dict[str, str]] = {}
    described: list[dict[str, str]] = []
    for ref in refs:
        secret = cache.get(ref.secret_path)
        if secret is None:
            secret = describe_secret_reference(secretsmanager, ref.secret_path)
            cache[ref.secret_path] = secret
        described.append(
            {
                "name": ref.name,
                "secretArn": secret["arn"],
                "secretId": secret["name"],
                "property": ref.property_name,
            }
        )
    return tuple(described)


def lambda_environment(
    request: models.EnsureLambdaFunctionRequest,
    temporal_secret: dict[str, str],
    secret_env: tuple[dict[str, str], ...] = (),
) -> dict[str, str]:
    environment = {key: str(value) for key, value in sorted(request.env.items())}
    environment.update(
        {
            "HOME": "/tmp",
            TEMPORAL_RUNTIME_SECRET_ARN_ENV: temporal_secret["arn"],
            TEMPORAL_RUNTIME_SECRET_ID_ENV: temporal_secret["name"],
        }
    )
    for item in default_worker_deployment_env(request):
        environment[item.name] = item.value
    for item in request.worker_deployment_env:
        environment[item.name] = item.value
    environment.pop("TEMPORAL_API_KEY", None)
    environment.pop("TEMPORAL_TLS_CLIENT_CERT_DATA", None)
    environment.pop("TEMPORAL_TLS_CLIENT_KEY_DATA", None)

    secret_env_manifest = [
        *temporal_secret_env_manifest(request, temporal_secret),
        *secret_env,
    ]
    if secret_env_manifest:
        environment[SECRET_ENV_MANIFEST_ENV] = json.dumps(
            secret_env_manifest,
            sort_keys=True,
            separators=(",", ":"),
        )

    secret_file_manifest = temporal_secret_file_manifest(request, temporal_secret)
    if secret_file_manifest:
        environment[SECRET_FILE_MANIFEST_ENV] = json.dumps(
            secret_file_manifest,
            sort_keys=True,
            separators=(",", ":"),
        )
        for item in request.mtls_file_path_env:
            environment[item.name] = item.value
    return environment


def default_worker_deployment_env(
    request: models.EnsureLambdaFunctionRequest,
) -> tuple[models.LambdaEnvironmentVariable, ...]:
    return (
        models.LambdaEnvironmentVariable(
            name="TEMPORAL_DEPLOYMENT_NAME",
            value=request.deployment_name,
        ),
        models.LambdaEnvironmentVariable(
            name="TEMPORAL_WORKER_DEPLOYMENT_NAME",
            value=request.deployment_name,
        ),
        models.LambdaEnvironmentVariable(
            name="TEMPORAL_WORKER_BUILD_ID",
            value=request.build_id,
        ),
    )


def temporal_secret_env_manifest(
    request: models.EnsureLambdaFunctionRequest,
    temporal_secret: dict[str, str],
) -> list[dict[str, str]]:
    refs = [
        {
            "name": "TEMPORAL_ADDRESS",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
            "property": "address",
        },
        {
            "name": "TEMPORAL_NAMESPACE",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
            "property": "namespace",
        },
        {
            "name": "TEMPORAL_TLS",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
            "property": "tls",
        },
    ]
    if request.temporal_auth_mode == "apiKey":
        refs.append(
            {
                "name": "TEMPORAL_API_KEY",
                "secretArn": temporal_secret["arn"],
                "secretId": temporal_secret["name"],
                "property": "apiKey",
            }
        )
    return refs


def temporal_secret_file_manifest(
    request: models.EnsureLambdaFunctionRequest,
    temporal_secret: dict[str, str],
) -> list[dict[str, str]]:
    if request.temporal_auth_mode != "mtls":
        return []
    return [
        {
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
            "property": "caCert",
            "path": TEMPORAL_MTLS_CA_PATH,
        },
        {
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
            "property": "clientCert",
            "path": TEMPORAL_MTLS_CERT_PATH,
        },
        {
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
            "property": "clientKey",
            "path": TEMPORAL_MTLS_KEY_PATH,
        },
    ]


def lambda_description(request: models.EnsureLambdaFunctionRequest) -> str:
    return (
        "tmprl-demo.cloud Temporal Serverless Worker "
        f"{request.project}/{request.component} {request.build_id}"
    )


def unqualified_function_arn(function_arn: str) -> str:
    marker = ":function:"
    if marker not in function_arn:
        return function_arn
    prefix, suffix = function_arn.split(marker, 1)
    function_name = suffix.split(":", 1)[0]
    return f"{prefix}{marker}{function_name}"


def lambda_application_error(error: Exception, *, operation: str) -> Exception:
    message = str(error)
    if "cannot be assumed by Lambda" in message or "ResourceConflictException" in message:
        return errors.retryable_provider_error(f"{operation} failed: {error}")
    return errors.aws_application_error(error, operation=operation)
