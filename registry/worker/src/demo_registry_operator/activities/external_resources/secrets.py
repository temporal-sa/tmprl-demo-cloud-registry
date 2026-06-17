import json
from typing import Any

from temporalio import activity

from demo_registry_operator.activities.external_resources.common import (
    error_code,
    isoformat_datetime,
)
from demo_registry_operator.activities.external_resources.models import (
    SecretRequirement,
    SecretsManagerVerificationResult,
    VerifySecretsManagerResourcesRequest,
)
from demo_registry_operator.activities.external_resources.planning import (
    aggregate_secret_phase,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    CONFIGURATION_ERROR,
    aws_application_error,
    non_retryable_error,
)


@activity.defn
def verify_secrets_manager_resources(
    request: VerifySecretsManagerResourcesRequest,
) -> SecretsManagerVerificationResult:
    config = WorkerConfig.from_env()

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    secretsmanager = boto3.session.Session(region_name=config.aws_region).client(
        "secretsmanager"
    )
    bootstrap = SecretRequirement(
        path=config.temporal_cloud_bootstrap_secret_path,
        required=request.temporal_bootstrap.required,
        managed_by=request.temporal_bootstrap.managed_by,
        required_properties=request.temporal_bootstrap.required_properties,
        components=request.temporal_bootstrap.components,
    )
    try:
        return SecretsManagerVerificationResult(
            verify_secrets_manager_paths(
                secretsmanager,
                request,
                temporal_bootstrap=bootstrap,
            )
        )
    except (BotoCoreError, ClientError) as error:
        raise aws_application_error(
            error,
            operation="Verify Secrets Manager paths",
        ) from error


def verify_secrets_manager_paths(
    secretsmanager: Any,
    request: VerifySecretsManagerResourcesRequest,
    *,
    temporal_bootstrap: SecretRequirement,
) -> dict[str, Any]:
    bootstrap_status = secret_status(secretsmanager, temporal_bootstrap)
    temporal_secret = secret_status(secretsmanager, request.temporal_credentials)
    temporal_mtls_ca = secret_status(secretsmanager, request.temporal_mtls_ca)
    user_managed = [
        secret_status(secretsmanager, requirement)
        for requirement in request.user_managed
    ]
    all_secret_statuses = [
        bootstrap_status,
        temporal_secret,
        temporal_mtls_ca,
        *user_managed,
    ]
    return {
        "phase": aggregate_secret_phase(all_secret_statuses),
        "checkedAt": request.checked_at,
        "secretPrefix": request.secret_prefix,
        "temporalBootstrap": bootstrap_status,
        "temporalCredentials": temporal_secret,
        "temporalMtlsCa": temporal_mtls_ca,
        "userManaged": user_managed,
    }


def secret_status(
    secretsmanager: Any,
    requirement: SecretRequirement,
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "path": requirement.path,
        "required": requirement.required,
        "managedBy": requirement.managed_by,
    }
    if requirement.required_properties:
        status["requiredProperties"] = list(requirement.required_properties)
    if requirement.components:
        status["components"] = sorted(requirement.components)

    if not requirement.required:
        return {
            **status,
            "phase": "NotRequired",
            "message": "No component currently requires this secret.",
        }

    try:
        response = secretsmanager.describe_secret(SecretId=requirement.path)
    except Exception as error:
        if error_code(error) == "ResourceNotFoundException":
            return {
                **status,
                "phase": "MissingValue",
                "message": "Secret value must be created outside git before ExternalSecret can sync it.",
            }
        raise aws_application_error(
            error,
            operation=f"Describe Secrets Manager secret {requirement.path}",
        ) from error

    rendered = {
        **status,
        "phase": "Ready",
        "arn": response.get("ARN"),
        "lastChangedDate": isoformat_datetime(response.get("LastChangedDate")),
    }
    if requirement.required_properties:
        rendered.update(
            verify_secret_properties(
                secretsmanager,
                requirement.path,
                required_properties=list(requirement.required_properties),
            )
        )
    return rendered


def verify_secret_properties(
    secretsmanager: Any,
    path: str,
    *,
    required_properties: list[str],
) -> dict[str, Any]:
    try:
        response = secretsmanager.get_secret_value(SecretId=path)
    except Exception as error:
        raise aws_application_error(
            error,
            operation=f"Read Secrets Manager secret {path}",
        ) from error

    secret_string = response.get("SecretString")
    if not isinstance(secret_string, str):
        return missing_properties_status(
            required_properties,
            message="Secret value must be a JSON object with the required properties.",
        )

    try:
        decoded = json.loads(secret_string)
    except json.JSONDecodeError as error:
        return missing_properties_status(
            required_properties,
            message=f"Secret value must be valid JSON: {error}",
        )

    if not isinstance(decoded, dict):
        return missing_properties_status(
            required_properties,
            message="Secret value must be a JSON object with the required properties.",
        )

    missing = [
        name
        for name in required_properties
        if not secret_property_present(decoded.get(name))
    ]
    if missing:
        return {
            "phase": "MissingProperties",
            "message": "Secret value is missing required JSON properties.",
            "missingProperties": missing,
        }

    return {
        "phase": "Ready",
        "verifiedProperties": sorted(required_properties),
    }


def missing_properties_status(
    required_properties: list[str],
    *,
    message: str,
) -> dict[str, Any]:
    return {
        "phase": "MissingProperties",
        "message": message,
        "missingProperties": sorted(required_properties),
    }


def secret_property_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True
