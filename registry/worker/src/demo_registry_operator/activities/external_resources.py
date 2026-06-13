from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.resources import (
    builds,
    components_for_user_secret,
    project_name,
    project_namespace,
    project_secret_prefix,
    spec_of,
    temporal_components,
    temporal_secret_remote_path,
    user_secret_names,
    user_secret_remote_path,
)


TEMPORAL_API_KEY_TTL_DAYS = 365
TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS = 60
TEMPORAL_SECRET_PROPERTIES = ["address", "namespace", "apiKey", "tls"]
TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES = [
    "TEMPORAL_CLOUD_API_KEY",
    "TEMPORAL_CLOUD_ACCOUNT_ID",
    "TEMPORAL_CLOUD_REGION",
]


@activity.defn
def reconcile_external_resources(demo_project: dict[str, Any]) -> dict[str, Any]:
    """Create or verify non-Kubernetes resources for one DemoProject.

    `plan` mode is non-mutating and records the resources the operator would
    manage. `aws` mode performs the safe AWS subset: create-or-verify ECR
    repositories and verify Secrets Manager paths. Temporal Cloud namespace and
    API key automation remains explicit manual setup until the provider contract
    is implemented.
    """

    config = WorkerConfig.from_env()
    activity.logger.info(
        "Reconciling external resources project=%s mode=%s",
        project_name(demo_project),
        config.external_resource_mode,
    )

    if config.external_resource_mode == "aws":
        return aws_external_resources(demo_project, config)
    return planned_external_resources(demo_project, config)


def planned_external_resources(
    demo_project: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "phase": "Planned",
        "mode": config.external_resource_mode,
        "checkedAt": now,
        "kubernetesNamespace": project_namespace(demo_project),
        "ecr": {
            "phase": "Planned",
            "region": config.aws_region,
            "accountId": config.aws_account_id,
            "repositories": [
                {
                    **repository_plan_entry(demo_project, build_name),
                    "phase": "Planned",
                    "action": "create-or-verify",
                }
                for build_name in sorted(builds(demo_project))
            ],
        },
        "secretsManager": planned_secrets_manager_status(demo_project, config),
        "temporalCloud": temporal_cloud_status(
            demo_project,
            config,
            phase="ManualSetupRequired",
        ),
    }


def aws_external_resources(
    demo_project: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    try:
        import boto3
        from botocore.exceptions import ClientError, BotoCoreError
    except ImportError as error:
        return {
            **planned_external_resources(demo_project, config),
            "phase": "Error",
            "mode": config.external_resource_mode,
            "checkedAt": now,
            "lastError": {
                "reason": "MissingDependency",
                "message": f"boto3 is required for aws external resource mode: {error}",
                "lastTransitionTime": now,
            },
        }

    session = boto3.session.Session(region_name=config.aws_region)
    ecr = session.client("ecr")
    secretsmanager = session.client("secretsmanager")

    repositories = []
    for build_name in sorted(builds(demo_project)):
        repository = repository_plan_entry(demo_project, build_name)
        try:
            repositories.append(ensure_ecr_repository(ecr, repository))
        except (BotoCoreError, ClientError) as error:
            repositories.append(
                {
                    **repository,
                    "phase": "Error",
                    "action": "error",
                    "message": str(error),
                }
            )

    secrets_status = verify_secrets_manager_paths(
        secretsmanager,
        demo_project,
        config,
        checked_at=now,
    )
    ecr_phase = aggregate_phase(repositories)
    temporal_status = temporal_cloud_status(
        demo_project,
        config,
        phase="ManualSetupRequired",
    )
    phase = aggregate_top_level_phase(
        [
            ecr_phase,
            secrets_status["phase"],
            temporal_status["phase"],
        ]
    )

    return {
        "phase": phase,
        "mode": config.external_resource_mode,
        "checkedAt": now,
        "kubernetesNamespace": project_namespace(demo_project),
        "ecr": {
            "phase": ecr_phase,
            "region": config.aws_region,
            "accountId": config.aws_account_id,
            "repositories": repositories,
        },
        "secretsManager": secrets_status,
        "temporalCloud": temporal_status,
    }


def ensure_ecr_repository(ecr: Any, repository: dict[str, Any]) -> dict[str, Any]:
    name = repository["name"]
    try:
        response = ecr.describe_repositories(repositoryNames=[name])
        repositories = response.get("repositories", [])
        existing = repositories[0] if repositories else {}
        return {
            **repository,
            "phase": "Ready",
            "action": "verified",
            "repositoryUri": existing.get("repositoryUri"),
        }
    except Exception as error:
        if error_code(error) != "RepositoryNotFoundException":
            raise

    response = ecr.create_repository(
        repositoryName=name,
        imageScanningConfiguration={"scanOnPush": True},
        tags=[
            {"Key": "managed-by", "Value": "demo-registry"},
            {"Key": "project", "Value": repository["project"]},
            {"Key": "build", "Value": repository["build"]},
        ],
    )
    created = response.get("repository", {})
    return {
        **repository,
        "phase": "Ready",
        "action": "created",
        "repositoryUri": created.get("repositoryUri"),
    }


def verify_secrets_manager_paths(
    secretsmanager: Any,
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    checked_at: str,
) -> dict[str, Any]:
    temporal_required = bool(temporal_components(demo_project))
    temporal_bootstrap = secret_status(
        secretsmanager,
        config.temporal_cloud_bootstrap_secret_path,
        required=temporal_required,
        managed_by="platform",
        required_properties=TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
    )
    temporal_secret = secret_status(
        secretsmanager,
        temporal_secret_remote_path(demo_project),
        required=temporal_required,
        managed_by="platform",
        required_properties=TEMPORAL_SECRET_PROPERTIES,
    )
    user_managed = [
        secret_status(
            secretsmanager,
            user_secret_remote_path(demo_project, secret_name),
            required=True,
            managed_by="project-owner",
            components=components_for_user_secret(demo_project, secret_name),
        )
        for secret_name in user_secret_names(demo_project)
    ]
    all_secret_statuses = [temporal_bootstrap, temporal_secret, *user_managed]
    phase = aggregate_secret_phase(all_secret_statuses)
    return {
        "phase": phase,
        "checkedAt": checked_at,
        "secretPrefix": project_secret_prefix(demo_project),
        "temporalBootstrap": temporal_bootstrap,
        "temporalCredentials": temporal_secret,
        "userManaged": user_managed,
    }


def secret_status(
    secretsmanager: Any,
    path: str,
    *,
    required: bool,
    managed_by: str,
    required_properties: list[str] | None = None,
    components: list[str] | None = None,
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "path": path,
        "required": required,
        "managedBy": managed_by,
    }
    if required_properties:
        status["requiredProperties"] = required_properties
    if components is not None:
        status["components"] = sorted(components)

    if not required:
        return {
            **status,
            "phase": "NotRequired",
            "message": "No component currently requires this secret.",
        }

    try:
        response = secretsmanager.describe_secret(SecretId=path)
    except Exception as error:
        if error_code(error) == "ResourceNotFoundException":
            return {
                **status,
                "phase": "MissingValue",
                "message": "Secret value must be created outside git before ExternalSecret can sync it.",
            }
        return {
            **status,
            "phase": "Error",
            "message": str(error),
        }

    rendered = {
        **status,
        "phase": "Ready",
        "arn": response.get("ARN"),
        "lastChangedDate": isoformat_datetime(response.get("LastChangedDate")),
    }
    if required_properties:
        rendered.update(
            verify_secret_properties(
                secretsmanager,
                path,
                required_properties=required_properties,
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
        return {
            "phase": "Error",
            "message": f"Secret exists but value properties could not be verified: {error}",
        }

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


def planned_secrets_manager_status(
    demo_project: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    temporal_required = bool(temporal_components(demo_project))
    return {
        "phase": "Planned",
        "secretPrefix": project_secret_prefix(demo_project),
        "temporalBootstrap": {
            "path": config.temporal_cloud_bootstrap_secret_path,
            "required": temporal_required,
            "managedBy": "platform",
            "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
            "phase": "Planned" if temporal_required else "NotRequired",
        },
        "temporalCredentials": {
            "path": temporal_secret_remote_path(demo_project),
            "required": temporal_required,
            "managedBy": "platform",
            "requiredProperties": TEMPORAL_SECRET_PROPERTIES,
            "phase": "Planned" if temporal_required else "NotRequired",
        },
        "userManaged": [
            {
                "path": user_secret_remote_path(demo_project, secret_name),
                "required": True,
                "managedBy": "project-owner",
                "components": sorted(
                    components_for_user_secret(demo_project, secret_name)
                ),
                "phase": "Planned",
            }
            for secret_name in user_secret_names(demo_project)
        ],
    }


def temporal_cloud_status(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    phase: str,
) -> dict[str, Any]:
    temporal_required = bool(temporal_components(demo_project))
    if not temporal_required:
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "manual",
            "message": "No component currently requires Temporal credentials.",
        }

    return {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": "manual",
        "accountId": config.temporal_cloud_account_id,
        "region": config.temporal_cloud_region,
        "message": (
            "Create or verify the Temporal Cloud namespace and namespace-scoped "
            "API key manually until provider automation is implemented."
        ),
        "namespace": {
            "name": temporal_namespace(demo_project),
            "action": "create-or-verify",
            "deletionPolicy": "retain-until-explicitly-approved",
        },
        "apiKey": {
            "action": "create-or-verify-or-rotate",
            "scope": "namespace",
            "namespace": temporal_namespace(demo_project),
            "ttlDays": TEMPORAL_API_KEY_TTL_DAYS,
            "rotateBeforeExpiryDays": TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS,
            "secretRemotePath": temporal_secret_remote_path(demo_project),
            "secretProperties": TEMPORAL_SECRET_PROPERTIES,
            "components": sorted(temporal_components(demo_project)),
        },
        "bootstrapCredential": {
            "secretRemotePath": config.temporal_cloud_bootstrap_secret_path,
            "kubernetesSecret": "temporal-cloud-bootstrap",
            "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
        },
    }


def repository_plan_entry(demo_project: dict[str, Any], build_name: str) -> dict[str, Any]:
    return {
        "name": image_repository_name(demo_project, build_name),
        "project": project_name(demo_project),
        "build": build_name,
        "components": components_for_build(demo_project, build_name),
    }


def image_repository_name(demo_project: dict[str, Any], build_name: str) -> str:
    derived = spec_of(demo_project).get("derived", {})
    prefix = "tmprl-cloud-demo"
    if isinstance(derived, dict) and isinstance(derived.get("imageRepositoryPrefix"), str):
        prefix = derived["imageRepositoryPrefix"]
    return f"{prefix}/{project_name(demo_project)}/{build_name}"


def components_for_build(demo_project: dict[str, Any], build_name: str) -> list[str]:
    return [
        component_name
        for component_name, component in sorted(spec_components(demo_project).items())
        if component.get("build") == build_name
    ]


def spec_components(demo_project: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = spec_of(demo_project).get("components")
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def temporal_namespace(demo_project: dict[str, Any]) -> str:
    derived = spec_of(demo_project).get("derived", {})
    if isinstance(derived, dict) and isinstance(derived.get("temporalNamespace"), str):
        return derived["temporalNamespace"]
    return project_name(demo_project)


def aggregate_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if "Error" in phases:
        return "Error"
    if "MissingValue" in phases:
        return "ValuesRequired"
    if phases and phases <= {"Ready", "NotRequired"}:
        return "Ready"
    if "Planned" in phases:
        return "Planned"
    return "Unknown"


def aggregate_secret_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if "Error" in phases:
        return "Error"
    if "MissingValue" in phases or "MissingProperties" in phases:
        return "ValuesRequired"
    if phases and phases <= {"Ready", "NotRequired"}:
        return "Ready"
    if "Planned" in phases:
        return "Planned"
    return "Unknown"


def aggregate_top_level_phase(phases: list[str]) -> str:
    if "Error" in phases:
        return "Error"
    if "ManualSetupRequired" in phases:
        return "ManualSetupRequired"
    if "ValuesRequired" in phases:
        return "ValuesRequired"
    if phases and set(phases) <= {"Ready", "NotRequired"}:
        return "Ready"
    if "Planned" in phases:
        return "Planned"
    return "Unknown"


def error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        error_info = response.get("Error")
        if isinstance(error_info, dict) and isinstance(error_info.get("Code"), str):
            return error_info["Code"]
    return None


def isoformat_datetime(value: Any) -> str | None:
    if hasattr(value, "astimezone"):
        return value.astimezone(UTC).isoformat()
    return None
