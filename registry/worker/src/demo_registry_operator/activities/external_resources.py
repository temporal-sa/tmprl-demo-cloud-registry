from __future__ import annotations

import json
import shutil
import subprocess
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
    user_secret_required_properties,
    user_secret_remote_path,
)


TEMPORAL_API_KEY_TTL_DAYS = 30
TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS = 10
TEMPORAL_SECRET_PROPERTIES = ["address", "namespace", "apiKey", "tls"]
TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES = [
    "TEMPORAL_CLOUD_API_KEY",
    "TEMPORAL_CLOUD_ACCOUNT_ID",
    "TEMPORAL_CLOUD_REGION",
]
TCLD_COMMAND_TIMEOUT_SECONDS = 30


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

    # Run the Temporal Cloud reconcile first: in cloud-api mode it creates the
    # namespace/service-account/API key and writes the runtime secret, which the
    # secret verification below then sees as present.
    temporal_status = temporal_cloud_status(
        demo_project,
        config,
        phase="ManualSetupRequired",
        checked_at=now,
    )
    secrets_status = verify_secrets_manager_paths(
        secretsmanager,
        demo_project,
        config,
        checked_at=now,
    )
    ecr_phase = aggregate_phase(repositories)
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
            required_properties=user_secret_required_properties(
                demo_project,
                secret_name,
            ),
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
                "requiredProperties": user_secret_required_properties(
                    demo_project,
                    secret_name,
                ),
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
    checked_at: str | None = None,
) -> dict[str, Any]:
    temporal_required = bool(temporal_components(demo_project))
    if not temporal_required:
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": config.temporal_cloud_mode,
            "message": "No component currently requires Temporal credentials.",
        }

    if config.temporal_cloud_mode == "cloud-api" and config.external_resource_mode == "aws":
        return temporal_cloud_api_status(
            demo_project,
            config,
            checked_at=checked_at or datetime.now(UTC).isoformat(),
        )

    if config.temporal_cloud_mode == "tcld-verify":
        return temporal_cloud_tcld_status(
            demo_project,
            config,
            checked_at=checked_at or datetime.now(UTC).isoformat(),
        )

    return {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": config.temporal_cloud_mode,
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
        "tcldCommandPlan": temporal_cloud_tcld_command_plan(
            demo_project,
            region=config.temporal_cloud_region,
        ),
        "bootstrapCredential": {
            "secretRemotePath": config.temporal_cloud_bootstrap_secret_path,
            "kubernetesSecret": "temporal-cloud-bootstrap",
            "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
        },
    }


def temporal_cloud_api_status(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    checked_at: str,
) -> dict[str, Any]:
    """Create or verify the Temporal Cloud namespace, scoped service account, and
    runtime API key via the Cloud Operations API, writing the runtime credential
    to AWS Secrets Manager."""

    import asyncio

    from demo_registry_operator.activities.temporal_cloud import (
        SecretsManagerStore,
        run_cloud_api_reconcile,
    )

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return temporal_cloud_api_error(
            checked_at, f"boto3 is required for cloud-api Temporal Cloud mode: {error}"
        )

    session = boto3.session.Session(region_name=config.aws_region)
    store = SecretsManagerStore(session.client("secretsmanager"))
    try:
        bootstrap = store.read_json(config.temporal_cloud_bootstrap_secret_path)
    except (BotoCoreError, ClientError) as error:
        return temporal_cloud_api_error(
            checked_at, f"Could not read Temporal Cloud bootstrap secret: {error}"
        )

    if not isinstance(bootstrap, dict) or not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return {
            "phase": "ManualSetupRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "message": (
                "Temporal Cloud bootstrap secret is missing or lacks "
                "TEMPORAL_CLOUD_API_KEY; create it before enabling cloud-api mode."
            ),
            "bootstrapCredential": {
                "secretRemotePath": config.temporal_cloud_bootstrap_secret_path,
                "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
            },
        }

    def heartbeat(detail: Any) -> None:
        try:
            activity.heartbeat(detail)
        except Exception:  # noqa: BLE001 - heartbeat is best-effort
            pass

    try:
        return asyncio.run(
            run_cloud_api_reconcile(
                demo_project,
                bootstrap=bootstrap,
                secret_store=store,
                checked_at=checked_at,
                target_host=config.temporal_cloud_ops_host,
                api_version=config.temporal_cloud_api_version,
                heartbeat=heartbeat,
            )
        )
    except Exception as error:  # noqa: BLE001 - surface as status, do not crash reconcile
        return temporal_cloud_api_error(checked_at, str(error))


def temporal_cloud_api_error(checked_at: str, message: str) -> dict[str, Any]:
    return {
        "phase": "Error",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "message": "Temporal Cloud cloud-api reconcile failed.",
        "lastError": {
            "reason": "TemporalCloudReconcileFailed",
            "message": message,
            "lastTransitionTime": checked_at,
        },
    }


def temporal_cloud_tcld_status(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    checked_at: str,
) -> dict[str, Any]:
    namespace_status = verify_temporal_namespace_with_tcld(demo_project)
    namespace_phase = namespace_status.get("phase")
    phase = "ManualSetupRequired"
    if namespace_phase == "Error":
        phase = "Error"
    message = (
        "Temporal Cloud namespace exists; create or verify the namespace-scoped "
        "service account/API key manually."
        if namespace_phase == "Ready"
        else "Temporal Cloud namespace/API key setup still requires manual action."
    )
    if namespace_phase == "Error":
        message = "Temporal Cloud namespace verification failed."

    return {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": config.temporal_cloud_mode,
        "checkedAt": checked_at,
        "accountId": config.temporal_cloud_account_id,
        "region": config.temporal_cloud_region,
        "message": message,
        "namespace": {
            "name": temporal_namespace(demo_project),
            "action": "create-or-verify",
            "deletionPolicy": "retain-until-explicitly-approved",
            **namespace_status,
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
            "phase": "ManualSetupRequired",
        },
        "tcldCommandPlan": temporal_cloud_tcld_command_plan(
            demo_project,
            region=config.temporal_cloud_region,
        ),
        "bootstrapCredential": {
            "secretRemotePath": config.temporal_cloud_bootstrap_secret_path,
            "kubernetesSecret": "temporal-cloud-bootstrap",
            "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
        },
    }


def verify_temporal_namespace_with_tcld(demo_project: dict[str, Any]) -> dict[str, Any]:
    command = [
        "tcld",
        "namespace",
        "get",
        "--namespace",
        temporal_namespace(demo_project),
    ]
    if shutil.which("tcld") is None:
        return {
            "phase": "Error",
            "command": command,
            "message": "tcld is not installed in the registry operator runtime.",
        }

    try:
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
            timeout=TCLD_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return {
            "phase": "Error",
            "command": command,
            "message": "Timed out verifying Temporal Cloud namespace with tcld.",
        }
    except OSError as error:
        return {
            "phase": "Error",
            "command": command,
            "message": f"Failed to run tcld: {error}",
        }
    if completed.returncode == 0:
        return {
            "phase": "Ready",
            "command": command,
            "message": "Temporal Cloud namespace verified with tcld.",
        }

    output = trim_command_output(completed.stdout, completed.stderr)
    if "not found" in output.lower():
        return {
            "phase": "Missing",
            "command": command,
            "message": "Temporal Cloud namespace was not found.",
        }
    return {
        "phase": "Error",
        "command": command,
        "message": output or f"tcld exited with status {completed.returncode}.",
    }


def trim_command_output(stdout: str, stderr: str, *, limit: int = 1000) -> str:
    output = "\n".join(part.strip() for part in (stderr, stdout) if part.strip())
    return output if len(output) <= limit else output[-limit:]


def temporal_cloud_tcld_command_plan(
    demo_project: dict[str, Any],
    *,
    region: str | None,
) -> list[dict[str, Any]]:
    namespace = temporal_namespace(demo_project)
    service_account_name = f"{project_name(demo_project)}-runtime"
    return [
        {
            "step": "verify-namespace",
            "command": [
                "tcld",
                "namespace",
                "get",
                "--namespace",
                namespace,
            ],
        },
        {
            "step": "create-namespace",
            "command": [
                "tcld",
                "namespace",
                "create",
                "--namespace",
                namespace,
                "--region",
                region or "<temporal-region>",
                "--auth-method",
                "api_key",
                "--enable-delete-protection",
            ],
        },
        {
            "step": "create-scoped-service-account",
            "command": [
                "tcld",
                "service-account",
                "create-scoped",
                "--name",
                service_account_name,
                "--namespace-permission",
                f"{namespace}=Write",
            ],
        },
        {
            "step": "create-service-account-api-key",
            "command": [
                "tcld",
                "apikey",
                "create",
                "--name",
                f"{project_name(demo_project)}-runtime",
                "--description",
                f"tmprl-demo.cloud runtime key for {project_name(demo_project)}",
                "--duration",
                f"{TEMPORAL_API_KEY_TTL_DAYS}d",
                "--service-account-id",
                "<service-account-id>",
            ],
        },
        {
            "step": "list-service-account-api-keys",
            "command": [
                "tcld",
                "apikey",
                "list",
                "--owner-type",
                "service-account",
                "--owner-id",
                "<service-account-id>",
            ],
        },
    ]


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
