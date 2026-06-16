from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from demo_registry_operator import derive
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    CONFIGURATION_ERROR,
    PROVIDER_AUTHENTICATION_ERROR,
    PROVIDER_CONFIGURATION_ERROR,
    aws_application_error,
    non_retryable_error,
    retryable_provider_error,
)
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


@activity.defn
def plan_external_resources(demo_project: dict[str, Any]) -> dict[str, Any]:
    """Return a side-effect-free external-resource reconcile plan."""

    config = WorkerConfig.from_env()
    checked_at = datetime.now(UTC).isoformat()
    activity.logger.info(
        "Reconciling external resources project=%s mode=%s",
        project_name(demo_project),
        config.external_resource_mode,
    )

    return {
        "action": "aws",
        "demoProject": demo_project,
        "mode": config.external_resource_mode,
        "checkedAt": checked_at,
        "kubernetesNamespace": project_namespace(demo_project),
    }


@activity.defn
def reconcile_ecr_repositories(plan: dict[str, Any]) -> dict[str, Any]:
    demo_project = demo_project_from_plan(plan)
    config = WorkerConfig.from_env()
    checked_at = string_field(plan, "checkedAt")

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    repositories = []
    for build_name in sorted(builds(demo_project)):
        repository = repository_plan_entry(demo_project, build_name)
        try:
            repositories.append(ensure_ecr_repository(ecr, repository))
        except (BotoCoreError, ClientError) as error:
            raise aws_application_error(
                error,
                operation=f"Reconcile ECR repository {repository['name']}",
            ) from error

    return {
        "phase": aggregate_phase(repositories),
        "region": config.aws_region,
        "accountId": config.aws_account_id,
        "checkedAt": checked_at,
        "repositories": repositories,
    }


@activity.defn
def reconcile_temporal_cloud_resource(plan: dict[str, Any]) -> dict[str, Any]:
    demo_project = demo_project_from_plan(plan)
    config = WorkerConfig.from_env()
    return temporal_cloud_status(
        demo_project,
        config,
        phase="ManualSetupRequired",
        checked_at=string_field(plan, "checkedAt"),
        fail_activity_on_error=True,
    )


@activity.defn
def verify_secrets_manager_resources(plan: dict[str, Any]) -> dict[str, Any]:
    demo_project = demo_project_from_plan(plan)
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
    try:
        return verify_secrets_manager_paths(
            secretsmanager,
            demo_project,
            config,
            checked_at=string_field(plan, "checkedAt"),
        )
    except (BotoCoreError, ClientError) as error:
        raise aws_application_error(
            error,
            operation="Verify Secrets Manager paths",
        ) from error


@activity.defn
def delete_temporal_cloud_api_key_resource(plan: dict[str, Any]) -> dict[str, Any]:
    return run_temporal_cloud_finalization_step(
        plan,
        runner_name="run_cloud_api_delete_api_key",
        operation="Temporal Cloud API key teardown",
    )


@activity.defn
def delete_temporal_cloud_service_account_resource(
    plan: dict[str, Any],
) -> dict[str, Any]:
    return run_temporal_cloud_finalization_step(
        plan,
        runner_name="run_cloud_api_delete_service_account",
        operation="Temporal Cloud service account teardown",
    )


@activity.defn
def disable_temporal_cloud_namespace_delete_protection_resource(
    plan: dict[str, Any],
) -> dict[str, Any]:
    return run_temporal_cloud_finalization_step(
        plan,
        runner_name="run_cloud_api_disable_namespace_delete_protection",
        operation="Temporal Cloud namespace delete-protection teardown",
    )


@activity.defn
def delete_temporal_cloud_namespace_resource(plan: dict[str, Any]) -> dict[str, Any]:
    return run_temporal_cloud_finalization_step(
        plan,
        runner_name="run_cloud_api_delete_namespace",
        operation="Temporal Cloud namespace teardown",
    )


@activity.defn
def delete_temporal_cloud_runtime_secret_resource(
    plan: dict[str, Any],
) -> dict[str, Any]:
    demo_project = demo_project_from_plan(plan)
    config = WorkerConfig.from_env()
    if not temporal_components(demo_project):
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": string_field(plan, "checkedAt"),
            "message": "No Temporal Cloud resources to delete.",
        }
    if config.temporal_cloud_mode != "cloud-api":
        return {"phase": "Skipped", "mode": config.temporal_cloud_mode}

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for cloud-api Temporal Cloud teardown: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    from demo_registry_operator.activities.temporal_cloud import (
        SecretsManagerStore,
        delete_temporal_cloud_runtime_secret,
    )

    store = SecretsManagerStore(
        boto3.session.Session(region_name=config.aws_region).client("secretsmanager")
    )
    try:
        return delete_temporal_cloud_runtime_secret(
            demo_project,
            secret_store=store,
            checked_at=string_field(plan, "checkedAt"),
        )
    except (BotoCoreError, ClientError) as error:
        raise aws_application_error(
            error,
            operation="Delete Temporal Cloud runtime secret",
        ) from error


@activity.defn
def delete_project_ecr_repositories_resource(plan: dict[str, Any]) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    if config.external_resource_mode == "aws":
        return delete_project_ecr_repositories(
            demo_project_from_plan(plan),
            config,
            string_field(plan, "checkedAt"),
        )
    return {"phase": "Skipped", "mode": config.external_resource_mode}


def external_resource_finalization_status(
    plan: dict[str, Any],
    *,
    temporal_status: dict[str, Any],
    ecr_status: dict[str, Any],
) -> dict[str, Any]:
    phase = "Deleted"
    if temporal_status.get("phase") == "Error" or ecr_status.get("phase") == "Error":
        phase = "Error"
    elif {temporal_status.get("phase"), ecr_status.get("phase")} <= {"Skipped"}:
        phase = "Skipped"
    return {
        "phase": phase,
        "checkedAt": string_field(plan, "checkedAt"),
        "temporalCloud": temporal_status,
        "ecr": ecr_status,
    }


def temporal_cloud_finalization_status(
    plan: dict[str, Any],
    *,
    api_key_status: dict[str, Any],
    service_account_status: dict[str, Any],
    namespace_protection_status: dict[str, Any],
    namespace_status: dict[str, Any],
    runtime_secret_status: dict[str, Any],
) -> dict[str, Any]:
    statuses = [
        api_key_status,
        service_account_status,
        namespace_protection_status,
        namespace_status,
        runtime_secret_status,
    ]
    phases = {status.get("phase") for status in statuses}
    phase = "Deleted"
    if "Error" in phases:
        phase = "Error"
    elif phases <= {"Skipped", "NotRequired"}:
        phase = "Skipped"

    deleted: dict[str, Any] = {}
    namespace: dict[str, Any] = {}
    for status in statuses:
        status_deleted = status.get("deleted")
        if isinstance(status_deleted, dict):
            deleted.update(status_deleted)
        status_namespace = status.get("namespace")
        if isinstance(status_namespace, dict) and status_namespace.get("name"):
            namespace = status_namespace

    return {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": string_field(plan, "checkedAt"),
        "namespace": namespace,
        "deleted": deleted,
        "steps": {
            "apiKey": api_key_status,
            "serviceAccount": service_account_status,
            "namespaceDeleteProtection": namespace_protection_status,
            "namespace": namespace_status,
            "secret": runtime_secret_status,
        },
    }


def external_resources_status(
    plan: dict[str, Any],
    *,
    ecr_status: dict[str, Any],
    temporal_status: dict[str, Any],
    secrets_status: dict[str, Any],
) -> dict[str, Any]:
    phases = [
        str(ecr_status.get("phase")),
        str(secrets_status.get("phase")),
        str(temporal_status.get("phase")),
    ]
    return {
        "phase": aggregate_top_level_phase(phases),
        "mode": string_field(plan, "mode"),
        "checkedAt": string_field(plan, "checkedAt"),
        "kubernetesNamespace": string_field(plan, "kubernetesNamespace"),
        "ecr": ecr_status,
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
        raise aws_application_error(
            error,
            operation=f"Describe Secrets Manager secret {path}",
        ) from error

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


def temporal_cloud_status(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    phase: str,
    checked_at: str | None = None,
    fail_activity_on_error: bool = False,
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
            fail_activity_on_error=fail_activity_on_error,
        )

    return {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": config.temporal_cloud_mode,
        "accountId": config.temporal_cloud_account_id,
        "region": config.temporal_cloud_region,
        "message": (
            "Temporal Cloud reconciliation requires cloud-api mode with aws "
            "external resource reconciliation."
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


def temporal_cloud_api_status(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    checked_at: str,
    fail_activity_on_error: bool = False,
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
        if fail_activity_on_error:
            raise non_retryable_error(
                f"boto3 is required for cloud-api Temporal Cloud mode: {error}",
                error_type=CONFIGURATION_ERROR,
            ) from error
        return temporal_cloud_api_error(
            checked_at, f"boto3 is required for cloud-api Temporal Cloud mode: {error}"
        )

    session = boto3.session.Session(region_name=config.aws_region)
    store = SecretsManagerStore(session.client("secretsmanager"))
    try:
        bootstrap = store.read_json(config.temporal_cloud_bootstrap_secret_path)
    except (BotoCoreError, ClientError) as error:
        if fail_activity_on_error:
            raise aws_application_error(
                error,
                operation="Read Temporal Cloud bootstrap secret",
            ) from error
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
        if fail_activity_on_error:
            message = str(error)
            if authentication_error_message(message):
                raise non_retryable_error(
                    f"Temporal Cloud cloud-api reconcile failed: {message}",
                    error_type=PROVIDER_AUTHENTICATION_ERROR,
                ) from error
            raise retryable_provider_error(
                f"Temporal Cloud cloud-api reconcile failed: {message}"
            ) from error
        return temporal_cloud_api_error(checked_at, str(error))


def finalize_external_resources(
    demo_project: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    """Delete a removed project's external resources: Temporal Cloud namespace,
    scoped service account, API key, runtime secret (cloud-api mode), and ECR
    repositories (aws mode). Idempotent — already-gone resources are fine."""

    now = datetime.now(UTC).isoformat()
    result: dict[str, Any] = {"phase": "Deleted", "checkedAt": now}

    if config.temporal_cloud_mode == "cloud-api":
        result["temporalCloud"] = finalize_temporal_cloud_via_api(
            demo_project,
            config,
            now,
            fail_activity_on_error=True,
        )
    else:
        result["temporalCloud"] = {"phase": "Skipped", "mode": config.temporal_cloud_mode}

    if config.external_resource_mode == "aws":
        result["ecr"] = delete_project_ecr_repositories(demo_project, config, now)
    else:
        result["ecr"] = {"phase": "Skipped", "mode": config.external_resource_mode}

    return result


def finalize_temporal_cloud_via_api(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    checked_at: str,
    *,
    fail_activity_on_error: bool = False,
) -> dict[str, Any]:
    import asyncio

    from demo_registry_operator.activities.temporal_cloud import (
        SecretsManagerStore,
        run_cloud_api_finalize,
    )

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        if fail_activity_on_error:
            raise non_retryable_error(
                f"boto3 is required for cloud-api Temporal Cloud teardown: {error}",
                error_type=CONFIGURATION_ERROR,
            ) from error
        return temporal_cloud_api_error(checked_at, f"boto3 is required: {error}")

    session = boto3.session.Session(region_name=config.aws_region)
    store = SecretsManagerStore(session.client("secretsmanager"))
    try:
        bootstrap = store.read_json(config.temporal_cloud_bootstrap_secret_path)
    except (BotoCoreError, ClientError) as error:
        if fail_activity_on_error:
            raise aws_application_error(
                error,
                operation="Read Temporal Cloud bootstrap secret for teardown",
            ) from error
        return temporal_cloud_api_error(checked_at, f"Could not read bootstrap secret: {error}")
    if not isinstance(bootstrap, dict) or not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        if fail_activity_on_error:
            raise non_retryable_error(
                (
                    "Temporal Cloud cloud-api teardown requires bootstrap secret "
                    f"{config.temporal_cloud_bootstrap_secret_path} with "
                    "TEMPORAL_CLOUD_API_KEY"
                ),
                error_type=PROVIDER_CONFIGURATION_ERROR,
            )
        return {
            "phase": "Skipped",
            "mode": "cloud-api",
            "message": "No bootstrap credential; skipping Temporal Cloud teardown.",
        }

    def heartbeat(detail: Any) -> None:
        try:
            activity.heartbeat(detail)
        except Exception:  # noqa: BLE001
            pass

    try:
        return asyncio.run(
            run_cloud_api_finalize(
                demo_project,
                bootstrap=bootstrap,
                secret_store=store,
                checked_at=checked_at,
                target_host=config.temporal_cloud_ops_host,
                api_version=config.temporal_cloud_api_version,
                heartbeat=heartbeat,
            )
        )
    except Exception as error:  # noqa: BLE001
        if fail_activity_on_error:
            raise temporal_cloud_application_error(
                error,
                operation="Temporal Cloud cloud-api teardown",
            ) from error
        return temporal_cloud_api_error(checked_at, str(error))


def run_temporal_cloud_finalization_step(
    plan: dict[str, Any],
    *,
    runner_name: str,
    operation: str,
) -> dict[str, Any]:
    demo_project = demo_project_from_plan(plan)
    config = WorkerConfig.from_env()
    checked_at = string_field(plan, "checkedAt")
    if not temporal_components(demo_project):
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "message": "No Temporal Cloud resources to delete.",
        }
    if config.temporal_cloud_mode != "cloud-api":
        return {"phase": "Skipped", "mode": config.temporal_cloud_mode}

    import asyncio

    from demo_registry_operator.activities import temporal_cloud
    from demo_registry_operator.activities.temporal_cloud import SecretsManagerStore

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for cloud-api Temporal Cloud teardown: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    session = boto3.session.Session(region_name=config.aws_region)
    store = SecretsManagerStore(session.client("secretsmanager"))
    try:
        bootstrap = store.read_json(config.temporal_cloud_bootstrap_secret_path)
    except (BotoCoreError, ClientError) as error:
        raise aws_application_error(
            error,
            operation="Read Temporal Cloud bootstrap secret for teardown",
        ) from error
    if not isinstance(bootstrap, dict) or not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        raise non_retryable_error(
            (
                "Temporal Cloud cloud-api teardown requires bootstrap secret "
                f"{config.temporal_cloud_bootstrap_secret_path} with "
                "TEMPORAL_CLOUD_API_KEY"
            ),
            error_type=PROVIDER_CONFIGURATION_ERROR,
        )

    def heartbeat(detail: Any) -> None:
        try:
            activity.heartbeat(detail)
        except Exception:  # noqa: BLE001
            pass

    runner = getattr(temporal_cloud, runner_name)
    try:
        return asyncio.run(
            runner(
                demo_project,
                bootstrap=bootstrap,
                secret_store=store,
                checked_at=checked_at,
                target_host=config.temporal_cloud_ops_host,
                api_version=config.temporal_cloud_api_version,
                heartbeat=heartbeat,
            )
        )
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(error, operation=operation) from error


def delete_project_ecr_repositories(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    checked_at: str,
) -> dict[str, Any]:
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return {"phase": "Error", "message": f"boto3 is required: {error}"}

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    repositories: list[dict[str, Any]] = []
    for build_name in sorted(builds(demo_project)):
        name = image_repository_name(demo_project, build_name)
        try:
            ecr.delete_repository(repositoryName=name, force=True)
            repositories.append({"name": name, "action": "deleted"})
        except (BotoCoreError, ClientError) as error:
            if error_code(error) == "RepositoryNotFoundException":
                repositories.append({"name": name, "action": "not-found"})
            else:
                repositories.append({"name": name, "action": "error", "message": str(error)})
    return {"phase": "Deleted", "checkedAt": checked_at, "repositories": repositories}


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


def temporal_cloud_application_error(
    error: Exception,
    *,
    operation: str,
) -> Exception:
    message = f"{operation} failed: {error}"
    if authentication_error_message(str(error)):
        return non_retryable_error(
            message,
            error_type=PROVIDER_AUTHENTICATION_ERROR,
        )
    if temporal_cloud_transient_error_message(str(error)):
        return retryable_provider_error(message)
    return non_retryable_error(
        message,
        error_type=PROVIDER_CONFIGURATION_ERROR,
    )


def temporal_cloud_transient_error_message(message: str) -> bool:
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "connection reset",
            "deadline exceeded",
            "invalid resource version",
            "rate limit",
            "resource exhausted",
            "temporarily unavailable",
            "timeout",
            "timed out",
            "too many requests",
            "transport",
            "unavailable",
        )
    )


def repository_plan_entry(demo_project: dict[str, Any], build_name: str) -> dict[str, Any]:
    return {
        "name": image_repository_name(demo_project, build_name),
        "project": project_name(demo_project),
        "build": build_name,
        "components": components_for_build(demo_project, build_name),
    }


def image_repository_name(demo_project: dict[str, Any], build_name: str) -> str:
    return f"{derive.image_repository_prefix()}/{project_name(demo_project)}/{build_name}"


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
    return derive.temporal_namespace(project_name(demo_project))


def aggregate_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if "Error" in phases:
        return "Error"
    if "MissingValue" in phases:
        return "ValuesRequired"
    if phases and phases <= {"Ready", "NotRequired"}:
        return "Ready"
    return "Unknown"


def aggregate_secret_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if "Error" in phases:
        return "Error"
    if "MissingValue" in phases or "MissingProperties" in phases:
        return "ValuesRequired"
    if phases and phases <= {"Ready", "NotRequired"}:
        return "Ready"
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
    return "Unknown"


def demo_project_from_plan(plan: dict[str, Any]) -> dict[str, Any]:
    demo_project = plan.get("demoProject")
    if isinstance(demo_project, dict):
        return demo_project
    if isinstance(plan.get("metadata"), dict) and isinstance(plan.get("spec"), dict):
        return plan
    raise non_retryable_error(
        "External resource plan must include a DemoProject object",
        error_type=CONFIGURATION_ERROR,
    )


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field:
        return field
    raise non_retryable_error(
        f"External resource plan is missing {key}",
        error_type=CONFIGURATION_ERROR,
    )


def authentication_error_message(message: str) -> bool:
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "access denied",
            "auth",
            "forbidden",
            "permission denied",
            "unauthenticated",
            "unauthorized",
        )
    )


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
