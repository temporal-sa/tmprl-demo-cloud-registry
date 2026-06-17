from __future__ import annotations

import asyncio
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
    TEMPORAL_AUTH_MODE_API_KEY,
    TEMPORAL_AUTH_MODE_MTLS,
    TEMPORAL_MTLS_CA_SECRET_PROPERTIES,
    temporal_mtls_ca_secret_remote_path,
    temporal_components,
    temporal_auth_runtime_status,
    temporal_auth_mode,
    temporal_auth_status,
    temporal_runtime_secret_properties,
    temporal_secret_remote_path,
    user_secret_names,
    user_secret_required_properties,
    user_secret_remote_path,
)


TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES = [
    "TEMPORAL_CLOUD_API_KEY",
    "TEMPORAL_CLOUD_ACCOUNT_ID",
    "TEMPORAL_CLOUD_REGION",
]


@activity.defn
def plan_external_resources(request: dict[str, Any]) -> dict[str, Any]:
    """Return a side-effect-free external-resource reconcile plan."""

    demo_project = request.get("demoProject") if isinstance(request, dict) else None
    if not isinstance(demo_project, dict):
        demo_project = request
    force_temporal_auth_rotation = (
        bool(request.get("forceTemporalAuthRotation"))
        if isinstance(request, dict)
        else False
    )
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
        "forceTemporalAuthRotation": force_temporal_auth_rotation,
        "ecrRepositories": [
            repository_plan_entry(demo_project, build_name)
            for build_name in sorted(builds(demo_project))
        ],
    }


@activity.defn
def reconcile_ecr_repository(repository: dict[str, Any]) -> dict[str, Any]:
    config = WorkerConfig.from_env()

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    try:
        status = ensure_ecr_repository(ecr, repository)
    except (BotoCoreError, ClientError) as error:
        raise aws_application_error(
            error,
            operation=f"Reconcile ECR repository {repository['name']}",
        ) from error

    return {
        **status,
        "region": config.aws_region,
        "accountId": config.aws_account_id,
    }


@activity.defn
def ensure_temporal_mtls_ca_resource(plan: dict[str, Any]) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(plan, operation="Stage Temporal mTLS CA")
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    return temporal_cloud.stage_temporal_mtls_ca(
        demo_project,
        secret_store=store,
        checked_at=checked_at,
    )


@activity.defn
def ensure_temporal_cloud_namespace_resource(plan: dict[str, Any]) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    return run_temporal_cloud_control_plane_activity(
        plan,
        operation="Ensure Temporal Cloud namespace",
        step=lambda demo_project, reconciler, secret_store, bootstrap, checked_at: (
            temporal_cloud.ensure_temporal_cloud_namespace(
                demo_project,
                reconciler=reconciler,
                secret_store=secret_store,
                account_id=bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID"),
                region=bootstrap.get("TEMPORAL_CLOUD_REGION")
                or temporal_cloud.DEFAULT_TEMPORAL_CLOUD_REGION,
                checked_at=checked_at,
            )
        ),
    )


@activity.defn
def configure_temporal_cloud_namespace_mtls_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    return run_temporal_cloud_control_plane_activity(
        request,
        operation="Configure Temporal Cloud namespace mTLS",
        step=lambda demo_project, reconciler, secret_store, _bootstrap, checked_at: (
            temporal_cloud.configure_temporal_cloud_namespace_mtls(
                demo_project,
                namespace_status_value=dict_field(request, "namespaceStatus"),
                reconciler=reconciler,
                secret_store=secret_store,
                checked_at=checked_at,
            )
        ),
    )


@activity.defn
def ensure_temporal_cloud_service_account_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    return run_temporal_cloud_control_plane_activity(
        request,
        operation="Ensure Temporal Cloud service account",
        step=lambda demo_project, reconciler, _secret_store, _bootstrap, checked_at: (
            temporal_cloud.ensure_temporal_cloud_service_account(
                demo_project,
                namespace_status_value=dict_field(request, "namespaceStatus"),
                reconciler=reconciler,
                checked_at=checked_at,
            )
        ),
    )


@activity.defn
def stage_temporal_api_key_candidate_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    return run_temporal_cloud_control_plane_activity(
        request,
        operation="Stage Temporal API key candidate",
        step=lambda demo_project, reconciler, secret_store, _bootstrap, checked_at: (
            temporal_cloud.stage_temporal_api_key_candidate(
                demo_project,
                namespace_status_value=dict_field(request, "namespaceStatus"),
                service_account_status=dict_field(request, "serviceAccountStatus"),
                reconciler=reconciler,
                secret_store=secret_store,
                checked_at=checked_at,
                force_rotation=bool(plan_from_request(request).get("forceTemporalAuthRotation")),
            )
        ),
    )


@activity.defn
def stage_temporal_mtls_client_certificate_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(
        request,
        operation="Stage Temporal mTLS client certificate",
    )
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    return temporal_cloud.stage_temporal_mtls_client_certificate(
        demo_project,
        namespace_status_value=dict_field(request, "namespaceStatus"),
        ca_status=dict_field(request, "caStatus"),
        secret_store=store,
        checked_at=checked_at,
        force_client_rotation=bool(
            plan_from_request(request).get("forceTemporalAuthRotation")
        ),
    )


@activity.defn
def run_temporal_runtime_candidate_canary_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(
        request,
        operation="Run Temporal runtime candidate canary",
    )
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    try:
        return asyncio.run(
            temporal_cloud.run_temporal_runtime_candidate_canary(
                demo_project,
                stage_status=dict_field(request, "stageStatus"),
                secret_store=store,
                checked_at=checked_at,
            )
        )
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Temporal runtime candidate canary",
        ) from error


@activity.defn
def promote_temporal_runtime_secret_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(
        request,
        operation="Promote Temporal runtime credential",
    )
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    return temporal_cloud.promote_temporal_runtime_secret(
        demo_project,
        stage_status=dict_field(request, "stageStatus"),
        canary_status=optional_dict_field(request, "canaryStatus"),
        secret_store=store,
        checked_at=checked_at,
    )


@activity.defn
def promote_temporal_mtls_ca_secret_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(
        request,
        operation="Promote Temporal mTLS CA",
    )
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    return temporal_cloud.promote_temporal_mtls_ca_secret(
        demo_project,
        stage_status=dict_field(request, "stageStatus"),
        secret_store=store,
        checked_at=checked_at,
    )


@activity.defn
def mirror_temporal_runtime_secret_resource(plan: dict[str, Any]) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(
        plan,
        operation="Mirror Temporal runtime credential",
        require_bootstrap=False,
    )
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    return temporal_cloud.mirror_temporal_runtime_secret(
        demo_project,
        secret_store=store,
        checked_at=checked_at,
    )


@activity.defn
def delete_temporal_runtime_candidate_secret_resource(
    plan: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(
        plan,
        operation="Delete Temporal runtime candidate secret",
        require_bootstrap=False,
    )
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    return temporal_cloud.delete_temporal_runtime_candidate_secret(
        demo_project,
        secret_store=store,
        checked_at=checked_at,
    )


@activity.defn
def delete_temporal_cloud_mtls_ca_secret_resource(
    plan: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    context = temporal_cloud_secret_context(
        plan,
        operation="Delete Temporal mTLS CA secret",
        require_bootstrap=False,
    )
    if isinstance(context, dict):
        return context
    demo_project, _config, checked_at, store = context
    return temporal_cloud.delete_temporal_cloud_mtls_ca_secret(
        demo_project,
        secret_store=store,
        checked_at=checked_at,
    )


@activity.defn
def delete_temporal_cloud_candidate_api_key_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    key_id = string_field(request, "keyId")
    return run_temporal_cloud_control_plane_activity(
        request,
        operation="Delete Temporal Cloud candidate API key",
        step=lambda _demo_project, reconciler, _secret_store, _bootstrap, checked_at: (
            temporal_cloud.delete_temporal_cloud_api_key_id(
                key_id,
                reconciler=reconciler,
                checked_at=checked_at,
            )
        ),
    )


@activity.defn
def delete_temporal_cloud_service_account_id_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    service_account_id = string_field(request, "serviceAccountId")
    return run_temporal_cloud_control_plane_activity(
        request,
        operation="Delete Temporal Cloud service account",
        step=lambda _demo_project, reconciler, _secret_store, _bootstrap, checked_at: (
            temporal_cloud.delete_temporal_cloud_service_account_id(
                service_account_id,
                reconciler=reconciler,
                checked_at=checked_at,
            )
        ),
    )


@activity.defn
def disable_temporal_cloud_namespace_delete_protection_name_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    namespace = string_field(request, "namespace")
    return run_temporal_cloud_control_plane_activity(
        request,
        operation="Disable Temporal Cloud namespace delete protection",
        step=lambda _demo_project, reconciler, _secret_store, _bootstrap, checked_at: (
            temporal_cloud.disable_temporal_cloud_namespace_delete_protection_name(
                namespace,
                reconciler=reconciler,
                checked_at=checked_at,
            )
        ),
    )


@activity.defn
def delete_temporal_cloud_namespace_name_resource(
    request: dict[str, Any],
) -> dict[str, Any]:
    from demo_registry_operator.activities import temporal_cloud

    namespace = string_field(request, "namespace")
    return run_temporal_cloud_control_plane_activity(
        request,
        operation="Delete Temporal Cloud namespace",
        step=lambda _demo_project, reconciler, _secret_store, _bootstrap, checked_at: (
            temporal_cloud.delete_temporal_cloud_namespace_name(
                namespace,
                reconciler=reconciler,
                checked_at=checked_at,
            )
        ),
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


def ecr_repositories_status(
    plan: dict[str, Any],
    repositories: list[dict[str, Any]],
) -> dict[str, Any]:
    first = repositories[0] if repositories else {}
    return {
        "phase": aggregate_phase(repositories) if repositories else "NotRequired",
        "region": first.get("region"),
        "accountId": first.get("accountId"),
        "checkedAt": string_field(plan, "checkedAt"),
        "repositories": repositories,
    }


def run_temporal_cloud_control_plane_activity(
    request: dict[str, Any],
    *,
    operation: str,
    step: Any,
) -> dict[str, Any]:
    from demo_registry_operator.activities.temporal_cloud import (
        CloudApiReconciler,
    )
    from temporalio.client import CloudOperationsClient

    context = temporal_cloud_secret_context(request, operation=operation)
    if isinstance(context, dict):
        return context
    demo_project, config, checked_at, store, bootstrap = context

    def heartbeat(detail: Any) -> None:
        try:
            activity.heartbeat(detail)
        except Exception:  # noqa: BLE001
            pass

    async def run() -> dict[str, Any]:
        api_key = bootstrap.get("TEMPORAL_CLOUD_API_KEY")
        connect_kwargs: dict[str, Any] = {
            "api_key": api_key,
            "target_host": config.temporal_cloud_ops_host,
        }
        if config.temporal_cloud_api_version:
            connect_kwargs["version"] = config.temporal_cloud_api_version
        client = await CloudOperationsClient.connect(**connect_kwargs)
        reconciler = CloudApiReconciler(
            client.cloud_service,
            account_id=bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID"),
            heartbeat=heartbeat,
        )
        return await step(demo_project, reconciler, store, bootstrap, checked_at)

    try:
        return asyncio.run(run())
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(error, operation=operation) from error


def temporal_cloud_secret_context(
    request: dict[str, Any],
    *,
    operation: str,
    require_bootstrap: bool = True,
) -> tuple[dict[str, Any], WorkerConfig, str, Any, dict[str, Any]] | tuple[
    dict[str, Any], WorkerConfig, str, Any
] | dict[str, Any]:
    from demo_registry_operator.activities.temporal_cloud import (
        SecretsManagerStore,
    )

    plan = plan_from_request(request)
    demo_project = demo_project_from_plan(plan)
    config = WorkerConfig.from_env()
    checked_at = string_field(plan, "checkedAt")
    if not temporal_components(demo_project):
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "message": "No component currently requires Temporal credentials.",
        }
    if config.temporal_cloud_mode != "cloud-api" or config.external_resource_mode != "aws":
        return temporal_cloud_status(
            demo_project,
            config,
            phase="ManualSetupRequired",
            checked_at=checked_at,
            force_auth_rotation=bool(plan.get("forceTemporalAuthRotation")),
        )

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for cloud-api Temporal Cloud mode: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    store = SecretsManagerStore(
        boto3.session.Session(region_name=config.aws_region).client("secretsmanager")
    )
    if not require_bootstrap:
        return demo_project, config, checked_at, store

    try:
        bootstrap = store.read_json(config.temporal_cloud_bootstrap_secret_path)
    except (BotoCoreError, ClientError) as error:
        raise aws_application_error(
            error,
            operation=f"{operation}: read Temporal Cloud bootstrap secret",
        ) from error
    if not isinstance(bootstrap, dict) or not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return temporal_cloud_bootstrap_required_status(config, checked_at)
    return demo_project, config, checked_at, store, bootstrap


def temporal_cloud_bootstrap_required_status(
    config: WorkerConfig,
    checked_at: str,
) -> dict[str, Any]:
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


def plan_from_request(request: dict[str, Any]) -> dict[str, Any]:
    plan = request.get("plan")
    return plan if isinstance(plan, dict) else request


def dict_field(value: dict[str, Any], key: str) -> dict[str, Any]:
    field = value.get(key)
    if isinstance(field, dict):
        return field
    raise non_retryable_error(
        f"Temporal Cloud activity request is missing {key}",
        error_type=CONFIGURATION_ERROR,
    )


def optional_dict_field(value: dict[str, Any], key: str) -> dict[str, Any] | None:
    field = value.get(key)
    return field if isinstance(field, dict) else None


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
    mtls_ca_secret_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    statuses = [
        api_key_status,
        service_account_status,
        namespace_protection_status,
        namespace_status,
        runtime_secret_status,
    ]
    if mtls_ca_secret_status is not None:
        statuses.append(mtls_ca_secret_status)
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

    rendered = {
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
    if mtls_ca_secret_status is not None:
        rendered["steps"]["mtlsCaSecret"] = mtls_ca_secret_status
    return rendered


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
        required_properties=temporal_runtime_secret_properties(demo_project),
    )
    temporal_mtls_ca = secret_status(
        secretsmanager,
        temporal_mtls_ca_secret_remote_path(demo_project),
        required=(
            temporal_required
            and temporal_auth_mode(demo_project) == TEMPORAL_AUTH_MODE_MTLS
        ),
        managed_by="platform",
        required_properties=TEMPORAL_MTLS_CA_SECRET_PROPERTIES,
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
    all_secret_statuses = [
        temporal_bootstrap,
        temporal_secret,
        temporal_mtls_ca,
        *user_managed,
    ]
    phase = aggregate_secret_phase(all_secret_statuses)
    return {
        "phase": phase,
        "checkedAt": checked_at,
        "secretPrefix": project_secret_prefix(demo_project),
        "temporalBootstrap": temporal_bootstrap,
        "temporalCredentials": temporal_secret,
        "temporalMtlsCa": temporal_mtls_ca,
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
    force_auth_rotation: bool = False,
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
            force_auth_rotation=force_auth_rotation,
        )

    return {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": config.temporal_cloud_mode,
        "accountId": config.temporal_cloud_account_id,
        "region": config.temporal_cloud_region,
        "auth": temporal_auth_status(demo_project),
        "forceTemporalAuthRotation": force_auth_rotation,
        "message": (
            "Temporal Cloud reconciliation requires cloud-api mode with aws "
            "external resource reconciliation."
        ),
        "namespace": {
            "name": temporal_namespace(demo_project),
            "action": "create-or-verify",
            "deletionPolicy": "retain-until-explicitly-approved",
        },
        **temporal_auth_runtime_status(demo_project),
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
    force_auth_rotation: bool = False,
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
                force_auth_rotation=force_auth_rotation,
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
