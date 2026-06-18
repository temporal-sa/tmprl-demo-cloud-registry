import asyncio
from typing import Any

from temporalio import activity

from demo_registry_operator.activities import temporal_cloud
from demo_registry_operator.activities.external_resources.common import (
    authentication_error_message,
)
from demo_registry_operator.activities.external_resources.models import (
    CreateOrVerifyTemporalCloudNamespaceRequest,
    CreateOrVerifyTemporalCloudServiceAccountRequest,
    ConfigureTemporalCloudNamespaceMtlsRequest,
    DeleteTemporalCloudApiKeyRequest,
    DeleteTemporalCloudMtlsCaSecretRequest,
    DeleteTemporalCloudServiceAccountRequest,
    DeleteTemporalRuntimeCandidateSecretRequest,
    MirrorTemporalRuntimeSecretRequest,
    PromoteTemporalMtlsCaRequest,
    PromoteTemporalRuntimeCredentialRequest,
    RunTemporalRuntimeCandidateCanaryRequest,
    StageTemporalApiKeyCandidateRequest,
    StageTemporalMtlsCaRequest,
    StageTemporalMtlsClientCertificateRequest,
    TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
    TemporalCloudCredentialStageResult,
    TemporalCloudNamespaceRequest,
    TemporalCloudNamespaceResult,
    TemporalCloudProject,
    TemporalCloudPromotionResult,
    TemporalCloudServiceAccountResult,
    TemporalCloudStatusResult,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    CONFIGURATION_ERROR,
    PROVIDER_AUTHENTICATION_ERROR,
    PROVIDER_CONFIGURATION_ERROR,
    aws_application_error,
    non_retryable_error,
    retryable_provider_error,
)


@activity.defn
def stage_temporal_mtls_ca(
    request: StageTemporalMtlsCaRequest,
) -> TemporalCloudCredentialStageResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return stage_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return stage_result(manual_setup_required_status(request.project, request.checked_at))

    status = temporal_cloud.stage_temporal_mtls_ca(
        request.project.as_demo_project(),
        secret_store=store,
        checked_at=request.checked_at,
        candidate_secret_token=request.candidate_secret_token,
    )
    return stage_result(status)


@activity.defn
def create_or_verify_temporal_cloud_namespace(
    request: CreateOrVerifyTemporalCloudNamespaceRequest,
) -> TemporalCloudNamespaceResult:
    if not request.project.temporal_required:
        return namespace_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return namespace_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    store = temporal_secret_store()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return namespace_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.ensure_temporal_cloud_namespace(
            request.project.as_demo_project(),
            reconciler=reconciler,
            secret_store=store,
            account_id=request.account_id or bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID"),
            region=(
                request.region
                or bootstrap.get("TEMPORAL_CLOUD_REGION")
                or temporal_cloud.DEFAULT_TEMPORAL_CLOUD_REGION
            ),
            checked_at=request.checked_at,
            retention_days=request.retention_days,
            enable_delete_protection=request.enable_delete_protection,
            candidate_ca_secret_path=request.candidate_ca_secret_path,
            candidate_secret_token=request.candidate_secret_token,
        )

    try:
        return namespace_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Create or verify Temporal Cloud namespace",
        ) from error


@activity.defn
def configure_temporal_cloud_namespace_mtls(
    request: ConfigureTemporalCloudNamespaceMtlsRequest,
) -> TemporalCloudStatusResult:
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    config = WorkerConfig.from_env()
    store = temporal_secret_store()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return status_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.configure_temporal_cloud_namespace_mtls(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            reconciler=reconciler,
            secret_store=store,
            checked_at=request.checked_at,
            candidate_ca_secret_path=request.candidate_ca_secret_path,
            candidate_secret_token=request.candidate_secret_token,
        )

    try:
        return status_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Configure Temporal Cloud namespace mTLS",
        ) from error


@activity.defn
def create_or_verify_temporal_cloud_service_account(
    request: CreateOrVerifyTemporalCloudServiceAccountRequest,
) -> TemporalCloudServiceAccountResult:
    if not request.project.temporal_required:
        return service_account_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return service_account_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return service_account_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.ensure_temporal_cloud_service_account(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return service_account_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Create or verify Temporal Cloud service account",
        ) from error


@activity.defn
def stage_temporal_api_key_candidate(
    request: StageTemporalApiKeyCandidateRequest,
) -> TemporalCloudCredentialStageResult:
    if not request.project.temporal_required:
        return stage_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return stage_result(manual_setup_required_status(request.project, request.checked_at))

    config = WorkerConfig.from_env()
    store = temporal_secret_store()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return stage_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.stage_temporal_api_key_candidate(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            service_account_status=request.service_account.status,
            reconciler=reconciler,
            secret_store=store,
            checked_at=request.checked_at,
            force_rotation=request.force_rotation,
            candidate_secret_token=request.candidate_secret_token,
        )

    try:
        return stage_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Stage Temporal API key candidate",
        ) from error


@activity.defn
def stage_temporal_mtls_client_certificate(
    request: StageTemporalMtlsClientCertificateRequest,
) -> TemporalCloudCredentialStageResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return stage_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return stage_result(manual_setup_required_status(request.project, request.checked_at))

    status = temporal_cloud.stage_temporal_mtls_client_certificate(
        request.project.as_demo_project(),
        namespace_status_value=request.namespace.status,
        ca_status=request.ca.status,
        secret_store=store,
        checked_at=request.checked_at,
        force_client_rotation=request.force_rotation,
        candidate_secret_token=request.candidate_secret_token,
    )
    return stage_result(status)


@activity.defn
def run_temporal_runtime_candidate_canary(
    request: RunTemporalRuntimeCandidateCanaryRequest,
) -> TemporalCloudStatusResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    try:
        return status_result(
            asyncio.run(
                temporal_cloud.run_temporal_runtime_candidate_canary(
                    request.project.as_demo_project(),
                    stage_status=request.stage.status,
                    secret_store=store,
                    checked_at=request.checked_at,
                )
            )
        )
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Temporal runtime candidate canary",
        ) from error


@activity.defn
def promote_temporal_mtls_ca(
    request: PromoteTemporalMtlsCaRequest,
) -> TemporalCloudPromotionResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return promotion_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return promotion_result(manual_setup_required_status(request.project, request.checked_at))

    status = temporal_cloud.promote_temporal_mtls_ca_secret(
        request.project.as_demo_project(),
        stage_status=request.stage.status,
        secret_store=store,
        checked_at=request.checked_at,
    )
    return promotion_result(status)


@activity.defn
def promote_temporal_runtime_credential(
    request: PromoteTemporalRuntimeCredentialRequest,
) -> TemporalCloudPromotionResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return promotion_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return promotion_result(manual_setup_required_status(request.project, request.checked_at))

    status = temporal_cloud.promote_temporal_runtime_secret(
        request.project.as_demo_project(),
        stage_status=request.stage.status,
        canary_status=request.canary.status if request.canary else None,
        secret_store=store,
        checked_at=request.checked_at,
    )
    return promotion_result(status)


@activity.defn
def mirror_temporal_runtime_secret(
    request: MirrorTemporalRuntimeSecretRequest,
) -> TemporalCloudStatusResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    status = temporal_cloud.mirror_temporal_runtime_secret(
        request.project.as_demo_project(),
        secret_store=store,
        checked_at=request.checked_at,
    )
    return status_result(status)


@activity.defn
def delete_temporal_runtime_candidate_secret(
    request: DeleteTemporalRuntimeCandidateSecretRequest,
) -> TemporalCloudStatusResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    status = temporal_cloud.delete_temporal_runtime_candidate_secret(
        request.project.as_demo_project(),
        secret_store=store,
        checked_at=request.checked_at,
        candidate_secret_path=request.candidate_secret_path,
        candidate_ca_secret_path=request.candidate_ca_secret_path,
        candidate_secret_token=request.candidate_secret_token,
    )
    return status_result(status)


@activity.defn
def delete_temporal_cloud_mtls_ca_secret(
    request: DeleteTemporalCloudMtlsCaSecretRequest,
) -> TemporalCloudStatusResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    status = temporal_cloud.delete_temporal_cloud_mtls_ca_secret(
        request.project.as_demo_project(),
        secret_store=store,
        checked_at=request.checked_at,
    )
    return status_result(status)


@activity.defn
def delete_temporal_cloud_api_key_by_id(
    request: DeleteTemporalCloudApiKeyRequest,
) -> TemporalCloudStatusResult:
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return status_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.delete_temporal_cloud_api_key_id(
            request.key_id,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return status_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Delete Temporal Cloud API key",
        ) from error


@activity.defn
def delete_temporal_cloud_service_account_by_id(
    request: DeleteTemporalCloudServiceAccountRequest,
) -> TemporalCloudStatusResult:
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return status_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.delete_temporal_cloud_service_account_id(
            request.service_account_id,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return status_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Delete Temporal Cloud service account",
        ) from error


@activity.defn
def disable_temporal_cloud_namespace_delete_protection_by_name(
    request: TemporalCloudNamespaceRequest,
) -> TemporalCloudStatusResult:
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return status_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.disable_temporal_cloud_namespace_delete_protection_name(
            request.namespace,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return status_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Disable Temporal Cloud namespace delete protection",
        ) from error


@activity.defn
def delete_temporal_cloud_namespace_by_name(
    request: TemporalCloudNamespaceRequest,
) -> TemporalCloudStatusResult:
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return status_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.delete_temporal_cloud_namespace_name(
            request.namespace,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return status_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Delete Temporal Cloud namespace",
        ) from error


@activity.defn
def delete_temporal_cloud_api_key(
    request: DeleteTemporalRuntimeCandidateSecretRequest,
) -> TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.delete_temporal_cloud_api_key,
        operation="Temporal Cloud API key teardown",
    )


@activity.defn
def delete_temporal_cloud_service_account(
    request: DeleteTemporalRuntimeCandidateSecretRequest,
) -> TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.delete_temporal_cloud_service_account,
        operation="Temporal Cloud service account teardown",
    )


@activity.defn
def disable_temporal_cloud_namespace_delete_protection(
    request: DeleteTemporalRuntimeCandidateSecretRequest,
) -> TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.disable_temporal_cloud_namespace_delete_protection,
        operation="Temporal Cloud namespace delete-protection teardown",
    )


@activity.defn
def delete_temporal_cloud_namespace(
    request: DeleteTemporalRuntimeCandidateSecretRequest,
) -> TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.delete_temporal_cloud_namespace,
        operation="Temporal Cloud namespace teardown",
    )


@activity.defn
def delete_temporal_cloud_runtime_secret(
    request: DeleteTemporalRuntimeCandidateSecretRequest,
) -> TemporalCloudStatusResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    try:
        return status_result(
            temporal_cloud.delete_temporal_cloud_runtime_secret(
                request.project.as_demo_project(),
                secret_store=store,
                checked_at=request.checked_at,
            )
        )
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Delete Temporal Cloud runtime secret",
        ) from error


def run_project_temporal_delete(
    request: DeleteTemporalRuntimeCandidateSecretRequest,
    delete_fn: Any,
    *,
    operation: str,
) -> TemporalCloudStatusResult:
    if not request.project.temporal_required:
        return status_result(temporal_cloud.temporal_cloud_not_required_status(request.checked_at))
    if platform_manual_setup_required(request.project):
        return status_result(manual_setup_required_status(request.project, request.checked_at))

    config = WorkerConfig.from_env()
    store = temporal_secret_store()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return status_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await delete_fn(
            request.project.as_demo_project(),
            reconciler=reconciler,
            secret_store=store,
            checked_at=request.checked_at,
        )

    try:
        return status_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(error, operation=operation) from error


def temporal_secret_store() -> Any:
    config = WorkerConfig.from_env()
    try:
        import boto3
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for cloud-api Temporal Cloud mode: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    return temporal_cloud.SecretsManagerStore(
        boto3.session.Session(region_name=config.aws_region).client("secretsmanager")
    )


def temporal_bootstrap_secret(
    config: WorkerConfig,
    checked_at: str,
) -> dict[str, Any]:
    store = temporal_secret_store()
    try:
        bootstrap = store.read_json(config.temporal_cloud_bootstrap_secret_path)
    except Exception as error:
        raise aws_application_error(
            error,
            operation="Read Temporal Cloud bootstrap secret",
        ) from error
    if isinstance(bootstrap, dict) and bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return bootstrap
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
            "requiredProperties": list(TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES),
        },
    }


async def cloud_reconciler(
    config: WorkerConfig,
    bootstrap: dict[str, Any],
) -> Any:
    from temporalio.client import CloudOperationsClient

    connect_kwargs: dict[str, Any] = {
        "api_key": bootstrap.get("TEMPORAL_CLOUD_API_KEY"),
        "target_host": config.temporal_cloud_ops_host,
    }
    if config.temporal_cloud_api_version:
        connect_kwargs["version"] = config.temporal_cloud_api_version
    client = await CloudOperationsClient.connect(**connect_kwargs)
    return temporal_cloud.CloudApiReconciler(
        client.cloud_service,
        account_id=bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID"),
        heartbeat=activity_heartbeat,
    )


def activity_heartbeat(detail: Any) -> None:
    try:
        activity.heartbeat(detail)
    except Exception:  # noqa: BLE001
        pass


def platform_manual_setup_required(project: TemporalCloudProject) -> bool:
    config = WorkerConfig.from_env()
    return config.temporal_cloud_mode != "cloud-api" or config.external_resource_mode != "aws"


def manual_setup_required_status(
    project: TemporalCloudProject,
    checked_at: str,
) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    return {
        "phase": "ManualSetupRequired",
        "provider": "temporal-cloud",
        "mode": config.temporal_cloud_mode,
        "checkedAt": checked_at,
        "accountId": config.temporal_cloud_account_id,
        "region": config.temporal_cloud_region,
        "auth": {
            "mode": project.auth_mode,
            "defaulted": False,
            "modeSwitchPolicy": "destructive-recreate",
        },
        "forceTemporalAuthRotation": False,
        "message": (
            "Temporal Cloud reconciliation requires cloud-api mode with aws "
            "external resource reconciliation."
        ),
        "namespace": {
            "name": project.namespace,
            "action": "create-or-verify",
            "deletionPolicy": "retain-until-explicitly-approved",
        },
        "bootstrapCredential": {
            "secretRemotePath": config.temporal_cloud_bootstrap_secret_path,
            "kubernetesSecret": "temporal-cloud-bootstrap",
            "requiredProperties": list(TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES),
        },
    }


def namespace_result(status: dict[str, Any]) -> TemporalCloudNamespaceResult:
    namespace = status.get("namespace")
    namespace = namespace if isinstance(namespace, dict) else {}
    return TemporalCloudNamespaceResult(
        status=status,
        namespace=string_value(namespace.get("name")),
        grpc_address=string_value(namespace.get("grpcAddress")),
        auth_mode=string_value(namespace.get("authMode")),
        created=namespace.get("action") == "created",
    )


def service_account_result(status: dict[str, Any]) -> TemporalCloudServiceAccountResult:
    service_account = status.get("serviceAccount")
    service_account = service_account if isinstance(service_account, dict) else {}
    return TemporalCloudServiceAccountResult(
        status=status,
        service_account_id=string_value(service_account.get("id")),
        service_account_name=string_value(service_account.get("name")),
        created=service_account.get("action") == "created",
    )


def stage_result(status: dict[str, Any]) -> TemporalCloudCredentialStageResult:
    return TemporalCloudCredentialStageResult(
        status=status,
        promote_required=bool(status.get("promoteRequired")),
        candidate_api_key_id=string_value(status.get("candidateApiKeyId")),
        candidate_secret_path=string_value(status.get("candidateSecretRemotePath")),
        candidate_ca_secret_path=string_value(
            status.get("candidateCaSecretRemotePath")
        ),
        candidate_secret_token=string_value(status.get("candidateSecretToken")),
    )


def promotion_result(status: dict[str, Any]) -> TemporalCloudPromotionResult:
    promoted = status.get("promoted")
    promoted = promoted if isinstance(promoted, dict) else {}
    return TemporalCloudPromotionResult(
        status=status,
        promoted=bool(promoted.get("runtimeSecret")),
        ca_promoted=bool(promoted.get("caSecret")),
    )


def status_result(status: dict[str, Any]) -> TemporalCloudStatusResult:
    return TemporalCloudStatusResult(status=status)


def string_value(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


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
