import asyncio
import socket
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError

from demo_registry_operator import errors, kube, resources
from demo_registry_operator.activities import temporal_cloud
from demo_registry_operator.activities.external_resources import common, models
from demo_registry_operator.config import WorkerConfig

TEMPORAL_NAMESPACE_DNS_PENDING = "TemporalNamespaceDnsPending"


@activity.defn
def stage_temporal_mtls_ca(
    request: models.StageTemporalMtlsCaRequest,
) -> models.TemporalCloudCredentialStageResult:
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
    request: models.CreateOrVerifyTemporalCloudNamespaceRequest,
) -> models.TemporalCloudNamespaceResult:
    if not request.project.namespace_required:
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
                or resources.resolve_temporal_cloud_region(
                    explicit_region=request.project.region,
                    serverless=request.project.serverless,
                    operator_region=(
                        bootstrap.get("TEMPORAL_CLOUD_REGION")
                        or config.temporal_cloud_region
                        or temporal_cloud.DEFAULT_TEMPORAL_CLOUD_REGION
                    ),
                    serverless_region=config.serverless_temporal_cloud_region,
                )
            ),
            checked_at=request.checked_at,
            retention_days=request.retention_days,
            task_queue_fairness=request.project.task_queue_fairness,
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
def reconcile_nexus_endpoints(
    request: models.ReconcileNexusEndpointsRequest,
) -> models.NexusEndpointResult:
    if not request.project.nexus_endpoints_required:
        return nexus_endpoint_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return nexus_endpoint_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return nexus_endpoint_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.reconcile_nexus_endpoints(
            request.project.as_demo_project(),
            endpoints=request.project.nexus_endpoints,
            reconciler=reconciler,
            checked_at=request.checked_at,
            account_id=(
                request.account_id or bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID")
            ),
            live_project_names=_live_demo_project_names(),
        )

    try:
        return nexus_endpoint_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Reconcile Nexus Endpoints",
        ) from error


@activity.defn
def delete_nexus_endpoints(
    request: models.DeleteNexusEndpointsRequest,
) -> models.NexusEndpointResult:
    if not request.project.nexus_endpoints_required:
        return nexus_endpoint_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return nexus_endpoint_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return nexus_endpoint_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.delete_nexus_endpoints(
            request.project.as_demo_project(),
            endpoints=request.project.nexus_endpoints,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return nexus_endpoint_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Delete Nexus Endpoints",
        ) from error


@activity.defn
def wait_for_temporal_cloud_namespace_dns(
    request: models.WaitForTemporalCloudNamespaceDnsRequest,
) -> models.TemporalCloudNamespaceDnsResult:
    """Resolve the namespace gRPC host once; raise a retryable error if it fails.

    Temporal Cloud reports a new namespace ACTIVE before its hostname is
    resolvable everywhere. The Worker Controller dials that hostname as soon as
    the WorkerDeployment lands and fails with "no children to pick from" until
    it resolves. The workflow retries this activity on a bounded schedule so
    the rollout starts only after the operator itself can resolve the host.
    """
    grpc_address = request.namespace.grpc_address
    if not request.project.namespace_required or not grpc_address:
        return models.TemporalCloudNamespaceDnsResult(host=None)

    host, _, port = grpc_address.rpartition(":")
    if not host:
        host, port = grpc_address, "7233"
    try:
        infos = socket.getaddrinfo(host, int(port), proto=socket.IPPROTO_TCP)
    except (socket.gaierror, UnicodeError) as error:
        raise ApplicationError(
            f"Temporal Cloud namespace host {host} does not resolve yet: {error}",
            type=TEMPORAL_NAMESPACE_DNS_PENDING,
        ) from error

    addresses = tuple(sorted({str(info[4][0]) for info in infos}))
    if not addresses:
        raise ApplicationError(
            f"Temporal Cloud namespace host {host} resolved to no addresses.",
            type=TEMPORAL_NAMESPACE_DNS_PENDING,
        )
    return models.TemporalCloudNamespaceDnsResult(host=host, addresses=addresses)


@activity.defn
def ensure_temporal_cloud_namespace_tags(
    request: models.EnsureTemporalCloudNamespaceTagsRequest,
) -> models.TemporalCloudNamespaceTagsResult:
    if not request.project.namespace_required:
        return namespace_tags_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return namespace_tags_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return namespace_tags_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.ensure_temporal_cloud_namespace_tags(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            tags=request.tags,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return namespace_tags_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Ensure Temporal Cloud namespace tags",
        ) from error


@activity.defn
def ensure_temporal_cloud_namespace_readers_access(
    request: models.EnsureTemporalCloudNamespaceReadersAccessRequest,
) -> models.TemporalCloudStatusResult:
    if not request.project.namespace_required:
        return status_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return status_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    if not config.namespace_readers_group:
        return status_result(
            {
                "phase": "NotRequired",
                "provider": "temporal-cloud",
                "mode": "cloud-api",
                "checkedAt": request.checked_at,
                "message": "No namespace readers group is configured.",
            }
        )
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return status_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.ensure_temporal_cloud_namespace_readers_access(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            group_name=config.namespace_readers_group,
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return status_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Ensure Temporal Cloud namespace readers access",
        ) from error


@activity.defn
def create_or_verify_temporal_cloud_search_attribute(
    request: models.CreateOrVerifyTemporalCloudSearchAttributeRequest,
) -> models.TemporalCloudSearchAttributeResult:
    if not request.project.namespace_required:
        return search_attribute_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return search_attribute_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return search_attribute_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.ensure_temporal_cloud_search_attribute(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            search_attribute={
                "name": request.search_attribute.name,
                "type": request.search_attribute.type,
            },
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return search_attribute_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Create or verify Temporal Cloud search attribute",
        ) from error


@activity.defn
def reconcile_temporal_cloud_search_attributes(
    request: models.ReconcileTemporalCloudSearchAttributesRequest,
) -> models.TemporalCloudSearchAttributesResult:
    if not request.project.namespace_required:
        return search_attributes_result(
            [temporal_cloud.temporal_cloud_not_required_status(request.checked_at)]
        )
    if platform_manual_setup_required(request.project):
        return search_attributes_result(
            [manual_setup_required_status(request.project, request.checked_at)]
        )

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return search_attributes_result([bootstrap])

    async def run() -> list[dict[str, Any]]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.ensure_temporal_cloud_search_attributes(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            desired=[
                {"name": attribute.name, "type": attribute.type}
                for attribute in request.desired
            ],
            delete_names=list(request.delete_names),
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return search_attributes_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Reconcile Temporal Cloud search attributes",
        ) from error


@activity.defn
def delete_temporal_cloud_search_attribute(
    request: models.DeleteTemporalCloudSearchAttributeRequest,
) -> models.TemporalCloudSearchAttributeResult:
    if not request.project.namespace_required:
        return search_attribute_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return search_attribute_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    config = WorkerConfig.from_env()
    bootstrap = temporal_bootstrap_secret(config, request.checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return search_attribute_result(bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_reconciler(config, bootstrap)
        return await temporal_cloud.delete_temporal_cloud_search_attribute_value(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            search_attribute={
                "name": request.search_attribute.name,
                "type": request.search_attribute.type,
            },
            reconciler=reconciler,
            checked_at=request.checked_at,
        )

    try:
        return search_attribute_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Delete Temporal Cloud search attribute",
        ) from error


@activity.defn
def configure_temporal_cloud_namespace_mtls(
    request: models.ConfigureTemporalCloudNamespaceMtlsRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.CreateOrVerifyTemporalCloudServiceAccountRequest,
) -> models.TemporalCloudServiceAccountResult:
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
def check_temporal_runtime_secret(
    request: models.CheckTemporalRuntimeSecretRequest,
) -> models.TemporalRuntimeSecretCheckResult:
    store = temporal_secret_store()
    if not request.project.temporal_required:
        return runtime_secret_check_result(
            temporal_cloud.temporal_cloud_not_required_status(request.checked_at)
        )
    if platform_manual_setup_required(request.project):
        return runtime_secret_check_result(
            manual_setup_required_status(request.project, request.checked_at)
        )

    status = temporal_cloud.check_temporal_api_key_runtime_secret(
        request.project.as_demo_project(),
        namespace_status_value=request.namespace.status,
        secret_store=store,
        checked_at=request.checked_at,
        force_rotation=request.force_rotation,
    )
    return runtime_secret_check_result(status)


@activity.defn
def create_temporal_api_key_candidate_secret(
    request: models.CreateTemporalApiKeyCandidateSecretRequest,
) -> models.TemporalCloudCredentialStageResult:
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
        return await temporal_cloud.create_temporal_api_key_candidate_secret(
            request.project.as_demo_project(),
            namespace_status_value=request.namespace.status,
            service_account_status=request.service_account.status,
            runtime_secret_status=request.runtime_secret.status,
            reconciler=reconciler,
            secret_store=store,
            checked_at=request.checked_at,
            candidate_secret_token=request.candidate_secret_token,
        )

    try:
        return stage_result(asyncio.run(run()))
    except Exception as error:  # noqa: BLE001
        raise temporal_cloud_application_error(
            error,
            operation="Create Temporal API key candidate secret",
        ) from error


@activity.defn
def stage_temporal_api_key_candidate(
    request: models.StageTemporalApiKeyCandidateRequest,
) -> models.TemporalCloudCredentialStageResult:
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
    request: models.StageTemporalMtlsClientCertificateRequest,
) -> models.TemporalCloudCredentialStageResult:
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
    request: models.RunTemporalRuntimeCandidateCanaryRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.PromoteTemporalMtlsCaRequest,
) -> models.TemporalCloudPromotionResult:
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
    request: models.PromoteTemporalRuntimeCredentialRequest,
) -> models.TemporalCloudPromotionResult:
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
    request: models.MirrorTemporalRuntimeSecretRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.DeleteTemporalRuntimeCandidateSecretRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.DeleteTemporalCloudMtlsCaSecretRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.DeleteTemporalCloudApiKeyRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.DeleteTemporalCloudServiceAccountRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.TemporalCloudNamespaceRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.TemporalCloudNamespaceRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.DeleteTemporalRuntimeCandidateSecretRequest,
) -> models.TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.delete_temporal_cloud_api_key,
        operation="Temporal Cloud API key teardown",
    )


@activity.defn
def delete_temporal_cloud_service_account(
    request: models.DeleteTemporalRuntimeCandidateSecretRequest,
) -> models.TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.delete_temporal_cloud_service_account,
        operation="Temporal Cloud service account teardown",
    )


@activity.defn
def disable_temporal_cloud_namespace_delete_protection(
    request: models.DeleteTemporalRuntimeCandidateSecretRequest,
) -> models.TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.disable_temporal_cloud_namespace_delete_protection,
        operation="Temporal Cloud namespace delete-protection teardown",
    )


@activity.defn
def delete_temporal_cloud_namespace(
    request: models.DeleteTemporalRuntimeCandidateSecretRequest,
) -> models.TemporalCloudStatusResult:
    return run_project_temporal_delete(
        request,
        temporal_cloud.delete_temporal_cloud_namespace,
        operation="Temporal Cloud namespace teardown",
    )


@activity.defn
def delete_temporal_cloud_runtime_secret(
    request: models.DeleteTemporalRuntimeCandidateSecretRequest,
) -> models.TemporalCloudStatusResult:
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
    request: models.DeleteTemporalRuntimeCandidateSecretRequest,
    delete_fn: Any,
    *,
    operation: str,
) -> models.TemporalCloudStatusResult:
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
        raise errors.non_retryable_error(
            f"boto3 is required for cloud-api Temporal Cloud mode: {error}",
            error_type=errors.CONFIGURATION_ERROR,
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
        raise errors.aws_application_error(
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
            "requiredProperties": list(models.TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES),
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
        readers_group=config.namespace_readers_group,
    )


def activity_heartbeat(detail: Any) -> None:
    try:
        activity.heartbeat(detail)
    except Exception:  # noqa: BLE001
        pass


def platform_manual_setup_required(project: models.TemporalCloudProject) -> bool:
    config = WorkerConfig.from_env()
    return config.temporal_cloud_mode != "cloud-api" or config.external_resource_mode != "aws"


def manual_setup_required_status(
    project: models.TemporalCloudProject,
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
            "requiredProperties": list(models.TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES),
        },
    }


def namespace_result(status: dict[str, Any]) -> models.TemporalCloudNamespaceResult:
    namespace = status.get("namespace")
    namespace = namespace if isinstance(namespace, dict) else {}
    return models.TemporalCloudNamespaceResult(
        status=status,
        namespace=string_value(namespace.get("name")),
        grpc_address=string_value(namespace.get("grpcAddress")),
        auth_mode=string_value(namespace.get("authMode")),
        created=namespace.get("action") == "created",
        region=namespace.get("region") if isinstance(namespace.get("region"), str) else None,
    )


def service_account_result(status: dict[str, Any]) -> models.TemporalCloudServiceAccountResult:
    service_account = status.get("serviceAccount")
    service_account = service_account if isinstance(service_account, dict) else {}
    return models.TemporalCloudServiceAccountResult(
        status=status,
        service_account_id=string_value(service_account.get("id")),
        service_account_name=string_value(service_account.get("name")),
        created=service_account.get("action") == "created",
    )


def namespace_tags_result(
    status: dict[str, Any],
) -> models.TemporalCloudNamespaceTagsResult:
    return models.TemporalCloudNamespaceTagsResult(status=status)


def search_attribute_result(
    status: dict[str, Any],
) -> models.TemporalCloudSearchAttributeResult:
    search_attribute = status.get("searchAttribute")
    search_attribute = search_attribute if isinstance(search_attribute, dict) else {}
    return models.TemporalCloudSearchAttributeResult(
        status=status,
        name=string_value(search_attribute.get("name")),
        type=string_value(search_attribute.get("type")),
        action=string_value(search_attribute.get("action")),
        managed=search_attribute.get("action") == "created",
    )


def search_attributes_result(
    statuses: list[dict[str, Any]],
) -> models.TemporalCloudSearchAttributesResult:
    return models.TemporalCloudSearchAttributesResult(statuses=list(statuses))


def stage_result(status: dict[str, Any]) -> models.TemporalCloudCredentialStageResult:
    return models.TemporalCloudCredentialStageResult(
        status=status,
        promote_required=bool(status.get("promoteRequired")),
        candidate_api_key_id=string_value(status.get("candidateApiKeyId")),
        candidate_secret_path=string_value(status.get("candidateSecretRemotePath")),
        candidate_ca_secret_path=string_value(
            status.get("candidateCaSecretRemotePath")
        ),
        candidate_secret_token=string_value(status.get("candidateSecretToken")),
    )


def runtime_secret_check_result(
    status: dict[str, Any],
) -> models.TemporalRuntimeSecretCheckResult:
    runtime_secret = status.get("runtimeSecret")
    runtime_secret = runtime_secret if isinstance(runtime_secret, dict) else {}
    rotation = status.get("rotation")
    rotation = rotation if isinstance(rotation, dict) else {}
    api_key = status.get("apiKey")
    api_key = api_key if isinstance(api_key, dict) else {}
    return models.TemporalRuntimeSecretCheckResult(
        status=status,
        runtime_secret_path=string_value(runtime_secret.get("secretRemotePath")),
        state=string_value(runtime_secret.get("state")) or "Unknown",
        exists=bool(runtime_secret.get("exists")),
        rotate=bool(status.get("rotate")),
        rotation_reason=string_value(rotation.get("reason")),
        key_id=string_value(api_key.get("keyId")),
        expiry_time=string_value(api_key.get("expiryTime")),
        created_at=string_value(api_key.get("createdAt")),
    )


def promotion_result(status: dict[str, Any]) -> models.TemporalCloudPromotionResult:
    promoted = status.get("promoted")
    promoted = promoted if isinstance(promoted, dict) else {}
    return models.TemporalCloudPromotionResult(
        status=status,
        promoted=bool(promoted.get("runtimeSecret")),
        ca_promoted=bool(promoted.get("caSecret")),
    )


def status_result(status: dict[str, Any]) -> models.TemporalCloudStatusResult:
    return models.TemporalCloudStatusResult(status=status)


def _live_demo_project_names() -> set[str]:
    names: set[str] = set()
    for item in kube.list_demo_projects():
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        name = metadata.get("name")
        if isinstance(name, str) and name:
            names.add(name)
    return names


def nexus_endpoint_result(status: dict[str, Any]) -> models.NexusEndpointResult:
    endpoint_ids = status.get("endpointIds")
    return models.NexusEndpointResult(
        status=status,
        endpoint_ids=endpoint_ids if isinstance(endpoint_ids, dict) else {},
    )


def string_value(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def temporal_cloud_application_error(
    error: Exception,
    *,
    operation: str,
) -> Exception:
    from botocore.exceptions import BotoCoreError

    if isinstance(error, ApplicationError):
        return error
    if isinstance(error, RPCError):
        return errors.temporal_service_application_error(error, operation=operation)
    if errors.aws_error_code(error) or isinstance(error, BotoCoreError):
        return errors.aws_application_error(error, operation=operation)
    message = f"{operation} failed: {error}"
    if isinstance(error, (TimeoutError, ConnectionError)):
        return errors.retryable_provider_error(message)
    if temporal_cloud_transient_error_message(str(error)):
        return errors.retryable_provider_error(message)
    if common.authentication_error_message(str(error)):
        return errors.non_retryable_error(
            message,
            error_type=errors.PROVIDER_AUTHENTICATION_ERROR,
        )
    if isinstance(error, ValueError) or any(
        marker in str(error).lower()
        for marker in ("invalid ", "is required", "must be", "not supported")
    ):
        return errors.non_retryable_error(
            message, error_type=errors.PROVIDER_CONFIGURATION_ERROR
        )
    # Unrecognized provider failures still get the activity's bounded retries.
    return errors.retryable_provider_error(message)


def temporal_cloud_transient_error_message(message: str) -> bool:
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "connection reset",
            "deadline exceeded",
            "invalid resource version",
            # Optimistic-concurrency conflict on the namespace spec, surfaced by
            # a failed async operation. Transient: retry against a fresh version.
            "resource version mismatch",
            # A freshly-created namespace is briefly not ACTIVE; spec updates
            # against it fail until it finishes provisioning. Retry until active.
            "not active",
            "namespace is not ready",
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
