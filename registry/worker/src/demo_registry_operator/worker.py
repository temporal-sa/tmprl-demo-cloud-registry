from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from temporalio.api.batch.v1 import message_pb2 as batch_pb2
from temporalio.api.common.v1 import message_pb2 as common_pb2
from temporalio.api.workflowservice.v1 import (
    request_response_pb2 as workflowservice_pb2,
)
from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from demo_registry_operator.activities.builds.codebuild.execution import (
    start_project_image_build,
    wait_for_project_image_build,
)
from demo_registry_operator.activities.builds.planning import (
    resolve_project_image_build_plan,
)
from demo_registry_operator.activities.canary import (
    apply_registry_operator_canary_project,
    delete_registry_operator_canary_project,
    load_canary_templates,
    prepare_registry_operator_canary_plan,
    rotate_registry_operator_canary_credentials,
    signal_registry_operator_canary_finalize,
    signal_registry_operator_canary_reconcile,
    wait_for_registry_operator_canary_promotion,
    wait_for_registry_operator_canary_rotation,
    wait_registry_operator_canary_cleanup,
)
from demo_registry_operator.activities.workspace_canary import (
    begin_workspace_canary,
    check_workspace_canary,
    cleanup_workspace_canary,
)
from demo_registry_operator.activities.external_resources import (
    check_temporal_runtime_secret,
    configure_temporal_cloud_namespace_mtls,
    create_or_verify_ecr_repository,
    create_or_verify_temporal_cloud_namespace,
    create_or_verify_temporal_cloud_search_attribute,
    reconcile_temporal_cloud_search_attributes,
    ensure_temporal_cloud_namespace_readers_access,
    ensure_temporal_cloud_namespace_tags,
    reconcile_nexus_endpoints,
    delete_nexus_endpoints,
    create_or_verify_temporal_cloud_service_account,
    create_temporal_api_key_candidate_secret,
    delete_ecr_repositories,
    delete_serverless_lambda_resources,
    delete_temporal_cloud_api_key,
    delete_temporal_cloud_api_key_by_id,
    delete_temporal_cloud_mtls_ca_secret,
    delete_temporal_cloud_namespace,
    delete_temporal_cloud_namespace_by_name,
    delete_temporal_cloud_runtime_secret,
    delete_temporal_cloud_search_attribute,
    delete_temporal_cloud_service_account,
    delete_temporal_cloud_service_account_by_id,
    delete_temporal_runtime_candidate_secret,
    delete_serverless_worker_deployment,
    describe_serverless_worker_deployment_version,
    disable_temporal_cloud_namespace_delete_protection,
    disable_temporal_cloud_namespace_delete_protection_by_name,
    ensure_lambda_execution_role,
    ensure_lambda_function,
    ensure_serverless_worker_deployment,
    ensure_temporal_lambda_invoke_role,
    mirror_temporal_runtime_secret,
    promote_temporal_mtls_ca,
    promote_temporal_runtime_credential,
    run_temporal_runtime_candidate_canary,
    set_serverless_worker_deployment_current,
    stage_temporal_api_key_candidate,
    stage_temporal_mtls_ca,
    stage_temporal_mtls_client_certificate,
    upsert_serverless_worker_deployment_version,
    validate_serverless_worker_compute_config,
    verify_secrets_manager_resources,
)
from demo_registry_operator.activities.force_reconcile import (
    load_current_demo_project_for_reconcile,
    load_current_instruqt_course_for_reconcile,
)
from demo_registry_operator.activities.instruqt import (
    delete_instruqt_course_content_auth_secret,
    delete_instruqt_course_content_repository,
    delete_instruqt_course_ecr_repositories,
    delete_instruqt_course_source_auth_secret,
    delete_instruqt_course_source_repository,
    delete_instruqt_course_track,
    deploy_instruqt_course_track,
    load_instruqt_course_finalization_plan,
    prepare_instruqt_course_artifact,
    push_instruqt_course_track,
    reconcile_instruqt_course_ecr_repositories,
    record_instruqt_course_status,
    remove_instruqt_course_finalizer_resource,
    resolve_instruqt_course_image_build_plan,
    start_instruqt_course_image_build,
    test_instruqt_course_track,
    wait_for_instruqt_course_image_build,
)
from demo_registry_operator.activities.namespace_readers import (
    grant_user_namespace_read_access,
    sync_namespace_readers_group,
)
from demo_registry_operator.activities.reconcile import (
    apply_demo_project_resources,
    delete_project_namespace_resource,
    delete_project_source_auth_secret,
    delete_project_source_repository,
    delete_runtime_candidate_resources,
    load_project_finalization_plan,
    promote_candidate_routes,
    prune_stale_runtime_candidates,
    record_catalog_project_status,
    record_demo_project_status,
    remove_project_finalizer,
    restore_active_runtime_routes,
    rollback_worker_deployments,
    run_smoke_check,
    wait_for_candidate_readiness,
    wait_for_worker_deployment_rollouts,
)
from demo_registry_operator.activities.isolated_lambda_workspace import (
    build_isolated_workspace_plan_activity,
    delete_workspace_lambda_functions,
    resolve_shared_lambda_roles,
    update_workspace_lambda_invoke_policy,
)
from demo_registry_operator.activities.crashable_workspace import (
    apply_workspace_resource,
    build_workspace_plan,
    crash_workspace_pods,
    delete_workspace_namespace,
    resolve_source_images,
    scale_workspace_workloads,
    wait_workspace_ready,
)
from demo_registry_operator.activities.revision import (
    adopt_active_revision_resources,
    delete_revision,
    ensure_revision,
    patch_demo_project_revision_refs,
    plan_candidate_revision,
    plan_revision_deletes,
    set_revision_phase,
    wait_for_revision_cleanup,
)
from demo_registry_operator.activities.rollout_diagnostics import (
    capture_rollout_diagnostics,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.derive import (
    demo_project_workflow_id,
    instruqt_course_workflow_id,
)
from demo_registry_operator.resources import WORKER_LIVENESS_FILE
from demo_registry_operator.temporal_client import connect
from demo_registry_operator.watch import run_demo_project_watch
from demo_registry_operator.workflows.demo_project import DemoProjectWorkflow
from demo_registry_operator.workflows.demo_project_artifact_build import (
    DemoProjectArtifactBuildWorkflow,
)
from demo_registry_operator.workflows.demo_project_kubernetes_runtime import (
    DemoProjectKubernetesRuntimeWorkflow,
)
from demo_registry_operator.workflows.demo_project_serverless_lambda import (
    DemoProjectServerlessLambdaWorkflow,
)
from demo_registry_operator.workflows.crashable_workspace import (
    CrashableWorkspaceWorkflow,
)
from demo_registry_operator.workflows.isolated_lambda_workspace import (
    IsolatedLambdaWorkspaceWorkflow,
)
from demo_registry_operator.workflows.demo_rollout import DemoRolloutWorkflow
from demo_registry_operator.workflows.instruqt_course import InstruqtCourseWorkflow
from demo_registry_operator.workflows.messages import CheckWorkerVersionSignal
from demo_registry_operator.workflows.namespace_readers import (
    GrantNamespaceReadAccessWorkflow,
    SyncNamespaceReadersWorkflow,
)
from demo_registry_operator.workflows.operator_auth_canary import (
    RegistryOperatorAuthCanaryWorkflow,
)
from demo_registry_operator.workflows.operator_canary import (
    RegistryOperatorCanaryWorkflow,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENTITY_WORKFLOW_TYPES = ("DemoProjectWorkflow", "InstruqtCourseWorkflow")
CHECK_WORKER_VERSION_SIGNAL = "check_worker_version"
WORKFLOW_PASSTHROUGH_MODULES = (
    "demo_registry_operator.activities.builds.codebuild.execution",
    "demo_registry_operator.activities.builds.planning",
    "demo_registry_operator.activities.builds.status",
    "demo_registry_operator.activities.canary",
    "demo_registry_operator.activities.crashable_workspace",
    "demo_registry_operator.activities.isolated_lambda_workspace",
    "demo_registry_operator.activities.external_resources",
    "demo_registry_operator.activities.external_resources.serverless_names",
    "demo_registry_operator.activities.force_reconcile",
    "demo_registry_operator.activities.instruqt",
    "demo_registry_operator.activities.namespace_readers",
    "demo_registry_operator.activities.reconcile",
    "demo_registry_operator.activities.rollout_diagnostics",
    "demo_registry_operator.derive",
    "demo_registry_operator.errors",
    "demo_registry_operator.resources",
    "demo_registry_operator.workflows.crashable_workspace_messages",
    "demo_registry_operator.workflows.isolated_lambda_workspace_messages",
    "demo_registry_operator.isolated_lambda_workspace_resources",
    "demo_registry_operator.workflows.demo_project_artifact_build",
    "demo_registry_operator.workflows.demo_project_kubernetes_runtime",
    "demo_registry_operator.workflows.demo_project_serverless_lambda",
    "demo_registry_operator.workflows.messages",
    "demo_registry_operator.workflows.namespace_readers_messages",
    "demo_registry_operator.workflows.operator_auth_canary",
)


def create_workflow_runner() -> SandboxedWorkflowRunner:
    return SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules(
            *WORKFLOW_PASSTHROUGH_MODULES
        )
    )


WORKER_LIVENESS_INTERVAL_SECONDS = 30
WORKER_LIVENESS_CHECK_TIMEOUT_SECONDS = 10


async def liveness_heartbeat(
    client: Client,
    *,
    interval_seconds: int = WORKER_LIVENESS_INTERVAL_SECONDS,
    timeout_seconds: int = WORKER_LIVENESS_CHECK_TIMEOUT_SECONDS,
    path: str = WORKER_LIVENESS_FILE,
) -> None:
    """Refresh the liveness heartbeat file only while Temporal is actually reachable.

    Pairs with resources.worker_liveness_probe(). A wedged worker (e.g. the gRPC
    DNS resolver stuck) stays Running but can't poll, and an OS-level probe would
    falsely pass — so the heartbeat is driven by a real Temporal round-trip over
    the worker's own client. If that stops succeeding the file goes stale and the
    kubelet restarts the container (fresh process -> fresh resolver).
    """
    heartbeat = Path(path)
    try:
        heartbeat.touch()  # cover the probe's initialDelay window before the first check
    except OSError:
        logger.warning("Liveness heartbeat: cannot write %s", path)
    while True:
        try:
            # A real WorkflowService round-trip on the worker's own channel — the
            # same path polling uses, so it fails exactly when the worker is
            # wedged. Deliberately NOT service_client.check_health(): the gRPC
            # health service can't decompress the gzip responses the 1.29 client
            # negotiates ('Decompressor is not installed for grpc-encoding
            # "gzip"'), so it would never refresh on a perfectly healthy worker.
            await client.workflow_service.describe_namespace(
                workflowservice_pb2.DescribeNamespaceRequest(
                    namespace=client.namespace
                ),
                timeout=timedelta(seconds=timeout_seconds),
            )
            heartbeat.touch()
        except Exception:  # noqa: BLE001 - any failure means we can't reach Temporal
            logger.warning(
                "Liveness heartbeat: Temporal round-trip failed; %s not refreshed",
                path,
            )
        await asyncio.sleep(interval_seconds)


async def main() -> None:
    config = WorkerConfig.from_env()
    client = await connect(config)
    logger.info(
        "Starting registry process namespace=%s task_queue=%s worker_enabled=%s watch_enabled=%s watch_mode=%s",
        config.temporal_namespace,
        config.task_queue,
        config.worker_enabled,
        config.watch_enabled,
        config.watch_mode,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.activity_workers,
    ) as activity_executor:
        tasks = []
        if config.worker_enabled:
            deployment_config = worker_deployment_config_from_env()
            worker = Worker(
                client,
                task_queue=config.task_queue,
                workflow_runner=create_workflow_runner(),
                workflows=[
                    DemoProjectWorkflow,
                    DemoProjectArtifactBuildWorkflow,
                    DemoProjectKubernetesRuntimeWorkflow,
                    DemoProjectServerlessLambdaWorkflow,
                    InstruqtCourseWorkflow,
                    DemoRolloutWorkflow,
                    RegistryOperatorCanaryWorkflow,
                    RegistryOperatorAuthCanaryWorkflow,
                    CrashableWorkspaceWorkflow,
                    IsolatedLambdaWorkspaceWorkflow,
                    GrantNamespaceReadAccessWorkflow,
                    SyncNamespaceReadersWorkflow,
                ],
                activities=[
                    load_canary_templates,
                    prepare_registry_operator_canary_plan,
                    apply_registry_operator_canary_project,
                    signal_registry_operator_canary_reconcile,
                    wait_for_registry_operator_canary_promotion,
                    rotate_registry_operator_canary_credentials,
                    wait_for_registry_operator_canary_rotation,
                    delete_registry_operator_canary_project,
                    signal_registry_operator_canary_finalize,
                    wait_registry_operator_canary_cleanup,
                    begin_workspace_canary,
                    check_workspace_canary,
                    cleanup_workspace_canary,
                    load_current_demo_project_for_reconcile,
                    load_current_instruqt_course_for_reconcile,
                    resolve_project_image_build_plan,
                    start_project_image_build,
                    wait_for_project_image_build,
                    prepare_instruqt_course_artifact,
                    reconcile_instruqt_course_ecr_repositories,
                    delete_instruqt_course_ecr_repositories,
                    resolve_instruqt_course_image_build_plan,
                    start_instruqt_course_image_build,
                    wait_for_instruqt_course_image_build,
                    push_instruqt_course_track,
                    test_instruqt_course_track,
                    deploy_instruqt_course_track,
                    delete_instruqt_course_track,
                    record_instruqt_course_status,
                    load_instruqt_course_finalization_plan,
                    delete_instruqt_course_source_repository,
                    delete_instruqt_course_source_auth_secret,
                    delete_instruqt_course_content_repository,
                    delete_instruqt_course_content_auth_secret,
                    remove_instruqt_course_finalizer_resource,
                    create_or_verify_ecr_repository,
                    stage_temporal_mtls_ca,
                    create_or_verify_temporal_cloud_namespace,
                    create_or_verify_temporal_cloud_search_attribute,
                    reconcile_temporal_cloud_search_attributes,
                    ensure_temporal_cloud_namespace_tags,
                    ensure_temporal_cloud_namespace_readers_access,
                    reconcile_nexus_endpoints,
                    delete_nexus_endpoints,
                    delete_temporal_cloud_search_attribute,
                    configure_temporal_cloud_namespace_mtls,
                    create_or_verify_temporal_cloud_service_account,
                    check_temporal_runtime_secret,
                    create_temporal_api_key_candidate_secret,
                    stage_temporal_api_key_candidate,
                    stage_temporal_mtls_client_certificate,
                    run_temporal_runtime_candidate_canary,
                    promote_temporal_mtls_ca,
                    promote_temporal_runtime_credential,
                    mirror_temporal_runtime_secret,
                    delete_temporal_runtime_candidate_secret,
                    delete_temporal_cloud_api_key_by_id,
                    delete_temporal_cloud_mtls_ca_secret,
                    delete_temporal_cloud_service_account_by_id,
                    disable_temporal_cloud_namespace_delete_protection_by_name,
                    delete_temporal_cloud_namespace_by_name,
                    verify_secrets_manager_resources,
                    ensure_lambda_execution_role,
                    ensure_lambda_function,
                    ensure_temporal_lambda_invoke_role,
                    ensure_serverless_worker_deployment,
                    upsert_serverless_worker_deployment_version,
                    validate_serverless_worker_compute_config,
                    set_serverless_worker_deployment_current,
                    describe_serverless_worker_deployment_version,
                    delete_serverless_worker_deployment,
                    delete_serverless_lambda_resources,
                    delete_temporal_cloud_api_key,
                    delete_temporal_cloud_service_account,
                    disable_temporal_cloud_namespace_delete_protection,
                    delete_temporal_cloud_namespace,
                    delete_temporal_cloud_runtime_secret,
                    delete_ecr_repositories,
                    apply_demo_project_resources,
                    load_project_finalization_plan,
                    delete_project_namespace_resource,
                    delete_project_source_repository,
                    delete_project_source_auth_secret,
                    remove_project_finalizer,
                    run_smoke_check,
                    promote_candidate_routes,
                    restore_active_runtime_routes,
                    rollback_worker_deployments,
                    prune_stale_runtime_candidates,
                    plan_candidate_revision,
                    plan_revision_deletes,
                    ensure_revision,
                    set_revision_phase,
                    delete_revision,
                    patch_demo_project_revision_refs,
                    wait_for_revision_cleanup,
                    adopt_active_revision_resources,
                    delete_runtime_candidate_resources,
                    record_demo_project_status,
                    record_catalog_project_status,
                    wait_for_candidate_readiness,
                    wait_for_worker_deployment_rollouts,
                    capture_rollout_diagnostics,
                    resolve_source_images,
                    build_workspace_plan,
                    apply_workspace_resource,
                    wait_workspace_ready,
                    crash_workspace_pods,
                    scale_workspace_workloads,
                    delete_workspace_namespace,
                    build_isolated_workspace_plan_activity,
                    resolve_shared_lambda_roles,
                    update_workspace_lambda_invoke_policy,
                    delete_workspace_lambda_functions,
                    grant_user_namespace_read_access,
                    sync_namespace_readers_group,
                ],
                activity_executor=activity_executor,
                deployment_config=deployment_config,
            )
            tasks.append(asyncio.create_task(worker.run()))
            tasks.append(asyncio.create_task(liveness_heartbeat(client)))
            if deployment_config is not None:
                tasks.append(
                    asyncio.create_task(
                        signal_entity_workflows_for_worker_update_when_current(
                            client,
                            poll_interval_seconds=config.watch_interval_seconds,
                            cluster=config.cluster,
                        )
                    )
                )
        if config.watch_enabled:
            tasks.append(asyncio.create_task(run_demo_project_watch(client, config)))
        if not tasks:
            raise RuntimeError(
                "At least one of DEMO_REGISTRY_WORKER_ENABLED or DEMO_REGISTRY_WATCH_ENABLED must be true."
            )

        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_EXCEPTION,
        )
        for task in pending:
            task.cancel()
        for task in done:
            task.result()


def worker_deployment_config_from_env() -> WorkerDeploymentConfig | None:
    version = worker_deployment_version_from_env()
    if version is None:
        return None
    return WorkerDeploymentConfig(
        version=version,
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


def worker_deployment_version_from_env() -> WorkerDeploymentVersion | None:
    deployment_name = os.getenv("TEMPORAL_DEPLOYMENT_NAME")
    build_id = os.getenv("TEMPORAL_WORKER_BUILD_ID")
    if not deployment_name or not build_id:
        return None
    return WorkerDeploymentVersion(
        deployment_name=deployment_name,
        build_id=build_id,
    )


async def signal_entity_workflows_for_worker_update_when_current(
    temporal_client: Client,
    *,
    poll_interval_seconds: int,
    cluster: str | None = None,
    restart_process: Callable[[], None] | None = None,
) -> None:
    version = worker_deployment_version_from_env()
    if version is None:
        return
    restart_process = restart_process or restart_process_after_worker_promotion
    observed_not_current = False

    logger.info(
        "Waiting for WorkerDeployment version %s/%s to become current "
        "before signaling entity workflows.",
        version.deployment_name,
        version.build_id,
    )
    while True:
        try:
            if await temporal_worker_deployment_version_is_current(
                temporal_client,
                version,
            ):
                break
            observed_not_current = True
        except Exception:
            logger.warning(
                "Could not verify current WorkerDeployment version; will retry.",
                exc_info=True,
            )
        await asyncio.sleep(poll_interval_seconds)

    try:
        result = await signal_running_entity_workflows_for_worker_update(
            temporal_client,
            signal_name=CHECK_WORKER_VERSION_SIGNAL,
            cluster=cluster,
        )
        logger.info(
            "Signaled running entity workflows for worker version update: %s",
            result,
        )
    except Exception:
        logger.warning(
            "Failed to signal entity workflows for worker version update.",
            exc_info=True,
        )
    if observed_not_current:
        logger.warning(
            "WorkerDeployment version %s/%s became current after this process "
            "started; exiting so Kubernetes restarts Temporal worker pollers "
            "with a fresh SDK core.",
            version.deployment_name,
            version.build_id,
        )
        restart_process()


def restart_process_after_worker_promotion() -> None:
    os._exit(0)


async def temporal_worker_deployment_version_is_current(
    temporal_client: Client,
    version: WorkerDeploymentVersion,
) -> bool:
    response = await temporal_client.workflow_service.describe_worker_deployment(
        workflowservice_pb2.DescribeWorkerDeploymentRequest(
            namespace=temporal_client.namespace,
            deployment_name=version.deployment_name,
        ),
        retry=True,
    )
    return worker_deployment_info_current_matches(
        response.worker_deployment_info,
        version,
    )


def worker_deployment_info_current_matches(
    worker_deployment_info: Any,
    version: WorkerDeploymentVersion,
) -> bool:
    routing_config = getattr(worker_deployment_info, "routing_config", None)
    current = getattr(routing_config, "current_deployment_version", None)
    return (
        getattr(current, "deployment_name", None) == version.deployment_name
        and getattr(current, "build_id", None) == version.build_id
    )


async def signal_running_entity_workflows_for_worker_update(
    temporal_client: Client,
    *,
    signal_name: str = CHECK_WORKER_VERSION_SIGNAL,
    cluster: str | None = None,
) -> dict[str, Any]:
    query = running_entity_workflows_query(cluster)
    try:
        job_id = worker_update_batch_job_id()
        signal_operation = batch_pb2.BatchOperationSignal(
            signal=signal_name,
            identity="registry-operator-worker-update",
        )
        if signal_name == CHECK_WORKER_VERSION_SIGNAL:
            payloads = await temporal_client.data_converter.encode(
                [CheckWorkerVersionSignal()]
            )
            signal_operation.input.CopyFrom(common_pb2.Payloads(payloads=payloads))
        await temporal_client.workflow_service.start_batch_operation(
            workflowservice_pb2.StartBatchOperationRequest(
                namespace=temporal_client.namespace,
                visibility_query=query,
                job_id=job_id,
                reason=(
                    "Ask running registry entity workflows to continue-as-new "
                    "if their target WorkerDeployment version changed."
                ),
                signal_operation=signal_operation,
            ),
            retry=True,
        )
        return {
            "mode": "batch",
            "jobID": job_id,
            "query": query,
        }
    except Exception:
        logger.warning(
            "Temporal batch signal failed; falling back to per-workflow signaling.",
            exc_info=True,
        )

    signaled = await signal_running_entity_workflows_individually(
        temporal_client,
        signal_name=signal_name,
        query=query,
    )
    return {
        "mode": "per-workflow",
        "signaled": signaled,
        "query": query,
    }


async def signal_running_entity_workflows_individually(
    temporal_client: Client,
    *,
    signal_name: str,
    query: str,
    page_size: int = 100,
) -> int:
    signaled = 0
    async for execution in temporal_client.list_workflows(
        query,
        page_size=page_size,
    ):
        try:
            handle = temporal_client.get_workflow_handle(
                execution.id,
                run_id=execution.run_id,
            )
            if signal_name == CHECK_WORKER_VERSION_SIGNAL:
                await handle.signal(signal_name, CheckWorkerVersionSignal())
            else:
                await handle.signal(signal_name)
            signaled += 1
        except Exception:
            logger.warning(
                "Failed to signal workflow_id=%s run_id=%s with %s.",
                execution.id,
                execution.run_id,
                signal_name,
                exc_info=True,
            )
    return signaled


def running_entity_workflows_query(cluster: str | None = None) -> str:
    workflow_type_query = " OR ".join(
        f'WorkflowType = "{workflow_type}"' for workflow_type in ENTITY_WORKFLOW_TYPES
    )
    query = f'({workflow_type_query}) AND ExecutionStatus = "Running"'
    if cluster:
        # This batch signal is namespace-wide, and blue/green (plus the uncolored
        # pet) share one Temporal namespace. Without an id scope, a colored
        # operator's worker-update handoff would signal the OTHER cluster's running
        # entity workflows. Scope it to this color's ids. The prefixes come from the
        # same derive helpers the producers use (empty name -> just the
        # "<type>/<color>/" segment) so the scope can never drift from real ids.
        demo_prefix = demo_project_workflow_id("", cluster)
        course_prefix = instruqt_course_workflow_id("", cluster)
        query += (
            f' AND (WorkflowId STARTS_WITH "{demo_prefix}"'
            f' OR WorkflowId STARTS_WITH "{course_prefix}")'
        )
    return query


def worker_update_batch_job_id() -> str:
    build_id = os.getenv("TEMPORAL_WORKER_BUILD_ID", "unknown")
    sanitized_build_id = "".join(
        character if character.isalnum() or character in {"-", "_", "."} else "-"
        for character in build_id
    )
    return f"registry-worker-update-{sanitized_build_id[:64]}-{uuid.uuid4().hex[:12]}"


if __name__ == "__main__":
    asyncio.run(main())
