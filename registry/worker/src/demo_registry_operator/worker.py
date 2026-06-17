from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import uuid
from typing import Any

from temporalio.api.batch.v1 import message_pb2 as batch_pb2
from temporalio.api.workflowservice.v1 import request_response_pb2 as workflowservice_pb2
from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from demo_registry_operator.activities.canary import (
    apply_registry_operator_canary_project,
    delete_registry_operator_canary_project,
    prepare_registry_operator_canary_plan,
    rotate_registry_operator_canary_credentials,
    signal_registry_operator_canary_reconcile,
    wait_registry_operator_canary_cleanup,
    wait_for_registry_operator_canary_promotion,
    wait_for_registry_operator_canary_rotation,
)
from demo_registry_operator.activities.builds.codebuild.execution import (
    start_project_image_build,
    wait_for_project_image_build,
)
from demo_registry_operator.activities.builds.planning import (
    resolve_project_image_build_plan,
)
from demo_registry_operator.activities.external_resources import (
    configure_temporal_cloud_namespace_mtls,
    create_or_verify_ecr_repository,
    create_or_verify_temporal_cloud_namespace,
    create_or_verify_temporal_cloud_service_account,
    delete_ecr_repositories,
    delete_temporal_cloud_api_key,
    delete_temporal_cloud_api_key_by_id,
    delete_temporal_cloud_mtls_ca_secret,
    delete_temporal_cloud_namespace,
    delete_temporal_cloud_namespace_by_name,
    delete_temporal_cloud_runtime_secret,
    delete_temporal_cloud_service_account,
    delete_temporal_cloud_service_account_by_id,
    delete_temporal_runtime_candidate_secret,
    disable_temporal_cloud_namespace_delete_protection,
    disable_temporal_cloud_namespace_delete_protection_by_name,
    mirror_temporal_runtime_secret,
    promote_temporal_mtls_ca,
    promote_temporal_runtime_credential,
    run_temporal_runtime_candidate_canary,
    stage_temporal_api_key_candidate,
    stage_temporal_mtls_ca,
    stage_temporal_mtls_client_certificate,
    verify_secrets_manager_resources,
)
from demo_registry_operator.activities.force_reconcile import (
    load_current_demo_project_for_reconcile,
    load_current_instruqt_course_for_reconcile,
)
from demo_registry_operator.activities.instruqt import (
    delete_instruqt_course_ecr_repositories,
    delete_instruqt_course_content_auth_secret,
    delete_instruqt_course_content_repository,
    delete_instruqt_course_source_auth_secret,
    delete_instruqt_course_source_repository,
    delete_instruqt_course_track,
    deploy_instruqt_course_track,
    load_instruqt_course_finalization_plan,
    push_instruqt_course_track,
    prepare_instruqt_course_artifact,
    reconcile_instruqt_course_ecr_repositories,
    record_instruqt_course_status,
    remove_instruqt_course_finalizer_resource,
    resolve_instruqt_course_image_build_plan,
    start_instruqt_course_image_build,
    test_instruqt_course_track,
    wait_for_instruqt_course_image_build,
)
from demo_registry_operator.activities.reconcile import (
    apply_demo_project_resources,
    delete_project_namespace_resource,
    delete_project_source_auth_secret,
    delete_project_source_repository,
    load_project_finalization_plan,
    promote_candidate_routes,
    prune_stale_runtime_candidates,
    record_catalog_project_status,
    record_demo_project_status,
    remove_project_finalizer,
    run_smoke_check,
    wait_for_candidate_readiness,
    wait_for_worker_deployment_rollouts,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.temporal_client import connect
from demo_registry_operator.watch import run_demo_project_watch
from demo_registry_operator.workflows.demo_project import (
    DemoProjectWorkflow,
    DemoRolloutWorkflow,
    RegistryOperatorCanaryWorkflow,
)
from demo_registry_operator.workflows.instruqt_course import InstruqtCourseWorkflow


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENTITY_WORKFLOW_TYPES = ("DemoProjectWorkflow", "InstruqtCourseWorkflow")
CHECK_WORKER_VERSION_SIGNAL = "check_worker_version"


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
                workflows=[
                    DemoProjectWorkflow,
                    InstruqtCourseWorkflow,
                    DemoRolloutWorkflow,
                    RegistryOperatorCanaryWorkflow,
                ],
                activities=[
                    prepare_registry_operator_canary_plan,
                    apply_registry_operator_canary_project,
                    signal_registry_operator_canary_reconcile,
                    wait_for_registry_operator_canary_promotion,
                    rotate_registry_operator_canary_credentials,
                    wait_for_registry_operator_canary_rotation,
                    delete_registry_operator_canary_project,
                    wait_registry_operator_canary_cleanup,
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
                    configure_temporal_cloud_namespace_mtls,
                    create_or_verify_temporal_cloud_service_account,
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
                    prune_stale_runtime_candidates,
                    record_demo_project_status,
                    record_catalog_project_status,
                    wait_for_candidate_readiness,
                    wait_for_worker_deployment_rollouts,
                ],
                activity_executor=activity_executor,
                deployment_config=deployment_config,
            )
            tasks.append(asyncio.create_task(worker.run()))
            if deployment_config is not None:
                tasks.append(
                    asyncio.create_task(
                        signal_entity_workflows_for_worker_update_when_current(
                            client,
                            poll_interval_seconds=config.watch_interval_seconds,
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
) -> None:
    version = worker_deployment_version_from_env()
    if version is None:
        return

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
) -> dict[str, Any]:
    query = running_entity_workflows_query()
    try:
        job_id = worker_update_batch_job_id()
        await temporal_client.workflow_service.start_batch_operation(
            workflowservice_pb2.StartBatchOperationRequest(
                namespace=temporal_client.namespace,
                visibility_query=query,
                job_id=job_id,
                reason=(
                    "Ask running registry entity workflows to continue-as-new "
                    "if their target WorkerDeployment version changed."
                ),
                signal_operation=batch_pb2.BatchOperationSignal(
                    signal=signal_name,
                    identity="registry-operator-worker-update",
                ),
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


def running_entity_workflows_query() -> str:
    workflow_type_query = " OR ".join(
        f'WorkflowType = "{workflow_type}"' for workflow_type in ENTITY_WORKFLOW_TYPES
    )
    return f"({workflow_type_query}) AND ExecutionStatus = \"Running\""


def worker_update_batch_job_id() -> str:
    build_id = os.getenv("TEMPORAL_WORKER_BUILD_ID", "unknown")
    sanitized_build_id = "".join(
        character if character.isalnum() or character in {"-", "_", "."} else "-"
        for character in build_id
    )
    return f"registry-worker-update-{sanitized_build_id[:64]}-{uuid.uuid4().hex[:12]}"


if __name__ == "__main__":
    asyncio.run(main())
