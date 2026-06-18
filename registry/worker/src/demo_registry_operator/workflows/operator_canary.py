from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.canary import (
        ApplyRegistryOperatorCanaryProjectRequest,
        DeleteRegistryOperatorCanaryProjectRequest,
        RegistryOperatorCanaryPlanRequest,
        RegistryOperatorCanaryPromotionWaitRequest,
        RegistryOperatorCanaryRotationWaitRequest,
        RotateRegistryOperatorCanaryCredentialsRequest,
        SignalRegistryOperatorCanaryReconcileRequest,
        WaitRegistryOperatorCanaryCleanupRequest,
        apply_registry_operator_canary_project,
        delete_registry_operator_canary_project,
        prepare_registry_operator_canary_plan,
        rotate_registry_operator_canary_credentials,
        signal_registry_operator_canary_reconcile,
        wait_for_registry_operator_canary_promotion,
        wait_for_registry_operator_canary_rotation,
        wait_registry_operator_canary_cleanup,
    )
    from demo_registry_operator.errors import NON_RETRYABLE_ERROR_TYPES
    from demo_registry_operator.workflows.messages import (
        RegistryOperatorAuthCanaryRequest,
        RegistryOperatorAuthCanaryResult,
        RegistryOperatorCanaryRequest,
        RegistryOperatorCanaryResult,
    )


COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
CHECK_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)


@workflow.defn
class RegistryOperatorCanaryWorkflow:
    """End-to-end gate workflow for registry-operator WorkerDeployment rollouts."""

    @workflow.run
    async def run(
        self,
        request: RegistryOperatorCanaryRequest,
    ) -> RegistryOperatorCanaryResult:
        workflow_info = workflow.info()
        branch_requests = [
            registry_operator_canary_branch_request(
                request,
                auth_mode="apiKey",
                workflow_id=workflow_info.workflow_id,
                run_id=workflow_info.run_id,
            ),
            registry_operator_canary_branch_request(
                request,
                auth_mode="mtls",
                workflow_id=workflow_info.workflow_id,
                run_id=workflow_info.run_id,
            ),
        ]
        canary_results = await asyncio.gather(
            *(
                workflow.execute_child_workflow(
                    RegistryOperatorAuthCanaryWorkflow.run,
                    branch_request,
                    id=branch_request.workflowID,
                    task_queue=workflow_info.task_queue,
                    parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
                    versioning_intent=workflow.VersioningIntent.COMPATIBLE,
                )
                for branch_request in branch_requests
            )
        )
        canaries = {result.authMode: result for result in canary_results}
        ok = all(result.phase == "Succeeded" for result in canary_results)
        result = RegistryOperatorCanaryResult(
            phase="Succeeded" if ok else "Failed",
            canaries=canaries,
        )
        if result.phase != "Succeeded":
            raise ApplicationError(
                "Registry operator canary failed; one or more auth-mode canaries did not pass",
                result,
                type="RegistryOperatorCanaryFailed",
                non_retryable=True,
            )
        return result


@workflow.defn
class RegistryOperatorAuthCanaryWorkflow:
    """Single auth-mode branch for a registry-operator rollout canary."""

    @workflow.run
    async def run(
        self,
        branch_request: RegistryOperatorAuthCanaryRequest,
    ) -> RegistryOperatorAuthCanaryResult:
        auth_mode = branch_request.authMode
        return await _run_registry_operator_auth_canary(branch_request, auth_mode)


async def _run_registry_operator_auth_canary(
    branch_request: RegistryOperatorAuthCanaryRequest,
    auth_mode: str,
) -> RegistryOperatorAuthCanaryResult:
    plan_result = await workflow.execute_activity(
        prepare_registry_operator_canary_plan,
        RegistryOperatorCanaryPlanRequest(
            demo_project_template=(
                branch_request.demoProjectTemplate
                or branch_request.demoProject
                or {}
            ),
            workflow_id=branch_request.workflowID,
            run_id=branch_request.runID,
            project_name_prefix=branch_request.projectNamePrefix,
            project_name=branch_request.projectName,
            expected_source_commit_sha=branch_request.expectedSourceCommitSha,
            expected_images=branch_request.expectedImages,
            auth_mode=branch_request.authMode,
            expected_operator_image=branch_request.expectedOperatorImage,
            expected_operator_source_sha=branch_request.expectedOperatorSourceSha,
            timeout_seconds=int(branch_request.timeoutSeconds),
            poll_seconds=int(branch_request.pollSeconds),
            cleanup_timeout_seconds=int(branch_request.cleanupTimeoutSeconds),
            cleanup_policy=str(branch_request.cleanupPolicy),
        ),
        start_to_close_timeout=timedelta(minutes=2),
        retry_policy=COMMON_RETRY_POLICY,
    )
    plan = plan_result.plan
    apply_activity = await workflow.execute_activity(
        apply_registry_operator_canary_project,
        ApplyRegistryOperatorCanaryProjectRequest(
            project=plan["project"],
            namespace=plan["namespace"],
            demo_project=plan["demoProject"],
        ),
        start_to_close_timeout=timedelta(minutes=2),
        retry_policy=COMMON_RETRY_POLICY,
    )
    apply_result = apply_activity.status
    signal_activity = await workflow.execute_activity(
        signal_registry_operator_canary_reconcile,
        SignalRegistryOperatorCanaryReconcileRequest(
            project=plan["project"],
            demo_project=plan["demoProject"],
            target_deployment_name=plan["targetDeploymentName"],
            target_build_id=plan["targetBuildID"],
        ),
        start_to_close_timeout=timedelta(minutes=2),
        retry_policy=COMMON_RETRY_POLICY,
    )
    signal_result = signal_activity.status
    workflow_id = str(signal_result.get("workflowID") or "")
    run_id = str(signal_result.get("runID") or "")
    promotion_result: dict[str, Any] = {
        "phase": "NotStarted",
        "ok": False,
    }
    cleanup_result: dict[str, Any] = {
        "phase": "NotStarted",
        "ok": False,
    }
    rotation_signal_result: dict[str, Any] = {
        "phase": "NotStarted",
        "ok": False,
    }
    rotation_result: dict[str, Any] = {
        "phase": "NotStarted",
        "ok": False,
    }
    promotion_ok = False
    rotation_ok = False
    try:
        timeout_seconds = max(1, int(plan.get("timeoutSeconds", 900)))
        promotion_activity = await workflow.execute_activity(
            wait_for_registry_operator_canary_promotion,
            RegistryOperatorCanaryPromotionWaitRequest(
                project=plan["project"],
                namespace=plan["namespace"],
                expected_source_commit_sha=plan.get("expectedSourceCommitSha"),
                expected_images=plan.get("expectedImages", {}),
                workflow_id=workflow_id or None,
                run_id=run_id or None,
                timeout_seconds=timeout_seconds,
                poll_seconds=int(plan.get("pollSeconds", 10)),
            ),
            start_to_close_timeout=timedelta(seconds=timeout_seconds + 60),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        promotion_result = promotion_activity.status
        promotion_ok = bool(promotion_result.get("ok"))
        if promotion_ok:
            rotation_signal_activity = await workflow.execute_activity(
                rotate_registry_operator_canary_credentials,
                RotateRegistryOperatorCanaryCredentialsRequest(
                    project=plan["project"],
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            rotation_signal_result = rotation_signal_activity.status
            rotation_activity = await workflow.execute_activity(
                wait_for_registry_operator_canary_rotation,
                RegistryOperatorCanaryRotationWaitRequest(
                    project=plan["project"],
                    auth_mode=plan["authMode"],
                    rotation_token=str(rotation_signal_result.get("token") or ""),
                    workflow_id=workflow_id or None,
                    run_id=run_id or None,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=int(plan.get("pollSeconds", 10)),
                ),
                start_to_close_timeout=timedelta(seconds=timeout_seconds + 60),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=CHECK_RETRY_POLICY,
            )
            rotation_result = rotation_activity.status
            rotation_ok = bool(rotation_result.get("ok"))
    finally:
        delete_activity = await workflow.execute_activity(
            delete_registry_operator_canary_project,
            DeleteRegistryOperatorCanaryProjectRequest(
                project=plan["project"],
                cleanup_policy=str(plan.get("cleanupPolicy", "Always")),
            ),
            start_to_close_timeout=timedelta(minutes=2),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        delete_result = delete_activity.status
        cleanup_result = delete_result
        if delete_result.get("phase") != "Skipped":
            cleanup_timeout_seconds = max(
                1,
                int(plan.get("cleanupTimeoutSeconds", 600)),
            )
            cleanup_activity = await workflow.execute_activity(
                wait_registry_operator_canary_cleanup,
                WaitRegistryOperatorCanaryCleanupRequest(
                    project=plan["project"],
                    namespace=plan["namespace"],
                    git_repository=plan["gitRepository"],
                    source_watch_namespace=plan["sourceWatchNamespace"],
                    catalog_status_namespace=plan["catalogStatusNamespace"],
                    catalog_status_mirror_namespace=plan[
                        "catalogStatusMirrorNamespace"
                    ],
                    catalog_status_config_map=plan["catalogStatusConfigMap"],
                    cleanup_policy=str(plan.get("cleanupPolicy", "Always")),
                    cleanup_timeout_seconds=cleanup_timeout_seconds,
                    poll_seconds=int(plan.get("pollSeconds", 10)),
                    delete_action=delete_result.get("deleteAction"),
                ),
                start_to_close_timeout=timedelta(
                    seconds=cleanup_timeout_seconds + 60
                ),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=CHECK_RETRY_POLICY,
            )
            cleanup_result = cleanup_activity.status

    succeeded = promotion_ok and rotation_ok and bool(cleanup_result.get("ok"))
    return RegistryOperatorAuthCanaryResult(
        project=plan.get("project"),
        authMode=auth_mode,
        phase="Succeeded" if succeeded else "Failed",
        plan=plan,
        apply=apply_result,
        workflow=signal_result,
        promotion=promotion_result,
        rotationSignal=rotation_signal_result,
        rotation=rotation_result,
        cleanup=cleanup_result,
    )


def registry_operator_canary_branch_request(
    request: RegistryOperatorCanaryRequest,
    *,
    auth_mode: str,
    workflow_id: str,
    run_id: str,
) -> RegistryOperatorAuthCanaryRequest:
    suffix = "api-key" if auth_mode == "apiKey" else auth_mode
    prefix = request.projectNamePrefix
    prefix = prefix if isinstance(prefix, str) and prefix else "operator-canary"
    project_name = request.projectName
    if isinstance(project_name, str) and project_name:
        project_name = f"{project_name}-{suffix}"
    return RegistryOperatorAuthCanaryRequest(
        workflowID=f"{workflow_id}/{auth_mode}",
        runID=run_id,
        authMode=auth_mode,
        projectNamePrefix=f"{prefix}-{suffix}",
        projectName=project_name,
        demoProjectTemplate=request.demoProjectTemplate,
        demoProject=request.demoProject,
        expectedSourceCommitSha=request.expectedSourceCommitSha,
        expectedImages=request.expectedImages,
        expectedOperatorImage=request.expectedOperatorImage,
        expectedOperatorSourceSha=request.expectedOperatorSourceSha,
        timeoutSeconds=request.timeoutSeconds,
        pollSeconds=request.pollSeconds,
        cleanupTimeoutSeconds=request.cleanupTimeoutSeconds,
        cleanupPolicy=request.cleanupPolicy,
    )
