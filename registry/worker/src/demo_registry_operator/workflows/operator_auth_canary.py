from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors
from demo_registry_operator.activities import canary as canary_activities
from demo_registry_operator.workflows import messages


COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
CHECK_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


@workflow.defn
class RegistryOperatorAuthCanaryWorkflow:
    """Single auth-mode branch for a registry-operator rollout canary."""

    @workflow.run
    async def run(
        self,
        branch_request: messages.RegistryOperatorAuthCanaryRequest,
    ) -> messages.RegistryOperatorAuthCanaryResult:
        auth_mode = branch_request.authMode
        return await _run_registry_operator_auth_canary(branch_request, auth_mode)


async def _run_registry_operator_auth_canary(
    branch_request: messages.RegistryOperatorAuthCanaryRequest,
    auth_mode: str,
) -> messages.RegistryOperatorAuthCanaryResult:
    plan_result = await workflow.execute_activity(
        canary_activities.prepare_registry_operator_canary_plan,
        canary_activities.RegistryOperatorCanaryPlanRequest(
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
        canary_activities.apply_registry_operator_canary_project,
        canary_activities.ApplyRegistryOperatorCanaryProjectRequest(
            project=plan["project"],
            namespace=plan["namespace"],
            demo_project=plan["demoProject"],
        ),
        start_to_close_timeout=timedelta(minutes=2),
        retry_policy=COMMON_RETRY_POLICY,
    )
    apply_result = apply_activity.status
    signal_activity = await workflow.execute_activity(
        canary_activities.signal_registry_operator_canary_reconcile,
        canary_activities.SignalRegistryOperatorCanaryReconcileRequest(
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
    finalize_signal_result: dict[str, Any] = {
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
            canary_activities.wait_for_registry_operator_canary_promotion,
            canary_activities.RegistryOperatorCanaryPromotionWaitRequest(
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
                canary_activities.rotate_registry_operator_canary_credentials,
                canary_activities.RotateRegistryOperatorCanaryCredentialsRequest(
                    project=plan["project"],
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            rotation_signal_result = rotation_signal_activity.status
            rotation_activity = await workflow.execute_activity(
                canary_activities.wait_for_registry_operator_canary_rotation,
                canary_activities.RegistryOperatorCanaryRotationWaitRequest(
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
            canary_activities.delete_registry_operator_canary_project,
            canary_activities.DeleteRegistryOperatorCanaryProjectRequest(
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
            finalize_signal_activity = await workflow.execute_activity(
                canary_activities.signal_registry_operator_canary_finalize,
                canary_activities.SignalRegistryOperatorCanaryFinalizeRequest(
                    project=plan["project"],
                    demo_project=plan["demoProject"],
                ),
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=CHECK_RETRY_POLICY,
            )
            finalize_signal_result = finalize_signal_activity.status
            cleanup_timeout_seconds = max(
                1,
                int(plan.get("cleanupTimeoutSeconds", 600)),
            )
            cleanup_activity = await workflow.execute_activity(
                canary_activities.wait_registry_operator_canary_cleanup,
                canary_activities.WaitRegistryOperatorCanaryCleanupRequest(
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
    return messages.RegistryOperatorAuthCanaryResult(
        project=plan.get("project"),
        authMode=auth_mode,
        phase="Succeeded" if succeeded else "Failed",
        plan=plan,
        apply=apply_result,
        workflow=signal_result,
        promotion=promotion_result,
        rotationSignal=rotation_signal_result,
        rotation=rotation_result,
        finalizeSignal=finalize_signal_result,
        cleanup=cleanup_result,
    )
