from __future__ import annotations

from dataclasses import dataclass, field
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


def not_started_status() -> dict[str, Any]:
    return {
        "phase": "NotStarted",
        "ok": False,
    }


@dataclass
class RegistryOperatorAuthCanaryState:
    plan: dict[str, Any]
    auth_mode: str
    apply_result: dict[str, Any]
    signal_result: dict[str, Any]
    workflow_id: str
    run_id: str
    promotion_result: dict[str, Any] = field(default_factory=not_started_status)
    cleanup_result: dict[str, Any] = field(default_factory=not_started_status)
    finalize_signal_result: dict[str, Any] = field(default_factory=not_started_status)
    rotation_signal_result: dict[str, Any] = field(default_factory=not_started_status)
    rotation_result: dict[str, Any] = field(default_factory=not_started_status)
    promotion_ok: bool = False
    rotation_ok: bool = False


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
    plan = await _prepare_registry_operator_canary_plan(branch_request)
    apply_result = await _apply_registry_operator_canary_project(plan)
    signal_result = await _signal_registry_operator_canary_reconcile(plan)
    state = RegistryOperatorAuthCanaryState(
        plan=plan,
        auth_mode=auth_mode,
        apply_result=apply_result,
        signal_result=signal_result,
        workflow_id=str(signal_result.get("workflowID") or ""),
        run_id=str(signal_result.get("runID") or ""),
    )
    try:
        await _wait_for_registry_operator_canary_promotion(state)
        if state.promotion_ok:
            await _run_registry_operator_canary_rotation(state)
    finally:
        await _cleanup_registry_operator_canary(state)

    return _registry_operator_auth_canary_result(state)


async def _prepare_registry_operator_canary_plan(
    branch_request: messages.RegistryOperatorAuthCanaryRequest,
) -> dict[str, Any]:
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
    return plan_result.plan


async def _apply_registry_operator_canary_project(
    plan: dict[str, Any],
) -> dict[str, Any]:
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
    return apply_activity.status


async def _signal_registry_operator_canary_reconcile(
    plan: dict[str, Any],
) -> dict[str, Any]:
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
    return signal_activity.status


async def _wait_for_registry_operator_canary_promotion(
    state: RegistryOperatorAuthCanaryState,
) -> None:
    plan = state.plan
    timeout_seconds = max(1, int(plan.get("timeoutSeconds", 900)))
    promotion_activity = await workflow.execute_activity(
        canary_activities.wait_for_registry_operator_canary_promotion,
        canary_activities.RegistryOperatorCanaryPromotionWaitRequest(
            project=plan["project"],
            namespace=plan["namespace"],
            expected_source_commit_sha=plan.get("expectedSourceCommitSha"),
            expected_images=plan.get("expectedImages", {}),
            workflow_id=state.workflow_id or None,
            run_id=state.run_id or None,
            timeout_seconds=timeout_seconds,
            poll_seconds=int(plan.get("pollSeconds", 10)),
        ),
        start_to_close_timeout=timedelta(seconds=timeout_seconds + 60),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=CHECK_RETRY_POLICY,
    )
    state.promotion_result = promotion_activity.status
    state.promotion_ok = bool(state.promotion_result.get("ok"))


async def _run_registry_operator_canary_rotation(
    state: RegistryOperatorAuthCanaryState,
) -> None:
    plan = state.plan
    timeout_seconds = max(1, int(plan.get("timeoutSeconds", 900)))
    rotation_signal_activity = await workflow.execute_activity(
        canary_activities.rotate_registry_operator_canary_credentials,
        canary_activities.RotateRegistryOperatorCanaryCredentialsRequest(
            project=plan["project"],
        ),
        start_to_close_timeout=timedelta(minutes=2),
        retry_policy=COMMON_RETRY_POLICY,
    )
    state.rotation_signal_result = rotation_signal_activity.status
    rotation_activity = await workflow.execute_activity(
        canary_activities.wait_for_registry_operator_canary_rotation,
        canary_activities.RegistryOperatorCanaryRotationWaitRequest(
            project=plan["project"],
            auth_mode=plan["authMode"],
            rotation_token=str(state.rotation_signal_result.get("token") or ""),
            workflow_id=state.workflow_id or None,
            run_id=state.run_id or None,
            timeout_seconds=timeout_seconds,
            poll_seconds=int(plan.get("pollSeconds", 10)),
        ),
        start_to_close_timeout=timedelta(seconds=timeout_seconds + 60),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=CHECK_RETRY_POLICY,
    )
    state.rotation_result = rotation_activity.status
    state.rotation_ok = bool(state.rotation_result.get("ok"))


async def _cleanup_registry_operator_canary(
    state: RegistryOperatorAuthCanaryState,
) -> None:
    plan = state.plan
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
    state.cleanup_result = delete_result
    if delete_result.get("phase") == "Skipped":
        return

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
    state.finalize_signal_result = finalize_signal_activity.status
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
        start_to_close_timeout=timedelta(seconds=cleanup_timeout_seconds + 60),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=CHECK_RETRY_POLICY,
    )
    state.cleanup_result = cleanup_activity.status


def _registry_operator_auth_canary_result(
    state: RegistryOperatorAuthCanaryState,
) -> messages.RegistryOperatorAuthCanaryResult:
    succeeded = (
        state.promotion_ok
        and state.rotation_ok
        and bool(state.cleanup_result.get("ok"))
    )
    return messages.RegistryOperatorAuthCanaryResult(
        project=state.plan.get("project"),
        authMode=state.auth_mode,
        phase="Succeeded" if succeeded else "Failed",
        plan=state.plan,
        apply=state.apply_result,
        workflow=state.signal_result,
        promotion=state.promotion_result,
        rotationSignal=state.rotation_signal_result,
        rotation=state.rotation_result,
        finalizeSignal=state.finalize_signal_result,
        cleanup=state.cleanup_result,
    )
