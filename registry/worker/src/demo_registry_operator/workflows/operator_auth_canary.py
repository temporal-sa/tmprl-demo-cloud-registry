from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, CancelledError, is_cancelled_exception

from demo_registry_operator import errors
from demo_registry_operator.activities import canary as canary_activities
from demo_registry_operator.activities import workspace_canary
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
    failure: dict[str, Any] = field(default_factory=dict)
    compensation_failures: list[dict[str, Any]] = field(default_factory=list)
    cleanup_summary_prefix: str | None = None
    workspace_result: dict[str, Any] = field(default_factory=dict)
    workspace_cleanup_result: dict[str, Any] = field(default_factory=dict)
    workspace_required: bool = False
    workspace_ok: bool = False


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
    if workflow.patched("operator-auth-canary-compensation"):
        return await _run_registry_operator_auth_canary_saga(plan, auth_mode)
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


async def _run_registry_operator_auth_canary_saga(
    plan: dict[str, Any],
    auth_mode: str,
) -> messages.RegistryOperatorAuthCanaryResult:
    state = RegistryOperatorAuthCanaryState(
        plan=plan,
        auth_mode=auth_mode,
        apply_result=not_started_status(),
        signal_result=not_started_status(),
        workflow_id="",
        run_id="",
    )
    # Register before apply: a failed response can still leave a created project.
    # Unwind in delete, finalize, wait order, including after cancellation.
    compensations: list[tuple[str, Callable[[], Awaitable[None]]]] = [
        ("cleanup_result", lambda: _wait_registry_operator_canary_cleanup(state)),
        (
            "finalize_signal_result",
            lambda: _signal_registry_operator_canary_finalize(state),
        ),
        ("cleanup_result", lambda: _delete_registry_operator_canary_project(state)),
    ]
    stage = "apply_result"
    failure: BaseException | None = None
    try:
        state.apply_result = await _apply_registry_operator_canary_project(plan)
        stage = "signal_result"
        state.signal_result = await _signal_registry_operator_canary_reconcile(plan)
        state.workflow_id = str(state.signal_result.get("workflowID") or "")
        state.run_id = str(state.signal_result.get("runID") or "")
        stage = "promotion_result"
        await _wait_for_registry_operator_canary_promotion(state)
        if state.promotion_ok:
            stage = "rotation_result"
            await _run_registry_operator_canary_rotation(state)
            if (
                state.rotation_ok
                and plan.get("workspaceMode")
                and workflow.patched("operator-canary-workspace-lifecycle")
            ):
                state.workspace_required = True
                # Unwind the workspace before deleting its shared credentials,
                # Lambda roles and Temporal namespace, including lost start acks.
                compensations.append(
                    (
                        "workspace_cleanup_result",
                        lambda: _cleanup_workspace_canary(state),
                    )
                )
                stage = "workspace_result"
                await _run_workspace_canary(state)
    except (ActivityError, CancelledError, asyncio.CancelledError) as error:
        failure = error
        state.failure = errors.failure_details(error)
        if isinstance(error, ActivityError) and not is_cancelled_exception(error):
            if error.activity_type == "rotate_registry_operator_canary_credentials":
                stage = "rotation_signal_result"
            failed_status = errors.activity_failure_status(
                error, fallback_phase="Failed"
            )
            if stage == "workspace_result":
                state.workspace_result.update(failed_status)
            else:
                setattr(state, stage, failed_status)
    finally:
        state.cleanup_summary_prefix = (
            "Compensate operator canary"
            if state.failure or not (state.promotion_ok and state.rotation_ok)
            else "Clean up operator canary"
        )
        cleanup = asyncio.create_task(
            _run_registry_operator_canary_compensations(state, compensations)
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await asyncio.shield(cleanup)
            raise
    if failure is not None and (
        isinstance(failure, asyncio.CancelledError) or is_cancelled_exception(failure)
    ):
        raise failure
    return _registry_operator_auth_canary_result(state)


async def _run_registry_operator_canary_compensations(
    state: RegistryOperatorAuthCanaryState,
    compensations: list[tuple[str, Callable[[], Awaitable[None]]]],
) -> None:
    for result_field, compensate in reversed(compensations):
        try:
            await compensate()
            if state.cleanup_result.get("phase") == "Skipped":
                break
        except ActivityError as error:
            failure = errors.failure_details(error)
            state.compensation_failures.append(failure)
            setattr(
                state,
                result_field,
                errors.activity_failure_status(error, fallback_phase="CleanupFailed"),
            )
            workflow.logger.error(
                "Operator canary compensation failed: %s", failure["message"]
            )
            if result_field == "workspace_cleanup_result":
                # Preserve the source dependencies so workspace cleanup can be
                # retried after a failed gate rather than stranding resources.
                break


async def _prepare_registry_operator_canary_plan(
    branch_request: messages.RegistryOperatorAuthCanaryRequest,
) -> dict[str, Any]:
    plan_result = await workflow.execute_activity(
        canary_activities.prepare_registry_operator_canary_plan,
        canary_activities.RegistryOperatorCanaryPlanRequest(
            demo_project_template=(
                branch_request.demoProjectTemplate or branch_request.demoProject or {}
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
            workspace_mode=branch_request.workspaceMode,
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
    await _delete_registry_operator_canary_project(state)
    if state.cleanup_result.get("phase") == "Skipped":
        return
    await _signal_registry_operator_canary_finalize(state)
    await _wait_registry_operator_canary_cleanup(state)


async def _delete_registry_operator_canary_project(
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
        summary=f"{state.cleanup_summary_prefix}: delete project"
        if state.cleanup_summary_prefix
        else None,
    )
    delete_result = delete_activity.status
    state.cleanup_result = delete_result


async def _signal_registry_operator_canary_finalize(
    state: RegistryOperatorAuthCanaryState,
) -> None:
    plan = state.plan
    finalize_signal_activity = await workflow.execute_activity(
        canary_activities.signal_registry_operator_canary_finalize,
        canary_activities.SignalRegistryOperatorCanaryFinalizeRequest(
            project=plan["project"],
            demo_project=plan["demoProject"],
        ),
        start_to_close_timeout=timedelta(minutes=2),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=CHECK_RETRY_POLICY,
        summary=f"{state.cleanup_summary_prefix}: finalize project workflow"
        if state.cleanup_summary_prefix
        else None,
    )
    state.finalize_signal_result = finalize_signal_activity.status


async def _wait_registry_operator_canary_cleanup(
    state: RegistryOperatorAuthCanaryState,
) -> None:
    plan = state.plan
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
            catalog_status_mirror_namespace=plan["catalogStatusMirrorNamespace"],
            catalog_status_config_map=plan["catalogStatusConfigMap"],
            cleanup_policy=str(plan.get("cleanupPolicy", "Always")),
            cleanup_timeout_seconds=cleanup_timeout_seconds,
            poll_seconds=int(plan.get("pollSeconds", 10)),
            delete_action=state.cleanup_result.get("deleteAction"),
        ),
        start_to_close_timeout=timedelta(seconds=cleanup_timeout_seconds + 60),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=CHECK_RETRY_POLICY,
        summary=f"{state.cleanup_summary_prefix}: wait for resource cleanup"
        if state.cleanup_summary_prefix
        else None,
    )
    state.cleanup_result = cleanup_activity.status


def _registry_operator_auth_canary_result(
    state: RegistryOperatorAuthCanaryState,
) -> messages.RegistryOperatorAuthCanaryResult:
    succeeded = (
        state.promotion_ok
        and state.rotation_ok
        and bool(state.cleanup_result.get("ok"))
        and not state.failure
        and not state.compensation_failures
        and (
            not state.workspace_required
            or (state.workspace_ok and bool(state.workspace_cleanup_result.get("ok")))
        )
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
        failure=state.failure,
        compensationFailures=state.compensation_failures,
        workspace=state.workspace_result,
        workspaceCleanup=state.workspace_cleanup_result,
    )


async def _run_workspace_canary(state: RegistryOperatorAuthCanaryState) -> None:
    started = await workflow.execute_activity(
        workspace_canary.begin_workspace_canary,
        workspace_canary.WorkspaceCanaryRequest(state.plan),
        start_to_close_timeout=timedelta(minutes=2),
        retry_policy=COMMON_RETRY_POLICY,
        summary="Start workspace on the candidate operator version",
    )
    state.workspace_result = started.status
    await _check_workspace_canary(state, "ready")
    await _check_workspace_canary(state, "exercise")
    if state.plan["workspaceMode"] == "crashable":
        await _check_workspace_canary(state, "crash")
        await _check_workspace_canary(state, "scale_down")
        await _check_workspace_canary(state, "scale_up")
        await _check_workspace_canary(state, "exercise")
    state.workspace_ok = bool(state.workspace_result.get("ok"))


async def _check_workspace_canary(
    state: RegistryOperatorAuthCanaryState, step: str
) -> None:
    checked = await workflow.execute_activity(
        workspace_canary.check_workspace_canary,
        workspace_canary.WorkspaceCanaryRequest(
            state.plan, state.workspace_result, step
        ),
        start_to_close_timeout=timedelta(
            seconds=int(state.plan.get("timeoutSeconds", 900)) + 60
        ),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=CHECK_RETRY_POLICY,
        summary=f"Check workspace {step}",
    )
    state.workspace_result = checked.status


async def _cleanup_workspace_canary(state: RegistryOperatorAuthCanaryState) -> None:
    cleaned = await workflow.execute_activity(
        workspace_canary.cleanup_workspace_canary,
        workspace_canary.WorkspaceCanaryRequest(state.plan, state.workspace_result),
        start_to_close_timeout=timedelta(
            seconds=int(state.plan.get("cleanupTimeoutSeconds", 600)) + 60
        ),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=CHECK_RETRY_POLICY,
        summary="Clean up workspace before its source demo",
    )
    state.workspace_cleanup_result = cleaned.status
