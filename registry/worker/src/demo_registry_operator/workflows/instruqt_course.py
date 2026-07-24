from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors, resources
from demo_registry_operator.activities import force_reconcile as force_reconcile_activities
from demo_registry_operator.activities import instruqt as instruqt_activities
from demo_registry_operator.workflows import messages


MAX_RECONCILES_PER_WORKFLOW_RUN = 50

COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
STATUS_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=12,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
BUILD_WAIT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=15),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
INCOMPLETE_RECONCILE_PHASES = {
    "BuildFailed",
    "DeleteFailed",
    "DeployFailed",
    "Error",
    "PushFailed",
    "TestFailed",
    "ValidationFailed",
    "ValidationBlocked",
    "WaitingForImages",
}
SUCCESSFUL_BUILD_PHASES = {"Pushed", "Resolved", "NotRequired"}


@dataclass
class InstruqtCourseReconcileInput:
    instruqt_course: dict[str, Any] | None


@dataclass
class InstruqtCourseSourceDeletes:
    source_action: str
    source_auth_action: str
    content_source_action: str
    content_source_auth_action: str


@dataclass
class InstruqtCourseTrackDeletes:
    production_track_action: dict[str, Any]
    testing_track_action: dict[str, Any]


def reconcile_identity(instruqt_course: dict[str, Any]) -> dict[str, str]:
    annotations = resources.metadata_annotations(instruqt_course)
    identity = {
        "reconcileKey": resources.instruqt_course_reconcile_key(instruqt_course),
    }
    source_commit_sha = annotations.get(resources.SOURCE_COMMIT_ANNOTATION)
    if source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = annotations.get(resources.SOURCE_ARTIFACT_DIGEST_ANNOTATION)
    if source_artifact_digest:
        identity["sourceArtifactDigest"] = source_artifact_digest
    content_source_commit_sha = annotations.get(resources.CONTENT_SOURCE_COMMIT_ANNOTATION)
    if content_source_commit_sha:
        identity["contentSourceCommitSha"] = content_source_commit_sha
    content_source_artifact_digest = annotations.get(
        resources.CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION
    )
    if content_source_artifact_digest:
        identity["contentSourceArtifactDigest"] = content_source_artifact_digest
    testing_branch_state = annotations.get(resources.TESTING_BRANCH_STATE_ANNOTATION)
    if testing_branch_state:
        identity["testingBranchState"] = testing_branch_state
    testing_source_commit_sha = annotations.get(resources.TESTING_SOURCE_COMMIT_ANNOTATION)
    if testing_source_commit_sha:
        identity["testingSourceCommitSha"] = testing_source_commit_sha
    testing_source_artifact_digest = annotations.get(
        resources.TESTING_SOURCE_ARTIFACT_DIGEST_ANNOTATION
    )
    if testing_source_artifact_digest:
        identity["testingSourceArtifactDigest"] = testing_source_artifact_digest
    testing_content_source_commit_sha = annotations.get(
        resources.TESTING_CONTENT_SOURCE_COMMIT_ANNOTATION
    )
    if testing_content_source_commit_sha:
        identity["testingContentSourceCommitSha"] = testing_content_source_commit_sha
    testing_content_source_artifact_digest = annotations.get(
        resources.TESTING_CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION
    )
    if testing_content_source_artifact_digest:
        identity["testingContentSourceArtifactDigest"] = (
            testing_content_source_artifact_digest
        )
    return identity


def force_reconcile_request(
    token: str,
    reason: str | None = None,
) -> dict[str, str] | None:
    if not isinstance(token, str) or not token.strip():
        return None
    request = {"token": token.strip()}
    if isinstance(reason, str) and reason.strip():
        request["reason"] = reason.strip()
    return request


def status_reconcile_identity(status: dict[str, Any]) -> dict[str, str] | None:
    reconcile_key = status.get("reconcileKey")
    if not isinstance(reconcile_key, str) or not reconcile_key:
        return None
    identity = {"reconcileKey": reconcile_key}
    source = status.get("source")
    source = source if isinstance(source, dict) else {}
    source_commit_sha = source.get("commitSha")
    if isinstance(source_commit_sha, str) and source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = source.get("artifactDigest")
    if isinstance(source_artifact_digest, str) and source_artifact_digest:
        identity["sourceArtifactDigest"] = source_artifact_digest
    content = status.get("content")
    content = content if isinstance(content, dict) else {}
    content_source_commit_sha = content.get("commitSha")
    if isinstance(content_source_commit_sha, str) and content_source_commit_sha:
        identity["contentSourceCommitSha"] = content_source_commit_sha
    content_source_artifact_digest = content.get("artifactDigest")
    if (
        isinstance(content_source_artifact_digest, str)
        and content_source_artifact_digest
    ):
        identity["contentSourceArtifactDigest"] = content_source_artifact_digest
    testing = status.get("testing")
    testing = testing if isinstance(testing, dict) else {}
    testing_state = testing.get("state")
    if isinstance(testing_state, str) and testing_state:
        identity["testingBranchState"] = testing_state
    testing_source = testing.get("source")
    testing_source = testing_source if isinstance(testing_source, dict) else {}
    testing_source_commit_sha = testing_source.get("commitSha")
    if isinstance(testing_source_commit_sha, str) and testing_source_commit_sha:
        identity["testingSourceCommitSha"] = testing_source_commit_sha
    testing_source_artifact_digest = testing_source.get("artifactDigest")
    if (
        isinstance(testing_source_artifact_digest, str)
        and testing_source_artifact_digest
    ):
        identity["testingSourceArtifactDigest"] = testing_source_artifact_digest
    testing_content = testing.get("content")
    testing_content = testing_content if isinstance(testing_content, dict) else {}
    testing_content_source_commit_sha = testing_content.get("commitSha")
    if (
        isinstance(testing_content_source_commit_sha, str)
        and testing_content_source_commit_sha
    ):
        identity["testingContentSourceCommitSha"] = testing_content_source_commit_sha
    testing_content_source_artifact_digest = testing_content.get("artifactDigest")
    if (
        isinstance(testing_content_source_artifact_digest, str)
        and testing_content_source_artifact_digest
    ):
        identity["testingContentSourceArtifactDigest"] = (
            testing_content_source_artifact_digest
        )
    return identity


@workflow.defn
class InstruqtCourseWorkflow:
    """Durable entity workflow for one operator-managed InstruqtCourse."""

    @workflow.init
    def __init__(
        self,
        request: messages.InstruqtCourseRunRequest,
    ) -> None:
        self._course_name = request.course_name
        self._pending_spec: dict[str, Any] | None = None
        self._pending_reconcile_identity: dict[str, str] | None = None
        self._current_reconcile_identity: dict[str, str] | None = None
        self._completed_reconcile_identity: dict[str, str] | None = None
        self._reconcile_sequence = 0
        self._shutdown = False
        self._finalize = False
        self._continue_as_new_for_worker_version = False
        self._pending_force_reconcile: dict[str, str] | None = None
        self._current_force_reconcile_token: str | None = None
        self._completed_force_reconcile_token: str | None = None
        self._finalize_spec: dict[str, Any] | None = None
        self._last_build_status: dict[str, Any] | None = None
        self._last_testing_build_status: dict[str, Any] | None = None
        self._status: dict[str, Any] = {
            "phase": "Starting",
            "message": "Workflow has not received course state.",
        }

    @workflow.run
    async def run(
        self,
        request: messages.InstruqtCourseRunRequest,
    ) -> None:
        course_name = request.course_name
        self._course_name = course_name
        self._apply_carried_state(request, course_name)

        while not self._shutdown:
            await self._wait_for_next_action()
            if self._shutdown:
                break

            if self._finalize:
                await self._run_finalize(course_name)
                return

            if self._continue_as_new_for_worker_version:
                self._continue_as_new_if_needed(course_name)
                continue

            reconcile_input = await self._select_instruqt_course_for_reconcile(
                course_name
            )
            reconciled = await self._run_instruqt_course_reconcile(
                course_name,
                reconcile_input,
            )
            if reconciled:
                self._complete_current_reconcile()
            self._continue_as_new_if_needed(course_name)

        self._status = {
            "course": course_name,
            "phase": "Stopped",
            "message": "Workflow received shutdown signal.",
        }

    def _apply_carried_state(
        self,
        request: messages.InstruqtCourseRunRequest,
        course_name: str,
    ) -> None:
        if request.carried_pending_spec is not None:
            self._pending_spec = request.carried_pending_spec
            self._pending_reconcile_identity = reconcile_identity(
                request.carried_pending_spec
            )
        if request.carried_last_build_status is not None:
            self._last_build_status = request.carried_last_build_status
        if request.carried_last_testing_build_status is not None:
            self._last_testing_build_status = (
                request.carried_last_testing_build_status
            )
        if (
            request.carried_completed_reconcile_identity is not None
            and remember_completed_reconcile_identity(request.carried_status)
        ):
            self._completed_reconcile_identity = (
                request.carried_completed_reconcile_identity
            )
        if request.carried_pending_force_reconcile is not None:
            self._pending_force_reconcile = request.carried_pending_force_reconcile
        if request.carried_completed_force_reconcile_token is not None:
            self._completed_force_reconcile_token = (
                request.carried_completed_force_reconcile_token
            )
        if request.carried_status is not None:
            self._status = request.carried_status
            if self._last_build_status is None:
                self._last_build_status = rememberable_instruqt_build_status(
                    request.carried_status.get("buildStatus")
                )
            if self._last_testing_build_status is None:
                testing = request.carried_status.get("testing")
                testing = testing if isinstance(testing, dict) else {}
                self._last_testing_build_status = rememberable_instruqt_build_status(
                    testing.get("buildStatus")
                )
            if (
                self._completed_reconcile_identity is None
                and remember_completed_reconcile_identity(request.carried_status)
            ):
                self._completed_reconcile_identity = status_reconcile_identity(
                    request.carried_status
                )
            return

        self._status = {
            "course": course_name,
            "phase": "Waiting",
            "message": "Waiting for InstruqtCourse reconcile signal.",
        }

    async def _wait_for_next_action(self) -> None:
        await workflow.wait_condition(self._has_pending_action)

    def _has_pending_action(self) -> bool:
        return (
            self._pending_spec is not None
            or self._pending_force_reconcile is not None
            or self._finalize
            or self._shutdown
            or self._continue_as_new_for_worker_version
        )

    async def _select_instruqt_course_for_reconcile(
        self,
        course_name: str,
    ) -> InstruqtCourseReconcileInput:
        force_reconcile = self._pending_force_reconcile
        if force_reconcile is not None:
            instruqt_course = await self._load_forced_instruqt_course_reconcile(
                course_name,
                force_reconcile,
            )
        else:
            instruqt_course = self._pending_spec
            self._pending_spec = None
            self._current_reconcile_identity = (
                self._pending_reconcile_identity
                if self._pending_reconcile_identity is not None
                else reconcile_identity(instruqt_course)
            )
            self._pending_reconcile_identity = None

        return InstruqtCourseReconcileInput(instruqt_course=instruqt_course)

    async def _load_forced_instruqt_course_reconcile(
        self,
        course_name: str,
        force_reconcile: dict[str, str],
    ) -> dict[str, Any]:
        self._pending_force_reconcile = None
        self._pending_spec = None
        self._pending_reconcile_identity = None
        result = await workflow.execute_activity(
            force_reconcile_activities.load_current_instruqt_course_for_reconcile,
            force_reconcile_activities.LoadCurrentInstruqtCourseForReconcileRequest(
                name=course_name,
                token=force_reconcile["token"],
                reason=force_reconcile.get("reason"),
            ),
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=COMMON_RETRY_POLICY,
        )
        instruqt_course = result.resource
        self._current_force_reconcile_token = force_reconcile["token"]
        self._current_reconcile_identity = reconcile_identity(instruqt_course)
        return instruqt_course

    async def _run_instruqt_course_reconcile(
        self,
        course_name: str,
        reconcile_input: InstruqtCourseReconcileInput,
    ) -> bool:
        instruqt_course = reconcile_input.instruqt_course
        if instruqt_course is None:
            return False

        self._reconcile_sequence += 1
        self._status = {
            "course": course_name,
            "phase": "Reconciling",
        }

        self._status = instruqt_activities.reconcile_instruqt_course(
            instruqt_course,
            checked_at=workflow.now().isoformat(),
        )
        if self._status.get("phase") != "Error":
            ecr_status = await self._reconcile_instruqt_ecr(instruqt_course)
            await self._reconcile_production_channel(
                instruqt_course,
                ecr_status,
            )
            if production_deployment_complete(self._status):
                await self._reconcile_testing_channel(
                    instruqt_course,
                    ecr_status,
                    force_reconcile=bool(
                        resources.force_reconcile_token(instruqt_course)
                    ),
                )
            self._status = finalized_instruqt_status(self._status)

        await self._record_status(instruqt_course, self._status)
        return True

    async def _reconcile_instruqt_ecr(
        self,
        instruqt_course: dict[str, Any],
    ) -> dict[str, Any]:
        ecr_activity = await workflow.execute_activity(
            instruqt_activities.reconcile_instruqt_course_ecr_repositories,
            instruqt_activities.InstruqtCourseRequest(instruqt_course),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        ecr_status = ecr_activity.status
        self._status = status_with_ecr_status(self._status, ecr_status)
        return ecr_status

    async def _reconcile_production_channel(
        self,
        instruqt_course: dict[str, Any],
        ecr_status: dict[str, Any],
    ) -> None:
        build_status = await self._resolve_images(
            instruqt_course,
            ecr_status,
            channel="production",
            previous_build_status=(
                None
                if resources.force_reconcile_token(instruqt_course)
                else self._last_build_status
            ),
        )
        remembered_build_status = rememberable_instruqt_build_status(build_status)
        if remembered_build_status is not None:
            self._last_build_status = remembered_build_status
        self._status = status_with_build_status(
            self._status,
            build_status,
            testing=False,
        )

        if build_status.get("phase") not in SUCCESSFUL_BUILD_PHASES:
            return

        deployment_status = await self._deploy_track_channel(
            instruqt_course,
            images=build_status.get("images", {}),
            channel="production",
        )
        self._status = status_with_deployment_status(
            self._status,
            deployment_status,
            testing=False,
        )

    @workflow.signal
    def reconcile(self, request: messages.InstruqtCourseReconcileSignal) -> None:
        instruqt_course = request.instruqtCourse
        identity = reconcile_identity(instruqt_course)
        if self._is_duplicate_reconcile_signal(identity):
            return
        self._pending_spec = instruqt_course
        self._pending_reconcile_identity = identity

    @workflow.signal
    def force_reconcile(self, signal: messages.ForceReconcileSignal) -> None:
        request = force_reconcile_request(signal.token, signal.reason)
        if request is None:
            return
        token = request["token"]
        if token in {
            self._pending_force_reconcile_token(),
            self._current_force_reconcile_token,
            self._completed_force_reconcile_token,
        }:
            return
        self._pending_force_reconcile = request

    @workflow.signal
    def check_worker_version(
        self,
        _signal: messages.CheckWorkerVersionSignal = messages.CheckWorkerVersionSignal(),
    ) -> None:
        if workflow.info().is_target_worker_deployment_version_changed():
            self._continue_as_new_for_worker_version = True

    @workflow.signal
    def finalize(self, request: messages.InstruqtCourseFinalizeSignal) -> None:
        self._finalize_spec = request.instruqtCourse
        self._finalize = True

    @workflow.signal
    def shutdown(self, _signal: messages.ShutdownSignal = messages.ShutdownSignal()) -> None:
        self._shutdown = True

    async def _reconcile_testing_channel(
        self,
        instruqt_course: dict[str, Any],
        ecr_status: dict[str, Any],
        *,
        force_reconcile: bool = False,
    ) -> None:
        testing_status = self._status.get("testing")
        testing_status = testing_status if isinstance(testing_status, dict) else {}
        if testing_status.get("phase") == "BranchMissing":
            testing_delete_activity = await workflow.execute_activity(
                instruqt_activities.delete_instruqt_course_track,
                instruqt_activities.InstruqtTrackDeleteRequest(
                    instruqt_course=instruqt_course,
                    channel="testing",
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            testing_delete_status = testing_delete_activity.status
            self._status = status_with_deployment_status(
                self._status,
                testing_delete_status,
                testing=True,
            )
            return

        if testing_status.get("resolved") is not True:
            return

        testing_build_status = await self._resolve_images(
            instruqt_course,
            ecr_status,
            channel="testing",
            previous_build_status=(
                None if force_reconcile else self._last_testing_build_status
            ),
        )
        remembered_testing_build_status = rememberable_instruqt_build_status(
            testing_build_status
        )
        if remembered_testing_build_status is not None:
            self._last_testing_build_status = remembered_testing_build_status
        self._status = status_with_build_status(
            self._status,
            testing_build_status,
            testing=True,
        )
        if testing_build_status.get("phase") not in SUCCESSFUL_BUILD_PHASES:
            return

        testing_deployment_status = await self._deploy_track_channel(
            instruqt_course,
            images=testing_build_status.get("images", {}),
            channel="testing",
        )
        self._status = status_with_deployment_status(
            self._status,
            testing_deployment_status,
            testing=True,
        )

    async def _deploy_track_channel(
        self,
        instruqt_course: dict[str, Any],
        *,
        images: dict[str, Any],
        channel: str,
    ) -> dict[str, Any]:
        request = instruqt_activities.InstruqtCourseArtifactRequest(
            instruqt_course=instruqt_course,
            images=images,
            channel=channel,
        )
        push_activity = await workflow.execute_activity(
            instruqt_activities.push_instruqt_course_track,
            request,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        push_status = push_activity.status
        if push_status.get("phase") != "Pushed":
            return push_status

        deploy_activity = await workflow.execute_activity(
            instruqt_activities.deploy_instruqt_course_track,
            instruqt_activities.InstruqtTrackDeployRequest(
                instruqt_course=instruqt_course,
                images=images,
                channel=channel,
                push_status=push_status,
            ),
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return deploy_activity.status

    async def _resolve_images(
        self,
        instruqt_course: dict[str, Any],
        ecr_status: dict[str, Any],
        *,
        channel: str,
        previous_build_status: dict[str, Any] | None,
    ) -> dict[str, Any]:
        build_plan_result = await workflow.execute_activity(
            instruqt_activities.resolve_instruqt_course_image_build_plan,
            instruqt_activities.InstruqtCourseImageBuildPlanRequest(
                instruqt_course=instruqt_course,
                external_resources={"ecr": ecr_status},
                previous_build_status=previous_build_status,
                channel=channel,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        build_plan = build_plan_result.plan
        build_status = instruqt_activities.completed_instruqt_image_status(build_plan)
        if build_status is not None:
            return build_status
        started_activity = await workflow.execute_activity(
            instruqt_activities.start_instruqt_course_image_build,
            instruqt_activities.InstruqtCourseImageBuildStartRequest(build_plan),
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=COMMON_RETRY_POLICY,
        )
        started_build = started_activity.plan
        build_status = instruqt_activities.completed_instruqt_image_status(started_build)
        if build_status is not None:
            return build_status
        wait_activity = await workflow.execute_activity(
            instruqt_activities.wait_for_instruqt_course_image_build,
            instruqt_activities.InstruqtCourseImageBuildWaitRequest(started_build),
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=BUILD_WAIT_RETRY_POLICY,
        )
        return wait_activity.status

    async def _run_finalize(self, course_name: str) -> None:
        instruqt_course = (
            self._finalize_spec
            or self._pending_spec
            or {"metadata": {"name": course_name}}
        )
        finalize_plan = await self._load_instruqt_finalization_plan(instruqt_course)
        source_deletes = await self._delete_instruqt_source_resources(finalize_plan)
        track_deletes = await self._delete_instruqt_tracks(instruqt_course)
        ecr_action = await self._delete_instruqt_ecr(finalize_plan)
        self._status = instruqt_activities.instruqt_course_finalization_status(
            finalize_plan,
            source_action=source_deletes.source_action,
            source_auth_action=source_deletes.source_auth_action,
            content_source_action=source_deletes.content_source_action,
            content_source_auth_action=source_deletes.content_source_auth_action,
            production_track_action=track_deletes.production_track_action,
            testing_track_action=track_deletes.testing_track_action,
            ecr_action=ecr_action,
        )
        await self._record_status(instruqt_course, self._status)
        await self._remove_instruqt_finalizer(finalize_plan)
        self._shutdown = True

    async def _load_instruqt_finalization_plan(
        self,
        instruqt_course: dict[str, Any],
    ) -> dict[str, Any]:
        finalize_plan_activity = await workflow.execute_activity(
            instruqt_activities.load_instruqt_course_finalization_plan,
            instruqt_activities.InstruqtCourseFinalizationRequest(instruqt_course),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return finalize_plan_activity.plan

    async def _delete_instruqt_source_resources(
        self,
        finalize_plan: dict[str, Any],
    ) -> InstruqtCourseSourceDeletes:
        source_action_activity = await workflow.execute_activity(
            instruqt_activities.delete_instruqt_course_source_repository,
            instruqt_activities.InstruqtKubernetesResourceDeleteRequest(
                namespace=finalize_plan["sourceWatchNamespace"],
                name=finalize_plan["gitRepository"],
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        source_auth_action_activity = await workflow.execute_activity(
            instruqt_activities.delete_instruqt_course_source_auth_secret,
            instruqt_activities.InstruqtKubernetesResourceDeleteRequest(
                namespace=finalize_plan["sourceWatchNamespace"],
                name=finalize_plan.get("sourceAuthSecret"),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        content_source_action_activity = await workflow.execute_activity(
            instruqt_activities.delete_instruqt_course_content_repository,
            instruqt_activities.InstruqtKubernetesResourceDeleteRequest(
                namespace=finalize_plan["sourceWatchNamespace"],
                name=finalize_plan.get("contentGitRepository"),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        content_source_auth_action_activity = await workflow.execute_activity(
            instruqt_activities.delete_instruqt_course_content_auth_secret,
            instruqt_activities.InstruqtKubernetesResourceDeleteRequest(
                namespace=finalize_plan["sourceWatchNamespace"],
                name=finalize_plan.get("contentSourceAuthSecret"),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return InstruqtCourseSourceDeletes(
            source_action=source_action_activity.action,
            source_auth_action=source_auth_action_activity.action,
            content_source_action=content_source_action_activity.action,
            content_source_auth_action=content_source_auth_action_activity.action,
        )

    async def _delete_instruqt_tracks(
        self,
        instruqt_course: dict[str, Any],
    ) -> InstruqtCourseTrackDeletes:
        production_track_activity = await workflow.execute_activity(
            instruqt_activities.delete_instruqt_course_track,
            instruqt_activities.InstruqtTrackDeleteRequest(
                instruqt_course=instruqt_course,
                channel="production",
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        testing_track_activity = await workflow.execute_activity(
            instruqt_activities.delete_instruqt_course_track,
            instruqt_activities.InstruqtTrackDeleteRequest(
                instruqt_course=instruqt_course,
                channel="testing",
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return InstruqtCourseTrackDeletes(
            production_track_action=production_track_activity.status,
            testing_track_action=testing_track_activity.status,
        )

    async def _delete_instruqt_ecr(
        self,
        finalize_plan: dict[str, Any],
    ) -> dict[str, Any]:
        ecr_activity = await workflow.execute_activity(
            instruqt_activities.delete_instruqt_course_ecr_repositories,
            instruqt_activities.InstruqtCourseFinalizationPlan(finalize_plan),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return ecr_activity.status

    async def _remove_instruqt_finalizer(
        self,
        finalize_plan: dict[str, Any],
    ) -> None:
        await workflow.execute_activity(
            instruqt_activities.remove_instruqt_course_finalizer_resource,
            instruqt_activities.InstruqtFinalizerRemovalRequest(
                course=finalize_plan["course"]
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _record_status(
        self,
        instruqt_course: dict[str, Any],
        status: dict[str, Any],
    ) -> None:
        await workflow.execute_activity(
            instruqt_activities.record_instruqt_course_status,
            instruqt_activities.InstruqtCourseStatusRecordRequest(
                instruqt_course=instruqt_course,
                status=status_with_force_reconcile(status, instruqt_course),
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=STATUS_RETRY_POLICY,
        )

    def _is_duplicate_reconcile_signal(self, identity: dict[str, str]) -> bool:
        return any(
            identity == comparable
            for comparable in (
                self._pending_reconcile_identity,
                self._current_reconcile_identity,
                self._completed_reconcile_identity,
            )
            if comparable is not None
        )

    def _complete_current_reconcile(self) -> None:
        if remember_completed_reconcile_identity(self._status):
            self._completed_reconcile_identity = self._current_reconcile_identity
            if self._current_force_reconcile_token is not None:
                self._completed_force_reconcile_token = (
                    self._current_force_reconcile_token
                )
        elif self._completed_reconcile_identity == self._current_reconcile_identity:
            self._completed_reconcile_identity = None
            if self._completed_force_reconcile_token == self._current_force_reconcile_token:
                self._completed_force_reconcile_token = None
        self._current_reconcile_identity = None
        self._current_force_reconcile_token = None

    def _pending_force_reconcile_token(self) -> str | None:
        if not isinstance(self._pending_force_reconcile, dict):
            return None
        token = self._pending_force_reconcile.get("token")
        return token if isinstance(token, str) and token else None

    def _continue_as_new_if_needed(self, course_name: str) -> None:
        if (
            self._reconcile_sequence >= MAX_RECONCILES_PER_WORKFLOW_RUN
            or self._continue_as_new_for_worker_version
        ):
            self._continue_as_new_for_worker_version = False
            workflow.continue_as_new(
                args=[
                    messages.InstruqtCourseRunRequest(
                        course_name=course_name,
                        carried_status=self._status,
                        carried_pending_spec=self._pending_spec,
                        carried_last_build_status=self._last_build_status,
                        carried_last_testing_build_status=(
                            self._last_testing_build_status
                        ),
                        carried_completed_reconcile_identity=(
                            self._completed_reconcile_identity
                        ),
                        carried_pending_force_reconcile=(
                            self._pending_force_reconcile
                        ),
                        carried_completed_force_reconcile_token=(
                            self._completed_force_reconcile_token
                        ),
                    )
                ],
                initial_versioning_behavior=(
                    workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
                ),
            )


def remember_completed_reconcile_identity(status: dict[str, Any] | None) -> bool:
    if not isinstance(status, dict):
        return False
    phase = status.get("phase")
    return isinstance(phase, str) and phase not in INCOMPLETE_RECONCILE_PHASES


def rememberable_instruqt_build_status(status: Any) -> dict[str, Any] | None:
    if not isinstance(status, dict):
        return None
    if status.get("phase") not in {"Pushed", "Resolved"}:
        return None
    if status.get("lastError"):
        return None
    return status


def status_with_force_reconcile(
    status: dict[str, Any],
    resource: dict[str, Any],
) -> dict[str, Any]:
    force_status = resources.force_reconcile_status(resource)
    if force_status is None:
        return status
    return {
        **status,
        resources.BASE_RECONCILE_KEY_STATUS_FIELD: resources.instruqt_course_reconcile_key(
            resources.resource_without_force_reconcile(resource)
        ),
        "forceReconcile": force_status,
    }


def production_deployment_complete(status: dict[str, Any]) -> bool:
    deployment = status.get("deployment")
    return isinstance(deployment, dict) and deployment.get("phase") == "Deployed"


def status_with_ecr_status(
    status: dict[str, Any],
    ecr_status: dict[str, Any],
) -> dict[str, Any]:
    external_resources = status.get("externalResources")
    external_resources = external_resources if isinstance(external_resources, dict) else {}
    return {
        **status,
        "externalResources": {
            **external_resources,
            "ecr": ecr_status,
        },
    }


def status_with_build_status(
    status: dict[str, Any],
    build_status: dict[str, Any],
    *,
    testing: bool,
) -> dict[str, Any]:
    if testing:
        nested = status.get("testing")
        nested = nested if isinstance(nested, dict) else {}
        updated = {
            **status,
            "testing": {
                **nested,
                "buildStatus": build_status,
            },
        }
    else:
        updated = {**status, "buildStatus": build_status}

    if build_status.get("phase") == "Error":
        return status_with_phase(updated, "BuildFailed", build_status.get("message"))
    return updated


def status_with_deployment_status(
    status: dict[str, Any],
    deployment_status: dict[str, Any],
    *,
    testing: bool,
) -> dict[str, Any]:
    if testing:
        nested = status.get("testing")
        nested = nested if isinstance(nested, dict) else {}
        updated = {
            **status,
            "testing": {
                **nested,
                "deployment": deployment_status,
            },
        }
    else:
        updated = {**status, "deployment": deployment_status}

    phase = deployment_status.get("phase")
    if isinstance(phase, str) and phase in INCOMPLETE_RECONCILE_PHASES:
        return status_with_phase(updated, phase, deployment_status.get("message"))
    return updated


def finalized_instruqt_status(status: dict[str, Any]) -> dict[str, Any]:
    phase = status.get("phase")
    if isinstance(phase, str) and phase in INCOMPLETE_RECONCILE_PHASES:
        return reconciled_condition(status, ready=False)

    deployment = status.get("deployment")
    if not isinstance(deployment, dict) or deployment.get("phase") != "Deployed":
        return reconciled_condition(status, ready=False)

    testing = status.get("testing")
    testing = testing if isinstance(testing, dict) else {}
    testing_deployment = testing.get("deployment")
    testing_deployment = (
        testing_deployment if isinstance(testing_deployment, dict) else {}
    )
    if testing.get("resolved") is True and testing_deployment.get("phase") != "Deployed":
        return reconciled_condition(status, ready=False)
    if testing.get("phase") == "BranchMissing" and testing_deployment.get("phase") not in {
        "Deleted",
        "NotFound",
    }:
        return reconciled_condition(status, ready=False)

    message = "Instruqt production track is deployed."
    if testing_deployment.get("phase") == "Deployed":
        message = "Instruqt production and testing tracks are deployed."
    elif testing_deployment.get("phase") in {"Deleted", "NotFound"}:
        message = "Instruqt production track is deployed and testing track is absent."
    return reconciled_condition(
        {
            **status,
            "phase": "Ready",
            "message": message,
        },
        ready=True,
    )


def status_with_phase(
    status: dict[str, Any],
    phase: str,
    message: Any,
) -> dict[str, Any]:
    return {
        **status,
        "phase": phase,
        "message": message if isinstance(message, str) and message else phase,
    }


def reconciled_condition(status: dict[str, Any], *, ready: bool) -> dict[str, Any]:
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return status
    phase = str(status.get("phase") or "Unknown")
    message = str(status.get("message") or "")
    updated_conditions = []
    for condition in conditions:
        if not isinstance(condition, dict) or condition.get("type") != "Reconciled":
            updated_conditions.append(condition)
            continue
        updated_conditions.append(
            {
                **condition,
                "status": "True" if ready else "False",
                "reason": phase,
                "message": message,
            }
        )
    return {**status, "conditions": updated_conditions}
