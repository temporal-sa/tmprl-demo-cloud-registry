from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.instruqt import (
        completed_instruqt_image_status,
        delete_instruqt_course_content_auth_secret,
        delete_instruqt_course_content_repository,
        delete_instruqt_course_ecr_repositories,
        delete_instruqt_course_source_auth_secret,
        delete_instruqt_course_source_repository,
        delete_instruqt_course_track,
        deploy_instruqt_course_track,
        instruqt_course_finalization_status,
        plan_instruqt_course_finalization,
        plan_instruqt_course_images,
        reconcile_instruqt_course,
        reconcile_instruqt_course_ecr_repositories,
        record_instruqt_course_status,
        remove_instruqt_course_finalizer_resource,
        start_instruqt_course_image_build,
        wait_for_instruqt_course_image_build,
    )
    from demo_registry_operator.activities.force_reconcile import (
        load_current_instruqt_course_for_reconcile,
    )
    from demo_registry_operator.errors import NON_RETRYABLE_ERROR_TYPES
    from demo_registry_operator.resources import (
        CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
        CONTENT_SOURCE_COMMIT_ANNOTATION,
        SOURCE_ARTIFACT_DIGEST_ANNOTATION,
        SOURCE_COMMIT_ANNOTATION,
        TESTING_BRANCH_STATE_ANNOTATION,
        TESTING_CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
        TESTING_CONTENT_SOURCE_COMMIT_ANNOTATION,
        TESTING_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
        TESTING_SOURCE_COMMIT_ANNOTATION,
        BASE_RECONCILE_KEY_STATUS_FIELD,
        force_reconcile_status,
        force_reconcile_token,
        instruqt_course_reconcile_key,
        metadata_annotations,
        resource_without_force_reconcile,
    )


MAX_RECONCILES_PER_WORKFLOW_RUN = 50

COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
STATUS_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=12,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
BUILD_WAIT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=15),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
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


def reconcile_identity(instruqt_course: dict[str, Any]) -> dict[str, str]:
    annotations = metadata_annotations(instruqt_course)
    identity = {
        "reconcileKey": instruqt_course_reconcile_key(instruqt_course),
    }
    source_commit_sha = annotations.get(SOURCE_COMMIT_ANNOTATION)
    if source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = annotations.get(SOURCE_ARTIFACT_DIGEST_ANNOTATION)
    if source_artifact_digest:
        identity["sourceArtifactDigest"] = source_artifact_digest
    content_source_commit_sha = annotations.get(CONTENT_SOURCE_COMMIT_ANNOTATION)
    if content_source_commit_sha:
        identity["contentSourceCommitSha"] = content_source_commit_sha
    content_source_artifact_digest = annotations.get(
        CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION
    )
    if content_source_artifact_digest:
        identity["contentSourceArtifactDigest"] = content_source_artifact_digest
    testing_branch_state = annotations.get(TESTING_BRANCH_STATE_ANNOTATION)
    if testing_branch_state:
        identity["testingBranchState"] = testing_branch_state
    testing_source_commit_sha = annotations.get(TESTING_SOURCE_COMMIT_ANNOTATION)
    if testing_source_commit_sha:
        identity["testingSourceCommitSha"] = testing_source_commit_sha
    testing_source_artifact_digest = annotations.get(
        TESTING_SOURCE_ARTIFACT_DIGEST_ANNOTATION
    )
    if testing_source_artifact_digest:
        identity["testingSourceArtifactDigest"] = testing_source_artifact_digest
    testing_content_source_commit_sha = annotations.get(
        TESTING_CONTENT_SOURCE_COMMIT_ANNOTATION
    )
    if testing_content_source_commit_sha:
        identity["testingContentSourceCommitSha"] = testing_content_source_commit_sha
    testing_content_source_artifact_digest = annotations.get(
        TESTING_CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION
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
        course_name: str,
        carried_status: dict[str, Any] | None = None,
        carried_pending_spec: dict[str, Any] | None = None,
        carried_last_build_status: dict[str, Any] | None = None,
        carried_last_testing_build_status: dict[str, Any] | None = None,
        carried_completed_reconcile_identity: dict[str, str] | None = None,
        carried_pending_force_reconcile: dict[str, str] | None = None,
        carried_completed_force_reconcile_token: str | None = None,
    ) -> None:
        self._course_name = course_name
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
        course_name: str,
        carried_status: dict[str, Any] | None = None,
        carried_pending_spec: dict[str, Any] | None = None,
        carried_last_build_status: dict[str, Any] | None = None,
        carried_last_testing_build_status: dict[str, Any] | None = None,
        carried_completed_reconcile_identity: dict[str, str] | None = None,
        carried_pending_force_reconcile: dict[str, str] | None = None,
        carried_completed_force_reconcile_token: str | None = None,
    ) -> None:
        self._course_name = course_name
        if carried_pending_spec is not None:
            self._pending_spec = carried_pending_spec
            self._pending_reconcile_identity = reconcile_identity(carried_pending_spec)
        if carried_last_build_status is not None:
            self._last_build_status = carried_last_build_status
        if carried_last_testing_build_status is not None:
            self._last_testing_build_status = carried_last_testing_build_status
        if (
            carried_completed_reconcile_identity is not None
            and remember_completed_reconcile_identity(carried_status)
        ):
            self._completed_reconcile_identity = carried_completed_reconcile_identity
        if carried_pending_force_reconcile is not None:
            self._pending_force_reconcile = carried_pending_force_reconcile
        if carried_completed_force_reconcile_token is not None:
            self._completed_force_reconcile_token = (
                carried_completed_force_reconcile_token
            )
        if carried_status is not None:
            self._status = carried_status
            if self._last_build_status is None:
                self._last_build_status = rememberable_instruqt_build_status(
                    carried_status.get("buildStatus")
                )
            if self._last_testing_build_status is None:
                testing = carried_status.get("testing")
                testing = testing if isinstance(testing, dict) else {}
                self._last_testing_build_status = rememberable_instruqt_build_status(
                    testing.get("buildStatus")
                )
            if (
                self._completed_reconcile_identity is None
                and remember_completed_reconcile_identity(carried_status)
            ):
                self._completed_reconcile_identity = status_reconcile_identity(
                    carried_status
                )
        else:
            self._status = {
                "course": course_name,
                "phase": "Waiting",
                "message": "Waiting for InstruqtCourse reconcile signal.",
            }

        while not self._shutdown:
            await workflow.wait_condition(
                lambda: (
                    self._pending_spec is not None
                    or self._pending_force_reconcile is not None
                    or self._finalize
                    or self._shutdown
                    or self._continue_as_new_for_worker_version
                )
            )
            if self._shutdown:
                break

            if self._finalize:
                await self._run_finalize(course_name)
                return

            if self._continue_as_new_for_worker_version:
                self._continue_as_new_if_needed(course_name)
                continue

            force_reconcile = self._pending_force_reconcile
            if force_reconcile is not None:
                self._pending_force_reconcile = None
                self._pending_spec = None
                self._pending_reconcile_identity = None
                instruqt_course = await workflow.execute_activity(
                    load_current_instruqt_course_for_reconcile,
                    {
                        "name": course_name,
                        **force_reconcile,
                    },
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                self._current_force_reconcile_token = force_reconcile["token"]
                self._current_reconcile_identity = reconcile_identity(instruqt_course)
            else:
                instruqt_course = self._pending_spec
                self._pending_spec = None
                self._current_reconcile_identity = (
                    self._pending_reconcile_identity
                    if self._pending_reconcile_identity is not None
                    else reconcile_identity(instruqt_course)
                )
                self._pending_reconcile_identity = None
            self._reconcile_sequence += 1
            self._status = {
                "course": course_name,
                "phase": "Reconciling",
            }

            base_status = await workflow.execute_activity(
                reconcile_instruqt_course,
                instruqt_course,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            self._status = base_status
            if base_status.get("phase") != "Error":
                ecr_status = await workflow.execute_activity(
                    reconcile_instruqt_course_ecr_repositories,
                    instruqt_course,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                self._status = status_with_ecr_status(self._status, ecr_status)

                build_status = await self._resolve_images(
                    instruqt_course,
                    ecr_status,
                    channel="production",
                    previous_build_status=(
                        None
                        if force_reconcile_token(instruqt_course)
                        else self._last_build_status
                    ),
                )
                remembered_build_status = rememberable_instruqt_build_status(
                    build_status
                )
                if remembered_build_status is not None:
                    self._last_build_status = remembered_build_status
                self._status = status_with_build_status(
                    self._status,
                    build_status,
                    testing=False,
                )

                if build_status.get("phase") in SUCCESSFUL_BUILD_PHASES:
                    deployment_status = await workflow.execute_activity(
                        deploy_instruqt_course_track,
                        {
                            "instruqtCourse": instruqt_course,
                            "images": build_status.get("images", {}),
                            "channel": "production",
                        },
                        start_to_close_timeout=timedelta(hours=3),
                        heartbeat_timeout=timedelta(minutes=5),
                        retry_policy=COMMON_RETRY_POLICY,
                    )
                    self._status = status_with_deployment_status(
                        self._status,
                        deployment_status,
                        testing=False,
                    )

                if production_deployment_complete(self._status):
                    await self._reconcile_testing_channel(
                        instruqt_course,
                        ecr_status,
                        force_reconcile=bool(force_reconcile_token(instruqt_course)),
                    )

                self._status = finalized_instruqt_status(self._status)

            await self._record_status(instruqt_course, self._status)
            self._complete_current_reconcile()
            self._continue_as_new_if_needed(course_name)

        self._status = {
            "course": course_name,
            "phase": "Stopped",
            "message": "Workflow received shutdown signal.",
        }

    @workflow.signal
    def reconcile(self, instruqt_course: dict[str, Any]) -> None:
        identity = reconcile_identity(instruqt_course)
        if self._is_duplicate_reconcile_signal(identity):
            return
        self._pending_spec = instruqt_course
        self._pending_reconcile_identity = identity

    @workflow.signal
    def force_reconcile(self, token: str, reason: str | None = None) -> None:
        request = force_reconcile_request(token, reason)
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
    def check_worker_version(self) -> None:
        if workflow.info().is_target_worker_deployment_version_changed():
            self._continue_as_new_for_worker_version = True

    @workflow.signal
    def finalize(self, instruqt_course: dict[str, Any]) -> None:
        self._finalize_spec = instruqt_course
        self._finalize = True

    @workflow.signal
    def shutdown(self) -> None:
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
            testing_delete_status = await workflow.execute_activity(
                delete_instruqt_course_track,
                {
                    "instruqtCourse": instruqt_course,
                    "channel": "testing",
                },
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
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

        testing_deployment_status = await workflow.execute_activity(
            deploy_instruqt_course_track,
            {
                "instruqtCourse": instruqt_course,
                "images": testing_build_status.get("images", {}),
                "channel": "testing",
            },
            start_to_close_timeout=timedelta(hours=3),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        self._status = status_with_deployment_status(
            self._status,
            testing_deployment_status,
            testing=True,
        )

    async def _resolve_images(
        self,
        instruqt_course: dict[str, Any],
        ecr_status: dict[str, Any],
        *,
        channel: str,
        previous_build_status: dict[str, Any] | None,
    ) -> dict[str, Any]:
        build_plan = await workflow.execute_activity(
            plan_instruqt_course_images,
            {
                "instruqtCourse": instruqt_course,
                "externalResources": {"ecr": ecr_status},
                "previousBuildStatus": previous_build_status,
                "channel": channel,
            },
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        build_status = completed_instruqt_image_status(build_plan)
        if build_status is not None:
            return build_status
        started_build = await workflow.execute_activity(
            start_instruqt_course_image_build,
            build_plan,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=COMMON_RETRY_POLICY,
        )
        build_status = completed_instruqt_image_status(started_build)
        if build_status is not None:
            return build_status
        return await workflow.execute_activity(
            wait_for_instruqt_course_image_build,
            started_build,
            start_to_close_timeout=timedelta(hours=1),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=BUILD_WAIT_RETRY_POLICY,
        )

    async def _run_finalize(self, course_name: str) -> None:
        instruqt_course = (
            self._finalize_spec
            or self._pending_spec
            or {"metadata": {"name": course_name}}
        )
        finalize_plan = await workflow.execute_activity(
            plan_instruqt_course_finalization,
            {"instruqtCourse": instruqt_course},
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        source_action = await workflow.execute_activity(
            delete_instruqt_course_source_repository,
            finalize_plan,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        source_auth_action = await workflow.execute_activity(
            delete_instruqt_course_source_auth_secret,
            finalize_plan,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        content_source_action = await workflow.execute_activity(
            delete_instruqt_course_content_repository,
            finalize_plan,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        content_source_auth_action = await workflow.execute_activity(
            delete_instruqt_course_content_auth_secret,
            finalize_plan,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        production_track_action = await workflow.execute_activity(
            delete_instruqt_course_track,
            {
                "instruqtCourse": instruqt_course,
                "channel": "production",
            },
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        testing_track_action = await workflow.execute_activity(
            delete_instruqt_course_track,
            {
                "instruqtCourse": instruqt_course,
                "channel": "testing",
            },
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        ecr_action = await workflow.execute_activity(
            delete_instruqt_course_ecr_repositories,
            finalize_plan,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        self._status = instruqt_course_finalization_status(
            finalize_plan,
            source_action=source_action,
            source_auth_action=source_auth_action,
            content_source_action=content_source_action,
            content_source_auth_action=content_source_auth_action,
            production_track_action=production_track_action,
            testing_track_action=testing_track_action,
            ecr_action=ecr_action,
        )
        await self._record_status(instruqt_course, self._status)
        await workflow.execute_activity(
            remove_instruqt_course_finalizer_resource,
            finalize_plan,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        self._shutdown = True

    async def _record_status(
        self,
        instruqt_course: dict[str, Any],
        status: dict[str, Any],
    ) -> None:
        await workflow.execute_activity(
            record_instruqt_course_status,
            {
                "instruqtCourse": instruqt_course,
                "status": status_with_force_reconcile(status, instruqt_course),
            },
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
                    course_name,
                    self._status,
                    self._pending_spec,
                    self._last_build_status,
                    self._last_testing_build_status,
                    self._completed_reconcile_identity,
                    self._pending_force_reconcile,
                    self._completed_force_reconcile_token,
                ]
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
    force_status = force_reconcile_status(resource)
    if force_status is None:
        return status
    return {
        **status,
        BASE_RECONCILE_KEY_STATUS_FIELD: instruqt_course_reconcile_key(
            resource_without_force_reconcile(resource)
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
