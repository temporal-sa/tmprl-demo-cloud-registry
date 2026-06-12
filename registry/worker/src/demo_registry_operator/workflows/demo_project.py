from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.builds import (
        completed_build_status,
        plan_project_images,
        start_project_image_build,
        wait_for_project_image_build,
    )
    from demo_registry_operator.activities.canary import (
        apply_registry_operator_canary_project,
        delete_registry_operator_canary_project,
        plan_registry_operator_canary,
        wait_for_registry_operator_canary_promotion,
        wait_registry_operator_canary_cleanup,
    )
    from demo_registry_operator.activities.external_resources import (
        completed_external_resources_status,
        delete_project_ecr_repositories_resource,
        delete_temporal_cloud_api_key_resource,
        delete_temporal_cloud_namespace_resource,
        delete_temporal_cloud_runtime_secret_resource,
        delete_temporal_cloud_service_account_resource,
        disable_temporal_cloud_namespace_delete_protection_resource,
        external_resource_finalization_status,
        external_resources_status,
        plan_external_resources,
        reconcile_ecr_repositories,
        reconcile_temporal_cloud_resource,
        temporal_cloud_finalization_status,
        verify_secrets_manager_resources,
    )
    from demo_registry_operator.activities.reconcile import (
        completed_activity_status,
        delete_project_namespace_resource,
        delete_project_source_auth_secret,
        delete_project_source_repository,
        plan_project_finalization,
        plan_rollout,
        project_finalization_status,
        promote_candidate,
        reconcile_project,
        record_catalog_project_status,
        record_demo_project_status,
        remove_project_finalizer,
        run_smoke_check,
        wait_for_candidate_readiness,
        wait_for_worker_deployment_rollouts,
    )
    from demo_registry_operator.errors import NON_RETRYABLE_ERROR_TYPES
    from demo_registry_operator.resources import (
        SOURCE_ARTIFACT_DIGEST_ANNOTATION,
        SOURCE_COMMIT_ANNOTATION,
        demo_project_reconcile_key,
        metadata_annotations,
        worker_deployment_rollout_wait_seconds,
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
CHECK_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
FAILED_RECONCILE_PHASES = {
    "CandidateNotReady",
    "Error",
    "SmokeCheckFailed",
    "WorkerRolloutFailed",
}


def reconcile_identity(demo_project: dict[str, Any]) -> dict[str, str]:
    annotations = metadata_annotations(demo_project)
    identity = {
        "reconcileKey": demo_project_reconcile_key(demo_project),
    }
    source_commit_sha = annotations.get(SOURCE_COMMIT_ANNOTATION)
    if source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = annotations.get(SOURCE_ARTIFACT_DIGEST_ANNOTATION)
    if source_artifact_digest:
        identity["sourceArtifactDigest"] = source_artifact_digest
    return identity


def status_reconcile_identity(status: dict[str, Any]) -> dict[str, str] | None:
    reconcile_key = status.get("reconcileKey")
    if not isinstance(reconcile_key, str) or not reconcile_key:
        return None
    identity = {"reconcileKey": reconcile_key}
    build_status = status.get("buildStatus")
    build_status = build_status if isinstance(build_status, dict) else {}
    source_commit_sha = status.get("sourceCommitSha") or build_status.get(
        "sourceCommitSha"
    )
    if isinstance(source_commit_sha, str) and source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = status.get("sourceArtifactDigest") or build_status.get(
        "sourceArtifactDigest"
    )
    if isinstance(source_artifact_digest, str) and source_artifact_digest:
        identity["sourceArtifactDigest"] = source_artifact_digest
    return identity


@workflow.defn
class DemoProjectWorkflow:
    """Durable entity workflow for one DemoProject."""

    @workflow.init
    def __init__(
        self,
        project_name: str,
        carried_status: dict[str, Any] | None = None,
        carried_pending_spec: dict[str, Any] | None = None,
        carried_last_build_status: dict[str, Any] | None = None,
        carried_completed_reconcile_identity: dict[str, str] | None = None,
    ) -> None:
        self._project_name = project_name
        self._pending_spec: dict[str, Any] | None = None
        self._pending_reconcile_identity: dict[str, str] | None = None
        self._current_reconcile_identity: dict[str, str] | None = None
        self._completed_reconcile_identity: dict[str, str] | None = None
        self._reconcile_sequence = 0
        self._shutdown = False
        self._finalize = False
        self._continue_as_new_for_worker_version = False
        self._finalize_spec: dict[str, Any] | None = None
        self._last_build_status: dict[str, Any] | None = None
        self._status: dict[str, Any] = {
            "phase": "Starting",
            "message": "Workflow has not received project state.",
        }

    @workflow.run
    async def run(
        self,
        project_name: str,
        carried_status: dict[str, Any] | None = None,
        carried_pending_spec: dict[str, Any] | None = None,
        carried_last_build_status: dict[str, Any] | None = None,
        carried_completed_reconcile_identity: dict[str, str] | None = None,
    ) -> None:
        self._project_name = project_name
        # Do NOT clobber a pending spec set by a start signal. With
        # SignalWithStart the reconcile signal is delivered on the first workflow
        # task and its handler sets self._pending_spec before/around run()'s body;
        # unconditionally assigning carried_pending_spec (None on a fresh start)
        # would wipe it, leaving wait_condition permanently false. Only adopt a
        # spec actually carried over from continue-as-new.
        if carried_pending_spec is not None:
            self._pending_spec = carried_pending_spec
            self._pending_reconcile_identity = reconcile_identity(carried_pending_spec)
        if carried_last_build_status is not None:
            self._last_build_status = carried_last_build_status
        if (
            carried_completed_reconcile_identity is not None
            and remember_completed_reconcile_identity(carried_status)
        ):
            self._completed_reconcile_identity = carried_completed_reconcile_identity
        if carried_status is not None:
            self._status = carried_status
            if self._last_build_status is None:
                self._last_build_status = rememberable_build_status(
                    carried_status.get("buildStatus")
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
                "project": project_name,
                "phase": "Waiting",
                "message": "Waiting for DemoProject reconcile signal.",
            }

        while not self._shutdown:
            await workflow.wait_condition(
                lambda: (
                    self._pending_spec is not None
                    or self._finalize
                    or self._shutdown
                    or self._continue_as_new_for_worker_version
                )
            )
            if self._shutdown:
                break

            if self._finalize:
                await self._run_finalize(project_name)
                return

            if self._continue_as_new_for_worker_version:
                self._continue_as_new_if_needed(project_name)
                continue

            demo_project = self._pending_spec
            self._pending_spec = None
            self._current_reconcile_identity = (
                self._pending_reconcile_identity
                if self._pending_reconcile_identity is not None
                else reconcile_identity(demo_project)
            )
            self._pending_reconcile_identity = None
            self._reconcile_sequence += 1
            self._status = {
                "project": project_name,
                "phase": "Reconciling",
            }
            external_plan = await workflow.execute_activity(
                plan_external_resources,
                demo_project,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=COMMON_RETRY_POLICY,
            )
            external_resources = completed_external_resources_status(external_plan)
            if external_resources is None:
                ecr_status = await workflow.execute_activity(
                    reconcile_ecr_repositories,
                    external_plan,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                temporal_status = await workflow.execute_activity(
                    reconcile_temporal_cloud_resource,
                    external_plan,
                    start_to_close_timeout=timedelta(minutes=15),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                secrets_status = await workflow.execute_activity(
                    verify_secrets_manager_resources,
                    external_plan,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                external_resources = external_resources_status(
                    external_plan,
                    ecr_status=ecr_status,
                    temporal_status=temporal_status,
                    secrets_status=secrets_status,
                )
            build_plan = await workflow.execute_activity(
                plan_project_images,
                {
                    "demoProject": demo_project,
                    "externalResources": external_resources,
                    "previousBuildStatus": self._last_build_status,
                },
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            build_status = completed_build_status(build_plan)
            if build_status is None:
                started_build = await workflow.execute_activity(
                    start_project_image_build,
                    build_plan,
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                build_status = completed_build_status(started_build)
                if build_status is None:
                    build_status = await workflow.execute_activity(
                        wait_for_project_image_build,
                        started_build,
                        start_to_close_timeout=timedelta(hours=1),
                        heartbeat_timeout=timedelta(minutes=2),
                        retry_policy=BUILD_WAIT_RETRY_POLICY,
                    )
            remembered_build_status = rememberable_build_status(build_status)
            if remembered_build_status is not None:
                self._last_build_status = remembered_build_status
            reconcile_status = await workflow.execute_activity(
                reconcile_project,
                {
                    "demoProject": demo_project,
                    "externalResources": external_resources,
                    "buildStatus": build_status,
                },
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            self._status = reconcile_status

            smoke_plan = {
                "project": project_name,
                "demoProject": demo_project,
                "host": reconcile_status.get("desired", {}).get("host"),
                "candidate": reconcile_status.get("candidateRolloutStatus", {}),
                "smokeCheck": demo_project.get("spec", {}).get("smokeCheck"),
                "workerDeployments": reconcile_status.get(
                    "candidateRolloutStatus",
                    {},
                ).get("workerDeployments", {}),
                "workerDeploymentTimeoutSeconds": (
                    worker_deployment_rollout_wait_seconds(
                        demo_project,
                        base_timeout_seconds=300,
                    )
                ),
            }

            if reconcile_status.get("phase") == "WorkerRolloutPending":
                worker_rollout_result = await workflow.execute_activity(
                    wait_for_worker_deployment_rollouts,
                    smoke_plan,
                    start_to_close_timeout=timedelta(
                        seconds=int(
                            smoke_plan.get("workerDeploymentTimeoutSeconds", 300)
                        )
                        + 60
                    ),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                if not worker_rollout_result.get("ok"):
                    candidate_status = reconcile_status.get(
                        "candidateRolloutStatus",
                        {},
                    )
                    self._status = {
                        **reconcile_status,
                        "phase": "WorkerRolloutFailed",
                        "message": "WorkerDeployment rollout did not become ready; active route was not promoted.",
                        "workerDeploymentStatus": worker_rollout_result,
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "WorkerRolloutFailed",
                            "message": "WorkerDeployment rollout did not become ready; active route was not promoted.",
                        },
                    }
                    await self._record_status(demo_project, self._status)
                    self._complete_current_reconcile()
                    self._continue_as_new_if_needed(project_name)
                    continue

                reconcile_status = {
                    **reconcile_status,
                    "phase": (
                        "SmokeCheckPending"
                        if demo_project.get("spec", {}).get("smokeCheck")
                        else "WorkerRolloutReady"
                    ),
                    "message": "WorkerDeployment rollout is ready.",
                    "workerDeploymentStatus": worker_rollout_result,
                }
                self._status = reconcile_status
                await self._record_status(demo_project, self._status)

                if not demo_project.get("spec", {}).get("smokeCheck"):
                    promotion = await workflow.execute_activity(
                        promote_candidate,
                        smoke_plan,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=COMMON_RETRY_POLICY,
                    )
                    candidate_status = promotion.get(
                        "candidate",
                        reconcile_status.get("candidateRolloutStatus", {}),
                    )
                    promoted_status = {
                        **reconcile_status,
                        "phase": promotion.get("phase", "Promoted"),
                        "message": promotion.get("message"),
                        "activeImages": candidate_status.get("images", {}),
                        "activeRouteServices": candidate_status.get("services", {}),
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "Promoted",
                            "completedAt": worker_rollout_result.get("checkedAt"),
                            "message": "WorkerDeployment rollout passed and active route resources were promoted.",
                        },
                        "appliedResources": [
                            *reconcile_status.get("appliedResources", []),
                            *promotion.get("appliedResources", []),
                        ],
                        "pruneStatus": promotion.get("pruneStatus"),
                        "prunedResources": promotion.get("prunedResources", []),
                    }
                    active_version = candidate_status.get("imageVersion")
                    if isinstance(active_version, str):
                        promoted_status["activeImageVersion"] = active_version
                    self._status = promoted_status
                    await self._record_status(demo_project, self._status)
                    self._complete_current_reconcile()
                    self._continue_as_new_if_needed(project_name)
                    continue

            if reconcile_status.get("phase") == "SmokeCheckPending":
                readiness_result = await workflow.execute_activity(
                    wait_for_candidate_readiness,
                    smoke_plan,
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                if not readiness_result.get("ok"):
                    candidate_status = reconcile_status.get(
                        "candidateRolloutStatus",
                        {},
                    )
                    self._status = {
                        **reconcile_status,
                        "phase": "CandidateNotReady",
                        "message": "Candidate did not become ready; active route was not promoted.",
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "CandidateNotReady",
                            "message": "Candidate did not become ready; active route was not promoted.",
                        },
                        "readinessStatus": readiness_result,
                    }
                    await self._record_status(demo_project, self._status)
                    self._complete_current_reconcile()
                    self._continue_as_new_if_needed(project_name)
                    continue

                self._status = {
                    **reconcile_status,
                    "readinessStatus": readiness_result,
                }
                await self._record_status(demo_project, self._status)

                smoke_result = await workflow.execute_activity(
                    run_smoke_check,
                    smoke_plan,
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                if not smoke_result.get("ok"):
                    candidate_status = reconcile_status.get(
                        "candidateRolloutStatus",
                        {},
                    )
                    self._status = {
                        **reconcile_status,
                        "phase": "SmokeCheckFailed",
                        "message": "Candidate smoke check failed; active route was not promoted.",
                        **(
                            {"readinessStatus": readiness_result}
                            if readiness_result
                            else {}
                        ),
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "SmokeCheckFailed",
                            "message": "Candidate smoke check failed; active route was not promoted.",
                        },
                        "smokeCheckStatus": {
                            "phase": "Failed",
                            "path": smoke_result.get("smokeCheck", {}).get("path"),
                            "expectedStatus": smoke_result.get("expectedStatus"),
                            "actualStatus": smoke_result.get("actualStatus"),
                            "checkedAt": smoke_result.get("checkedAt"),
                            "message": smoke_result.get("message"),
                        },
                    }
                else:
                    promotion = await workflow.execute_activity(
                        promote_candidate,
                        smoke_plan,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=COMMON_RETRY_POLICY,
                    )
                    candidate_status = promotion.get(
                        "candidate",
                        reconcile_status.get("candidateRolloutStatus", {}),
                    )
                    promoted_status = {
                        **reconcile_status,
                        "phase": promotion.get("phase", "Promoted"),
                        "message": promotion.get("message"),
                        "activeImages": candidate_status.get("images", {}),
                        "activeRouteServices": candidate_status.get("services", {}),
                        **(
                            {"readinessStatus": readiness_result}
                            if readiness_result
                            else {}
                        ),
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "Promoted",
                            "completedAt": smoke_result.get("checkedAt"),
                            "message": "Candidate passed smoke check and was promoted.",
                        },
                        "appliedResources": [
                            *reconcile_status.get("appliedResources", []),
                            *promotion.get("appliedResources", []),
                        ],
                        "pruneStatus": promotion.get("pruneStatus"),
                        "prunedResources": promotion.get("prunedResources", []),
                        "smokeCheckStatus": {
                            "phase": "Passed",
                            "path": smoke_result.get("smokeCheck", {}).get("path"),
                            "expectedStatus": smoke_result.get("expectedStatus"),
                            "actualStatus": smoke_result.get("actualStatus"),
                            "checkedAt": smoke_result.get("checkedAt"),
                            "message": smoke_result.get("message"),
                        },
                    }
                    active_version = candidate_status.get("imageVersion")
                    if isinstance(active_version, str):
                        promoted_status["activeImageVersion"] = active_version
                    self._status = promoted_status

                await self._record_status(demo_project, self._status)
            else:
                await self._record_status(demo_project, self._status)

            self._complete_current_reconcile()
            self._continue_as_new_if_needed(project_name)

        self._status = {
            "project": project_name,
            "phase": "Stopped",
            "message": "Workflow received shutdown signal.",
        }

    @workflow.signal
    def reconcile(self, demo_project: dict[str, Any]) -> None:
        identity = reconcile_identity(demo_project)
        if self._is_duplicate_reconcile_signal(identity):
            return
        self._pending_spec = demo_project
        self._pending_reconcile_identity = identity

    @workflow.signal
    def check_worker_version(self) -> None:
        if workflow.info().is_target_worker_deployment_version_changed():
            self._continue_as_new_for_worker_version = True

    @workflow.signal
    def finalize(self, demo_project: dict[str, Any]) -> None:
        self._finalize_spec = demo_project
        self._finalize = True

    @workflow.signal
    def shutdown(self) -> None:
        self._shutdown = True

    async def _run_finalize(self, project_name: str) -> None:
        demo_project = (
            self._finalize_spec
            or self._pending_spec
            or {"metadata": {"name": project_name}}
        )
        finalize_plan = await workflow.execute_activity(
            plan_project_finalization,
            {"demoProject": demo_project},
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        result = completed_activity_status(finalize_plan)
        if result is None:
            namespace_action = await workflow.execute_activity(
                delete_project_namespace_resource,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            source_action = await workflow.execute_activity(
                delete_project_source_repository,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            source_auth_action = await workflow.execute_activity(
                delete_project_source_auth_secret,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_api_key_teardown = await workflow.execute_activity(
                delete_temporal_cloud_api_key_resource,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_service_account_teardown = await workflow.execute_activity(
                delete_temporal_cloud_service_account_resource,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_namespace_protection_teardown = await workflow.execute_activity(
                disable_temporal_cloud_namespace_delete_protection_resource,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_namespace_teardown = await workflow.execute_activity(
                delete_temporal_cloud_namespace_resource,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_secret_teardown = await workflow.execute_activity(
                delete_temporal_cloud_runtime_secret_resource,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_teardown = temporal_cloud_finalization_status(
                finalize_plan,
                api_key_status=temporal_api_key_teardown,
                service_account_status=temporal_service_account_teardown,
                namespace_protection_status=temporal_namespace_protection_teardown,
                namespace_status=temporal_namespace_teardown,
                runtime_secret_status=temporal_secret_teardown,
            )
            ecr_teardown = await workflow.execute_activity(
                delete_project_ecr_repositories_resource,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            external = external_resource_finalization_status(
                finalize_plan,
                temporal_status=temporal_teardown,
                ecr_status=ecr_teardown,
            )
            result = project_finalization_status(
                finalize_plan,
                namespace_action=namespace_action,
                source_action=source_action,
                source_auth_action=source_auth_action,
                external=external,
            )
            self._status = {
                "project": project_name,
                "phase": "Finalized",
                "message": "Project removed from git; resources torn down and workflow completed.",
                "finalizeStatus": result,
            }
            await self._record_status(demo_project, self._status)
            await workflow.execute_activity(
                remove_project_finalizer,
                finalize_plan,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=STATUS_RETRY_POLICY,
            )
        self._status = {
            "project": project_name,
            "phase": "Finalized",
            "message": "Project removed from git; resources torn down and workflow completed.",
            "finalizeStatus": result,
        }

    async def _record_status(
        self,
        demo_project: dict[str, Any],
        status: dict[str, Any],
    ) -> None:
        request = {
            "demoProject": demo_project,
            "status": status,
        }
        await workflow.execute_activity(
            record_demo_project_status,
            request,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=STATUS_RETRY_POLICY,
        )
        await workflow.execute_activity(
            record_catalog_project_status,
            request,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=STATUS_RETRY_POLICY,
        )

    @workflow.query
    def status(self) -> dict[str, Any]:
        return self._status

    def _continue_as_new_if_needed(self, project_name: str) -> None:
        if self._shutdown:
            return
        if (
            self._reconcile_sequence < MAX_RECONCILES_PER_WORKFLOW_RUN
            and not workflow.info().is_continue_as_new_suggested()
            and not self._continue_as_new_for_worker_version
        ):
            return
        self._continue_as_new_for_worker_version = False
        workflow.continue_as_new(
            args=[
                project_name,
                self._status,
                self._pending_spec,
                self._last_build_status,
                self._completed_reconcile_identity,
            ],
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
        elif self._completed_reconcile_identity == self._current_reconcile_identity:
            self._completed_reconcile_identity = None
        self._current_reconcile_identity = None


def remember_completed_reconcile_identity(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    phase = status.get("phase")
    if not isinstance(phase, str) or not phase:
        return False
    return phase not in FAILED_RECONCILE_PHASES and not phase.endswith("Failed")


def rememberable_build_status(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    if value.get("phase") not in {"Pushed", "Resolved"}:
        return None
    source_commit_sha = value.get("sourceCommitSha")
    images = value.get("images")
    if not isinstance(source_commit_sha, str) or not source_commit_sha:
        return None
    if not isinstance(images, dict) or not images:
        return None
    return value


@workflow.defn
class DemoRolloutWorkflow:
    """Smoke-test-gated rollout workflow for a demo update."""

    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        plan = await workflow.execute_activity(
            plan_rollout,
            request,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        smoke_result = await workflow.execute_activity(
            run_smoke_check,
            plan,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        if not smoke_result.get("ok"):
            return {
                "project": plan.get("project"),
                "phase": "SmokeCheckFailed",
                "smokeCheck": smoke_result,
            }

        promotion = await workflow.execute_activity(
            promote_candidate,
            plan,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return {
            **promotion,
            "smokeCheck": smoke_result,
        }


@workflow.defn
class RegistryOperatorCanaryWorkflow:
    """End-to-end gate workflow for registry-operator WorkerDeployment rollouts."""

    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        workflow_info = workflow.info()
        plan = await workflow.execute_activity(
            plan_registry_operator_canary,
            {
                **request,
                "workflowID": workflow_info.workflow_id,
                "runID": workflow_info.run_id,
            },
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        apply_result = await workflow.execute_activity(
            apply_registry_operator_canary_project,
            plan,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        promotion_result: dict[str, Any] = {
            "phase": "NotStarted",
            "ok": False,
        }
        cleanup_result: dict[str, Any] = {
            "phase": "NotStarted",
            "ok": False,
        }
        promotion_ok = False
        try:
            promotion_result = await workflow.execute_activity(
                wait_for_registry_operator_canary_promotion,
                plan,
                start_to_close_timeout=timedelta(minutes=20),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=CHECK_RETRY_POLICY,
            )
            promotion_ok = bool(promotion_result.get("ok"))
        finally:
            delete_result = await workflow.execute_activity(
                delete_registry_operator_canary_project,
                plan,
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=CHECK_RETRY_POLICY,
            )
            cleanup_result = delete_result
            if delete_result.get("phase") not in {"Skipped", "Planned"}:
                cleanup_timeout_seconds = max(
                    1,
                    int(plan.get("cleanupTimeoutSeconds", 600)),
                )
                cleanup_result = await workflow.execute_activity(
                    wait_registry_operator_canary_cleanup,
                    {
                        **plan,
                        "deleteAction": delete_result.get("deleteAction"),
                    },
                    start_to_close_timeout=timedelta(
                        seconds=cleanup_timeout_seconds + 60
                    ),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )

        result = {
            "project": plan.get("project"),
            "phase": (
                "Succeeded" if promotion_ok and cleanup_result.get("ok") else "Failed"
            ),
            "plan": plan,
            "apply": apply_result,
            "promotion": promotion_result,
            "cleanup": cleanup_result,
        }
        if result["phase"] != "Succeeded":
            raise RuntimeError(
                "Registry operator canary failed; promotion or cleanup did not pass"
            )
        return result
