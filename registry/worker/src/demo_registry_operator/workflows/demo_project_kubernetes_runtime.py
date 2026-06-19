from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors, resources
from demo_registry_operator.activities import reconcile as reconcile_activities
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
class DemoProjectKubernetesRuntimeWorkflow:
    """Apply and promote the Kubernetes runtime for one DemoProject reconcile."""

    @workflow.run
    async def run(
        self,
        request: messages.DemoProjectKubernetesRuntimeRequest,
    ) -> messages.DemoProjectKubernetesRuntimeResult:
        project = request.project
        demo_project = request.demoProject
        previous_status = request.previousStatus or {}
        reconcile_result = await workflow.execute_activity(
            reconcile_activities.apply_demo_project_resources,
            reconcile_activities.ApplyDemoProjectResourcesRequest(
                demo_project=demo_project,
                external_resources=request.externalResources,
                build_status=request.buildStatus,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        reconcile_status = reconcile_result.status

        smoke_plan = {
            "project": project,
            "demoProject": demo_project,
            "host": reconcile_status.get("desired", {}).get("host"),
            "candidate": reconcile_status.get("candidateRolloutStatus", {}),
            "smokeCheck": demo_project.get("spec", {}).get("smokeCheck"),
            "workerDeployments": reconcile_status.get(
                "candidateRolloutStatus",
                {},
            ).get("workerDeployments", {}),
            "workerDeploymentTimeoutSeconds": (
                resources.worker_deployment_rollout_wait_seconds(
                    demo_project,
                    base_timeout_seconds=300,
                )
            ),
        }

        if reconcile_status.get("phase") == "WorkerRolloutPending":
            worker_rollout_result = await self._wait_for_worker_rollout(
                demo_project,
                smoke_plan,
            )
            if not worker_rollout_result.get("ok"):
                cleanup_status = await self._cleanup_failed_candidate(
                    project,
                    demo_project,
                    reconcile_status,
                    previous_status,
                )
                status = self._worker_rollout_failed_status(
                    reconcile_status,
                    worker_rollout_result,
                    cleanup_status,
                )
                return messages.DemoProjectKubernetesRuntimeResult(status=status)

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

            if not demo_project.get("spec", {}).get("smokeCheck"):
                promoted_status = await self._promote_without_smoke_check(
                    project,
                    demo_project,
                    reconcile_status,
                    smoke_plan,
                    completed_at=worker_rollout_result.get("checkedAt"),
                    message=(
                        "WorkerDeployment rollout passed and active route "
                        "resources were promoted."
                    ),
                )
                return messages.DemoProjectKubernetesRuntimeResult(
                    status=promoted_status
                )

        if reconcile_status.get("phase") != "SmokeCheckPending":
            return messages.DemoProjectKubernetesRuntimeResult(
                status=reconcile_status
            )

        readiness_result = await self._wait_for_candidate_readiness(
            demo_project,
            smoke_plan,
        )
        if not readiness_result.get("ok"):
            cleanup_status = await self._cleanup_failed_candidate(
                project,
                demo_project,
                reconcile_status,
                previous_status,
            )
            status = self._candidate_not_ready_status(
                reconcile_status,
                readiness_result,
                cleanup_status,
            )
            return messages.DemoProjectKubernetesRuntimeResult(status=status)

        smoke_result = await self._run_smoke_check(
            project,
            demo_project,
            smoke_plan,
        )
        if not smoke_result.get("ok"):
            cleanup_status = await self._cleanup_failed_candidate(
                project,
                demo_project,
                reconcile_status,
                previous_status,
            )
            status = self._smoke_check_failed_status(
                reconcile_status,
                readiness_result,
                smoke_result,
                cleanup_status,
            )
            return messages.DemoProjectKubernetesRuntimeResult(status=status)

        promoted_status = await self._promote_with_smoke_check(
            project,
            demo_project,
            reconcile_status,
            readiness_result,
            smoke_result,
            smoke_plan,
        )
        return messages.DemoProjectKubernetesRuntimeResult(status=promoted_status)

    async def _wait_for_worker_rollout(
        self,
        demo_project: dict[str, Any],
        smoke_plan: dict[str, Any],
    ) -> dict[str, Any]:
        timeout_seconds = int(smoke_plan.get("workerDeploymentTimeoutSeconds", 300))
        worker_rollout_activity = await workflow.execute_activity(
            reconcile_activities.wait_for_worker_deployment_rollouts,
            reconcile_activities.WorkerDeploymentRolloutRequest(
                demo_project=demo_project,
                worker_deployments=smoke_plan.get("workerDeployments", {}),
                timeout_seconds=timeout_seconds,
            ),
            start_to_close_timeout=timedelta(seconds=timeout_seconds + 60),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        return worker_rollout_activity.status

    async def _wait_for_candidate_readiness(
        self,
        demo_project: dict[str, Any],
        smoke_plan: dict[str, Any],
    ) -> dict[str, Any]:
        readiness_activity = await workflow.execute_activity(
            reconcile_activities.wait_for_candidate_readiness,
            reconcile_activities.CandidateReadinessRequest(
                demo_project=demo_project,
                candidate=smoke_plan.get("candidate", {}),
            ),
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        return readiness_activity.status

    async def _run_smoke_check(
        self,
        project: str,
        demo_project: dict[str, Any],
        smoke_plan: dict[str, Any],
    ) -> dict[str, Any]:
        smoke_activity = await workflow.execute_activity(
            reconcile_activities.run_smoke_check,
            reconcile_activities.SmokeCheckRequest(
                project=project,
                demo_project=demo_project,
                host=smoke_plan.get("host"),
                candidate=smoke_plan.get("candidate", {}),
                smoke_check=smoke_plan.get("smokeCheck"),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        return smoke_activity.status

    async def _promote_with_smoke_check(
        self,
        project: str,
        demo_project: dict[str, Any],
        reconcile_status: dict[str, Any],
        readiness_result: dict[str, Any],
        smoke_result: dict[str, Any],
        smoke_plan: dict[str, Any],
    ) -> dict[str, Any]:
        promotion = await self._promote_candidate(project, demo_project, smoke_plan)
        candidate_status = promotion.get(
            "candidate",
            reconcile_status.get("candidateRolloutStatus", {}),
        )
        prune_status = await self._prune_promoted_candidate(
            demo_project,
            candidate_status,
        )
        promoted_status = {
            **reconcile_status,
            "phase": promotion.get("phase", "Promoted"),
            "message": promotion.get("message"),
            "activeImages": candidate_status.get("images", {}),
            "activeRouteServices": candidate_status.get("services", {}),
            "readinessStatus": readiness_result,
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
            "pruneStatus": prune_status,
            "prunedResources": prune_status.get("resources", []),
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
        return promoted_status

    async def _promote_without_smoke_check(
        self,
        project: str,
        demo_project: dict[str, Any],
        reconcile_status: dict[str, Any],
        smoke_plan: dict[str, Any],
        *,
        completed_at: Any,
        message: str,
    ) -> dict[str, Any]:
        promotion = await self._promote_candidate(project, demo_project, smoke_plan)
        candidate_status = promotion.get(
            "candidate",
            reconcile_status.get("candidateRolloutStatus", {}),
        )
        prune_status = await self._prune_promoted_candidate(
            demo_project,
            candidate_status,
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
                "completedAt": completed_at,
                "message": message,
            },
            "appliedResources": [
                *reconcile_status.get("appliedResources", []),
                *promotion.get("appliedResources", []),
            ],
            "pruneStatus": prune_status,
            "prunedResources": prune_status.get("resources", []),
        }
        active_version = candidate_status.get("imageVersion")
        if isinstance(active_version, str):
            promoted_status["activeImageVersion"] = active_version
        return promoted_status

    async def _promote_candidate(
        self,
        project: str,
        demo_project: dict[str, Any],
        smoke_plan: dict[str, Any],
    ) -> dict[str, Any]:
        promotion_result = await workflow.execute_activity(
            reconcile_activities.promote_candidate_routes,
            reconcile_activities.PromoteCandidateRoutesRequest(
                project=project,
                demo_project=demo_project,
                candidate=smoke_plan.get("candidate", {}),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return promotion_result.status

    async def _prune_promoted_candidate(
        self,
        demo_project: dict[str, Any],
        candidate_status: dict[str, Any],
    ) -> dict[str, Any]:
        active_version = candidate_status.get("imageVersion")
        if not isinstance(active_version, str) or not active_version:
            return {
                "phase": "Skipped",
                "message": "Promoted candidate did not report an image version.",
                "resources": [],
            }
        prune_result = await workflow.execute_activity(
            reconcile_activities.prune_stale_runtime_candidates,
            reconcile_activities.PruneStaleRuntimeCandidatesRequest(
                demo_project=demo_project,
                active_version=active_version,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return prune_result.status

    async def _cleanup_failed_candidate(
        self,
        project: str,
        demo_project: dict[str, Any],
        reconcile_status: dict[str, Any],
        previous_status: dict[str, Any],
    ) -> dict[str, Any]:
        rollback_status = await self._restore_previous_active_runtime_routes(
            project,
            demo_project,
            previous_status,
        )
        prune_status = await self._delete_failed_candidate_resources(
            demo_project,
            reconcile_status,
        )
        return {
            "rollbackStatus": rollback_status,
            "pruneStatus": prune_status,
        }

    async def _restore_previous_active_runtime_routes(
        self,
        project: str,
        demo_project: dict[str, Any],
        previous_status: dict[str, Any],
    ) -> dict[str, Any]:
        active_images = string_map(previous_status.get("activeImages"))
        active_version = previous_status.get("activeImageVersion")
        if not active_images or not isinstance(active_version, str):
            return {
                "phase": "Skipped",
                "message": "Previous active runtime version was not available.",
                "appliedResources": [],
            }

        rollback_result = await workflow.execute_activity(
            reconcile_activities.restore_active_runtime_routes,
            reconcile_activities.RestoreActiveRuntimeRoutesRequest(
                project=project,
                demo_project=demo_project,
                active_images=active_images,
                active_version=active_version,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return rollback_result.status

    async def _delete_failed_candidate_resources(
        self,
        demo_project: dict[str, Any],
        reconcile_status: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_status = reconcile_status.get("candidateRolloutStatus", {})
        candidate_version = (
            candidate_status.get("imageVersion")
            if isinstance(candidate_status, dict)
            else None
        )
        if not isinstance(candidate_version, str) or not candidate_version:
            return {
                "phase": "Skipped",
                "message": "Failed candidate did not report an image version.",
                "resources": [],
            }

        delete_result = await workflow.execute_activity(
            reconcile_activities.delete_runtime_candidate_resources,
            reconcile_activities.DeleteRuntimeCandidateResourcesRequest(
                demo_project=demo_project,
                candidate_version=candidate_version,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return delete_result.status

    def _worker_rollout_failed_status(
        self,
        reconcile_status: dict[str, Any],
        worker_rollout_result: dict[str, Any],
        cleanup_status: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        candidate_status = reconcile_status.get("candidateRolloutStatus", {})
        status = {
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
        return self._status_with_cleanup(status, cleanup_status)

    def _candidate_not_ready_status(
        self,
        reconcile_status: dict[str, Any],
        readiness_result: dict[str, Any],
        cleanup_status: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        candidate_status = reconcile_status.get("candidateRolloutStatus", {})
        status = {
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
        return self._status_with_cleanup(status, cleanup_status)

    def _smoke_check_failed_status(
        self,
        reconcile_status: dict[str, Any],
        readiness_result: dict[str, Any],
        smoke_result: dict[str, Any],
        cleanup_status: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        candidate_status = reconcile_status.get("candidateRolloutStatus", {})
        status = {
            **reconcile_status,
            "phase": "SmokeCheckFailed",
            "message": "Candidate smoke check failed; active route was not promoted.",
            "readinessStatus": readiness_result,
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
        return self._status_with_cleanup(status, cleanup_status)

    def _status_with_cleanup(
        self,
        status: dict[str, Any],
        cleanup_status: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not cleanup_status:
            return status

        rendered = {**status}
        rollback_status = cleanup_status.get("rollbackStatus")
        if isinstance(rollback_status, dict):
            rendered["rollbackStatus"] = rollback_status

        prune_status = cleanup_status.get("pruneStatus")
        if isinstance(prune_status, dict):
            rendered["pruneStatus"] = prune_status
            rendered["prunedResources"] = prune_status.get("resources", [])
        return rendered


def string_map(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(item)
        for key, item in value.items()
        if isinstance(key, str) and isinstance(item, str) and item
    }
