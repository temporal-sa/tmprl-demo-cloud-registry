from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.builds.codebuild.execution import (
        StartProjectImageBuildRequest,
        WaitForProjectImageBuildRequest,
        start_project_image_build,
        wait_for_project_image_build,
    )
    from demo_registry_operator.activities.builds.planning import (
        ResolveProjectImageBuildRequest,
        resolve_project_image_build_plan,
    )
    from demo_registry_operator.activities.builds.status import completed_build_status
    from demo_registry_operator.activities.reconcile import (
        ApplyDemoProjectResourcesRequest,
        CandidateReadinessRequest,
        PromoteCandidateRoutesRequest,
        PruneStaleRuntimeCandidatesRequest,
        SmokeCheckRequest,
        WorkerDeploymentRolloutRequest,
        apply_demo_project_resources,
        promote_candidate_routes,
        prune_stale_runtime_candidates,
        run_smoke_check,
        wait_for_candidate_readiness,
        wait_for_worker_deployment_rollouts,
    )
    from demo_registry_operator.errors import NON_RETRYABLE_ERROR_TYPES
    from demo_registry_operator.resources import (
        worker_deployment_rollout_wait_seconds,
    )
    from demo_registry_operator.workflows.messages import (
        DemoProjectArtifactBuildRequest,
        DemoProjectArtifactBuildResult,
        DemoProjectKubernetesRuntimeRequest,
        DemoProjectKubernetesRuntimeResult,
    )


COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
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


@workflow.defn
class DemoProjectArtifactBuildWorkflow:
    """Resolve and build the source artifacts for one DemoProject reconcile."""

    @workflow.run
    async def run(
        self,
        request: DemoProjectArtifactBuildRequest,
    ) -> DemoProjectArtifactBuildResult:
        build_plan_result = await workflow.execute_activity(
            resolve_project_image_build_plan,
            ResolveProjectImageBuildRequest(
                demo_project=request.demoProject,
                external_resources=request.externalResources,
                previous_build_status=request.previousBuildStatus,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        build_plan = build_plan_result.plan
        build_status = completed_build_status(build_plan)
        if build_status is None:
            started_build_result = await workflow.execute_activity(
                start_project_image_build,
                StartProjectImageBuildRequest(build_plan),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            started_build = started_build_result.plan
            build_status = completed_build_status(started_build)
            if build_status is None:
                build_status_result = await workflow.execute_activity(
                    wait_for_project_image_build,
                    WaitForProjectImageBuildRequest(started_build),
                    start_to_close_timeout=timedelta(hours=1),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=BUILD_WAIT_RETRY_POLICY,
                )
                build_status = build_status_result.status

        return DemoProjectArtifactBuildResult(status=build_status)


@workflow.defn
class DemoProjectKubernetesRuntimeWorkflow:
    """Apply and promote the Kubernetes runtime for one DemoProject reconcile."""

    @workflow.run
    async def run(
        self,
        request: DemoProjectKubernetesRuntimeRequest,
    ) -> DemoProjectKubernetesRuntimeResult:
        project = request.project
        demo_project = request.demoProject
        reconcile_result = await workflow.execute_activity(
            apply_demo_project_resources,
            ApplyDemoProjectResourcesRequest(
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
                worker_deployment_rollout_wait_seconds(
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
                status = self._worker_rollout_failed_status(
                    reconcile_status,
                    worker_rollout_result,
                )
                return DemoProjectKubernetesRuntimeResult(status=status)

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
                return DemoProjectKubernetesRuntimeResult(status=promoted_status)

        if reconcile_status.get("phase") != "SmokeCheckPending":
            return DemoProjectKubernetesRuntimeResult(status=reconcile_status)

        readiness_result = await self._wait_for_candidate_readiness(
            demo_project,
            smoke_plan,
        )
        if not readiness_result.get("ok"):
            status = self._candidate_not_ready_status(
                reconcile_status,
                readiness_result,
            )
            return DemoProjectKubernetesRuntimeResult(status=status)

        smoke_result = await self._run_smoke_check(
            project,
            demo_project,
            smoke_plan,
        )
        if not smoke_result.get("ok"):
            status = self._smoke_check_failed_status(
                reconcile_status,
                readiness_result,
                smoke_result,
            )
            return DemoProjectKubernetesRuntimeResult(status=status)

        promoted_status = await self._promote_with_smoke_check(
            project,
            demo_project,
            reconcile_status,
            readiness_result,
            smoke_result,
            smoke_plan,
        )
        return DemoProjectKubernetesRuntimeResult(status=promoted_status)

    async def _wait_for_worker_rollout(
        self,
        demo_project: dict[str, Any],
        smoke_plan: dict[str, Any],
    ) -> dict[str, Any]:
        timeout_seconds = int(smoke_plan.get("workerDeploymentTimeoutSeconds", 300))
        worker_rollout_activity = await workflow.execute_activity(
            wait_for_worker_deployment_rollouts,
            WorkerDeploymentRolloutRequest(
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
            wait_for_candidate_readiness,
            CandidateReadinessRequest(
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
            run_smoke_check,
            SmokeCheckRequest(
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
            promote_candidate_routes,
            PromoteCandidateRoutesRequest(
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
            prune_stale_runtime_candidates,
            PruneStaleRuntimeCandidatesRequest(
                demo_project=demo_project,
                active_version=active_version,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return prune_result.status

    def _worker_rollout_failed_status(
        self,
        reconcile_status: dict[str, Any],
        worker_rollout_result: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_status = reconcile_status.get("candidateRolloutStatus", {})
        return {
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

    def _candidate_not_ready_status(
        self,
        reconcile_status: dict[str, Any],
        readiness_result: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_status = reconcile_status.get("candidateRolloutStatus", {})
        return {
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

    def _smoke_check_failed_status(
        self,
        reconcile_status: dict[str, Any],
        readiness_result: dict[str, Any],
        smoke_result: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_status = reconcile_status.get("candidateRolloutStatus", {})
        return {
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
