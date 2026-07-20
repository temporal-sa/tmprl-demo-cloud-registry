from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors, resources
from demo_registry_operator.activities import reconcile as reconcile_activities
from demo_registry_operator.activities import revision as revision_activities
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
        self._project = project
        self._demo_project = demo_project
        self._external_resources = request.externalResources
        self._previous_status = previous_status
        self._revision_summary = None
        self._previous_active_revision = _revision_name_of(
            previous_status.get("activeRevision")
        )

        # 4.1: plan candidate renderability and create the Revision BEFORE apply, so
        # the candidate Namespace / Deploy+Svc are owned by the Revision from birth.
        plan = await self._plan_candidate_revision(
            demo_project, request.externalResources, request.buildStatus
        )
        revision_summary = None
        revision_owner_reference = None
        if plan.ready:
            revision_ref = await self._ensure_revision(demo_project, plan)
            revision_summary = revision_ref_summary(revision_ref)
            self._revision_summary = revision_summary
            revision_owner_reference = revision_owner_reference_from_ref(revision_ref)
            await self._patch_revision_refs(
                project, {"candidateRevision": revision_summary}
            )

        reconcile_result = await workflow.execute_activity(
            reconcile_activities.apply_demo_project_resources,
            reconcile_activities.ApplyDemoProjectResourcesRequest(
                demo_project=demo_project,
                external_resources=request.externalResources,
                build_status=request.buildStatus,
                revision_ref=revision_summary,
                revision_owner_reference=revision_owner_reference,
            ),
            start_to_close_timeout=timedelta(minutes=7),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        reconcile_status = reconcile_result.status
        if revision_summary is not None:
            await self._set_revision_phase(
                revision_summary["name"],
                _revision_phase_for_reconcile(reconcile_status),
            )

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
            "runtimeNamespace": reconcile_status.get(
                "candidateRolloutStatus",
                {},
            ).get("runtimeNamespace"),
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
                    worker_rollout_failed=True,
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
                    previous_status,
                    completed_at=worker_rollout_result.get("checkedAt"),
                    message=(
                        "WorkerDeployment rollout passed and active route "
                        "resources were promoted."
                    ),
                )
                return messages.DemoProjectKubernetesRuntimeResult(
                    status=promoted_status
                )

        if reconcile_status.get("phase") == "Ready":
            candidate_status = reconcile_status.get("candidateRolloutStatus", {})
            candidate_status = (
                candidate_status if isinstance(candidate_status, dict) else {}
            )
            prune_status = await self._prune_promoted_candidate(
                demo_project,
                candidate_status,
                previous_status,
            )
            reconcile_status = {
                **reconcile_status,
                "pruneStatus": prune_status,
                "prunedResources": prune_status.get("resources", []),
            }
            reconcile_status = await self._finalize_promoted_revision(
                reconcile_status
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
            previous_status,
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
                runtime_namespace=smoke_plan.get("runtimeNamespace"),
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
        previous_status: dict[str, Any],
    ) -> dict[str, Any]:
        promotion = await self._promote_candidate(project, demo_project, smoke_plan)
        candidate_status = promotion.get(
            "candidate",
            reconcile_status.get("candidateRolloutStatus", {}),
        )
        prune_status = await self._prune_promoted_candidate(
            demo_project,
            candidate_status,
            previous_status,
        )
        promoted_status = {
            **reconcile_status,
            "phase": promotion.get("phase", "Promoted"),
            "message": promotion.get("message"),
            "activeImages": candidate_status.get("images", {}),
            "activeRouteServices": candidate_status.get("services", {}),
            "activeRuntimeNamespace": candidate_status.get("runtimeNamespace"),
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
        promoted_status = await self._finalize_promoted_revision(promoted_status)
        return promoted_status

    async def _promote_without_smoke_check(
        self,
        project: str,
        demo_project: dict[str, Any],
        reconcile_status: dict[str, Any],
        smoke_plan: dict[str, Any],
        previous_status: dict[str, Any],
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
            previous_status,
        )
        promoted_status = {
            **reconcile_status,
            "phase": promotion.get("phase", "Promoted"),
            "message": promotion.get("message"),
            "activeImages": candidate_status.get("images", {}),
            "activeRouteServices": candidate_status.get("services", {}),
            "activeRuntimeNamespace": candidate_status.get("runtimeNamespace"),
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
        promoted_status = await self._finalize_promoted_revision(promoted_status)
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
        previous_status: dict[str, Any],
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
                active_runtime_namespace=candidate_status.get("runtimeNamespace"),
                previous_runtime_namespace=previous_status.get(
                    "activeRuntimeNamespace"
                ),
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
        *,
        worker_rollout_failed: bool = False,
    ) -> dict[str, Any]:
        rollback_status = await self._restore_previous_active_runtime_routes(
            project,
            demo_project,
            previous_status,
        )
        result: dict[str, Any] = {"rollbackStatus": rollback_status}
        # 4.2: on a worker rollout failure, render+apply the previous active
        # WorkerDeployment (Worker Versioning rollback) before tearing the candidate down.
        if worker_rollout_failed:
            result["workerDeploymentRollbackStatus"] = (
                await self._rollback_worker_deployments(demo_project, previous_status)
            )
        prune_status = await self._delete_failed_candidate_resources(
            demo_project,
            reconcile_status,
        )
        result["pruneStatus"] = prune_status
        # 4.2: finalize the candidate Revision (Failed -> lastFailedRevision ->
        # guarded delete -> bounded cleanup wait -> revisionCleanupStatus).
        revision_cleanup = await self._finalize_failed_revision()
        if revision_cleanup is not None:
            result["revisionCleanupStatus"] = revision_cleanup
        return result

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
                active_runtime_namespace=previous_status.get(
                    "activeRuntimeNamespace"
                ),
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
                candidate_runtime_namespace=candidate_status.get("runtimeNamespace"),
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

        worker_rollback = cleanup_status.get("workerDeploymentRollbackStatus")
        if isinstance(worker_rollback, dict):
            rendered["workerDeploymentRollbackStatus"] = worker_rollback
        revision_cleanup = cleanup_status.get("revisionCleanupStatus")
        if isinstance(revision_cleanup, dict):
            rendered["revisionCleanupStatus"] = revision_cleanup
        return rendered

    # --- Phase 4.1-4.3: revision lifecycle (activity-calling) ---

    async def _plan_candidate_revision(
        self,
        demo_project: dict[str, Any],
        external_resources: dict[str, Any] | None,
        build_status: dict[str, Any] | None,
    ) -> Any:
        return await workflow.execute_activity(
            revision_activities.plan_candidate_revision,
            revision_activities.PlanCandidateRevisionRequest(
                demo_project=demo_project,
                external_resources=external_resources,
                build_status=build_status,
            ),
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=CHECK_RETRY_POLICY,
        )

    async def _ensure_revision(
        self, demo_project: dict[str, Any], plan: Any
    ) -> Any:
        return await workflow.execute_activity(
            revision_activities.ensure_revision,
            revision_activities.EnsureRevisionRequest(
                demo_project=demo_project,
                rollout_version=plan.rollout_version,
                images=plan.images or {},
                reconcile_key=resources.demo_project_reconcile_key(demo_project),
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _set_revision_phase(
        self, name: str, phase: str, message: str | None = None
    ) -> None:
        await workflow.execute_activity(
            revision_activities.set_revision_phase,
            revision_activities.SetRevisionPhaseRequest(
                name=name, phase=phase, message=message
            ),
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _patch_revision_refs(
        self, project: str, refs: dict[str, Any]
    ) -> None:
        await workflow.execute_activity(
            revision_activities.patch_demo_project_revision_refs,
            revision_activities.PatchRevisionRefsRequest(project=project, refs=refs),
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _delete_revision(self, name: str) -> dict[str, Any]:
        return await workflow.execute_activity(
            revision_activities.delete_revision,
            revision_activities.DeleteRevisionRequest(name=name),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _teardown_revision_runtime_namespace(
        self,
        ref_summary: dict[str, Any] | None,
    ) -> None:
        if not ref_summary or not ref_summary.get("runtimeNamespaceEnabled"):
            return
        namespace = ref_summary.get("runtimeNamespace")
        if not isinstance(namespace, str) or not namespace:
            return
        await workflow.execute_activity(
            reconcile_activities.delete_project_namespace_resource,
            reconcile_activities.ProjectNamespaceDeleteRequest(namespace=namespace),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _plan_revision_deletes(
        self,
        project: str,
        *,
        protected_revisions: list[str],
    ) -> dict[str, Any]:
        return await workflow.execute_activity(
            revision_activities.plan_revision_deletes,
            revision_activities.PlanRevisionDeletesRequest(
                project=project,
                protected_revisions=protected_revisions,
            ),
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _wait_revision_cleanup(
        self, ref_summary: dict[str, Any]
    ) -> dict[str, Any]:
        namespace = (
            ref_summary.get("runtimeNamespace")
            if ref_summary.get("runtimeNamespaceEnabled")
            else resources.project_namespace(self._demo_project)
        )
        return await workflow.execute_activity(
            revision_activities.wait_for_revision_cleanup,
            revision_activities.WaitForRevisionCleanupRequest(
                name=ref_summary["name"],
                mode=revision_cleanup_mode(ref_summary),
                namespace=namespace,
                rollout_version=ref_summary.get("rolloutVersion"),
            ),
            start_to_close_timeout=timedelta(minutes=7),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )

    async def _rollback_worker_deployments(
        self, demo_project: dict[str, Any], previous_status: dict[str, Any]
    ) -> dict[str, Any]:
        active_images = string_map(previous_status.get("activeImages"))
        active_version = previous_status.get("activeImageVersion")
        if not active_images or not isinstance(active_version, str):
            return {
                "phase": "Skipped",
                "message": "Previous active worker images were not available.",
            }
        result = await workflow.execute_activity(
            reconcile_activities.rollback_worker_deployments,
            reconcile_activities.WorkerDeploymentRollbackRequest(
                demo_project=demo_project,
                external_resources=self._external_resources,
                active_images=active_images,
                active_version=active_version,
                worker_deployments=(
                    previous_status.get("workerDeploymentStatus", {}) or {}
                ),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return result.status

    async def _finalize_failed_revision(self) -> dict[str, Any] | None:
        revision = self._revision_summary
        if not revision:
            return None
        name = revision["name"]
        await self._set_revision_phase(name, "Failed", "Candidate rollout failed.")
        await self._patch_revision_refs(
            self._project, {"lastFailedRevision": revision}
        )
        decision = compute_delete_targets(
            active_revision=self._previous_active_revision,
            candidate_revision=name,
            promoted=False,
            previous_active_revision=self._previous_active_revision,
        )
        cleanup: dict[str, Any] = {"phase": "Skipped", "decision": decision}
        if name in (decision.get("delete") or []):
            target = require_allowed_revision_delete(name, decision)
            await self._delete_revision(target)
            cleanup = await self._wait_revision_cleanup(revision)
        await self._patch_revision_refs(
            self._project, {"revisionCleanupStatus": cleanup}
        )
        return cleanup

    async def _finalize_promoted_revision(
        self, promoted_status: dict[str, Any]
    ) -> dict[str, Any]:
        revision = self._revision_summary
        if not revision:
            return promoted_status
        name = revision["name"]
        await self._set_revision_phase(name, "Promoted", "Candidate promoted.")
        # Record the new active FIRST, so the previous active is never deleted before
        # the new one is durably recorded (compute_delete_targets safety).
        await self._patch_revision_refs(self._project, {"activeRevision": revision})
        promoted_status["activeRevision"] = revision
        decision = await self._plan_revision_deletes(
            self._project,
            protected_revisions=[name],
        )
        cleanup: dict[str, Any] = {"phase": "Skipped", "decision": decision}
        for target in decision.get("delete") or []:
            allowed = require_allowed_revision_delete(target, decision)
            target_summary = revision_summary_from_delete_decision(decision, allowed)
            await self._set_revision_phase(
                allowed, "Superseded", "Superseded by a newer revision."
            )
            await self._teardown_revision_runtime_namespace(target_summary)
            await self._delete_revision(allowed)
            cleanup = await self._wait_revision_cleanup(target_summary or revision)
        await self._patch_revision_refs(
            self._project, {"revisionCleanupStatus": cleanup}
        )
        promoted_status["revisionCleanupStatus"] = cleanup
        return promoted_status

def _revision_name_of(ref: Any) -> str | None:
    if isinstance(ref, dict):
        name = ref.get("name")
        if isinstance(name, str) and name:
            return name
    return None


def string_map(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): str(item)
        for key, item in value.items()
        if isinstance(key, str) and isinstance(item, str) and item
    }


def compute_delete_targets(
    *,
    active_revision: str | None,
    candidate_revision: str | None,
    promoted: bool,
    previous_active_revision: str | None,
) -> dict[str, Any]:
    """First-failure-safe delete planning. Never selects the active revision.

    On promotion we may delete the *previous* active revision, but only once the new
    active is recorded (else 'active-not-recorded'). On failure we delete the
    candidate — including a failed FIRST rollout with no active revision yet.
    """
    delete: list[str] = []
    refused: list[str] = []
    if promoted:
        if not active_revision:
            return {"delete": [], "refused": [], "reason": "active-not-recorded"}
        candidates = (
            [previous_active_revision]
            if (
                previous_active_revision
                and previous_active_revision != candidate_revision
            )
            else []
        )
    else:
        candidates = [candidate_revision] if candidate_revision else []
    for name in candidates:
        if active_revision and name == active_revision:
            refused.append(name)
        else:
            delete.append(name)
    return {"delete": delete, "refused": refused}


def require_allowed_revision_delete(target: str, decision: dict[str, Any]) -> str:
    """Workflow-visible guard: a delete target must be in compute_delete_targets()['delete'].

    Python ``assert`` can be optimized away and is not an acceptable safety boundary,
    so this raises a real error instead.
    """
    if target not in set(decision.get("delete") or []):
        raise RuntimeError(
            f"Refusing to delete revision {target!r}; not in computed delete targets."
        )
    return target


def revision_summary_from_delete_decision(
    decision: dict[str, Any],
    name: str,
) -> dict[str, Any] | None:
    revisions = decision.get("revisions")
    if not isinstance(revisions, dict):
        return None
    revision = revisions.get(name)
    return revision if isinstance(revision, dict) else None


# --- Phase 4.1-4.3: revision lifecycle helpers (pure) ---

REVISION_API_VERSION = "registry.tmprl-demo.cloud/v1alpha1"
REVISION_KIND = "DemoProjectRevision"


def revision_owner_reference_from_ref(ref: Any) -> dict[str, Any]:
    """Build a controller ownerReference dict from a RevisionRef (pure, no kube)."""
    return {
        "apiVersion": REVISION_API_VERSION,
        "kind": REVISION_KIND,
        "name": ref.name,
        "uid": ref.uid,
        "controller": True,
        "blockOwnerDeletion": True,
    }


def revision_ref_summary(ref: Any) -> dict[str, Any]:
    """Serialize a RevisionRef to the dict stored in activeRevision/candidateRevision."""
    return {
        "name": ref.name,
        "uid": ref.uid,
        "rolloutVersion": ref.rollout_version,
        "runtimeNamespace": ref.runtime_namespace,
        "runtimeNamespaceEnabled": ref.runtime_namespace_enabled,
    }


def revision_cleanup_mode(ref_summary: dict[str, Any] | None) -> str:
    if ref_summary and ref_summary.get("runtimeNamespaceEnabled"):
        return "runtime-namespace"
    return "control-plane"


def _revision_phase_for_reconcile(reconcile_status: dict[str, Any]) -> str:
    if reconcile_status.get("phase") == "WorkerRolloutPending":
        return "WorkerRolloutPending"
    return "Applied"
