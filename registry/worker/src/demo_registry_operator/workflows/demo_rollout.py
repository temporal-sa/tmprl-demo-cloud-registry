from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors
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
class DemoRolloutWorkflow:
    """Smoke-test-gated rollout workflow for a demo update."""

    @workflow.run
    async def run(
        self,
        request: messages.DemoRolloutRequest,
    ) -> messages.DemoRolloutResult:
        plan = reconcile_activities.plan_rollout(
            {
                "project": request.project,
                "sourceCommitSha": request.sourceCommitSha,
                "demoProject": request.demoProject,
                "host": request.host,
                "candidate": request.candidate,
                "smokeCheck": request.smokeCheck,
            }
        )
        smoke_activity = await workflow.execute_activity(
            reconcile_activities.run_smoke_check,
            reconcile_activities.SmokeCheckRequest(
                project=str(plan.get("project", "unknown")),
                demo_project=plan.get("demoProject"),
                host=plan.get("host"),
                candidate=plan.get("candidate", {}),
                smoke_check=plan.get("smokeCheck"),
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        smoke_result = smoke_activity.status
        if not smoke_result.get("ok"):
            status = {
                "project": plan.get("project"),
                "phase": "SmokeCheckFailed",
                "smokeCheck": smoke_result,
            }
            return messages.DemoRolloutResult(
                project=plan.get("project"),
                phase="SmokeCheckFailed",
                status=status,
            )

        promotion_result = await workflow.execute_activity(
            reconcile_activities.promote_candidate_routes,
            reconcile_activities.PromoteCandidateRoutesRequest(
                project=str(plan.get("project", "unknown")),
                demo_project=plan.get("demoProject", {}),
                candidate=plan.get("candidate", {}),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        promotion = promotion_result.status
        prune_status = await self._prune_rollout_candidate(plan, promotion)
        status = {
            **promotion,
            "pruneStatus": prune_status,
            "prunedResources": prune_status.get("resources", []),
            "smokeCheck": smoke_result,
        }
        return messages.DemoRolloutResult(
            project=status.get("project", plan.get("project")),
            phase=status.get("phase", "Unknown"),
            status=status,
        )

    async def _prune_rollout_candidate(
        self,
        plan: dict[str, Any],
        promotion: dict[str, Any],
    ) -> dict[str, Any]:
        demo_project = plan.get("demoProject")
        candidate = promotion.get("candidate", plan.get("candidate", {}))
        active_version = (
            candidate.get("imageVersion") if isinstance(candidate, dict) else None
        )
        if not isinstance(demo_project, dict) or not isinstance(active_version, str):
            return {"phase": "Skipped", "resources": []}
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
