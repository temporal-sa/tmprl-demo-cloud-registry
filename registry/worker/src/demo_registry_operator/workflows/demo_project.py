from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.builds import build_project_images
    from demo_registry_operator.activities.external_resources import (
        reconcile_external_resources,
    )
    from demo_registry_operator.activities.reconcile import (
        plan_rollout,
        promote_candidate,
        record_project_status,
        reconcile_project,
        run_smoke_check,
        wait_for_candidate_readiness,
    )


@workflow.defn
class DemoProjectWorkflow:
    """Durable entity workflow for one DemoProject."""

    def __init__(self) -> None:
        self._project_name = "unknown"
        self._pending_spec: dict[str, Any] | None = None
        self._reconcile_sequence = 0
        self._shutdown = False
        self._status: dict[str, Any] = {
            "phase": "Starting",
            "message": "Workflow has not received project state.",
        }

    @workflow.run
    async def run(self, project_name: str) -> None:
        self._project_name = project_name
        self._status = {
            "project": project_name,
            "phase": "Waiting",
            "message": "Waiting for DemoProject reconcile signal.",
        }

        while not self._shutdown:
            await workflow.wait_condition(
                lambda: self._pending_spec is not None or self._shutdown
            )
            if self._shutdown:
                break

            demo_project = self._pending_spec
            self._pending_spec = None
            self._reconcile_sequence += 1
            self._status = {
                "project": project_name,
                "phase": "Reconciling",
            }
            if workflow.patched(
                f"registry-external-build-v1-{self._reconcile_sequence}"
            ):
                external_resources = await workflow.execute_activity(
                    reconcile_external_resources,
                    demo_project,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                build_status = await workflow.execute_activity(
                    build_project_images,
                    {
                        "demoProject": demo_project,
                        "externalResources": external_resources,
                    },
                    start_to_close_timeout=timedelta(hours=1),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=RetryPolicy(maximum_attempts=1),
                )
                reconcile_status = await workflow.execute_activity(
                    reconcile_project,
                    {
                        "demoProject": demo_project,
                        "externalResources": external_resources,
                        "buildStatus": build_status,
                    },
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            else:
                reconcile_status = await workflow.execute_activity(
                    reconcile_project,
                    demo_project,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            self._status = reconcile_status

            if reconcile_status.get("phase") == "SmokeCheckPending":
                smoke_plan = {
                    "project": project_name,
                    "demoProject": demo_project,
                    "host": reconcile_status.get("desired", {}).get("host"),
                    "candidate": reconcile_status.get("candidateRolloutStatus", {}),
                    "smokeCheck": demo_project.get("spec", {}).get("smokeCheck"),
                }
                if workflow.patched(
                    f"candidate-readiness-v1-{self._reconcile_sequence}"
                ):
                    readiness_result = await workflow.execute_activity(
                        wait_for_candidate_readiness,
                        smoke_plan,
                        start_to_close_timeout=timedelta(minutes=10),
                        heartbeat_timeout=timedelta(minutes=2),
                        retry_policy=RetryPolicy(maximum_attempts=1),
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
                        await workflow.execute_activity(
                            record_project_status,
                            {
                                "demoProject": demo_project,
                                "status": self._status,
                            },
                            start_to_close_timeout=timedelta(minutes=1),
                            retry_policy=RetryPolicy(maximum_attempts=3),
                        )
                        continue

                    self._status = {
                        **reconcile_status,
                        "readinessStatus": readiness_result,
                    }
                    await workflow.execute_activity(
                        record_project_status,
                        {
                            "demoProject": demo_project,
                            "status": self._status,
                        },
                        start_to_close_timeout=timedelta(minutes=1),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                else:
                    readiness_result = None

                smoke_result = await workflow.execute_activity(
                    run_smoke_check,
                    smoke_plan,
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=RetryPolicy(maximum_attempts=1),
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
                        retry_policy=RetryPolicy(maximum_attempts=3),
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

                await workflow.execute_activity(
                    record_project_status,
                    {
                        "demoProject": demo_project,
                        "status": self._status,
                    },
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            elif workflow.patched(
                f"record-non-smoke-status-v1-{self._reconcile_sequence}"
            ):
                await workflow.execute_activity(
                    record_project_status,
                    {
                        "demoProject": demo_project,
                        "status": self._status,
                    },
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )

        self._status = {
            "project": project_name,
            "phase": "Stopped",
            "message": "Workflow received shutdown signal.",
        }

    @workflow.signal
    def reconcile(self, demo_project: dict[str, Any]) -> None:
        self._pending_spec = demo_project

    @workflow.signal
    def shutdown(self) -> None:
        self._shutdown = True

    @workflow.query
    def status(self) -> dict[str, Any]:
        return self._status


@workflow.defn
class DemoRolloutWorkflow:
    """Smoke-test-gated rollout workflow for a demo update."""

    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        plan = await workflow.execute_activity(
            plan_rollout,
            request,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        smoke_result = await workflow.execute_activity(
            run_smoke_check,
            plan,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
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
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        return {
            **promotion,
            "smokeCheck": smoke_result,
        }
