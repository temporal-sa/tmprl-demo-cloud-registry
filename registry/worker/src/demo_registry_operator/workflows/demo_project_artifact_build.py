from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors
from demo_registry_operator.activities.builds import (
    planning as build_planning,
    status as build_statuses,
)
from demo_registry_operator.activities.builds.codebuild import (
    execution as build_execution,
)
from demo_registry_operator.workflows import messages


COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
BUILD_WAIT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=15),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


@workflow.defn
class DemoProjectArtifactBuildWorkflow:
    """Resolve and build the source artifacts for one DemoProject reconcile."""

    @workflow.run
    async def run(
        self,
        request: messages.DemoProjectArtifactBuildRequest,
    ) -> messages.DemoProjectArtifactBuildResult:
        build_plan_result = await workflow.execute_activity(
            build_planning.resolve_project_image_build_plan,
            build_planning.ResolveProjectImageBuildRequest(
                demo_project=request.demoProject,
                external_resources=request.externalResources,
                previous_build_status=request.previousBuildStatus,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        build_plan = build_plan_result.plan
        build_status = build_statuses.completed_build_status(build_plan)
        if build_status is None:
            started_build_result = await workflow.execute_activity(
                build_execution.start_project_image_build,
                build_execution.StartProjectImageBuildRequest(build_plan),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            started_build = started_build_result.plan
            build_status = build_statuses.completed_build_status(started_build)
            if build_status is None:
                build_status_result = await workflow.execute_activity(
                    build_execution.wait_for_project_image_build,
                    build_execution.WaitForProjectImageBuildRequest(started_build),
                    start_to_close_timeout=timedelta(hours=1),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=BUILD_WAIT_RETRY_POLICY,
                )
                build_status = build_status_result.status

        return messages.DemoProjectArtifactBuildResult(status=build_status)
