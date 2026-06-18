from __future__ import annotations

from datetime import timedelta

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
    from demo_registry_operator.errors import NON_RETRYABLE_ERROR_TYPES
    from demo_registry_operator.workflows.messages import (
        DemoProjectArtifactBuildRequest,
        DemoProjectArtifactBuildResult,
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
