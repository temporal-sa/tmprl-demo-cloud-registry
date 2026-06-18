from __future__ import annotations

import asyncio

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.workflows.messages import (
        RegistryOperatorAuthCanaryRequest,
        RegistryOperatorCanaryRequest,
        RegistryOperatorCanaryResult,
    )
    from demo_registry_operator.workflows.operator_auth_canary import (
        RegistryOperatorAuthCanaryWorkflow,
    )


@workflow.defn
class RegistryOperatorCanaryWorkflow:
    """End-to-end gate workflow for registry-operator WorkerDeployment rollouts."""

    @workflow.run
    async def run(
        self,
        request: RegistryOperatorCanaryRequest,
    ) -> RegistryOperatorCanaryResult:
        workflow_info = workflow.info()
        branch_requests = [
            registry_operator_canary_branch_request(
                request,
                auth_mode="apiKey",
                workflow_id=workflow_info.workflow_id,
                run_id=workflow_info.run_id,
            ),
            registry_operator_canary_branch_request(
                request,
                auth_mode="mtls",
                workflow_id=workflow_info.workflow_id,
                run_id=workflow_info.run_id,
            ),
        ]
        canary_results = await asyncio.gather(
            *(
                workflow.execute_child_workflow(
                    RegistryOperatorAuthCanaryWorkflow.run,
                    branch_request,
                    id=branch_request.workflowID,
                    task_queue=workflow_info.task_queue,
                    parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
                    versioning_intent=workflow.VersioningIntent.COMPATIBLE,
                )
                for branch_request in branch_requests
            )
        )
        canaries = {result.authMode: result for result in canary_results}
        ok = all(result.phase == "Succeeded" for result in canary_results)
        result = RegistryOperatorCanaryResult(
            phase="Succeeded" if ok else "Failed",
            canaries=canaries,
        )
        if result.phase != "Succeeded":
            raise ApplicationError(
                "Registry operator canary failed; one or more auth-mode canaries did not pass",
                result,
                type="RegistryOperatorCanaryFailed",
                non_retryable=True,
            )
        return result


def registry_operator_canary_branch_request(
    request: RegistryOperatorCanaryRequest,
    *,
    auth_mode: str,
    workflow_id: str,
    run_id: str,
) -> RegistryOperatorAuthCanaryRequest:
    suffix = "api-key" if auth_mode == "apiKey" else auth_mode
    prefix = request.projectNamePrefix
    prefix = prefix if isinstance(prefix, str) and prefix else "operator-canary"
    project_name = request.projectName
    if isinstance(project_name, str) and project_name:
        project_name = f"{project_name}-{suffix}"
    return RegistryOperatorAuthCanaryRequest(
        workflowID=f"{workflow_id}/{auth_mode}",
        runID=run_id,
        authMode=auth_mode,
        projectNamePrefix=f"{prefix}-{suffix}",
        projectName=project_name,
        demoProjectTemplate=request.demoProjectTemplate,
        demoProject=request.demoProject,
        expectedSourceCommitSha=request.expectedSourceCommitSha,
        expectedImages=request.expectedImages,
        expectedOperatorImage=request.expectedOperatorImage,
        expectedOperatorSourceSha=request.expectedOperatorSourceSha,
        timeoutSeconds=request.timeoutSeconds,
        pollSeconds=request.pollSeconds,
        cleanupTimeoutSeconds=request.cleanupTimeoutSeconds,
        cleanupPolicy=request.cleanupPolicy,
    )
