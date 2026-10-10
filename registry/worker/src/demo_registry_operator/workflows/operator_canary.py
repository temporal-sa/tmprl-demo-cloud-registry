from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from demo_registry_operator import errors
from demo_registry_operator.activities import canary as canary_activities
from demo_registry_operator.workflows import messages
from demo_registry_operator.workflows import (
    operator_auth_canary as auth_canary_workflow,
)

LOAD_CANARY_TEMPLATES_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_attempts=5,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


@workflow.defn
class RegistryOperatorCanaryWorkflow:
    """End-to-end gate workflow for registry-operator WorkerDeployment rollouts.

    Loads the external canary templates (one self-contained YAML per type, e.g.
    api-key, mTLS, Lambda, or Nexus) and launches one branch per template in
    parallel. Adding a new canary type is just a new template file — no change
    here or in registry.yaml.
    """

    @workflow.run
    async def run(
        self,
        request: messages.RegistryOperatorCanaryRequest,
    ) -> messages.RegistryOperatorCanaryResult:
        workflow_info = workflow.info()
        templates = await workflow.execute_activity(
            canary_activities.load_canary_templates,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=LOAD_CANARY_TEMPLATES_RETRY_POLICY,
        )
        branch_requests = [
            registry_operator_canary_branch_request(
                request,
                template=template,
                workflow_id=workflow_info.workflow_id,
                run_id=workflow_info.run_id,
            )
            for template in templates
        ]
        wait_for_all = workflow.patched("operator-canary-wait-for-all-branches")
        canary_results = await asyncio.gather(
            *(
                workflow.execute_child_workflow(
                    auth_canary_workflow.RegistryOperatorAuthCanaryWorkflow.run,
                    branch_request,
                    id=branch_request.workflowID,
                    task_queue=workflow_info.task_queue,
                    parent_close_policy=(
                        workflow.ParentClosePolicy.REQUEST_CANCEL
                        if wait_for_all
                        else workflow.ParentClosePolicy.TERMINATE
                    ),
                    versioning_intent=workflow.VersioningIntent.COMPATIBLE,
                )
                for branch_request in branch_requests
            ),
            return_exceptions=wait_for_all,
        )
        if wait_for_all:
            canary_results = [
                messages.RegistryOperatorAuthCanaryResult(
                    authMode=branch.authMode,
                    phase="Failed",
                    failure=errors.failure_details(result),
                )
                if isinstance(result, BaseException)
                else result
                for branch, result in zip(branch_requests, canary_results, strict=True)
            ]
        canaries = canary_results_by_template_name(templates, canary_results)
        ok = all(result.phase == "Succeeded" for result in canary_results)
        result = messages.RegistryOperatorCanaryResult(
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


def canary_results_by_template_name(
    templates: list[dict],
    canary_results: list[messages.RegistryOperatorAuthCanaryResult],
) -> dict[str, messages.RegistryOperatorAuthCanaryResult]:
    """Keep every branch visible even when several use the same auth mode."""
    canaries: dict[str, messages.RegistryOperatorAuthCanaryResult] = {}
    for index, (template, result) in enumerate(
        zip(templates, canary_results, strict=True)
    ):
        name = template.get("name") or f"canary-{index + 1}"
        if name in canaries:
            raise ApplicationError(
                f"Duplicate operator canary template name {name!r}",
                type="RegistryOperatorCanaryConfigurationError",
                non_retryable=True,
            )
        canaries[name] = result
    return canaries


def registry_operator_canary_branch_request(
    request: messages.RegistryOperatorCanaryRequest,
    *,
    template: dict,
    workflow_id: str,
    run_id: str,
) -> messages.RegistryOperatorAuthCanaryRequest:
    name = template.get("name") or "canary"
    auth_mode = template.get("authMode") or "apiKey"
    demo_project_template = template.get("demoProjectTemplate")
    prefix = request.projectNamePrefix
    prefix = prefix if isinstance(prefix, str) and prefix else "operator-canary"
    project_name = request.projectName
    if isinstance(project_name, str) and project_name:
        project_name = f"{project_name}-{name}"
    return messages.RegistryOperatorAuthCanaryRequest(
        workflowID=f"{workflow_id}/{name}",
        runID=run_id,
        authMode=auth_mode,
        projectNamePrefix=f"{prefix}-{name}",
        projectName=project_name,
        demoProjectTemplate=demo_project_template,
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
