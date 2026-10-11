"""Run with DEMO_REGISTRY_TEST_TEMPORAL_HISTORY=1 and a local Temporal CLI."""

from __future__ import annotations

import asyncio
import os
import shutil
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
from temporalio import activity
from temporalio.api.workflowservice.v1 import DescribeWorkerDeploymentResponse
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from demo_registry_operator.activities.external_resources import serverless_worker
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows import isolated_lambda_workspace as workspace
from demo_registry_operator.workflows.crashable_workspace_messages import (
    DeleteWorkspaceNamespaceRequest,
    DeleteWorkspaceNamespaceResult,
)
from demo_registry_operator.workflows.isolated_lambda_workspace_messages import (
    DeleteWorkspaceLambdaFunctionsRequest,
    DeleteWorkspaceLambdaFunctionsResult,
    EnsureIsolatedWorkspaceRequest,
    UpdateWorkspaceLambdaInvokePolicyRequest,
    UpdateWorkspaceLambdaInvokePolicyResult,
    WorkspaceLambdaWorker,
)


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)
def test_lambda_cleanup_retries_cached_pollers_and_replays_previous_budget(monkeypatch):
    policy = workspace.WORKER_DEPLOYMENT_CLEANUP_RETRY_POLICY
    retry_window = sum(
        min(
            policy.initial_interval.total_seconds()
            * policy.backoff_coefficient**attempt,
            policy.maximum_interval.total_seconds(),
        )
        for attempt in range(policy.maximum_attempts - 1)
    )
    assert retry_window > 5 * 60
    scenario = {"attempts": 0, "steps": []}

    class Service:
        async def describe_worker_deployment(self, request, **kwargs):
            return DescribeWorkerDeploymentResponse(conflict_token=b"token")

        async def set_worker_deployment_current_version(self, request, **kwargs):
            assert request.build_id == ""

        async def delete_worker_deployment_version(self, request, **kwargs):
            scenario["attempts"] += 1
            assert request.skip_drainage
            if scenario["attempts"] <= 8:
                raise RPCError(
                    "version cannot be deleted since it has active pollers",
                    RPCStatusCode.FAILED_PRECONDITION,
                    b"",
                )

        async def delete_worker_deployment(self, request, **kwargs):
            scenario["steps"].append("deployment")

    async def connect(_path):
        return SimpleNamespace(workflow_service=Service()), "test-namespace"

    monkeypatch.setattr(
        serverless_worker, "connect_to_project_namespace_from_secret_path", connect
    )

    @activity.defn(name="delete_workspace_lambda_functions")
    async def functions(
        request: DeleteWorkspaceLambdaFunctionsRequest,
    ) -> DeleteWorkspaceLambdaFunctionsResult:
        scenario["steps"].append("function")
        return DeleteWorkspaceLambdaFunctionsResult(functions=())

    @activity.defn(name="update_workspace_lambda_invoke_policy")
    async def invoke_policy(
        request: UpdateWorkspaceLambdaInvokePolicyRequest,
    ) -> UpdateWorkspaceLambdaInvokePolicyResult:
        assert request.remove
        scenario["steps"].append("policy")
        return UpdateWorkspaceLambdaInvokePolicyResult(action="updated")

    @activity.defn(name="delete_workspace_namespace")
    async def namespace(
        request: DeleteWorkspaceNamespaceRequest,
    ) -> DeleteWorkspaceNamespaceResult:
        scenario["steps"].append("namespace")
        return DeleteWorkspaceNamespaceResult(action="deleting")

    # Accelerate backoff only; preserve the production attempt limits. The
    # production workflow and deletion Activity still own the actual retries.
    runner = SandboxedWorkflowRunner(
        restrictions=create_workflow_runner().restrictions.with_passthrough_modules(
            workspace.__name__
        )
    )
    request = EnsureIsolatedWorkspaceRequest(
        demo_name="demo",
        owner_email="canary@temporal.io",
        carried_already_provisioned=True,
        carried_expires_at="2026-01-01T00:00:00+00:00",
        carried_lambda_workers=(
            WorkspaceLambdaWorker(
                component="worker",
                function_name="function",
                function_arn="arn:function",
                function_version_arn="arn:function:1",
                deployment_name="workspace",
                build_id="v1",
                invoke_role_name="shared",
                temporal_secret_path="test/temporal",
            ),
        ),
    )

    async def verify():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            for attempts in (8, policy.maximum_attempts):
                scenario.update(attempts=0, steps=[])
                monkeypatch.setattr(
                    workspace,
                    "WORKER_DEPLOYMENT_CLEANUP_RETRY_POLICY",
                    replace(
                        policy,
                        initial_interval=timedelta(milliseconds=10),
                        maximum_interval=timedelta(milliseconds=20),
                        maximum_attempts=attempts,
                    ),
                )
                async with Worker(
                    env.client,
                    task_queue="lambda-cleanup-history",
                    workflows=[workspace.IsolatedLambdaWorkspaceWorkflow],
                    workflow_runner=runner,
                    activities=[
                        functions,
                        serverless_worker.delete_serverless_worker_deployment,
                        invoke_policy,
                        namespace,
                    ],
                ):
                    handle = await env.client.start_workflow(
                        workspace.IsolatedLambdaWorkspaceWorkflow.run,
                        request,
                        id=f"lambda-cleanup-{attempts}",
                        task_queue="lambda-cleanup-history",
                    )
                    if attempts == 8:
                        with pytest.raises(WorkflowFailureError):
                            await asyncio.wait_for(handle.result(), 15)
                        assert scenario["steps"] == ["function"]
                    else:
                        await asyncio.wait_for(handle.result(), 15)
                        assert (
                            await handle.describe()
                        ).status == WorkflowExecutionStatus.COMPLETED
                        assert scenario["attempts"] == 9
                        assert scenario["steps"] == [
                            "function",
                            "deployment",
                            "policy",
                            "namespace",
                        ]
                    history = await handle.fetch_history()
                monkeypatch.setattr(
                    workspace, "WORKER_DEPLOYMENT_CLEANUP_RETRY_POLICY", policy
                )
                await Replayer(
                    workflows=[workspace.IsolatedLambdaWorkspaceWorkflow],
                    workflow_runner=runner,
                ).replay_workflow(history)

    asyncio.run(verify())
