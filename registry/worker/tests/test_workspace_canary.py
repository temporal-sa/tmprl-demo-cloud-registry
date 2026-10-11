from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.service import RPCError, RPCStatusCode
from temporalio.api.workflowservice.v1 import DescribeWorkerDeploymentResponse

from demo_registry_operator.activities import canary, workspace_canary as checks
from demo_registry_operator.workflows import messages, operator_canary


def plan(mode="crashable"):
    return {
        "project": "canary-123",
        "workspaceMode": mode,
        "targetDeploymentName": "operator",
        "targetBuildID": "candidate",
        "timeoutSeconds": 2,
        "pollSeconds": 0,
        "cleanupTimeoutSeconds": 2,
    }


def context(mode="crashable"):
    workflow_id, wid, namespace = checks.workspace_identity(plan(mode))
    return {
        "workflowID": workflow_id,
        "workspaceID": wid,
        "namespace": namespace,
        "mode": mode,
        "source": {"namespace": "source", "secretPath": "promoted/temporal-v2"},
        "workerPods": ["old-worker"],
        "status": {
            "app_url": "https://workspace.example",
            "task_queue": f"queue-{wid}",
        },
    }


def test_workspace_templates_use_real_opted_in_demos_and_branch_mode():
    templates = {t["name"]: t for t in canary.load_canary_templates()}
    for name, key in (
        ("crashable", "crashable"),
        ("isolated-lambda", "isolatedLambda"),
    ):
        template = templates[name]
        assert template["workspaceMode"] == name
        assert template["demoProjectTemplate"]["spec"]["registry"][key]["enabled"]
        branch = operator_canary.registry_operator_canary_branch_request(
            messages.RegistryOperatorCanaryRequest(),
            template=template,
            workflow_id="gate",
            run_id="run",
        )
        assert branch.workspaceMode == name
        assert branch.workflowID == f"gate/{name}"


def test_workspace_start_is_idempotent_and_pinned_to_candidate(monkeypatch):
    client = SimpleNamespace(execute_update_with_start_workflow=AsyncMock())
    monkeypatch.setattr(checks, "connect", AsyncMock(return_value=client))
    monkeypatch.setattr(
        checks.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(task_queue="registry-blue"),
    )
    monkeypatch.setattr(checks, "_source_snapshot", lambda _: context()["source"])
    result = asyncio.run(
        ActivityEnvironment().run(
            checks.begin_workspace_canary, checks.WorkspaceCanaryRequest(plan())
        )
    )
    call = client.execute_update_with_start_workflow.call_args
    operation = call.kwargs["start_workflow_operation"]
    # Inspect the SDK's actual start request, including versioning override.
    assert operation._start_workflow_input.id == result.status["workflowID"]
    assert operation._start_workflow_input.task_queue == "registry-blue"
    assert (
        operation._start_workflow_input.versioning_override.version.build_id
        == "candidate"
    )
    assert operation._start_workflow_input.args[0]["owner_email"] == checks.OWNER


@pytest.mark.parametrize("step", ["ready", "crash", "scale_down", "scale_up"])
def test_workspace_controls_wait_for_real_workloads_and_preserve_source(
    monkeypatch, step
):
    ctx = context()
    status = {
        "phase": "ScaledDown" if step == "scale_down" else "Active",
        "namespace": ctx["namespace"],
        "task_queue": ctx["status"]["task_queue"],
    }
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(status=WorkflowExecutionStatus.RUNNING)
        ),
        query=AsyncMock(return_value=status),
        execute_update=AsyncMock(),
    )
    monkeypatch.setattr(
        checks,
        "connect",
        AsyncMock(return_value=SimpleNamespace(get_workflow_handle=lambda _: handle)),
    )
    monkeypatch.setattr(checks.WorkerConfig, "from_env", lambda: None)
    monkeypatch.setattr(checks, "_source_snapshot", lambda _: ctx["source"])
    monkeypatch.setattr(checks, "_workloads_ready", lambda *args, **kw: True)
    pods = iter(
        [["old-worker"], ["new-worker"]]
        if step == "crash"
        else ([[]] if step == "scale_down" else [["new-worker"]])
    )
    monkeypatch.setattr(checks, "_worker_pods", lambda _: next(pods))
    result = asyncio.run(
        ActivityEnvironment().run(
            checks.check_workspace_canary,
            checks.WorkspaceCanaryRequest(plan(), ctx, step),
        )
    )
    assert result.status["ok"]
    if step == "ready":
        handle.execute_update.assert_not_awaited()
    else:
        assert handle.execute_update.call_args.kwargs["id"] == f"operator-canary/{step}"


def test_lambda_canary_rejects_the_unsuffixed_secret(monkeypatch):
    ctx = context("isolated-lambda")
    component = {
        "serverless": {
            "provider": "aws-lambda",
            "workerDeployment": {"deploymentName": "source"},
        }
    }
    demo = {
        "spec": {
            "registry": {"isolatedLambda": {"taskQueueEnv": "TEMPORAL_TASK_QUEUE"}},
            "components": {"worker": component},
        }
    }
    monkeypatch.setattr(checks.kube, "read_demo_project", lambda _: demo)
    monkeypatch.setattr(
        checks,
        "_aws",
        lambda _: SimpleNamespace(
            get_function=lambda **_: {
                "Configuration": {
                    "Environment": {
                        "Variables": {"TEMPORAL_RUNTIME_SECRET_ID": "default/temporal"}
                    }
                }
            }
        ),
    )
    worker = {
        "component": "worker",
        "function_name": checks.serverless_names.workspace_lambda_function_name(
            "canary-123", "worker", ctx["workspaceID"]
        ),
        "deployment_name": checks.resources.suffixed_dns_name(
            "source", ctx["workspaceID"]
        ),
    }
    with pytest.raises(ApplicationError, match="promoted runtime secret"):
        checks._verify_lambda_configuration(
            plan("isolated-lambda"), ctx, {"lambda_workers": [worker]}
        )


@pytest.mark.parametrize(
    "shared_queue,success", [(True, True), (False, False), (False, True)]
)
def test_workspace_exercise_verifies_app_queue_and_real_workflow_result(
    monkeypatch, shared_queue, success
):
    ctx = context()
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(
                status=WorkflowExecutionStatus.COMPLETED,
                task_queue="shared" if shared_queue else ctx["status"]["task_queue"],
            )
        ),
        result=AsyncMock(return_value={"success": success}),
    )
    monkeypatch.setattr(
        checks,
        "connect",
        AsyncMock(return_value=SimpleNamespace(get_workflow_handle=lambda _: handle)),
    )
    monkeypatch.setattr(checks.WorkerConfig, "from_env", lambda: None)
    monkeypatch.setattr(
        checks.serverless_worker,
        "connect_to_project_namespace_from_secret_path",
        AsyncMock(
            return_value=(SimpleNamespace(get_workflow_handle=lambda _: handle), "demo")
        ),
    )
    monkeypatch.setattr(
        checks, "_http_json", lambda url, payload: {"workflow_id": "order-123"}
    )
    run = lambda: asyncio.run(
        ActivityEnvironment().run(
            checks.check_workspace_canary,
            checks.WorkspaceCanaryRequest(plan(), ctx, "exercise"),
        )
    )
    if shared_queue or not success:
        with pytest.raises(ApplicationError):
            run()
    else:
        assert run().status["phase"] == "Succeeded"


def test_workspace_cleanup_requires_namespace_absent(monkeypatch):
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(status=WorkflowExecutionStatus.COMPLETED)
        )
    )
    monkeypatch.setattr(
        checks,
        "connect",
        AsyncMock(return_value=SimpleNamespace(get_workflow_handle=lambda _: handle)),
    )
    monkeypatch.setattr(checks.WorkerConfig, "from_env", lambda: None)
    monkeypatch.setattr(checks.kube, "read_namespace", lambda _: {"name": "leftover"})
    with pytest.raises(ApplicationError, match="namespace remains"):
        asyncio.run(
            ActivityEnvironment().run(
                checks.cleanup_workspace_canary, checks.WorkspaceCanaryRequest(plan())
            )
        )


def test_scaled_down_readiness_keeps_client_components_running(monkeypatch):
    monkeypatch.setattr(
        checks.kube,
        "list_deployments",
        lambda _: [
            {"name": "worker", "labels": {checks.CRASHABLE_WORKER_LABEL: "true"}},
            {"name": "app", "labels": {}},
        ],
    )
    replicas = {"worker": 0, "app": 1}
    monkeypatch.setattr(
        checks.kube,
        "read_deployment_status",
        lambda ns, name: {
            "replicas": replicas[name],
            "replicasStatus": replicas[name],
            "readyReplicas": replicas[name],
            "generation": 1,
            "observedGeneration": 1,
        },
    )
    assert checks._workloads_ready("workspace", scaled_down=True)
    replicas["app"] = 0
    assert not checks._workloads_ready("workspace", scaled_down=True)


@pytest.mark.parametrize(
    "permissions", ["source", "missing-source", "workspace-leftover"]
)
def test_lambda_cleanup_checks_shared_invoke_policy(monkeypatch, permissions):
    canary_plan = plan("isolated-lambda")
    _, wid, _ = checks.workspace_identity(canary_plan)
    source_arn = "arn:aws:lambda:us-east-1:123:function:source"
    workspace_name = checks.serverless_names.workspace_lambda_function_name(
        canary_plan["project"], "worker", wid
    )
    component = {
        "serverless": {
            "provider": "aws-lambda",
            "workerDeployment": {"deploymentName": "source"},
        }
    }
    demo = {"spec": {"components": {"worker": component}}}
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(status=WorkflowExecutionStatus.COMPLETED)
        )
    )
    monkeypatch.setattr(
        checks,
        "connect",
        AsyncMock(return_value=SimpleNamespace(get_workflow_handle=lambda _: handle)),
    )
    monkeypatch.setattr(checks.WorkerConfig, "from_env", lambda: None)
    monkeypatch.setattr(checks.kube, "read_namespace", lambda _: None)
    monkeypatch.setattr(checks.kube, "read_demo_project", lambda _: demo)
    monkeypatch.setattr(
        checks, "_source_snapshot", lambda _: {"secretPath": "promoted/temporal"}
    )
    describe = AsyncMock(side_effect=RPCError("missing", RPCStatusCode.NOT_FOUND, b""))
    monkeypatch.setattr(
        checks.serverless_worker,
        "connect_to_project_namespace_from_secret_path",
        AsyncMock(
            return_value=(
                SimpleNamespace(
                    workflow_service=SimpleNamespace(
                        describe_worker_deployment=describe
                    )
                ),
                "demo",
            )
        ),
    )

    class MissingFunction(Exception):
        pass

    def get_function(*, FunctionName):
        if FunctionName == workspace_name:
            raise MissingFunction()
        return {"Configuration": {"FunctionArn": source_arn}}

    arns = [source_arn + ":1"] if permissions != "missing-source" else []
    if permissions == "workspace-leftover":
        arns.append(f"arn:aws:lambda:us-east-1:123:function:{workspace_name}:2")
    policy = checks.iam.temporal_lambda_invoke_policy_resources(set(arns))
    lambda_client = SimpleNamespace(
        get_function=get_function,
        exceptions=SimpleNamespace(ResourceNotFoundException=MissingFunction),
    )
    iam_client = SimpleNamespace(get_role_policy=lambda **_: {"PolicyDocument": policy})
    monkeypatch.setattr(
        checks,
        "_aws",
        lambda service: lambda_client if service == "lambda" else iam_client,
    )
    run = lambda: asyncio.run(
        ActivityEnvironment().run(
            checks.cleanup_workspace_canary, checks.WorkspaceCanaryRequest(canary_plan)
        )
    )
    if permissions == "source":
        assert run().status["phase"] == "Deleted"
    else:
        with pytest.raises(ApplicationError, match="invoke permissions"):
            run()


def test_source_lambda_routing_snapshot_includes_typed_current_version(monkeypatch):
    response = DescribeWorkerDeploymentResponse()
    config = response.worker_deployment_info.routing_config
    config.current_deployment_version.deployment_name = "source"
    config.current_deployment_version.build_id = "v1"
    client = SimpleNamespace(
        workflow_service=SimpleNamespace(
            describe_worker_deployment=AsyncMock(return_value=response)
        )
    )
    monkeypatch.setattr(
        checks.serverless_worker,
        "connect_to_project_namespace_from_secret_path",
        AsyncMock(return_value=(client, "demo")),
    )
    monkeypatch.setattr(
        checks.kube,
        "read_demo_project",
        lambda _: {
            "spec": {
                "components": {
                    "worker": {
                        "serverless": {
                            "provider": "aws-lambda",
                            "workerDeployment": {"deploymentName": "source"},
                        }
                    }
                }
            }
        },
    )
    result = asyncio.run(
        checks._lambda_routing(plan("isolated-lambda"), "promoted/temporal")
    )
    assert result["source"]["currentBuild"] == "v1"
