"""Rollout checks through the same workspace workflows used by the catalog."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from urllib.request import Request, urlopen

from temporalio import activity
from temporalio.api.workflowservice.v1 import request_response_pb2 as service_pb
from temporalio.client import WithStartWorkflowOperation, WorkflowExecutionStatus
from temporalio.common import (
    PinnedVersioningOverride,
    WorkerDeploymentVersion,
    WorkflowIDConflictPolicy,
)
from temporalio.service import RPCError, RPCStatusCode

from demo_registry_operator import derive, errors, kube, resources
from demo_registry_operator.activities.canary import (
    RegistryOperatorCanaryActivityResult,
)
from demo_registry_operator.activities.crashable_workspace import (
    _source_runtime_namespace,
)
from demo_registry_operator.activities.external_resources import (
    iam,
    serverless_names,
    serverless_worker,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.crashable_workspace_resources import CRASHABLE_WORKER_LABEL
from demo_registry_operator.temporal_client import connect

OWNER = "operator-canary@temporal.io"
RPC_TIMEOUT = timedelta(seconds=30)


@dataclass(frozen=True)
class WorkspaceCanaryRequest:
    plan: dict[str, Any]
    workspace: dict[str, Any] = field(default_factory=dict)
    step: str = "ready"


def workspace_identity(plan: dict[str, Any]) -> tuple[str, str, str]:
    project = plan["project"]
    wid = derive.crashable_workspace_id(OWNER, project)
    return (
        f"crashable-workspace/{OWNER}/{project}",
        wid,
        derive.crashable_workspace_namespace(wid),
    )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise errors.terminal_activity_error(
            {"phase": "Failed", "ok": False, "message": message},
            error_type="RegistryOperatorWorkspaceCanaryFailed",
        )


def _source_snapshot(project: str) -> dict[str, Any]:
    demo = kube.read_demo_project(project)
    _require(
        bool(demo) and demo.get("status", {}).get("phase") == "Promoted",
        "Workspace canary source is not promoted",
    )
    status = demo["status"]
    return {
        "namespace": _source_runtime_namespace(demo, project),
        "secretPath": resources.temporal_runtime_secret_path_from_status(status)
        or resources.temporal_secret_remote_path(demo),
        "images": status.get("activeImages", {}),
    }


def _worker_pods(namespace: str) -> list[str]:
    return sorted(
        p["name"]
        for p in kube.list_pods(
            namespace, label_selector=f"{CRASHABLE_WORKER_LABEL}=true"
        )
    )


def _workloads_ready(namespace: str, *, scaled_down: bool = False) -> bool:
    deployments = kube.list_deployments(namespace)
    if not deployments:
        return False
    workers = 0
    for deployment in deployments:
        worker = deployment.get("labels", {}).get(CRASHABLE_WORKER_LABEL) == "true"
        workers += int(worker)
        status = kube.read_deployment_status(namespace, deployment["name"])
        replicas = 0 if scaled_down and worker else 1
        if status.get("replicas") != replicas:
            return False
        if replicas == 0:
            if (status.get("replicasStatus") or 0) != 0:
                return False
        elif (status.get("readyReplicas") or 0) < replicas:
            return False
        if (status.get("observedGeneration") or 0) < (status.get("generation") or 0):
            return False
    return not scaled_down or workers > 0


@activity.defn
async def begin_workspace_canary(
    request: WorkspaceCanaryRequest,
) -> RegistryOperatorCanaryActivityResult:
    plan = request.plan
    mode = plan.get("workspaceMode")
    _require(mode in {"isolated-lambda", "crashable"}, "Unknown workspace canary mode")
    source = _source_snapshot(plan["project"])
    routing = (
        await _lambda_routing(plan, source["secretPath"])
        if mode == "isolated-lambda"
        else {}
    )
    workflow_id, wid, namespace = workspace_identity(plan)
    client = await connect(WorkerConfig.from_env())
    operation = WithStartWorkflowOperation(
        "IsolatedLambdaWorkspaceWorkflow"
        if mode == "isolated-lambda"
        else "CrashableWorkspaceWorkflow",
        {"demo_name": plan["project"], "owner_email": OWNER, "ttl_seconds": 3600},
        id=workflow_id,
        task_queue=WorkerConfig.from_env().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        versioning_override=PinnedVersioningOverride(
            WorkerDeploymentVersion(plan["targetDeploymentName"], plan["targetBuildID"])
        ),
    )
    await client.execute_update_with_start_workflow(
        "ensure", start_workflow_operation=operation, rpc_timeout=RPC_TIMEOUT
    )
    return RegistryOperatorCanaryActivityResult(
        {
            "workflowID": workflow_id,
            "workspaceID": wid,
            "namespace": namespace,
            "source": source,
            "lambdaRouting": routing,
            "mode": mode,
            "phase": "Started",
            "ok": True,
        }
    )


@activity.defn
async def check_workspace_canary(
    request: WorkspaceCanaryRequest,
) -> RegistryOperatorCanaryActivityResult:
    context = dict(request.workspace)
    client = await connect(WorkerConfig.from_env())
    handle = client.get_workflow_handle(context["workflowID"])
    step = request.step
    _require(
        step in {"ready", "crash", "scale_down", "scale_up", "exercise"},
        "Unknown workspace canary step",
    )
    if step == "exercise":
        return RegistryOperatorCanaryActivityResult(await _exercise(request))
    if step != "ready":
        _require(
            context["mode"] == "crashable",
            "Lambda workspaces do not support crash or scale controls",
        )
        # A retry must observe the original Update, even after its phase changes.
        await handle.execute_update(
            step, id=f"operator-canary/{step}", rpc_timeout=RPC_TIMEOUT
        )
    deadline = time.monotonic() + int(request.plan.get("timeoutSeconds", 900))
    expected_phase = "ScaledDown" if step == "scale_down" else "Active"
    while True:
        description = await handle.describe(rpc_timeout=RPC_TIMEOUT)
        _require(
            description.status == WorkflowExecutionStatus.RUNNING,
            "Workspace workflow closed before it became ready",
        )
        status = await handle.query("status", rpc_timeout=RPC_TIMEOUT)
        _require(
            not status.get("last_error") and not status.get("failure"),
            "Workspace workflow reported a failure",
        )
        if status.get("phase") == expected_phase and _workloads_ready(
            context["namespace"], scaled_down=step == "scale_down"
        ):
            pods = _worker_pods(context["namespace"])
            if (step != "scale_down" or not pods) and (
                step != "crash"
                or (pods and not set(pods).intersection(context.get("workerPods", [])))
            ):
                break
        activity.heartbeat({"step": step, "phase": status.get("phase")})
        _require(
            time.monotonic() < deadline,
            f"Workspace {step} did not complete before timeout",
        )
        await asyncio.sleep(int(request.plan.get("pollSeconds", 10)))
    _require(
        status.get("namespace")
        == context["namespace"]
        != context["source"]["namespace"],
        "Workspace namespace is not isolated",
    )
    _require(
        str(status.get("task_queue", "")).endswith(context["workspaceID"]),
        "Workspace task queue is not isolated",
    )
    _require(
        _source_snapshot(request.plan["project"]) == context["source"],
        "Workspace action changed the promoted source",
    )
    _require(
        _workloads_ready(context["source"]["namespace"]),
        "Workspace action disrupted the source workloads",
    )
    if context["mode"] == "isolated-lambda":
        _verify_lambda_configuration(request.plan, context, status)
        _require(
            await _lambda_routing(request.plan, context["source"]["secretPath"])
            == context["lambdaRouting"],
            "Workspace changed source Lambda routing",
        )
    elif step != "scale_down":
        _require(bool(pods), "Crashable workspace has no labeled worker pods")
    context.update(status=status, workerPods=pods, phase=expected_phase, ok=True)
    return RegistryOperatorCanaryActivityResult(context)


def _aws(service: str) -> Any:
    import boto3

    return boto3.session.Session(region_name=WorkerConfig.from_env().aws_region).client(
        service
    )


def _verify_lambda_configuration(
    plan: dict[str, Any], context: dict[str, Any], status: dict[str, Any]
) -> None:
    workers = status.get("lambda_workers", [])
    _require(bool(workers), "Isolated workspace has no Lambda workers")
    demo = kube.read_demo_project(plan["project"])
    for worker in workers:
        component = resources.serverless_lambda_components(demo)[worker["component"]]
        _require(
            worker["function_name"]
            == serverless_names.workspace_lambda_function_name(
                plan["project"], worker["component"], context["workspaceID"]
            ),
            "Lambda function is not workspace-scoped",
        )
        _require(
            worker["deployment_name"]
            == resources.suffixed_dns_name(
                resources.serverless_worker_deployment_name(
                    worker["component"], component
                ),
                context["workspaceID"],
            ),
            "Lambda Worker Deployment is not workspace-scoped",
        )
        config = _aws("lambda").get_function(FunctionName=worker["function_name"])[
            "Configuration"
        ]
        env = config.get("Environment", {}).get("Variables", {})
        _require(
            env.get("TEMPORAL_RUNTIME_SECRET_ID") == context["source"]["secretPath"],
            "Lambda did not use the promoted runtime secret",
        )
        queue_env = demo["spec"]["registry"]["isolatedLambda"]["taskQueueEnv"]
        _require(
            env.get(queue_env) == status["task_queue"],
            "Lambda did not use the workspace task queue",
        )


async def _lambda_routing(plan: dict[str, Any], secret_path: str) -> dict[str, Any]:
    (
        client,
        namespace,
    ) = await serverless_worker.connect_to_project_namespace_from_secret_path(
        secret_path
    )
    demo = kube.read_demo_project(plan["project"])
    deployments = {
        resources.serverless_worker_deployment_name(name, component)
        for name, component in resources.serverless_lambda_components(demo).items()
    }
    routing = {}
    for deployment in sorted(deployments):
        described = await client.workflow_service.describe_worker_deployment(
            service_pb.DescribeWorkerDeploymentRequest(
                namespace=namespace, deployment_name=deployment
            ),
            timeout=RPC_TIMEOUT,
        )
        config = described.worker_deployment_info.routing_config
        routing[deployment] = {
            "current": config.current_version,
            "currentDeployment": config.current_deployment_version.deployment_name,
            "currentBuild": config.current_deployment_version.build_id,
            "ramping": config.ramping_version,
            "rampingDeployment": config.ramping_deployment_version.deployment_name,
            "rampingBuild": config.ramping_deployment_version.build_id,
            "rampingPercentage": config.ramping_version_percentage,
        }
    _require(bool(routing), "Source demo has no Lambda Worker Deployments")
    return routing


def _http_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    req = Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=30) as response:
        return json.load(response)


async def _exercise(request: WorkspaceCanaryRequest) -> dict[str, Any]:
    context = dict(request.workspace)
    status = context["status"]
    url = status["app_url"].rstrip("/")
    # Retain the submitted execution across retries of the completion check.
    checkpoint = activity.info().heartbeat_details
    submission = (
        checkpoint[0] if checkpoint and isinstance(checkpoint[0], dict) else None
    )
    if not submission or submission.get("step") != "exercise":
        if context["mode"] == "isolated-lambda":
            response = await asyncio.to_thread(
                _http_json, f"{url}/api/submit", {"name": "operator-canary"}
            )
            workflow_id = response["workflowId"]
        else:
            await asyncio.to_thread(
                _http_json,
                f"{url}/api/settings",
                {
                    "mode": "temporal",
                    "failure_scenario": "none",
                    "presentation_mode": "simple",
                },
            )
            response = await asyncio.to_thread(
                _http_json,
                f"{url}/api/orders",
                {
                    "items": [
                        {
                            "menu_item_id": "canary",
                            "name": "Canary taco",
                            "quantity": 1,
                            "price": 1.0,
                        }
                    ]
                },
            )
            workflow_id = response["workflow_id"]
        _require(bool(workflow_id), "Workspace app did not start a Temporal workflow")
        submission = {"step": "exercise", "workflowID": workflow_id}
        activity.heartbeat(submission)
    client, _ = await serverless_worker.connect_to_project_namespace_from_secret_path(
        context["source"]["secretPath"]
    )
    handle = client.get_workflow_handle(submission["workflowID"])
    description = await handle.describe(rpc_timeout=RPC_TIMEOUT)
    _require(
        description.task_queue == status["task_queue"],
        "Workspace app submitted work to the shared task queue",
    )
    if (
        context["mode"] == "crashable"
        and description.status == WorkflowExecutionStatus.RUNNING
    ):
        await handle.signal("order_ready", rpc_timeout=RPC_TIMEOUT)
    deadline = time.monotonic() + int(request.plan.get("timeoutSeconds", 900))
    while description.status == WorkflowExecutionStatus.RUNNING:
        activity.heartbeat(submission)
        _require(
            time.monotonic() < deadline,
            "Demo workflow did not complete on the workspace queue",
        )
        await asyncio.sleep(int(request.plan.get("pollSeconds", 10)))
        description = await handle.describe(rpc_timeout=RPC_TIMEOUT)
    _require(
        description.status == WorkflowExecutionStatus.COMPLETED,
        "Demo workflow failed on the workspace queue",
    )
    output = await handle.result(rpc_timeout=RPC_TIMEOUT)
    _require(
        isinstance(output, dict)
        and (
            bool(output.get("message"))
            if context["mode"] == "isolated-lambda"
            else output.get("success") is True
        ),
        "Demo workflow returned an unsuccessful result",
    )
    context.update(execution=submission, phase="Succeeded", ok=True)
    return context


@activity.defn
async def cleanup_workspace_canary(
    request: WorkspaceCanaryRequest,
) -> RegistryOperatorCanaryActivityResult:
    plan = request.plan
    if plan.get("cleanupPolicy") == "Never":
        return RegistryOperatorCanaryActivityResult({"phase": "Skipped", "ok": True})
    workflow_id, wid, namespace = workspace_identity(plan)
    client = await connect(WorkerConfig.from_env())
    handle = client.get_workflow_handle(workflow_id)
    deadline = time.monotonic() + int(plan.get("cleanupTimeoutSeconds", 600))
    try:
        description = await handle.describe(rpc_timeout=RPC_TIMEOUT)
        if description.status == WorkflowExecutionStatus.RUNNING:
            await handle.execute_update(
                "delete", id="operator-canary/delete", rpc_timeout=RPC_TIMEOUT
            )
        while description.status == WorkflowExecutionStatus.RUNNING:
            activity.heartbeat({"step": "delete", "workflowID": workflow_id})
            _require(
                time.monotonic() < deadline,
                "Workspace cleanup did not complete before timeout",
            )
            await asyncio.sleep(int(plan.get("pollSeconds", 10)))
            description = await handle.describe(rpc_timeout=RPC_TIMEOUT)
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:
            raise
    # Workspace teardown requests deletion; Kubernetes finishes namespace
    # finalization asynchronously after the entity workflow has completed.
    while kube.read_namespace(namespace) is not None:
        activity.heartbeat({"step": "wait_namespace_deletion", "namespace": namespace})
        _require(
            time.monotonic() < deadline,
            "Workspace namespace remains after cleanup timeout",
        )
        await asyncio.sleep(int(plan.get("pollSeconds", 10)))
    if plan["workspaceMode"] == "isolated-lambda":
        demo = kube.read_demo_project(plan["project"])
        (
            project_client,
            project_namespace,
        ) = await serverless_worker.connect_to_project_namespace_from_secret_path(
            _source_snapshot(plan["project"])["secretPath"]
        )
        for name, component in resources.serverless_lambda_components(demo).items():
            function = serverless_names.workspace_lambda_function_name(
                plan["project"], name, wid
            )
            lambda_client = _aws("lambda")
            try:
                lambda_client.get_function(FunctionName=function)
            except lambda_client.exceptions.ResourceNotFoundException:
                pass
            else:
                _require(False, "Workspace Lambda function remains after cleanup")
            deployment = resources.suffixed_dns_name(
                resources.serverless_worker_deployment_name(name, component), wid
            )
            try:
                await project_client.workflow_service.describe_worker_deployment(
                    service_pb.DescribeWorkerDeploymentRequest(
                        namespace=project_namespace, deployment_name=deployment
                    ),
                    timeout=RPC_TIMEOUT,
                )
            except RPCError as error:
                if error.status != RPCStatusCode.NOT_FOUND:
                    raise
            else:
                _require(False, "Workspace Worker Deployment remains after cleanup")
            # Workspace teardown shares roles and credentials with the source.
            source_function = lambda_client.get_function(
                FunctionName=serverless_names.lambda_function_name(
                    plan["project"], name
                )
            )
            source_arn = source_function["Configuration"]["FunctionArn"]
            policy = _aws("iam").get_role_policy(
                RoleName=serverless_names.temporal_lambda_invoke_role_name(
                    plan["project"], name
                ),
                PolicyName=iam.TEMPORAL_LAMBDA_INVOKE_POLICY_NAME,
            )
            policy_arns = iam.invoke_policy_resource_arns(policy["PolicyDocument"])
            _require(
                any(
                    arn == source_arn or arn.startswith(f"{source_arn}:")
                    for arn in policy_arns
                ),
                "Workspace cleanup removed source invoke permissions",
            )
            _require(
                not any(
                    arn.split(":function:")[-1].split(":")[0] == function
                    for arn in policy_arns
                ),
                "Workspace invoke permissions remain after cleanup",
            )
    if request.workspace.get("source"):
        _require(
            _source_snapshot(plan["project"]) == request.workspace["source"],
            "Workspace cleanup changed the promoted source",
        )
        _require(
            _workloads_ready(request.workspace["source"]["namespace"]),
            "Workspace cleanup disrupted the source workloads",
        )
        if request.workspace.get("lambdaRouting"):
            _require(
                await _lambda_routing(plan, request.workspace["source"]["secretPath"])
                == request.workspace["lambdaRouting"],
                "Workspace cleanup changed source Lambda routing",
            )
    return RegistryOperatorCanaryActivityResult(
        {
            "phase": "Deleted",
            "ok": True,
            "workflowID": workflow_id,
            "namespace": namespace,
        }
    )
