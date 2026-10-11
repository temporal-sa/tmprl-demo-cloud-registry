from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from temporalio.exceptions import ApplicationError

from demo_registry_operator import derive, errors
from demo_registry_operator.activities import crashable_workspace as crashable_activities
from demo_registry_operator.activities import external_resources
from demo_registry_operator.activities import isolated_lambda_workspace as isolated_activities
from demo_registry_operator.activities.external_resources.aws_lambda import (
    workspace_invoke_arns,
)
from demo_registry_operator.activities.external_resources.iam import (
    remove_invoke_policy_resources,
    temporal_lambda_invoke_policy,
    union_invoke_policy_resources,
)
from demo_registry_operator.activities.external_resources.serverless_names import (
    lambda_function_name,
    workspace_lambda_function_name,
)
from demo_registry_operator.crashable_workspace_resources import CRASHABLE_WORKER_LABEL
from demo_registry_operator.isolated_lambda_workspace_resources import (
    build_isolated_workspace_plan,
    serverless_status_images,
)
from demo_registry_operator.resources import catalog_entry
from demo_registry_operator.workflows import crashable_workspace as crashable_workflow
from demo_registry_operator.workflows import isolated_lambda_workspace as isolated_workflow
from demo_registry_operator.workflows.crashable_workspace import (
    CrashableWorkspaceWorkflow,
)
from demo_registry_operator.workflows.crashable_workspace_messages import (
    ApplyWorkspaceResourceResult,
    BuildWorkspacePlanResult,
    EnsureWorkspaceRequest,
    ResolveSourceImagesResult,
    WaitWorkspaceReadyResult,
)
from demo_registry_operator.workflows.isolated_lambda_workspace import (
    IsolatedLambdaWorkspaceWorkflow,
)
from demo_registry_operator.workflows.isolated_lambda_workspace_messages import (
    BuildIsolatedWorkspacePlanRequest,
    EnsureIsolatedWorkspaceRequest,
    UpdateWorkspaceLambdaInvokePolicyRequest,
    WorkspaceLambdaWorker,
)


ALWAYS_ON_ARN = "arn:aws:lambda:us-east-1:123:function:always-on"
ALWAYS_ON_VERSION = "arn:aws:lambda:us-east-1:123:function:always-on:4"


def project(name, **spec):
    return {"metadata": {"name": name}, "spec": spec}


def _isolated_demo(name="rainbow"):
    return project(
        name,
        source={"repo": "https://github.com/temporal-sa/temporal-demo-rainbow"},
        builds={
            "app": {"context": "."},
            "orders": {"context": "."},
        },
        registry={
            "isolatedLambda": {
                "enabled": True,
                "taskQueueEnv": "TEMPORAL_TASK_QUEUE",
                "baseTaskQueue": "orders",
            }
        },
        components={
            "app": {
                "build": "app",
                "temporalAccess": True,
                "ports": [{"name": "http", "containerPort": 8000, "servicePort": 80}],
            },
            "orders-v1": {
                "build": "orders",
                "env": [{"name": "ORDER_VERSION", "value": "v1"}],
                "serverless": {
                    "provider": "aws-lambda",
                    "workerDeployment": {
                        "deploymentName": "orders",
                        "buildId": "v1",
                        "initialCurrent": True,
                    },
                },
            },
            "orders-v2": {
                "build": "orders",
                "env": [{"name": "ORDER_VERSION", "value": "v2"}],
                "serverless": {
                    "provider": "aws-lambda",
                    "workerDeployment": {
                        "deploymentName": "orders",
                        "buildId": "v2",
                    },
                },
            },
        },
        ingress={"subdomain": name, "component": "app", "port": "http"},
    )


def test_workspace_lambda_function_name_is_stable_and_within_limit():
    wid = derive.crashable_workspace_id("alice@temporal.io", "rainbow")
    first = workspace_lambda_function_name(
        "temporal-demo-rainbow-serverless",
        "order-worker",
        wid,
    )
    second = workspace_lambda_function_name(
        "temporal-demo-rainbow-serverless",
        "order-worker",
        wid,
    )
    assert first == second
    assert len(first) <= 64
    assert first != lambda_function_name(
        "temporal-demo-rainbow-serverless",
        "order-worker",
    )


def test_isolated_plan_skips_crashable_labels_and_serverless_pods():
    demo = _isolated_demo()
    wid = derive.crashable_workspace_id("alice@temporal.io", "rainbow")
    images = {"app": "ecr/app:sha", "orders": "ecr/orders:sha"}
    plan = build_isolated_workspace_plan(demo, images, wid)

    assert plan.task_queue == f"orders-{wid}"
    assert CRASHABLE_WORKER_LABEL not in json.dumps(plan.resources)
    deployments = [
        manifest for manifest in plan.resources if manifest.get("kind") == "Deployment"
    ]
    components = {
        manifest["metadata"]["annotations"]["platform.temporal.io/component"]
        for manifest in deployments
    }
    assert components == {"app"}
    injected = 0
    for deployment in deployments:
        for container in deployment["spec"]["template"]["spec"]["containers"]:
            env = {item["name"]: item.get("value") for item in container.get("env", [])}
            if "TEMPORAL_TASK_QUEUE" in env:
                assert env["TEMPORAL_TASK_QUEUE"] == plan.task_queue
                injected += 1
    assert injected == 1

    assert len(plan.components) == 2
    assert len({component.function_name for component in plan.components}) == 2
    assert len({component.deployment_name for component in plan.components}) == 1
    assert all(component.deployment_name.endswith(wid) for component in plan.components)
    current = [component for component in plan.components if component.set_current]
    assert [component.component for component in current] == ["orders-v1"]
    assert {component.build_id for component in plan.components} == {"v1", "v2"}
    by_name = {component.component: component for component in plan.components}
    assert by_name["orders-v1"].env["ORDER_VERSION"] == "v1"
    assert by_name["orders-v1"].env["TEMPORAL_TASK_QUEUE"] == plan.task_queue


@pytest.mark.parametrize("configured_path", [None, "superseded/temporal"])
def test_isolated_plan_uses_promoted_runtime_secret_for_lambda_and_kubernetes(configured_path):
    demo = _isolated_demo()
    demo["spec"]["temporal"] = {"runtimeSecretPath": configured_path}
    demo["status"] = {"temporalRuntimeSecretPath": "promoted/temporal-git-v2"}
    plan = build_isolated_workspace_plan(
        demo, {"app": "ecr/app:sha", "orders": "ecr/orders:sha"}, "abc123"
    )
    assert {component.temporal_secret_path for component in plan.components} == {
        "promoted/temporal-git-v2"
    }
    remote_paths = {
        item["remoteRef"]["key"]
        for manifest in plan.resources
        if manifest["kind"] == "ExternalSecret"
        for item in manifest["spec"]["data"]
        if item["remoteRef"]["property"] in {"address", "namespace", "apiKey"}
    }
    assert remote_paths == {"promoted/temporal-git-v2"}


def test_missing_initial_current_uses_lexicographic_component():
    demo = _isolated_demo()
    demo["spec"]["components"]["orders-v1"]["serverless"]["workerDeployment"].pop(
        "initialCurrent"
    )
    wid = derive.crashable_workspace_id("alice@temporal.io", "rainbow")
    plan = build_isolated_workspace_plan(
        demo,
        {"app": "ecr/app:sha", "orders": "ecr/orders:sha"},
        wid,
    )
    current = [component for component in plan.components if component.set_current]
    assert [component.component for component in current] == ["orders-v1"]
    assert plan.initial_current_defaulted


def test_plan_activity_reuses_promoted_worker_image_when_status_was_pruned(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    demo = yaml.safe_load((root / "projects/demo/no-roads-serverless.yaml").read_text())
    demo["status"] = {
        "activeImages": {"app": "ecr/app:promoted", "worker": "ecr/worker:promoted"},
        "buildStatus": {"images": {"worker": "ecr/worker:unpromoted"}},
    }
    monkeypatch.setattr(isolated_activities.kube, "read_demo_project", lambda _name: demo)
    plan = isolated_activities.build_isolated_workspace_plan_activity(
        BuildIsolatedWorkspacePlanRequest(
            demo_name="no-roads-serverless",
            owner_email="alice@temporal.io",
            workspace_id="0123456789",
            images={"app": "ecr/app:running"},
        )
    )

    assert len(plan.components) == 1
    assert plan.components[0].image_uri == "ecr/worker:promoted"
    assert plan.components[0].build_id == "1.0.0"
    deployments = [item for item in plan.resources if item.get("kind") == "Deployment"]
    assert len(deployments) == 1
    assert deployments[0]["spec"]["template"]["spec"]["containers"][0]["image"] == (
        "ecr/app:running"
    )


def test_serverless_status_images_prefers_component_image_over_promoted_images():
    demo = _isolated_demo()
    demo["status"] = {
        "activeImages": {"orders": "ecr/orders:promoted", "app": "ecr/app:promoted"},
        "serverlessWorkerStatus": {
            "components": {
                "orders-v1": {"image": "ecr/orders:running"},
                "orders-v2": {"image": "ecr/orders:running"},
            }
        },
    }

    assert serverless_status_images(demo) == {"orders": "ecr/orders:running"}


@pytest.mark.parametrize("recorded_image", [None, "", 123])
def test_serverless_status_images_uses_promoted_image_for_missing_component_image(
    recorded_image,
):
    demo = _isolated_demo()
    demo["status"] = {
        "activeImages": {"orders": "ecr/orders:promoted"},
        "serverlessWorkerStatus": {
            "components": {
                name: {"image": recorded_image} for name in ("orders-v1", "orders-v2")
            }
        },
    }

    assert serverless_status_images(demo) == {"orders": "ecr/orders:promoted"}


def test_plan_activity_rejects_unpromoted_images_when_no_running_image_exists(monkeypatch):
    demo = _isolated_demo()
    demo["status"] = {"buildStatus": {"images": {"orders": "ecr/orders:unpromoted"}}}
    monkeypatch.setattr(isolated_activities.kube, "read_demo_project", lambda _name: demo)

    with pytest.raises(ApplicationError, match="has no resolved image") as failure:
        isolated_activities.build_isolated_workspace_plan_activity(
            BuildIsolatedWorkspacePlanRequest(
                demo_name="rainbow",
                owner_email="alice@temporal.io",
                workspace_id="0123456789",
                images={"app": "ecr/app:running"},
            )
        )

    assert failure.value.type == errors.VALIDATION_ERROR
    assert failure.value.non_retryable is True


def test_invoke_policy_union_keeps_the_always_on_function():
    existing = temporal_lambda_invoke_policy(ALWAYS_ON_ARN, ALWAYS_ON_VERSION)
    workspace_arn = "arn:aws:lambda:us-east-1:123:function:workspace"
    workspace_version = f"{workspace_arn}:3"
    updated = union_invoke_policy_resources(
        existing,
        workspace_invoke_arns(workspace_arn, workspace_version),
    )
    resources = set(updated["Statement"][0]["Resource"])
    assert ALWAYS_ON_ARN in resources
    assert ALWAYS_ON_VERSION in resources
    assert workspace_arn in resources
    assert workspace_version in resources

    removed = remove_invoke_policy_resources(
        updated,
        workspace_invoke_arns(workspace_arn, workspace_version),
    )
    assert set(removed["Statement"][0]["Resource"]) == set(
        existing["Statement"][0]["Resource"]
    )
    repeated = remove_invoke_policy_resources(
        removed,
        workspace_invoke_arns(workspace_arn, workspace_version),
    )
    assert repeated == removed


def test_invoke_policy_activity_retains_always_on_arn_and_repeats(monkeypatch):
    document = temporal_lambda_invoke_policy(ALWAYS_ON_ARN, ALWAYS_ON_VERSION)
    stored = {"document": document}

    class FakeIam:
        def get_role_policy(self, **_kwargs):
            return {"PolicyDocument": stored["document"]}

        def put_role_policy(self, **kwargs):
            stored["document"] = json.loads(kwargs["PolicyDocument"])

    monkeypatch.setattr(
        isolated_activities,
        "iam_client",
        lambda _config: FakeIam(),
    )
    monkeypatch.setattr(
        isolated_activities.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(),
    )
    workspace_arn = "arn:aws:lambda:us-east-1:123:function:workspace"
    workspace_version = f"{workspace_arn}:9"
    added = isolated_activities.update_workspace_lambda_invoke_policy(
        UpdateWorkspaceLambdaInvokePolicyRequest(
            role_name="shared-invoke",
            function_arn=workspace_arn,
            function_version_arn=workspace_version,
        )
    )
    assert added.action == "updated"
    assert ALWAYS_ON_ARN in stored["document"]["Statement"][0]["Resource"]

    removed = isolated_activities.update_workspace_lambda_invoke_policy(
        UpdateWorkspaceLambdaInvokePolicyRequest(
            role_name="shared-invoke",
            function_arn=workspace_arn,
            function_version_arn=workspace_version,
            remove=True,
        )
    )
    assert removed.action == "updated"
    assert set(stored["document"]["Statement"][0]["Resource"]) == {
        ALWAYS_ON_ARN,
        f"{ALWAYS_ON_ARN}:*",
        ALWAYS_ON_VERSION,
    }
    repeated = isolated_activities.update_workspace_lambda_invoke_policy(
        UpdateWorkspaceLambdaInvokePolicyRequest(
            role_name="shared-invoke",
            function_arn=workspace_arn,
            function_version_arn=workspace_version,
            remove=True,
        )
    )
    assert repeated.action == "verified"


def test_delete_workspace_functions_is_idempotent_and_leaves_other_names(monkeypatch):
    deleted: list[str] = []

    class Missing(Exception):
        pass

    class FakeLambda:
        def delete_function(self, FunctionName):
            if FunctionName in deleted:
                error = Missing("gone")
                error.response = {"Error": {"Code": "ResourceNotFoundException"}}
                raise error
            deleted.append(FunctionName)

    class FakeSession:
        def client(self, _name):
            return FakeLambda()

    class FakeBoto3:
        class session:
            Session = lambda **_kwargs: FakeSession()

    monkeypatch.setattr(
        isolated_activities.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(aws_region="us-east-1"),
    )
    monkeypatch.setitem(__import__("sys").modules, "boto3", FakeBoto3)
    request = isolated_activities.DeleteWorkspaceLambdaFunctionsRequest(
        function_names=("workspace-fn",)
    )
    first = isolated_activities.delete_workspace_lambda_functions(request)
    second = isolated_activities.delete_workspace_lambda_functions(request)
    assert deleted == ["workspace-fn"]
    assert first.functions[0]["action"] == "deleted"
    assert second.functions[0]["action"] == "not-found"


def test_two_components_register_two_versions_and_one_current(monkeypatch):
    demo = _isolated_demo()
    wid = derive.crashable_workspace_id("alice@temporal.io", "rainbow")
    plan = build_isolated_workspace_plan(
        demo,
        {"app": "ecr/app:sha", "orders": "ecr/orders:sha"},
        wid,
    )
    request = EnsureIsolatedWorkspaceRequest(
        demo_name="rainbow",
        owner_email="alice@temporal.io",
        ttl_seconds=60,
    )
    workspace = IsolatedLambdaWorkspaceWorkflow(request)
    calls: list[tuple[object, object]] = []

    async def execute_activity(activity_fn, activity_request, **_kwargs):
        calls.append((activity_fn, activity_request))
        if activity_fn is crashable_activities.resolve_source_images:
            return ResolveSourceImagesResult(images={"app": "ecr/app:sha", "orders": "ecr/orders:sha"})
        if activity_fn is isolated_activities.build_isolated_workspace_plan_activity:
            return plan
        if activity_fn is isolated_activities.resolve_shared_lambda_roles:
            return SimpleNamespace(
                execution_role_arn="arn:aws:iam::1:role/exec",
                invoke_role_arn="arn:aws:iam::1:role/invoke",
            )
        if activity_fn is external_resources.ensure_lambda_function:
            return SimpleNamespace(
                function_name=activity_request.function_name,
                function_arn=f"arn:aws:lambda:us-east-1:1:function:{activity_request.function_name}",
                function_version_arn=(
                    f"arn:aws:lambda:us-east-1:1:function:{activity_request.function_name}:1"
                ),
            )
        return SimpleNamespace(action="ok")

    async def wait_condition(_predicate, *, timeout):
        workspace.delete()

    now = datetime(2026, 10, 11, tzinfo=UTC)
    monkeypatch.setattr(isolated_workflow.workflow, "now", lambda: now)
    monkeypatch.setattr(
        isolated_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(isolated_workflow.workflow, "wait_condition", wait_condition)
    monkeypatch.setattr(isolated_workflow.workflow, "execute_activity", execute_activity)

    asyncio.run(workspace.run(request))

    names = [activity_fn for activity_fn, _request in calls]
    assert names.count(external_resources.ensure_lambda_function) == 2
    assert names.count(
        external_resources.upsert_serverless_worker_deployment_version
    ) == 2
    assert names.count(
        external_resources.set_serverless_worker_deployment_current
    ) == 1
    upserts = [
        activity_request
        for activity_fn, activity_request in calls
        if activity_fn is external_resources.upsert_serverless_worker_deployment_version
    ]
    assert len({item.deployment_name for item in upserts}) == 1
    assert {item.build_id for item in upserts} == {"v1", "v2"}
    current = next(
        activity_request
        for activity_fn, activity_request in calls
        if activity_fn is external_resources.set_serverless_worker_deployment_current
    )
    assert current.build_id == "v1"
    assert external_resources.ensure_lambda_execution_role not in names
    assert external_resources.ensure_temporal_lambda_invoke_role not in names
    assert crashable_activities.delete_workspace_namespace in names


def test_cleanup_after_continue_as_new_uses_carried_workers(monkeypatch):
    worker = WorkspaceLambdaWorker(
        component="orders-v1",
        function_name="workspace-fn",
        function_arn="arn:aws:lambda:us-east-1:1:function:workspace-fn",
        function_version_arn="arn:aws:lambda:us-east-1:1:function:workspace-fn:1",
        deployment_name="orders-abc",
        build_id="v1",
        invoke_role_name="shared-invoke",
        temporal_secret_path="demo/temporal",
    )
    request = EnsureIsolatedWorkspaceRequest(
        demo_name="rainbow",
        owner_email="alice@temporal.io",
        carried_already_provisioned=True,
        carried_workspace_id="0123abcd89",
        carried_task_queue="orders-0123abcd89",
        carried_created_at="2026-10-11T00:00:00+00:00",
        carried_expires_at="2026-10-11T01:00:00+00:00",
        carried_lambda_workers=(worker,),
        carried_initial_current={"orders-abc": "orders-v1"},
    )
    workspace = IsolatedLambdaWorkspaceWorkflow(request)
    calls: list[object] = []

    async def execute_activity(activity_fn, _request, **_kwargs):
        calls.append(activity_fn)
        return SimpleNamespace()

    async def wait_condition(_predicate, *, timeout):
        workspace.delete()

    now = datetime(2026, 10, 11, tzinfo=UTC)
    monkeypatch.setattr(isolated_workflow.workflow, "now", lambda: now)
    monkeypatch.setattr(
        isolated_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(isolated_workflow.workflow, "wait_condition", wait_condition)
    monkeypatch.setattr(isolated_workflow.workflow, "execute_activity", execute_activity)

    asyncio.run(workspace.run(request))

    assert calls == [
        isolated_activities.delete_workspace_lambda_functions,
        external_resources.delete_serverless_worker_deployment,
        isolated_activities.update_workspace_lambda_invoke_policy,
        crashable_activities.delete_workspace_namespace,
    ]
    assert not hasattr(workspace, "crash")
    assert not hasattr(workspace, "scale_down")
    assert not hasattr(workspace, "scale_up")


def test_crashable_provision_does_not_schedule_lambda_activities(monkeypatch):
    request = EnsureWorkspaceRequest(
        demo_name="canonical-ai-demo",
        owner_email="alice@temporal.io",
    )
    workspace = CrashableWorkspaceWorkflow(request)
    calls: list[object] = []

    async def execute_activity(activity_fn, _request, **_kwargs):
        calls.append(activity_fn)
        if activity_fn is crashable_activities.resolve_source_images:
            return ResolveSourceImagesResult(images={"app": "ecr/app:sha"})
        if activity_fn is crashable_activities.build_workspace_plan:
            return BuildWorkspacePlanResult(
                resources=[{"kind": "ConfigMap", "metadata": {"name": "meta"}}],
                task_queue="music-0123abcd89",
            )
        if activity_fn is crashable_activities.apply_workspace_resource:
            return ApplyWorkspaceResourceResult(action="created")
        if activity_fn is crashable_activities.wait_workspace_ready:
            return WaitWorkspaceReadyResult(ready=True)
        raise AssertionError(activity_fn)

    monkeypatch.setattr(
        crashable_workflow.workflow,
        "now",
        lambda: datetime(2026, 10, 11, tzinfo=UTC),
    )
    monkeypatch.setattr(crashable_workflow.workflow, "execute_activity", execute_activity)

    asyncio.run(workspace._provision(request))

    assert calls == [
        crashable_activities.resolve_source_images,
        crashable_activities.build_workspace_plan,
        crashable_activities.apply_workspace_resource,
        crashable_activities.wait_workspace_ready,
    ]
    assert external_resources.ensure_lambda_function not in calls


def test_catalog_isolated_lambda_uses_the_two_field_gate():
    enabled = catalog_entry(_isolated_demo())
    assert enabled["isolatedLambda"] == {"enabled": True}
    assert enabled["crashable"] == {"enabled": False}

    demo = _isolated_demo()
    demo["spec"]["registry"]["isolatedLambda"] = {"enabled": True}
    assert catalog_entry(demo)["isolatedLambda"] == {"enabled": False}

    both = _isolated_demo()
    both["spec"]["registry"]["crashable"] = {
        "enabled": True,
        "taskQueueEnv": "TEMPORAL_TASK_QUEUE",
    }
    entry = catalog_entry(both)
    assert entry["crashable"] == {"enabled": True}
    assert entry["isolatedLambda"] == {"enabled": True}
