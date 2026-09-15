from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from temporalio.exceptions import ActivityError

from demo_registry_operator import derive
from demo_registry_operator.activities import (
    crashable_workspace as crashable_activities,
)
from demo_registry_operator.activities import reconcile
from demo_registry_operator.crashable_workspace_resources import (
    CRASHABLE_WORKER_LABEL,
    build_workspace_manifests,
)
from demo_registry_operator.resources import catalog_entry
from demo_registry_operator.workflows import (
    crashable_workspace as crashable_workflow,
)
from demo_registry_operator.workflows.crashable_workspace import (
    CrashableWorkspaceWorkflow,
)
from demo_registry_operator.workflows.crashable_workspace_messages import (
    ApplyWorkspaceResourceRequest,
    BuildWorkspacePlanRequest,
    CrashWorkspacePodsRequest,
    DeleteWorkspaceNamespaceRequest,
    EnsureWorkspaceRequest,
    ResolveSourceImagesRequest,
    ScaleWorkspaceWorkloadsRequest,
    WaitWorkspaceReadyRequest,
)


def project(name, **spec):
    return {"metadata": {"name": name}, "spec": spec}


def _crashable_demo(name="canonical-ai-demo"):
    return project(
        name,
        source={"repo": "https://github.com/temporal-sa/x"},
        builds={"worker": {"context": "."}, "app": {"context": "."}},
        registry={
            "crashable": {
                "enabled": True,
                "taskQueueEnv": "TEMPORAL_TASK_QUEUE",
                "baseTaskQueue": "music",
            }
        },
        components={
            "worker": {"build": "worker", "worker": True},
            "app": {
                "build": "app",
                "temporalAccess": True,
                "ports": [{"name": "http", "containerPort": 8000, "servicePort": 80}],
            },
        },
        ingress={"subdomain": name, "component": "app", "port": "http"},
    )


def _versioned_crashable_demo(name="canonical-ai-demo"):
    demo = _crashable_demo(name)
    demo["spec"]["components"]["worker"]["worker"] = {
        "versioning": {
            "enabled": True,
            "deploymentName": "travel-worker",
            "rollout": {
                "strategy": "Progressive",
                "steps": [{"rampPercentage": 50, "pauseDuration": "5m"}],
                "gate": {"workflowType": "WorkerCanary"},
            },
        }
    }
    return demo


# --- naming helpers -------------------------------------------------------


def test_workspace_id_is_stable_per_user_and_demo():
    a = derive.crashable_workspace_id("Alice@Temporal.io", "banking")
    b = derive.crashable_workspace_id("alice@temporal.io", "banking")
    c = derive.crashable_workspace_id("bob@temporal.io", "banking")
    d = derive.crashable_workspace_id("alice@temporal.io", "pizza")
    assert a == b  # case/whitespace-insensitive on the email
    assert a != c  # different user
    assert a != d  # different demo
    assert len(a) == derive.CRASHABLE_WORKSPACE_ID_LENGTH


def test_workspace_namespace_host_and_task_queue():
    wid = "0123abcd89"
    assert derive.crashable_workspace_namespace(wid) == f"tmprl-dem-cld-crash-{wid}"
    assert derive.crashable_workspace_host(wid) == f"crash-{wid}.tmprl-demo.cloud"
    assert derive.crashable_workspace_task_queue("music", wid) == f"music-{wid}"


# --- source image resolution ---------------------------------------------


def test_resolve_source_images_uses_active_revision_runtime_namespace(monkeypatch):
    namespaces = []
    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: {
            "status": {
                "activeRevision": {
                    "runtimeNamespace": "tmprl-dem-cld-canonical-ai-demo-a1b2c",
                },
                "activeRuntimeNamespace": "tmprl-dem-cld-canonical-ai-demo-old12",
            }
        },
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_deployment_images",
        lambda namespace: namespaces.append(namespace)
        or {"app": "ecr/app:sha", "worker": "ecr/worker:sha"},
    )

    result = crashable_activities.resolve_source_images(
        ResolveSourceImagesRequest(demo_name="canonical-ai-demo")
    )

    assert result.images == {
        "app": "ecr/app:sha",
        "worker": "ecr/worker:sha",
    }
    assert namespaces == ["tmprl-dem-cld-canonical-ai-demo-a1b2c"]


def test_resolve_source_images_uses_active_runtime_namespace_fallback(monkeypatch):
    namespaces = []
    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: {
            "status": {
                "activeRuntimeNamespace": "tmprl-dem-cld-canonical-ai-demo-a1b2c",
            }
        },
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_deployment_images",
        lambda namespace: namespaces.append(namespace)
        or {"app": "ecr/app:sha", "worker": "ecr/worker:sha"},
    )

    crashable_activities.resolve_source_images(
        ResolveSourceImagesRequest(demo_name="canonical-ai-demo")
    )

    assert namespaces == ["tmprl-dem-cld-canonical-ai-demo-a1b2c"]


def test_resolve_source_images_falls_back_to_base_namespace(monkeypatch):
    namespaces = []
    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: {"status": {}},
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_deployment_images",
        lambda namespace: namespaces.append(namespace)
        or {"app": "ecr/app:sha", "worker": "ecr/worker:sha"},
    )

    crashable_activities.resolve_source_images(
        ResolveSourceImagesRequest(demo_name="canonical-ai-demo")
    )

    assert namespaces == ["tmprl-dem-cld-canonical-ai-demo"]


def test_resolve_source_images_includes_versioned_worker(monkeypatch):
    demo = _versioned_crashable_demo()
    demo["status"] = {
        "activeRuntimeNamespace": "tmprl-dem-cld-canonical-ai-demo-a1b2c"
    }
    worker_namespaces = []
    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: demo,
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_deployment_images",
        lambda namespace: {"app": "ecr/app:sha"},
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_worker_deployment_images",
        lambda namespace: worker_namespaces.append(namespace)
        or {"worker": "ecr/worker:sha"},
    )

    result = crashable_activities.resolve_source_images(
        ResolveSourceImagesRequest(demo_name="canonical-ai-demo")
    )

    assert result.images == {
        "app": "ecr/app:sha",
        "worker": "ecr/worker:sha",
    }
    assert worker_namespaces == ["tmprl-dem-cld-canonical-ai-demo-a1b2c"]


# --- manifest rendering ---------------------------------------------------


def test_build_workspace_manifests_isolates_namespace_host_and_task_queue():
    demo = _crashable_demo()
    wid = derive.crashable_workspace_id("alice@temporal.io", "canonical-ai-demo")
    images = {"worker": "ecr/worker:sha", "app": "ecr/app:sha"}

    ordered, task_queue, host, namespace, deployment_names = build_workspace_manifests(
        demo, images, wid
    )

    # Isolation: workspace-scoped namespace, host, and task queue.
    assert namespace == f"tmprl-dem-cld-crash-{wid}"
    assert host == f"crash-{wid}.tmprl-demo.cloud"
    assert task_queue == f"music-{wid}"

    # Namespace is rendered FIRST (cleanup boundary) and tagged for reaping.
    assert ordered[0]["kind"] == "Namespace"
    assert ordered[0]["metadata"]["name"] == namespace
    labels = ordered[0]["metadata"]["labels"]
    assert labels["platform.temporal.io/crashable-workspace"] == wid

    # Every namespaced object lands in the workspace namespace, not the demo's.
    for manifest in ordered:
        md = manifest["metadata"]
        if manifest["kind"] != "Namespace" and "namespace" in md:
            assert md["namespace"] == namespace

    # The task-queue env is injected into worker + temporal-access components.
    deployments = {
        m["metadata"]["name"]: m for m in ordered if m["kind"] == "Deployment"
    }
    assert deployment_names and set(deployment_names) == set(deployments)
    worker_deployment = next(
        deployment
        for deployment in deployments.values()
        if deployment["metadata"]["annotations"][
            "platform.temporal.io/component"
        ]
        == "worker"
    )
    app_deployment = next(
        deployment
        for deployment in deployments.values()
        if deployment["metadata"]["annotations"][
            "platform.temporal.io/component"
        ]
        == "app"
    )
    assert worker_deployment["metadata"]["labels"][CRASHABLE_WORKER_LABEL] == "true"
    assert (
        worker_deployment["spec"]["template"]["metadata"]["labels"][
            CRASHABLE_WORKER_LABEL
        ]
        == "true"
    )
    assert CRASHABLE_WORKER_LABEL not in app_deployment["metadata"]["labels"]
    assert (
        CRASHABLE_WORKER_LABEL
        not in app_deployment["spec"]["template"]["metadata"]["labels"]
    )
    injected = 0
    for deployment in deployments.values():
        for container in deployment["spec"]["template"]["spec"]["containers"]:
            env = {e["name"]: e.get("value") for e in container.get("env", [])}
            if "TEMPORAL_TASK_QUEUE" in env:
                assert env["TEMPORAL_TASK_QUEUE"] == task_queue
                injected += 1
    assert injected >= 1


def test_build_workspace_manifests_does_not_mutate_source_project():
    demo = _crashable_demo()
    before = demo["spec"]["ingress"]["subdomain"]
    build_workspace_manifests(demo, {"worker": "i", "app": "i"}, "deadbeef00")
    # The renderer deep-copies; the caller's CR is untouched.
    assert demo["spec"]["ingress"]["subdomain"] == before
    assert "env" not in demo["spec"]["components"]["worker"]


def test_build_workspace_manifests_uses_promoted_temporal_secret_path():
    demo = _crashable_demo()
    runtime_secret_path = "tmprl-dem-cld/canonical-ai-demo/temporal-2649e761a6ed"
    demo["status"] = {"temporalRuntimeSecretPath": runtime_secret_path}

    ordered, _, _, _, _ = build_workspace_manifests(
        demo,
        {"worker": "ecr/worker:sha", "app": "ecr/app:sha"},
        "0123abcd89",
    )

    temporal_credentials = next(
        manifest
        for manifest in ordered
        if manifest["kind"] == "ExternalSecret"
        and manifest["metadata"]["name"] == "temporal-credentials"
    )
    assert {
        item["remoteRef"]["key"] for item in temporal_credentials["spec"]["data"]
    } == {runtime_secret_path}
    assert "temporal" not in demo["spec"]


def test_build_workspace_manifests_isolates_versioned_worker():
    demo = _versioned_crashable_demo()
    original_versioning = demo["spec"]["components"]["worker"]["worker"][
        "versioning"
    ]
    wid = "0123abcd89"

    ordered, task_queue, _, namespace, _ = build_workspace_manifests(
        demo,
        {"worker": "ecr/worker:sha", "app": "ecr/app:sha"},
        wid,
    )

    worker_deployment = next(
        manifest for manifest in ordered if manifest["kind"] == "WorkerDeployment"
    )
    assert worker_deployment["metadata"]["name"] == f"travel-worker-{wid}"
    assert worker_deployment["metadata"]["namespace"] == namespace
    assert worker_deployment["metadata"]["labels"][CRASHABLE_WORKER_LABEL] == "true"
    assert (
        worker_deployment["spec"]["template"]["metadata"]["labels"][
            CRASHABLE_WORKER_LABEL
        ]
        == "true"
    )
    assert worker_deployment["spec"]["rollout"] == {"strategy": "AllAtOnce"}
    container = worker_deployment["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item.get("value") for item in container["env"]}
    assert env["TEMPORAL_TASK_QUEUE"] == task_queue

    # Rendering a workspace must not rewrite the always-on demo's identity or gate.
    assert original_versioning["deploymentName"] == "travel-worker"
    assert original_versioning["rollout"]["strategy"] == "Progressive"
    assert original_versioning["rollout"]["gate"] == {
        "workflowType": "WorkerCanary"
    }


def test_build_workspace_plan_returns_versioned_worker_names(monkeypatch):
    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: _versioned_crashable_demo(),
    )

    result = crashable_activities.build_workspace_plan(
        BuildWorkspacePlanRequest(
            demo_name="canonical-ai-demo",
            owner_email="alice@temporal.io",
            workspace_id="0123abcd89",
            images={"worker": "ecr/worker:sha", "app": "ecr/app:sha"},
        )
    )

    assert result.worker_deployment_names == ["travel-worker-0123abcd89"]


def test_wait_workspace_ready_checks_deployments_and_versioned_workers(monkeypatch):
    checked = []
    monkeypatch.setattr(
        crashable_activities,
        "_deployment_ready",
        lambda namespace, name: checked.append(("Deployment", namespace, name))
        or True,
    )
    monkeypatch.setattr(
        crashable_activities,
        "_worker_deployment_ready",
        lambda namespace, name: checked.append(
            ("WorkerDeployment", namespace, name)
        )
        or True,
    )
    monkeypatch.setattr(crashable_activities.activity, "heartbeat", lambda value: None)

    result = crashable_activities.wait_workspace_ready(
        WaitWorkspaceReadyRequest(
            namespace="tmprl-dem-cld-crash-0123abcd89",
            deployment_names=["app-git-123"],
            worker_deployment_names=["travel-worker-0123abcd89"],
        )
    )

    assert result.ready is True
    assert checked == [
        ("Deployment", "tmprl-dem-cld-crash-0123abcd89", "app-git-123"),
        (
            "WorkerDeployment",
            "tmprl-dem-cld-crash-0123abcd89",
            "travel-worker-0123abcd89",
        ),
    ]


def test_worker_deployment_ready_requires_ready_true_condition(monkeypatch):
    statuses = iter(
        [
            {"conditions": [{"type": "Ready", "status": "False"}]},
            {"conditions": [{"type": "Ready", "status": "True"}]},
        ]
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "read_worker_deployment_status",
        lambda namespace, name: next(statuses),
    )

    assert (
        crashable_activities._worker_deployment_ready("workspace", "worker") is False
    )
    assert (
        crashable_activities._worker_deployment_ready("workspace", "worker") is True
    )


def test_apply_workspace_resource_waits_for_secrets_before_workload(monkeypatch):
    events = []
    monkeypatch.setattr(
        crashable_activities,
        "_wait_for_workspace_external_secrets",
        lambda namespace: events.append(("wait", namespace)),
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "apply_resource",
        lambda manifest: events.append(("apply", manifest["kind"])) or "created",
    )

    result = crashable_activities.apply_workspace_resource(
        ApplyWorkspaceResourceRequest(
            manifest={
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "app", "namespace": "workspace"},
            }
        )
    )

    assert result.action == "created"
    assert events == [("wait", "workspace"), ("apply", "Deployment")]


def test_wait_for_workspace_external_secrets_polls_until_ready(monkeypatch):
    statuses = iter(
        [
            {"ready": False, "message": "Secret does not exist"},
            {"ready": True, "message": "Secret was synced"},
        ]
    )
    heartbeats = []
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_external_secrets",
        lambda namespace: [{"metadata": {"name": "temporal-credentials"}}],
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "read_external_secret_status",
        lambda namespace, name: next(statuses),
    )
    monotonic = iter([0.0, 1.0])
    monkeypatch.setattr(crashable_activities.time, "monotonic", lambda: next(monotonic))
    monkeypatch.setattr(crashable_activities.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        crashable_activities.activity,
        "heartbeat",
        lambda value: heartbeats.append(value),
    )

    crashable_activities._wait_for_workspace_external_secrets("workspace")

    assert heartbeats == [
        {
            "namespace": "workspace",
            "pendingExternalSecrets": {
                "temporal-credentials": "Secret does not exist"
            },
        }
    ]


def test_delete_workspace_namespace_uses_dependency_ordered_cleanup(monkeypatch):
    deleted = []
    monkeypatch.setattr(
        reconcile,
        "delete_runtime_namespace_in_dependency_order",
        lambda namespace: deleted.append(namespace) or "deleting",
    )

    result = crashable_activities.delete_workspace_namespace(
        DeleteWorkspaceNamespaceRequest(
            namespace="tmprl-dem-cld-crash-0123abcd89"
        )
    )

    assert result.action == "deleting"
    assert deleted == ["tmprl-dem-cld-crash-0123abcd89"]


def test_scale_workspace_workloads_updates_only_worker_controllers(
    monkeypatch,
):
    scaled = []
    listed = []

    def list_deployments(namespace):
        listed.append(("Deployment", namespace))
        return [
            {
                "name": "labeled-worker",
                "labels": {CRASHABLE_WORKER_LABEL: "true"},
            },
            {
                "name": "legacy-worker",
                "annotations": {"platform.temporal.io/component": "worker"},
            },
            {
                "name": "app",
                "annotations": {"platform.temporal.io/component": "app"},
            },
        ]

    def list_worker_deployments(namespace):
        listed.append(("WorkerDeployment", namespace))
        return [{"metadata": {"name": "versioned-worker"}}]

    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: _crashable_demo(name),
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_deployments",
        list_deployments,
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_worker_deployments",
        list_worker_deployments,
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "scale_deployment",
        lambda namespace, name, replicas: scaled.append(
            ("Deployment", namespace, name, replicas)
        ),
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "scale_worker_deployment",
        lambda namespace, name, replicas: scaled.append(
            ("WorkerDeployment", namespace, name, replicas)
        ),
    )

    result = crashable_activities.scale_workspace_workloads(
        ScaleWorkspaceWorkloadsRequest(
            namespace="workspace",
            replicas=0,
            demo_name="canonical-ai-demo",
        )
    )

    assert result.deployments == 2
    assert result.worker_deployments == 1
    assert listed == [
        ("Deployment", "workspace"),
        ("WorkerDeployment", "workspace"),
    ]
    assert scaled == [
        ("Deployment", "workspace", "labeled-worker", 0),
        ("Deployment", "workspace", "legacy-worker", 0),
        ("WorkerDeployment", "workspace", "versioned-worker", 0),
    ]


def test_scale_down_and_up_preserve_workspace_lifecycle(monkeypatch):
    now = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    request = EnsureWorkspaceRequest(
        demo_name="canonical-ai-demo",
        owner_email="alice@temporal.io",
        carried_already_provisioned=True,
        carried_expires_at=(now + timedelta(hours=1)).isoformat(),
        carried_created_at=now.isoformat(),
        carried_task_queue="music-0123abcd89",
        carried_workspace_id="0123abcd89",
    )
    workspace = CrashableWorkspaceWorkflow(request)
    activities_run = []
    actions = iter(("scale_down", "scale_up", "delete"))

    async def request_action(_predicate, *, timeout):
        action = next(actions)
        status = getattr(workspace, action)()
        assert status.phase == {
            "scale_down": "ScalingDown",
            "scale_up": "ScalingUp",
            "delete": "Deleting",
        }[action]

    async def execute_activity(activity_fn, activity_request, **_kwargs):
        activities_run.append((activity_fn, activity_request))
        return SimpleNamespace(action="done")

    monkeypatch.setattr(crashable_workflow.workflow, "now", lambda: now)
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(
        crashable_workflow.workflow, "wait_condition", request_action
    )
    monkeypatch.setattr(
        crashable_workflow.workflow, "execute_activity", execute_activity
    )

    asyncio.run(workspace.run(request))

    assert [activity_fn for activity_fn, _ in activities_run] == [
        crashable_activities.scale_workspace_workloads,
        crashable_activities.scale_workspace_workloads,
        crashable_activities.delete_workspace_namespace,
    ]
    assert [
        activity_request.replicas for _, activity_request in activities_run[:2]
    ] == [0, 1]
    assert [
        activity_request.demo_name for _, activity_request in activities_run[:2]
    ] == ["canonical-ai-demo", "canonical-ai-demo"]
    assert workspace._status.phase == "Deleting"


def test_continue_as_new_carries_scaled_down_state(monkeypatch):
    now = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    request = EnsureWorkspaceRequest(
        demo_name="canonical-ai-demo",
        owner_email="alice@temporal.io",
        carried_already_provisioned=True,
        carried_expires_at=(now + timedelta(hours=1)).isoformat(),
        carried_created_at=now.isoformat(),
        carried_task_queue="music-0123abcd89",
        carried_workspace_id="0123abcd89",
        carried_scaled_down=True,
    )
    workspace = CrashableWorkspaceWorkflow(request)
    carried = []

    class ContinuedAsNew(Exception):
        pass

    def continue_as_new(next_request):
        carried.append(next_request)
        raise ContinuedAsNew

    monkeypatch.setattr(crashable_workflow.workflow, "now", lambda: now)
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: True),
    )
    monkeypatch.setattr(
        crashable_workflow.workflow, "continue_as_new", continue_as_new
    )

    try:
        asyncio.run(workspace.run(request))
    except ContinuedAsNew:
        pass
    else:
        raise AssertionError("expected the settled workspace to continue as new")

    assert workspace._status.phase == "ScaledDown"
    assert workspace._status.step == "Workers scaled to zero"
    assert len(carried) == 1
    assert carried[0].carried_scaled_down is True


def test_scale_failure_does_not_prevent_requested_workspace_cleanup(monkeypatch):
    now = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
    request = EnsureWorkspaceRequest(
        demo_name="canonical-ai-demo",
        owner_email="alice@temporal.io",
        carried_already_provisioned=True,
        carried_expires_at=(now + timedelta(hours=1)).isoformat(),
        carried_created_at=now.isoformat(),
        carried_task_queue="music-0123abcd89",
        carried_workspace_id="0123abcd89",
    )
    workspace = CrashableWorkspaceWorkflow(request)
    activities_run = []

    async def request_scale_down(_predicate, *, timeout):
        workspace.scale_down()

    async def execute_activity(activity_fn, activity_request, **_kwargs):
        activities_run.append((activity_fn, activity_request))
        if activity_fn is crashable_activities.scale_workspace_workloads:
            workspace.delete()
            raise ActivityError(
                "deployment patch forbidden",
                scheduled_event_id=1,
                started_event_id=2,
                identity="registry-operator",
                activity_type="scale_workspace_workloads",
                activity_id="3",
                retry_state=None,
            )
        return SimpleNamespace(action="deleting")

    monkeypatch.setattr(crashable_workflow.workflow, "now", lambda: now)
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(
        crashable_workflow.workflow, "wait_condition", request_scale_down
    )
    monkeypatch.setattr(
        crashable_workflow.workflow, "execute_activity", execute_activity
    )
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "logger",
        SimpleNamespace(error=lambda *args: None),
    )

    asyncio.run(workspace.run(request))

    assert [activity_fn for activity_fn, _ in activities_run] == [
        crashable_activities.scale_workspace_workloads,
        crashable_activities.delete_workspace_namespace,
    ]
    assert workspace._status.phase == "Deleting"
    assert "deployment patch forbidden" in (workspace._status.last_error or "")


def test_crash_failure_does_not_prevent_requested_workspace_cleanup(monkeypatch):
    now = datetime(2026, 9, 8, 20, 0, tzinfo=UTC)
    request = EnsureWorkspaceRequest(
        demo_name="canonical-ai-demo",
        owner_email="alice@temporal.io",
        carried_already_provisioned=True,
        carried_expires_at=(now + timedelta(hours=1)).isoformat(),
        carried_created_at=now.isoformat(),
        carried_task_queue="music-0123abcd89",
        carried_workspace_id="0123abcd89",
    )
    workspace = CrashableWorkspaceWorkflow(request)
    activities_run = []
    logged = []

    async def request_crash(_predicate, *, timeout):
        workspace.crash()

    async def execute_activity(activity_fn, activity_request, **_kwargs):
        activities_run.append((activity_fn, activity_request))
        if activity_fn is crashable_activities.crash_workspace_pods:
            workspace.delete()
            raise ActivityError(
                "pod deletion forbidden",
                scheduled_event_id=1,
                started_event_id=2,
                identity="registry-operator",
                activity_type="crash_workspace_pods",
                activity_id="3",
                retry_state=None,
            )
        return SimpleNamespace(action="deleting")

    monkeypatch.setattr(crashable_workflow.workflow, "now", lambda: now)
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(
        crashable_workflow.workflow, "wait_condition", request_crash
    )
    monkeypatch.setattr(
        crashable_workflow.workflow, "execute_activity", execute_activity
    )
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "logger",
        SimpleNamespace(error=lambda *args: logged.append(args)),
    )

    asyncio.run(workspace.run(request))

    assert [activity_fn for activity_fn, _ in activities_run] == [
        crashable_activities.crash_workspace_pods,
        crashable_activities.delete_workspace_namespace,
    ]
    assert isinstance(activities_run[0][1], CrashWorkspacePodsRequest)
    assert isinstance(activities_run[1][1], DeleteWorkspaceNamespaceRequest)
    assert workspace._status.phase == "Deleting"
    assert "pod deletion forbidden" in (workspace._status.last_error or "")
    assert logged


# --- catalog flag ---------------------------------------------------------


def test_catalog_entry_marks_crashable_when_enabled_with_task_queue_env():
    entry = catalog_entry(_crashable_demo())
    assert entry["crashable"] == {"enabled": True}


def test_catalog_entry_crashable_requires_task_queue_env():
    # Opt-in isolation only: the button (catalog flag) appears only when the demo
    # declares BOTH enabled AND the task-queue env var used to isolate the clone,
    # matching the workflow's provision gate so a visible button never fails.
    demo = _crashable_demo()
    demo["spec"]["registry"]["crashable"] = {"enabled": True}  # no taskQueueEnv
    assert catalog_entry(demo)["crashable"] == {"enabled": False}

    demo["spec"]["registry"]["crashable"] = {"enabled": False, "taskQueueEnv": "TQ"}
    assert catalog_entry(demo)["crashable"] == {"enabled": False}


def test_catalog_entry_not_crashable_when_block_absent():
    entry = catalog_entry(project("plain", source={"repo": "x"}))
    assert entry["crashable"] == {"enabled": False}
