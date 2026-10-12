from __future__ import annotations

import asyncio
import dataclasses
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from temporalio.api.enums.v1.batch_operation_pb2 import (
    BATCH_OPERATION_STATE_COMPLETED,
    BATCH_OPERATION_STATE_FAILED,
    BATCH_OPERATION_STATE_RUNNING,
)
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment

from demo_registry_operator import derive, resources
from demo_registry_operator.activities import (
    crashable_workspace as crashable_activities,
)
from demo_registry_operator.activities import reconcile
from demo_registry_operator.activities.external_resources import serverless_worker
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
    TerminateWorkspaceWorkflowsRequest,
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


def test_versioned_workspace_reuses_resolved_cloud_connection():
    demo = _versioned_crashable_demo()
    cloud_namespace = "tmprl-dem-cld-canonical-ai-demo.a2dd6"
    cloud_address = "us-west1.gcp.api.temporal.io:7233"
    demo["status"] = {
        "externalResources": {
            "temporalCloud": {
                "namespace": {
                    "name": cloud_namespace,
                    "grpcAddress": cloud_address,
                }
            }
        }
    }

    ordered, _, _, _, _ = build_workspace_manifests(
        demo,
        {"worker": "ecr/worker:sha", "app": "ecr/app:sha"},
        "0123abcd89",
    )

    connection = next(item for item in ordered if item["kind"] == "Connection")
    worker = next(item for item in ordered if item["kind"] == "WorkerDeployment")
    assert connection["spec"]["hostPort"] == cloud_address
    assert worker["spec"]["workerOptions"]["temporalNamespace"] == cloud_namespace


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


# --- terminate workspace workflows -----------------------------------------

WORKSPACE_QUEUE_QUERY = (
    "TaskQueue = 'music-0123abcd89' AND ExecutionStatus = 'Running'"
)


class _FakeWorkflowService:
    def __init__(self, states, *, start_error=None):
        self.states = list(states)
        self.start_error = start_error
        self.started = []
        self.described = []

    async def start_batch_operation(self, request, **_kwargs):
        self.started.append(request)
        if self.start_error is not None:
            raise self.start_error

    async def describe_batch_operation(self, request, **_kwargs):
        self.described.append(request.job_id)
        state = self.states.pop(0)
        if isinstance(state, Exception):
            raise state
        return SimpleNamespace(state=state, complete_operation_count=3)


class _FakeProjectClient:
    identity = "registry-operator"

    def __init__(self, counts, service):
        self.counts = list(counts)
        self.queries = []
        self.workflow_service = service

    async def count_workflows(self, query, **_kwargs):
        self.queries.append(query)
        return SimpleNamespace(count=self.counts.pop(0))


def _terminate_request(task_queue="music-0123abcd89"):
    return TerminateWorkspaceWorkflowsRequest(
        demo_name="canonical-ai-demo",
        workspace_id="0123abcd89",
        task_queue=task_queue,
    )


def _patch_project_namespace(monkeypatch, client, *, demo=None):
    connected = []

    async def connect(secret_path):
        connected.append(secret_path)
        return client, "demo-ns.acct"

    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: demo
        if demo is not None
        else {
            "metadata": {"name": name},
            "spec": {},
            "status": {"temporalRuntimeSecretPath": "demo/temporal-v2"},
        },
    )
    monkeypatch.setattr(
        serverless_worker,
        "connect_to_project_namespace_from_secret_path",
        connect,
    )
    monkeypatch.setattr(crashable_activities, "TERMINATE_POLL_SECONDS", 0)
    return connected


def test_workspace_task_queue_query_targets_only_the_workspace_queue():
    assert (
        crashable_activities.workspace_task_queue_query(
            "music-0123abcd89", "0123abcd89"
        )
        == WORKSPACE_QUEUE_QUERY
    )
    for task_queue, workspace_id in (
        ("music", "0123abcd89"),
        ("", "0123abcd89"),
        ("music-0123abcd89", ""),
        ("music' OR '1'='1-0123abcd89", "0123abcd89"),
    ):
        with pytest.raises(ApplicationError) as raised:
            crashable_activities.workspace_task_queue_query(task_queue, workspace_id)
        assert raised.value.non_retryable


def test_terminate_workspace_workflows_skips_batch_when_queue_is_empty(monkeypatch):
    service = _FakeWorkflowService([])
    client = _FakeProjectClient([0], service)
    connected = _patch_project_namespace(monkeypatch, client)

    result = asyncio.run(
        ActivityEnvironment().run(
            crashable_activities.terminate_workspace_workflows, _terminate_request()
        )
    )

    assert result.action == "none"
    assert connected == ["demo/temporal-v2"]
    assert client.queries == [WORKSPACE_QUEUE_QUERY]
    assert service.started == []


def test_terminate_workspace_workflows_batch_terminates_and_waits(monkeypatch):
    service = _FakeWorkflowService(
        [
            RPCError("not yet", RPCStatusCode.NOT_FOUND, b""),
            BATCH_OPERATION_STATE_RUNNING,
            BATCH_OPERATION_STATE_COMPLETED,
        ],
        start_error=RPCError("duplicate", RPCStatusCode.ALREADY_EXISTS, b""),
    )
    client = _FakeProjectClient([3, 1, 0], service)
    _patch_project_namespace(monkeypatch, client)
    heartbeats = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: heartbeats.append(details[0]["step"])

    result = asyncio.run(
        env.run(crashable_activities.terminate_workspace_workflows, _terminate_request())
    )

    assert result.action == "terminated"
    assert result.terminated == 3
    (started,) = service.started
    assert started.namespace == "demo-ns.acct"
    assert started.visibility_query == WORKSPACE_QUEUE_QUERY
    assert started.HasField("termination_operation")
    assert started.job_id == result.job_id
    assert "@" not in started.job_id
    assert service.described == [result.job_id] * 3
    assert "wait_visibility" in heartbeats


def test_terminate_workspace_workflows_fails_attempt_on_stragglers(monkeypatch):
    service = _FakeWorkflowService([BATCH_OPERATION_STATE_COMPLETED])
    client = _FakeProjectClient([2, 1, 1], service)
    _patch_project_namespace(monkeypatch, client)
    monkeypatch.setattr(crashable_activities, "TERMINATE_SETTLE_SECONDS", 0)

    with pytest.raises(RuntimeError, match="1 workflows still running"):
        asyncio.run(
            ActivityEnvironment().run(
                crashable_activities.terminate_workspace_workflows,
                _terminate_request(),
            )
        )


def test_terminate_workspace_workflows_raises_when_batch_fails(monkeypatch):
    service = _FakeWorkflowService([BATCH_OPERATION_STATE_FAILED])
    client = _FakeProjectClient([2], service)
    _patch_project_namespace(monkeypatch, client)

    with pytest.raises(RuntimeError, match="failed"):
        asyncio.run(
            ActivityEnvironment().run(
                crashable_activities.terminate_workspace_workflows,
                _terminate_request(),
            )
        )


def test_terminate_workspace_workflows_skips_missing_demo(monkeypatch):
    monkeypatch.setattr(
        crashable_activities.kube, "read_demo_project", lambda name: None
    )

    result = asyncio.run(
        ActivityEnvironment().run(
            crashable_activities.terminate_workspace_workflows, _terminate_request()
        )
    )

    assert result.action == "skipped"


def test_terminate_workspace_workflows_rejects_shared_queue(monkeypatch):
    def read_demo_project(name):
        raise AssertionError("must reject before touching the cluster")

    monkeypatch.setattr(
        crashable_activities.kube, "read_demo_project", read_demo_project
    )

    with pytest.raises(ApplicationError):
        asyncio.run(
            ActivityEnvironment().run(
                crashable_activities.terminate_workspace_workflows,
                _terminate_request(task_queue="music"),
            )
        )


def test_crash_workspace_pods_deletes_only_worker_pods(monkeypatch):
    deleted = []

    monkeypatch.setattr(
        crashable_activities.kube,
        "read_demo_project",
        lambda name: _crashable_demo(name),
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "list_pods",
        lambda namespace: [
            {"name": "labeled-worker-abc", "labels": {CRASHABLE_WORKER_LABEL: "true"}},
            {
                "name": "legacy-worker-def",
                "labels": {"app.kubernetes.io/component": "worker"},
            },
            {"name": "app-ghi", "labels": {"app.kubernetes.io/component": "app"}},
            {"name": "unlabeled-jkl", "labels": {}},
        ],
    )
    monkeypatch.setattr(
        crashable_activities.kube,
        "delete_pod",
        lambda namespace, name, *, grace_period_seconds: deleted.append(
            (namespace, name, grace_period_seconds)
        ),
    )

    result = crashable_activities.crash_workspace_pods(
        CrashWorkspacePodsRequest(
            namespace="tmprl-dem-cld-crash-0123abcd89",
            demo_name="canonical-ai-demo",
        )
    )

    assert result.deleted == 2
    assert deleted == [
        ("tmprl-dem-cld-crash-0123abcd89", "labeled-worker-abc", 0),
        ("tmprl-dem-cld-crash-0123abcd89", "legacy-worker-def", 0),
    ]


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
        crashable_activities.terminate_workspace_workflows,
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
        crashable_activities.terminate_workspace_workflows,
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
        crashable_activities.terminate_workspace_workflows,
        crashable_activities.delete_workspace_namespace,
    ]
    assert isinstance(activities_run[0][1], CrashWorkspacePodsRequest)
    assert activities_run[0][1].demo_name == "canonical-ai-demo"
    assert activities_run[1][1] == TerminateWorkspaceWorkflowsRequest(
        demo_name="canonical-ai-demo",
        workspace_id="0123abcd89",
        task_queue="music-0123abcd89",
    )
    assert isinstance(activities_run[2][1], DeleteWorkspaceNamespaceRequest)
    assert workspace._status.phase == "Deleting"
    assert "pod deletion forbidden" in (workspace._status.last_error or "")
    assert logged


def _carried_workspace_request():
    now = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
    return now, EnsureWorkspaceRequest(
        demo_name="canonical-ai-demo",
        owner_email="alice@temporal.io",
        carried_already_provisioned=True,
        carried_expires_at=(now + timedelta(hours=1)).isoformat(),
        carried_created_at=now.isoformat(),
        carried_task_queue="music-0123abcd89",
        carried_workspace_id="0123abcd89",
    )


def _run_teardown(monkeypatch, execute_activity):
    now, request = _carried_workspace_request()
    workspace = CrashableWorkspaceWorkflow(request)

    async def request_delete(_predicate, *, timeout):
        workspace.delete()

    monkeypatch.setattr(crashable_workflow.workflow, "now", lambda: now)
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(crashable_workflow.workflow, "wait_condition", request_delete)
    monkeypatch.setattr(
        crashable_workflow.workflow, "execute_activity", execute_activity
    )
    monkeypatch.setattr(
        crashable_workflow.workflow,
        "logger",
        SimpleNamespace(error=lambda *args: None),
    )
    asyncio.run(workspace.run(request))
    return workspace


def test_terminate_failure_does_not_prevent_workspace_deletion(monkeypatch):
    activities_run = []

    async def execute_activity(activity_fn, activity_request, **_kwargs):
        activities_run.append(activity_fn)
        if activity_fn is crashable_activities.terminate_workspace_workflows:
            raise ActivityError(
                "batch limit reached",
                scheduled_event_id=1,
                started_event_id=2,
                identity="registry-operator",
                activity_type="terminate_workspace_workflows",
                activity_id="3",
                retry_state=None,
            )
        return SimpleNamespace(action="deleting")

    workspace = _run_teardown(monkeypatch, execute_activity)

    assert activities_run == [
        crashable_activities.terminate_workspace_workflows,
        crashable_activities.delete_workspace_namespace,
    ]
    assert "Failed to terminate workspace workflows" in (
        workspace._status.last_error or ""
    )


def test_failed_provision_terminates_queue_workflows_before_namespace_delete(
    monkeypatch,
):
    request = EnsureWorkspaceRequest(
        demo_name="canonical-ai-demo",
        owner_email="alice@temporal.io",
    )
    workspace = CrashableWorkspaceWorkflow(request)
    calls = []

    async def execute_activity(activity_fn, activity_request, **_kwargs):
        calls.append(activity_fn)
        if activity_fn is crashable_activities.resolve_source_images:
            return SimpleNamespace(images={"app": "ecr/app:sha"})
        if activity_fn is crashable_activities.build_workspace_plan:
            return SimpleNamespace(
                resources=[],
                task_queue="music-0123abcd89",
                deployment_names=[],
                worker_deployment_names=[],
            )
        if activity_fn is crashable_activities.wait_workspace_ready:
            raise ActivityError(
                "pods never ready",
                scheduled_event_id=1,
                started_event_id=2,
                identity="registry-operator",
                activity_type="wait_workspace_ready",
                activity_id="3",
                retry_state=None,
            )
        return SimpleNamespace(action="ok")

    monkeypatch.setattr(
        crashable_workflow.workflow,
        "now",
        lambda: datetime(2026, 10, 11, tzinfo=UTC),
    )
    monkeypatch.setattr(crashable_workflow.workflow, "execute_activity", execute_activity)

    with pytest.raises(ActivityError):
        asyncio.run(workspace._provision(request))

    assert calls[-2:] == [
        crashable_activities.terminate_workspace_workflows,
        crashable_activities.delete_workspace_namespace,
    ]
    assert workspace._status.compensation_status == {
        "phase": "Completed",
        "failures": [],
    }
    assert "pods never ready" in (workspace._status.last_error or "")


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


# --- workspace share-link auth isolation ----------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]


def _auth_middlewares(manifests):
    return [
        manifest
        for manifest in manifests
        if manifest.get("kind") == "Middleware"
        and manifest["metadata"]["name"] == resources.AUTH_MIDDLEWARE_NAME
    ]


def test_workspace_auth_middleware_uses_workspace_verifier():
    ordered, _, _, _, _ = build_workspace_manifests(
        _crashable_demo(), {"worker": "i", "app": "i"}, "deadbeef00"
    )

    middlewares = _auth_middlewares(ordered)
    assert len(middlewares) == 1
    assert (
        middlewares[0]["spec"]["forwardAuth"]["address"]
        == resources.CATALOG_WORKSPACE_AUTH_VERIFY_URL
    )


@pytest.mark.parametrize(
    "path",
    sorted((REPO_ROOT / "projects").glob("**/*.yaml")),
    ids=lambda path: path.stem,
)
def test_always_on_demo_routes_never_use_workspace_verifier(path, monkeypatch):
    # Share links are only honored by the workspace verifier, so no always-on
    # demo render may ever reference it.
    monkeypatch.setenv("DEMO_REGISTRY_TEMPORAL_PROXY_IMAGE", "proxy-image")
    demo = yaml.safe_load(path.read_text())
    if demo.get("kind") != "DemoProject":
        pytest.skip("not a DemoProject")
    images = {name: "image" for name in resources.components(demo)}
    images.update({name: "image" for name in _dict_of(demo["spec"].get("builds"))})
    rendered = json.dumps(resources.runtime_resources(demo, images))

    assert "verify-workspace" not in rendered
    if resources.auth_required(demo) and resources.ingress_routes(demo):
        assert resources.CATALOG_AUTH_VERIFY_URL in rendered


def _dict_of(value):
    return value if isinstance(value, dict) else {}


def test_revoke_share_links_bumps_epoch_only_while_live():
    _, request = _carried_workspace_request()
    workspace = CrashableWorkspaceWorkflow(request)
    workspace._status.phase = "Active"

    assert workspace.revoke_share_links().share_epoch == 1
    assert workspace.revoke_share_links().share_epoch == 2

    workspace._status.phase = "Deleting"
    assert workspace.revoke_share_links().share_epoch == 2


def test_continue_as_new_carries_share_epoch(monkeypatch):
    now, request = _carried_workspace_request()
    request = dataclasses.replace(request, carried_share_epoch=3)
    workspace = CrashableWorkspaceWorkflow(request)
    assert workspace._status.share_epoch == 3
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

    with pytest.raises(ContinuedAsNew):
        asyncio.run(workspace.run(request))
    assert carried[0].carried_share_epoch == 3
