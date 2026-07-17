from __future__ import annotations

from demo_registry_operator import derive
from demo_registry_operator.crashable_workspace_resources import (
    build_workspace_manifests,
)
from demo_registry_operator.resources import catalog_entry


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
