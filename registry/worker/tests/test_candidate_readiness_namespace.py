from __future__ import annotations

import asyncio
from types import SimpleNamespace

import demo_registry_operator.workflows.demo_project_kubernetes_runtime as kubernetes_runtime_workflow
from demo_registry_operator.activities import reconcile


class _Cfg:
    readiness_timeout_seconds = 30
    readiness_poll_seconds = 1

    @staticmethod
    def from_env() -> "_Cfg":
        return _Cfg()


def _probe(monkeypatch, candidate: dict) -> str:
    """Run wait_for_candidate_readiness and capture the namespace it probes."""
    seen: dict[str, str] = {}
    monkeypatch.setattr(reconcile, "WorkerConfig", _Cfg)

    def fake_read(namespace, deployments):
        seen["ns"] = namespace
        return {c: {"name": n, "ready": True} for c, n in deployments.items()}

    monkeypatch.setattr(reconcile, "read_candidate_deployments", fake_read)
    result = reconcile.wait_for_candidate_readiness(
        reconcile.CandidateReadinessRequest(
            demo_project={"metadata": {"name": "operator-canary-mtls-abc"}},
            candidate=candidate,
        )
    )
    assert result.status["phase"] == "Ready", result.status
    return seen["ns"]


def test_readiness_probes_candidate_runtime_namespace(monkeypatch):
    # The active->candidate rollout deploys into a suffixed candidate namespace;
    # readiness must probe THAT, not the base project namespace (else it sees an
    # empty namespace, times out, and fails the gate even though pods are healthy).
    ns = _probe(
        monkeypatch,
        {
            "runtimeNamespace": "tmprl-dem-cld-operator-canary-mtls-abc-8dc6d",
            "deployments": {"worker": "worker-git-x"},
        },
    )
    assert ns == "tmprl-dem-cld-operator-canary-mtls-abc-8dc6d"


def test_readiness_falls_back_to_base_namespace_when_no_runtime_namespace(monkeypatch):
    # Control-plane projects deploy in place (no runtime-namespace rollout).
    ns = _probe(monkeypatch, {"deployments": {"worker": "worker-git-x"}})
    assert ns == reconcile.resources.project_namespace(
        {"metadata": {"name": "operator-canary-mtls-abc"}}
    )


def _probe_worker_rollout(monkeypatch, runtime_namespace: str | None) -> str:
    """Run the WorkerDeployment gate and capture the namespace it probes."""
    seen: dict[str, str] = {}
    monkeypatch.setattr(reconcile, "WorkerConfig", _Cfg)

    def fake_read(namespace, deployments):
        seen["ns"] = namespace
        return {
            component: {
                "name": name,
                "conditions": [{"type": "Ready", "status": "True"}],
            }
            for component, name in deployments.items()
        }

    monkeypatch.setattr(reconcile, "read_worker_deployments", fake_read)
    result = reconcile.wait_for_worker_deployment_rollouts(
        reconcile.WorkerDeploymentRolloutRequest(
            demo_project={"metadata": {"name": "operator-canary-mtls-abc"}},
            worker_deployments={"worker": "retrieval-v2"},
            runtime_namespace=runtime_namespace,
            timeout_seconds=1,
        )
    )
    assert result.status["phase"] == "Ready", result.status
    return seen["ns"]


def test_worker_rollout_probes_candidate_runtime_namespace(monkeypatch):
    ns = _probe_worker_rollout(
        monkeypatch,
        "tmprl-dem-cld-operator-canary-mtls-abc-8dc6d",
    )
    assert ns == "tmprl-dem-cld-operator-canary-mtls-abc-8dc6d"


def test_worker_rollout_falls_back_to_base_namespace(monkeypatch):
    ns = _probe_worker_rollout(monkeypatch, None)
    assert ns == reconcile.resources.project_namespace(
        {"metadata": {"name": "operator-canary-mtls-abc"}}
    )


def test_worker_rollout_waits_for_temporal_state_fetch_failure(monkeypatch):
    attempts = 0
    monkeypatch.setattr(reconcile, "WorkerConfig", _Cfg)
    monkeypatch.setattr(reconcile.time, "sleep", lambda _: None)
    monkeypatch.setattr(reconcile.activity, "heartbeat", lambda *args: None)

    def fake_read(namespace, deployments):
        nonlocal attempts
        attempts += 1
        condition = (
            {
                "type": "Ready",
                "status": "False",
                "reason": "TemporalStateFetchFailed",
                "message": 'Secret "temporal-worker-runtime" not found',
            }
            if attempts == 1
            else {"type": "Ready", "status": "True"}
        )
        return {
            "worker": {
                "name": "retrieval-v2",
                "namespace": namespace,
                "conditions": [condition],
            }
        }

    monkeypatch.setattr(reconcile, "read_worker_deployments", fake_read)
    result = reconcile.wait_for_worker_deployment_rollouts(
        reconcile.WorkerDeploymentRolloutRequest(
            demo_project={"metadata": {"name": "golden-retriever"}},
            worker_deployments={"worker": "retrieval-v2"},
            runtime_namespace="tmprl-dem-cld-golden-retriever-53d78",
            timeout_seconds=1,
        )
    )

    assert attempts == 2
    assert result.status["phase"] == "Ready", result.status


def test_worker_resources_wait_for_external_secrets_before_apply(monkeypatch):
    apply_calls: list[list[str]] = []
    secret_reads = 0
    monkeypatch.setattr(reconcile, "WorkerConfig", _Cfg)
    monkeypatch.setattr(reconcile.time, "sleep", lambda _: None)
    monkeypatch.setattr(reconcile.activity, "heartbeat", lambda *args: None)

    rendered = [
        {
            "apiVersion": "external-secrets.io/v1",
            "kind": "ExternalSecret",
            "metadata": {
                "name": "temporal-worker-runtime",
                "namespace": "candidate",
            },
        },
        {
            "apiVersion": "temporal.io/v1alpha1",
            "kind": "Connection",
            "metadata": {"name": "temporal", "namespace": "candidate"},
        },
        {
            "apiVersion": "temporal.io/v1alpha1",
            "kind": "WorkerDeployment",
            "metadata": {"name": "retrieval-v2", "namespace": "candidate"},
        },
    ]

    def fake_apply(resources, *, dry_run):
        assert dry_run is False
        apply_calls.append([resource["kind"] for resource in resources])
        return [
            {
                "apiVersion": resource["apiVersion"],
                "kind": resource["kind"],
                "name": resource["metadata"]["name"],
                "namespace": resource["metadata"]["namespace"],
                "action": "applied",
            }
            for resource in resources
        ]

    def fake_read_external_secret(namespace, name):
        nonlocal secret_reads
        secret_reads += 1
        ready = secret_reads > 1
        return {
            "name": name,
            "namespace": namespace,
            "phase": "Ready" if ready else "Waiting",
            "ready": ready,
        }

    monkeypatch.setattr(reconcile.kube, "apply_resources", fake_apply)
    monkeypatch.setattr(
        reconcile.kube,
        "read_external_secret_status",
        fake_read_external_secret,
    )

    applied = reconcile.apply_resources_with_worker_secret_barrier(rendered)

    assert apply_calls == [
        ["ExternalSecret"],
        ["Connection", "WorkerDeployment"],
    ]
    assert secret_reads == 2
    assert [resource["kind"] for resource in applied] == [
        "ExternalSecret",
        "Connection",
        "WorkerDeployment",
    ]


def test_worker_rollout_workflow_passes_candidate_runtime_namespace(monkeypatch):
    seen = {}

    async def fake_execute_activity(activity, request, **kwargs):
        seen["request"] = request
        return SimpleNamespace(status={"phase": "Ready", "ok": True})

    monkeypatch.setattr(
        kubernetes_runtime_workflow.workflow,
        "execute_activity",
        fake_execute_activity,
    )
    runtime = kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()
    result = asyncio.run(
        runtime._wait_for_worker_rollout(
            {"metadata": {"name": "operator-canary-mtls-abc"}},
            {
                "workerDeployments": {"worker": "retrieval-v2"},
                "workerDeploymentTimeoutSeconds": 30,
                "runtimeNamespace": "tmprl-dem-cld-operator-canary-mtls-abc-8dc6d",
            },
        )
    )

    assert result["phase"] == "Ready"
    assert (
        seen["request"].runtime_namespace
        == "tmprl-dem-cld-operator-canary-mtls-abc-8dc6d"
    )


def test_revision_runtime_namespace_teardown_uses_dependency_ordered_activity(
    monkeypatch,
):
    seen = {}

    async def fake_execute_activity(activity, request, **kwargs):
        seen["activity"] = activity
        seen["request"] = request
        return SimpleNamespace(action="deleted")

    monkeypatch.setattr(
        kubernetes_runtime_workflow.workflow,
        "execute_activity",
        fake_execute_activity,
    )
    runtime = kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()

    asyncio.run(
        runtime._teardown_revision_runtime_namespace(
            {
                "runtimeNamespaceEnabled": True,
                "runtimeNamespace": "tmprl-dem-cld-demo-old12",
            }
        )
    )

    assert seen["activity"] is reconcile.delete_project_namespace_resource
    assert seen["request"].namespace == "tmprl-dem-cld-demo-old12"
