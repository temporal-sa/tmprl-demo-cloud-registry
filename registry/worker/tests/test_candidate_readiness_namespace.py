from __future__ import annotations

import asyncio
from types import SimpleNamespace

import demo_registry_operator.workflows.demo_project_kubernetes_runtime as kubernetes_runtime_workflow
import pytest
from demo_registry_operator.activities import reconcile
from temporalio.exceptions import ActivityError, ApplicationError


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


@pytest.mark.parametrize(
    "reason",
    ["TemporalStateFetchFailed", "TemporalClientCreationFailed"],
)
def test_worker_rollout_waits_for_transient_temporal_failure(monkeypatch, reason):
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
                "reason": reason,
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


def test_worker_rollout_retries_failed_kubernetes_status_read(monkeypatch):
    reads = 0
    heartbeats = []
    monkeypatch.setattr(reconcile, "WorkerConfig", _Cfg)
    monkeypatch.setattr(reconcile, "time", SimpleNamespace(monotonic=lambda: 0, sleep=lambda _: None))
    monkeypatch.setattr(reconcile.activity, "heartbeat", lambda status: heartbeats.append(status))

    def read(namespace, name):
        nonlocal reads
        reads += 1
        if reads == 1:
            raise SystemError("Objects/dictobject.c:1882: bad argument to internal function")
        return {"name": name, "namespace": namespace, "conditions": [{"type": "Ready", "status": "True"}]}

    monkeypatch.setattr(reconcile.kube, "read_worker_deployment_status", read)
    result = reconcile.wait_for_worker_deployment_rollouts(
        reconcile.WorkerDeploymentRolloutRequest(
            demo_project={"metadata": {"name": "operator"}},
            worker_deployments={"worker": "operator-blue"}, timeout_seconds=2,
        )
    )
    assert reads == 2
    assert result.status["phase"] == "Ready"
    assert heartbeats[0]["workerDeployments"]["worker"]["phase"] == "Error"


def test_worker_rollout_status_read_failures_remain_time_bounded(monkeypatch):
    clock = iter([0, 1, 2])
    monkeypatch.setattr(reconcile, "WorkerConfig", _Cfg)
    monkeypatch.setattr(reconcile, "time", SimpleNamespace(monotonic=lambda: next(clock), sleep=lambda _: None))
    monkeypatch.setattr(reconcile.activity, "heartbeat", lambda *args: None)

    def read(namespace, name):
        raise SystemError("status read failed")

    monkeypatch.setattr(reconcile.kube, "read_worker_deployment_status", read)
    with pytest.raises(ApplicationError) as raised:
        reconcile.wait_for_worker_deployment_rollouts(
            reconcile.WorkerDeploymentRolloutRequest(
                demo_project={"metadata": {"name": "operator"}},
                worker_deployments={"worker": "operator-blue"}, timeout_seconds=2,
            )
        )
    status = raised.value.details[0]["status"]
    assert status["phase"] == "TimedOut"
    assert status["resources"][0]["message"] == "status read failed"


def test_worker_rollout_ignores_ready_condition_from_previous_generation(monkeypatch):
    reads = 0
    monkeypatch.setattr(reconcile, "WorkerConfig", _Cfg)
    monkeypatch.setattr(reconcile, "time", SimpleNamespace(monotonic=lambda: 0, sleep=lambda _: None))
    monkeypatch.setattr(reconcile.activity, "heartbeat", lambda *args: None)

    def fake_read(namespace, deployments):
        nonlocal reads
        reads += 1
        # First read: spec bumped to 123 but the controller has not observed it, so
        # the previous rollout's Ready=True is still present.
        observed = 122 if reads == 1 else 123
        return {
            "worker": {
                "name": "operator-blue",
                "namespace": namespace,
                "generation": 123,
                "conditions": [
                    {"type": "Ready", "status": "True", "observedGeneration": observed},
                ],
            }
        }

    monkeypatch.setattr(reconcile, "read_worker_deployments", fake_read)
    result = reconcile.wait_for_worker_deployment_rollouts(
        reconcile.WorkerDeploymentRolloutRequest(
            demo_project={"metadata": {"name": "registry-operator"}},
            worker_deployments={"worker": "operator-blue"},
            timeout_seconds=2,
        )
    )

    assert reads == 2
    assert result.status["phase"] == "Ready", result.status


def test_stale_failure_condition_does_not_fail_new_rollout():
    status = {
        "generation": 123,
        "conditions": [
            {
                "type": "Ready",
                "status": "False",
                "reason": "InvalidSpec",
                "observedGeneration": 122,
            },
        ],
    }
    assert reconcile.worker_deployment_phase(status) == "Waiting"


def test_actual_gate_failure_takes_precedence_over_status_read_error():
    aggregate = reconcile.aggregate_worker_deployment_status({
        "unreadable": {"phase": "Error", "message": "client read error"},
        "failed": {"targetVersion": {"testWorkflows": [{"status": "Failed"}]}},
    })
    assert aggregate["phase"] == "Failed"


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


def activity_error(message: str) -> ActivityError:
    return ActivityError(
        message,
        scheduled_event_id=1,
        started_event_id=2,
        identity="test-worker",
        activity_type="test_activity",
        activity_id="test-activity",
        retry_state=None,
    )


def test_worker_rollback_passes_previous_active_runtime_namespace(monkeypatch):
    seen = {}

    async def fake_execute_activity(activity, request, **kwargs):
        seen["request"] = request
        return SimpleNamespace(status={"phase": "RolledBack"})

    monkeypatch.setattr(
        kubernetes_runtime_workflow.workflow,
        "execute_activity",
        fake_execute_activity,
    )
    runtime = kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()
    runtime._external_resources = None

    result = asyncio.run(
        runtime._rollback_worker_deployments(
            {"metadata": {"name": "demo"}},
            {
                "activeImages": {"worker": "example/worker:git-old"},
                "activeImageVersion": "git-old",
                "activeRuntimeNamespace": "tmprl-dem-cld-demo-old12",
            },
        )
    )

    assert result == {"phase": "RolledBack"}
    assert seen["request"].active_runtime_namespace == (
        "tmprl-dem-cld-demo-old12"
    )


def test_failed_candidate_cleanup_continues_after_recovery_failures():
    events = []
    runtime = kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()

    async def capture_diagnostics(*args, **kwargs):
        events.append("capture-diagnostics")
        return {"phase": "Captured", "s3Uri": "s3://diagnostics/failure.json.gz"}

    async def fail_routes(*args, **kwargs):
        events.append("routes")
        raise activity_error("route rollback failed")

    async def fail_worker(*args, **kwargs):
        events.append("worker")
        raise activity_error("worker rollback failed")

    async def delete_candidate(*args, **kwargs):
        events.append("delete-candidate")
        return {"phase": "Deleted", "resources": [{"kind": "Namespace"}]}

    async def finalize_revision():
        events.append("finalize-revision")
        return {"phase": "Cleaned"}

    runtime._capture_failed_candidate_diagnostics = capture_diagnostics
    runtime._restore_previous_active_runtime_routes = fail_routes
    runtime._rollback_worker_deployments = fail_worker
    runtime._delete_failed_candidate_resources = delete_candidate
    runtime._finalize_failed_revision = finalize_revision

    result = asyncio.run(
        runtime._cleanup_failed_candidate(
            "demo",
            {"metadata": {"name": "demo"}},
            {"candidateRolloutStatus": {"imageVersion": "git-failed"}},
            {"activeImageVersion": "git-old"},
            failure_phase="WorkerRolloutFailed",
            failure_status={"workerDeploymentStatus": {"phase": "TimedOut"}},
            worker_rollout_failed=True,
        )
    )

    assert events == [
        "capture-diagnostics",
        "routes",
        "worker",
        "delete-candidate",
        "finalize-revision",
    ]
    assert result["rollbackStatus"]["phase"] == "Error"
    assert result["diagnosticsStatus"]["phase"] == "Captured"
    assert result["workerDeploymentRollbackStatus"]["phase"] == "Error"
    assert result["pruneStatus"]["phase"] == "Deleted"
    assert result["revisionCleanupStatus"]["phase"] == "Cleaned"


def test_failed_candidate_cleanup_retains_resources_when_diagnostics_fail():
    events = []
    runtime = kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()

    async def fail_diagnostics(*args, **kwargs):
        events.append("capture-diagnostics")
        raise activity_error("s3 upload failed")

    async def restore_routes(*args, **kwargs):
        events.append("routes")
        return {"phase": "Restored"}

    async def forbidden_delete(*args, **kwargs):
        raise AssertionError("candidate must be retained")

    async def forbidden_finalize():
        raise AssertionError("revision delete must be retained")

    async def retain_revision():
        events.append("retain-revision")
        return {"phase": "Retained", "reason": "DiagnosticsCaptureFailed"}

    runtime._capture_failed_candidate_diagnostics = fail_diagnostics
    runtime._restore_previous_active_runtime_routes = restore_routes
    runtime._delete_failed_candidate_resources = forbidden_delete
    runtime._finalize_failed_revision = forbidden_finalize
    runtime._retain_failed_revision_for_diagnostics = retain_revision

    result = asyncio.run(
        runtime._cleanup_failed_candidate(
            "demo",
            {"metadata": {"name": "demo"}},
            {"candidateRolloutStatus": {"imageVersion": "git-failed"}},
            {"activeImageVersion": "git-old"},
            failure_phase="CandidateNotReady",
            failure_status={"readinessStatus": {"phase": "TimedOut"}},
        )
    )

    assert events == ["capture-diagnostics", "routes", "retain-revision"]
    assert result["diagnosticsStatus"]["phase"] == "Error"
    assert result["rollbackStatus"]["phase"] == "Restored"
    assert result["pruneStatus"]["phase"] == "Retained"
    assert result["revisionCleanupStatus"]["phase"] == "Retained"
