from __future__ import annotations

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
