from __future__ import annotations

from typing import Any

from demo_registry_operator.activities.external_resources import (
    StageTemporalMtlsCaRequest,
    TemporalCloudProject,
    stage_temporal_mtls_ca,
)
from demo_registry_operator.activities.external_resources.planning import (
    temporal_cloud_project,
)
from demo_registry_operator.activities.external_resources import temporal
from demo_registry_operator.activities import temporal_cloud


PROJECT = TemporalCloudProject(
    name="banking-money-transfer",
    namespace="tmprl-dem-cld-banking-money-transfer.acct1",
    kubernetes_namespace="tmprl-dem-cld-banking-money-transfer",
    auth_mode="mtls",
    components={"worker": {"build": "app", "worker": True}},
    runtime_secret_path="tmprl-dem-cld/banking-money-transfer/temporal",
    mtls_ca_secret_path="tmprl-dem-cld/banking-money-transfer/temporal/ca",
)


def test_stage_temporal_mtls_ca_uses_explicit_dataclass_request_without_bootstrap(
    monkeypatch,
):
    store = object()
    captured: dict[str, Any] = {}

    def fake_stage_temporal_mtls_ca(
        demo_project: dict[str, Any],
        *,
        secret_store: object,
        checked_at: str,
        candidate_secret_token: str | None = None,
    ) -> dict[str, Any]:
        captured["stageDemoProject"] = demo_project
        captured["stageSecretStore"] = secret_store
        captured["stageCheckedAt"] = checked_at
        captured["stageCandidateSecretToken"] = candidate_secret_token
        return {"phase": "Ready"}

    monkeypatch.setattr(temporal, "temporal_secret_store", lambda: store)
    monkeypatch.setattr(temporal, "platform_manual_setup_required", lambda _project: False)
    monkeypatch.setattr(
        temporal_cloud,
        "stage_temporal_mtls_ca",
        fake_stage_temporal_mtls_ca,
    )

    result = stage_temporal_mtls_ca(
        StageTemporalMtlsCaRequest(
            checked_at="2026-06-17T00:00:00+00:00",
            project=PROJECT,
        )
    )

    assert result.status == {"phase": "Ready"}
    assert captured["stageDemoProject"] == PROJECT.as_demo_project()
    assert captured["stageSecretStore"] is store
    assert captured["stageCheckedAt"] == "2026-06-17T00:00:00+00:00"
    assert captured["stageCandidateSecretToken"] is None


def test_temporal_cloud_project_as_demo_project_preserves_runtime_secret_path():
    demo_project = PROJECT.as_demo_project()

    assert demo_project["spec"]["temporal"]["runtimeSecretPath"] == (
        "tmprl-dem-cld/banking-money-transfer/temporal"
    )


def test_temporal_cloud_project_plan_uses_explicit_runtime_secret_path():
    project = {
        "metadata": {
            "name": "dejavu-tacos",
        },
        "spec": {
            "temporal": {
                "runtimeSecretPath": "tmprl-dem-cld/dejavu-tacos/temporal-owned"
            },
            "components": {"worker": {"build": "worker", "worker": True}},
        },
    }

    temporal_project = temporal_cloud_project(project)
    demo_project = temporal_project.as_demo_project()

    assert temporal_project.runtime_secret_path == (
        "tmprl-dem-cld/dejavu-tacos/temporal-owned"
    )
    assert "uid" not in demo_project["metadata"]
    assert demo_project["spec"]["temporal"]["runtimeSecretPath"] == (
        "tmprl-dem-cld/dejavu-tacos/temporal-owned"
    )


def test_temporal_cloud_project_carries_region_and_serverless():
    project = {
        "metadata": {"name": "money-transfer"},
        "spec": {
            "temporal": {"region": "aws-ca-central-1"},
            "components": {
                "worker": {
                    "build": "worker",
                    "worker": True,
                    "serverless": {"provider": "aws-lambda"},
                }
            },
        },
    }

    temporal_project = temporal_cloud_project(project)

    assert temporal_project.region == "aws-ca-central-1"
    assert temporal_project.serverless is True
    assert (
        temporal_project.as_demo_project()["spec"]["temporal"]["region"]
        == "aws-ca-central-1"
    )


def test_temporal_cloud_project_region_omitted_is_none_and_not_serialized():
    project = {
        "metadata": {"name": "dejavu-tacos"},
        "spec": {"components": {"worker": {"build": "worker", "worker": True}}},
    }

    temporal_project = temporal_cloud_project(project)

    assert temporal_project.region is None
    assert temporal_project.serverless is False
    assert "region" not in temporal_project.as_demo_project()["spec"]["temporal"]


def test_namespace_result_carries_region():
    result = temporal.namespace_result(
        {
            "phase": "Ready",
            "namespace": {
                "name": "ns.acct1",
                "grpcAddress": "ns.acct1.tmprl.cloud:7233",
                "authMode": "apiKey",
                "region": "aws-ca-central-1",
            },
        }
    )

    assert result.region == "aws-ca-central-1"
