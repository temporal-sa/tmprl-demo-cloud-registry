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


def test_temporal_cloud_project_plan_preserves_live_uid():
    project = {
        "metadata": {
            "name": "dejavu-tacos",
            "uid": "df1a4cee-d539-4009-999c-9fcc547f9c10",
        },
        "spec": {
            "components": {"worker": {"build": "worker", "worker": True}},
        },
    }

    temporal_project = temporal_cloud_project(project)
    demo_project = temporal_project.as_demo_project()

    assert temporal_project.uid == "df1a4cee-d539-4009-999c-9fcc547f9c10"
    assert demo_project["metadata"]["uid"] == "df1a4cee-d539-4009-999c-9fcc547f9c10"
    assert demo_project["spec"]["temporal"]["runtimeSecretPath"].startswith(
        "tmprl-dem-cld/dejavu-tacos/temporal-"
    )
