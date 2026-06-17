from __future__ import annotations

from typing import Any

from demo_registry_operator.activities.external_resources import (
    StageTemporalMtlsCaRequest,
    TemporalCloudProject,
    stage_temporal_mtls_ca,
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
    ) -> dict[str, Any]:
        captured["stageDemoProject"] = demo_project
        captured["stageSecretStore"] = secret_store
        captured["stageCheckedAt"] = checked_at
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
