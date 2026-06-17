from __future__ import annotations

from demo_registry_operator.config import (
    WATCH_MODE_BOOTSTRAP,
    WATCH_MODE_CURRENT,
    WorkerConfig,
)


def test_codebuild_timer_defaults_match_heartbeat_policy(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CODEBUILD_POLL_SECONDS", raising=False)
    monkeypatch.delenv("DEMO_REGISTRY_CODEBUILD_TIMEOUT_MINUTES", raising=False)

    config = WorkerConfig.from_env()

    assert config.codebuild_poll_seconds == 30
    assert config.codebuild_timeout_minutes == 30


def test_public_ecr_region_defaults_to_global_region(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_PUBLIC_ECR_REGION", raising=False)

    config = WorkerConfig.from_env()

    assert config.public_ecr_region == "us-east-1"


def test_local_docker_build_mode_is_no_longer_accepted(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_BUILD_MODE", "local-docker")

    config = WorkerConfig.from_env()

    assert config.build_mode == "aws-codebuild"


def test_source_watch_defaults(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_SOURCE_WATCH_NAMESPACE", raising=False)
    monkeypatch.delenv("DEMO_REGISTRY_SOURCE_WATCH_INTERVAL", raising=False)
    monkeypatch.delenv("DEMO_REGISTRY_CATALOG_STATUS_MIRROR_NAMESPACE", raising=False)

    config = WorkerConfig.from_env()

    assert config.source_watch_namespace == "tmprl-dem-cld-registry-operator"
    assert config.source_watch_interval == "1m"
    assert config.catalog_status_mirror_namespace == "tmprl-dem-cld-registry-operator"


def test_worker_enabled_default_and_override(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_WORKER_ENABLED", raising=False)
    assert WorkerConfig.from_env().worker_enabled is True

    monkeypatch.setenv("DEMO_REGISTRY_WORKER_ENABLED", "false")
    assert WorkerConfig.from_env().worker_enabled is False


def test_watch_enabled_auto_uses_current_worker_mode(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_WATCH_ENABLED", "auto")
    monkeypatch.setenv(
        "DEMO_REGISTRY_SOURCE_WATCH_NAMESPACE",
        "tmprl-dem-cld-registry-operator",
    )

    config = WorkerConfig.from_env()

    assert config.watch_enabled is True
    assert config.watch_mode == WATCH_MODE_CURRENT
    assert config.watch_handoff_deployment == (
        "tmprl-dem-cld-registry-operator/registry-operator"
    )


def test_watch_mode_overrides_legacy_watch_enabled(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_WATCH_ENABLED", "true")
    monkeypatch.setenv("DEMO_REGISTRY_WATCH_MODE", "bootstrap")

    config = WorkerConfig.from_env()

    assert config.watch_enabled is True
    assert config.watch_mode == WATCH_MODE_BOOTSTRAP
