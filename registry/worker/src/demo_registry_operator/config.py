from __future__ import annotations

import os
from dataclasses import dataclass


TRUE_VALUES = {"1", "true", "yes", "on"}
DEFAULT_TEMPORAL_CLOUD_BOOTSTRAP_SECRET_PATH = (
    "tmprl-cloud-demo/platform/temporal-cloud-bootstrap"
)
DEFAULT_TEMPORAL_CLOUD_API_VERSION = "v0.17.0"
DEFAULT_TEMPORAL_CLOUD_OPS_HOST = "saas-api.tmprl.cloud:443"
DEFAULT_CATALOG_STATUS_NAMESPACE = "demo-registry-system"
DEFAULT_CATALOG_STATUS_CONFIGMAP = "demo-registry-catalog-status"
DEFAULT_CATALOG_STATUS_MIRROR_NAMESPACE = "tmprl-cloud-demo-demo-registry"


@dataclass(frozen=True)
class WorkerConfig:
    temporal_address: str
    temporal_namespace: str
    task_queue: str
    temporal_api_key: str | None
    temporal_tls: bool
    activity_workers: int
    watch_enabled: bool
    watch_interval_seconds: int
    reconcile_mode: str
    external_resource_mode: str
    build_mode: str
    build_workspace: str
    codebuild_project: str | None
    codebuild_poll_seconds: int
    codebuild_timeout_minutes: int
    readiness_timeout_seconds: int
    readiness_poll_seconds: int
    aws_region: str
    aws_account_id: str | None
    temporal_cloud_bootstrap_secret_path: str
    temporal_cloud_account_id: str | None
    temporal_cloud_region: str | None
    temporal_cloud_mode: str
    temporal_cloud_api_version: str
    temporal_cloud_ops_host: str
    catalog_status_namespace: str
    catalog_status_configmap: str
    catalog_status_mirror_namespace: str

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        return cls(
            temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
            task_queue=os.getenv(
                "DEMO_REGISTRY_TASK_QUEUE",
                "demo-registry-management",
            ),
            temporal_api_key=os.getenv("TEMPORAL_API_KEY"),
            temporal_tls=env_bool("TEMPORAL_TLS", default=False),
            activity_workers=env_int("DEMO_REGISTRY_ACTIVITY_WORKERS", default=20),
            watch_enabled=env_bool("DEMO_REGISTRY_WATCH_ENABLED", default=False),
            watch_interval_seconds=env_int(
                "DEMO_REGISTRY_WATCH_INTERVAL_SECONDS",
                default=15,
            ),
            reconcile_mode=env_choice(
                "DEMO_REGISTRY_RECONCILE_MODE",
                default="plan",
                choices={"plan", "apply"},
            ),
            external_resource_mode=env_choice(
                "DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE",
                default="plan",
                choices={"plan", "aws"},
            ),
            build_mode=env_choice(
                "DEMO_REGISTRY_BUILD_MODE",
                default="plan",
                choices={"plan", "local-docker", "aws-codebuild"},
            ),
            build_workspace=env_first(
                "DEMO_REGISTRY_BUILD_WORKSPACE",
                default="/tmp/demo-registry-builds",
            ),
            codebuild_project=env_optional("DEMO_REGISTRY_CODEBUILD_PROJECT"),
            codebuild_poll_seconds=env_int(
                "DEMO_REGISTRY_CODEBUILD_POLL_SECONDS",
                default=15,
            ),
            codebuild_timeout_minutes=env_int(
                "DEMO_REGISTRY_CODEBUILD_TIMEOUT_MINUTES",
                default=60,
            ),
            readiness_timeout_seconds=env_int(
                "DEMO_REGISTRY_READINESS_TIMEOUT_SECONDS",
                default=300,
            ),
            readiness_poll_seconds=env_int(
                "DEMO_REGISTRY_READINESS_POLL_SECONDS",
                default=5,
            ),
            aws_region=env_first(
                "DEMO_REGISTRY_AWS_REGION",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
                default="us-west-1",
            ),
            aws_account_id=env_optional("DEMO_REGISTRY_AWS_ACCOUNT_ID"),
            temporal_cloud_bootstrap_secret_path=env_first(
                "DEMO_REGISTRY_TEMPORAL_CLOUD_BOOTSTRAP_SECRET_PATH",
                default=DEFAULT_TEMPORAL_CLOUD_BOOTSTRAP_SECRET_PATH,
            ),
            temporal_cloud_account_id=env_first_optional(
                "DEMO_REGISTRY_TEMPORAL_CLOUD_ACCOUNT_ID",
                "TEMPORAL_CLOUD_ACCOUNT_ID",
            ),
            temporal_cloud_region=env_first_optional(
                "DEMO_REGISTRY_TEMPORAL_CLOUD_REGION",
                "TEMPORAL_CLOUD_REGION",
            ),
            temporal_cloud_mode=env_choice(
                "DEMO_REGISTRY_TEMPORAL_CLOUD_MODE",
                default="manual",
                choices={"manual", "tcld-verify", "cloud-api"},
            ),
            temporal_cloud_api_version=env_first(
                "DEMO_REGISTRY_TEMPORAL_CLOUD_API_VERSION",
                default=DEFAULT_TEMPORAL_CLOUD_API_VERSION,
            ),
            temporal_cloud_ops_host=env_first(
                "DEMO_REGISTRY_TEMPORAL_CLOUD_OPS_HOST",
                default=DEFAULT_TEMPORAL_CLOUD_OPS_HOST,
            ),
            catalog_status_namespace=env_first(
                "DEMO_REGISTRY_CATALOG_STATUS_NAMESPACE",
                default=DEFAULT_CATALOG_STATUS_NAMESPACE,
            ),
            catalog_status_configmap=env_first(
                "DEMO_REGISTRY_CATALOG_STATUS_CONFIGMAP",
                default=DEFAULT_CATALOG_STATUS_CONFIGMAP,
            ),
            catalog_status_mirror_namespace=env_first(
                "DEMO_REGISTRY_CATALOG_STATUS_MIRROR_NAMESPACE",
                default=DEFAULT_CATALOG_STATUS_MIRROR_NAMESPACE,
            ),
        )


def env_bool(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in TRUE_VALUES


def env_int(name: str, *, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def env_choice(name: str, *, default: str, choices: set[str]) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    parsed = value.strip().lower()
    return parsed if parsed in choices else default


def env_first(*names: str, default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return default


def env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    parsed = value.strip()
    return parsed or None


def env_first_optional(*names: str) -> str | None:
    for name in names:
        value = env_optional(name)
        if value:
            return value
    return None
