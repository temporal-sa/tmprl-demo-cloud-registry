from __future__ import annotations

import os
from dataclasses import dataclass


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}
WATCH_MODE_ALWAYS = "always"
WATCH_MODE_BOOTSTRAP = "bootstrap"
WATCH_MODE_CURRENT = "current"
WATCH_MODE_DISABLED = "disabled"
DEFAULT_TEMPORAL_CLOUD_BOOTSTRAP_SECRET_PATH = (
    "tmprl-dem-cld/platform/temporal-cloud-bootstrap"
)
DEFAULT_TEMPORAL_CLOUD_API_VERSION = "v0.17.0"
DEFAULT_TEMPORAL_CLOUD_OPS_HOST = "saas-api.tmprl.cloud:443"
DEFAULT_CATALOG_STATUS_NAMESPACE = "tmprl-dem-cld-registry-operator"
DEFAULT_CATALOG_STATUS_CONFIGMAP = "registry-catalog-status"
DEFAULT_CATALOG_STATUS_MIRROR_NAMESPACE = "tmprl-dem-cld-registry-operator"
DEFAULT_SOURCE_WATCH_NAMESPACE = "tmprl-dem-cld-registry-operator"
DEFAULT_SOURCE_WATCH_INTERVAL = "1m"
DEFAULT_WATCH_HANDOFF_DEPLOYMENT_NAME = "registry-operator"
DEFAULT_PUBLIC_ECR_REGION = "us-east-1"


@dataclass(frozen=True)
class WorkerConfig:
    temporal_address: str
    temporal_namespace: str
    task_queue: str
    temporal_api_key: str | None
    temporal_tls: bool
    activity_workers: int
    worker_enabled: bool
    watch_enabled: bool
    watch_mode: str
    watch_handoff_deployment: str
    watch_interval_seconds: int
    external_resource_mode: str
    build_mode: str
    codebuild_project: str | None
    codebuild_poll_seconds: int
    codebuild_timeout_minutes: int
    readiness_timeout_seconds: int
    readiness_poll_seconds: int
    aws_region: str
    public_ecr_region: str
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
    source_watch_namespace: str
    source_watch_interval: str

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        source_watch_namespace = env_first(
            "DEMO_REGISTRY_SOURCE_WATCH_NAMESPACE",
            default=DEFAULT_SOURCE_WATCH_NAMESPACE,
        )
        watch_mode = env_watch_mode("DEMO_REGISTRY_WATCH_MODE", default=None)
        if watch_mode is None:
            watch_mode = env_watch_mode("DEMO_REGISTRY_WATCH_ENABLED")
        return cls(
            temporal_address=os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
            task_queue=os.getenv(
                "DEMO_REGISTRY_TASK_QUEUE",
                "registry-management",
            ),
            temporal_api_key=os.getenv("TEMPORAL_API_KEY"),
            temporal_tls=env_bool("TEMPORAL_TLS", default=False),
            activity_workers=env_int("DEMO_REGISTRY_ACTIVITY_WORKERS", default=20),
            worker_enabled=env_bool("DEMO_REGISTRY_WORKER_ENABLED", default=True),
            watch_enabled=watch_mode != WATCH_MODE_DISABLED,
            watch_mode=watch_mode,
            watch_handoff_deployment=env_first(
                "DEMO_REGISTRY_WATCH_HANDOFF_DEPLOYMENT",
                default=(
                    f"{source_watch_namespace}/"
                    f"{DEFAULT_WATCH_HANDOFF_DEPLOYMENT_NAME}"
                ),
            ),
            watch_interval_seconds=env_int(
                "DEMO_REGISTRY_WATCH_INTERVAL_SECONDS",
                default=15,
            ),
            external_resource_mode=env_choice(
                "DEMO_REGISTRY_EXTERNAL_RESOURCE_MODE",
                default="aws",
                choices={"aws"},
            ),
            build_mode=env_choice(
                "DEMO_REGISTRY_BUILD_MODE",
                default="aws-codebuild",
                choices={"aws-codebuild"},
            ),
            codebuild_project=env_optional("DEMO_REGISTRY_CODEBUILD_PROJECT"),
            codebuild_poll_seconds=env_int(
                "DEMO_REGISTRY_CODEBUILD_POLL_SECONDS",
                default=30,
            ),
            codebuild_timeout_minutes=env_int(
                "DEMO_REGISTRY_CODEBUILD_TIMEOUT_MINUTES",
                default=30,
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
            public_ecr_region=env_first(
                "DEMO_REGISTRY_PUBLIC_ECR_REGION",
                default=DEFAULT_PUBLIC_ECR_REGION,
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
                default="cloud-api",
                choices={"cloud-api"},
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
            source_watch_namespace=source_watch_namespace,
            source_watch_interval=env_first(
                "DEMO_REGISTRY_SOURCE_WATCH_INTERVAL",
                default=DEFAULT_SOURCE_WATCH_INTERVAL,
            ),
        )


def env_bool(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    parsed = value.strip().lower()
    if parsed in TRUE_VALUES:
        return True
    if parsed in FALSE_VALUES:
        return False
    return default


def env_watch_mode(
    name: str,
    *,
    default: str | None = WATCH_MODE_DISABLED,
) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    parsed = value.strip().lower()
    if parsed in TRUE_VALUES:
        return WATCH_MODE_ALWAYS
    if parsed in FALSE_VALUES or not parsed:
        return WATCH_MODE_DISABLED
    if parsed in {"auto", "current", "promoted"}:
        return WATCH_MODE_CURRENT
    if parsed in {"bootstrap", "handoff"}:
        return WATCH_MODE_BOOTSTRAP
    return WATCH_MODE_DISABLED


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
