from __future__ import annotations

import os
from dataclasses import dataclass


TRUE_VALUES = {"1", "true", "yes", "on"}


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
    aws_region: str
    aws_account_id: str | None

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
            aws_region=env_first(
                "DEMO_REGISTRY_AWS_REGION",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
                default="us-west-1",
            ),
            aws_account_id=env_optional("DEMO_REGISTRY_AWS_ACCOUNT_ID"),
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
