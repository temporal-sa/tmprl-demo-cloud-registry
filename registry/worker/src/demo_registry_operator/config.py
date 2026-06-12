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
