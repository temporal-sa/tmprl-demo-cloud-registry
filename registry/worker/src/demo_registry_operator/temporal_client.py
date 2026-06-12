from __future__ import annotations

from collections.abc import Mapping

from temporalio.client import Client

from demo_registry_operator.config import WorkerConfig


async def connect(config: WorkerConfig) -> Client:
    rpc_metadata: Mapping[str, str] | None = None
    if config.temporal_api_key:
        rpc_metadata = {"authorization": f"Bearer {config.temporal_api_key}"}

    return await Client.connect(
        config.temporal_address,
        namespace=config.temporal_namespace,
        tls=config.temporal_tls,
        rpc_metadata=rpc_metadata,
    )
