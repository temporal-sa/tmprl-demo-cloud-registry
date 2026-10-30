from __future__ import annotations

from collections.abc import Mapping

from temporal_agent_harness.plugin import AgentHarnessPlugin
from temporalio.client import Client
from temporalio.service import DnsLoadBalancingConfig

from demo_registry_operator.config import WorkerConfig

# Periodically re-resolve the Temporal Cloud endpoint. The gRPC client's DNS
# resolver can get poisoned under polling load and never re-resolve on its own,
# leaving the worker Running but unable to poll ("dns error: failed to lookup
# address information: Name or service not known") until a pod restart — even
# though the node's OS DNS is fine. Core's DNS load balancing forces a fresh
# lookup on this interval, which recovers a stuck resolver in place. Off by
# default in the SDK; safe here because the operator connects directly (no HTTP
# CONNECT proxy, which would silently disable it). The worker liveness probe
# stays as the backstop for any failure this doesn't cover.
TEMPORAL_DNS_RESOLUTION_INTERVAL_MS = 15000


async def connect(config: WorkerConfig) -> Client:
    # Newer temporalio clients reject None here.
    rpc_metadata: Mapping[str, str] = {}
    if config.temporal_api_key:
        rpc_metadata = {"authorization": f"Bearer {config.temporal_api_key}"}

    return await Client.connect(
        config.temporal_address,
        namespace=config.temporal_namespace,
        tls=config.temporal_tls,
        rpc_metadata=rpc_metadata,
        dns_load_balancing_config=DnsLoadBalancingConfig(
            resolution_interval_millis=TEMPORAL_DNS_RESOLUTION_INTERVAL_MS
        ),
        # NB: gzip compression (the 1.29 default) is intentionally left ON — it
        # compresses workflow histories/payloads and normal polling handles it
        # fine. The liveness heartbeat avoids service_client.check_health(), which
        # can't decompress gzip responses; see worker.liveness_heartbeat.
    )


def agent_harness_client(client: Client) -> Client:
    """The same connection, configured for agent-harness workflows (Ziggy).

    AgentHarnessPlugin installs a Pydantic data converter, which the operator's
    other workflows must not get, hence a separate client. Large-payload
    offload is off: its default store is the local filesystem, which the catalog
    web server (another pod) can't read, and Ziggy's payloads are small.
    """

    config = client.config()
    config["plugins"] = [
        *config.get("plugins", []),
        AgentHarnessPlugin(large_payload_offload=None),
    ]
    return Client(**config)
