"""The Security team's own way to reach Temporal, dev server or Temporal Cloud.

Four environment variables, the same four every process on this platform reads:

  TEMPORAL_ADDRESS    host:port. `localhost:7233` for the dev server; on the demo
                      cloud platform the operator injects the Cloud gRPC endpoint.
  TEMPORAL_NAMESPACE  the namespace this worker polls. On Cloud it arrives fully
                      qualified (`<name>.<account>`) and it is *this team's*
                      namespace, mounted from this team's own credential. The
                      worker next door reads the same variable name and gets a
                      different value, which is the whole point.
  TEMPORAL_API_KEY    absent on a dev server, present on Cloud.
  TEMPORAL_TLS        "true" on Cloud. Unset locally.

The `default_namespace` argument is what a bare `docker compose up` gets. It is a
fallback, not an override: an injected TEMPORAL_NAMESPACE always wins.

This file duplicates common/temporal_client.py on purpose. Nothing in this package
may import from `common/` -- tests/test_security_scan_contract.py enforces that,
because two teams that share a Python package are not two teams. A shared third
package would pass the import test while dissolving the boundary it protects, so
the convention is stated on both sides instead, exactly as the two
nexus_contracts.py files already state the wire contract twice.
"""

from __future__ import annotations

import os
from typing import Any

from temporalio.client import Client

# Where `temporal server start-dev` listens. Only ever reached when nothing has
# injected an address, which in practice means somebody's laptop.
DEV_SERVER_ADDRESS = "localhost:7233"

_TRUTHY = {"1", "true", "yes", "on"}


def address() -> str:
    return os.getenv("TEMPORAL_ADDRESS", "").strip() or DEV_SERVER_ADDRESS


def namespace(default: str) -> str:
    return os.getenv("TEMPORAL_NAMESPACE", "").strip() or default


def api_key() -> str | None:
    return os.getenv("TEMPORAL_API_KEY", "").strip() or None


def tls() -> bool | None:
    """Whether to use TLS, or None to let the SDK decide.

    None is the right answer when nothing has an opinion, and it is not a fudge:
    `Client.connect` documents `tls=None` as "TLS will be enabled automatically
    when api_key is provided, otherwise TLS is disabled". Plaintext against a
    local dev server, TLS against Cloud, without branching on which one it is.
    """
    raw = os.getenv("TEMPORAL_TLS", "").strip().lower()
    return raw in _TRUTHY if raw else None


async def connect(default_namespace: str, **kwargs: Any) -> Client:
    """Connect to Temporal, reading everything but the fallback namespace from env."""
    return await Client.connect(
        address(),
        namespace=namespace(default_namespace),
        api_key=api_key(),
        tls=tls(),
        **kwargs,
    )
