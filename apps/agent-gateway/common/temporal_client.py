"""One way to reach Temporal, whether that is a dev server or Temporal Cloud.

The difference between the two is entirely in the environment, so no caller has to
know which one it is talking to:

  TEMPORAL_ADDRESS    host:port. `localhost:7233` for the dev server; on the demo
                      cloud platform the operator injects the Cloud gRPC endpoint.
  TEMPORAL_NAMESPACE  the namespace this process works in. On Cloud it arrives
                      fully qualified (`<name>.<account>`), and it is injected
                      per component -- which is what keeps the gateway's and the
                      Security team's credentials apart without either process
                      knowing the other exists.
  TEMPORAL_API_KEY    absent on a dev server, present on Cloud.
  TEMPORAL_TLS        "true" on Cloud. Unset locally.

The `default_namespace` argument is the value each process used before any of this
existed, and it is still what a bare `docker compose up` gets. It is a fallback,
not an override: an injected TEMPORAL_NAMESPACE always wins, because on Cloud the
namespace is not something this code is entitled to choose.

security_scan/temporal_client.py holds the same functions, and that duplication is
deliberate. The Security team's package imports nothing from `common/`
(tests/test_security_scan_contract.py enforces it), and a shared third package
between the two teams would satisfy the test while quietly dissolving the boundary
the test exists to protect. Two teams reading the same four environment variables
is not shared code; it is a shared platform convention, and conventions are the
legitimate thing to state twice -- the same argument the two nexus_contracts.py
files already make.
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
    when api_key is provided, otherwise TLS is disabled". That is exactly the rule
    this demo wants -- plaintext against a local dev server, TLS against Cloud --
    so the one helper serves both without branching on which one it is.
    """
    raw = os.getenv("TEMPORAL_TLS", "").strip().lower()
    return raw in _TRUTHY if raw else None


async def connect(default_namespace: str, **kwargs: Any) -> Client:
    """Connect to Temporal, reading everything but the fallback namespace from env.

    **kwargs passes through to Client.connect, which is what lets worker.py hand
    in its Google ADK plugin without this helper knowing anything about ADK.
    """
    return await Client.connect(
        address(),
        namespace=namespace(default_namespace),
        api_key=api_key(),
        tls=tls(),
        **kwargs,
    )
