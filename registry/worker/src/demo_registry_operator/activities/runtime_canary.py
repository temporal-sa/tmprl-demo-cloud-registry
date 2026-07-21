"""Process-wide Temporal Runtime for disposable credential canary workers."""

from __future__ import annotations

import threading

from temporalio.runtime import Runtime, TelemetryConfig


_runtime: Runtime | None = None
_runtime_lock = threading.Lock()


def get_runtime() -> Runtime:
    """Return a shared Runtime with Worker Heartbeating disabled.

    Runtime credential canaries create short-lived Clients for credentials and
    namespaces that may be revoked immediately after validation. Disabling
    Worker Heartbeating prevents SDK Core from adding a hidden Nexus command
    poller whose lifetime can extend beyond the canary Worker.
    """

    global _runtime
    if _runtime is None:
        with _runtime_lock:
            if _runtime is None:
                _runtime = Runtime(
                    telemetry=TelemetryConfig(),
                    worker_heartbeat_interval=None,
                )
    return _runtime
