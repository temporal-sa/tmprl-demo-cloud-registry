"""The Security team's worker. A separate process from Agent Gateway's.

Separate namespace, separate task queue, separate container, separate identity in
the Temporal UI. None of that is decoration: the claim CASE-2b makes is that the
pre-prod security scan is another team's independently durable system, and a class
name in someone else's worker would not be that.

Run it with:  python -m security_scan.worker
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os

import requests
from temporalio.worker import Worker

from security_scan import temporal_client, worker_power
from security_scan.security_scan_activities import (
    check_cve_database,
    check_license_compliance,
    generate_sbom,
    publish_scan_state,
    run_scan_check,
)
from security_scan.security_scan_workflow import SecurityScanWorkflow
from security_scan.nexus_handlers import SecurityScanServiceHandler
from security_scan.models import SECURITY_NAMESPACE, SECURITY_TASK_QUEUE
from security_scan.nexus_contracts import SECURITY_ENDPOINT

TASK_QUEUE = os.getenv("TASK_QUEUE", SECURITY_TASK_QUEUE)
REGISTRY_URL = os.getenv("SECURITY_SCAN_BACKEND_URL", "http://mock-tool:9000/invoke")
# How often this worker tells the shared platform it is here. The registration
# carries a TTL on the other side, so stopping this container makes the
# capability go away on its own rather than lingering as a stale advertisement.
HEARTBEAT_SECONDS = float(os.getenv("SECURITY_SCAN_HEARTBEAT_SECONDS", "5"))


async def _announce_forever() -> None:
    """Advertise pre-prod security scanning to the shared platform, on a heartbeat.

    This is how the Waypoint team's release pipeline discovers that the
    capability exists at all. Before the Security team onboarded them, the
    pipeline went from
    green quality gates straight to opening the production promotion, because
    there was nothing else to route through. It still does exactly that whenever
    this worker is not running. The pipeline is not configured for the scan; it
    looks to see whether the scan is there.
    """
    while True:
        # A scanner the demonstrator has taken offline stops advertising itself.
        # Its registration then ages out on the TTL below, and the gateway's
        # provider health check starts reporting the scanner unreachable --
        # which is the behaviour a release is supposed to stop on, and the
        # reason killing this worker is worth demonstrating at all.
        if worker_power.is_down_now(worker_power.WORKER_SECURITY):
            await asyncio.sleep(HEARTBEAT_SECONDS)
            continue
        try:
            await asyncio.to_thread(
                lambda: requests.post(
                    REGISTRY_URL,
                    json={
                        "tool_name": "register_security_scan_capability",
                        "arguments": {
                            "provider": "security",
                            # The endpoint, not the namespace or task queue
                            # behind it. Advertising those would hand callers
                            # exactly the coupling the endpoint exists to
                            # prevent.
                            "endpoint": SECURITY_ENDPOINT,
                            "ttl_seconds": HEARTBEAT_SECONDS * 4,
                        },
                    },
                    timeout=5,
                ).raise_for_status()
            )
        except Exception as err:  # noqa: BLE001 - advertising is best effort
            print(f"capability heartbeat failed: {err}", flush=True)
        await asyncio.sleep(HEARTBEAT_SECONDS)


async def main() -> None:
    client = await temporal_client.connect(SECURITY_NAMESPACE)
    heartbeat = asyncio.create_task(_announce_forever())
    # run_scan_check and publish_scan_state are synchronous, so they need a
    # thread pool; the two gateway-facing Activities are async and do not.
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:

        def make_worker() -> Worker:
            # Rebuilt on every restart: a Temporal Worker is single-use, so the
            # dashboard's kill switch needs a factory rather than an instance.
            return Worker(
                client,
                task_queue=TASK_QUEUE,
                workflows=[SecurityScanWorkflow],
                activities=[
                    run_scan_check,
                    publish_scan_state,
                    # The three sub-steps dependency_scan is actually made of.
                    generate_sbom,
                    check_cve_database,
                    check_license_compliance,
                ],
                # This team's Nexus front door. Agent Gateway reaches it through the
                # `security` Endpoint and never learns what is behind it.
                nexus_service_handlers=[SecurityScanServiceHandler()],
                activity_executor=executor,
                identity=f"security-scan-worker@{os.getenv('HOSTNAME', 'local')}",
            )

        print(
            f"security scan worker started, namespace {client.namespace!r}, "
            f"task queue {TASK_QUEUE!r}",
            flush=True,
        )
        try:
            await worker_power.supervise(
                worker_power.WORKER_SECURITY,
                make_worker,
                label="security scan worker",
            )
        finally:
            heartbeat.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
