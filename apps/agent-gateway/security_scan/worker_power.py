"""Run a Temporal Worker under the dashboard's kill switch.

A demonstrator needs to take a Worker offline mid-story and show, in the
Temporal UI, that the work outlives it: pollers vanish from the task queue,
scheduled work sits there, and nothing is lost when the Worker comes back. That
only means anything if the outage is real, so this stops the actual Temporal
Worker rather than faking a status somewhere.

Polled rather than pushed. The alternative -- an HTTP control port on each
Worker -- would give an inbound listener, a Service and an exposed port to a
component whose entire job is to hold no inbound surface. These processes
already call the gateway for their MCP activities, so asking it a question on a
timer costs nothing new. The cost is latency: a kill lands on the next poll, a
second or two later, which no one watching can tell from instant.

The shutdown is graceful. An Activity already running finishes; the Worker stops
accepting new ones. That is a truer picture of a rolling deploy or an evicted pod
than SIGKILL would be, and it is the picture the demo is actually claiming.

NOTE: common/worker_power.py is a byte-identical copy. That duplication
is deliberate and enforced -- test_the_security_team_does_not_import_the_gateway
forbids security_scan/ importing common/, because the two teams' code sharing a
module is exactly the coupling the Nexus boundary exists to disprove. A third
shared package would satisfy the test while dissolving the thing it protects.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Callable, Optional

import requests

# The two keys the gateway's switch is keyed on. Literal strings on both sides
# of an HTTP boundary rather than a shared constant, for the same reason the
# rest of this module is duplicated: the Security team's process importing the
# gateway's code is the coupling this architecture exists to avoid.
WORKER_WAYPOINT = "waypoint"
WORKER_SECURITY = "security"

# Where to ask. Same host the MCP activities already call, so a Worker that can
# do its job can always answer this question too.
WORKER_POWER_URL = os.getenv("WORKER_POWER_URL", "http://gateway:8080/worker-power")
POLL_SECONDS = float(os.getenv("WORKER_POWER_POLL_SECONDS", "2"))


# Last known switch position, for anything in the process that is not the
# Worker itself but should still go quiet during an outage -- the Security
# team's capability heartbeat being the case that matters. A stopped scanner
# that keeps advertising itself as available is a lie the demo would then have
# to explain.
_DOWN_NOW: dict = {}


def is_down_now(worker_key: str) -> bool:
    return bool(_DOWN_NOW.get(worker_key, False))


async def _is_down(worker_key: str) -> Optional[bool]:
    """Should this Worker be offline? None when the gateway could not be asked.

    None and False are different answers and the caller treats them differently:
    an unreachable gateway must not take Workers down. A demo whose Workers all
    stop because the dashboard restarted would be worse than one with no kill
    switch at all.
    """
    try:
        resp = await asyncio.to_thread(requests.get, WORKER_POWER_URL, timeout=3)
        resp.raise_for_status()
        return bool(resp.json().get(worker_key, False))
    except Exception:
        return None


async def supervise(
    worker_key: str,
    make_worker: Callable[[], Any],
    label: str = "worker",
) -> None:
    """Own a Worker's lifetime, starting and stopping it as the switch moves.

    `make_worker` is called afresh for every start because a Temporal Worker is
    single-use: once shut down it cannot be run again, so a restart is a new
    instance on the same client and task queue.

    Runs forever. The process stays alive across an outage on purpose -- it is
    what makes the switch reversible from the dashboard, and it means a Worker
    that is "down" is down the way a stopped poller is, not the way a crashed
    pod is.
    """
    worker: Any = None
    task: Optional[asyncio.Task] = None
    stopped = False

    while True:
        down = await _is_down(worker_key)
        if down is not None:
            _DOWN_NOW[worker_key] = down

        if down is True and worker is not None:
            print(f"[{label}] kill switch: shutting down", flush=True)
            await worker.shutdown()
            if task is not None:
                await task
            worker, task, stopped = None, None, True
            print(f"[{label}] stopped; task queue has no poller", flush=True)

        elif down is False and worker is None:
            worker = make_worker()
            task = asyncio.create_task(worker.run())
            if stopped:
                print(f"[{label}] kill switch released: polling again", flush=True)
            stopped = False

        # A Worker that fell over on its own -- not via the switch -- would
        # otherwise leave this loop spinning against a dead task forever.
        if task is not None and task.done():
            exc = task.exception()
            worker, task = None, None
            if exc is not None:
                raise exc

        await asyncio.sleep(POLL_SECONDS)
