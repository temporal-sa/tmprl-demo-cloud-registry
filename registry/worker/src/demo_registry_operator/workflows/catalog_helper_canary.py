"""The registry operator's rollout gate, as it runs on Ziggy's queue.

Ziggy's worker shares the operator's Worker Deployment Version, and the worker
controller starts the gate workflow (``RegistryOperatorCanaryWorkflow`` in
projects/operator/registry.yaml) on every task queue in the version. The full
end-to-end canary (operator_canary.py) runs on the operator's queue; on Ziggy's
queue this lightweight one under the same name only checks that the new
version's Ziggy worker runs workflows and activities. It takes no model call.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator.activities import catalog_helper as activities

# Long enough for a scaled-up pod to start polling; a version whose Ziggy
# worker never polls fails the gate instead of holding it open.
CHECK_TIMEOUT = timedelta(minutes=5)


@workflow.defn(name="RegistryOperatorCanaryWorkflow")
class CatalogHelperCanaryWorkflow:
    """Gate check for Ziggy's queue."""

    @workflow.run
    async def run(self, request: dict[str, Any] | None = None) -> dict[str, str]:
        # The gate input is the operator canary's run policy; this check
        # doesn't use it.
        del request
        await workflow.execute_activity(
            activities.check_catalog_helper_worker,
            schedule_to_close_timeout=CHECK_TIMEOUT,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(seconds=2)),
        )
        return {"phase": "Succeeded"}
