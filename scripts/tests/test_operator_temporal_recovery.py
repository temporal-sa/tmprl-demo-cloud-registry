"""History contracts requiring the guarded, owned local Temporal runner."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from demo_registry_operator.activities import reconcile
from demo_registry_operator.workflows import messages
from test_rollout_failure_history import ProjectFailureHistoryWorkflow


def test_pending_reconcile_survives_worker_restart_without_repeating_compensation():
    """Exercise the real entity loop across a stopped worker and queued signal."""
    attempts = 0
    compensations = 0

    @activity.defn(name="test_provision")
    async def provision():
        nonlocal attempts
        attempts += 1
        raise ApplicationError(f"attempt {attempts} denied", type="ProvisionDenied", non_retryable=True)

    @activity.defn(name="test_compensate")
    async def compensate():
        nonlocal compensations
        compensations += 1

    @activity.defn(name="record_demo_project_status")
    async def record_project(request: reconcile.RecordDemoProjectStatusRequest) -> reconcile.DemoProjectStatusResult:
        return reconcile.DemoProjectStatusResult(request.status)

    @activity.defn(name="record_catalog_project_status")
    async def record_catalog(request: reconcile.RecordDemoProjectStatusRequest) -> reconcile.DemoProjectStatusResult:
        return reconcile.DemoProjectStatusResult(request.status)

    async def verify():
        async with await WorkflowEnvironment.start_local() as environment:
            def worker():
                return Worker(
                    environment.client, task_queue="restart-recovery",
                    workflows=[ProjectFailureHistoryWorkflow],
                    activities=[provision, compensate, record_project, record_catalog],
                    graceful_shutdown_timeout=timedelta(seconds=2),
                )

            async def wait_for_attempt(handle, expected):
                while True:
                    status = await handle.query("status")
                    if status.get("message") == f"attempt {expected} denied":
                        assert status["compensationStatus"]["phase"] == "Completed"
                        return
                    await asyncio.sleep(0.02)

            def signal(generation):
                return messages.DemoProjectReconcileSignal(demoProject={
                    "metadata": {"name": "recovery", "generation": generation}, "spec": {},
                })

            async with worker():
                handle = await environment.client.start_workflow(
                    ProjectFailureHistoryWorkflow.run,
                    messages.DemoProjectRunRequest(project_name="recovery"),
                    id="pending-reconcile-recovery", task_queue="restart-recovery",
                )
                await handle.signal("reconcile", signal(1))
                await asyncio.wait_for(wait_for_attempt(handle, 1), 10)
            assert attempts == compensations == 1
            # No poller exists while this signal is durably accepted.
            await handle.signal("reconcile", signal(2))
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            async with worker():
                await asyncio.wait_for(wait_for_attempt(handle, 2), 10)
                assert attempts == compensations == 2
                await handle.signal("shutdown", messages.ShutdownSignal())
                await asyncio.wait_for(handle.result(), 10)
            history = await handle.fetch_history()
            assert sum(event.HasField("activity_task_failed_event_attributes") for event in history.events) == 2
            await Replayer(workflows=[ProjectFailureHistoryWorkflow]).replay_workflow(history)

    asyncio.run(verify())
