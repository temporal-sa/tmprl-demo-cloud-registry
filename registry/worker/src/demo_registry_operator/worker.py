from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from collections.abc import Mapping

from temporalio.client import Client
from temporalio.worker import Worker

from demo_registry_operator.activities.external_resources import (
    reconcile_external_resources,
)
from demo_registry_operator.activities.reconcile import (
    plan_rollout,
    promote_candidate,
    record_project_status,
    reconcile_project,
    run_smoke_check,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.watch import run_demo_project_watch
from demo_registry_operator.workflows.demo_project import (
    DemoProjectWorkflow,
    DemoRolloutWorkflow,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    config = WorkerConfig.from_env()
    client = await connect(config)
    logger.info(
        "Starting registry worker namespace=%s task_queue=%s watch_enabled=%s reconcile_mode=%s",
        config.temporal_namespace,
        config.task_queue,
        config.watch_enabled,
        config.reconcile_mode,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.activity_workers,
    ) as activity_executor:
        worker = Worker(
            client,
            task_queue=config.task_queue,
            workflows=[
                DemoProjectWorkflow,
                DemoRolloutWorkflow,
            ],
            activities=[
                reconcile_external_resources,
                reconcile_project,
                plan_rollout,
                run_smoke_check,
                promote_candidate,
                record_project_status,
            ],
            activity_executor=activity_executor,
        )
        worker_task = asyncio.create_task(worker.run())
        tasks = [worker_task]
        if config.watch_enabled:
            tasks.append(asyncio.create_task(run_demo_project_watch(client, config)))

        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_EXCEPTION,
        )
        for task in pending:
            task.cancel()
        for task in done:
            task.result()


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


if __name__ == "__main__":
    asyncio.run(main())
