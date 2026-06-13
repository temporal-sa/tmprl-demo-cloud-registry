from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

from temporalio.client import Client

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.kube import list_demo_projects
from demo_registry_operator.resources import (
    IMAGE_OVERRIDES_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
    metadata_annotations,
    project_name,
    spec_of,
)
from demo_registry_operator.workflows.demo_project import DemoProjectWorkflow


logger = logging.getLogger(__name__)


async def run_demo_project_watch(
    temporal_client: Client,
    config: WorkerConfig,
) -> None:
    seen: dict[str, str] = {}
    logger.info(
        "Starting DemoProject watch interval_seconds=%s",
        config.watch_interval_seconds,
    )

    while True:
        try:
            demo_projects = await asyncio.to_thread(list_demo_projects)
        except Exception:
            logger.exception("DemoProject watch list failed")
            await asyncio.sleep(config.watch_interval_seconds)
            continue

        for demo_project in demo_projects:
            name = project_name(demo_project)
            key = reconcile_key(demo_project)
            if seen.get(name) == key:
                continue

            await signal_reconcile(temporal_client, config, demo_project)
            seen[name] = key

        await asyncio.sleep(config.watch_interval_seconds)


async def signal_reconcile(
    temporal_client: Client,
    config: WorkerConfig,
    demo_project: dict[str, Any],
) -> None:
    name = project_name(demo_project)
    workflow_id = f"demo-project/{name}"

    handle = temporal_client.get_workflow_handle(workflow_id)
    try:
        await handle.signal("reconcile", demo_project)
        logger.info("Signaled DemoProject workflow project=%s", name)
        return
    except Exception as signal_error:
        logger.info(
            "Signal before start failed for project=%s: %s",
            name,
            signal_error,
        )

    try:
        handle = await temporal_client.start_workflow(
            DemoProjectWorkflow.run,
            name,
            id=workflow_id,
            task_queue=config.task_queue,
        )
        logger.info("Started DemoProject workflow project=%s", name)
    except Exception as start_error:
        logger.info(
            "Start workflow returned error for project=%s: %s",
            name,
            start_error,
        )
        handle = temporal_client.get_workflow_handle(workflow_id)

    await handle.signal("reconcile", demo_project)
    logger.info("Signaled DemoProject workflow project=%s", name)


def reconcile_key(demo_project: dict[str, Any]) -> str:
    metadata = demo_project.get("metadata", {})
    generation = metadata.get("generation") if isinstance(metadata, dict) else None
    annotations = metadata_annotations(demo_project)
    payload = {
        "generation": generation,
        "spec": spec_of(demo_project),
        "imageOverrides": annotations.get(IMAGE_OVERRIDES_ANNOTATION),
        "sourceCommitSha": annotations.get(SOURCE_COMMIT_ANNOTATION),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
