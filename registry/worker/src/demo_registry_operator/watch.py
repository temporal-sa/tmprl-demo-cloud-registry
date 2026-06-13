from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

from temporalio.client import Client

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.kube import (
    demo_project_deletion_timestamp,
    ensure_demo_project_finalizer,
    list_demo_projects,
)
from demo_registry_operator.resources import (
    IMAGE_OVERRIDES_ANNOTATION,
    REGISTRY_PROJECT_NAME,
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
            try:
                await reconcile_one(temporal_client, config, demo_project, seen)
            except Exception:
                # Don't advance `seen` on failure, so the next resync retries.
                logger.exception(
                    "Watch step failed for project=%s",
                    project_name(demo_project),
                )

        await asyncio.sleep(config.watch_interval_seconds)


async def reconcile_one(
    temporal_client: Client,
    config: WorkerConfig,
    demo_project: dict[str, Any],
    seen: dict[str, str],
) -> None:
    name = project_name(demo_project)
    # Control-plane guard: never finalize the registry (it would tear down the
    # operator's own sibling resources). It also gets no finalizer.
    is_registry = name == REGISTRY_PROJECT_NAME

    deletion_timestamp = demo_project_deletion_timestamp(demo_project)
    if deletion_timestamp and not is_registry:
        finalize_key = f"finalize:{deletion_timestamp}"
        if seen.get(name) == finalize_key:
            return
        await signal_workflow(temporal_client, config, demo_project, "finalize")
        seen[name] = finalize_key
        return

    # Adopt the project with a finalizer so a later deletion is observable and
    # held until the entity workflow tears everything down.
    if not is_registry:
        await asyncio.to_thread(ensure_demo_project_finalizer, name)

    key = reconcile_key(demo_project)
    if seen.get(name) == key:
        return
    await signal_workflow(temporal_client, config, demo_project, "reconcile")
    seen[name] = key


async def signal_workflow(
    temporal_client: Client,
    config: WorkerConfig,
    demo_project: dict[str, Any],
    signal_name: str,
) -> None:
    """Signal the project's entity workflow, starting it if absent (SignalWithStart).

    The signal carries the full DemoProject (desired state), so the workflow is
    level-triggered: a missed or duplicate signal converges on the next resync.
    """
    name = project_name(demo_project)
    # SignalWithStart: signals the existing run if present, otherwise starts the
    # workflow and signals it. (Do not set id_conflict_policy here — combined with
    # start_signal it suppresses delivery to an already-running workflow.)
    await temporal_client.start_workflow(
        DemoProjectWorkflow.run,
        args=[name],
        id=f"demo-project/{name}",
        task_queue=config.task_queue,
        start_signal=signal_name,
        start_signal_args=[demo_project],
    )
    logger.info("Signaled %s for project=%s", signal_name, name)


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
