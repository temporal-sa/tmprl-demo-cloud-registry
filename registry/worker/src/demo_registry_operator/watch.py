from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
from typing import Any

from temporalio.client import Client
from temporalio.common import AutoUpgradeVersioningOverride

from demo_registry_operator.config import (
    WATCH_MODE_ALWAYS,
    WATCH_MODE_BOOTSTRAP,
    WATCH_MODE_CURRENT,
    WATCH_MODE_DISABLED,
    WorkerConfig,
)
from demo_registry_operator.kube import (
    apply_resources,
    demo_project_deletion_timestamp,
    ensure_demo_project_finalizer,
    list_demo_projects,
    read_git_repository,
    read_worker_deployment_status,
    remove_demo_project_finalizer,
)
from demo_registry_operator.resources import (
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
    catalog_configmap,
    demo_project_reconcile_key,
    is_control_plane_project,
    is_operator_rollout_canary,
    metadata_annotations,
    project_name,
    source_git_repository_name,
    source_watch_resources,
    source_watch_required,
    spec_of,
)
from demo_registry_operator.workflows.demo_project import DemoProjectWorkflow


logger = logging.getLogger(__name__)

FAILED_RECONCILE_PHASES = {
    "CandidateNotReady",
    "Error",
    "SmokeCheckFailed",
    "WorkerRolloutFailed",
}


async def run_demo_project_watch(
    temporal_client: Client,
    config: WorkerConfig,
) -> None:
    seen: dict[str, str] = {}
    last_activation_key: str | None = None
    logger.info(
        "Starting DemoProject watch interval_seconds=%s mode=%s",
        config.watch_interval_seconds,
        config.watch_mode,
    )

    while True:
        activation = await asyncio.to_thread(watch_activation_status, config)
        activation_key = json.dumps(activation, sort_keys=True)
        if activation_key != last_activation_key:
            log = logger.info if activation["active"] else logger.warning
            log(
                "DemoProject watch %s mode=%s reason=%s message=%s",
                "active" if activation["active"] else "idle",
                activation["mode"],
                activation["reason"],
                activation["message"],
            )
            last_activation_key = activation_key
        if not activation["active"]:
            await asyncio.sleep(config.watch_interval_seconds)
            continue

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

        # Republish the catalog the registry web app mounts. Done in the poller
        # (outside Temporal) so it costs no billable workflow actions.
        try:
            await asyncio.to_thread(publish_catalog, demo_projects, config)
        except Exception:
            logger.exception("Catalog publish failed")

        await asyncio.sleep(config.watch_interval_seconds)


def watch_activation_status(config: WorkerConfig) -> dict[str, Any]:
    mode = getattr(config, "watch_mode", None)
    if not isinstance(mode, str):
        mode = WATCH_MODE_ALWAYS if getattr(config, "watch_enabled", False) else (
            WATCH_MODE_DISABLED
        )

    if mode == WATCH_MODE_ALWAYS:
        return {
            "active": True,
            "mode": mode,
            "reason": "AlwaysEnabled",
            "message": "Watcher is explicitly enabled.",
        }
    if mode == WATCH_MODE_DISABLED:
        return {
            "active": False,
            "mode": mode,
            "reason": "Disabled",
            "message": "Watcher is disabled by configuration.",
        }
    if mode == WATCH_MODE_CURRENT:
        return current_worker_activation_status(config)
    if mode == WATCH_MODE_BOOTSTRAP:
        return bootstrap_activation_status(config)
    return {
        "active": False,
        "mode": mode,
        "reason": "UnknownMode",
        "message": f"Unsupported watcher mode {mode!r}.",
    }


def current_worker_activation_status(config: WorkerConfig) -> dict[str, Any]:
    build_id = os.getenv("TEMPORAL_WORKER_BUILD_ID", "").strip()
    deployment_name = os.getenv("TEMPORAL_DEPLOYMENT_NAME", "").strip()
    namespace, name = parse_namespaced_ref(
        deployment_name,
        default_namespace=config.source_watch_namespace,
    )
    if not build_id or not namespace or not name:
        return {
            "active": False,
            "mode": WATCH_MODE_CURRENT,
            "reason": "WorkerVersionUnavailable",
            "message": (
                "Watcher is waiting for Worker Controller to inject "
                "TEMPORAL_DEPLOYMENT_NAME and TEMPORAL_WORKER_BUILD_ID."
            ),
        }

    status = read_worker_deployment_status_for_activation(
        namespace,
        name,
        mode=WATCH_MODE_CURRENT,
    )
    if status.get("missing") or status.get("error"):
        return status

    current_build_id = worker_deployment_current_build_id(status)
    if current_build_id != build_id:
        return {
            "active": False,
            "mode": WATCH_MODE_CURRENT,
            "reason": "NotCurrentWorkerVersion",
            "message": (
                f"Watcher build {build_id} is idle until WorkerDeployment "
                f"{namespace}/{name} promotes it; current build is "
                f"{current_build_id or 'unknown'}."
            ),
            "workerDeployment": f"{namespace}/{name}",
            "buildID": build_id,
            "currentBuildID": current_build_id,
        }
    if not worker_deployment_ready(status):
        return {
            "active": False,
            "mode": WATCH_MODE_CURRENT,
            "reason": "WorkerDeploymentNotReady",
            "message": (
                f"Watcher build {build_id} is current but "
                f"WorkerDeployment {namespace}/{name} is not Ready yet."
            ),
            "workerDeployment": f"{namespace}/{name}",
            "buildID": build_id,
            "currentBuildID": current_build_id,
        }

    return {
        "active": True,
        "mode": WATCH_MODE_CURRENT,
        "reason": "CurrentWorkerVersion",
        "message": (
            f"Watcher build {build_id} is the current ready version for "
            f"WorkerDeployment {namespace}/{name}."
        ),
        "workerDeployment": f"{namespace}/{name}",
        "buildID": build_id,
        "currentBuildID": current_build_id,
    }


def bootstrap_activation_status(config: WorkerConfig) -> dict[str, Any]:
    namespace, name = parse_namespaced_ref(
        getattr(config, "watch_handoff_deployment", ""),
        default_namespace=config.source_watch_namespace,
    )
    if not namespace or not name:
        return {
            "active": True,
            "mode": WATCH_MODE_BOOTSTRAP,
            "reason": "HandoffTargetUnavailable",
            "message": "Bootstrap watcher is active because no handoff target is set.",
        }

    status = read_worker_deployment_status_for_activation(
        namespace,
        name,
        mode=WATCH_MODE_BOOTSTRAP,
    )
    if status.get("missing"):
        return {
            **status,
            "active": True,
            "message": (
                f"Bootstrap watcher is active because WorkerDeployment "
                f"{namespace}/{name} does not exist yet."
            ),
        }
    if status.get("error"):
        return {
            **status,
            "active": True,
            "message": (
                f"Bootstrap watcher is active because WorkerDeployment "
                f"{namespace}/{name} status could not be read: "
                f"{status.get('message', 'unknown error')}"
            ),
        }

    current_build_id = worker_deployment_current_build_id(status)
    if current_build_id and worker_deployment_ready(status):
        return {
            "active": False,
            "mode": WATCH_MODE_BOOTSTRAP,
            "reason": "HandoffComplete",
            "message": (
                f"Bootstrap watcher is idle because WorkerDeployment "
                f"{namespace}/{name} has current ready build {current_build_id}."
            ),
            "workerDeployment": f"{namespace}/{name}",
            "currentBuildID": current_build_id,
        }

    return {
        "active": True,
        "mode": WATCH_MODE_BOOTSTRAP,
        "reason": "HandoffPending",
        "message": (
            f"Bootstrap watcher is active until WorkerDeployment "
            f"{namespace}/{name} has a current ready build."
        ),
        "workerDeployment": f"{namespace}/{name}",
        "currentBuildID": current_build_id,
    }


def read_worker_deployment_status_for_activation(
    namespace: str,
    name: str,
    *,
    mode: str,
) -> dict[str, Any]:
    try:
        return read_worker_deployment_status(namespace, name)
    except Exception as error:
        status_code = getattr(error, "status", None)
        if str(status_code) == "404":
            return {
                "active": False,
                "mode": mode,
                "reason": "WorkerDeploymentMissing",
                "message": f"WorkerDeployment {namespace}/{name} does not exist.",
                "workerDeployment": f"{namespace}/{name}",
                "missing": True,
            }
        return {
            "active": False,
            "mode": mode,
            "reason": "WorkerDeploymentStatusError",
            "message": str(error),
            "workerDeployment": f"{namespace}/{name}",
            "error": True,
        }


def parse_namespaced_ref(
    value: str,
    *,
    default_namespace: str,
) -> tuple[str | None, str | None]:
    parsed = value.strip() if isinstance(value, str) else ""
    if not parsed:
        return None, None
    if "/" not in parsed:
        return default_namespace.strip() or None, parsed
    namespace, name = parsed.split("/", 1)
    namespace = namespace.strip()
    name = name.strip()
    return namespace or None, name or None


def worker_deployment_current_build_id(status: dict[str, Any]) -> str | None:
    current_version = status.get("currentVersion")
    if not isinstance(current_version, dict):
        return None
    for key in ("buildID", "buildId", "build_id"):
        build_id = current_version.get(key)
        if isinstance(build_id, str) and build_id.strip():
            return build_id.strip()
    return None


def worker_deployment_ready(status: dict[str, Any]) -> bool:
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return False
    return any(
        isinstance(condition, dict)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions
    )


def publish_catalog(
    demo_projects: list[dict[str, Any]],
    config: WorkerConfig,
) -> None:
    configmap = catalog_configmap(
        demo_projects,
        namespace=config.catalog_status_mirror_namespace,
    )
    apply_resources([configmap], dry_run=config.reconcile_mode != "apply")


async def reconcile_one(
    temporal_client: Client,
    config: WorkerConfig,
    demo_project: dict[str, Any],
    seen: dict[str, str],
) -> None:
    name = project_name(demo_project)
    # Control-plane projects retain their sibling resources and get no finalizer.
    is_control_plane = is_control_plane_project(demo_project)
    gate_owned_canary = is_operator_rollout_canary(demo_project)

    deletion_timestamp = demo_project_deletion_timestamp(demo_project)
    if deletion_timestamp and is_control_plane:
        await asyncio.to_thread(remove_demo_project_finalizer, name)
        seen[name] = f"retain:{deletion_timestamp}"
        return

    if deletion_timestamp and not is_control_plane:
        finalize_key = f"finalize:{deletion_timestamp}"
        if seen.get(name) == finalize_key:
            return
        await signal_workflow(temporal_client, config, demo_project, "finalize")
        seen[name] = finalize_key
        return

    # Adopt the project with a finalizer so a later deletion is observable and
    # held until the entity workflow tears everything down.
    if is_control_plane:
        await asyncio.to_thread(remove_demo_project_finalizer, name)
    else:
        await asyncio.to_thread(ensure_demo_project_finalizer, name)

    if gate_owned_canary:
        logger.info("Skipping gate-owned canary reconcile project=%s", name)
        return

    source_state = None
    if source_watch_required(demo_project):
        source_state = await asyncio.to_thread(reconcile_source_watch, demo_project, config)
        if not source_state.get("sourceCommitSha") or not source_state.get(
            "sourceArtifactDigest"
        ):
            logger.info(
                "Waiting for source artifact project=%s gitRepository=%s reason=%s",
                name,
                source_state.get("gitRepository"),
                source_state.get("reason", "Unknown"),
            )
            return

    payload = workflow_signal_payload(
        demo_project,
        source_commit_sha=(
            source_state.get("sourceCommitSha")
            if isinstance(source_state, dict)
            else None
        ),
        source_artifact_digest=(
            source_state.get("sourceArtifactDigest")
            if isinstance(source_state, dict)
            else None
        ),
    )
    key = reconcile_key(payload)
    if seen.get(name) == key:
        return
    if isinstance(source_state, dict) and status_observed_reconcile_key(
        demo_project,
        reconcile_key=key,
        source_artifact_digest=source_state.get("sourceArtifactDigest"),
    ):
        seen[name] = key
        return
    if not isinstance(source_state, dict) and status_observed_reconcile_key(
        demo_project,
        reconcile_key=key,
        source_artifact_digest=None,
    ):
        seen[name] = key
        return
    await signal_workflow(temporal_client, config, payload, "reconcile")
    seen[name] = key


def reconcile_source_watch(
    demo_project: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    resources = source_watch_resources(
        demo_project,
        namespace=config.source_watch_namespace,
        interval=config.source_watch_interval,
    )
    apply_resources(resources, dry_run=config.reconcile_mode != "apply")

    git_repository_resource = resources[-1]
    metadata = git_repository_resource["metadata"]
    name = metadata["name"]
    namespace = metadata["namespace"]
    git_repository = read_git_repository(namespace, name)
    state = git_repository_source_state(git_repository)
    return {
        "gitRepository": name,
        "namespace": namespace,
        **state,
    }


def git_repository_source_state(
    git_repository: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(git_repository, dict):
        return {
            "phase": "Waiting",
            "reason": "GitRepositoryNotFound",
            "message": "Flux GitRepository has not been created yet.",
        }

    metadata = git_repository.get("metadata", {})
    status = git_repository.get("status", {})
    generation = metadata.get("generation") if isinstance(metadata, dict) else None
    observed_generation = (
        status.get("observedGeneration") if isinstance(status, dict) else None
    )
    if generation and observed_generation != generation:
        return {
            "phase": "Waiting",
            "reason": "GitRepositoryNotObserved",
            "message": "Flux has not observed the current GitRepository spec.",
        }

    ready = ready_condition(status)
    if ready and ready.get("status") != "True":
        return {
            "phase": "Waiting",
            "reason": ready.get("reason") or "GitRepositoryNotReady",
            "message": ready.get("message") or "Flux GitRepository is not Ready.",
        }

    artifact = status.get("artifact") if isinstance(status, dict) else None
    revision = artifact.get("revision") if isinstance(artifact, dict) else None
    commit_sha = source_commit_from_revision(revision)
    if not commit_sha:
        return {
            "phase": "Waiting",
            "reason": "RevisionUnavailable",
            "message": "Flux has not reported a git commit revision yet.",
            **({"revision": revision} if isinstance(revision, str) else {}),
        }
    digest = artifact.get("digest") if isinstance(artifact, dict) else None
    if not isinstance(digest, str) or not digest.strip():
        return {
            "phase": "Waiting",
            "reason": "ArtifactDigestUnavailable",
            "message": "Flux has not reported a source artifact digest yet.",
            "revision": revision,
        }

    return {
        "phase": "Resolved",
        "reason": "RevisionResolved",
        "revision": revision,
        "sourceCommitSha": commit_sha,
        "sourceArtifactDigest": digest.strip(),
    }


def ready_condition(status: Any) -> dict[str, Any] | None:
    conditions = status.get("conditions") if isinstance(status, dict) else None
    if not isinstance(conditions, list):
        return None
    for condition in conditions:
        if isinstance(condition, dict) and condition.get("type") == "Ready":
            return condition
    return None


def source_commit_from_revision(revision: Any) -> str | None:
    if not isinstance(revision, str):
        return None
    marker = "@sha1:"
    if marker not in revision:
        return None
    commit_sha = revision.rsplit(marker, 1)[1].strip()
    if 7 <= len(commit_sha) <= 40 and all(
        character in "0123456789abcdefABCDEF" for character in commit_sha
    ):
        return commit_sha
    return None


def workflow_signal_payload(
    demo_project: dict[str, Any],
    *,
    source_commit_sha: str | None = None,
    source_artifact_digest: str | None = None,
) -> dict[str, Any]:
    metadata = demo_project.get("metadata", {})
    payload_metadata: dict[str, Any] = {"name": project_name(demo_project)}
    if isinstance(metadata, dict):
        generation = metadata.get("generation")
        if isinstance(generation, int):
            payload_metadata["generation"] = generation
        deletion_timestamp = metadata.get("deletionTimestamp")
        if isinstance(deletion_timestamp, str) and deletion_timestamp:
            payload_metadata["deletionTimestamp"] = deletion_timestamp

    annotations = metadata_annotations(demo_project)
    if source_commit_sha:
        annotations[SOURCE_COMMIT_ANNOTATION] = source_commit_sha
    if source_artifact_digest:
        annotations[SOURCE_ARTIFACT_DIGEST_ANNOTATION] = source_artifact_digest
    if annotations:
        payload_metadata["annotations"] = annotations

    return {
        "apiVersion": demo_project.get(
            "apiVersion",
            "registry.tmprl-demo.cloud/v1alpha1",
        ),
        "kind": demo_project.get("kind", "DemoProject"),
        "metadata": payload_metadata,
        "spec": copy.deepcopy(spec_of(demo_project)),
    }


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
        versioning_override=AutoUpgradeVersioningOverride(),
        start_signal=signal_name,
        start_signal_args=[demo_project],
    )
    logger.info("Signaled %s for project=%s", signal_name, name)


def reconcile_key(demo_project: dict[str, Any]) -> str:
    return demo_project_reconcile_key(demo_project)


def status_observed_reconcile_key(
    demo_project: dict[str, Any],
    *,
    reconcile_key: str,
    source_artifact_digest: Any,
) -> bool:
    metadata = demo_project.get("metadata", {})
    status = demo_project.get("status", {})
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        return False

    generation = metadata.get("generation")
    observed_generation = status.get("observedGeneration")
    if not isinstance(generation, int) or observed_generation != generation:
        return False

    if status.get("reconcileKey") == reconcile_key:
        return reconciled_condition_true(status, generation)

    if isinstance(source_artifact_digest, str) and source_artifact_digest:
        build_status = status.get("buildStatus")
        if not isinstance(build_status, dict):
            return False
        if build_status.get("sourceArtifactDigest") != source_artifact_digest:
            return False
        return reconciled_condition_true(status, generation)

    return False


def reconciled_condition_true(status: dict[str, Any], generation: int) -> bool:
    phase = status.get("phase")
    if isinstance(phase, str) and (
        phase in FAILED_RECONCILE_PHASES or phase.endswith("Failed")
    ):
        return False

    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return False
    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        if condition.get("type") != "Reconciled":
            continue
        if condition.get("observedGeneration") != generation:
            continue
        return condition.get("status") == "True"
    return False
