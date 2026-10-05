from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import timedelta
from typing import Any
from urllib.request import urlopen

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

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
    ensure_instruqt_course_finalizer,
    instruqt_course_deletion_timestamp,
    list_demo_projects,
    list_instruqt_courses,
    read_worker_deployment_status,
    remove_demo_project_finalizer,
)
from demo_registry_operator.resources import (
    BASE_RECONCILE_KEY_STATUS_FIELD,
    catalog_configmap,
    demo_project_reconcile_key,
    instruqt_course_reconcile_key,
    is_control_plane_project,
    is_operator_rollout_canary,
    project_name,
    source_watch_resources,
)
from demo_registry_operator import reconcile_payloads as _reconcile_payloads
from demo_registry_operator.derive import (
    demo_project_workflow_id,
    instruqt_course_workflow_id,
)
from demo_registry_operator.reconcile_payloads import (
    active_version_status,
    cleanup_instruqt_testing_source_watches,
    git_repository_source_state,
    github_token_from_secret_ref,
    reconcile_source_watch,
    source_commit_from_revision,
    workflow_signal_payload,
)
from demo_registry_operator.workflows.demo_project import DemoProjectWorkflow
from demo_registry_operator.workflows.instruqt_course import InstruqtCourseWorkflow
from demo_registry_operator.workflows.messages import (
    DemoProjectFinalizeSignal,
    DemoProjectReconcileSignal,
    DemoProjectRunRequest,
    InstruqtCourseFinalizeSignal,
    InstruqtCourseReconcileSignal,
    InstruqtCourseRunRequest,
)


logger = logging.getLogger(__name__)

FAILED_RECONCILE_PHASES = {
    "CandidateNotReady",
    "Error",
    "SmokeCheckFailed",
    "WorkerRolloutFailed",
}


def demo_project_reconcile_payload_state(
    demo_project: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    return _reconcile_payloads.demo_project_reconcile_payload_state(
        demo_project,
        config,
        source_watch_fn=reconcile_source_watch,
    )


def instruqt_course_reconcile_payload_state(
    instruqt_course: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    return _reconcile_payloads.instruqt_course_reconcile_payload_state(
        instruqt_course,
        config,
        source_watch_fn=reconcile_source_watch,
        probe_github_branch_fn=probe_github_branch,
        cleanup_testing_source_watches_fn=cleanup_instruqt_testing_source_watches,
    )


def probe_github_branch(
    instruqt_course: dict[str, Any],
    config: WorkerConfig,
    *,
    source_key: str,
    branch: str,
) -> dict[str, str]:
    return _reconcile_payloads.probe_github_branch(
        instruqt_course,
        config,
        source_key=source_key,
        branch=branch,
        github_token_fn=github_token_from_secret_ref,
        urlopen_fn=urlopen,
        repo_access_fn=probe_github_repo_access,
    )


def probe_github_repo_access(
    owner: str,
    name: str,
    headers: dict[str, str],
) -> str:
    return _reconcile_payloads.probe_github_repo_access(
        owner,
        name,
        headers,
        urlopen_fn=urlopen,
    )


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

        try:
            instruqt_courses = await asyncio.to_thread(list_instruqt_courses)
        except Exception:
            logger.exception("InstruqtCourse watch list failed")
            instruqt_courses = []

        for instruqt_course in instruqt_courses:
            try:
                await reconcile_instruqt_course_resource(
                    temporal_client,
                    config,
                    instruqt_course,
                    seen,
                )
            except Exception:
                logger.exception(
                    "Watch step failed for instruqt_course=%s",
                    project_name(instruqt_course),
                )

        # Republish the catalog the registry web app mounts. Done in the poller
        # (outside Temporal) so it costs no billable workflow actions.
        try:
            await asyncio.to_thread(
                publish_catalog,
                demo_projects,
                instruqt_courses,
                config,
            )
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
    # A current-but-not-Ready WorkerDeployment must keep this worker watching:
    # idling the current build deadlocks a failed self-rollout (the running
    # operator stops reconciling its own recovery). Readiness is surfaced in the
    # status so callers can observe it without changing activation.
    ready = worker_deployment_ready(status)
    readiness_text = (
        "and is Ready" if ready else "but the WorkerDeployment is not Ready yet"
    )
    return {
        "active": True,
        "mode": WATCH_MODE_CURRENT,
        "reason": "CurrentWorkerVersion",
        "ready": ready,
        "message": (
            f"Watcher build {build_id} is the current version for "
            f"WorkerDeployment {namespace}/{name} {readiness_text}."
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
    instruqt_courses: list[dict[str, Any]],
    config: WorkerConfig,
) -> None:
    configmap = catalog_configmap(
        demo_projects,
        instruqt_courses=instruqt_courses,
        namespace=config.catalog_status_mirror_namespace,
    )
    apply_resources([configmap], dry_run=False)


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

    payload_state = await asyncio.to_thread(
        demo_project_reconcile_payload_state,
        demo_project,
        config,
    )
    if payload_state.get("ready") is not True:
        source_state = payload_state.get("sourceState")
        source_state = source_state if isinstance(source_state, dict) else {}
        logger.info(
            "Waiting for source artifact project=%s gitRepository=%s reason=%s",
            name,
            source_state.get("gitRepository"),
            source_state.get("reason", payload_state.get("reason", "Unknown")),
        )
        return
    payload = payload_state["payload"]
    source_state = payload_state.get("sourceState")
    source_state = source_state if isinstance(source_state, dict) else None
    key = reconcile_key(payload)
    if seen.get(name) == key:
        return
    status_observed = status_observed_reconcile_key(
        demo_project,
        reconcile_key=key,
        source_artifact_digest=(
            source_state.get("sourceArtifactDigest")
            if isinstance(source_state, dict)
            else None
        ),
    )
    if status_observed:
        workflow_running = await entity_workflow_running(
            temporal_client,
            demo_project_workflow_id(name, config.cluster),
        )
        if workflow_running:
            seen[name] = key
            return
    await signal_workflow(temporal_client, config, payload, "reconcile")
    seen[name] = key


async def reconcile_instruqt_course_resource(
    temporal_client: Client,
    config: WorkerConfig,
    instruqt_course: dict[str, Any],
    seen: dict[str, str],
) -> None:
    name = project_name(instruqt_course)
    seen_name = f"instruqt-course/{name}"

    deletion_timestamp = instruqt_course_deletion_timestamp(instruqt_course)
    if deletion_timestamp:
        finalize_key = f"finalize:{deletion_timestamp}"
        if seen.get(seen_name) == finalize_key:
            return
        await signal_instruqt_course_workflow(
            temporal_client,
            config,
            instruqt_course,
            "finalize",
        )
        seen[seen_name] = finalize_key
        return

    await asyncio.to_thread(ensure_instruqt_course_finalizer, name)

    payload_state = await asyncio.to_thread(
        instruqt_course_reconcile_payload_state,
        instruqt_course,
        config,
    )
    if payload_state.get("ready") is not True:
        source_state = payload_state.get("sourceState") or payload_state.get(
            "contentState"
        )
        source_state = source_state if isinstance(source_state, dict) else {}
        logger.info(
            "Waiting for source artifact instruqt_course=%s gitRepository=%s reason=%s",
            name,
            source_state.get("gitRepository"),
            source_state.get("reason", payload_state.get("reason", "Unknown")),
        )
        return
    payload = payload_state["payload"]
    source_state = payload_state.get("sourceState")
    source_state = source_state if isinstance(source_state, dict) else None
    key = instruqt_course_reconcile_key(payload)
    if seen.get(seen_name) == key:
        return
    status_observed = status_observed_reconcile_key(
        instruqt_course,
        reconcile_key=key,
        source_artifact_digest=(
            source_state.get("sourceArtifactDigest")
            if isinstance(source_state, dict)
            else None
        ),
    )
    if status_observed:
        workflow_running = await entity_workflow_running(
            temporal_client,
            instruqt_course_workflow_id(name, config.cluster),
        )
        if workflow_running:
            seen[seen_name] = key
            return
    await signal_instruqt_course_workflow(temporal_client, config, payload, "reconcile")
    seen[seen_name] = key


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
    signal_arg = demo_project_signal_request(signal_name, demo_project)
    # SignalWithStart: signals the existing run if present, otherwise starts the
    # workflow and signals it. (Do not set id_conflict_policy here — combined with
    # start_signal it suppresses delivery to an already-running workflow.)
    await temporal_client.start_workflow(
        DemoProjectWorkflow.run,
        args=[DemoProjectRunRequest(project_name=name)],
        id=demo_project_workflow_id(name, config.cluster),
        task_queue=config.task_queue,
        start_signal=signal_name,
        start_signal_args=[signal_arg],
    )
    logger.info("Signaled %s for project=%s", signal_name, name)


async def signal_instruqt_course_workflow(
    temporal_client: Client,
    config: WorkerConfig,
    instruqt_course: dict[str, Any],
    signal_name: str,
) -> None:
    """Signal the course entity workflow, starting it if absent."""
    name = project_name(instruqt_course)
    signal_arg = instruqt_course_signal_request(signal_name, instruqt_course)
    await temporal_client.start_workflow(
        InstruqtCourseWorkflow.run,
        args=[InstruqtCourseRunRequest(course_name=name)],
        id=instruqt_course_workflow_id(name, config.cluster),
        task_queue=config.task_queue,
        start_signal=signal_name,
        start_signal_args=[signal_arg],
    )
    logger.info("Signaled %s for instruqt_course=%s", signal_name, name)


def demo_project_signal_request(
    signal_name: str,
    demo_project: dict[str, Any],
) -> DemoProjectReconcileSignal | DemoProjectFinalizeSignal:
    if signal_name == "reconcile":
        return DemoProjectReconcileSignal(demoProject=demo_project)
    if signal_name == "finalize":
        return DemoProjectFinalizeSignal(demoProject=demo_project)
    raise ValueError(f"Unsupported DemoProject signal {signal_name!r}")


def instruqt_course_signal_request(
    signal_name: str,
    instruqt_course: dict[str, Any],
) -> InstruqtCourseReconcileSignal | InstruqtCourseFinalizeSignal:
    if signal_name == "reconcile":
        return InstruqtCourseReconcileSignal(instruqtCourse=instruqt_course)
    if signal_name == "finalize":
        return InstruqtCourseFinalizeSignal(instruqtCourse=instruqt_course)
    raise ValueError(f"Unsupported InstruqtCourse signal {signal_name!r}")


async def entity_workflow_running(
    temporal_client: Client,
    workflow_id: str,
) -> bool:
    handle = temporal_client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe(rpc_timeout=timedelta(seconds=5))
    except RPCError as error:
        if error.status == RPCStatusCode.NOT_FOUND:
            return False
        logger.warning(
            "Could not describe entity workflow %s; forcing signal-with-start: %s",
            workflow_id,
            error,
        )
        return False

    return description.status == WorkflowExecutionStatus.RUNNING


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

    if status.get(BASE_RECONCILE_KEY_STATUS_FIELD) == reconcile_key:
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
