from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import time
from datetime import UTC, datetime
from typing import Any

from temporalio import activity
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    CONFIGURATION_ERROR,
    VALIDATION_ERROR,
    non_retryable_error,
)
from demo_registry_operator.kube import (
    apply_demo_project,
    delete_demo_project,
    demo_project_finalizers,
    read_demo_project,
    read_config_map,
    read_deployment_status,
    read_git_repository,
    read_namespace,
    read_service,
)
from demo_registry_operator.resources import (
    CANARY_KIND_LABEL,
    CANARY_LABEL,
    IMAGE_OVERRIDES_ANNOTATION,
    OPERATOR_ROLLOUT_CANARY_KIND,
    SOURCE_COMMIT_ANNOTATION,
    dns_label,
    project_namespace,
    project_name as demo_project_name,
    source_git_repository_name,
)
from demo_registry_operator.temporal_client import connect


@activity.defn
def plan_registry_operator_canary(request: dict[str, Any]) -> dict[str, Any]:
    template = request.get("demoProjectTemplate") or request.get("demoProject")
    if not isinstance(template, dict):
        raise non_retryable_error(
            "RegistryOperatorCanaryWorkflow requires demoProjectTemplate",
            error_type=VALIDATION_ERROR,
        )

    workflow_id = string_value(request, "workflowID", default="operator-canary")
    run_id = string_value(request, "runID", default="")
    prefix = string_value(request, "projectNamePrefix", default="operator-canary")
    requested_name = request.get("projectName")
    project_name = (
        requested_name
        if isinstance(requested_name, str) and requested_name
        else canary_project_name(prefix, workflow_id, run_id)
    )
    demo_project = render_canary_demo_project(
        template,
        project_name=project_name,
        expected_source_sha=request.get("expectedSourceCommitSha"),
        expected_images=request.get("expectedImages"),
    )

    expected_operator_image = request.get("expectedOperatorImage")
    actual_operator_image = os.getenv("DEMO_REGISTRY_OPERATOR_IMAGE")
    if (
        isinstance(expected_operator_image, str)
        and expected_operator_image
        and actual_operator_image != expected_operator_image
    ):
        raise non_retryable_error(
            (
                "Registry operator canary is running on unexpected image "
                f"{actual_operator_image!r}; expected {expected_operator_image!r}"
            ),
            error_type=VALIDATION_ERROR,
        )

    expected_operator_source_sha = request.get("expectedOperatorSourceSha")
    actual_operator_source_sha = os.getenv("DEMO_REGISTRY_OPERATOR_SOURCE_SHA")
    if (
        isinstance(expected_operator_source_sha, str)
        and expected_operator_source_sha
        and actual_operator_source_sha != expected_operator_source_sha
    ):
        raise non_retryable_error(
            (
                "Registry operator canary is running unexpected source SHA "
                f"{actual_operator_source_sha!r}; expected {expected_operator_source_sha!r}"
            ),
            error_type=VALIDATION_ERROR,
        )

    config = WorkerConfig.from_env()
    target_deployment_name = os.getenv("TEMPORAL_DEPLOYMENT_NAME")
    target_build_id = os.getenv("TEMPORAL_WORKER_BUILD_ID")
    if config.reconcile_mode != "plan" and (
        not target_deployment_name or not target_build_id
    ):
        raise non_retryable_error(
            (
                "Registry operator canary requires TEMPORAL_DEPLOYMENT_NAME and "
                "TEMPORAL_WORKER_BUILD_ID so project reconciliation can be pinned "
                "to the target WorkerDeployment version"
            ),
            error_type=CONFIGURATION_ERROR,
        )
    return {
        "project": project_name,
        "namespace": project_namespace(demo_project),
        "demoProject": demo_project,
        "expectedOperatorImage": expected_operator_image,
        "actualOperatorImage": actual_operator_image,
        "expectedOperatorSourceSha": expected_operator_source_sha,
        "actualOperatorSourceSha": actual_operator_source_sha,
        "targetDeploymentName": target_deployment_name,
        "targetBuildID": target_build_id,
        "expectedSourceCommitSha": request.get("expectedSourceCommitSha"),
        "expectedImages": request.get("expectedImages", {}),
        "timeoutSeconds": int(request.get("timeoutSeconds", 900)),
        "pollSeconds": int(request.get("pollSeconds", 10)),
        "cleanupTimeoutSeconds": int(request.get("cleanupTimeoutSeconds", 600)),
        "cleanupPolicy": request.get("cleanupPolicy", "Always"),
        "sourceWatchNamespace": config.source_watch_namespace,
        "gitRepository": source_git_repository_name(demo_project),
        "catalogStatusNamespace": config.catalog_status_namespace,
        "catalogStatusMirrorNamespace": config.catalog_status_mirror_namespace,
        "catalogStatusConfigMap": config.catalog_status_configmap,
        "dryRun": config.reconcile_mode != "apply",
        "plannedAt": datetime.now(UTC).isoformat(),
    }


@activity.defn
def apply_registry_operator_canary_project(plan: dict[str, Any]) -> dict[str, Any]:
    demo_project = demo_project_from_plan(plan)
    try:
        action = apply_demo_project(
            demo_project,
            dry_run=bool(plan.get("dryRun")),
        )
    except ValueError as error:
        raise non_retryable_error(str(error), error_type=VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error

    workflow_signal = signal_registry_operator_canary_reconcile(plan, demo_project)
    return {
        "project": string_field(plan, "project"),
        "namespace": string_field(plan, "namespace"),
        "phase": "Applied" if action != "planned" else "Planned",
        "action": action,
        "workflow": workflow_signal,
        "checkedAt": datetime.now(UTC).isoformat(),
    }


@activity.defn
def wait_for_registry_operator_canary_promotion(plan: dict[str, Any]) -> dict[str, Any]:
    if bool(plan.get("dryRun")):
        return {
            "project": string_field(plan, "project"),
            "phase": "Planned",
            "ok": True,
            "message": "Canary promotion wait skipped because reconcile mode is plan.",
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    deadline = time.monotonic() + int(plan.get("timeoutSeconds", 900))
    poll_seconds = int(plan.get("pollSeconds", 10))
    last_status: dict[str, Any] | None = None
    while True:
        resource = read_demo_project(string_field(plan, "project"))
        last_status = (
            resource.get("status", {}) if isinstance(resource, dict) else None
        )
        if isinstance(last_status, dict) and canary_promoted(plan, last_status):
            namespace_status = read_namespace(string_field(plan, "namespace"))
            if namespace_status is None:
                return {
                    "project": string_field(plan, "project"),
                    "phase": "Failed",
                    "ok": False,
                    "message": "Canary promoted but namespace was not found.",
                    "status": last_status,
                    "checkedAt": datetime.now(UTC).isoformat(),
                }
            workload_status = verify_promoted_workload_resources(plan, last_status)
            if not workload_status["ok"]:
                return {
                    "project": string_field(plan, "project"),
                    "phase": "Failed",
                    "ok": False,
                    "message": workload_status["message"],
                    "namespace": namespace_status,
                    "workloads": workload_status,
                    "status": last_status,
                    "checkedAt": datetime.now(UTC).isoformat(),
                }
            return {
                "project": string_field(plan, "project"),
                "phase": "Promoted",
                "ok": True,
                "message": "Canary project promoted successfully.",
                "namespace": namespace_status,
                "workloads": workload_status,
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            }

        if isinstance(last_status, dict) and canary_failed(last_status):
            return {
                "project": string_field(plan, "project"),
                "phase": "Failed",
                "ok": False,
                "message": canary_failure_message(last_status),
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            }

        if time.monotonic() >= deadline:
            return {
                "project": string_field(plan, "project"),
                "phase": "TimedOut",
                "ok": False,
                "message": "Timed out waiting for canary project promotion.",
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            }

        activity.heartbeat(
            {
                "project": string_field(plan, "project"),
                "status": last_status,
            }
        )
        time.sleep(poll_seconds)


def canary_failed(status: dict[str, Any]) -> bool:
    if status.get("phase") in {"Error", "FinalizationError"}:
        return True
    build_status = status.get("buildStatus")
    if isinstance(build_status, dict) and build_status.get("phase") == "Error":
        return True
    candidate_status = status.get("candidateRolloutStatus")
    return isinstance(candidate_status, dict) and candidate_status.get("phase") in {
        "BuildFailed",
        "WorkerRolloutFailed",
    }


def canary_failure_message(status: dict[str, Any]) -> str:
    message = status.get("message")
    if isinstance(message, str) and message:
        return "Canary project failed before promotion: " + message
    build_status = status.get("buildStatus")
    if isinstance(build_status, dict):
        build_message = build_status.get("message")
        if isinstance(build_message, str) and build_message:
            return "Canary project failed before promotion: " + build_message
    return "Canary project failed before promotion."


@activity.defn
def delete_registry_operator_canary_project(plan: dict[str, Any]) -> dict[str, Any]:
    if plan.get("cleanupPolicy") == "Never":
        return {
            "project": string_field(plan, "project"),
            "phase": "Skipped",
            "ok": True,
            "message": "Canary cleanup skipped by cleanupPolicy=Never.",
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    if bool(plan.get("dryRun")):
        return {
            "project": string_field(plan, "project"),
            "phase": "Planned",
            "ok": True,
            "message": "Canary cleanup skipped because reconcile mode is plan.",
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    action = delete_demo_project(string_field(plan, "project"))
    return {
        "project": string_field(plan, "project"),
        "phase": "DeleteRequested",
        "ok": True,
        "deleteAction": action,
        "message": "Canary DemoProject deletion was requested.",
        "checkedAt": datetime.now(UTC).isoformat(),
    }


@activity.defn
def check_registry_operator_canary_cleanup(plan: dict[str, Any]) -> dict[str, Any]:
    if plan.get("cleanupPolicy") == "Never":
        return {
            "project": string_field(plan, "project"),
            "phase": "Skipped",
            "ok": True,
            "message": "Canary cleanup skipped by cleanupPolicy=Never.",
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    if bool(plan.get("dryRun")):
        return {
            "project": string_field(plan, "project"),
            "phase": "Planned",
            "ok": True,
            "message": "Canary cleanup skipped because reconcile mode is plan.",
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    return canary_cleanup_snapshot(plan)


@activity.defn
def wait_registry_operator_canary_cleanup(plan: dict[str, Any]) -> dict[str, Any]:
    if plan.get("cleanupPolicy") == "Never":
        return {
            "project": string_field(plan, "project"),
            "phase": "Skipped",
            "ok": True,
            "message": "Canary cleanup skipped by cleanupPolicy=Never.",
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    if bool(plan.get("dryRun")):
        return {
            "project": string_field(plan, "project"),
            "phase": "Planned",
            "ok": True,
            "message": "Canary cleanup skipped because reconcile mode is plan.",
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    deadline = time.monotonic() + int(plan.get("cleanupTimeoutSeconds", 600))
    poll_seconds = max(1, int(plan.get("pollSeconds", 10)))
    delete_action = plan.get("deleteAction")
    last_status: dict[str, Any] | None = None
    while True:
        last_status = canary_cleanup_snapshot(plan)
        if delete_action is not None:
            last_status = {
                **last_status,
                "deleteAction": delete_action,
            }
        if last_status["ok"]:
            return last_status

        if time.monotonic() >= deadline:
            return {
                **last_status,
                "phase": "CleanupFailed",
                "ok": False,
                "message": "Timed out waiting for canary cleanup to complete.",
                "checkedAt": datetime.now(UTC).isoformat(),
            }

        activity.heartbeat(last_status)
        time.sleep(poll_seconds)


@activity.defn
def cleanup_registry_operator_canary_project(plan: dict[str, Any]) -> dict[str, Any]:
    """Legacy aggregate cleanup activity; new canary workflows use split steps."""

    delete_result = delete_registry_operator_canary_project(plan)
    if delete_result.get("phase") in {"Skipped", "Planned"}:
        return delete_result

    deadline = time.monotonic() + int(plan.get("cleanupTimeoutSeconds", 600))
    poll_seconds = int(plan.get("pollSeconds", 10))
    last_status: dict[str, Any] | None = None
    while True:
        last_status = canary_cleanup_snapshot(plan)
        if last_status["ok"]:
            return {
                **last_status,
                "deleteAction": delete_result.get("deleteAction"),
            }

        if time.monotonic() >= deadline:
            return {
                **last_status,
                "phase": "CleanupFailed",
                "ok": False,
                "deleteAction": delete_result.get("deleteAction"),
                "message": "Timed out waiting for canary cleanup to complete.",
                "checkedAt": datetime.now(UTC).isoformat(),
            }

        activity.heartbeat(last_status)
        time.sleep(poll_seconds)


def canary_cleanup_snapshot(plan: dict[str, Any]) -> dict[str, Any]:
    namespace = string_field(plan, "namespace")
    git_repository = string_field(plan, "gitRepository")
    source_watch_namespace = string_field(plan, "sourceWatchNamespace")
    last_namespace = read_namespace(namespace)
    last_source = read_git_repository(source_watch_namespace, git_repository)
    last_project = read_demo_project(string_field(plan, "project"))
    last_catalog = catalog_cleanup_status(plan)
    project_finalizer_removed = (
        last_project is None
        or not demo_project_finalizers(last_project)
    )
    waiting_on = []
    if last_namespace is not None:
        waiting_on.append("namespace")
    if last_source is not None:
        waiting_on.append("source")
    if not project_finalizer_removed:
        waiting_on.append("finalizer")
    if not last_catalog["ok"]:
        waiting_on.append("catalog")

    if not waiting_on:
        return {
            "project": string_field(plan, "project"),
            "phase": "Deleted",
            "ok": True,
            "message": "Canary namespace, source watch, finalizer, and catalog status were cleaned up.",
            "catalog": last_catalog,
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    return {
        "project": string_field(plan, "project"),
        "phase": "CleanupPending",
        "ok": False,
        "message": "Canary cleanup is still waiting for resources to clear.",
        "waitingOn": waiting_on,
        "namespace": last_namespace,
        "demoProject": last_project,
        "source": last_source,
        "catalog": last_catalog,
        "checkedAt": datetime.now(UTC).isoformat(),
    }


def render_canary_demo_project(
    template: dict[str, Any],
    *,
    project_name: str,
    expected_source_sha: Any,
    expected_images: Any,
) -> dict[str, Any]:
    demo_project = copy.deepcopy(template)
    metadata = demo_project.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    annotations = metadata.get("annotations")
    if not isinstance(annotations, dict):
        annotations = {}
    labels = metadata.get("labels")
    if not isinstance(labels, dict):
        labels = {}
    if isinstance(expected_source_sha, str) and expected_source_sha:
        annotations[SOURCE_COMMIT_ANNOTATION] = expected_source_sha
    if isinstance(expected_images, dict) and expected_images:
        annotations[IMAGE_OVERRIDES_ANNOTATION] = json_dumps(expected_images)
    labels[CANARY_LABEL] = "true"
    labels[CANARY_KIND_LABEL] = OPERATOR_ROLLOUT_CANARY_KIND
    metadata["name"] = project_name
    metadata["annotations"] = annotations
    metadata["labels"] = labels
    demo_project["metadata"] = metadata
    demo_project.setdefault("apiVersion", "registry.tmprl-demo.cloud/v1alpha1")
    demo_project.setdefault("kind", "DemoProject")
    spec = demo_project.get("spec")
    ingress = spec.get("ingress") if isinstance(spec, dict) else None
    if isinstance(ingress, dict):
        ingress["subdomain"] = project_name
    return demo_project


def signal_registry_operator_canary_reconcile(
    plan: dict[str, Any],
    demo_project: dict[str, Any],
) -> dict[str, Any]:
    if bool(plan.get("dryRun")):
        return {
            "phase": "Planned",
            "message": "Canary workflow signal skipped because reconcile mode is plan.",
        }
    return asyncio.run(signal_registry_operator_canary_reconcile_async(plan, demo_project))


async def signal_registry_operator_canary_reconcile_async(
    plan: dict[str, Any],
    demo_project: dict[str, Any],
) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    deployment_name = string_field(plan, "targetDeploymentName")
    build_id = string_field(plan, "targetBuildID")
    name = demo_project_name(demo_project)
    workflow_id = f"demo-project/{name}"
    client = await connect(config)
    handle = await client.start_workflow(
        "DemoProjectWorkflow",
        args=[name],
        id=workflow_id,
        task_queue=config.task_queue,
        start_signal="reconcile",
        start_signal_args=[demo_project],
        versioning_override=PinnedVersioningOverride(
            WorkerDeploymentVersion(
                deployment_name=deployment_name,
                build_id=build_id,
            )
        ),
    )
    return {
        "phase": "Signaled",
        "workflowID": workflow_id,
        "runID": handle.first_execution_run_id,
        "targetDeploymentName": deployment_name,
        "targetBuildID": build_id,
    }


def canary_promoted(plan: dict[str, Any], status: dict[str, Any]) -> bool:
    if status.get("phase") not in {"Promoted", "Ready"}:
        return False
    expected_source = plan.get("expectedSourceCommitSha")
    candidate = status.get("candidateRolloutStatus", {})
    if isinstance(expected_source, str) and expected_source:
        if not isinstance(candidate, dict):
            return False
        if candidate.get("sourceCommitSha") != expected_source:
            return False

    expected_images = plan.get("expectedImages")
    if isinstance(expected_images, dict) and expected_images:
        active_images = status.get("activeImages")
        if not isinstance(active_images, dict):
            return False
        for build_name, image in expected_images.items():
            if active_images.get(build_name) != image:
                return False
    return True


def verify_promoted_workload_resources(
    plan: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    namespace = string_field(plan, "namespace")
    candidate = status.get("candidateRolloutStatus")
    candidate = candidate if isinstance(candidate, dict) else {}
    deployments = candidate.get("deployments")
    services = status.get("activeRouteServices") or candidate.get("services")
    deployments = deployments if isinstance(deployments, dict) else {}
    services = services if isinstance(services, dict) else {}

    missing: list[dict[str, str]] = []
    deployment_statuses = {}
    for component, name in sorted(deployments.items()):
        if not isinstance(component, str) or not isinstance(name, str):
            continue
        try:
            deployment_statuses[component] = read_deployment_status(namespace, name)
        except Exception as error:
            missing.append(
                {
                    "kind": "Deployment",
                    "component": component,
                    "name": name,
                    "message": str(error),
                }
            )

    service_statuses = {}
    for component, name in sorted(services.items()):
        if not isinstance(component, str) or not isinstance(name, str):
            continue
        try:
            service = read_service(namespace, name)
        except Exception as error:
            service = None
            missing.append(
                {
                    "kind": "Service",
                    "component": component,
                    "name": name,
                    "message": str(error),
                }
            )
        if service is None:
            missing.append(
                {
                    "kind": "Service",
                    "component": component,
                    "name": name,
                    "message": "Service was not found.",
                }
            )
        else:
            service_statuses[component] = service

    return {
        "ok": not missing,
        "message": (
            "Promoted workload resources exist."
            if not missing
            else "Promoted workload resources are missing."
        ),
        "deployments": deployment_statuses,
        "services": service_statuses,
        "missing": missing,
    }


def catalog_cleanup_status(plan: dict[str, Any]) -> dict[str, Any]:
    namespaces = [
        string_field(plan, "catalogStatusNamespace"),
        string_field(plan, "catalogStatusMirrorNamespace"),
    ]
    configmap_name = string_field(plan, "catalogStatusConfigMap")
    key = f"{string_field(plan, 'project')}.json"
    entries = []
    for namespace in sorted(set(namespaces)):
        configmap = read_config_map(namespace, configmap_name)
        entry = catalog_entry_from_configmap(configmap, key)
        entries.append(
            {
                "namespace": namespace,
                "configMap": configmap_name,
                "key": key,
                "phase": entry.get("phase") if isinstance(entry, dict) else None,
                "present": entry is not None,
                "ok": catalog_entry_finalized_or_absent(entry),
                **({"entry": entry} if isinstance(entry, dict) else {}),
            }
        )

    return {
        "ok": all(entry["ok"] for entry in entries),
        "entries": entries,
    }


def catalog_entry_from_configmap(
    configmap: dict[str, Any] | None,
    key: str,
) -> dict[str, Any] | None:
    data = configmap.get("data") if isinstance(configmap, dict) else None
    if not isinstance(data, dict) or key not in data:
        return None
    raw = data.get(key)
    if not isinstance(raw, str):
        return None
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {"phase": "Invalid", "message": "Catalog status JSON is invalid."}
    return decoded if isinstance(decoded, dict) else None


def catalog_entry_finalized_or_absent(entry: dict[str, Any] | None) -> bool:
    if entry is None:
        return True
    if entry.get("phase") != "Finalized":
        return False
    finalize_status = entry.get("finalizeStatus")
    if not isinstance(finalize_status, dict):
        return False
    external = finalize_status.get("externalResources")
    if not isinstance(external, dict):
        return False
    return external.get("phase") in {"Deleted", "Skipped", "Planned"}


def canary_project_name(prefix: str, workflow_id: str, run_id: str) -> str:
    digest = hashlib.sha256(f"{workflow_id}:{run_id}".encode("utf-8")).hexdigest()[:8]
    return dns_label(f"{prefix}-{digest}")


def demo_project_from_plan(plan: dict[str, Any]) -> dict[str, Any]:
    demo_project = plan.get("demoProject")
    if not isinstance(demo_project, dict):
        raise non_retryable_error(
            "Canary plan is missing demoProject",
            error_type=VALIDATION_ERROR,
        )
    return demo_project


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field:
        return field
    raise non_retryable_error(
        f"Canary request is missing {key}",
        error_type=VALIDATION_ERROR,
    )


def string_value(value: dict[str, Any], key: str, *, default: str) -> str:
    field = value.get(key)
    return field if isinstance(field, str) and field else default


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
