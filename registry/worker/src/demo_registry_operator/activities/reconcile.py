from dataclasses import dataclass
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    CONFIGURATION_ERROR,
    VALIDATION_ERROR,
    non_retryable_error,
)
from demo_registry_operator.kube import (
    apply_resources,
    delete_deployment,
    delete_external_secret,
    delete_git_repository,
    delete_namespace,
    delete_service,
    list_deployments,
    list_services,
    patch_demo_project_status,
    read_deployment_status,
    read_external_secret_status,
    read_worker_deployment_status,
    remove_demo_project_finalizer,
)
from demo_registry_operator.resources import (
    SOURCE_COMMIT_ANNOTATION,
    base_resources,
    build_reconcile_status,
    catalog_status_configmap,
    components,
    demo_project_reconcile_key,
    is_control_plane_project,
    missing_image_overrides,
    parse_image_overrides,
    promotion_resources,
    project_host,
    project_name,
    project_namespace,
    runtime_secret_resources,
    runtime_workload_resources,
    source_auth_kubernetes_secret_name,
    source_git_repository_name,
    spec_of,
    worker_deployment_rollout_wait_seconds,
    worker_versioning_enabled,
    versioned_worker_components,
)


@dataclass(frozen=True)
class ApplyDemoProjectResourcesRequest:
    demo_project: dict[str, Any]
    external_resources: dict[str, Any] | None = None
    build_status: dict[str, Any] | None = None


@dataclass(frozen=True)
class DemoProjectStatusResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class ProjectFinalizationRequest:
    demo_project: dict[str, Any]


@dataclass(frozen=True)
class ProjectFinalizationPlan:
    plan: dict[str, Any]


@dataclass(frozen=True)
class ResourceDeleteRequest:
    namespace: str
    name: str


@dataclass(frozen=True)
class OptionalResourceDeleteRequest:
    namespace: str
    name: str | None = None


@dataclass(frozen=True)
class ProjectNamespaceDeleteRequest:
    namespace: str


@dataclass(frozen=True)
class ProjectFinalizerRemovalRequest:
    project: str


@dataclass(frozen=True)
class ActivityActionResult:
    action: str


@dataclass(frozen=True)
class SmokeCheckRequest:
    project: str
    demo_project: dict[str, Any] | None = None
    host: str | None = None
    candidate: dict[str, Any] | None = None
    smoke_check: dict[str, Any] | None = None


@dataclass(frozen=True)
class ActivityStatusResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class CandidateReadinessRequest:
    demo_project: dict[str, Any]
    candidate: dict[str, Any]


@dataclass(frozen=True)
class PromoteCandidateRoutesRequest:
    project: str
    demo_project: dict[str, Any]
    candidate: dict[str, Any]


@dataclass(frozen=True)
class PruneStaleRuntimeCandidatesRequest:
    demo_project: dict[str, Any]
    active_version: str


@dataclass(frozen=True)
class WorkerDeploymentRolloutRequest:
    demo_project: dict[str, Any]
    worker_deployments: dict[str, Any]
    timeout_seconds: int | None = None


@dataclass(frozen=True)
class RecordDemoProjectStatusRequest:
    demo_project: dict[str, Any]
    status: dict[str, Any]


@activity.defn
def apply_demo_project_resources(
    request: ApplyDemoProjectResourcesRequest,
) -> DemoProjectStatusResult:
    """Reconcile one DemoProject.

    The operator runs in apply mode and creates or patches Kubernetes resources.
    """
    demo_project = request.demo_project
    external_resources = request.external_resources
    build_status = request.build_status
    demo_project = demo_project_with_build_source(demo_project, build_status)
    name = project_name(demo_project)
    image_overrides = images_from_build_status(build_status) or parse_image_overrides(demo_project)
    missing_images = missing_image_overrides(demo_project, image_overrides)
    build_failed = bool(build_status and build_status.get("phase") == "Error")
    external_resource_block = external_resource_blocker(external_resources)

    resources = base_resources(demo_project)
    resources.extend(runtime_secret_resources(demo_project))
    smoke_check = spec_of(demo_project).get("smokeCheck")
    has_versioned_workers = bool(versioned_worker_components(demo_project))
    defer_route_promotion = bool(smoke_check) or has_versioned_workers
    if not missing_images and not build_failed and external_resource_block is None:
        resources.extend(
            runtime_workload_resources(
                demo_project,
                image_overrides,
                include_routes=not defer_route_promotion,
                external_resources=external_resources,
            )
        )

    activity.logger.info(
        "Reconciling DemoProject project=%s resources=%s missing_images=%s",
        name,
        len(resources),
        missing_images,
    )

    try:
        applied = apply_resources(resources, dry_run=False)
        external_secret_status = external_secret_sync_status(applied)
        worker_deployments_status = worker_deployment_sync_status(applied)
    except ValueError as error:
        raise non_retryable_error(str(error), error_type=VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error

    if build_failed:
        phase = "Error"
        message = "Image build failed; workload resources were not reconciled."
    elif missing_images:
        phase = "WaitingForImages"
        message = "Base resources and ExternalSecrets reconciled; waiting for immutable images."
    elif external_resource_block:
        phase = "WaitingForExternalResources"
        message = external_resource_block["message"]
    elif has_versioned_workers:
        phase = "WorkerRolloutPending"
        message = "WorkerDeployment resources reconciled; waiting for Worker Controller rollout."
    elif spec_of(demo_project).get("smokeCheck"):
        phase = "SmokeCheckPending"
        message = "Runtime resources reconciled; smoke check is ready to run."
    else:
        phase = "Ready"
        message = "Runtime resources reconciled."

    status = build_reconcile_status(
        demo_project,
        phase=phase,
        message=message,
        mode="apply",
        applied_resources=applied,
        image_overrides=image_overrides,
        missing_images=missing_images,
        external_resources=external_resources,
        build_status=build_status,
        external_secret_status=external_secret_status,
        worker_deployments_status=worker_deployments_status,
        external_resource_block=external_resource_block,
    )
    return DemoProjectStatusResult(status)


@activity.defn
def load_project_finalization_plan(
    request: ProjectFinalizationRequest,
) -> ProjectFinalizationPlan:
    demo_project = request.demo_project
    config = WorkerConfig.from_env()
    name = project_name(demo_project)
    namespace = project_namespace(demo_project)

    # Never tear down the control plane, even if a finalize somehow reached it.
    if is_control_plane_project(demo_project):
        activity.logger.warning(
            "Refusing to finalize control-plane project=%s", name
        )
        return ProjectFinalizationPlan({
            "action": "complete",
            "status": {
                "phase": "Skipped",
                "project": name,
                "message": "Control-plane project is not finalized.",
            },
        })

    return ProjectFinalizationPlan({
        "action": "finalize",
        "demoProject": demo_project,
        "project": name,
        "namespace": namespace,
        "sourceWatchNamespace": config.source_watch_namespace,
        "gitRepository": source_git_repository_name(demo_project),
        "sourceAuthSecret": source_auth_kubernetes_secret_name(demo_project),
        "checkedAt": datetime.now(UTC).isoformat(),
    })


@activity.defn
def delete_project_namespace_resource(
    request: ProjectNamespaceDeleteRequest,
) -> ActivityActionResult:
    try:
        return ActivityActionResult(delete_namespace(
            required_string(request.namespace, "namespace"),
            dry_run=False,
        ))
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error


@activity.defn
def delete_project_source_repository(
    request: ResourceDeleteRequest,
) -> ActivityActionResult:
    try:
        return ActivityActionResult(delete_git_repository(
            required_string(request.namespace, "namespace"),
            required_string(request.name, "name"),
            dry_run=False,
        ))
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error


@activity.defn
def delete_project_source_auth_secret(
    request: OptionalResourceDeleteRequest,
) -> ActivityActionResult:
    name = request.name
    if not isinstance(name, str) or not name:
        return ActivityActionResult("not-required")
    try:
        return ActivityActionResult(delete_external_secret(
            required_string(request.namespace, "namespace"),
            name,
            dry_run=False,
        ))
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error


@activity.defn
def remove_project_finalizer(
    request: ProjectFinalizerRemovalRequest,
) -> ActivityActionResult:
    try:
        remove_demo_project_finalizer(
            required_string(request.project, "project"),
            dry_run=False,
        )
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error
    return ActivityActionResult("removed")


def project_finalization_status(
    plan: dict[str, Any],
    *,
    namespace_action: str,
    source_action: str,
    external: dict[str, Any],
    source_auth_action: str = "not-required",
) -> dict[str, Any]:
    source_status = {
        "gitRepository": string_field(plan, "gitRepository"),
        "namespace": string_field(plan, "sourceWatchNamespace"),
        "action": source_action,
    }
    if source_auth_action != "not-required":
        source_status["authSecretAction"] = source_auth_action
    return {
        "phase": "Finalized",
        "project": string_field(plan, "project"),
        "checkedAt": string_field(plan, "checkedAt"),
        "kubernetesNamespace": {
            "name": string_field(plan, "namespace"),
            "action": namespace_action,
        },
        "source": source_status,
        "externalResources": external,
    }


def completed_activity_status(value: dict[str, Any]) -> dict[str, Any] | None:
    status = value.get("status")
    return (
        status
        if value.get("action") == "complete" and isinstance(status, dict)
        else None
    )


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    return required_string(field, key)


def required_string(field: Any, key: str) -> str:
    if isinstance(field, str) and field:
        return field
    raise non_retryable_error(
        f"Activity request is missing {key}",
        error_type=VALIDATION_ERROR,
    )


def plan_rollout(request: dict[str, Any]) -> dict[str, Any]:
    project_name = request.get("project", "unknown")
    source_commit_sha = request.get("sourceCommitSha")
    demo_project = request.get("demoProject")
    activity.logger.info(
        "Planning candidate rollout for project=%s sourceCommitSha=%s",
        project_name,
        source_commit_sha,
    )
    host = request.get("host")
    if not isinstance(host, str) and isinstance(demo_project, dict):
        host = project_host(demo_project)

    return {
        "project": project_name,
        "phase": "CandidatePlanned",
        "sourceCommitSha": source_commit_sha,
        "host": host,
        "demoProject": demo_project,
        "candidate": request.get("candidate", {}),
        "smokeCheck": request.get("smokeCheck"),
    }


@activity.defn
def run_smoke_check(request: SmokeCheckRequest) -> ActivityStatusResult:
    smoke_check = request.smoke_check
    if not smoke_check:
        return ActivityStatusResult({
            "ok": True,
            "skipped": True,
            "message": "No smoke check configured.",
        })

    url = smoke_check_url(request, smoke_check)
    expected_status = int(smoke_check.get("expectedStatus", 200))
    timeout = int(smoke_check.get("timeoutSeconds", 10))
    # Poll with backoff rather than a single shot: a freshly-promoted candidate
    # may still be starting (e.g. an app that builds/initializes on boot), so one
    # early miss should not fail the rollout. The readiness gate already waits for
    # the port to listen; this tolerates the remaining startup gap.
    deadline = time.monotonic() + smoke_check_retry_window_seconds(smoke_check)
    attempt = 0
    actual_status: int | None = None
    last_error: str | None = None

    while True:
        attempt += 1
        checked_at = datetime.now(UTC).isoformat()
        try:
            request = Request(url, headers={"User-Agent": "registry-operator/0"})
            with urlopen(request, timeout=timeout) as response:
                actual_status = int(response.status)
            last_error = None
        except HTTPError as error:
            actual_status = int(error.code)
            last_error = None
        except (OSError, URLError) as error:
            actual_status = None
            last_error = f"Smoke check request failed: {error}"

        if actual_status == expected_status:
            return ActivityStatusResult({
                "ok": True,
                "skipped": False,
                "url": url,
                "expectedStatus": expected_status,
                "actualStatus": actual_status,
                "attempts": attempt,
                "checkedAt": checked_at,
                "message": "Smoke check passed.",
                "smokeCheck": smoke_check,
            })

        if time.monotonic() >= deadline:
            message = (
                last_error
                if last_error
                else "Smoke check status mismatch."
            )
            return ActivityStatusResult({
                "ok": False,
                "skipped": False,
                "url": url,
                "expectedStatus": expected_status,
                "actualStatus": actual_status,
                "attempts": attempt,
                "checkedAt": checked_at,
                "message": message,
                "smokeCheck": smoke_check,
            })

        activity.heartbeat({"smokeCheck": url, "attempt": attempt, "lastStatus": actual_status})
        time.sleep(smoke_check_poll_seconds(smoke_check))


def smoke_check_retry_window_seconds(smoke_check: dict[str, Any]) -> int:
    value = smoke_check.get("retryWindowSeconds")
    return int(value) if isinstance(value, (int, float)) and value > 0 else 120


def smoke_check_poll_seconds(smoke_check: dict[str, Any]) -> int:
    value = smoke_check.get("pollSeconds")
    return int(value) if isinstance(value, (int, float)) and value > 0 else 5


@activity.defn
def wait_for_candidate_readiness(
    request: CandidateReadinessRequest,
) -> ActivityStatusResult:
    config = WorkerConfig.from_env()
    demo_project = request.demo_project
    candidate = request.candidate
    if not isinstance(demo_project, dict) or not isinstance(candidate, dict):
        raise non_retryable_error(
            "wait_for_candidate_readiness requires demoProject and candidate",
            error_type=VALIDATION_ERROR,
        )

    deployments = candidate.get("deployments")
    if not isinstance(deployments, dict) or not deployments:
        return ActivityStatusResult({
            "phase": "Skipped",
            "ok": True,
            "message": "Candidate has no Deployment resources to wait for.",
            "checkedAt": datetime.now(UTC).isoformat(),
            "deployments": {},
        })

    namespace = project_namespace(demo_project)
    deadline = time.monotonic() + config.readiness_timeout_seconds
    last_status: dict[str, Any] = {}
    while True:
        last_status = read_candidate_deployments(namespace, deployments)
        if all(deployment.get("ready") for deployment in last_status.values()):
            return ActivityStatusResult({
                "phase": "Ready",
                "ok": True,
                "message": "All candidate Deployments are available.",
                "checkedAt": datetime.now(UTC).isoformat(),
                "deployments": last_status,
            })

        if time.monotonic() >= deadline:
            return ActivityStatusResult({
                "phase": "TimedOut",
                "ok": False,
                "message": "Timed out waiting for candidate Deployments to become available.",
                "checkedAt": datetime.now(UTC).isoformat(),
                "deployments": last_status,
            })

        activity.heartbeat(
            {
                "project": project_name(demo_project),
                "namespace": namespace,
                "deployments": last_status,
            }
        )
        time.sleep(config.readiness_poll_seconds)


@activity.defn
def promote_candidate_routes(
    request: PromoteCandidateRoutesRequest,
) -> ActivityStatusResult:
    project_name = request.project
    demo_project = request.demo_project
    candidate = request.candidate
    candidate_images = (
        candidate.get("images")
        if isinstance(candidate, dict) and isinstance(candidate.get("images"), dict)
        else None
    )
    promotion_project = demo_project
    if isinstance(demo_project, dict) and isinstance(candidate, dict):
        promotion_project = demo_project_with_build_source(demo_project, candidate)
    applied = []
    if isinstance(promotion_project, dict):
        applied = apply_resources(
            promotion_resources(
                promotion_project,
                image_overrides=candidate_images,
            ),
            dry_run=False,
        )
    activity.logger.info("Promoting candidate for project=%s", project_name)
    return ActivityStatusResult({
        "project": project_name,
        "phase": "Promoted",
        "message": "Candidate passed smoke check; active route resources were promoted.",
        "candidate": candidate or {},
        "appliedResources": applied,
    })


@activity.defn
def prune_stale_runtime_candidates(
    request: PruneStaleRuntimeCandidatesRequest,
) -> ActivityStatusResult:
    return ActivityStatusResult(
        prune_stale_runtime_candidates_impl(
            request.demo_project,
            active_version=required_string(request.active_version, "active_version"),
        )
    )


def prune_stale_runtime_candidates_impl(
    demo_project: dict[str, Any],
    *,
    active_version: str,
) -> dict[str, Any]:
    namespace = project_namespace(demo_project)
    project = project_name(demo_project)
    non_worker_deployment_components = {
        component_name
        for component_name, component in components(demo_project).items()
        if not worker_versioning_enabled(component)
    }
    if not non_worker_deployment_components:
        return {
            "phase": "Skipped",
            "message": "Every runtime component is managed by WorkerDeployment.",
            "activeVersion": active_version,
            "resources": [],
        }

    label_selector = ",".join(
        [
            f"app.kubernetes.io/name={project}",
            "app.kubernetes.io/managed-by=demo-registry",
            "platform.temporal.io/rollout-role=candidate",
        ]
    )
    resources = [
        *prune_stale_deployments(
            namespace,
            active_version=active_version,
            component_names=non_worker_deployment_components,
            label_selector=label_selector,
        ),
        *prune_stale_services(
            namespace,
            active_version=active_version,
            component_names=non_worker_deployment_components,
            label_selector=label_selector,
        ),
    ]
    return {
        "phase": "Pruned" if resources else "Skipped",
        "message": (
            f"Deleted {len(resources)} stale runtime candidate resource(s)."
            if resources
            else "No stale runtime candidate resources found."
        ),
        "activeVersion": active_version,
        "resources": resources,
    }


def prune_stale_deployments(
    namespace: str,
    *,
    active_version: str,
    component_names: set[str],
    label_selector: str,
) -> list[dict[str, Any]]:
    deleted = []
    for deployment in list_deployments(namespace, label_selector=label_selector):
        labels = deployment.get("labels", {})
        component = labels.get("app.kubernetes.io/component")
        version = labels.get("platform.temporal.io/rollout-version")
        name = deployment.get("name")
        if (
            not isinstance(name, str)
            or component not in component_names
            or version == active_version
        ):
            continue
        action = delete_deployment(namespace, name, dry_run=False)
        deleted.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "namespace": namespace,
                "name": name,
                "component": component,
                "rolloutVersion": version,
                "action": action,
            }
        )
    return deleted


def prune_stale_services(
    namespace: str,
    *,
    active_version: str,
    component_names: set[str],
    label_selector: str,
) -> list[dict[str, Any]]:
    deleted = []
    for service in list_services(namespace, label_selector=label_selector):
        labels = service.get("labels", {})
        annotations = service.get("annotations", {})
        component = labels.get("app.kubernetes.io/component")
        version = labels.get("platform.temporal.io/rollout-version")
        name = service.get("name")
        if (
            not isinstance(name, str)
            or component not in component_names
            or version == active_version
            or "platform.temporal.io/stable-service" not in annotations
        ):
            continue
        action = delete_service(namespace, name, dry_run=False)
        deleted.append(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "namespace": namespace,
                "name": name,
                "component": component,
                "rolloutVersion": version,
                "action": action,
            }
        )
    return deleted


def read_candidate_deployments(
    namespace: str,
    deployments: dict[str, Any],
) -> dict[str, Any]:
    statuses = {}
    for component, deployment_name in sorted(deployments.items()):
        if not isinstance(component, str) or not isinstance(deployment_name, str):
            continue
        try:
            status = read_deployment_status(namespace, deployment_name)
        except Exception as error:
            statuses[component] = {
                "name": deployment_name,
                "phase": "Missing",
                "ready": False,
                "message": str(error),
            }
            continue

        statuses[component] = {
            **status,
            "phase": "Ready" if deployment_ready(status) else "Waiting",
            "ready": deployment_ready(status),
        }
    return statuses


def deployment_ready(status: dict[str, Any]) -> bool:
    replicas = status.get("replicas")
    desired = int(replicas) if isinstance(replicas, int) else 1
    observed_generation = status.get("observedGeneration")
    generation = status.get("generation")
    if isinstance(generation, int) and isinstance(observed_generation, int):
        if observed_generation < generation:
            return False
    return (
        int(status.get("availableReplicas") or 0) >= desired
        and int(status.get("readyReplicas") or 0) >= desired
        and int(status.get("updatedReplicas") or 0) >= desired
    )


@activity.defn
def wait_for_worker_deployment_rollouts(
    request: WorkerDeploymentRolloutRequest,
) -> ActivityStatusResult:
    config = WorkerConfig.from_env()
    demo_project = request.demo_project
    worker_deployments = request.worker_deployments
    if not isinstance(demo_project, dict) or not isinstance(worker_deployments, dict):
        raise non_retryable_error(
            "wait_for_worker_deployment_rollouts requires demoProject and workerDeployments",
            error_type=VALIDATION_ERROR,
        )

    if not worker_deployments:
        return ActivityStatusResult({
            "phase": "Skipped",
            "ok": True,
            "message": "Candidate has no WorkerDeployment resources to wait for.",
            "checkedAt": datetime.now(UTC).isoformat(),
            "resources": [],
        })

    wait_seconds = positive_timeout_seconds(request.timeout_seconds) or worker_deployment_rollout_wait_seconds(
        demo_project,
        base_timeout_seconds=config.readiness_timeout_seconds,
    )
    deadline = time.monotonic() + wait_seconds
    last_status: dict[str, Any] = {}
    while True:
        last_status = read_worker_deployments(
            project_namespace(demo_project),
            worker_deployments,
        )
        aggregate = aggregate_worker_deployment_status(last_status)
        if aggregate["phase"] == "Ready":
            return ActivityStatusResult({
                **aggregate,
                "ok": True,
                "message": "All WorkerDeployment rollouts are ready.",
                "checkedAt": datetime.now(UTC).isoformat(),
                "resources": list(last_status.values()),
            })
        if aggregate["phase"] in {"Failed", "Error"}:
            return ActivityStatusResult({
                **aggregate,
                "ok": False,
                "checkedAt": datetime.now(UTC).isoformat(),
                "resources": list(last_status.values()),
            })

        if time.monotonic() >= deadline:
            return ActivityStatusResult({
                "phase": "TimedOut",
                "ok": False,
                "message": (
                    "Timed out waiting for WorkerDeployment rollouts to become ready "
                    f"after {wait_seconds} seconds."
                ),
                "checkedAt": datetime.now(UTC).isoformat(),
                "resources": list(last_status.values()),
            })

        activity.heartbeat(
            {
                "project": project_name(demo_project),
                "namespace": project_namespace(demo_project),
                "workerDeployments": last_status,
            }
        )
        time.sleep(config.readiness_poll_seconds)


def positive_timeout_seconds(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit():
        seconds = int(value)
        return seconds if seconds > 0 else 0
    return 0


def worker_deployment_sync_status(
    applied_resources: list[dict[str, Any]],
) -> dict[str, Any] | None:
    worker_deployments = [
        resource
        for resource in applied_resources
        if resource.get("kind") == "WorkerDeployment"
        and isinstance(resource.get("name"), str)
        and isinstance(resource.get("namespace"), str)
    ]
    if not worker_deployments:
        return None

    checked_at = datetime.now(UTC).isoformat()
    statuses = {}
    for resource in worker_deployments:
        name = resource["name"]
        namespace = resource["namespace"]
        try:
            status = read_worker_deployment_status(namespace, name)
        except Exception as error:
            status = {
                "name": name,
                "namespace": namespace,
                "phase": "Missing" if getattr(error, "status", None) == 404 else "Error",
                "ready": False,
                "message": str(error),
            }
        statuses[name] = status

    aggregate = aggregate_worker_deployment_status(statuses)
    return {
        **aggregate,
        "ok": aggregate["phase"] == "Ready",
        "checkedAt": checked_at,
        "resources": list(statuses.values()),
    }


def read_worker_deployments(
    namespace: str,
    worker_deployments: dict[str, Any],
) -> dict[str, Any]:
    statuses = {}
    for component, name in sorted(worker_deployments.items()):
        if not isinstance(component, str) or not isinstance(name, str):
            continue
        try:
            status = read_worker_deployment_status(namespace, name)
        except Exception as error:
            status = {
                "name": name,
                "namespace": namespace,
                "phase": "Missing" if getattr(error, "status", None) == 404 else "Error",
                "ready": False,
                "message": str(error),
            }
        statuses[component] = {
            **status,
            "component": component,
            "phase": worker_deployment_phase(status),
            "ready": worker_deployment_ready(status),
        }
    return statuses


def aggregate_worker_deployment_status(
    statuses: dict[str, Any],
) -> dict[str, str]:
    phases = {
        worker_deployment_phase(status)
        for status in statuses.values()
        if isinstance(status, dict)
    }
    if not phases:
        return {"phase": "Skipped", "message": "No WorkerDeployment resources."}
    if phases <= {"Ready"}:
        return {"phase": "Ready", "message": "All WorkerDeployments are ready."}
    if phases & {"Failed", "Error"}:
        return {
            "phase": "Failed",
            "message": "At least one WorkerDeployment rollout failed.",
        }
    if phases & {"Missing"}:
        return {
            "phase": "Waiting",
            "message": "At least one WorkerDeployment has not been observed yet.",
        }
    return {
        "phase": "Waiting",
        "message": "WorkerDeployment rollouts are still progressing.",
    }


def worker_deployment_phase(status: dict[str, Any]) -> str:
    if status.get("phase") in {"Missing", "Error"}:
        return str(status["phase"])
    if worker_deployment_failed(status):
        return "Failed"
    if worker_deployment_ready(status):
        return "Ready"
    return "Waiting"


def worker_deployment_ready(status: dict[str, Any]) -> bool:
    ready = worker_deployment_condition(status, "Ready")
    return bool(ready and ready.get("status") == "True")


def worker_deployment_failed(status: dict[str, Any]) -> bool:
    for workflow in target_test_workflows(status):
        if workflow.get("status") in {"Failed", "Canceled", "Terminated", "TimedOut"}:
            return True

    ready = worker_deployment_condition(status, "Ready")
    if ready and ready.get("status") == "False":
        return ready.get("reason") in {
            "InvalidSpec",
            "AuthSecretInvalid",
            "ConnectionNotFound",
            "TemporalClientCreationFailed",
            "TemporalStateFetchFailed",
        }
    progressing = worker_deployment_condition(status, "Progressing")
    if progressing and progressing.get("status") == "False":
        return progressing.get("reason") in {
            "InvalidSpec",
            "AuthSecretInvalid",
            "ConnectionNotFound",
        }
    return False


def worker_deployment_condition(
    status: dict[str, Any],
    condition_type: str,
) -> dict[str, Any] | None:
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return None
    return next(
        (
            condition
            for condition in conditions
            if isinstance(condition, dict) and condition.get("type") == condition_type
        ),
        None,
    )


def target_test_workflows(status: dict[str, Any]) -> list[dict[str, Any]]:
    target = status.get("targetVersion")
    workflows = target.get("testWorkflows") if isinstance(target, dict) else None
    return [item for item in workflows if isinstance(item, dict)] if isinstance(workflows, list) else []


def external_resource_blocker(
    external_resources: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(external_resources, dict):
        return None

    temporal_cloud = external_resources.get("temporalCloud")
    if isinstance(temporal_cloud, dict) and temporal_cloud.get("phase") == "Error":
        reason = str(temporal_cloud.get("reason") or "TemporalCloudError")
        return {
            "phase": "Error",
            "reason": reason,
            "message": temporal_cloud.get(
                "message",
                "Temporal Cloud external resource reconciliation failed.",
            ),
            "temporalCloud": {
                key: temporal_cloud.get(key)
                for key in ("phase", "reason", "message", "auth", "namespace")
                if key in temporal_cloud
            },
        }

    secrets_manager = external_resources.get("secretsManager")
    if not isinstance(secrets_manager, dict):
        return None

    phase = secrets_manager.get("phase")
    if phase not in {"ValuesRequired", "Error"}:
        return None

    blocking = blocking_secret_statuses(secrets_manager)
    return {
        "phase": phase,
        "reason": (
            "SecretValuesRequired"
            if phase == "ValuesRequired"
            else "SecretVerificationFailed"
        ),
        "message": (
            "Required AWS Secrets Manager values are not ready; runtime "
            "workloads were not deployed."
        ),
        "secretsManager": {
            "phase": phase,
            "blocking": blocking,
        },
    }


def blocking_secret_statuses(secrets_manager: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        secrets_manager.get("temporalBootstrap"),
        secrets_manager.get("temporalCredentials"),
        *(
            secrets_manager.get("userManaged")
            if isinstance(secrets_manager.get("userManaged"), list)
            else []
        ),
    ]
    blocking = []
    for secret in candidates:
        if not isinstance(secret, dict) or secret.get("required") is False:
            continue
        if secret.get("phase") in {"Ready", "NotRequired"}:
            continue
        blocking.append(
            {
                key: secret.get(key)
                for key in (
                    "path",
                    "phase",
                    "message",
                    "missingProperties",
                    "managedBy",
                    "components",
                )
                if key in secret
            }
        )
    return blocking


def external_secret_sync_status(
    applied_resources: list[dict[str, Any]],
) -> dict[str, Any] | None:
    external_secrets = [
        resource
        for resource in applied_resources
        if resource.get("kind") == "ExternalSecret"
        and isinstance(resource.get("name"), str)
        and isinstance(resource.get("namespace"), str)
    ]
    if not external_secrets:
        return None

    checked_at = datetime.now(UTC).isoformat()
    resources = []
    for resource in external_secrets:
        name = resource["name"]
        namespace = resource["namespace"]
        try:
            resources.append(read_external_secret_status(namespace, name))
        except Exception as error:
            resources.append(
                {
                    "name": name,
                    "namespace": namespace,
                    "phase": (
                        "Missing"
                        if getattr(error, "status", None) == 404
                        else "Error"
                    ),
                    "ready": False,
                    "message": str(error),
                }
            )

    return {
        "phase": aggregate_external_secret_phase(resources),
        "checkedAt": checked_at,
        "resources": resources,
    }


def aggregate_external_secret_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if phases <= {"Ready"}:
        return "Ready"
    if phases & {"Error", "Missing"}:
        return "Error"
    if "Waiting" in phases:
        return "Waiting"
    return "Unknown"


@activity.defn
def record_demo_project_status(
    request: RecordDemoProjectStatusRequest,
) -> DemoProjectStatusResult:
    demo_project = request.demo_project
    status = request.status
    if not isinstance(demo_project, dict) or not isinstance(status, dict):
        raise non_retryable_error(
            "record_demo_project_status requires demoProject and status objects",
            error_type=VALIDATION_ERROR,
        )

    config = WorkerConfig.from_env()
    name = project_name(demo_project)
    status = status_with_reconcile_key(demo_project, status)
    try:
        patch_status(name, status, raise_errors=True)
    except ValueError as error:
        raise non_retryable_error(str(error), error_type=VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error
    return DemoProjectStatusResult(status)


@activity.defn
def record_catalog_project_status(
    request: RecordDemoProjectStatusRequest,
) -> DemoProjectStatusResult:
    demo_project = request.demo_project
    status = request.status
    if not isinstance(demo_project, dict) or not isinstance(status, dict):
        raise non_retryable_error(
            "record_catalog_project_status requires demoProject and status objects",
            error_type=VALIDATION_ERROR,
        )

    config = WorkerConfig.from_env()
    status = status_with_reconcile_key(demo_project, status)
    try:
        patch_catalog_status(demo_project, status, config, raise_errors=True)
    except ValueError as error:
        raise non_retryable_error(str(error), error_type=VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error
    return DemoProjectStatusResult(status)


def status_with_reconcile_key(
    demo_project: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    return {
        **status,
        "reconcileKey": demo_project_reconcile_key(demo_project),
    }


def smoke_check_url(request: SmokeCheckRequest, smoke_check: dict[str, Any]) -> str:
    if isinstance(smoke_check.get("url"), str):
        return smoke_check["url"]

    candidate = request.candidate
    if isinstance(candidate, dict) and isinstance(candidate.get("url"), str):
        return candidate["url"]

    host = request.host
    if not isinstance(host, str) and isinstance(request.demo_project, dict):
        host = project_host(request.demo_project)
    if not isinstance(host, str):
        host = f"{request.project}.tmprl-demo.cloud"

    path = smoke_check.get("path", "/")
    return f"https://{host}{path}"


def patch_status(
    name: str,
    status: dict[str, Any],
    *,
    raise_errors: bool = False,
) -> None:
    try:
        patch_demo_project_status(name, status, dry_run=False)
    except Exception as error:
        if missing_demo_project_status_patch_can_be_ignored(error, status):
            activity.logger.info(
                "DemoProject status patch skipped because project is already gone "
                "project=%s phase=%s",
                name,
                status.get("phase"),
            )
            return
        activity.logger.exception("DemoProject status patch failed project=%s", name)
        if raise_errors:
            raise


def missing_demo_project_status_patch_can_be_ignored(
    error: Exception,
    status: dict[str, Any],
) -> bool:
    return getattr(error, "status", None) == 404 and status.get("phase") == "Finalized"


def patch_catalog_status(
    demo_project: dict[str, Any],
    status: dict[str, Any],
    config: WorkerConfig,
    *,
    raise_errors: bool = False,
) -> None:
    primary = catalog_status_configmap(
        demo_project,
        status,
        namespace=config.catalog_status_namespace,
        name=config.catalog_status_configmap,
    )
    try:
        apply_resources([primary], dry_run=False)
    except Exception:
        activity.logger.exception(
            "Catalog status patch failed project=%s",
            project_name(demo_project),
        )
        if raise_errors:
            raise

    if config.catalog_status_mirror_namespace == config.catalog_status_namespace:
        return

    mirror = catalog_status_configmap(
        demo_project,
        status,
        namespace=config.catalog_status_mirror_namespace,
        name=config.catalog_status_configmap,
    )
    try:
        apply_resources([mirror], dry_run=False)
    except Exception:
        activity.logger.exception(
            "Catalog status mirror patch failed project=%s namespace=%s",
            project_name(demo_project),
            config.catalog_status_mirror_namespace,
        )
        if raise_errors:
            raise


def demo_project_from_request(request: dict[str, Any]) -> dict[str, Any]:
    demo_project = request.get("demoProject")
    if isinstance(demo_project, dict):
        return demo_project
    return request


def external_resources_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    external_resources = request.get("externalResources")
    if isinstance(external_resources, dict):
        return external_resources
    return None


def build_status_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    build_status = request.get("buildStatus")
    if isinstance(build_status, dict):
        return build_status
    return None


def images_from_build_status(build_status: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(build_status, dict):
        return {}
    images = build_status.get("images")
    if not isinstance(images, dict):
        return {}
    return {
        str(build_name): str(image)
        for build_name, image in images.items()
        if isinstance(build_name, str) and isinstance(image, str) and image
    }


def demo_project_with_build_source(
    demo_project: dict[str, Any],
    build_status: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(build_status, dict):
        return demo_project
    source_commit = build_status.get("sourceCommitSha")
    if not isinstance(source_commit, str) or not source_commit:
        return demo_project

    metadata = demo_project.get("metadata", {})
    annotations = metadata.get("annotations", {}) if isinstance(metadata, dict) else {}
    if isinstance(annotations, dict) and annotations.get(SOURCE_COMMIT_ANNOTATION):
        return demo_project

    rendered = {**demo_project}
    rendered_metadata = {**metadata} if isinstance(metadata, dict) else {}
    rendered_annotations = {**annotations} if isinstance(annotations, dict) else {}
    rendered_annotations[SOURCE_COMMIT_ANNOTATION] = source_commit
    rendered_metadata["annotations"] = rendered_annotations
    rendered["metadata"] = rendered_metadata
    return rendered
