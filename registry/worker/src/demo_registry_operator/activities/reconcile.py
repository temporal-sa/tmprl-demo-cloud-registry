from dataclasses import dataclass
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from temporalio import activity

from demo_registry_operator import errors, kube, resources
from demo_registry_operator.config import WorkerConfig


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
class RestoreActiveRuntimeRoutesRequest:
    project: str
    demo_project: dict[str, Any]
    active_images: dict[str, str]
    active_version: str
    active_runtime_namespace: str | None = None


@dataclass(frozen=True)
class WorkerDeploymentRollbackRequest:
    demo_project: dict[str, Any]
    external_resources: dict[str, Any] | None
    active_images: dict[str, str]
    active_version: str
    worker_deployments: dict[str, Any]


@dataclass(frozen=True)
class PruneStaleRuntimeCandidatesRequest:
    demo_project: dict[str, Any]
    active_version: str
    active_runtime_namespace: str | None = None
    previous_runtime_namespace: str | None = None


@dataclass(frozen=True)
class DeleteRuntimeCandidateResourcesRequest:
    demo_project: dict[str, Any]
    candidate_version: str
    candidate_runtime_namespace: str | None = None


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
    name = resources.project_name(demo_project)
    image_overrides = images_from_build_status(build_status) or resources.parse_image_overrides(demo_project)
    missing_images = resources.missing_image_overrides(demo_project, image_overrides)
    build_failed = bool(build_status and build_status.get("phase") == "Error")
    external_resource_block = external_resource_blocker(external_resources)

    rendered_resources = resources.base_resources(demo_project)
    runtime_namespace_rollout = resources.runtime_namespace_rollout_enabled(
        demo_project
    )
    if not runtime_namespace_rollout:
        rendered_resources.extend(resources.runtime_secret_resources(demo_project))
    smoke_check = resources.spec_of(demo_project).get("smokeCheck")
    has_versioned_workers = bool(resources.versioned_worker_components(demo_project))
    defer_route_promotion = bool(smoke_check) or has_versioned_workers
    if not missing_images and not build_failed and external_resource_block is None:
        version = resources.candidate_version(demo_project, image_overrides)
        runtime_namespace = resources.candidate_runtime_namespace(
            demo_project,
            image_overrides,
        )
        if runtime_namespace and version:
            rendered_resources.extend(
                resources.runtime_base_resources(
                    demo_project,
                    namespace=runtime_namespace,
                    rollout_version=version,
                )
            )
            rendered_resources.extend(
                resources.runtime_secret_resources(
                    demo_project,
                    namespace=runtime_namespace,
                )
            )
        rendered_resources.extend(
            resources.runtime_workload_resources(
                demo_project,
                image_overrides,
                include_routes=not defer_route_promotion,
                external_resources=external_resources,
                namespace=runtime_namespace,
            )
        )

    activity.logger.info(
        "Reconciling DemoProject project=%s resources=%s missing_images=%s",
        name,
        len(rendered_resources),
        missing_images,
    )

    try:
        applied = kube.apply_resources(rendered_resources, dry_run=False)
        external_secret_status = external_secret_sync_status(applied)
        worker_deployments_status = worker_deployment_sync_status(applied)
    except ValueError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error

    if build_failed:
        phase = "Error"
        message = "Image build failed; workload resources were not reconciled."
    elif missing_images:
        phase = "WaitingForImages"
        if runtime_namespace_rollout:
            message = "Base resources reconciled; waiting for immutable images."
        else:
            message = (
                "Base resources and ExternalSecrets reconciled; waiting for "
                "immutable images."
            )
    elif external_resource_block:
        phase = "WaitingForExternalResources"
        message = external_resource_block["message"]
    elif has_versioned_workers:
        phase = "WorkerRolloutPending"
        message = "WorkerDeployment resources reconciled; waiting for Worker Controller rollout."
    elif resources.spec_of(demo_project).get("smokeCheck"):
        phase = "SmokeCheckPending"
        message = "Runtime resources reconciled; smoke check is ready to run."
    else:
        phase = "Ready"
        message = "Runtime resources reconciled."

    status = resources.build_reconcile_status(
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
    name = resources.project_name(demo_project)
    namespace = resources.project_namespace(demo_project)

    # Never tear down the control plane, even if a finalize somehow reached it.
    if resources.is_control_plane_project(demo_project):
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
        "runtimeNamespaces": runtime_namespaces_for_project(demo_project),
        "sourceWatchNamespace": config.source_watch_namespace,
        "gitRepository": resources.source_git_repository_name(demo_project),
        "sourceAuthSecret": resources.source_auth_kubernetes_secret_name(demo_project),
        "checkedAt": datetime.now(UTC).isoformat(),
    })


@activity.defn
def delete_project_namespace_resource(
    request: ProjectNamespaceDeleteRequest,
) -> ActivityActionResult:
    try:
        return ActivityActionResult(kube.delete_namespace(
            required_string(request.namespace, "namespace"),
            dry_run=False,
        ))
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error


@activity.defn
def delete_project_source_repository(
    request: ResourceDeleteRequest,
) -> ActivityActionResult:
    try:
        return ActivityActionResult(kube.delete_git_repository(
            required_string(request.namespace, "namespace"),
            required_string(request.name, "name"),
            dry_run=False,
        ))
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error


@activity.defn
def delete_project_source_auth_secret(
    request: OptionalResourceDeleteRequest,
) -> ActivityActionResult:
    name = request.name
    if not isinstance(name, str) or not name:
        return ActivityActionResult("not-required")
    try:
        return ActivityActionResult(kube.delete_external_secret(
            required_string(request.namespace, "namespace"),
            name,
            dry_run=False,
        ))
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error


@activity.defn
def remove_project_finalizer(
    request: ProjectFinalizerRemovalRequest,
) -> ActivityActionResult:
    try:
        kube.remove_demo_project_finalizer(
            required_string(request.project, "project"),
            dry_run=False,
        )
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error
    return ActivityActionResult("removed")


def project_finalization_status(
    plan: dict[str, Any],
    *,
    namespace_action: str,
    source_action: str,
    external: dict[str, Any],
    runtime_namespace_actions: list[dict[str, Any]] | None = None,
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
        "runtimeNamespaces": runtime_namespace_actions or [],
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
    raise errors.non_retryable_error(
        f"Activity request is missing {key}",
        error_type=errors.VALIDATION_ERROR,
    )


def runtime_namespace_from_candidate(candidate: dict[str, Any]) -> str | None:
    namespace = candidate.get("runtimeNamespace")
    return namespace if isinstance(namespace, str) and namespace else None


def runtime_namespaces_for_project(demo_project: dict[str, Any]) -> list[str]:
    project = resources.project_name(demo_project)
    control_namespace = resources.project_namespace(demo_project)
    label_selector = ",".join(
        [
            f"app.kubernetes.io/name={project}",
            "app.kubernetes.io/managed-by=demo-registry",
            f"{resources.RUNTIME_NAMESPACE_LABEL}=true",
        ]
    )
    namespaces = []
    for namespace in kube.list_namespaces(label_selector=label_selector):
        name = namespace.get("name")
        if isinstance(name, str) and name and name != control_namespace:
            namespaces.append(name)
    return sorted(set(namespaces))


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
        host = resources.project_host(demo_project)

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
        raise errors.non_retryable_error(
            "wait_for_candidate_readiness requires demoProject and candidate",
            error_type=errors.VALIDATION_ERROR,
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

    namespace = resources.project_namespace(demo_project)
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
                "project": resources.project_name(demo_project),
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
        applied = kube.apply_resources(
            resources.promotion_resources(
                promotion_project,
                image_overrides=candidate_images,
                service_namespace=runtime_namespace_from_candidate(candidate),
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
def restore_active_runtime_routes(
    request: RestoreActiveRuntimeRoutesRequest,
) -> ActivityStatusResult:
    project_name = request.project
    demo_project = request.demo_project
    active_images = request.active_images
    active_version = required_string(request.active_version, "active_version")
    if not active_images:
        return ActivityStatusResult({
            "project": project_name,
            "phase": "Skipped",
            "message": "Previous active images were not available.",
            "activeVersion": active_version,
            "appliedResources": [],
        })

    applied = kube.apply_resources(
        resources.promotion_resources(
            demo_project,
            image_overrides=active_images,
            rollout_version=active_version,
            service_namespace=request.active_runtime_namespace,
        ),
        dry_run=False,
    )
    return ActivityStatusResult({
        "project": project_name,
        "phase": "Restored",
        "message": "Previous active route resources were restored.",
        "activeVersion": active_version,
        "activeImages": active_images,
        "appliedResources": applied,
    })


@activity.defn
def rollback_worker_deployments(
    request: WorkerDeploymentRollbackRequest,
) -> ActivityStatusResult:
    """Render + apply the previous active WorkerDeployment(s) on rollout failure.

    Worker Versioning is the rollback mechanism: we re-render each Kubernetes
    versioned worker component at the previous active image/version and apply it
    (no delete, no guessed target field). In-flight pinned child workflows are
    unaffected; sunset only fires post-drain.
    """
    demo_project = request.demo_project
    active_images = request.active_images or {}
    active_version = required_string(request.active_version, "active_version")
    project = resources.project_name(demo_project)
    workers = resources.kubernetes_versioned_worker_components(demo_project)

    applied: list[dict[str, Any]] = []
    rolled_back: list[str] = []
    skipped: list[dict[str, str]] = []
    for component_name, component in sorted(workers.items()):
        build_name = component.get("build")
        if not isinstance(build_name, str) or build_name not in active_images:
            skipped.append(
                {"component": component_name, "reason": "no-active-image"}
            )
            continue
        rendered = resources.worker_deployment_resource(
            demo_project,
            component_name,
            component,
            active_images[build_name],
            version=active_version,
            external_resources=request.external_resources,
            image_overrides=active_images,
        )
        action = kube.apply_resource(rendered)
        rolled_back.append(component_name)
        metadata = rendered.get("metadata", {})
        applied.append(
            {
                "apiVersion": rendered.get("apiVersion"),
                "kind": rendered.get("kind"),
                "name": metadata.get("name"),
                "namespace": metadata.get("namespace"),
                "action": action,
            }
        )

    if not applied:
        return ActivityStatusResult({
            "project": project,
            "phase": "Skipped",
            "message": (
                "No versioned worker components had a previous active image to "
                "restore."
            ),
            "activeVersion": active_version,
            "rolledBackComponents": [],
            "skippedComponents": skipped,
            "appliedResources": [],
        })

    activity.logger.info(
        "Rolled back WorkerDeployment(s) for project=%s to version=%s components=%s",
        project,
        active_version,
        rolled_back,
    )
    return ActivityStatusResult({
        "project": project,
        "phase": "RolledBack",
        "message": (
            f"Previous active WorkerDeployment(s) restored to {active_version}."
        ),
        "activeVersion": active_version,
        "rolledBackComponents": rolled_back,
        "skippedComponents": skipped,
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
            active_runtime_namespace=request.active_runtime_namespace,
            previous_runtime_namespace=request.previous_runtime_namespace,
        )
    )


@activity.defn
def delete_runtime_candidate_resources(
    request: DeleteRuntimeCandidateResourcesRequest,
) -> ActivityStatusResult:
    return ActivityStatusResult(
        delete_runtime_candidate_resources_impl(
            request.demo_project,
            candidate_version=required_string(
                request.candidate_version,
                "candidate_version",
            ),
            candidate_runtime_namespace=request.candidate_runtime_namespace,
        )
    )


def prune_stale_runtime_candidates_impl(
    demo_project: dict[str, Any],
    *,
    active_version: str,
    active_runtime_namespace: str | None = None,
    previous_runtime_namespace: str | None = None,
) -> dict[str, Any]:
    namespace = resources.project_namespace(demo_project)
    if (
        isinstance(previous_runtime_namespace, str)
        and previous_runtime_namespace
        and previous_runtime_namespace != namespace
        and previous_runtime_namespace != active_runtime_namespace
    ):
        action = kube.delete_namespace(previous_runtime_namespace, dry_run=False)
        return {
            "phase": "Pruned",
            "message": "Deleted previous runtime namespace.",
            "activeVersion": active_version,
            "activeRuntimeNamespace": active_runtime_namespace,
            "resources": [
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "name": previous_runtime_namespace,
                    "rolloutVersion": active_version,
                    "action": action,
                }
            ],
        }

    project = resources.project_name(demo_project)
    non_worker_deployment_components = {
        component_name
        for component_name, component in resources.components(demo_project).items()
        if not resources.worker_versioning_enabled(component)
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
    pruned_resources = [
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
        "phase": "Pruned" if pruned_resources else "Skipped",
        "message": (
            f"Deleted {len(pruned_resources)} stale runtime candidate resource(s)."
            if pruned_resources
            else "No stale runtime candidate resources found."
        ),
        "activeVersion": active_version,
        "activeRuntimeNamespace": active_runtime_namespace,
        "resources": pruned_resources,
    }


def delete_runtime_candidate_resources_impl(
    demo_project: dict[str, Any],
    *,
    candidate_version: str,
    candidate_runtime_namespace: str | None = None,
) -> dict[str, Any]:
    namespace = resources.project_namespace(demo_project)
    if (
        isinstance(candidate_runtime_namespace, str)
        and candidate_runtime_namespace
        and candidate_runtime_namespace != namespace
    ):
        action = kube.delete_namespace(candidate_runtime_namespace, dry_run=False)
        return {
            "phase": "Deleted",
            "message": "Deleted failed runtime candidate namespace.",
            "candidateVersion": candidate_version,
            "candidateRuntimeNamespace": candidate_runtime_namespace,
            "resources": [
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "name": candidate_runtime_namespace,
                    "rolloutVersion": candidate_version,
                    "action": action,
                }
            ],
        }

    project = resources.project_name(demo_project)
    component_names = set(resources.components(demo_project))
    label_selector = ",".join(
        [
            f"app.kubernetes.io/name={project}",
            "app.kubernetes.io/managed-by=demo-registry",
            "platform.temporal.io/rollout-role=candidate",
            f"platform.temporal.io/rollout-version={candidate_version}",
        ]
    )
    deleted_resources = [
        *delete_candidate_deployments(
            namespace,
            candidate_version=candidate_version,
            component_names=component_names,
            label_selector=label_selector,
        ),
        *delete_candidate_services(
            namespace,
            candidate_version=candidate_version,
            component_names=component_names,
            label_selector=label_selector,
        ),
    ]
    return {
        "phase": "Deleted" if deleted_resources else "Skipped",
        "message": (
            f"Deleted {len(deleted_resources)} failed runtime candidate resource(s)."
            if deleted_resources
            else "No failed runtime candidate resources found."
        ),
        "candidateVersion": candidate_version,
        "candidateRuntimeNamespace": candidate_runtime_namespace,
        "resources": deleted_resources,
    }


def prune_stale_deployments(
    namespace: str,
    *,
    active_version: str,
    component_names: set[str],
    label_selector: str,
) -> list[dict[str, Any]]:
    deleted = []
    for deployment in kube.list_deployments(namespace, label_selector=label_selector):
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
        action = kube.delete_deployment(namespace, name, dry_run=False)
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


def delete_candidate_deployments(
    namespace: str,
    *,
    candidate_version: str,
    component_names: set[str],
    label_selector: str,
) -> list[dict[str, Any]]:
    deleted = []
    for deployment in kube.list_deployments(namespace, label_selector=label_selector):
        labels = deployment.get("labels", {})
        component = labels.get("app.kubernetes.io/component")
        version = labels.get("platform.temporal.io/rollout-version")
        name = deployment.get("name")
        if (
            not isinstance(name, str)
            or component not in component_names
            or version != candidate_version
        ):
            continue
        action = kube.delete_deployment(namespace, name, dry_run=False)
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
    for service in kube.list_services(namespace, label_selector=label_selector):
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
        action = kube.delete_service(namespace, name, dry_run=False)
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


def delete_candidate_services(
    namespace: str,
    *,
    candidate_version: str,
    component_names: set[str],
    label_selector: str,
) -> list[dict[str, Any]]:
    deleted = []
    for service in kube.list_services(namespace, label_selector=label_selector):
        labels = service.get("labels", {})
        annotations = service.get("annotations", {})
        component = labels.get("app.kubernetes.io/component")
        version = labels.get("platform.temporal.io/rollout-version")
        name = service.get("name")
        if (
            not isinstance(name, str)
            or component not in component_names
            or version != candidate_version
            or "platform.temporal.io/stable-service" not in annotations
        ):
            continue
        action = kube.delete_service(namespace, name, dry_run=False)
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
            status = kube.read_deployment_status(namespace, deployment_name)
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
        raise errors.non_retryable_error(
            "wait_for_worker_deployment_rollouts requires demoProject and workerDeployments",
            error_type=errors.VALIDATION_ERROR,
        )

    if not worker_deployments:
        return ActivityStatusResult({
            "phase": "Skipped",
            "ok": True,
            "message": "Candidate has no WorkerDeployment resources to wait for.",
            "checkedAt": datetime.now(UTC).isoformat(),
            "resources": [],
        })

    wait_seconds = positive_timeout_seconds(request.timeout_seconds) or resources.worker_deployment_rollout_wait_seconds(
        demo_project,
        base_timeout_seconds=config.readiness_timeout_seconds,
    )
    deadline = time.monotonic() + wait_seconds
    last_status: dict[str, Any] = {}
    namespace = resources.project_namespace(demo_project)
    while True:
        last_status = read_worker_deployments(namespace, worker_deployments)
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
                "project": resources.project_name(demo_project),
                "namespace": namespace,
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
            status = kube.read_worker_deployment_status(namespace, name)
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
            status = kube.read_worker_deployment_status(namespace, name)
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
    sync_resources = []
    for resource in external_secrets:
        name = resource["name"]
        namespace = resource["namespace"]
        try:
            sync_resources.append(kube.read_external_secret_status(namespace, name))
        except Exception as error:
            sync_resources.append(
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
        "phase": aggregate_external_secret_phase(sync_resources),
        "checkedAt": checked_at,
        "resources": sync_resources,
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
        raise errors.non_retryable_error(
            "record_demo_project_status requires demoProject and status objects",
            error_type=errors.VALIDATION_ERROR,
        )

    config = WorkerConfig.from_env()
    name = resources.project_name(demo_project)
    status = status_with_reconcile_key(demo_project, status)
    try:
        patch_status(name, status, raise_errors=True)
    except ValueError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error
    return DemoProjectStatusResult(status)


@activity.defn
def record_catalog_project_status(
    request: RecordDemoProjectStatusRequest,
) -> DemoProjectStatusResult:
    demo_project = request.demo_project
    status = request.status
    if not isinstance(demo_project, dict) or not isinstance(status, dict):
        raise errors.non_retryable_error(
            "record_catalog_project_status requires demoProject and status objects",
            error_type=errors.VALIDATION_ERROR,
        )

    config = WorkerConfig.from_env()
    status = status_with_reconcile_key(demo_project, status)
    try:
        patch_catalog_status(demo_project, status, config, raise_errors=True)
    except ValueError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error
    return DemoProjectStatusResult(status)


def status_with_reconcile_key(
    demo_project: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    return {
        **status,
        "reconcileKey": resources.demo_project_reconcile_key(demo_project),
    }


def smoke_check_url(request: SmokeCheckRequest, smoke_check: dict[str, Any]) -> str:
    if isinstance(smoke_check.get("url"), str):
        return smoke_check["url"]

    candidate = request.candidate
    if isinstance(candidate, dict) and isinstance(candidate.get("url"), str):
        return candidate["url"]

    host = request.host
    if not isinstance(host, str) and isinstance(request.demo_project, dict):
        host = resources.project_host(request.demo_project)
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
        kube.patch_demo_project_status(name, status, dry_run=False)
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
    primary = resources.catalog_status_configmap(
        demo_project,
        status,
        namespace=config.catalog_status_namespace,
        name=config.catalog_status_configmap,
    )
    try:
        kube.apply_resources([primary], dry_run=False)
    except Exception:
        activity.logger.exception(
            "Catalog status patch failed project=%s",
            resources.project_name(demo_project),
        )
        if raise_errors:
            raise

    if config.catalog_status_mirror_namespace == config.catalog_status_namespace:
        return

    mirror = resources.catalog_status_configmap(
        demo_project,
        status,
        namespace=config.catalog_status_mirror_namespace,
        name=config.catalog_status_configmap,
    )
    try:
        kube.apply_resources([mirror], dry_run=False)
    except Exception:
        activity.logger.exception(
            "Catalog status mirror patch failed project=%s namespace=%s",
            resources.project_name(demo_project),
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
    if isinstance(annotations, dict) and annotations.get(resources.SOURCE_COMMIT_ANNOTATION):
        return demo_project

    rendered = {**demo_project}
    rendered_metadata = {**metadata} if isinstance(metadata, dict) else {}
    rendered_annotations = {**annotations} if isinstance(annotations, dict) else {}
    rendered_annotations[resources.SOURCE_COMMIT_ANNOTATION] = source_commit
    rendered_metadata["annotations"] = rendered_annotations
    rendered["metadata"] = rendered_metadata
    return rendered
