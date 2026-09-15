"""Activities for the crashable-workspace entity workflow.

Each activity owns one bounded external-operation step and is idempotent so the
workflow can retry it safely. The workflow owns all sequencing and the saga.

The bounded retry policies live here (used by the workflow) so provisioning,
cleanup, crash, and readiness each fail fast in a way that fits their failure
mode rather than retrying forever.
"""

from __future__ import annotations

import time
from datetime import timedelta

from temporalio import activity
from temporalio.common import RetryPolicy

from demo_registry_operator import derive, errors, kube, resources
from demo_registry_operator.crashable_workspace_resources import (
    CRASHABLE_WORKER_LABEL,
    build_workspace_manifests,
)
from demo_registry_operator.workflows.crashable_workspace_messages import (
    ApplyWorkspaceResourceRequest,
    ApplyWorkspaceResourceResult,
    BuildWorkspacePlanRequest,
    BuildWorkspacePlanResult,
    CrashWorkspacePodsRequest,
    CrashWorkspacePodsResult,
    DeleteWorkspaceNamespaceRequest,
    DeleteWorkspaceNamespaceResult,
    ResolveSourceImagesRequest,
    ResolveSourceImagesResult,
    ScaleWorkspaceWorkloadsRequest,
    ScaleWorkspaceWorkloadsResult,
    WaitWorkspaceReadyRequest,
    WaitWorkspaceReadyResult,
)

# Provisioning side effects (reads + applies): retry transient Kubernetes errors a
# bounded number of times, then give up so the workflow can compensate.
PROVISIONING_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=6,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
# Cleanup must be persistent — we really want the namespace gone — so it retries
# more, but is still bounded.
CLEANUP_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
# Crash is best-effort and cheap; a couple of retries cover transient API blips.
CRASH_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
# Scaling is an explicit outage control, but Kubernetes API failures should not
# strand the workspace lifecycle. Retry transient failures briefly, then let the
# workflow return to its previous stable state while Delete/TTL remain available.
SCALE_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
# Readiness is already time-bounded by its own poll loop + start_to_close timeout;
# on timeout we want cleanup, not another full wait, so keep attempts tiny.
WAIT_READY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    maximum_attempts=2,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)

READINESS_TIMEOUT_SECONDS = 300
READINESS_POLL_SECONDS = 5
SECRET_SYNC_TIMEOUT_SECONDS = 120
SECRET_DEPENDENT_KINDS = frozenset({"Connection", "Deployment", "WorkerDeployment"})


@activity.defn
def resolve_source_images(
    request: ResolveSourceImagesRequest,
) -> ResolveSourceImagesResult:
    """Read the live demo's running images so the clone reuses them (no rebuild).

    Read the DemoProject's promoted runtime namespace, then list its Deployments
    and WorkerDeployments and map each build to its running image.
    """

    demo_project = kube.read_demo_project(request.demo_name)
    namespace = _source_runtime_namespace(demo_project, request.demo_name)
    images = kube.list_deployment_images(namespace)
    if _versioned_worker_builds(demo_project):
        images.update(kube.list_worker_deployment_images(namespace))

    missing_builds = sorted(_runtime_component_builds(demo_project) - images.keys())
    if missing_builds:
        raise errors.non_retryable_error(
            f"could not resolve running images for builds {missing_builds!r} "
            f"from namespace {namespace!r}; is the demo "
            f"{request.demo_name!r} fully deployed?",
            error_type=errors.CONFIGURATION_ERROR,
        )
    if not images:
        raise errors.non_retryable_error(
            f"could not resolve any running images from namespace {namespace!r}; "
            f"is the demo {request.demo_name!r} deployed?",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return ResolveSourceImagesResult(images=dict(images))


def _source_runtime_namespace(
    demo_project: dict | None,
    demo_name: str,
) -> str:
    """Return the namespace that contains the demo's promoted workloads."""

    status = demo_project.get("status") if isinstance(demo_project, dict) else None
    if isinstance(status, dict):
        active_revision = status.get("activeRevision")
        if isinstance(active_revision, dict):
            namespace = active_revision.get("runtimeNamespace")
            if isinstance(namespace, str) and namespace:
                return namespace

        namespace = status.get("activeRuntimeNamespace")
        if isinstance(namespace, str) and namespace:
            return namespace

    return derive.kubernetes_namespace(demo_name)


def _runtime_component_builds(demo_project: dict | None) -> set[str]:
    if not isinstance(demo_project, dict):
        return set()
    return {
        build
        for component in resources.kubernetes_components(demo_project).values()
        if isinstance((build := component.get("build")), str) and build
    }


def _versioned_worker_builds(demo_project: dict | None) -> set[str]:
    if not isinstance(demo_project, dict):
        return set()
    return {
        build
        for component in resources.kubernetes_versioned_worker_components(
            demo_project
        ).values()
        if isinstance((build := component.get("build")), str) and build
    }


def _worker_component_names(demo_project: dict | None) -> set[str]:
    if not isinstance(demo_project, dict):
        return set()
    return {
        name
        for name, component in resources.kubernetes_components(demo_project).items()
        if component.get("worker") is True
        or isinstance(component.get("worker"), dict)
    }


@activity.defn
def build_workspace_plan(
    request: BuildWorkspacePlanRequest,
) -> BuildWorkspacePlanResult:
    """Read the DemoProject CR, confirm it is crashable, and render the manifests.

    One side effect: read the CR. The render is pure. The manifests carry NO
    secret material (secrets are ExternalSecret references), so they are safe to
    keep in workflow history.
    """

    demo_project = kube.read_demo_project(request.demo_name)
    if demo_project is None:
        raise errors.non_retryable_error(
            f"DemoProject {request.demo_name!r} not found",
            error_type=errors.CONFIGURATION_ERROR,
        )
    if not _crashable_enabled(demo_project):
        raise errors.non_retryable_error(
            f"demo {request.demo_name!r} is not crashable "
            "(spec.registry.crashable.enabled is not true)",
            error_type=errors.CONFIGURATION_ERROR,
        )

    try:
        manifests, task_queue, host, namespace, deployment_names = (
            build_workspace_manifests(
                demo_project,
                request.images,
                request.workspace_id,
            )
        )
    except (ValueError, KeyError) as error:
        raise errors.non_retryable_error(
            f"failed to render workspace manifests: {error}",
            error_type=errors.VALIDATION_ERROR,
        ) from error

    return BuildWorkspacePlanResult(
        resources=manifests,
        task_queue=task_queue,
        host=host,
        namespace=namespace,
        deployment_names=deployment_names,
        worker_deployment_names=[
            manifest["metadata"]["name"]
            for manifest in manifests
            if manifest.get("kind") == "WorkerDeployment"
        ],
    )


@activity.defn
def apply_workspace_resource(
    request: ApplyWorkspaceResourceRequest,
) -> ApplyWorkspaceResourceResult:
    """Apply exactly one manifest (idempotent create-or-patch)."""

    manifest = request.manifest
    kind = manifest.get("kind")
    metadata = manifest.get("metadata")
    namespace = metadata.get("namespace") if isinstance(metadata, dict) else None
    if kind in SECRET_DEPENDENT_KINDS and isinstance(namespace, str) and namespace:
        _wait_for_workspace_external_secrets(namespace)

    try:
        action = kube.apply_resource(manifest)
    except ValueError as error:
        raise errors.non_retryable_error(
            str(error), error_type=errors.VALIDATION_ERROR
        ) from error
    return ApplyWorkspaceResourceResult(action=action)


def _wait_for_workspace_external_secrets(namespace: str) -> None:
    external_secrets = kube.list_external_secrets(namespace)
    names = sorted(
        {
            name
            for item in external_secrets
            if isinstance(item.get("metadata"), dict)
            and isinstance((name := item["metadata"].get("name")), str)
            and name
        }
    )
    if not names:
        return

    deadline = time.monotonic() + SECRET_SYNC_TIMEOUT_SECONDS
    while True:
        pending: dict[str, str | None] = {}
        for name in names:
            try:
                status = kube.read_external_secret_status(namespace, name)
            except Exception as error:  # noqa: BLE001 - transient / not observed yet
                pending[name] = str(error)
                continue
            if not status.get("ready"):
                pending[name] = status.get("message")
        if not pending:
            return
        activity.heartbeat(
            {
                "namespace": namespace,
                "pendingExternalSecrets": pending,
            }
        )
        if time.monotonic() >= deadline:
            raise errors.non_retryable_error(
                f"workspace ExternalSecrets not ready before timeout: {pending}",
                error_type=errors.CONFIGURATION_ERROR,
            )
        time.sleep(READINESS_POLL_SECONDS)


@activity.defn
def wait_workspace_ready(
    request: WaitWorkspaceReadyRequest,
) -> WaitWorkspaceReadyResult:
    """Poll until every workspace workload is ready, heartbeating each pass.

    Bounded internally (~5m) and by the caller's start_to_close timeout: if the
    workspace never becomes ready the activity raises and the workflow tears the
    attempt down.
    """

    deadline = time.monotonic() + READINESS_TIMEOUT_SECONDS
    pending_deployments = set(request.deployment_names)
    pending_worker_deployments = set(request.worker_deployment_names)
    while True:
        for name in list(pending_deployments):
            if _deployment_ready(request.namespace, name):
                pending_deployments.discard(name)
        for name in list(pending_worker_deployments):
            if _worker_deployment_ready(request.namespace, name):
                pending_worker_deployments.discard(name)
        pending = {
            "deployments": sorted(pending_deployments),
            "workerDeployments": sorted(pending_worker_deployments),
        }
        activity.heartbeat({"namespace": request.namespace, "pending": pending})
        if not pending_deployments and not pending_worker_deployments:
            return WaitWorkspaceReadyResult(ready=True)
        if time.monotonic() >= deadline:
            raise errors.non_retryable_error(
                f"workspace workloads not ready before timeout: {pending}",
                error_type=errors.CONFIGURATION_ERROR,
            )
        time.sleep(READINESS_POLL_SECONDS)


@activity.defn
def crash_workspace_pods(
    request: CrashWorkspacePodsRequest,
) -> CrashWorkspacePodsResult:
    """Force-delete every pod in the workspace namespace (grace period 0).

    The namespace is dedicated to this workspace, so no label selector is needed.
    Tolerates zero pods.
    """

    deleted = 0
    for pod in kube.list_pods(request.namespace):
        kube.delete_pod(request.namespace, pod["name"], grace_period_seconds=0)
        deleted += 1
    return CrashWorkspacePodsResult(deleted=deleted)


@activity.defn
def scale_workspace_workloads(
    request: ScaleWorkspaceWorkloadsRequest,
) -> ScaleWorkspaceWorkloadsResult:
    """Set every worker controller in the dedicated namespace to one size.

    Worker components can render as ordinary Deployments or Temporal
    WorkerDeployments. New workspaces carry the crashable-worker label;
    pre-existing workspaces fall back to their component annotation and the
    DemoProject's worker declarations. Reapplying the same replica count is safe
    when an Activity retry follows a partial failure.
    """

    if request.replicas not in {0, 1}:
        raise errors.non_retryable_error(
            "crashable workspace replicas must be 0 or 1",
            error_type=errors.VALIDATION_ERROR,
        )

    demo_project = (
        kube.read_demo_project(request.demo_name) if request.demo_name else None
    )
    worker_components = _worker_component_names(demo_project)
    deployments = sorted(
        item["name"]
        for item in kube.list_deployments(request.namespace)
        if isinstance(item.get("name"), str) and item["name"]
        and _is_worker_controller(item, worker_components)
    )
    worker_deployments = sorted(
        metadata["name"]
        for item in kube.list_worker_deployments(request.namespace)
        if isinstance((metadata := item.get("metadata")), dict)
        and isinstance(metadata.get("name"), str)
        and metadata["name"]
    )

    for name in deployments:
        kube.scale_deployment(request.namespace, name, request.replicas)
    for name in worker_deployments:
        kube.scale_worker_deployment(request.namespace, name, request.replicas)

    return ScaleWorkspaceWorkloadsResult(
        deployments=len(deployments),
        worker_deployments=len(worker_deployments),
    )


def _is_worker_controller(
    item: dict,
    worker_components: set[str],
) -> bool:
    metadata = item.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else item
    labels = metadata.get("labels")
    if (
        isinstance(labels, dict)
        and labels.get(CRASHABLE_WORKER_LABEL) == "true"
    ):
        return True
    annotations = metadata.get("annotations")
    return bool(
        isinstance(annotations, dict)
        and annotations.get("platform.temporal.io/component")
        in worker_components
    )


@activity.defn
def delete_workspace_namespace(
    request: DeleteWorkspaceNamespaceRequest,
) -> DeleteWorkspaceNamespaceResult:
    """Delete worker-controller resources, then the workspace namespace.

    Idempotent — a missing namespace returns "not-found". Doubles as the saga
    compensation for a failed provisioning attempt.
    """

    # Import locally so the Workflow sandbox can import this activity module
    # without traversing the much larger reconciliation activity module.
    from demo_registry_operator.activities.reconcile import (
        delete_runtime_namespace_in_dependency_order,
    )

    action = delete_runtime_namespace_in_dependency_order(request.namespace)
    return DeleteWorkspaceNamespaceResult(action=action)


def _deployment_ready(namespace: str, name: str) -> bool:
    try:
        status = kube.read_deployment_status(namespace, name)
    except Exception:  # noqa: BLE001 - not-yet-created / transient -> not ready
        return False
    desired = status.get("replicas")
    if not isinstance(desired, int) or desired < 1:
        # Scaled to zero (or unknown): nothing to wait for.
        return True
    generation = status.get("generation")
    observed = status.get("observedGeneration")
    if isinstance(generation, int) and isinstance(observed, int) and observed < generation:
        return False
    ready = status.get("readyReplicas") or 0
    available = status.get("availableReplicas") or 0
    updated = status.get("updatedReplicas") or 0
    return ready >= desired and available >= desired and updated >= desired


def _worker_deployment_ready(namespace: str, name: str) -> bool:
    try:
        status = kube.read_worker_deployment_status(namespace, name)
    except Exception:  # noqa: BLE001 - not-yet-created / transient -> not ready
        return False
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return False
    return any(
        isinstance(condition, dict)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions
    )


def _crashable_enabled(demo_project: dict) -> bool:
    # Opt-in isolation only: enabled AND a task-queue env var to isolate the clone.
    spec = demo_project.get("spec")
    registry = spec.get("registry") if isinstance(spec, dict) else None
    crashable = registry.get("crashable") if isinstance(registry, dict) else None
    if not isinstance(crashable, dict):
        return False
    return bool(crashable.get("enabled")) and bool(crashable.get("taskQueueEnv"))
