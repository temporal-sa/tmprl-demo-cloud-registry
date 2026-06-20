"""DemoProjectRevision lifecycle activities.

IMPORTANT: this module must NOT import ``activities/reconcile.py``. Shared planning
logic lives in the neutral ``candidate_plan`` module, which both this module and
``reconcile`` import. Keeping the dependency one-directional avoids a planning <->
apply import cycle and keeps renderability a single source of truth.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from temporalio import activity

from demo_registry_operator import kube, resources
from demo_registry_operator.candidate_plan import candidate_renderability

# The only DemoProject status keys the revision bookkeeping path may write. Patching
# is restricted to these via JSON merge patch so the parent's end-of-reconcile status
# write and the child's mid-rollout ref writes never clobber each other.
REVISION_REF_KEYS = (
    "activeRevision",
    "candidateRevision",
    "lastFailedRevision",
    "revisionCleanupStatus",
    "workerDeploymentRollbackStatus",
)


@dataclass(frozen=True)
class PlanCandidateRevisionRequest:
    demo_project: dict[str, Any]
    external_resources: dict[str, Any] | None = None
    build_status: dict[str, Any] | None = None


@dataclass(frozen=True)
class CandidateRevisionPlan:
    ready: bool
    phase: str
    message: str
    rollout_version: str | None = None
    images: dict[str, str] | None = None
    runtime_namespace: str | None = None


@activity.defn
def plan_candidate_revision(
    request: PlanCandidateRevisionRequest,
) -> CandidateRevisionPlan:
    """Wrap the shared renderability predicate as a Temporal activity result."""
    result = candidate_renderability(
        request.demo_project,
        request.external_resources,
        request.build_status,
    )
    return CandidateRevisionPlan(
        ready=result.ready,
        phase=result.phase,
        message=result.message,
        rollout_version=result.rollout_version,
        images=result.images,
        runtime_namespace=result.runtime_namespace,
    )


@dataclass(frozen=True)
class RevisionRef:
    name: str
    uid: str
    rollout_version: str
    runtime_namespace: str | None
    runtime_namespace_enabled: bool


@dataclass(frozen=True)
class EnsureRevisionRequest:
    demo_project: dict[str, Any]
    rollout_version: str
    images: dict[str, str]
    reconcile_key: str | None = None


@activity.defn
def ensure_revision(request: EnsureRevisionRequest) -> RevisionRef:
    resource = resources.revision_resource(
        request.demo_project,
        request.rollout_version,
        images=request.images,
        reconcile_key=request.reconcile_key,
    )
    kube.apply_demo_project_revision(resource, dry_run=False)
    name = resource["metadata"]["name"]
    stored = kube.read_demo_project_revision(name) or {}
    metadata = stored.get("metadata", {}) if isinstance(stored, dict) else {}
    spec = resource["spec"]
    return RevisionRef(
        name=name,
        uid=str(metadata.get("uid") or ""),
        rollout_version=request.rollout_version,
        runtime_namespace=spec.get("runtimeNamespace"),
        runtime_namespace_enabled=bool(spec.get("runtimeNamespaceEnabled")),
    )


@dataclass(frozen=True)
class SetRevisionPhaseRequest:
    name: str
    phase: str
    message: str | None = None


@activity.defn
def set_revision_phase(request: SetRevisionPhaseRequest) -> dict[str, Any]:
    status: dict[str, Any] = {"phase": request.phase}
    if request.message:
        status["message"] = request.message
    kube.patch_demo_project_revision_status(request.name, status, dry_run=False)
    return {"name": request.name, "phase": request.phase}


@dataclass(frozen=True)
class DeleteRevisionRequest:
    name: str


def _revision_ref_from_resource(resource: dict[str, Any]) -> dict[str, Any] | None:
    metadata = resource.get("metadata")
    spec = resource.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        return None
    name = metadata.get("name")
    rollout_version = spec.get("rolloutVersion")
    if not isinstance(name, str) or not name:
        return None
    if not isinstance(rollout_version, str) or not rollout_version:
        return None
    return {
        "name": name,
        "uid": str(metadata.get("uid") or ""),
        "rolloutVersion": rollout_version,
        "runtimeNamespace": spec.get("runtimeNamespace"),
        "runtimeNamespaceEnabled": bool(spec.get("runtimeNamespaceEnabled")),
    }


@dataclass(frozen=True)
class PlanRevisionDeletesRequest:
    project: str
    protected_revisions: list[str] | None = None


@activity.defn
def plan_revision_deletes(request: PlanRevisionDeletesRequest) -> dict[str, Any]:
    """Plan deletion of cluster-scoped revisions not protected by live status."""
    project = request.project
    if not isinstance(project, str) or not project:
        raise ValueError("plan_revision_deletes requires project")

    protected = {
        name
        for name in (request.protected_revisions or [])
        if isinstance(name, str) and name
    }
    selector = f"platform.temporal.io/project={project}"
    revisions = kube.list_demo_project_revisions(label_selector=selector)
    delete: list[str] = []
    refs: dict[str, dict[str, Any]] = {}
    for revision in revisions:
        ref = _revision_ref_from_resource(revision)
        if ref is None:
            continue
        name = ref["name"]
        refs[name] = ref
        if name not in protected:
            delete.append(name)

    return {
        "project": project,
        "delete": sorted(delete),
        "protected": sorted(protected),
        "refused": sorted(name for name in protected if name in refs),
        "revisions": refs,
    }


@activity.defn
def delete_revision(request: DeleteRevisionRequest) -> dict[str, Any]:
    # Foreground deletion: returns immediately; cleanup is awaited by
    # wait_for_revision_cleanup. GC reaps owned resources (Namespace or candidate
    # Deploy/Svc) before the Revision object itself disappears.
    action = kube.delete_demo_project_revision(
        request.name,
        propagation_policy="Foreground",
        dry_run=False,
    )
    return {"name": request.name, "action": action}


@dataclass(frozen=True)
class PatchRevisionRefsRequest:
    project: str
    refs: dict[str, Any]


@activity.defn
def patch_demo_project_revision_refs(
    request: PatchRevisionRefsRequest,
) -> dict[str, Any]:
    # Patch ONLY the revision-ref keys via merge patch; never replace the whole
    # status document. Unknown keys are dropped defensively.
    patch = {
        key: value
        for key, value in (request.refs or {}).items()
        if key in REVISION_REF_KEYS
    }
    if patch:
        kube.patch_demo_project_status(request.project, patch, dry_run=False)
    return {"project": request.project, "patched": sorted(patch)}


# --- Task 3.3: bounded cleanup wait ---

RUNTIME_NAMESPACE_CLEANUP_MODE = "runtime-namespace"
CONTROL_PLANE_CLEANUP_MODE = "control-plane"
_ROLLOUT_VERSION_LABEL = "platform.temporal.io/rollout-version"


def _revision_boundary_present(mode: str, boundary: dict[str, Any]) -> bool:
    namespace = boundary.get("namespace")
    if mode == RUNTIME_NAMESPACE_CLEANUP_MODE:
        return bool(namespace) and kube.read_namespace(namespace) is not None
    # control-plane: candidate Deploy/Svc for that rollout version still present.
    version = boundary.get("rolloutVersion")
    if not namespace or not version:
        return False
    selector = f"{_ROLLOUT_VERSION_LABEL}={version}"
    if kube.list_deployments(namespace, label_selector=selector):
        return True
    if kube.list_services(namespace, label_selector=selector):
        return True
    return False


def wait_for_revision_cleanup_impl(
    name: str,
    *,
    mode: str,
    boundary: dict[str, Any],
    timeout_seconds: int = 300,
    poll_seconds: int = 5,
    sleep=time.sleep,
    heartbeat=lambda *_: None,
    now=time.monotonic,
) -> dict[str, Any]:
    """Poll until the revision's cleanup boundary is gone, or the deadline hits.

    Never hangs: returns {"phase": "Terminating"} at the deadline so a stuck
    finalizer downstream surfaces as a status instead of a wedged activity.
    """
    deadline = now() + timeout_seconds
    while True:
        if not _revision_boundary_present(mode, boundary):
            return {"name": name, "mode": mode, "phase": "Cleaned"}
        if now() >= deadline:
            return {
                "name": name,
                "mode": mode,
                "phase": "Terminating",
                "message": "Revision cleanup did not finish before the deadline.",
            }
        heartbeat({"name": name, "mode": mode})
        sleep(poll_seconds)


@dataclass(frozen=True)
class WaitForRevisionCleanupRequest:
    name: str
    mode: str
    namespace: str | None = None
    rollout_version: str | None = None
    timeout_seconds: int = 300
    poll_seconds: int = 5


@activity.defn
def wait_for_revision_cleanup(
    request: WaitForRevisionCleanupRequest,
) -> dict[str, Any]:
    return wait_for_revision_cleanup_impl(
        request.name,
        mode=request.mode,
        boundary={
            "namespace": request.namespace,
            "rolloutVersion": request.rollout_version,
        },
        timeout_seconds=request.timeout_seconds,
        poll_seconds=request.poll_seconds,
        heartbeat=activity.heartbeat,
    )


# --- Task 3.4: adoption + repair (verify the LIVE boundary, don't trust status) ---


def _revision_owner_present(owner_references: Any, name: str) -> bool:
    return any(
        isinstance(ref, dict)
        and ref.get("kind") == "DemoProjectRevision"
        and ref.get("name") == name
        for ref in (owner_references or [])
    )


@dataclass(frozen=True)
class AdoptActiveRevisionRequest:
    demo_project: dict[str, Any]
    active_revision: dict[str, Any] | None
    active_runtime_namespace: str | None = None
    active_version: str | None = None
    active_images: dict[str, str] | None = None


@activity.defn
def adopt_active_revision_resources(
    request: AdoptActiveRevisionRequest,
) -> dict[str, Any]:
    ref = request.active_revision
    if not isinstance(ref, dict) or not ref.get("name"):
        return {"phase": "Skipped", "message": "No active revision recorded.", "repaired": []}

    name = ref["name"]
    repaired: list[str] = []

    # Verify the Revision object itself exists; re-create it from the recorded
    # identity if it was deleted out from under us.
    revision = kube.read_demo_project_revision(name)
    if revision is None:
        version = request.active_version or ref.get("rolloutVersion")
        if not version:
            return {
                "phase": "Repairing",
                "message": "Active revision is missing and its version is unknown.",
                "repaired": repaired,
            }
        resource = resources.revision_resource(
            request.demo_project, version, images=request.active_images or {}
        )
        kube.apply_demo_project_revision(resource, dry_run=False)
        revision = kube.read_demo_project_revision(name) or {}
        repaired.append("revision")

    metadata = revision.get("metadata", {}) if isinstance(revision, dict) else {}
    uid = str(metadata.get("uid") or ref.get("uid") or "")
    owner = kube.revision_owner_reference(name, uid)

    if request.active_runtime_namespace:
        namespace = kube.read_namespace(request.active_runtime_namespace)
        if namespace is None:
            return {
                "phase": "Repairing",
                "message": "Active runtime namespace was not found.",
                "repaired": repaired,
            }
        if not _revision_owner_present(namespace.get("ownerReferences"), name):
            kube.patch_namespace_owner_references(
                request.active_runtime_namespace, [owner], dry_run=False
            )
            repaired.append(f"namespace/{request.active_runtime_namespace}")
    else:
        version = request.active_version or ref.get("rolloutVersion")
        project_namespace = resources.project_namespace(request.demo_project)
        selector = f"platform.temporal.io/rollout-version={version}"
        # The rollout-version label only matches versioned candidate Deploy/Svc; the
        # stable Service and WorkerDeployment are never selected, so never adopted.
        for deployment in kube.list_deployments(project_namespace, label_selector=selector):
            dep_name = deployment.get("name")
            if not dep_name:
                continue
            owners = kube.read_deployment_owner_references(project_namespace, dep_name)
            if not _revision_owner_present(owners, name):
                kube.patch_deployment_owner_references(
                    project_namespace, dep_name, [owner], dry_run=False
                )
                repaired.append(f"deployment/{dep_name}")
        for service in kube.list_services(project_namespace, label_selector=selector):
            svc_name = service.get("name")
            if not svc_name:
                continue
            owners = kube.read_service_owner_references(project_namespace, svc_name)
            if not _revision_owner_present(owners, name):
                kube.patch_service_owner_references(
                    project_namespace, svc_name, [owner], dry_run=False
                )
                repaired.append(f"service/{svc_name}")

    if "revision" in repaired:
        phase = "Repairing"
    elif repaired:
        phase = "Adopted"
    else:
        phase = "Verified"
    return {"phase": phase, "name": name, "repaired": repaired}
