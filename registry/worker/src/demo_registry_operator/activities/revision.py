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
