"""DemoProjectRevision lifecycle activities.

IMPORTANT: this module must NOT import ``activities/reconcile.py``. Shared planning
logic lives in the neutral ``candidate_plan`` module, which both this module and
``reconcile`` import. Keeping the dependency one-directional avoids a planning <->
apply import cycle and keeps renderability a single source of truth.
"""

from __future__ import annotations

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
