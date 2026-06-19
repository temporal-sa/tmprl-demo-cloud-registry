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

from demo_registry_operator.candidate_plan import candidate_renderability


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
