"""Boundary payloads for the crashable-workspace entity workflow.

Every workflow/activity boundary uses a dataclass (Temporal rule 1). Requests and
results are frozen; ``WorkspaceStatus`` is mutable because the workflow keeps one
instance as its live state and mutates it as the workspace moves through its
lifecycle — the SDK serializes a snapshot each time it is returned from a query
or update.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EnsureWorkspaceRequest:
    """Run input for one user's crashable clone of one demo.

    The web caller starts the workflow with only demo_name/owner_email/ttl_seconds
    (a plain 3-key dict the SDK decodes into this dataclass), so every other field
    defaults. The ``carried_*`` fields are populated only on the continue-as-new
    path so the next run resumes without re-provisioning.
    """

    demo_name: str
    owner_email: str
    ttl_seconds: int = 3600
    carried_already_provisioned: bool = False
    carried_expires_at: str | None = None
    carried_created_at: str | None = None
    carried_task_queue: str | None = None
    carried_workspace_id: str | None = None
    carried_scaled_down: bool = False
    carried_share_epoch: int = 0


@dataclass
class WorkspaceStatus:
    phase: str
    step: str
    demo: str
    workspace_id: str
    namespace: str
    host: str
    app_url: str
    task_queue: str
    created_at: str | None = None
    expires_at: str | None = None
    last_error: str | None = None
    failure: dict[str, Any] = field(default_factory=dict)
    compensation_status: dict[str, Any] = field(default_factory=dict)
    # Bumped by revoke_share_links; share links and share cookies carry the epoch
    # they were minted under, so bumping it invalidates every outstanding link.
    share_epoch: int = 0


@dataclass(frozen=True)
class ResolveSourceImagesRequest:
    demo_name: str


@dataclass(frozen=True)
class ResolveSourceImagesResult:
    images: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class BuildWorkspacePlanRequest:
    demo_name: str
    owner_email: str
    workspace_id: str
    images: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class BuildWorkspacePlanResult:
    resources: list[dict[str, Any]] = field(default_factory=list)
    task_queue: str = ""
    host: str = ""
    namespace: str = ""
    deployment_names: list[str] = field(default_factory=list)
    worker_deployment_names: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ApplyWorkspaceResourceRequest:
    manifest: dict[str, Any]


@dataclass(frozen=True)
class ApplyWorkspaceResourceResult:
    action: str


@dataclass(frozen=True)
class WaitWorkspaceReadyRequest:
    namespace: str
    deployment_names: list[str] = field(default_factory=list)
    worker_deployment_names: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class WaitWorkspaceReadyResult:
    ready: bool


@dataclass(frozen=True)
class CrashWorkspacePodsRequest:
    namespace: str
    demo_name: str = ""


@dataclass(frozen=True)
class CrashWorkspacePodsResult:
    deleted: int


@dataclass(frozen=True)
class ScaleWorkspaceWorkloadsRequest:
    namespace: str
    replicas: int
    demo_name: str = ""


@dataclass(frozen=True)
class ScaleWorkspaceWorkloadsResult:
    deployments: int
    worker_deployments: int


@dataclass(frozen=True)
class DeleteWorkspaceNamespaceRequest:
    namespace: str


@dataclass(frozen=True)
class DeleteWorkspaceNamespaceResult:
    action: str


@dataclass(frozen=True)
class TerminateWorkspaceWorkflowsRequest:
    """Terminate executions still running on one workspace's task queue.

    ``workspace_id`` is a guard: the activity refuses any queue that is not the
    workspace's suffixed queue, so it can never target the always-on demo.
    """

    demo_name: str
    workspace_id: str
    task_queue: str


@dataclass(frozen=True)
class TerminateWorkspaceWorkflowsResult:
    action: str
    terminated: int = 0
    job_id: str | None = None
