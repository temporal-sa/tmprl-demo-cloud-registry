"""Boundary payloads for the onboarding test-deploy workflow.

``CatalogOnboardingWorkflow`` starts or updates a preview through
``deploy_onboarding_preview``; the catalog web server reads ``PreviewStatus``
back as a plain dict and ends a preview with the ``end`` update.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# "Deploying": applied, waiting for the operator to roll it out.
# "Ready": the operator reports the applied commit live.
# "Failed": the apply or the rollout failed; a redeploy or the TTL ends it.
# "Ending": deleting the DemoProject and waiting for finalize.
# "Ended": everything is gone; the run completes.
LIVE_PHASES = frozenset({"Deploying", "Ready", "Failed"})
# The stages of a rollout, in order, for the page's progress list:
# Temporal Cloud and image repositories, image builds, Kubernetes resources
# (and Lambda), then worker rollout and the health check.
ROLLOUT_STAGES = ("resources", "build", "deploy", "rollout")


@dataclass
class PreviewStatus:
    phase: str
    owner_email: str
    # The draft's metadata.name, and the DemoProject actually applied (pv-<name>).
    project: str = ""
    name: str = ""
    url: str = ""
    # What is deployed: the draft revision and the demo repo commit it builds.
    revision: int = 0
    commit_sha: str = ""
    # What the operator is doing right now: one of ROLLOUT_STAGES, a short
    # step, an optional detail (a CodeBuild phase, a retry), and the build log.
    stage: str = ""
    step: str = ""
    detail: str | None = None
    log_url: str | None = None
    last_error: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    expires_at: str | None = None
    # "Expired" | "Ended by user"
    ended_reason: str | None = None
    deploys: int = 0


@dataclass(frozen=True)
class StartPreviewRequest:
    owner_email: str
    # Continue-as-new carryover; None on a fresh start.
    carried_status: PreviewStatus | None = None


@dataclass(frozen=True)
class PreviewDeployRequest:
    """One deploy of a validated draft. Built by ``deploy_onboarding_preview``."""

    project: str
    document: dict[str, Any]
    revision: int
    commit_sha: str


@dataclass(frozen=True)
class DeployPreviewRequest:
    """What the onboarding workflow asks for: deploy this validated draft."""

    owner_email: str
    document: dict[str, Any]
    revision: int


@dataclass(frozen=True)
class ApplyPreviewRequest:
    owner_email: str
    document: dict[str, Any]
    commit_sha: str


@dataclass(frozen=True)
class AppliedPreview:
    name: str
    url: str


@dataclass(frozen=True)
class ReadPreviewRequest:
    name: str
    commit_sha: str


@dataclass(frozen=True)
class PreviewHealth:
    # "Deploying" | "Ready" | "Failed"
    phase: str
    step: str = ""
    message: str | None = None
    stage: str = ""
    detail: str | None = None
    log_url: str | None = None


@dataclass(frozen=True)
class PreviewNameRequest:
    name: str
