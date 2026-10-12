"""Boundary payloads for the catalog onboarding workflow.

The catalog web server drives ``CatalogOnboardingWorkflow`` with plain dicts that
the SDK decodes into these dataclasses, and reads ``OnboardingStatus`` back as a
plain dict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# The workflow has stopped taking edits in these phases.
CLOSED_PHASES = frozenset({"HandedOff", "Abandoned", "Expired"})


@dataclass(frozen=True)
class OnboardingRepo:
    url: str
    owner: str
    name: str
    default_branch: str
    commit_sha: str
    description: str = ""


@dataclass(frozen=True)
class DraftError:
    message: str
    # JSON pointer into the draft document, when the validator can tell which
    # field the error is about.
    pointer: str | None = None


@dataclass
class OnboardingStatus:
    # "NeedsRepo" | "Drafting" | "HandedOff" | "Abandoned" | "Expired"
    phase: str
    owner_email: str
    revision: int = 0
    repo: OnboardingRepo | None = None
    document: dict[str, Any] | None = None
    yaml: str = ""
    errors: list[DraftError] = field(default_factory=list)
    # True when the current draft passed validation and can be handed off.
    ready: bool = False
    project_path: str | None = None
    new_file_url: str | None = None
    pull_request_url: str | None = None
    # What the workflow is doing right now, e.g. "Checking repository".
    busy: str | None = None
    last_error: str | None = None
    updated_at: str | None = None
    expires_at: str | None = None


@dataclass(frozen=True)
class StartOnboardingRequest:
    owner_email: str
    # Continue-as-new carryover; None on a fresh start.
    carried_status: OnboardingStatus | None = None


@dataclass(frozen=True)
class SetRepoRequest:
    repo_url: str


@dataclass(frozen=True)
class UpdateDraftRequest:
    # The revision the editor started from, so a stale tab can't overwrite a
    # newer draft.
    base_revision: int
    # Exactly one of these: the form edits the document, the YAML tab edits text.
    document: dict[str, Any] | None = None
    yaml: str | None = None


@dataclass(frozen=True)
class SubmitPullRequestRequest:
    pull_request_url: str


@dataclass(frozen=True)
class InspectRepoRequest:
    repo_url: str


@dataclass(frozen=True)
class ValidateDraftRequest:
    document: dict[str, Any] | None = None
    yaml: str | None = None


@dataclass(frozen=True)
class DraftValidation:
    # The parsed document; None when the YAML could not be parsed.
    document: dict[str, Any] | None
    yaml: str
    errors: list[DraftError] = field(default_factory=list)
    project_path: str | None = None
    new_file_url: str | None = None


@dataclass(frozen=True)
class VerifyPullRequestRequest:
    pull_request_url: str
    project_path: str
    document: dict[str, Any]


@dataclass(frozen=True)
class PullRequestCheck:
    url: str
    # Why the PR doesn't match the validated draft; empty when it does.
    problems: list[str] = field(default_factory=list)
    number: int | None = None
    author: str | None = None
