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


@dataclass(frozen=True)
class AgentField:
    """Why the agent set one field, with file:line evidence from the repo."""

    pointer: str
    reason: str
    evidence: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AgentFinding:
    # "blocker" | "warning" | "suggestion"
    severity: str
    title: str
    detail: str
    location: str | None = None


@dataclass
class AgentProposal:
    """What one agent run suggests. Nothing here is applied until the user
    applies it; the document went through the same validator as any edit."""

    mode: str
    summary: str = ""
    fields: list[AgentField] = field(default_factory=list)
    findings: list[AgentFinding] = field(default_factory=list)
    document: dict[str, Any] | None = None
    yaml: str = ""
    errors: list[DraftError] = field(default_factory=list)
    # Suggested changes to the demo repo, as a unified diff, and a prompt for
    # applying them with Claude Code. Empty when the repo needs no changes.
    patch: str = ""
    prompt: str = ""
    cost_usd: float = 0.0
    num_turns: int = 0
    pointer: str | None = None
    question: str | None = None


@dataclass
class AgentState:
    # "Running" | "Done" | "Failed" | "Cancelled"
    phase: str
    mode: str
    run_id: str
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    proposal: AgentProposal | None = None
    pointer: str | None = None
    question: str | None = None
    # Stop was requested; the run ends once the activity has cleaned up.
    stopping: bool = False


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
    # The latest agent run, if any; agent_runs numbers them.
    agent: AgentState | None = None
    agent_runs: int = 0


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


@dataclass(frozen=True)
class AgentRequest:
    # "draft": write the whole DemoProject. "section": answer a question about
    # one part (pointer) and revise only that part.
    mode: str
    pointer: str | None = None
    question: str | None = None


@dataclass(frozen=True)
class AgentRunRequest:
    # Unique per agent run; the Job and the model key are named after it.
    run_id: str
    mode: str
    repo: OnboardingRepo
    project_name: str
    document: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)
    pointer: str | None = None
    question: str | None = None


@dataclass(frozen=True)
class AgentRunResult:
    summary: str
    fields: list[AgentField] = field(default_factory=list)
    findings: list[AgentFinding] = field(default_factory=list)
    document: dict[str, Any] | None = None
    patch: str = ""
    prompt: str = ""
    cost_usd: float = 0.0
    num_turns: int = 0
