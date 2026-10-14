"""Per-user entity workflow that takes a demo repo to a catalog pull request.

One run holds one user's draft DemoProject. The catalog web server drives it
with updates: pick a repo, edit the draft (form or YAML), and hand off by
pasting the pull request the user opened on GitHub. Every edit is checked by
scripts/validate_projects.py in an activity, so the draft the user hands off is
exactly what CI will accept. The run completes at hand-off; a repo owner merging
the PR is outside onboarding.

Users talk to the onboarding agent in one conversation (request_agent): ask
it to draft the project, ask about one section, or answer its questions. Each
message is one agent run, an activity started from the main loop, so the
update returns at once and the page polls status. Runs resume the agent's
saved session, so it remembers the conversation and the repo it has read.
What a run returns is a proposal the user applies or ignores, never a change
to the draft itself.

A valid draft can be test deployed (deploy_preview). The preview is its own
per-user workflow (workflows/onboarding_preview.py) that outlives this one, so
this workflow only hands it the draft; the page reads and ends the preview
directly.

The run waits on people for days, so it continues-as-new when its pinned worker
version is superseded (``check_worker_version``) or history grows, carrying the
whole status. An onboarding with no edits for ``IDLE_TIMEOUT`` expires.
"""

from __future__ import annotations

import asyncio
import dataclasses
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception
from temporalio.workflow import ActivityCancellationType

from demo_registry_operator import errors
from demo_registry_operator.activities import catalog_onboarding as activities
from demo_registry_operator.activities import onboarding_agent as agent_activities
from demo_registry_operator.activities import onboarding_preview as preview_activities
from demo_registry_operator.workflows import messages
from demo_registry_operator.workflows.catalog_onboarding_messages import (
    CLOSED_PHASES,
    AgentMessage,
    AgentProposal,
    AgentRequest,
    AgentRunRequest,
    AgentRunResult,
    AgentState,
    DeleteAgentSessionsRequest,
    DraftValidation,
    InspectRepoRequest,
    OnboardingRepo,
    OnboardingStatus,
    PullRequestCheck,
    SetRepoRequest,
    StartOnboardingRequest,
    SubmitPullRequestRequest,
    UpdateDraftRequest,
    ValidateDraftRequest,
    VerifyPullRequestRequest,
)
from demo_registry_operator.workflows.onboarding_preview_messages import (
    DeployPreviewRequest,
    PreviewStatus,
)

IDLE_TIMEOUT = timedelta(days=14)
ACTIVITY_TIMEOUT = timedelta(minutes=1)
AGENT_TIMEOUT = timedelta(minutes=30)
# The activity heartbeats every few seconds. Cancellation reaches it on a
# heartbeat, which the SDK throttles to 80% of this, so this bounds how long
# "Stop the agent" takes.
AGENT_HEARTBEAT_TIMEOUT = timedelta(seconds=30)
AGENT_MODES = frozenset({"draft", "section", "chat"})
MAX_AGENT_QUESTION_CHARS = 2000
# The conversation is carried through continue-as-new; keep it bounded.
MAX_CONVERSATION_MESSAGES = 60
MAX_AGENT_MESSAGE_CHARS = 4000
DRAFT_MESSAGE = "Draft the whole project from the repo."


@workflow.defn
class CatalogOnboardingWorkflow:
    """Entity workflow for one user's catalog onboarding."""

    @workflow.init
    def __init__(self, request: StartOnboardingRequest) -> None:
        self._status = request.carried_status or OnboardingStatus(
            phase="NeedsRepo", owner_email=request.owner_email
        )
        # Edits run activities; one at a time keeps the revision check honest.
        self._lock = asyncio.Lock()
        self._edits = 0
        self._continue_as_new_for_worker_version = False
        self._agent_pending: AgentRunRequest | None = None
        self._agent_cancel = False
        if self._status.agent and self._status.agent.phase == "Running":
            # Continued-as-new mid-run can't happen (the loop waits for the
            # agent), but never carry a run nothing is following.
            self._status.agent.phase = "Cancelled"

    @workflow.run
    async def run(self, request: StartOnboardingRequest) -> OnboardingStatus:
        if self._status.expires_at is None:
            self._touch()
        while self._status.phase not in CLOSED_PHASES:
            if self._agent_pending is not None:
                await self._run_agent(self._agent_pending)
                continue
            if self._continue_as_new_for_worker_version or (
                workflow.info().is_continue_as_new_suggested()
            ):
                await workflow.wait_condition(workflow.all_handlers_finished)
                if self._agent_pending is not None:
                    continue  # an agent request arrived while handlers drained
                workflow.continue_as_new(
                    StartOnboardingRequest(
                        owner_email=self._status.owner_email,
                        carried_status=self._status,
                    )
                )
            edits = self._edits
            remaining = _parse(self._status.expires_at) - workflow.now()
            try:
                await workflow.wait_condition(
                    lambda: self._edits != edits
                    or self._status.phase in CLOSED_PHASES
                    or self._continue_as_new_for_worker_version
                    or self._agent_pending is not None,
                    timeout=max(remaining, timedelta(seconds=1)),
                )
            except asyncio.TimeoutError:
                if self._lock.locked():
                    # An edit is in flight; finishing it moves the deadline.
                    await workflow.wait_condition(lambda: not self._lock.locked())
                    continue
                self._status.phase = "Expired"
        await workflow.wait_condition(workflow.all_handlers_finished)
        if self._status.agent and self._status.agent.phase == "Running":
            # Closed with a request still queued; it never ran.
            self._status.agent.phase = "Cancelled"
        if self._status.agent_runs:
            await self._delete_agent_sessions()
        return self._snapshot()

    @workflow.update
    def ensure(self) -> OnboardingStatus:
        """Idempotent handle for update-with-start: returns the current status."""

        return self._snapshot()

    @workflow.update
    async def set_repo(self, request: SetRepoRequest) -> OnboardingStatus:
        async with self._lock:
            self._require_open()
            self._status.busy = "Checking repository"
            try:
                repo: OnboardingRepo = await workflow.execute_activity(
                    activities.inspect_onboarding_repo,
                    InspectRepoRequest(repo_url=request.repo_url),
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=activities.ONBOARDING_RETRY_POLICY,
                )
            except ActivityError as error:
                raise self._edit_failed(error) from error
            finally:
                self._status.busy = None

            current = self._status.repo
            self._status.repo = repo
            if self._status.document is None or current is None or current.url != repo.url:
                await self._validate(ValidateDraftRequest(document=seed_document(repo)))
                self._reset_conversation()
            elif current.commit_sha != repo.commit_sha:
                # The repo moved on (perhaps with the suggested fixes); the
                # agent reads the new commit instead of reapplying its edits.
                self._status.agent_patch = ""
            self._status.phase = "Drafting"
            self._touch()
            return self._snapshot()

    @set_repo.validator
    def _validate_set_repo(self, request: SetRepoRequest) -> None:
        self._reject_if_closed()
        if not request.repo_url.strip():
            raise ValueError("A repo URL is required.")

    @workflow.update
    async def update_draft(self, request: UpdateDraftRequest) -> OnboardingStatus:
        async with self._lock:
            self._require_open()
            if request.base_revision != self._status.revision:
                raise ApplicationError(
                    "The draft changed in another tab. Reload to see the latest.",
                    type="StaleDraft",
                    non_retryable=True,
                )
            await self._validate(
                ValidateDraftRequest(document=request.document, yaml=request.yaml)
            )
            self._touch()
            return self._snapshot()

    @update_draft.validator
    def _validate_update_draft(self, request: UpdateDraftRequest) -> None:
        self._reject_if_closed()
        if self._status.repo is None:
            raise ValueError("Pick a repo first.")
        if (request.document is None) == (request.yaml is None):
            raise ValueError("Send either a document or YAML.")
        if request.base_revision != self._status.revision:
            raise ValueError("The draft changed in another tab. Reload to see the latest.")

    @workflow.update
    async def submit_pull_request(
        self, request: SubmitPullRequestRequest
    ) -> OnboardingStatus:
        async with self._lock:
            self._require_open()
            if not self._status.ready or self._status.document is None:
                raise ApplicationError(
                    "The draft has errors; fix them before handing off.",
                    type="DraftNotReady",
                    non_retryable=True,
                )
            self._status.busy = "Checking pull request"
            try:
                check: PullRequestCheck = await workflow.execute_activity(
                    activities.verify_onboarding_pull_request,
                    VerifyPullRequestRequest(
                        pull_request_url=request.pull_request_url,
                        project_path=self._status.project_path or "",
                        document=self._status.document,
                    ),
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=activities.ONBOARDING_RETRY_POLICY,
                )
            except ActivityError as error:
                raise self._edit_failed(error) from error
            finally:
                self._status.busy = None

            self._touch()
            if check.problems:
                self._status.last_error = " ".join(check.problems)
                return self._snapshot()
            self._status.pull_request_url = check.url
            self._status.last_error = None
            self._status.phase = "HandedOff"
            return self._snapshot()

    @submit_pull_request.validator
    def _validate_submit_pull_request(self, request: SubmitPullRequestRequest) -> None:
        self._reject_if_closed()
        if not request.pull_request_url.strip():
            raise ValueError("Paste the pull request link.")

    @workflow.update
    async def deploy_preview(self) -> PreviewStatus:
        """Test deploy the current draft, or redeploy it to the running preview."""

        async with self._lock:
            self._require_open()
            if not self._status.ready or self._status.document is None:
                raise ApplicationError(
                    "The draft has errors; fix them before deploying it.",
                    type="DraftNotReady",
                    non_retryable=True,
                )
            self._status.busy = "Starting test deploy"
            try:
                preview: PreviewStatus = await workflow.execute_activity(
                    preview_activities.deploy_onboarding_preview,
                    DeployPreviewRequest(
                        owner_email=self._status.owner_email,
                        document=self._status.document,
                        revision=self._status.revision,
                    ),
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=preview_activities.PREVIEW_RETRY_POLICY,
                )
            except ActivityError as error:
                raise self._edit_failed(error) from error
            finally:
                self._status.busy = None
            self._status.last_error = None
            self._touch()
            return preview

    @deploy_preview.validator
    def _validate_deploy_preview(self) -> None:
        self._reject_if_closed()
        if not self._status.ready:
            raise ValueError("The draft has errors; fix them before deploying it.")

    @workflow.update
    def abandon(self) -> OnboardingStatus:
        if self._status.phase not in CLOSED_PHASES:
            self._status.phase = "Abandoned"
        self._agent_cancel = True
        return self._snapshot()

    @workflow.update
    def request_agent(self, request: AgentRequest) -> OnboardingStatus:
        repo = self._status.repo
        assert repo is not None  # checked by the validator
        self._status.agent_runs += 1
        # The workflow id is per user and reused by every onboarding they start,
        # and the LiteLLM broker never re-issues a revoked participant. The first
        # run id keeps this unique per onboarding and survives continue-as-new.
        onboarding_id = self._onboarding_id()
        run_id = f"{onboarding_id}#{self._status.agent_runs}"
        if self._status.conversation_id is None:
            self._status.conversation_id = f"{onboarding_id}/c{self._status.agent_runs}"
        history = list(self._status.conversation)
        self._add_message(
            AgentMessage(
                role="user",
                text=(request.question or "").strip() or DRAFT_MESSAGE,
                at=workflow.now().astimezone(UTC).isoformat(),
                pointer=request.pointer,
            )
        )
        name = ((self._status.document or {}).get("metadata") or {}).get("name")
        self._agent_pending = AgentRunRequest(
            run_id=run_id,
            mode=request.mode,
            repo=repo,
            project_name=name if isinstance(name, str) else "",
            document=self._status.document,
            errors=[error.message for error in self._status.errors],
            pointer=request.pointer,
            question=request.question,
            conversation_id=self._status.conversation_id,
            onboarding_id=onboarding_id,
            history=history,
            patch=self._status.agent_patch,
        )
        self._agent_cancel = False
        self._status.agent = AgentState(
            phase="Running",
            mode=request.mode,
            run_id=run_id,
            started_at=workflow.now().astimezone(UTC).isoformat(),
            pointer=request.pointer,
            question=request.question,
        )
        self._touch()
        return self._snapshot()

    @request_agent.validator
    def _validate_request_agent(self, request: AgentRequest) -> None:
        self._reject_if_closed()
        if self._status.repo is None:
            raise ValueError("Pick a repo first.")
        if self._agent_pending is not None or (
            self._status.agent and self._status.agent.phase == "Running"
        ):
            raise ValueError("The agent is already working on this draft.")
        if request.mode not in AGENT_MODES:
            raise ValueError(f"Unknown agent mode {request.mode!r}.")
        if request.mode == "section" and not (request.question or "").strip():
            raise ValueError("Ask the agent a question about this section.")
        if request.mode == "chat" and not (request.question or "").strip():
            raise ValueError("Write a message for the agent.")
        if len(request.question or "") > MAX_AGENT_QUESTION_CHARS:
            raise ValueError("The question is too long.")

    @workflow.update
    def new_agent_conversation(self) -> OnboardingStatus:
        """Start over: the next message starts a fresh agent session."""

        self._reset_conversation()
        self._touch()
        return self._snapshot()

    @new_agent_conversation.validator
    def _validate_new_agent_conversation(self) -> None:
        self._reject_if_closed()
        if self._agent_pending is not None or (
            self._status.agent and self._status.agent.phase == "Running"
        ):
            raise ValueError("Stop the agent before starting a new conversation.")

    @workflow.update
    def cancel_agent(self) -> OnboardingStatus:
        if self._status.agent and self._status.agent.phase == "Running":
            self._agent_cancel = True
            self._status.agent.stopping = True
        return self._snapshot()

    @workflow.signal
    def check_worker_version(
        self,
        _signal: messages.CheckWorkerVersionSignal = messages.CheckWorkerVersionSignal(),
    ) -> None:
        if workflow.info().is_target_worker_deployment_version_changed():
            self._continue_as_new_for_worker_version = True

    @workflow.query
    def status(self) -> dict[str, Any]:
        return dataclasses.asdict(self._status)

    async def _run_agent(self, request: AgentRunRequest) -> None:
        agent = self._status.agent
        assert agent is not None
        handle = workflow.start_activity(
            agent_activities.run_onboarding_agent,
            request,
            start_to_close_timeout=AGENT_TIMEOUT,
            heartbeat_timeout=AGENT_HEARTBEAT_TIMEOUT,
            retry_policy=agent_activities.AGENT_RETRY_POLICY,
            # Wait for the activity to delete the Job and revoke the key.
            cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            summary=f"Onboarding agent: {request.mode}",
        )
        await workflow.wait_condition(lambda: handle.done() or self._agent_cancel)
        if not handle.done():
            handle.cancel()
        try:
            result: AgentRunResult = await handle
        except ActivityError as error:
            if not is_cancelled_exception(error):
                agent.phase = "Failed"
                agent.error = errors.failure_details(error)["message"]
            else:
                agent.phase = "Cancelled"
            self._agent_replied(
                agent.error or "Stopped before answering.", outcome=agent.phase
            )
            self._finish_agent()
            return

        proposal = AgentProposal(
            mode=request.mode,
            summary=result.summary,
            fields=list(result.fields),
            findings=list(result.findings),
            document=result.document,
            patch=result.patch,
            prompt=result.prompt,
            cost_usd=result.cost_usd,
            num_turns=result.num_turns,
            pointer=request.pointer,
            question=request.question,
            questions=list(result.questions),
        )
        if result.document is not None:
            # Same check as any edit, so the page can say whether applying the
            # proposal leaves the draft valid.
            try:
                checked: DraftValidation = await workflow.execute_activity(
                    activities.validate_onboarding_draft,
                    ValidateDraftRequest(document=result.document),
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=activities.ONBOARDING_RETRY_POLICY,
                )
                proposal.yaml = checked.yaml
                proposal.errors = list(checked.errors)
            except ActivityError as error:
                if is_cancelled_exception(error):
                    raise
                agent.error = errors.failure_details(error)["message"]
        agent.proposal = proposal
        agent.phase = "Done"
        self._status.agent_patch = result.patch
        self._status.agent_cost_usd = round(self._status.agent_cost_usd + result.cost_usd, 4)
        self._agent_replied(
            result.summary or "Done.",
            outcome="Done",
            questions=list(result.questions),
            cost_usd=result.cost_usd,
        )
        self._finish_agent()

    def _agent_replied(
        self,
        text: str,
        *,
        outcome: str,
        questions: list[str] | None = None,
        cost_usd: float = 0.0,
    ) -> None:
        self._add_message(
            AgentMessage(
                role="agent",
                text=text,
                at=workflow.now().astimezone(UTC).isoformat(),
                run=self._status.agent_runs,
                outcome=outcome,
                questions=questions or [],
                cost_usd=cost_usd,
            )
        )

    def _add_message(self, message: AgentMessage) -> None:
        if len(message.text) > MAX_AGENT_MESSAGE_CHARS:
            message = dataclasses.replace(
                message, text=message.text[:MAX_AGENT_MESSAGE_CHARS] + "…"
            )
        self._status.conversation = [
            *self._status.conversation[-(MAX_CONVERSATION_MESSAGES - 1) :],
            message,
        ]

    def _reset_conversation(self) -> None:
        self._status.conversation = []
        self._status.conversation_id = None
        self._status.agent_patch = ""
        self._status.agent = None

    async def _delete_agent_sessions(self) -> None:
        # Best effort: a session left behind holds only the public repo and
        # the draft.
        try:
            await workflow.execute_activity(
                agent_activities.delete_onboarding_agent_sessions,
                DeleteAgentSessionsRequest(onboarding_id=self._onboarding_id()),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=activities.ONBOARDING_RETRY_POLICY,
            )
        except ActivityError as error:
            workflow.logger.warning(
                "Could not delete agent sessions: %s",
                errors.failure_details(error)["message"],
            )

    def _onboarding_id(self) -> str:
        info = workflow.info()
        return f"{info.workflow_id}/{info.first_execution_run_id}"

    def _finish_agent(self) -> None:
        agent = self._status.agent
        if agent is not None:
            agent.finished_at = workflow.now().astimezone(UTC).isoformat()
            agent.stopping = False
        self._agent_pending = None
        self._agent_cancel = False
        if self._status.phase not in CLOSED_PHASES:
            self._touch()

    async def _validate(self, request: ValidateDraftRequest) -> None:
        self._status.busy = "Validating draft"
        try:
            result: DraftValidation = await workflow.execute_activity(
                activities.validate_onboarding_draft,
                request,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=activities.ONBOARDING_RETRY_POLICY,
            )
        except ActivityError as error:
            raise self._edit_failed(error) from error
        finally:
            self._status.busy = None
        # A YAML parse error keeps the last good document so the form still
        # renders; the YAML tab shows the text the user is fixing.
        if result.document is not None:
            self._status.document = result.document
        self._status.yaml = result.yaml
        self._status.errors = list(result.errors)
        self._status.project_path = result.project_path
        self._status.new_file_url = result.new_file_url
        self._status.ready = result.document is not None and not result.errors
        self._status.last_error = None
        self._status.revision += 1

    def _edit_failed(self, error: ActivityError) -> ApplicationError:
        if is_cancelled_exception(error):
            raise error
        message = errors.failure_details(error)["message"]
        self._status.last_error = message
        return ApplicationError(message, type="OnboardingEditFailed", non_retryable=True)

    def _touch(self) -> None:
        self._edits += 1
        now = workflow.now()
        self._status.updated_at = now.astimezone(UTC).isoformat()
        self._status.expires_at = (now + IDLE_TIMEOUT).astimezone(UTC).isoformat()

    def _reject_if_closed(self) -> None:
        if self._status.phase in CLOSED_PHASES:
            raise ValueError(f"This onboarding is {self._status.phase}.")

    def _require_open(self) -> None:
        # Re-checked under the lock: the phase can change while an edit waits.
        if self._status.phase in CLOSED_PHASES:
            raise ApplicationError(
                f"This onboarding is {self._status.phase}.",
                type="OnboardingClosed",
                non_retryable=True,
            )

    def _snapshot(self) -> OnboardingStatus:
        return dataclasses.replace(self._status, errors=list(self._status.errors))


def seed_document(repo: OnboardingRepo) -> dict[str, Any]:
    """The starting draft for a repo: one Dockerfile build serving HTTP on 8080.

    Most demos need more than this; it exists so the form has something to edit
    and the validator has something to point at.
    """

    name = project_name_from_repo(repo.name)
    return {
        "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
        "kind": "DemoProject",
        "metadata": {"name": name},
        "spec": {
            "registry": {
                "title": repo.name.replace("-", " ").replace("_", " ").title(),
                "summary": repo.description,
            },
            "source": {"repo": repo.url},
            "builds": {"app": {"context": ".", "dockerfile": "Dockerfile"}},
            "components": {
                "app": {
                    "build": "app",
                    "ports": [{"containerPort": 8080, "servicePort": 80}],
                }
            },
            "ingress": {"subdomain": name},
        },
    }


def project_name_from_repo(repo_name: str) -> str:
    name = re.sub(r"[^a-z0-9-]+", "-", repo_name.lower()).strip("-")
    return name[:63].rstrip("-") or "demo"


def _parse(value: str | None) -> datetime:
    parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
