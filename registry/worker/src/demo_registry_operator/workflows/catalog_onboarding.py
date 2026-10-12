"""Per-user entity workflow that takes a demo repo to a catalog pull request.

One run holds one user's draft DemoProject. The catalog web server drives it
with updates: pick a repo, edit the draft (form or YAML), and hand off by
pasting the pull request the user opened on GitHub. Every edit is checked by
scripts/validate_projects.py in an activity, so the draft the user hands off is
exactly what CI will accept. The run completes at hand-off; a repo owner merging
the PR is outside onboarding.

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

from demo_registry_operator import errors
from demo_registry_operator.activities import catalog_onboarding as activities
from demo_registry_operator.workflows import messages
from demo_registry_operator.workflows.catalog_onboarding_messages import (
    CLOSED_PHASES,
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

IDLE_TIMEOUT = timedelta(days=14)
ACTIVITY_TIMEOUT = timedelta(minutes=1)


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

    @workflow.run
    async def run(self, request: StartOnboardingRequest) -> OnboardingStatus:
        if self._status.expires_at is None:
            self._touch()
        while self._status.phase not in CLOSED_PHASES:
            if self._continue_as_new_for_worker_version or (
                workflow.info().is_continue_as_new_suggested()
            ):
                await workflow.wait_condition(workflow.all_handlers_finished)
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
                    or self._continue_as_new_for_worker_version,
                    timeout=max(remaining, timedelta(seconds=1)),
                )
            except asyncio.TimeoutError:
                if self._lock.locked():
                    # An edit is in flight; finishing it moves the deadline.
                    await workflow.wait_condition(lambda: not self._lock.locked())
                    continue
                self._status.phase = "Expired"
        await workflow.wait_condition(workflow.all_handlers_finished)
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
    def abandon(self) -> OnboardingStatus:
        if self._status.phase not in CLOSED_PHASES:
            self._status.phase = "Abandoned"
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
