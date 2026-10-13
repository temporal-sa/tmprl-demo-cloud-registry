"""Per-user entity workflow for an onboarding test deploy.

A test deploy applies the user's validated draft as a hidden DemoProject named
``pv-<name>`` for at most ``PREVIEW_TTL``, so they (and anyone signed in they
share the link with) can see the demo running before the registry PR merges.

``CatalogOnboardingWorkflow`` starts it, and redeploys newer drafts to it, with
the ``deploy`` update through ``deploy_onboarding_preview``. The run is keyed
by user, not by onboarding: it outlives hand-off, so PR reviewers can open it,
and the web server ends it directly with the ``end`` update. Redeploys replace
the DemoProject in place and never move the deadline.

The watcher reconciles the preview like any other project. This workflow polls
its status for the page, and at the deadline (or on ``end``) deletes it and
waits until finalize has removed everything it created.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, is_cancelled_exception

from demo_registry_operator import errors
from demo_registry_operator.activities import onboarding_preview as activities
from demo_registry_operator.workflows import messages
from demo_registry_operator.workflows.onboarding_preview_messages import (
    LIVE_PHASES,
    AppliedPreview,
    ApplyPreviewRequest,
    PreviewDeployRequest,
    PreviewHealth,
    PreviewNameRequest,
    PreviewStatus,
    ReadPreviewRequest,
    StartPreviewRequest,
)

PREVIEW_TTL = timedelta(hours=48)
ACTIVITY_TIMEOUT = timedelta(minutes=1)
# How often the page's status is refreshed while a deploy rolls out, and how
# often a failed deploy is rechecked (the operator keeps retrying some errors).
DEPLOYING_POLL = timedelta(seconds=20)
FAILED_POLL = timedelta(minutes=5)
CLEANUP_TIMEOUT = timedelta(minutes=35)
CLEANUP_HEARTBEAT_TIMEOUT = timedelta(minutes=1)


@workflow.defn
class OnboardingPreviewWorkflow:
    """Entity workflow for one user's test deploy."""

    @workflow.init
    def __init__(self, request: StartPreviewRequest) -> None:
        self._status = request.carried_status or PreviewStatus(
            phase="Deploying", owner_email=request.owner_email
        )
        self._pending: PreviewDeployRequest | None = None
        self._end_requested = False
        self._continue_as_new_for_worker_version = False

    @workflow.run
    async def run(self, request: StartPreviewRequest) -> PreviewStatus:
        # A fresh run is started by update-with-start; the deploy arrives with it.
        await workflow.wait_condition(
            lambda: self._status.deploys > 0 or self._end_requested
        )
        try:
            await self._serve()
        finally:
            if self._status.name:
                cleanup = asyncio.create_task(self._tear_down())
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    # Cancelled while tearing down: still finish, then cancel.
                    await asyncio.shield(cleanup)
                    raise
            else:
                self._status.phase = "Ended"  # ended before anything was applied
        await workflow.wait_condition(workflow.all_handlers_finished)
        return self._snapshot()

    async def _serve(self) -> None:
        while not self._end_requested:
            if self._pending is not None:
                await self._apply(self._pending)
                continue
            remaining = _parse(self._status.expires_at) - workflow.now()
            if remaining <= timedelta(0):
                self._status.ended_reason = "Expired"
                return
            if self._continue_as_new_for_worker_version or (
                workflow.info().is_continue_as_new_suggested()
            ):
                await workflow.wait_condition(workflow.all_handlers_finished)
                if self._pending is not None or self._end_requested:
                    continue
                workflow.continue_as_new(
                    StartPreviewRequest(
                        owner_email=self._status.owner_email,
                        carried_status=self._status,
                    )
                )
            timeout = remaining
            if self._status.phase == "Deploying":
                timeout = min(remaining, DEPLOYING_POLL)
            elif self._status.phase == "Failed" and self._status.name:
                timeout = min(remaining, FAILED_POLL)
            try:
                await workflow.wait_condition(
                    lambda: self._pending is not None
                    or self._end_requested
                    or self._continue_as_new_for_worker_version,
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                if timeout < remaining:
                    await self._refresh()

    async def _apply(self, request: PreviewDeployRequest) -> None:
        self._pending = None
        self._status.step = "Applying"
        try:
            applied: AppliedPreview = await workflow.execute_activity(
                activities.apply_onboarding_preview,
                ApplyPreviewRequest(
                    owner_email=self._status.owner_email,
                    document=request.document,
                    commit_sha=request.commit_sha,
                ),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=activities.PREVIEW_RETRY_POLICY,
            )
        except ActivityError as error:
            if is_cancelled_exception(error):
                raise
            self._status.phase = "Failed"
            self._status.step = "Apply failed"
            self._status.last_error = errors.failure_details(error)["message"]
            self._updated()
            return
        self._status.name = applied.name
        self._status.url = applied.url
        self._status.step = "Waiting for the operator"
        self._updated()

    async def _refresh(self) -> None:
        if not self._status.name:
            return
        try:
            health: PreviewHealth = await workflow.execute_activity(
                activities.read_onboarding_preview,
                ReadPreviewRequest(
                    name=self._status.name, commit_sha=self._status.commit_sha
                ),
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=activities.PREVIEW_RETRY_POLICY,
            )
        except ActivityError as error:
            if is_cancelled_exception(error):
                raise
            # Transient; the next poll tries again.
            workflow.logger.warning("test deploy status read failed: %s", error)
            return
        if self._pending is not None or self._end_requested:
            return  # superseded while reading
        self._status.phase = health.phase
        self._status.stage = health.stage
        self._status.step = health.step
        self._status.detail = health.detail
        self._status.log_url = health.log_url
        self._status.last_error = health.message

    async def _tear_down(self) -> None:
        self._status.phase = "Ending"
        self._status.step = "Deleting"
        self._status.ended_reason = self._status.ended_reason or "Ended by user"
        await workflow.execute_activity(
            activities.delete_onboarding_preview,
            PreviewNameRequest(name=self._status.name),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=activities.CLEANUP_RETRY_POLICY,
        )
        self._status.step = "Waiting for cleanup"
        await workflow.execute_activity(
            activities.wait_onboarding_preview_cleanup,
            PreviewNameRequest(name=self._status.name),
            start_to_close_timeout=CLEANUP_TIMEOUT,
            heartbeat_timeout=CLEANUP_HEARTBEAT_TIMEOUT,
            retry_policy=activities.CLEANUP_RETRY_POLICY,
        )
        self._status.phase = "Ended"
        self._status.step = ""
        self._updated()

    @workflow.update
    def deploy(self, request: PreviewDeployRequest) -> PreviewStatus:
        now = workflow.now()
        if self._status.deploys == 0:
            self._status.project = request.project
            self._status.created_at = _iso(now)
            self._status.expires_at = _iso(now + PREVIEW_TTL)
        self._status.deploys += 1
        self._status.phase = "Deploying"
        self._status.stage = "resources"
        self._status.step = "Queued"
        self._status.detail = None
        self._status.log_url = None
        self._status.revision = request.revision
        self._status.commit_sha = request.commit_sha
        self._status.last_error = None
        self._pending = request
        self._updated()
        return self._snapshot()

    @deploy.validator
    def _validate_deploy(self, request: PreviewDeployRequest) -> None:
        if self._end_requested or self._status.phase not in LIVE_PHASES:
            raise ValueError("This test deploy is ending. Try again once it has ended.")
        if self._status.project and request.project != self._status.project:
            raise ValueError(
                f"Your test deploy is for {self._status.project!r}. End it before "
                f"deploying {request.project!r}."
            )

    @workflow.update
    def end(self) -> PreviewStatus:
        if not self._end_requested:
            self._end_requested = True
            if self._status.phase in LIVE_PHASES:
                self._status.phase = "Ending"
                self._status.step = "Queued"
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

    def _updated(self) -> None:
        self._status.updated_at = _iso(workflow.now())

    def _snapshot(self) -> PreviewStatus:
        return dataclasses.replace(self._status)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse(value: str | None) -> datetime:
    parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
