"""Per-user chat with Ziggy, the floating helper on the catalog's demos page.

The catalog web server sends each message as an update (``send``), started
with the workflow when there's no open chat. The update runs one turn of the
helper (activities/catalog_helper.py) and returns the status, so the page gets
Ziggy's reply and the new view of the demos page in one call. The view (which
demos Ziggy filtered to and which it highlighted) lives here, so a reload
restores it; the page clears it with ``clear_view``.

A chat with no messages for ``IDLE_TIMEOUT`` expires, and "New chat" ends it,
so the next message starts a fresh run. Chats are short and bounded by
``MAX_TURNS``, so a run finishes on the worker version it started on.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception

from demo_registry_operator import errors
from demo_registry_operator.activities import catalog_helper as activities
from demo_registry_operator.workflows.catalog_helper_messages import (
    CLOSED_PHASES,
    HelperMessage,
    HelperStatus,
    HelperTurnRequest,
    HelperTurnResult,
    HelperView,
    SendHelperMessage,
    StartHelperRequest,
)

IDLE_TIMEOUT = timedelta(minutes=30)
TURN_TIMEOUT = timedelta(minutes=2)
MAX_TURNS = 30
MAX_MESSAGE_CHARS = 1000
MAX_REPLY_CHARS = 4000


@workflow.defn
class CatalogHelperWorkflow:
    """One user's chat with Ziggy."""

    @workflow.init
    def __init__(self, request: StartHelperRequest) -> None:
        self._status = HelperStatus(phase="Open", owner_email=request.owner_email)
        # One turn at a time; the validator turns a second message away.
        self._lock = asyncio.Lock()
        self._touches = 0

    @workflow.run
    async def run(self, request: StartHelperRequest) -> HelperStatus:
        self._touch()
        while self._status.phase not in CLOSED_PHASES:
            touches = self._touches
            remaining = _parse(self._status.expires_at) - workflow.now()
            try:
                await workflow.wait_condition(
                    lambda: self._touches != touches
                    or self._status.phase in CLOSED_PHASES,
                    timeout=max(remaining, timedelta(seconds=1)),
                )
            except asyncio.TimeoutError:
                if self._lock.locked():
                    # A turn is running; finishing it moves the deadline.
                    await workflow.wait_condition(lambda: not self._lock.locked())
                    continue
                self._status.phase = "Expired"
        await workflow.wait_condition(workflow.all_handlers_finished)
        return self._snapshot()

    @workflow.update
    async def send(self, request: SendHelperMessage) -> HelperStatus:
        async with self._lock:
            if self._status.phase in CLOSED_PHASES:
                raise ApplicationError(
                    "This chat has ended.", type="HelperClosed", non_retryable=True
                )
            history = list(self._status.messages)
            text = request.text.strip()
            self._add(HelperMessage(role="user", text=text, at=self._now()))
            self._status.turns += 1
            self._status.busy = True
            self._touch()
            info = workflow.info()
            try:
                result: HelperTurnResult = await workflow.execute_activity(
                    activities.run_catalog_helper_turn,
                    HelperTurnRequest(
                        turn_id=f"{info.workflow_id}/{info.first_execution_run_id}#{self._status.turns}",
                        message=text,
                        history=history,
                        view=self._status.view,
                    ),
                    start_to_close_timeout=TURN_TIMEOUT,
                    retry_policy=activities.HELPER_RETRY_POLICY,
                    summary="Ziggy turn",
                )
            except ActivityError as error:
                if is_cancelled_exception(error):
                    raise
                self._add(
                    HelperMessage(
                        role="ziggy",
                        text=errors.failure_details(error)["message"],
                        at=self._now(),
                        outcome="Failed",
                    )
                )
                return self._snapshot()
            finally:
                self._status.busy = False
                self._touch()

            self._status.view = result.view
            self._status.input_tokens += result.input_tokens
            self._status.output_tokens += result.output_tokens
            self._add(
                HelperMessage(
                    role="ziggy",
                    text=result.reply[:MAX_REPLY_CHARS],
                    at=self._now(),
                    outcome="Done",
                    actions=list(result.actions),
                )
            )
            return self._snapshot()

    @send.validator
    def _validate_send(self, request: SendHelperMessage) -> None:
        self._reject_if_closed()
        if self._lock.locked():
            raise ValueError("Ziggy is still answering your last message.")
        if not request.text.strip():
            raise ValueError("Write a message for Ziggy.")
        if len(request.text) > MAX_MESSAGE_CHARS:
            raise ValueError(f"Keep messages under {MAX_MESSAGE_CHARS} characters.")
        if self._status.turns >= MAX_TURNS:
            raise ValueError("This chat is at its limit. Start a new chat.")

    @workflow.update
    def clear_view(self) -> HelperStatus:
        """The user cleared Ziggy's filter and highlights on the page."""

        self._status.view = HelperView()
        self._touch()
        return self._snapshot()

    @clear_view.validator
    def _validate_clear_view(self) -> None:
        self._reject_if_closed()

    @workflow.update
    def end(self) -> HelperStatus:
        """New chat: end this one, so the next message starts a fresh run."""

        self._status.phase = "Ended"
        return self._snapshot()

    @end.validator
    def _validate_end(self) -> None:
        self._reject_if_closed()
        if self._lock.locked():
            raise ValueError("Wait for Ziggy to finish answering.")

    @workflow.query
    def status(self) -> dict[str, Any]:
        return dataclasses.asdict(self._status)

    def _add(self, message: HelperMessage) -> None:
        self._status.messages = [*self._status.messages, message]

    def _touch(self) -> None:
        self._touches += 1
        now = workflow.now()
        self._status.updated_at = now.astimezone(UTC).isoformat()
        self._status.expires_at = (now + IDLE_TIMEOUT).astimezone(UTC).isoformat()

    def _now(self) -> str:
        return workflow.now().astimezone(UTC).isoformat()

    def _reject_if_closed(self) -> None:
        if self._status.phase in CLOSED_PHASES:
            raise ValueError("This chat has ended.")

    def _snapshot(self) -> HelperStatus:
        return dataclasses.replace(
            self._status,
            messages=list(self._status.messages),
            view=dataclasses.replace(self._status.view),
        )


def _parse(value: str | None) -> datetime:
    parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
