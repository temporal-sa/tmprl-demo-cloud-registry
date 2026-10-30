"""Per-user chat with Ziggy, the floating helper on the catalog's demos page.

Ziggy is an agent on the Temporal agent harness (temporal-agent-harness). The
catalog web server sends each message to ``ask`` through the harness's
``send_agent_message`` front door, started with the workflow when there's no
open chat, waits on the agent's event stream for the reply, then reads
``status``. A turn runs the Claude tool loop here: each model call is an
activity (activities/catalog_helper.py), and the tool calls it makes run as
inline harness tools (catalog_helper_tools.py) that edit the view of the demos
page. The view (which demos Ziggy filtered to and which it highlighted) lives
here, so a reload restores it; the page clears it with ``clear_view``.

A chat with no messages for ``IDLE_TIMEOUT`` expires, and "New chat" ends it,
so the next message starts a fresh run. Chats are short and bounded by
``MAX_TURNS``, so a run finishes on the worker version it started on.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.exceptions import ActivityError, is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import AgentWorkflowRunner, agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        ToolApprovalPolicy,
    )

from demo_registry_operator import errors
from demo_registry_operator.activities import catalog_helper as activities
from demo_registry_operator.workflows.catalog_helper_messages import (
    CLOSED_PHASES,
    HelperAsk,
    HelperMessage,
    HelperModelRequest,
    HelperReply,
    HelperRound,
    HelperStatus,
    HelperView,
    StartHelperRequest,
)
from demo_registry_operator.workflows.catalog_helper_tools import HELPER_TOOLS, HelperPage

IDLE_TIMEOUT = timedelta(minutes=30)
TURN_TIMEOUT = timedelta(minutes=2)
MAX_TURNS = 30
MAX_MESSAGE_CHARS = 1000
MAX_REPLY_CHARS = 4000
# Model calls per turn. The last one may not call tools, so the turn ends
# with a reply.
MAX_ROUNDS = 4
REFUSAL_REPLY = "I can't help with that. Tell me what you'd like to demo and I'll find a fit."
# The harness's signal for stopping an agent: its turn loop drains and the
# workflow completes.
CLOSE_SIGNAL = "close"


@agent.defn(name="CatalogHelperWorkflow")
class CatalogHelperWorkflow:
    """One user's chat with Ziggy."""

    @agent.init
    def __init__(self, config: AgentConfig, data: StartHelperRequest) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # Ziggy's tools only change what the demos page shows.
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )
        self._status = HelperStatus(phase="Open", owner_email=data.owner_email)
        self._touches = 0
        self._idle_clock: asyncio.Task[None] | None = None

    @agent.setup
    async def start_idle_clock(self) -> None:
        self._touch()
        self._idle_clock = asyncio.create_task(self._expire_when_idle())

    @agent.accepts
    async def ask(self, message: HelperAsk) -> HelperReply:
        """Ask Ziggy for demos that fit a customer conversation, talk, or workshop.
        Ziggy replies and filters or highlights the demos page to match."""

        text = message.text.strip()
        problem = self._ask_problem(text)
        if problem:
            raise ValueError(problem)
        history = list(self._status.messages)
        self._add(HelperMessage(role="user", text=text, at=self._now()))
        self._status.turns += 1
        self._status.busy = True
        self._touch()
        page = HelperPage(view=self._status.view.model_copy(deep=True), titles={})
        try:
            reply = await self._run_turn(text, history, page)
        except ActivityError as error:
            if is_cancelled_exception(error):
                raise
            failed = HelperMessage(
                role="ziggy",
                text=errors.failure_details(error)["message"],
                at=self._now(),
                outcome="Failed",
            )
            self._add(failed)
            return HelperReply(text=failed.text, outcome="Failed")
        finally:
            self._status.busy = False
            self._touch()

        self._status.view = page.view
        done = HelperMessage(
            role="ziggy",
            text=reply[:MAX_REPLY_CHARS],
            at=self._now(),
            outcome="Done",
            actions=list(page.actions),
        )
        self._add(done)
        return HelperReply(text=done.text, outcome="Done", actions=done.actions)

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

        self._close("Ended")
        return self._snapshot()

    @end.validator
    def _validate_end(self) -> None:
        self._reject_if_closed()
        if self._status.busy:
            raise ValueError("Wait for Ziggy to finish answering.")

    @workflow.query
    def status(self) -> dict[str, Any]:
        return self._status.model_dump(mode="json")

    async def _run_turn(self, text: str, history: list[HelperMessage], page: HelperPage) -> str:
        """The Claude tool loop for one message; returns Ziggy's reply and
        leaves the page as Ziggy's tool calls left it."""

        deadline = workflow.now() + TURN_TIMEOUT
        rounds: list[HelperRound] = []
        texts: list[str] = []
        for round_number in range(MAX_ROUNDS):
            response = await workflow.execute_activity(
                activities.run_catalog_helper_model_call,
                HelperModelRequest(
                    message=text,
                    history=history,
                    view=self._status.view,
                    rounds=rounds,
                    last_round=round_number == MAX_ROUNDS - 1,
                ),
                # The whole turn is bounded, however many calls it takes.
                schedule_to_close_timeout=max(deadline - workflow.now(), timedelta(seconds=1)),
                retry_policy=activities.HELPER_RETRY_POLICY,
                summary=f"Ziggy model call {round_number + 1}",
            )
            self._status.input_tokens += response.input_tokens
            self._status.output_tokens += response.output_tokens
            page.titles = response.titles
            if not page.titles:
                # The catalog is empty, so there's nothing to filter to.
                page.view = HelperView()
            if response.stop_reason == "refusal":
                return REFUSAL_REPLY
            texts += [
                block["text"].strip()
                for block in response.content
                if block.get("type") == "text" and block.get("text", "").strip()
            ]
            calls = [block for block in response.content if block.get("type") == "tool_use"]
            if response.stop_reason != "tool_use" or not calls:
                break
            # In order: each call edits the page the next one sees.
            results = [await self._call_tool(call, page) for call in calls]
            # A reply that came with tool calls that all worked is the whole
            # turn: the results only confirm the page changed, and another
            # round would make the user wait for the model to say it again.
            if texts and not any(result["is_error"] for result in results):
                break
            rounds.append(HelperRound(content=response.content, tool_results=results))
        return "\n\n".join(texts) or (
            "I updated the page."
            if page.actions
            else "Sorry, I lost my train of thought. Could you say that again?"
        )

    async def _call_tool(self, call: dict[str, Any], page: HelperPage) -> dict[str, Any]:
        """Runs one tool call through the harness; a call that fails goes back to
        the model as an error result, so it can try again."""

        tool = HELPER_TOOLS.get(call.get("name", ""))
        try:
            if tool is None:
                raise ValueError(f"Unknown tool {call.get('name')}.")
            outcome = await self._runner.run_tool(
                call["id"], tool, injections={"page": page}, **(call.get("input") or {})
            )
            is_error = False
        except Exception as error:  # noqa: BLE001 - any tool failure is the model's to fix
            outcome, is_error = str(error), True
        return {
            "type": "tool_result",
            "tool_use_id": call["id"],
            "content": outcome,
            "is_error": is_error,
        }

    async def _expire_when_idle(self) -> None:
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
                if self._status.busy:
                    # A turn is running; finishing it moves the deadline.
                    await workflow.wait_condition(lambda: not self._status.busy)
                    continue
                self._close("Expired")

    def _ask_problem(self, text: str) -> str | None:
        if self._status.phase in CLOSED_PHASES:
            return "This chat has ended."
        if not text:
            return "Write a message for Ziggy."
        if len(text) > MAX_MESSAGE_CHARS:
            return f"Keep messages under {MAX_MESSAGE_CHARS} characters."
        if self._status.turns >= MAX_TURNS:
            return "This chat is at its limit. Start a new chat."
        return None

    def _close(self, phase: str) -> None:
        self._status.phase = phase
        close = workflow.get_signal_handler(CLOSE_SIGNAL)
        assert close is not None, "the agent harness registers the close signal"
        close()

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
        return self._status.model_copy(deep=True)


def _parse(value: str | None) -> datetime:
    parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
