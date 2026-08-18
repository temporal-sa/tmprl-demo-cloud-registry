"""Thin ADK Web agent that delegates every turn to a Temporal ADK workflow."""

from __future__ import annotations

import asyncio
import hashlib
import os
from typing import AsyncGenerator

from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai import types
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowUpdateStage,
)
from temporalio.common import WorkflowIDConflictPolicy

from common import temporal_client
from common.models import (
    WAYPOINT_NAMESPACE,
    AdkSessionTurnInput,
    AdkSessionWorkflowInput,
    AdkSessionWorkflowResult,
)


TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")
DEFAULT_MODEL = os.getenv("ADK_MODEL", "gemini-3.6-flash")
WORKFLOW_STATE_KEY = "dashy_temporal_workflow_id"
TURN_STATE_KEY = "dashy_temporal_turn_id"

_client: Client | None = None
_client_lock = asyncio.Lock()


async def get_temporal_client() -> Client:
    """The Waypoint team's namespace, where the ADK session workflows live.

    Named explicitly rather than left to the SDK's "default" fallback, which would
    connect to an empty namespace and simply never find the session workflow. On
    the demo cloud platform an injected TEMPORAL_NAMESPACE supersedes it.
    """
    global _client
    if _client is None:
        async with _client_lock:
            if _client is None:
                _client = await temporal_client.connect(WAYPOINT_NAMESPACE)
    return _client


def _message_text(content: types.Content | None) -> str:
    if content is None or not content.parts:
        return ""
    return "\n".join(part.text for part in content.parts if part.text).strip()


def _is_observation_prompt(prompt: str) -> bool:
    normalized = " ".join(prompt.lower().split()).rstrip("?!.,")
    return normalized in {
        "resume",
        "check status",
        "check the status",
        "what is the result",
        "what's the result",
    }


def _workflow_id(ctx: InvocationContext) -> str:
    seed = f"{ctx.user_id}:{ctx.session.id}:{ctx.invocation_id}"
    return "adk-session-" + hashlib.sha256(seed.encode()).hexdigest()[:24]


def _text_event(ctx: InvocationContext, author: str, text: str) -> Event:
    return Event(
        invocation_id=ctx.invocation_id,
        author=author,
        content=types.Content(
            role="model",
            parts=[types.Part(text=text)],
        ),
    )


class TemporalSessionProxyAgent(BaseAgent):
    """Render a Temporal-owned Dashy session through the standard ADK Web UI."""

    async def _run_async_impl(
        self,
        ctx: InvocationContext,
    ) -> AsyncGenerator[Event, None]:
        prompt = _message_text(ctx.user_content)
        if not prompt:
            yield _text_event(ctx, self.name, "Please provide a prompt.")
            return

        client = await get_temporal_client()
        active_workflow_id = str(
            ctx.session.state.get(WORKFLOW_STATE_KEY, "")
        )
        handle = (
            client.get_workflow_handle(
                active_workflow_id,
            )
            if active_workflow_id
            else None
        )
        workflow_running = False
        started_new_workflow = False
        if handle is not None:
            try:
                description = await handle.describe()
                workflow_running = (
                    description.status == WorkflowExecutionStatus.RUNNING
                )
            except Exception:
                handle = None

        if handle is None or not workflow_running:
            workflow_id = _workflow_id(ctx)
            handle = await client.start_workflow(
                "TemporalAdkSessionWorkflow",
                AdkSessionWorkflowInput(
                    user_id=ctx.user_id,
                    session_id=ctx.session.id,
                    model=DEFAULT_MODEL,
                ),
                id=workflow_id,
                task_queue=TASK_QUEUE,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
            active_workflow_id = workflow_id
            started_new_workflow = True

        saved_turn_id = str(ctx.session.state.get(TURN_STATE_KEY, ""))
        observe_existing = (
            not started_new_workflow
            and bool(saved_turn_id)
            and _is_observation_prompt(prompt)
        )
        if observe_existing:
            update_handle = handle.get_update_handle(
                saved_turn_id,
                result_type=AdkSessionWorkflowResult,
            )
            turn_id = saved_turn_id
        else:
            turn_id = ctx.invocation_id
            update_handle = await handle.start_update(
                "run_turn",
                AdkSessionTurnInput(
                    turn_id=turn_id,
                    prompt=prompt,
                ),
                id=turn_id,
                wait_for_stage=WorkflowUpdateStage.ACCEPTED,
                result_type=AdkSessionWorkflowResult,
            )
            # Persist both recovery handles only after Temporal accepts the
            # turn. Later prompts use this same workflow; "resume" and status
            # prompts attach to this exact Update.
            yield Event(
                invocation_id=ctx.invocation_id,
                author=self.name,
                actions=EventActions(
                    state_delta={
                        WORKFLOW_STATE_KEY: active_workflow_id,
                        TURN_STATE_KEY: turn_id,
                    },
                ),
            )

        result_task = asyncio.create_task(update_handle.result())
        initial_reported = False
        while not result_task.done():
            try:
                status: AdkSessionWorkflowResult | None = await handle.query(
                    "get_turn_status",
                    turn_id,
                    result_type=AdkSessionWorkflowResult,
                )
            except Exception:
                await asyncio.sleep(0.25)
                continue
            if status is not None and status.initial_response:
                # Reattaching to a turn the approval already resolved must not
                # replay its stale waiting_for_approval line; the resumed
                # answer below supersedes it.
                if not (observe_existing and status.resumed_response):
                    yield _text_event(
                        ctx,
                        self.name,
                        status.initial_response,
                    )
                    initial_reported = True
                break
            await asyncio.sleep(0.25)

        try:
            result: AdkSessionWorkflowResult = await result_task
        except Exception as err:
            # Never leave the browser on a spinner. The workflow keeps the
            # session; the user can retry with another prompt.
            yield _text_event(
                ctx,
                self.name,
                "The durable session turn failed on "
                f"{active_workflow_id}: {type(err).__name__}: {err}",
            )
            return
        if (
            result.initial_response
            and not initial_reported
            and not (observe_existing and result.resumed_response is not None)
        ):
            yield _text_event(ctx, self.name, result.initial_response)
        if result.resumed_response:
            yield _text_event(ctx, self.name, result.resumed_response)
