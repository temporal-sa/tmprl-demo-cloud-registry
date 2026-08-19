from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from google.genai import types
from temporalio.client import WorkflowExecutionStatus, WorkflowUpdateStage

from adk_agents.release_approval_agent import temporal_proxy
from adk_agents.release_approval_agent.agent import root_agent
from common.models import (
    AdkSessionTurnInput,
    AdkSessionWorkflowResult,
)


def _context(*, state=None, prompt="start scenario 3"):
    return SimpleNamespace(
        user_id="user",
        invocation_id="invocation-123",
        user_content=types.Content(
            role="user",
            parts=[types.Part(text=prompt)],
        ),
        session=SimpleNamespace(
            id="session-456",
            state=state or {},
        ),
    )


def _result(
    *,
    initial="Waiting for approval.",
    resumed="Release completed.",
):
    return AdkSessionWorkflowResult(
        workflow_id="adk-session-workflow",
        session_id="session-456",
        turn_id="invocation-123",
        initial_response=initial,
        resumed_response=resumed,
        complete=True,
    )


def _event_texts(events) -> list[str]:
    return [
        part.text
        for event in events
        if event.content and event.content.parts
        for part in event.content.parts
        if part.text
    ]


def test_proxy_starts_session_workflow_and_submits_first_turn(
    monkeypatch,
) -> None:
    class UpdateHandle:
        async def result(self):
            return _result()

    class Handle:
        def __init__(self):
            self.updates = []

        async def start_update(self, update, turn, **kwargs):
            self.updates.append((update, turn, kwargs))
            return UpdateHandle()

    class Client:
        def __init__(self):
            self.started = []
            self.handle = Handle()

        async def start_workflow(self, workflow, input, **kwargs):
            self.started.append((workflow, input, kwargs))
            return self.handle

    client = Client()

    async def get_client():
        return client

    monkeypatch.setattr(temporal_proxy, "get_temporal_client", get_client)

    async def run():
        return [
            event
            async for event in root_agent._run_async_impl(_context())
        ]

    events = asyncio.run(run())

    assert client.started[0][0] == "TemporalAdkSessionWorkflow"
    assert client.started[0][1].session_id == "session-456"
    update, turn, kwargs = client.handle.updates[0]
    assert update == "run_turn"
    assert turn == AdkSessionTurnInput(
        turn_id="invocation-123",
        prompt="start scenario 3",
    )
    assert kwargs["id"] == "invocation-123"
    assert kwargs["wait_for_stage"] == WorkflowUpdateStage.ACCEPTED
    assert events[0].actions.state_delta[
        temporal_proxy.WORKFLOW_STATE_KEY
    ].startswith("adk-session-")
    assert events[0].actions.state_delta[
        temporal_proxy.TURN_STATE_KEY
    ] == "invocation-123"
    assert _event_texts(events) == [
        "Waiting for approval.",
        "Release completed.",
    ]


def test_proxy_submits_next_turn_to_same_running_workflow(
    monkeypatch,
) -> None:
    class UpdateHandle:
        async def result(self):
            return _result(
                initial="The same session remembers the prior turn.",
                resumed=None,
            )

    class Handle:
        def __init__(self):
            self.updates = []

        async def describe(self):
            return SimpleNamespace(status=WorkflowExecutionStatus.RUNNING)

        async def start_update(self, update, turn, **kwargs):
            self.updates.append((update, turn, kwargs))
            return UpdateHandle()

    handle = Handle()

    class Client:
        def get_workflow_handle(self, workflow_id):
            assert workflow_id == "existing-workflow"
            return handle

        async def start_workflow(self, *_args, **_kwargs):
            raise AssertionError("a running session must be reused")

    async def get_client():
        return Client()

    monkeypatch.setattr(temporal_proxy, "get_temporal_client", get_client)
    ctx = _context(
        state={
            temporal_proxy.WORKFLOW_STATE_KEY: "existing-workflow",
            temporal_proxy.TURN_STATE_KEY: "prior-turn",
        },
        prompt="what did I ask before?",
    )

    async def run():
        return [
            event
            async for event in root_agent._run_async_impl(ctx)
        ]

    events = asyncio.run(run())

    assert len(handle.updates) == 1
    assert handle.updates[0][1].prompt == "what did I ask before?"
    assert _event_texts(events) == [
        "The same session remembers the prior turn."
    ]


def test_proxy_resume_observes_existing_turn_without_new_update(
    monkeypatch,
) -> None:
    class UpdateHandle:
        async def result(self):
            return _result(resumed="Release rejected.")

    class Handle:
        async def describe(self):
            return SimpleNamespace(status=WorkflowExecutionStatus.RUNNING)

        def get_update_handle(self, turn_id, *, result_type):
            assert turn_id == "existing-turn"
            assert result_type is AdkSessionWorkflowResult
            return UpdateHandle()

        async def start_update(self, *_args, **_kwargs):
            raise AssertionError("resume must not submit another turn")

    class Client:
        def get_workflow_handle(self, workflow_id):
            assert workflow_id == "existing-workflow"
            return Handle()

        async def start_workflow(self, *_args, **_kwargs):
            raise AssertionError("resume must not start another workflow")

    async def get_client():
        return Client()

    monkeypatch.setattr(temporal_proxy, "get_temporal_client", get_client)
    ctx = _context(
        state={
            temporal_proxy.WORKFLOW_STATE_KEY: "existing-workflow",
            temporal_proxy.TURN_STATE_KEY: "existing-turn",
        },
        prompt="resume",
    )

    async def run():
        return [
            event
            async for event in root_agent._run_async_impl(ctx)
        ]

    events = asyncio.run(run())

    assert _event_texts(events) == ["Release rejected."]


def test_proxy_starts_new_workflow_when_saved_one_is_not_running(
    monkeypatch,
) -> None:
    class UpdateHandle:
        async def result(self):
            return _result(initial="New session workflow.", resumed=None)

    class ClosedHandle:
        async def describe(self):
            return SimpleNamespace(status=WorkflowExecutionStatus.COMPLETED)

    class NewHandle:
        def __init__(self):
            self.updates = []

        async def start_update(self, *args, **kwargs):
            self.updates.append((args, kwargs))
            return UpdateHandle()

        def get_update_handle(self, *_args, **_kwargs):
            raise AssertionError(
                "a turn from the closed workflow must not be reused"
            )

    class Client:
        def __init__(self):
            self.started = []
            self.new_handle = NewHandle()

        def get_workflow_handle(self, workflow_id):
            assert workflow_id == "closed-workflow"
            return ClosedHandle()

        async def start_workflow(self, workflow, input, **kwargs):
            self.started.append((workflow, input, kwargs))
            return self.new_handle

    client = Client()

    async def get_client():
        return client

    monkeypatch.setattr(temporal_proxy, "get_temporal_client", get_client)
    ctx = _context(
        state={
            temporal_proxy.WORKFLOW_STATE_KEY: "closed-workflow",
            temporal_proxy.TURN_STATE_KEY: "closed-turn",
        },
        prompt="resume",
    )

    async def run():
        return [
            event
            async for event in root_agent._run_async_impl(ctx)
        ]

    events = asyncio.run(run())

    assert len(client.started) == 1
    new_workflow_id = events[0].actions.state_delta[
        temporal_proxy.WORKFLOW_STATE_KEY
    ]
    assert new_workflow_id != "closed-workflow"
    assert len(client.new_handle.updates) == 1


@pytest.mark.parametrize(
    ("prompt", "expected"),
    [
        ("resume", True),
        ("Check the status?", True),
        ("what's the result!", True),
        ("implement resume support", False),
        ("check status handling", False),
    ],
)
def test_observation_prompt_matching_is_command_scoped(
    prompt,
    expected,
) -> None:
    assert temporal_proxy._is_observation_prompt(prompt) is expected
