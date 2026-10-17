from __future__ import annotations

import asyncio
import concurrent.futures
import os
import shutil
from datetime import timedelta
from types import SimpleNamespace

import anthropic
import httpx2
import pytest
from temporalio import activity
from temporalio.client import WithStartWorkflowOperation, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from demo_registry_operator import derive
from demo_registry_operator.activities import catalog_helper
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows.catalog_helper import CatalogHelperWorkflow
from demo_registry_operator.workflows.catalog_helper_messages import (
    HelperMessage,
    HelperTurnRequest,
    HelperTurnResult,
    HelperView,
    SendHelperMessage,
    StartHelperRequest,
)

DEMOS = [
    {"id": "agent-harness", "title": "Agent Harness", "summary": "Travel agent."},
    {"id": "banking", "title": "Money Transfer", "summary": "Move money."},
    {"id": "pizza", "title": "Pizza Versioning", "summary": "Worker versioning."},
]


def text(value):
    return SimpleNamespace(type="text", text=value)


def tool_use(call_id, name, demo_ids=None):
    return SimpleNamespace(
        type="tool_use",
        id=call_id,
        name=name,
        input={} if demo_ids is None else {"demo_ids": demo_ids},
    )


def response(stop_reason, *content):
    return SimpleNamespace(
        stop_reason=stop_reason,
        content=list(content),
        usage=SimpleNamespace(
            input_tokens=10, output_tokens=5, cache_read_input_tokens=100,
            cache_creation_input_tokens=0,
        ),
    )


class FakeClient:
    """Plays back scripted model responses and records each request."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []
        self.messages = SimpleNamespace(create=self.create)

    def create(self, **kwargs):
        # The loop appends to the same list; keep what this call saw.
        self.requests.append({**kwargs, "messages": list(kwargs["messages"])})
        return self.responses.pop(0)


def test_turn_filters_and_highlights_then_replies():
    client = FakeClient(
        response(
            "tool_use",
            text("Two fit."),
            tool_use("t1", "show_demos", ["banking", "agent-harness", "nope", "banking"]),
            tool_use("t2", "highlight_demos", ["banking"]),
        ),
        response("end_turn", text("Start with Money Transfer.")),
    )
    request = HelperTurnRequest(
        turn_id="t#2",
        message="Something for a bank",
        history=[
            HelperMessage(role="user", text="hi"),
            HelperMessage(role="ziggy", text="Hello!", outcome="Done"),
        ],
    )

    result = catalog_helper.run_turn(client, request, DEMOS)

    assert result.view == HelperView(demo_ids=["banking", "agent-harness"], highlights=["banking"])
    assert result.actions == ["Showing 2 demos", "Highlighted 1"]
    assert result.reply == "Two fit.\n\nStart with Money Transfer."
    assert (result.input_tokens, result.output_tokens) == (220, 10)

    first = client.requests[0]
    assert first["model"] == catalog_helper.DEFAULT_MODEL
    assert first["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert '"id": "pizza"' in first["system"][0]["text"]
    ids_schema = first["tools"][0]["input_schema"]["properties"]["demo_ids"]
    assert ids_schema["items"]["enum"] == ["agent-harness", "banking", "pizza"]
    assert first["tool_choice"] == {"type": "auto"}
    assert first["messages"][:2] == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "Hello!"},
    ]
    note, message = first["messages"][2]["content"]
    assert note["text"] == "[The demos page currently shows every demo. Highlighted: none.]"
    assert message["text"] == "Something for a bank"

    results = client.requests[1]["messages"][-1]["content"]
    assert [r["tool_use_id"] for r in results] == ["t1", "t2"]
    assert not any(r["is_error"] for r in results)


def test_last_round_cannot_call_tools():
    calls = [response("tool_use", tool_use(f"t{i}", "clear_view")) for i in range(3)]
    client = FakeClient(*calls, response("end_turn", text("Everything's back.")))

    result = catalog_helper.run_turn(
        client,
        HelperTurnRequest(turn_id="t", message="show all", view=HelperView(["pizza"], ["pizza"])),
        DEMOS,
    )

    assert len(client.requests) == catalog_helper.MAX_ROUNDS
    assert client.requests[-1]["tool_choice"] == {"type": "none"}
    assert result.view == HelperView()
    assert result.reply == "Everything's back."


def test_refusal_and_empty_replies_still_answer():
    refused = catalog_helper.run_turn(
        FakeClient(response("refusal")), HelperTurnRequest(turn_id="t", message="x"), DEMOS
    )
    assert refused.reply.startswith("I can't help with that")

    silent = catalog_helper.run_turn(
        FakeClient(response("end_turn")), HelperTurnRequest(turn_id="t", message="x"), DEMOS
    )
    assert silent.reply.startswith("Sorry")


def test_tools_only_accept_catalog_demos():
    titles = {demo["id"]: demo["title"] for demo in DEMOS}
    view = HelperView(demo_ids=["pizza"], highlights=["pizza"])

    outcome, action, failed = catalog_helper.apply_tool(view, "show_demos", {"demo_ids": ["nope"]}, titles)
    assert failed and action is None
    assert view == HelperView(demo_ids=["pizza"], highlights=["pizza"])

    catalog_helper.apply_tool(view, "show_demos", {"demo_ids": ["banking"]}, titles)
    # A highlight outside the new filter would be invisible; drop it.
    assert view == HelperView(demo_ids=["banking"], highlights=[])

    outcome, _, failed = catalog_helper.apply_tool(view, "highlight_demos", {"demo_ids": ["pizza"]}, titles)
    assert not failed and "isn't in your current filter" in outcome


def test_conversation_skips_failed_turns_and_starts_with_the_user():
    request = HelperTurnRequest(
        turn_id="t",
        message="and in Go?",
        history=[
            HelperMessage(role="ziggy", text="cut off", outcome="Done"),
            HelperMessage(role="user", text="AI demos"),
            HelperMessage(role="ziggy", text="gateway down", outcome="Failed"),
            HelperMessage(role="user", text="AI demos please"),
            HelperMessage(role="ziggy", text="Here you go.", outcome="Done"),
        ],
        view=HelperView(demo_ids=["agent-harness"], highlights=[]),
    )

    messages = catalog_helper.conversation(request, {d["id"]: d["title"] for d in DEMOS})

    assert [(m["role"], m["content"]) for m in messages[:-1]] == [
        ("user", "AI demos"),
        ("user", "AI demos please"),
        ("assistant", "Here you go."),
    ]
    assert "only your picks: Agent Harness (agent-harness)" in messages[-1]["content"][0]["text"]


def test_catalog_demos_match_what_the_catalog_lists(monkeypatch):
    def project(name, **registry):
        return {
            "metadata": {"name": name, "labels": registry.pop("labels", {})},
            "spec": {"registry": registry, "ingress": {"subdomain": name}},
        }

    monkeypatch.setattr(
        catalog_helper.kube,
        "list_demo_projects",
        lambda: [
            project(
                "banking",
                title="Money Transfer",
                summary="Move money.",
                tags=["Payments"],
                temporalSdks=["Go"],
                canonical=True,
                links={"slideDeck": {"url": "https://x"}, "speakerNotes": {"label": "none"}},
                crashable={"enabled": True, "taskQueueEnv": "TASK_QUEUE"},
            ),
            project(
                "canary",
                labels={catalog_helper.resources.CANARY_LABEL: "true"},
            ),
        ],
    )

    demos = catalog_helper.catalog_demos()

    assert [demo["id"] for demo in demos] == ["banking"]
    assert demos[0] == {
        "id": "banking",
        "title": "Money Transfer",
        "summary": "Move money.",
        "tags": ["Payments"],
        "temporalSdks": ["Go"],
        "aiSdks": [],
        "canonical": True,
        "presenterAssets": ["slide deck"],
        "signInRequired": True,
        "privateCrashableEnvironment": True,
    }


@pytest.fixture
def broker(monkeypatch):
    seen = {"minted": [], "revoked": []}

    def mint(participant, *, track_id, policy):
        seen["minted"].append((participant, track_id, policy))
        return {"api_key": "sk-test", "litellm_base_url": "https://litellm.test"}

    def revoke(participant, *, track_id):
        seen["revoked"].append((participant, track_id))

    monkeypatch.setattr(catalog_helper.onboarding_agent, "mint_key", mint)
    monkeypatch.setattr(catalog_helper.onboarding_agent, "revoke_key", revoke)
    monkeypatch.setattr(catalog_helper, "catalog_demos", lambda: DEMOS)
    return seen


def test_turn_uses_a_short_lived_key_on_its_own_track(broker, monkeypatch):
    clients = []

    def make_client(**kwargs):
        clients.append(kwargs)
        return FakeClient(response("end_turn", text("Hi!")))

    monkeypatch.setattr(catalog_helper.anthropic, "Anthropic", make_client)

    result = ActivityEnvironment().run(
        catalog_helper.run_catalog_helper_turn, HelperTurnRequest(turn_id="chat#1", message="hi")
    )

    assert result.reply == "Hi!"
    assert clients[0]["base_url"] == "https://litellm.test"
    assert clients[0]["auth_token"] == "sk-test"
    participant, track, policy = broker["minted"][0]
    assert participant == "chat#1/1"
    assert track == "registry-catalog-helper"
    assert policy["max_budget"] <= 0.25
    assert broker["revoked"] == [("chat#1/1", "registry-catalog-helper")]


def status_error(code):
    request = httpx2.Request("POST", "https://litellm.test/v1/messages")
    return anthropic.APIStatusError(
        "nope", response=httpx2.Response(code, request=request), body=None
    )


@pytest.mark.parametrize(("code", "retryable"), [(400, False), (401, False), (429, True), (503, True)])
def test_gateway_errors_retry_only_when_they_can_pass(broker, monkeypatch, code, retryable):
    def failing(**kwargs):
        def create(**_):
            raise status_error(code)

        return SimpleNamespace(messages=SimpleNamespace(create=create))

    monkeypatch.setattr(catalog_helper.anthropic, "Anthropic", failing)

    with pytest.raises(Exception) as raised:
        ActivityEnvironment().run(
            catalog_helper.run_catalog_helper_turn, HelperTurnRequest(turn_id="c", message="hi")
        )

    if retryable:
        assert isinstance(raised.value, anthropic.APIStatusError)
    else:
        assert isinstance(raised.value, ApplicationError) and raised.value.non_retryable
    # The key is revoked whatever happened.
    assert len(broker["revoked"]) == 1


def test_helper_workflow_id_is_per_user_and_color():
    assert derive.catalog_helper_workflow_id(" Ada@Temporal.io ", "blue") == (
        "catalog-helper/blue/ada@temporal.io"
    )
    assert derive.catalog_helper_workflow_id("ada@temporal.io") == "catalog-helper/ada@temporal.io"


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)
def test_chat_turns_carry_the_view_until_cleared_and_end():
    seen: list[HelperTurnRequest] = []

    @activity.defn(name="run_catalog_helper_turn")
    def turn(request: HelperTurnRequest) -> HelperTurnResult:
        seen.append(request)
        if request.message == "break":
            raise ApplicationError("The model gateway refused the request.", non_retryable=True)
        return HelperTurnResult(
            reply=f"Re: {request.message}",
            view=HelperView(demo_ids=["banking"], highlights=["banking"]),
            actions=["Showing 1 demo"],
            input_tokens=100,
            output_tokens=20,
        )

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                async with Worker(
                    env.client,
                    task_queue="catalog-helper",
                    workflows=[CatalogHelperWorkflow],
                    activities=[turn],
                    activity_executor=executor,
                    workflow_runner=create_workflow_runner(),
                ):
                    async def send(message):
                        return await env.client.execute_update_with_start_workflow(
                            CatalogHelperWorkflow.send,
                            SendHelperMessage(text=message),
                            start_workflow_operation=WithStartWorkflowOperation(
                                CatalogHelperWorkflow.run,
                                StartHelperRequest(owner_email="ada@temporal.io"),
                                id="catalog-helper/ada@temporal.io",
                                task_queue="catalog-helper",
                                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                            ),
                        )

                    status = await send(" banks? ")
                    assert status.phase == "Open"
                    assert [m.text for m in status.messages] == ["banks?", "Re: banks?"]
                    assert status.messages[1].actions == ["Showing 1 demo"]
                    assert status.view == HelperView(demo_ids=["banking"], highlights=["banking"])
                    assert not status.busy

                    with pytest.raises(WorkflowUpdateFailedError):
                        await send("   ")

                    handle = env.client.get_workflow_handle("catalog-helper/ada@temporal.io")
                    cleared = await handle.execute_update(CatalogHelperWorkflow.clear_view)
                    assert cleared.view == HelperView()

                    failed = await send("break")
                    assert failed.messages[-1].outcome == "Failed"
                    assert "refused" in failed.messages[-1].text
                    # The failed turn leaves the view as it was.
                    assert failed.view == HelperView()

                    await send("and now?")
                    # Each turn sees the chat so far and the page as it is.
                    assert [m.text for m in seen[-1].history] == [
                        "banks?", "Re: banks?", "break", failed.messages[-1].text,
                    ]
                    assert seen[-1].view == HelperView()
                    first_run = (await handle.describe()).raw_description.workflow_execution_info.first_run_id
                    assert seen[-1].turn_id == f"catalog-helper/ada@temporal.io/{first_run}#3"

                    ended = await handle.execute_update(CatalogHelperWorkflow.end)
                    assert ended.phase == "Ended"
                    final = await handle.result()
                    assert final["phase"] == "Ended"
                    history = await handle.fetch_history()

                    # The next message starts a fresh chat.
                    fresh = await send("hello again")
                    assert [m.text for m in fresh.messages] == ["hello again", "Re: hello again"]

            await Replayer(
                workflows=[CatalogHelperWorkflow],
                workflow_runner=create_workflow_runner(),
            ).replay_workflow(history)

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)
def test_chat_expires_after_thirty_idle_minutes():
    @activity.defn(name="run_catalog_helper_turn")
    def turn(request: HelperTurnRequest) -> HelperTurnResult:
        return HelperTurnResult(reply="Hi", view=HelperView(demo_ids=["pizza"]))

    async def scenario():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with Worker(
                env.client,
                task_queue="catalog-helper",
                workflows=[CatalogHelperWorkflow],
                activities=[turn],
                activity_executor=concurrent.futures.ThreadPoolExecutor(),
                workflow_runner=create_workflow_runner(),
            ):
                handle = await env.client.start_workflow(
                    CatalogHelperWorkflow.run,
                    StartHelperRequest(owner_email="ada@temporal.io"),
                    id="catalog-helper/expiry",
                    task_queue="catalog-helper",
                )
                await handle.execute_update(CatalogHelperWorkflow.send, SendHelperMessage(text="hi"))
                await env.sleep(timedelta(minutes=29))
                status = await handle.query(CatalogHelperWorkflow.status)
                assert status["phase"] == "Open"
                await env.sleep(timedelta(minutes=2))
                final = await handle.result()
                assert final.phase == "Expired"

    asyncio.run(scenario())
