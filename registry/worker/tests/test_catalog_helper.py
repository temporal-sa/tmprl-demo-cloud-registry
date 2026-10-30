from __future__ import annotations

import asyncio
import concurrent.futures
import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import anthropic
import httpx2
import pytest
from anthropic.types import TextBlock, ThinkingBlock, ToolUseBlock
from temporal_agent_harness.harness.agent_client import AgentClient, MidTurnRejectedError
from temporal_agent_harness.harness.agent_protocol import (
    AgentConfig,
    AgentEvent,
    AgentEventType,
)
from temporalio import activity
from temporalio.client import WorkflowUpdateFailedError
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from demo_registry_operator import derive
from demo_registry_operator.activities import catalog_helper
from demo_registry_operator.temporal_client import agent_harness_client
from demo_registry_operator.worker import (
    create_catalog_helper_worker,
    create_workflow_runner,
)
from demo_registry_operator.workflows import catalog_helper as helper_workflow
from demo_registry_operator.workflows.catalog_helper import CatalogHelperWorkflow
from demo_registry_operator.workflows.catalog_helper_messages import (
    HelperMessage,
    HelperModelRequest,
    HelperModelResponse,
    HelperRound,
    HelperView,
)

DEMOS = [
    {"id": "agent-harness", "title": "Agent Harness", "summary": "Travel agent."},
    {"id": "banking", "title": "Money Transfer", "summary": "Move money."},
    {"id": "pizza", "title": "Pizza Versioning", "summary": "Worker versioning."},
]
TITLES = {demo["id"]: demo["title"] for demo in DEMOS}


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
        self.requests.append(kwargs)
        return self.responses.pop(0)


def test_model_call_offers_the_catalog_and_returns_the_blocks_as_sent():
    client = FakeClient(
        response(
            "tool_use",
            ThinkingBlock(type="thinking", thinking="Banks.", signature="sig"),
            TextBlock(type="text", text="Two fit."),
            ToolUseBlock(type="tool_use", id="t1", name="show_demos", input={"demo_ids": ["banking"]}),
        ),
    )
    request = HelperModelRequest(
        message="Something for a bank",
        history=[
            HelperMessage(role="user", text="hi"),
            HelperMessage(role="ziggy", text="Hello!", outcome="Done"),
        ],
    )

    result = catalog_helper.model_call(client, request, DEMOS)

    assert result.content == [
        {"type": "thinking", "thinking": "Banks.", "signature": "sig"},
        {"type": "text", "text": "Two fit."},
        {"type": "tool_use", "id": "t1", "name": "show_demos", "input": {"demo_ids": ["banking"]}},
    ]
    assert result.stop_reason == "tool_use"
    assert result.titles == TITLES
    assert (result.input_tokens, result.output_tokens) == (110, 5)

    sent = client.requests[0]
    assert sent["model"] == catalog_helper.DEFAULT_MODEL
    assert sent["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert '"id": "pizza"' in sent["system"][0]["text"]
    assert [tool["name"] for tool in sent["tools"]] == ["show_demos", "highlight_demos", "clear_view"]
    ids_schema = sent["tools"][0]["input_schema"]["properties"]["demo_ids"]
    assert ids_schema["items"]["enum"] == ["agent-harness", "banking", "pizza"]
    # The harness tools' docstrings are the model's descriptions.
    assert sent["tools"][2]["description"] == (
        "Remove your filter and highlights so the page shows every demo."
    )
    assert sent["tool_choice"] == {"type": "auto"}
    assert sent["messages"][:2] == [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "Hello!"},
    ]
    note, message = sent["messages"][2]["content"]
    assert note["text"] == "[The demos page currently shows every demo. Highlighted: none.]"
    assert message["text"] == "Something for a bank"


def test_model_call_sends_the_turns_earlier_rounds():
    client = FakeClient(response("end_turn", TextBlock(type="text", text="Done.")))
    earlier = HelperRound(
        content=[{"type": "tool_use", "id": "t1", "name": "show_demos", "input": {"demo_ids": []}}],
        tool_results=[{"type": "tool_result", "tool_use_id": "t1", "content": "Pick one.", "is_error": True}],
    )

    catalog_helper.model_call(
        client,
        HelperModelRequest(message="bank", rounds=[earlier], last_round=True),
        DEMOS,
    )

    sent = client.requests[0]
    assert sent["tool_choice"] == {"type": "none"}
    assert sent["messages"][-2:] == [
        {"role": "assistant", "content": earlier.content},
        {"role": "user", "content": earlier.tool_results},
    ]


def test_conversation_skips_failed_turns_and_starts_with_the_user():
    request = HelperModelRequest(
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

    messages = catalog_helper.conversation(request, TITLES)

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
            project("wip", title="Work In Progress", underConstruction=True),
            project(
                "canary",
                labels={catalog_helper.resources.CANARY_LABEL: "true"},
            ),
        ],
    )

    demos = catalog_helper.catalog_demos()

    assert [demo["id"] for demo in demos] == ["banking", "wip"]
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
    assert demos[1]["underConstruction"] is True
    assert "underConstruction" not in demos[0]


@pytest.fixture
def shared_key(monkeypatch):
    reads = []

    def read():
        reads.append(catalog_helper.ANTHROPIC_SECRET)
        return "sk-ant-test"

    monkeypatch.setattr(catalog_helper, "read_anthropic_key", read)
    monkeypatch.setattr(catalog_helper, "catalog_demos", lambda: DEMOS)
    catalog_helper.forget_anthropic_client()
    yield reads
    catalog_helper.forget_anthropic_client()


def test_calls_share_one_client_on_the_platform_key(shared_key, monkeypatch):
    clients = []

    def make_client(**kwargs):
        clients.append(kwargs)
        return FakeClient(
            response("end_turn", TextBlock(type="text", text="Hi!")),
            response("end_turn", TextBlock(type="text", text="Again!")),
        )

    monkeypatch.setattr(catalog_helper.anthropic, "Anthropic", make_client)
    env = ActivityEnvironment()

    first = env.run(catalog_helper.run_catalog_helper_model_call, HelperModelRequest(message="hi"))
    second = env.run(catalog_helper.run_catalog_helper_model_call, HelperModelRequest(message="hi"))

    assert (first.content[0]["text"], second.content[0]["text"]) == ("Hi!", "Again!")
    assert len(clients) == 1
    assert clients[0]["api_key"] == "sk-ant-test"
    assert "base_url" not in clients[0]
    assert shared_key == ["tmprl-dem-cld/platform/shared/anthropic"]


def test_an_empty_catalog_answers_without_the_model(monkeypatch):
    monkeypatch.setattr(catalog_helper, "catalog_demos", lambda: [])
    monkeypatch.setattr(catalog_helper, "anthropic_client", lambda: pytest.fail("no model call"))

    result = ActivityEnvironment().run(
        catalog_helper.run_catalog_helper_model_call, HelperModelRequest(message="hi")
    )

    assert result.content == [{"type": "text", "text": catalog_helper.NO_DEMOS_REPLY}]
    assert result.titles == {}


def test_read_anthropic_key_needs_the_property(monkeypatch):
    import boto3

    values = iter(['{"ANTHROPIC_API_KEY": "sk-ant"}', '{"OTHER": "x"}'])
    secretsmanager = SimpleNamespace(
        get_secret_value=lambda SecretId: {"SecretString": next(values)}
    )
    monkeypatch.setattr(
        boto3.session, "Session", lambda **_: SimpleNamespace(client=lambda name: secretsmanager)
    )

    assert catalog_helper.read_anthropic_key() == "sk-ant"
    with pytest.raises(ApplicationError) as raised:
        catalog_helper.read_anthropic_key()
    assert raised.value.non_retryable


def status_error(code):
    request = httpx2.Request("POST", "https://api.anthropic.test/v1/messages")
    return anthropic.APIStatusError(
        "nope", response=httpx2.Response(code, request=request), body=None
    )


@pytest.mark.parametrize(("code", "retryable"), [(400, False), (401, False), (429, True), (503, True)])
def test_api_errors_retry_only_when_they_can_pass(shared_key, monkeypatch, code, retryable):
    def failing(**kwargs):
        def create(**_):
            raise status_error(code)

        return SimpleNamespace(messages=SimpleNamespace(create=create))

    monkeypatch.setattr(catalog_helper.anthropic, "Anthropic", failing)

    with pytest.raises(Exception) as raised:
        ActivityEnvironment().run(
            catalog_helper.run_catalog_helper_model_call, HelperModelRequest(message="hi")
        )

    if retryable:
        assert isinstance(raised.value, anthropic.APIStatusError)
    else:
        assert isinstance(raised.value, ApplicationError) and raised.value.non_retryable
    # A rejected key is read again on the next call, in case it was rotated.
    assert (catalog_helper._client is None) == (code == 401)


def test_helper_workflow_id_and_queue_are_per_user_and_color():
    assert derive.catalog_helper_workflow_id(" Ada@Temporal.io ", "blue") == (
        "ziggy/blue/ada@temporal.io"
    )
    assert derive.catalog_helper_workflow_id("ada@temporal.io") == "ziggy/ada@temporal.io"
    assert derive.catalog_helper_task_queue("registry-management-blue") == (
        "registry-management-blue-ziggy"
    )


# --- the agent, on a local Temporal server -------------------------------------

TASK_QUEUE = "registry-management-ziggy"
needs_temporal = pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)


def text_block(value):
    return {"type": "text", "text": value}


def tool_block(call_id, name, demo_ids=None):
    return {
        "type": "tool_use",
        "id": call_id,
        "name": name,
        "input": {} if demo_ids is None else {"demo_ids": demo_ids},
    }


def model(stop_reason, *content, titles=TITLES):
    return HelperModelResponse(
        content=list(content), stop_reason=stop_reason, titles=titles,
        input_tokens=100, output_tokens=20,
    )


class ScriptedModel:
    """Stands in for the model-call activity: answers each message with its
    scripted responses, in order, and records every request."""

    def __init__(self, script):
        self.script = {message: list(responses) for message, responses in script.items()}
        self.requests: list[HelperModelRequest] = []

        @activity.defn(name="run_catalog_helper_model_call")
        def call(request: HelperModelRequest) -> HelperModelResponse:
            self.requests.append(request)
            reply = self.script[request.message].pop(0)
            if isinstance(reply, Exception):
                raise reply
            return reply

        self.activity = call


async def ask(client, workflow_id, text):
    """Sends a message the way the catalog web server does; returns the
    harness events of its turn and the handler's error, if any."""

    agent = AgentClient(client, workflow_id)
    reply = await agent.start_and_submit_message(
        "ask",
        {"text": text},
        workflow_name="CatalogHelperWorkflow",
        task_queue=TASK_QUEUE,
        start_config=AgentConfig(),
        start_data={"owner_email": "ada@temporal.io"},
    )
    events = []
    stream = await agent.attach(on_item=lambda item, _: item, from_offset=reply.accepted_offset)
    async for item in stream:
        if not isinstance(item, AgentEvent) or item.message_id != reply.message_id:
            continue
        events.append(item.event)
        if item.event.type == AgentEventType.MESSAGE_HANDLER_END:
            return events, None
        if item.event.type == AgentEventType.MESSAGE_HANDLER_ERROR:
            return events, item.event.message
    raise AssertionError("the turn ended without an answer")


def helper_worker(client, scripted):
    return Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[CatalogHelperWorkflow],
        activities=[scripted.activity],
        activity_executor=concurrent.futures.ThreadPoolExecutor(),
        workflow_runner=create_workflow_runner(),
    )


@needs_temporal
def test_chat_turns_run_the_tool_loop_and_carry_the_view_until_cleared_and_end():
    scripted = ScriptedModel(
        {
            "banks?": [
                model(
                    "tool_use",
                    text_block("Two fit."),
                    tool_block("t1", "show_demos", ["banking", "agent-harness", "nope", "banking"]),
                    tool_block("t2", "highlight_demos", ["banking"]),
                ),
            ],
            "only good ones": [
                model("tool_use", text_block("Here you go."), tool_block("t3", "show_demos", ["nope"])),
                model(
                    "tool_use",
                    text_block("Money Transfer fits."),
                    tool_block("t4", "show_demos", ["banking"]),
                    tool_block("t5", "highlight_demos", ["pizza"]),
                ),
            ],
            "break": [ApplicationError("The model gateway refused the request.", non_retryable=True)],
            "show all": [
                *(model("tool_use", tool_block(f"c{i}", "clear_view")) for i in range(3)),
                model("end_turn", text_block("Everything's back.")),
            ],
            "nothing": [model("end_turn")],
            "rude": [model("refusal")],
            "hello again": [model("end_turn", text_block("Hi again."))],
        }
    )
    workflow_id = "ziggy/ada@temporal.io"

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            client = agent_harness_client(env.client)
            async with helper_worker(client, scripted):
                handle = client.get_workflow_handle(workflow_id)

                async def status():
                    return await handle.query("status")

                events, error = await ask(client, workflow_id, " banks? ")
                assert error is None
                chat = await status()
                assert chat["phase"] == "Open"
                assert [m["text"] for m in chat["messages"]] == ["banks?", "Two fit."]
                assert chat["messages"][1]["actions"] == ["Showing 2 demos", "Highlighted 1"]
                assert chat["view"] == {"demo_ids": ["banking", "agent-harness"], "highlights": ["banking"]}
                assert (chat["input_tokens"], chat["output_tokens"]) == (100, 20)
                assert not chat["busy"]
                # A reply with tool calls that all worked is one model call, and
                # each call shows up on the agent's stream.
                assert len(scripted.requests) == 1
                tools = [(e.type, e.tool_name) for e in events if e.type in {"tool_start", "tool_end"}]
                assert tools == [
                    ("tool_start", "show_demos"), ("tool_end", "show_demos"),
                    ("tool_start", "highlight_demos"), ("tool_end", "highlight_demos"),
                ]

                # A failed call goes back to the model, which tries again.
                _, error = await ask(client, workflow_id, "only good ones")
                assert error is None
                retry = scripted.requests[-1]
                assert [r["is_error"] for r in retry.rounds[0].tool_results] == [True]
                assert retry.rounds[0].tool_results[0]["content"] == "Pick at least one demo id from the catalog."
                assert retry.view == HelperView(demo_ids=["banking", "agent-harness"], highlights=["banking"])
                chat = await status()
                assert chat["messages"][-1]["text"] == "Here you go.\n\nMoney Transfer fits."
                assert chat["view"] == {"demo_ids": ["banking"], "highlights": ["pizza"]}

                cleared = await handle.execute_update(CatalogHelperWorkflow.clear_view)
                assert cleared.view == HelperView()

                # A failed model call fails the turn and leaves the page as it was.
                _, error = await ask(client, workflow_id, "break")
                assert error is None
                chat = await status()
                assert chat["messages"][-1]["outcome"] == "Failed"
                assert "refused" in chat["messages"][-1]["text"]
                assert chat["view"] == {"demo_ids": None, "highlights": []}

                # The last call may not use tools, so the turn ends with a reply.
                await handle.execute_update(CatalogHelperWorkflow.clear_view)
                _, error = await ask(client, workflow_id, "show all")
                assert error is None
                calls = [r for r in scripted.requests if r.message == "show all"]
                assert [r.last_round for r in calls] == [False, False, False, True]
                assert len(calls[-1].rounds) == helper_workflow.MAX_ROUNDS - 1
                chat = await status()
                assert chat["messages"][-1]["text"] == "Everything's back."
                assert chat["messages"][-1]["actions"] == ["Showing every demo"] * 3

                _, error = await ask(client, workflow_id, "nothing")
                assert (await status())["messages"][-1]["text"].startswith("Sorry")
                _, error = await ask(client, workflow_id, "rude")
                assert (await status())["messages"][-1]["text"] == helper_workflow.REFUSAL_REPLY

                # Each call sees the chat so far, minus failed turns.
                last = scripted.requests[-1]
                assert [m.text for m in last.history][:2] == ["banks?", "Two fit."]

                _, error = await ask(client, workflow_id, "   ")
                assert error == "Write a message for Ziggy."

                ended = await handle.execute_update(CatalogHelperWorkflow.end)
                assert ended.phase == "Ended"
                await handle.result()
                history = await handle.fetch_history()

                # The next message starts a fresh chat.
                _, error = await ask(client, workflow_id, "hello again")
                assert error is None
                assert [m["text"] for m in (await status())["messages"]] == ["hello again", "Hi again."]

            await Replayer(
                workflows=[CatalogHelperWorkflow],
                workflow_runner=create_workflow_runner(),
                data_converter=client.data_converter,
            ).replay_workflow(history)

    asyncio.run(scenario())


@needs_temporal
def test_the_catalog_web_server_chats_with_the_agent():
    web_root = Path(__file__).resolve().parents[2] / "web"
    if str(web_root) not in sys.path:
        sys.path.insert(0, str(web_root))
    import server

    scripted = ScriptedModel(
        {
            "banks?": [
                model("tool_use", text_block("Money Transfer."), tool_block("t1", "show_demos", ["banking"])),
            ],
        }
    )

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            gateway = server.TemporalGateway(
                address=env.client.service_client.config.target_host,
                namespace=env.client.namespace,
                api_key=None,
                tls=False,
                task_queue="registry-management",
            )
            async with helper_worker(agent_harness_client(env.client), scripted):
                chat = await asyncio.to_thread(gateway.helper_send, "ada@temporal.io", "banks?")
                assert [m["text"] for m in chat["messages"]] == ["banks?", "Money Transfer."]
                assert chat["view"] == {"demo_ids": ["banking"], "highlights": []}
                assert await asyncio.to_thread(gateway.helper_status, "ada@temporal.io") == chat

                with pytest.raises(server.HelperRejected, match="under 1000 characters"):
                    await asyncio.to_thread(gateway.helper_send, "ada@temporal.io", "x" * 1001)

                cleared = await asyncio.to_thread(gateway.helper_update, "ada@temporal.io", "clear_view")
                assert cleared["view"] == {"demo_ids": None, "highlights": []}
                ended = await asyncio.to_thread(gateway.helper_update, "ada@temporal.io", "end")
                assert ended["phase"] == "Ended"
                await env.client.get_workflow_handle("ziggy/ada@temporal.io").result()
                assert await asyncio.to_thread(gateway.helper_status, "ada@temporal.io") == {"phase": "None"}

    asyncio.run(scenario())


@needs_temporal
def test_a_second_message_waits_for_the_first():
    release = asyncio.Event()
    started = asyncio.Event()

    @activity.defn(name="run_catalog_helper_model_call")
    async def slow(request: HelperModelRequest) -> HelperModelResponse:
        started.set()
        await release.wait()
        return model("end_turn", text_block("Done."))

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            client = agent_harness_client(env.client)
            async with Worker(
                client,
                task_queue=TASK_QUEUE,
                workflows=[CatalogHelperWorkflow],
                activities=[slow],
                workflow_runner=create_workflow_runner(),
            ):
                first = asyncio.create_task(ask(client, "ziggy/busy", "one"))
                await started.wait()
                with pytest.raises(MidTurnRejectedError):
                    await ask(client, "ziggy/busy", "two")
                handle = client.get_workflow_handle("ziggy/busy")
                with pytest.raises(WorkflowUpdateFailedError):
                    await handle.execute_update(CatalogHelperWorkflow.end)
                release.set()
                assert (await first)[1] is None
                assert (await handle.query("status"))["turns"] == 1

    asyncio.run(scenario())


@needs_temporal
def test_chat_expires_after_thirty_idle_minutes():
    scripted = ScriptedModel({"hi": [model("end_turn", text_block("Hi"))]})

    async def scenario():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            client = agent_harness_client(env.client)
            async with helper_worker(client, scripted):
                await ask(client, "ziggy/expiry", "hi")
                handle = client.get_workflow_handle("ziggy/expiry")
                await env.sleep(timedelta(minutes=29))
                assert (await handle.query("status"))["phase"] == "Open"
                await env.sleep(timedelta(minutes=2))
                await handle.result()
                description = await handle.describe()
                assert description.status.name == "COMPLETED"

    asyncio.run(scenario())


@needs_temporal
def test_the_rollout_gate_passes_on_ziggys_queue():
    """The worker controller starts the operator's gate workflow on every queue
    of the version, Ziggy's included, with the operator canary's input."""

    async def scenario():
        async with await WorkflowEnvironment.start_time_skipping() as env:
            async with create_catalog_helper_worker(
                env.client,
                task_queue="registry-management",
                activity_executor=concurrent.futures.ThreadPoolExecutor(),
                deployment_config=None,
            ):
                result = await agent_harness_client(env.client).execute_workflow(
                    "RegistryOperatorCanaryWorkflow",
                    {"projectNamePrefix": "operator-canary", "timeoutSeconds": 900},
                    id="registry-operator-canary/ziggy",
                    task_queue=TASK_QUEUE,
                )
                assert result == {"phase": "Succeeded"}

    asyncio.run(scenario())
