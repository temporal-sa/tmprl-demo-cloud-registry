from __future__ import annotations

from types import SimpleNamespace

from temporalio.contrib.google_adk_agents import (
    TemporalMcpToolSet,
    TemporalModel,
)

from adk_agents.release_approval_agent.agent import (
    AGENT_INSTRUCTION,
    DEFAULT_DEMO_TOKEN,
    DEFAULT_GATEWAY_URL,
    SCENARIO_3_TOOL_NAMES,
    AgentGatewaySettings,
    root_agent,
)
from adk_agents.release_approval_agent.temporal_integration import (
    build_temporal_agent,
    gateway_toolset_factory,
)
from adk_agents.release_approval_agent.temporal_proxy import (
    TemporalSessionProxyAgent,
)
from common.models import (
    ADK_SESSION_ID_HEADER,
    CALLBACK_RUN_ID_HEADER,
    CALLBACK_WORKFLOW_ID_HEADER,
    AdkTemporalSessionCallback,
)
from workflows.adk_session import TemporalAdkSessionWorkflow


def test_adk_web_root_is_only_a_temporal_session_proxy() -> None:
    assert isinstance(root_agent, TemporalSessionProxyAgent)
    assert root_agent.name == "release_approval_agent"
    assert root_agent.sub_agents == []


def test_adk_agent_has_durable_pause_and_recovery_instructions() -> None:
    assert (
        "You are Dashy, the general-purpose DoorDash coding assistant."
        in AGENT_INSTRUCTION
    )
    assert "software design, implementation, debugging" in AGENT_INSTRUCTION
    assert "Never claim to have changed code or external state" in AGENT_INSTRUCTION
    assert "start_google_adk_release_run" in AGENT_INSTRUCTION
    assert "waiting_for_approval" in AGENT_INSTRUCTION
    assert "Treat processing the same way" in AGENT_INSTRUCTION
    assert "workflow_id and operation_id" in AGENT_INSTRUCTION
    assert "Never invent a replacement ID" in AGENT_INSTRUCTION
    assert "get_operation_result returns completed" in AGENT_INSTRUCTION


def test_adk_settings_read_gateway_identity_from_environment(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "AGENT_GATEWAY_MCP_URL",
        "http://agent-gateway.internal/mcp",
    )
    monkeypatch.setenv("AGENT_GATEWAY_TOKEN", "runtime-token")
    monkeypatch.setenv("ADK_MODEL", "gemini-runtime")

    settings = AgentGatewaySettings.from_env()

    assert settings.mcp_url == "http://agent-gateway.internal/mcp"
    assert settings.bearer_token == "runtime-token"
    assert settings.model == "gemini-runtime"


def test_adk_settings_have_local_demo_defaults(monkeypatch) -> None:
    monkeypatch.delenv("AGENT_GATEWAY_MCP_URL", raising=False)
    monkeypatch.delenv("AGENT_GATEWAY_TOKEN", raising=False)
    monkeypatch.delenv("ADK_MODEL", raising=False)

    settings = AgentGatewaySettings.from_env()

    assert settings.mcp_url == DEFAULT_GATEWAY_URL
    assert settings.bearer_token == DEFAULT_DEMO_TOKEN


def test_temporal_toolset_propagates_session_callback_headers(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "AGENT_GATEWAY_MCP_URL",
        "http://gateway.test/mcp",
    )
    monkeypatch.setenv("AGENT_GATEWAY_TOKEN", "runtime-token")
    callback = AdkTemporalSessionCallback(
        workflow_id="adk-session-workflow",
        run_id="adk-session-run",
        session_id="adk-session-123",
    )

    toolset = gateway_toolset_factory(callback)

    assert tuple(toolset.tool_filter) == SCENARIO_3_TOOL_NAMES
    assert toolset._connection_params.headers == {
        "Authorization": "Bearer runtime-token",
        CALLBACK_WORKFLOW_ID_HEADER: "adk-session-workflow",
        CALLBACK_RUN_ID_HEADER: "adk-session-run",
        ADK_SESSION_ID_HEADER: "adk-session-123",
    }


def test_temporal_agent_routes_model_and_mcp_calls_through_plugin() -> None:
    callback = AdkTemporalSessionCallback(
        workflow_id="adk-session-workflow",
        run_id="adk-session-run",
        session_id="adk-session-123",
    )

    agent = build_temporal_agent(
        model="gemini-test",
        callback=callback,
    )

    assert isinstance(agent.model, TemporalModel)
    assert len(agent.tools) == 1
    assert isinstance(agent.tools[0], TemporalMcpToolSet)


def test_adk_session_waits_for_processing_governance_callbacks() -> None:
    for status in ("waiting_for_approval", "processing"):
        part = SimpleNamespace(
            function_response=SimpleNamespace(
                name="start_google_adk_release_run",
                response={
                    "structuredContent": {
                        "status": status,
                        "operation_id": "op-governed",
                    }
                },
            )
        )

        assert (
            TemporalAdkSessionWorkflow._waiting_operation_from_part(part)
            == "op-governed"
        )
