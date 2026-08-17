"""Temporal plugin integration for Dashy's Agent Gateway MCP toolset."""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Any

from google.adk import Agent
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import (
    StreamableHTTPConnectionParams,
)
from temporalio.contrib.google_adk_agents import (
    GoogleAdkPlugin,
    TemporalMcpToolSet,
    TemporalMcpToolSetProvider,
    TemporalModel,
)
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityConfig

from adk_agents.release_approval_agent.agent import (
    AGENT_INSTRUCTION,
    SCENARIO_3_TOOL_NAMES,
    AgentGatewaySettings,
)
from common.models import (
    ADK_SESSION_ID_HEADER,
    CALLBACK_RUN_ID_HEADER,
    CALLBACK_WORKFLOW_ID_HEADER,
    AdkTemporalSessionCallback,
)

TEMPORAL_GATEWAY_TOOLSET_NAME = "agent-gateway"

# Model and MCP Activities retry, but they must not retry forever. A rejected
# API key or a model that is closed to the caller never recovers, and an
# unbounded retry loop shows the user an ADK turn that silently hangs.
_TURN_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=4,
)


def _callback_from_factory_argument(
    value: Any | None,
) -> AdkTemporalSessionCallback | None:
    if value is None:
        return None
    if isinstance(value, AdkTemporalSessionCallback):
        return value
    if isinstance(value, dict):
        return AdkTemporalSessionCallback(
            workflow_id=str(value["workflow_id"]),
            run_id=(
                str(value["run_id"])
                if value.get("run_id") is not None
                else None
            ),
            session_id=str(value["session_id"]),
        )
    raise TypeError("unsupported ADK Temporal callback context")


def gateway_toolset_factory(
    factory_argument: Any | None = None,
) -> McpToolset:
    """Build the real MCP client used by Temporal tool activities."""

    settings = AgentGatewaySettings.from_env()
    headers = {
        "Authorization": f"Bearer {settings.bearer_token}",
    }
    callback = _callback_from_factory_argument(factory_argument)
    if callback is not None:
        headers[CALLBACK_WORKFLOW_ID_HEADER] = callback.workflow_id
        headers[ADK_SESSION_ID_HEADER] = callback.session_id
        if callback.run_id:
            headers[CALLBACK_RUN_ID_HEADER] = callback.run_id
    return McpToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=settings.mcp_url,
            headers=headers,
        ),
        tool_filter=list(SCENARIO_3_TOOL_NAMES),
    )


def build_temporal_agent(
    *,
    model: str,
    callback: AdkTemporalSessionCallback,
) -> Agent:
    """Build Dashy with model and MCP calls routed through Temporal activities."""

    return Agent(
        name="release_approval_agent",
        model=TemporalModel(
            model,
            activity_config=ActivityConfig(
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_TURN_RETRY_POLICY,
            ),
        ),
        description=(
            "Dashy, the general-purpose DoorDash coding assistant, running in "
            "a durable Temporal-backed ADK session."
        ),
        instruction=AGENT_INSTRUCTION,
        tools=[
            TemporalMcpToolSet(
                TEMPORAL_GATEWAY_TOOLSET_NAME,
                config=ActivityConfig(
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=_TURN_RETRY_POLICY,
                ),
                factory_argument=asdict(callback),
                not_in_workflow_toolset=gateway_toolset_factory,
            )
        ],
    )


def build_google_adk_plugin() -> GoogleAdkPlugin:
    """Create the worker plugin and register Agent Gateway MCP activities."""

    return GoogleAdkPlugin(
        toolset_providers=[
            TemporalMcpToolSetProvider(
                TEMPORAL_GATEWAY_TOOLSET_NAME,
                gateway_toolset_factory,
            )
        ]
    )
