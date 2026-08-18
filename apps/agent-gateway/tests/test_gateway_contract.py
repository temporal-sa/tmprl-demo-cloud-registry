from __future__ import annotations

import asyncio

import pytest
from temporalio.client import WorkflowQueryFailedError
from temporalio.service import RPCError, RPCStatusCode

from activities.gateway_activities import evaluate_policy
from common.models import (
    ADK_SESSION_ID_HEADER,
    CALLBACK_RUN_ID_HEADER,
    CALLBACK_WORKFLOW_ID_HEADER,
    SCAN_MODE_PLATFORM,
    AdkTemporalSessionCallback,
    EvaluatePolicyInput,
    ToolCallResponse,
)
from gateway import server


def test_mcp_transport_allows_local_and_compose_hosts() -> None:
    transport_security = server.mcp.settings.transport_security

    assert transport_security is not None
    assert transport_security.enable_dns_rebinding_protection is True
    assert "localhost:*" in transport_security.allowed_hosts
    assert "gateway:*" in transport_security.allowed_hosts


def test_authenticated_adk_callback_headers_are_resolved() -> None:
    callback = server._adk_callback_from_headers(
        [
            (
                CALLBACK_WORKFLOW_ID_HEADER.encode(),
                b"adk-session-workflow",
            ),
            (CALLBACK_RUN_ID_HEADER.encode(), b"adk-session-run"),
            (ADK_SESSION_ID_HEADER.encode(), b"adk-session-123"),
        ],
        "release-agent@google-adk",
    )

    assert callback is not None
    assert callback.workflow_id == "adk-session-workflow"
    assert callback.run_id == "adk-session-run"
    assert callback.session_id == "adk-session-123"
    assert (
        server._adk_callback_from_headers(
            [
                (
                    CALLBACK_WORKFLOW_ID_HEADER.encode(),
                    b"adk-session-workflow",
                ),
                (ADK_SESSION_ID_HEADER.encode(), b"adk-session-123"),
            ],
            server.UNVERIFIED_PRINCIPAL,
        )
        is None
    )


def test_mcp_exposes_all_required_scenario_and_recovery_tools() -> None:
    tools = server.mcp._tool_manager.list_tools()
    names = {tool.name for tool in tools}
    assert {
        "get_deployed_version",
        "cut_release",
        "promote_release",
        "run_nested_release",
        "resume_nested_release",
        "start_google_adk_release_run",
        "get_operation_status",
        "get_operation_result",
        "get_workflow_status",
        "get_workflow_ledger",
        "cancel_operation",
    }.issubset(names)
    assert all(tool.output_schema is not None for tool in tools)
    operation_schema = next(
        tool.output_schema
        for tool in tools
        if tool.name == "promote_release"
    )
    assert {
        "status",
        "workflow_id",
        "operation_id",
        "poll_after_seconds",
    }.issubset(operation_schema["properties"])


def test_sensitive_arguments_are_redacted_recursively() -> None:
    safe = server._safe_arguments(
        {
            "service": "delivery-matching-service",
            "token": "secret-token",
            "nested": {
                "api_key": "secret-key",
                "password": "secret-password",
                "environment": "prod",
            },
        }
    )
    assert safe == {
        "service": "delivery-matching-service",
        "token": "[REDACTED]",
        "nested": {
            "api_key": "[REDACTED]",
            "password": "[REDACTED]",
            "environment": "prod",
        },
    }


def test_explicit_workflow_ids_require_authenticated_identity() -> None:
    token = server._current_principal.set(server.UNVERIFIED_PRINCIPAL)
    try:
        with pytest.raises(PermissionError):
            server._resolve_workflow_id("wf-caller-supplied", None)
    finally:
        server._current_principal.reset(token)


def test_caller_idempotency_keys_are_stable_and_principal_scoped() -> None:
    first = server._caller_idempotency_key(
        "wf-123",
        "promote_release",
        {"environment": "prod"},
        "request-456",
        "alice@example.com",
    )
    duplicate = server._caller_idempotency_key(
        "wf-123",
        "promote_release",
        {"environment": "prod"},
        "request-456",
        "alice@example.com",
    )
    other_principal = server._caller_idempotency_key(
        "wf-123",
        "promote_release",
        {"environment": "prod"},
        "request-456",
        "mallory@example.com",
    )
    assert first == duplicate
    assert first != other_principal


def test_policy_normalizes_environment_before_protection_check() -> None:
    decision = evaluate_policy(
        EvaluatePolicyInput(
            tool_name="promote_release",
            arguments={"environment": " PROD "},
            caller_principal="requester@example.com",
        )
    )

    assert decision.requires_approval is True


def test_adk_callback_is_not_registered_before_owner_check(
    monkeypatch,
) -> None:
    callback = AdkTemporalSessionCallback(
        workflow_id="adk-session-workflow",
        run_id="adk-session-run",
        session_id="adk-session-123",
    )

    class Handle:
        def __init__(self) -> None:
            self.signals = []

        async def query(self, *_args, **_kwargs):
            return "workflow-owner@example.com"

        async def signal(self, *args, **kwargs):
            self.signals.append((args, kwargs))

    handle = Handle()

    class Client:
        async def start_workflow(self, *_args, **_kwargs):
            return handle

    async def get_client():
        return Client()

    monkeypatch.setattr(server, "get_client", get_client)
    monkeypatch.setattr(
        server,
        "_resolve_principal",
        lambda _ctx=None: "attacker@example.com",
    )
    callback_token = server._current_adk_callback.set(callback)
    try:
        with pytest.raises(PermissionError):
            asyncio.run(
                server.start_google_adk_release_run(
                    service="delivery-matching-service",
                    version="2.5.0",
                    environment="prod",
                    agent_run_id="owner-check",
                    workflow_id="existing-workflow",
                    ctx=None,
                )
            )
    finally:
        server._current_adk_callback.reset(callback_token)

    assert handle.signals == []


def test_adk_start_snapshots_the_same_security_controls_as_direct_calls(
    monkeypatch,
) -> None:
    """CASE-3 must carry the controls used by the Claude direct-call path.

    The toggle values are gateway process state, so the only safe place to read
    them is while accepting the MCP call. The autonomous workflow can then lodge
    the promotion with its companion AgenticChainWorkflow without either workflow
    consulting mutable process state during replay.
    """

    principal = "release-agent@google-adk"
    captured = {}

    class Handle:
        async def query(self, query, *args, **kwargs):
            if query == server.AutonomousAgentWorkflow.get_workflow_owner:
                return principal
            return ToolCallResponse(
                status="waiting_for_approval",
                workflow_id="wf-adk-controls",
                operation_id="op-adk-controls",
            )

        async def signal(self, *args, **kwargs):
            raise AssertionError("this test did not register an ADK callback")

    class Client:
        async def start_workflow(self, _run, input, **_kwargs):
            captured["input"] = input
            return Handle()

    async def get_client():
        return Client()

    monkeypatch.setattr(server, "get_client", get_client)
    monkeypatch.setattr(server, "_resolve_principal", lambda _ctx=None: principal)

    old_mandate = server.MANDATE_TOGGLE.is_on()
    old_scan_mode = server.SCAN_MODE_TOGGLE.mode()
    server.MANDATE_TOGGLE.set(True)
    server.SCAN_MODE_TOGGLE.set(SCAN_MODE_PLATFORM)
    try:
        response = asyncio.run(
            server.start_google_adk_release_run(
                service="delivery-matching-service",
                version="2.5.0",
                environment="prod",
                agent_run_id="security-controls",
                workflow_id="wf-adk-controls",
                ctx=None,
            )
        )
    finally:
        server.MANDATE_TOGGLE.set(old_mandate)
        server.SCAN_MODE_TOGGLE.set(old_scan_mode)

    assert response.status == "waiting_for_approval"
    governed = captured["input"].governed_request
    assert governed is not None
    assert governed.security_mandate is True
    assert governed.scan_mode == SCAN_MODE_PLATFORM
    assert governed.correlation.workflow_id == "wf-adk-controls:gateway"
    assert governed.correlation.caller_service == "google-adk-agent"
    assert governed.callback_workflow_id == "wf-adk-controls"


class _FakeHandle:
    """Stands in for a WorkflowHandle whose query is going to fail."""

    def __init__(self, error: Exception) -> None:
        self._error = error

    async def query(self, *args, **kwargs):
        raise self._error


class _FakeClient:
    def __init__(self, handle: _FakeHandle) -> None:
        self._handle = handle

    def get_workflow_handle(self, workflow_id: str) -> _FakeHandle:
        return self._handle


def _authorize_against(error: Exception, monkeypatch) -> Exception:
    """Run _authorized_handle against a handle that raises, return what escaped."""

    async def fake_get_client():
        return _FakeClient(_FakeHandle(error))

    monkeypatch.setattr(server, "get_client", fake_get_client)

    async def run():
        with pytest.raises(Exception) as excinfo:  # noqa: PT011 - type is the assertion
            await server._authorized_handle("quality-gate::svc::2.3.0::abc123", None)
        return excinfo.value

    return asyncio.run(run())


def test_non_chain_workflow_id_is_reported_as_the_wrong_kind_of_workflow(
    monkeypatch,
) -> None:
    # Verbatim shape of what a QualityGateChildWorkflow answers with: it
    # implements get_status and nothing the chain introspection tools want.
    raised = _authorize_against(
        WorkflowQueryFailedError(
            "Query handler for 'get_workflow_owner' expected but not found, "
            "known queries: [__enhanced_stack_trace __stack_trace "
            "__temporal_workflow_metadata get_status]"
        ),
        monkeypatch,
    )

    assert isinstance(raised, ValueError)
    message = str(raised)
    assert "not an agentic chain workflow" in message
    assert "quality-gate::svc::2.3.0::abc123" in message
    # The caller is told where the answer does live, not just that this failed.
    assert "Temporal UI" in message
    # And it no longer reads as gateway internals leaking out.
    assert "Query handler" not in message


def test_unrelated_query_failures_are_not_disguised_as_a_wrong_workflow(
    monkeypatch,
) -> None:
    raised = _authorize_against(
        WorkflowQueryFailedError("get_workflow_status failed: boom"),
        monkeypatch,
    )

    assert isinstance(raised, WorkflowQueryFailedError)


def test_missing_workflow_id_is_reported_as_missing(monkeypatch) -> None:
    raised = _authorize_against(
        RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b""),
        monkeypatch,
    )

    assert isinstance(raised, ValueError)
    assert "no workflow with id" in str(raised)


def test_transport_failures_still_surface_as_themselves(monkeypatch) -> None:
    raised = _authorize_against(
        RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b""),
        monkeypatch,
    )

    assert isinstance(raised, RPCError)
