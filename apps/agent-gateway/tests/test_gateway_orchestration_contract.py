"""Gateway contract for the bump-driven CASE-2 entry point.

Kept separate from tests/test_gateway_contract.py so that file, which covers the
CASE-1 and CASE-3 surface, stays exactly as it was.
"""

from __future__ import annotations

import asyncio

import pytest

from common.models import NestedToolCallRequest
from gateway import server


def _tool(name: str):
    return next(
        tool
        for tool in server.mcp._tool_manager.list_tools()
        if tool.name == name
    )


def test_mcp_exposes_the_orchestrated_entry_point_alongside_the_explicit_one() -> None:
    names = {tool.name for tool in server.mcp._tool_manager.list_tools()}
    assert {"run_nested_release", "run_release_orchestration"}.issubset(names)


def test_run_nested_release_signature_is_unchanged_for_existing_callers() -> None:
    # The explicit-version entry point still takes a required version and no
    # bump, so a caller or demo script written against it keeps working.
    properties = _tool("run_nested_release").parameters["properties"]
    required = _tool("run_nested_release").parameters.get("required", [])
    assert "version" in properties
    assert "bump" not in properties
    assert {"service", "version", "environment"}.issubset(set(required))


def test_orchestration_tool_takes_a_bump_enum_and_no_version() -> None:
    schema = _tool("run_release_orchestration").parameters
    properties = schema["properties"]
    assert "version" not in properties
    # The bump type is an enum in the schema, so a caller sees the three valid
    # values rather than guessing at a free-text string.
    bump = properties["bump"]
    assert bump.get("enum") == ["major", "minor", "bugfix"]
    # Same nested-approval knobs as the explicit path, same defaults.
    assert properties["tool1_mode"]["default"] == "controlled"
    assert properties["replay_safe"]["default"] is False
    assert _tool("run_release_orchestration").output_schema is not None


def test_the_pipeline_needs_no_arguments_at_all() -> None:
    """"Deploy the next minor version" has to be a complete instruction.

    Anything required here becomes a clarifying question in the middle of a demo,
    which is the opposite of the point: one sentence, no parameters.
    """
    schema = _tool("run_release_orchestration").parameters
    assert not schema.get("required")
    properties = schema["properties"]
    # "the next version" means minor.
    assert properties["bump"]["default"] == "minor"
    # A single-service demo backend should not have to be told its own service.
    assert properties["service"]["default"] == ""
    assert server.DEFAULT_SERVICE == "delivery-matching-service"
    # Absent environment means the full pipeline, through to production.
    assert properties["environment"]["default"] == ""
    assert server.PIPELINE_TARGET_ENVIRONMENT == "prod"


def test_omitted_arguments_resolve_before_the_workflow_sees_them(monkeypatch) -> None:
    """A bare call must arrive at the workflow fully resolved.

    The workflow should never have to guess how far the pipeline was meant to go or
    which service was meant, so the gateway fills both in first.
    """
    captured: dict = {}

    async def fake_submit(**kwargs):
        captured.update(kwargs)
        return None

    monkeypatch.setattr(server, "_submit_nested_release", fake_submit)
    asyncio.run(server.run_release_orchestration(ctx=None))

    assert captured["environment"] == "prod"
    assert captured["service"] == "delivery-matching-service"
    assert captured["bump"] == "minor"
    assert captured["tool1_mode"] == "controlled"
    assert captured["replay_safe"] is False
    assert "version" not in captured


def test_every_exposed_tool_still_has_a_structured_output_schema() -> None:
    assert all(
        tool.output_schema is not None
        for tool in server.mcp._tool_manager.list_tools()
    )


def _submit(**kwargs):
    return asyncio.run(
        server._submit_nested_release(
            service="delivery-matching-service",
            environment="prod",
            ctx=None,
            tool1_mode=kwargs.pop("tool1_mode", "controlled"),
            replay_safe=False,
            justification="",
            workflow_id="",
            idempotency_key="",
            **kwargs,
        )
    )


def test_version_and_bump_are_mutually_exclusive() -> None:
    # Both. Nothing is submitted; the caller has to say which one it means.
    with pytest.raises(ValueError, match="exactly one of version or bump"):
        _submit(version="2.3.0", bump="minor")
    # Neither.
    with pytest.raises(ValueError, match="exactly one of version or bump"):
        _submit()


def test_an_unknown_bump_type_is_rejected_before_a_workflow_starts() -> None:
    with pytest.raises(ValueError, match="bump must be one of"):
        _submit(bump="patch")


def test_tool1_mode_is_still_validated_first() -> None:
    with pytest.raises(ValueError, match="tool1_mode must be"):
        _submit(bump="minor", tool1_mode="semi-controlled")


def test_nested_request_defaults_to_no_bump() -> None:
    # The new field is additive: a request built the way the explicit path builds
    # it carries an empty bump, which is what selects the unchanged behavior in
    # AgenticChainWorkflow.request_nested_tool_call.
    request = NestedToolCallRequest(
        tool1_name="release_orchestrator",
        tool1_arguments={},
        tool2_name="promote_release",
        tool2_arguments={},
        idempotency_key="idem-x",
        correlation=server.CorrelationContext(workflow_id="wf-x"),
    )
    assert request.bump == ""
