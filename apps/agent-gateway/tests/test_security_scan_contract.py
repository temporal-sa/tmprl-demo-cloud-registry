"""The Nexus boundary between Agent Gateway and the Security team.

The previous version of this file greped the other team's source code to check
that two hand-copied dataclasses had not drifted apart. That test was a
compensating control for a boundary that was not real: two genuinely separate
teams do not have each other's repositories, so neither of them could have
written it.

What replaced it is a published interface. Each side declares the same service
contract -- endpoint names, operation names, payload field names -- and the
checks below are the ones a real integration test can make: that the two
declarations agree, and that neither side has quietly reacquired knowledge of
the other's internals.
"""

from __future__ import annotations

import ast
import dataclasses
from pathlib import Path

import nexusrpc

import common.nexus_contracts as gateway_contracts
import security_scan.nexus_contracts as security_contracts

REPO_ROOT = Path(__file__).resolve().parent.parent


def _fields(cls) -> set[str]:
    return {f.name for f in dataclasses.fields(cls)}


def _operations(service) -> dict[str, tuple[str, str]]:
    definition = nexusrpc.get_service_definition(service)
    return {
        name: (op.input_type.__name__, op.output_type.__name__)
        for name, op in definition.operation_definitions.items()
    }


def _code_only(path: Path) -> str:
    """Source with docstrings and comments stripped.

    The checks below are about what the code depends on, not about what the
    prose is allowed to explain. A comment in security_scan/ describing why the
    scan is started synchronously has to be able to name
    AgenticChainWorkflow and Continue-As-New, because that is the reason, and a
    reader who cannot be told the reason is worse off than one who can.
    """
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(ast.fix_missing_locations(tree))


def test_both_sides_declare_the_same_endpoints() -> None:
    assert (
        gateway_contracts.AGENT_GATEWAY_ENDPOINT
        == security_contracts.AGENT_GATEWAY_ENDPOINT
    )
    assert (
        gateway_contracts.SECURITY_ENDPOINT
        == security_contracts.SECURITY_ENDPOINT
    )


def test_both_sides_declare_the_same_operations() -> None:
    """Operation names and payload types, on the service each side handles.

    A rename on one side without the other is the failure this catches, and it
    is the one that would strand a suspended workflow rather than erroring
    loudly at deploy time.
    """
    for gateway_service, security_service in (
        (gateway_contracts.AgentGatewayService, security_contracts.AgentGatewayService),
        (
            gateway_contracts.SecurityScanService,
            security_contracts.SecurityScanService,
        ),
    ):
        assert _operations(gateway_service) == _operations(security_service)


def test_payload_field_names_match_on_both_sides() -> None:
    """Field names are the contract; the payload converter matches on them."""
    pairs = [
        (gateway_contracts.ProtectedActionRequest, security_contracts.ProtectedActionRequest),
        (gateway_contracts.ProtectedActionOutcome, security_contracts.ProtectedActionOutcome),
        (gateway_contracts.ToolOutcomeReport, security_contracts.ToolOutcomeReport),
        (gateway_contracts.ToolOutcomeAck, security_contracts.ToolOutcomeAck),
        (gateway_contracts.StartSecurityScanInput, security_contracts.StartSecurityScanInput),
        (gateway_contracts.SecurityScanStarted, security_contracts.SecurityScanStarted),
    ]
    for ours, theirs in pairs:
        assert _fields(ours) == _fields(theirs), ours.__name__


def test_the_correlation_token_survives_the_boundary() -> None:
    """One user-visible task across two systems rests on this one field.

    Everything else could be renamed and the demo would still make its point.
    Lose the chain workflow_id on the way through and the nested call opens a
    second chain, which is precisely the failure the requirements document's
    correlation section exists to prevent.
    """
    assert "gateway_workflow_id" in _fields(gateway_contracts.StartSecurityScanInput)
    assert "gateway_workflow_id" in _fields(gateway_contracts.ProtectedActionRequest)
    assert "gateway_workflow_id" in _fields(gateway_contracts.ToolOutcomeReport)


def _imported_packages(path: Path) -> set[str]:
    packages: set[str] = set()
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith(("from ", "import ")):
            packages.add(line.split()[1].split(".")[0])
    return packages


def test_the_security_team_does_not_import_the_gateway() -> None:
    forbidden = {"common", "workflows", "activities", "gateway", "mock_tool"}
    for path in sorted((REPO_ROOT / "security_scan").glob("*.py")):
        leaked = _imported_packages(path) & forbidden
        assert not leaked, f"{path.name} imports {sorted(leaked)} from the gateway"


def test_the_gateway_does_not_import_the_security_package() -> None:
    for module in ("workflows", "activities", "gateway", "common"):
        for path in sorted((REPO_ROOT / module).glob("*.py")):
            assert "security_scan" not in _imported_packages(path), (
                f"{module}/{path.name} imports the security_scan package"
            )


def test_neither_side_names_the_other_topology() -> None:
    """The isolation win, asserted rather than described.

    Under the previous design each side hardcoded the other's namespace, task
    queue, and either a workflow type or an update and signal name. A Nexus
    Endpoint replaces all of it, which means either team can rename their
    workflows, move task queues, or change namespace without the other team
    deploying. If one of these strings comes back, that property is gone.
    """
    for module in ("workflows", "activities", "common"):
        for path in (REPO_ROOT / module).glob("*.py"):
            code = _code_only(path)
            assert "security-tq" not in code, path.name
            assert "SecurityScanWorkflow" not in code, path.name

    for path in (REPO_ROOT / "security_scan").glob("*.py"):
        # The legacy script is the uncontrolled caller and deliberately talks to
        # the gateway's public MCP endpoint. Everything else must not know the
        # gateway exists beyond its Nexus endpoint name.
        if path.name == "legacy_security_scan_script.py":
            continue
        code = _code_only(path)
        assert "agentic-gateway" not in code, path.name
        assert "AgenticChainWorkflow" not in code, path.name
        assert "request_nested_tool_call" not in code, path.name
