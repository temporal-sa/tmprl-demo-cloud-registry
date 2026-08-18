from __future__ import annotations

from common.adk_resume import build_adk_resume_prompt
from common.models import ApprovalResolution


def _resolution(status: str = "completed") -> ApprovalResolution:
    return ApprovalResolution(
        gateway_workflow_id="wf-adk-123",
        operation_id="op-456",
        status=status,
        adk_session_id="session-789",
        agent_run_id="agent-run-1",
        result={"promoted": True} if status == "completed" else None,
        reason="risk too high" if status == "rejected" else None,
    )


def test_resume_prompt_continues_without_repeating_completed_work() -> None:
    prompt = build_adk_resume_prompt(_resolution())

    assert prompt.startswith("Resume the paused task")
    assert "report the result to the user" in prompt
    assert "do not repeat any completed action" in prompt
    assert '"status": "completed"' in prompt
    assert '"workflow_id": "wf-adk-123"' in prompt


def test_resume_prompt_stops_after_unsuccessful_terminal_result() -> None:
    prompt = build_adk_resume_prompt(_resolution("rejected"))

    assert "stop the protected path" in prompt
    assert '"status": "rejected"' in prompt
    assert '"reason": "risk too high"' in prompt
