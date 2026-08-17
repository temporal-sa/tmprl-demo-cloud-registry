"""Shared construction of the synthetic ADK approval-resume turn."""

from __future__ import annotations

import json

from common.models import ApprovalResolution


def build_adk_resume_prompt(resolution: ApprovalResolution) -> str:
    """Build the user turn injected after the gateway reaches a final state."""

    return (
        "Resume the paused task and report the result to the user. "
        "Agent Gateway sent an authoritative approval-resolution callback. "
        "Use the exact workflow_id and operation_id below. Do not start a "
        "replacement run and do not repeat any completed action. If the status "
        "is completed, report the result and continue only genuinely remaining "
        "safe work from the original request. If it is rejected, expired, "
        "canceled, or failed, stop the protected path and clearly report the "
        "terminal status and reason. You may check get_operation_result once if "
        "more detail is needed; do not poll repeatedly.\n"
        + json.dumps(
            {
                "workflow_id": resolution.gateway_workflow_id,
                "operation_id": resolution.operation_id,
                "status": resolution.status,
                "result": resolution.result,
                "reason": resolution.reason,
                "message": resolution.message,
            },
            sort_keys=True,
        )
    )
