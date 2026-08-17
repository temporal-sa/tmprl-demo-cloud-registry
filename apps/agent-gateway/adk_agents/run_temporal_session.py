"""Start a Dashy session that runs durably with the Temporal ADK plugin."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import uuid
from dataclasses import asdict

from temporalio.common import WorkflowIDConflictPolicy

from adk_agents.release_approval_agent.temporal_integration import (
    build_google_adk_plugin,
)
from common import temporal_client
from common.models import (
    WAYPOINT_NAMESPACE,
    AdkSessionTurnInput,
    AdkSessionWorkflowInput,
    AdkSessionWorkflowResult,
)
from workflows.adk_session import TemporalAdkSessionWorkflow

TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")
# The Waypoint team's namespace is the local-development fallback; an injected
# TEMPORAL_NAMESPACE wins. Named rather than left to the SDK's own "default",
# which would connect to an empty namespace and never find the session workflow.


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--user-id", default="user")
    parser.add_argument("--session-id", default="")
    parser.add_argument("--workflow-id", default="")
    parser.add_argument(
        "--model",
        default=os.getenv("ADK_MODEL", "gemini-3.6-flash"),
    )
    args = parser.parse_args()

    session_id = args.session_id or f"session-{uuid.uuid4().hex[:12]}"
    workflow_id = args.workflow_id or f"adk-session-{session_id}"
    plugin = build_google_adk_plugin()
    client = await temporal_client.connect(
        WAYPOINT_NAMESPACE,
        plugins=[plugin],
    )
    handle = await client.start_workflow(
        TemporalAdkSessionWorkflow.run,
        AdkSessionWorkflowInput(
            user_id=args.user_id,
            session_id=session_id,
            model=args.model,
        ),
        id=workflow_id,
        task_queue=TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    turn_id = f"turn-{uuid.uuid4().hex}"
    result = await handle.execute_update(
        TemporalAdkSessionWorkflow.run_turn,
        AdkSessionTurnInput(
            turn_id=turn_id,
            prompt=args.prompt,
        ),
        id=turn_id,
        result_type=AdkSessionWorkflowResult,
    )
    print(json.dumps(asdict(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
