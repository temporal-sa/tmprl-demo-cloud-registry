from __future__ import annotations

import asyncio
import concurrent.futures
import os

from temporalio.worker import Worker

from common import temporal_client
from common.models import WAYPOINT_NAMESPACE
from activities.gateway_activities import (
    archive_artifacts,
    await_quality_gate,
    calculate_integrity_hashes,
    deploy_binaries,
    evaluate_policy,
    health_check_new_instances,
    invoke_tool,
    publish_quality_gate_state,
    run_quality_check,
    signal_operation_callback,
    submit_nested_tool_call,
    submit_tool_call,
    tag_commit_in_source_control,
    update_release_notes,
    update_traffic_routing,
)
from workflows.nexus_handlers import AgentGatewayServiceHandler
from workflows.protected_action import ProtectedActionWorkflow
from adk_agents.release_approval_agent.temporal_integration import (
    build_google_adk_plugin,
)
from workflows.adk_session import TemporalAdkSessionWorkflow
from workflows.autonomous_agent import AutonomousAgentWorkflow
from workflows.chain import AgenticChainWorkflow
from workflows.release_children import (
    CutReleaseChildWorkflow,
    PromoteReleaseChildWorkflow,
    QualityGateChildWorkflow,
)

TASK_QUEUE = os.getenv("TASK_QUEUE", "agentic-gateway")
# The Waypoint team's namespace is the only one this Worker connects to, and it is
# passed as common.temporal_client's fallback rather than left to the SDK's own
# "default": this Worker hosts the gateway's Nexus service handler, and the
# Endpoint targets this namespace by name, so a Worker polling anywhere else
# leaves the Endpoint pointing at a namespace nobody serves. Nothing errors -- the
# Security team's calls into the gateway simply time out, which reads as a Nexus
# fault and is not one.
#
# On the demo cloud platform TEMPORAL_NAMESPACE is injected and wins, because
# there the namespace is a provisioned Cloud name rather than ours to pick.


async def main() -> None:
    adk_plugin = build_google_adk_plugin()
    client = await temporal_client.connect(
        WAYPOINT_NAMESPACE,
        plugins=[adk_plugin],
    )
    # Sync activities use requests, so they run on a thread pool executor.
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[
                AgenticChainWorkflow,
                AutonomousAgentWorkflow,
                TemporalAdkSessionWorkflow,
                # Services one external tool request each, for as long as the
                # approval takes.
                ProtectedActionWorkflow,
                # The Waypoint team's own release steps, as children of the
                # chain. Same namespace, same task queue, same worker: depth,
                # not a team boundary.
                CutReleaseChildWorkflow,
                PromoteReleaseChildWorkflow,
                QualityGateChildWorkflow,
            ],
            activities=[
                evaluate_policy,
                invoke_tool,
                # All stay inside this namespace. Nothing on this worker holds
                # a client for anyone else's cluster.
                submit_tool_call,
                submit_nested_tool_call,
                signal_operation_callback,
                # The individual steps inside the release children. Separate
                # Activity functions rather than one parameterized call, so
                # Event History names what actually happened.
                tag_commit_in_source_control,
                archive_artifacts,
                calculate_integrity_hashes,
                deploy_binaries,
                health_check_new_instances,
                update_traffic_routing,
                update_release_notes,
                run_quality_check,
                publish_quality_gate_state,
                await_quality_gate,
            ],
            # The gateway's Nexus front door, behind the `agent-gateway`
            # Endpoint. This is how another team's tool asks for a protected
            # action without any access to this namespace.
            nexus_service_handlers=[AgentGatewayServiceHandler()],
            activity_executor=executor,
        )
        print(
            f"worker started, namespace '{client.namespace}', "
            f"task queue '{TASK_QUEUE}'",
            flush=True,
        )
        await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
