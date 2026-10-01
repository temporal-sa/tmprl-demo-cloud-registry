from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal

from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker, WorkerDeploymentConfig

from temporal_agent_harness.plugin import AgentHarnessPlugin
from temporal_agent_harness.utils.large_payload import s3_payload_storage
from temporal_agent_harness.web import create_agent_harness_app, create_session_manager_worker


async def main(role: str) -> None:
    storage = await s3_payload_storage(bucket=os.environ["MONTY_PAYLOAD_BUCKET"])
    if role == "web":
        import uvicorn

        app = create_agent_harness_app(
            registry_path="/app/examples/monty/agents.toml",
            large_payload_offload=storage,
        )
        await uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8000)).serve()
        return

    from google.genai import Client as GeminiClient

    from examples.monty import activities
    from examples.monty.conversational_subagent_workflow import MontyChatSubagentWorkflow
    from examples.monty.conversational_workflow import MontyChatAgentWorkflow
    from examples.monty.workflow import TASK_QUEUE, MontyDynamicAgentWorkflow
    from temporal_agent_harness.ai_sdks.google_genai_plugin import GoogleGenAIPlugin

    client = await Client.connect(
        **ClientConfig.load_client_connect_config(),
        plugins=[
            GoogleGenAIPlugin(GeminiClient(api_key=os.environ["GEMINI_API_KEY"])),
            AgentHarnessPlugin(tools=activities.ALL_TOOLS, large_payload_offload=storage),
        ],
    )
    worker_options = {}
    if os.environ.get("TEMPORAL_WORKER_BUILD_ID"):
        worker_options["deployment_config"] = WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(
                deployment_name=os.environ["TEMPORAL_DEPLOYMENT_NAME"],
                build_id=os.environ["TEMPORAL_WORKER_BUILD_ID"],
            ),
            use_worker_versioning=True,
            default_versioning_behavior=VersioningBehavior.PINNED,
        )
    monty_worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            MontyDynamicAgentWorkflow,
            MontyChatAgentWorkflow,
            MontyChatSubagentWorkflow,
        ],
        **worker_options,
    )
    session_worker = create_session_manager_worker(client, **worker_options)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, stopped.set)
    async with monty_worker, session_worker:
        logging.info("Monty and session-manager workers ready in %s", client.namespace)
        await stopped.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("web", "worker"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main(args.role))
