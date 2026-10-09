"""Apply the pinned harness's two missing workspace-routing seams at build time."""

from pathlib import Path


def patch_workspace_routing(root: Path) -> None:
    subagent = root / "examples/monty/conversational_subagent_workflow.py"
    source = subagent.read_text()
    original = "            task_queue=TASK_QUEUE,"
    if source.count(original) != 1:
        raise ValueError("Monty subagent routing changed; review the upstream pin")
    # Workflow info is recorded in history, so this also works during replay.
    subagent.write_text(
        source.replace(original, "            task_queue=workflow.info().task_queue,")
    )

    web = root / "temporal_agent_harness/web/app.py"
    source = web.read_text()
    original = '    query = f"ExecutionStatus=\'Running\' AND ({types_filter})"'
    if source.count(original) != 1:
        raise ValueError("Harness session discovery changed; review the upstream pin")
    replacement = '''    queues = sorted({descriptor.task_queue for descriptor in registry.agents})
    queues_filter = " OR ".join(
        "TaskQueue='" + queue.replace("'", "''") + "'" for queue in queues
    )
    query = f"ExecutionStatus='Running' AND ({types_filter}) AND ({queues_filter})"'''
    web.write_text(source.replace(original, replacement))


if __name__ == "__main__":
    patch_workspace_routing(Path("/app"))
