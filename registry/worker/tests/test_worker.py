from __future__ import annotations

import asyncio
from types import SimpleNamespace

from temporalio.common import WorkerDeploymentVersion
from temporalio.converter import DataConverter

from demo_registry_operator import worker
from demo_registry_operator.workflows.messages import CheckWorkerVersionSignal


def test_worker_deployment_info_current_matches_target_version():
    version = WorkerDeploymentVersion(
        deployment_name="registry-operator",
        build_id="git-abc123",
    )
    info = SimpleNamespace(
        routing_config=SimpleNamespace(
            current_deployment_version=SimpleNamespace(
                deployment_name="registry-operator",
                build_id="git-abc123",
            )
        )
    )

    assert worker.worker_deployment_info_current_matches(info, version) is True


def test_worker_deployment_info_current_rejects_other_build():
    version = WorkerDeploymentVersion(
        deployment_name="registry-operator",
        build_id="git-abc123",
    )
    info = SimpleNamespace(
        routing_config=SimpleNamespace(
            current_deployment_version=SimpleNamespace(
                deployment_name="registry-operator",
                build_id="git-def456",
            )
        )
    )

    assert worker.worker_deployment_info_current_matches(info, version) is False


def test_running_entity_workflows_query_selects_running_entity_workflows():
    assert worker.running_entity_workflows_query() == (
        '(WorkflowType = "DemoProjectWorkflow" OR WorkflowType = "InstruqtCourseWorkflow") '
        'AND ExecutionStatus = "Running"'
    )


def test_worker_update_batch_job_id_includes_sanitized_build(monkeypatch):
    monkeypatch.setenv("TEMPORAL_WORKER_BUILD_ID", "git/abc:123")

    job_id = worker.worker_update_batch_job_id()

    assert job_id.startswith("registry-worker-update-git-abc-123-")


def test_signal_running_entity_workflows_uses_batch_operation(monkeypatch):
    requests = []

    class WorkflowService:
        async def start_batch_operation(self, request, **kwargs):
            requests.append((request, kwargs))

    client = SimpleNamespace(
        namespace="default",
        workflow_service=WorkflowService(),
        data_converter=DataConverter.default,
    )

    monkeypatch.setattr(
        worker,
        "worker_update_batch_job_id",
        lambda: "registry-worker-update-test",
    )

    result = asyncio.run(
        worker.signal_running_entity_workflows_for_worker_update(client)
    )

    assert result["mode"] == "batch"
    assert result["jobID"] == "registry-worker-update-test"
    request, kwargs = requests[0]
    assert request.namespace == "default"
    assert request.visibility_query == worker.running_entity_workflows_query()
    assert request.signal_operation.signal == worker.CHECK_WORKER_VERSION_SIGNAL
    decoded = asyncio.run(
        DataConverter.default.decode(
            list(request.signal_operation.input.payloads),
            [CheckWorkerVersionSignal],
        )
    )
    assert decoded == [CheckWorkerVersionSignal()]
    assert kwargs["retry"] is True


def test_signal_running_entity_workflows_falls_back_to_individual_signals():
    signaled = []

    class WorkflowService:
        async def start_batch_operation(self, request, **kwargs):
            raise RuntimeError("batch unavailable")

    class WorkflowHandle:
        def __init__(self, workflow_id: str, run_id: str):
            self.workflow_id = workflow_id
            self.run_id = run_id

        async def signal(self, signal_name: str, arg=None):
            signaled.append((self.workflow_id, self.run_id, signal_name, arg))

    class WorkflowIterator:
        def __init__(self):
            self.executions = iter(
                [
                    SimpleNamespace(id="demo-project/one", run_id="run-1"),
                    SimpleNamespace(id="demo-project/two", run_id="run-2"),
                ]
            )

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self.executions)
            except StopIteration:
                raise StopAsyncIteration

    class Client:
        namespace = "default"
        workflow_service = WorkflowService()

        def list_workflows(self, query, *, page_size):
            self.query = query
            self.page_size = page_size
            return WorkflowIterator()

        def get_workflow_handle(self, workflow_id, *, run_id):
            return WorkflowHandle(workflow_id, run_id)

    client = Client()

    result = asyncio.run(
        worker.signal_running_entity_workflows_for_worker_update(client)
    )

    assert result["mode"] == "per-workflow"
    assert result["signaled"] == 2
    assert client.query == worker.running_entity_workflows_query()
    assert client.page_size == 100
    assert signaled == [
        (
            "demo-project/one",
            "run-1",
            worker.CHECK_WORKER_VERSION_SIGNAL,
            CheckWorkerVersionSignal(),
        ),
        (
            "demo-project/two",
            "run-2",
            worker.CHECK_WORKER_VERSION_SIGNAL,
            CheckWorkerVersionSignal(),
        ),
    ]
