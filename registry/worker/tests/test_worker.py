from __future__ import annotations

import asyncio
from types import SimpleNamespace

from temporalio import workflow
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


def test_workflow_runner_prepares_registered_workflows_in_sandbox():
    async def prepare_workflows() -> None:
        workflow_runner = worker.create_workflow_runner()
        for workflow_type in (
            worker.DemoProjectWorkflow,
            worker.DemoProjectArtifactBuildWorkflow,
            worker.DemoProjectKubernetesRuntimeWorkflow,
            worker.DemoProjectServerlessLambdaWorkflow,
            worker.InstruqtCourseWorkflow,
            worker.DemoRolloutWorkflow,
            worker.RegistryOperatorCanaryWorkflow,
            worker.RegistryOperatorAuthCanaryWorkflow,
        ):
            workflow_runner.prepare_workflow(
                workflow._Definition.must_from_class(workflow_type)
            )

    asyncio.run(prepare_workflows())


def test_workflow_runner_has_centralized_passthrough_modules():
    workflow_runner = worker.create_workflow_runner()
    passthrough_modules = workflow_runner.restrictions.passthrough_modules

    for module in worker.WORKFLOW_PASSTHROUGH_MODULES:
        assert module in passthrough_modules


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


def test_liveness_heartbeat_refreshes_via_temporal_health_check(tmp_path, monkeypatch):
    # Must drive the heartbeat from the worker's OWN client via a real
    # WorkflowService round-trip (describe_namespace) — an OS-level probe would
    # falsely pass while the in-process resolver is wedged. (Not check_health:
    # the gRPC health service can't decompress the 1.29 client's gzip responses.)
    path = tmp_path / "alive"
    calls = {"describe": 0}

    async def fake_describe_namespace(_req, **kwargs):
        calls["describe"] += 1
        return SimpleNamespace()

    client = SimpleNamespace(
        namespace="ns.acct",
        workflow_service=SimpleNamespace(describe_namespace=fake_describe_namespace),
    )

    async def stop_after_one(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(worker.asyncio, "sleep", stop_after_one)
    try:
        asyncio.run(worker.liveness_heartbeat(client, path=str(path), interval_seconds=0))
    except asyncio.CancelledError:
        pass
    assert path.exists()
    assert calls["describe"] == 1


def test_liveness_heartbeat_survives_unreachable_temporal(tmp_path, monkeypatch):
    # A wedged connection (the round-trip raises) must NOT crash the loop; it
    # stops refreshing the file so the probe restarts the pod.
    path = tmp_path / "alive"
    sleeps = {"n": 0}

    async def fake_describe_namespace(_req, **kwargs):
        raise RuntimeError("dns error")

    client = SimpleNamespace(
        namespace="ns.acct",
        workflow_service=SimpleNamespace(describe_namespace=fake_describe_namespace),
    )

    async def count_then_stop(_seconds):
        sleeps["n"] += 1
        raise asyncio.CancelledError

    monkeypatch.setattr(worker.asyncio, "sleep", count_then_stop)
    try:
        asyncio.run(worker.liveness_heartbeat(client, path=str(path), interval_seconds=0))
    except asyncio.CancelledError:
        pass
    assert sleeps["n"] == 1  # the failed round-trip was caught; loop proceeded


def test_connect_enables_dns_load_balancing(monkeypatch):
    # The poisoned-resolver wedge is recovered by Core periodically re-resolving
    # DNS; connect() must pass dns_load_balancing_config so that's actually on.
    import asyncio as _asyncio
    from temporalio.client import Client
    from demo_registry_operator import temporal_client

    captured = {}

    async def fake_connect(address, **kwargs):
        captured["address"] = address
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(Client, "connect", staticmethod(fake_connect))
    config = SimpleNamespace(
        temporal_address="ns.acct.tmprl.cloud:7233",
        temporal_namespace="ns.acct",
        temporal_tls=True,
        temporal_api_key="k",
    )
    _asyncio.run(temporal_client.connect(config))
    dlb = captured.get("dns_load_balancing_config")
    assert dlb is not None, "connect() did not pass dns_load_balancing_config"
    assert dlb.resolution_interval_millis == temporal_client.TEMPORAL_DNS_RESOLUTION_INTERVAL_MS
    # gzip (1.29 default) is intentionally left on — the heartbeat avoids the one
    # RPC that can't decompress it rather than disabling compression globally.
    from temporalio.service import GrpcCompression

    assert captured.get("grpc_compression") in (None, GrpcCompression.GZIP), (
        "connect() should not disable gzip compression"
    )
