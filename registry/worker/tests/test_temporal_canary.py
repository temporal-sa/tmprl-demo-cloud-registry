from __future__ import annotations

import asyncio
import json
import time
from datetime import timedelta
from types import SimpleNamespace

import pytest
from temporalio import workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from demo_registry_operator.activities import temporal_canary
from demo_registry_operator.workflows.messages import (
    TemporalRuntimeCredentialCanaryRequest,
)


API_KEY_CREDENTIAL = {
    "address": "example.tmprl.cloud:7233",
    "namespace": "example.a2dd6",
    "apiKey": "candidate-key",
    "tls": "true",
}


def test_runtime_canary_retries_after_attempt_timeout(monkeypatch):
    calls = []

    async def fake_attempt(*args, **kwargs):
        calls.append(kwargs["timeout_seconds"])
        if len(calls) == 1:
            await asyncio.sleep(1)
        return {
            "phase": "Passed",
            "ok": True,
            "authMode": "apiKey",
        }

    monkeypatch.setattr(
        temporal_canary,
        "run_temporal_runtime_canary_attempt",
        fake_attempt,
    )

    started = time.monotonic()
    status = asyncio.run(
        temporal_canary.run_temporal_runtime_canary(
            API_KEY_CREDENTIAL,
            timeout_seconds=0.5,
            attempt_timeout_seconds=0.01,
            retry_delay_seconds=0.001,
        )
    )

    assert time.monotonic() - started < 0.5
    assert status["ok"] is True
    assert status["attempts"] == 2
    assert calls[0] == temporal_canary.MIN_RUNTIME_CANARY_TIMEOUT_SECONDS


def test_runtime_canary_passes_when_worker_shutdown_cancelled_after_completion(monkeypatch):
    calls = {
        "connect": 0,
        "execute": 0,
        "exit": 0,
        "shutdown": 0,
    }

    class FakeClient:
        async def execute_workflow(self, workflow, request, **kwargs):
            calls["execute"] += 1
            return {"ok": True}

    async def fake_connect(target_host, **kwargs):
        calls["connect"] += 1
        return FakeClient()

    class FakeWorker:
        def __init__(self, client, *, task_queue, workflows):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            calls["exit"] += 1
            if calls["exit"] == 1:
                raise asyncio.CancelledError()
            return False

        async def shutdown(self):
            calls["shutdown"] += 1

    monkeypatch.setattr(
        temporal_canary,
        "Client",
        SimpleNamespace(connect=fake_connect),
    )
    monkeypatch.setattr(temporal_canary, "Worker", FakeWorker)
    monkeypatch.setattr(temporal_canary, "runtime_canary_runtime", object)

    status = asyncio.run(
        temporal_canary.run_temporal_runtime_canary(
            API_KEY_CREDENTIAL,
            timeout_seconds=1,
            attempt_timeout_seconds=0.2,
            retry_delay_seconds=0.001,
        )
    )

    assert status["ok"] is True
    assert status["attempts"] == 1
    assert status["workerShutdownCancelledAfterCompletion"] is True
    assert calls == {
        "connect": 1,
        "execute": 1,
        "exit": 1,
        "shutdown": 1,
    }


def test_runtime_canary_reraises_real_activity_cancellation(monkeypatch):
    async def cancelled_attempt(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        temporal_canary,
        "run_temporal_runtime_canary_attempt",
        cancelled_attempt,
    )
    monkeypatch.setattr(
        temporal_canary.activity,
        "is_cancelled",
        lambda: True,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            temporal_canary.run_temporal_runtime_canary(
                API_KEY_CREDENTIAL,
                timeout_seconds=1,
                attempt_timeout_seconds=0.2,
                retry_delay_seconds=0.001,
            )
        )


def test_runtime_canary_attempt_sets_client_and_workflow_timeouts(monkeypatch):
    calls = {}
    canary_runtime = object()

    class FakeClient:
        async def execute_workflow(self, workflow, request, **kwargs):
            calls["workflow"] = {
                "workflow": workflow,
                "request": request,
                **kwargs,
            }
            return {"ok": True}

    async def fake_connect(target_host, **kwargs):
        calls["connect"] = {
            "target_host": target_host,
            **kwargs,
        }
        return FakeClient()

    class FakeWorker:
        def __init__(self, client, *, task_queue, workflows):
            calls["worker"] = {
                "client": client,
                "task_queue": task_queue,
                "workflows": workflows,
            }

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def shutdown(self):
            pass

    monkeypatch.setattr(
        temporal_canary,
        "Client",
        SimpleNamespace(connect=fake_connect),
    )
    monkeypatch.setattr(temporal_canary, "Worker", FakeWorker)
    monkeypatch.setattr(
        temporal_canary,
        "runtime_canary_runtime",
        lambda: canary_runtime,
    )

    status = asyncio.run(
        temporal_canary.run_temporal_runtime_canary_attempt(
            API_KEY_CREDENTIAL,
            address="example.tmprl.cloud:7233",
            namespace="example.a2dd6",
            auth_mode="apiKey",
            timeout_seconds=5,
            connect_timeout_seconds=2,
            workflow_rpc_timeout_seconds=3,
        )
    )

    assert status["ok"] is True
    assert calls["connect"]["target_host"] == "example.tmprl.cloud:7233"
    assert calls["connect"]["namespace"] == "example.a2dd6"
    assert calls["connect"]["api_key"] == "candidate-key"
    assert calls["connect"]["tls"] is True
    assert calls["connect"]["runtime"] is canary_runtime
    assert calls["workflow"]["execution_timeout"] == timedelta(seconds=5)
    request = calls["workflow"]["request"]
    assert isinstance(request, TemporalRuntimeCredentialCanaryRequest)
    assert request.authMode == "apiKey"
    assert calls["workflow"]["run_timeout"] == timedelta(seconds=5)
    assert calls["workflow"]["task_timeout"] == timedelta(seconds=5)
    assert calls["workflow"]["rpc_timeout"] == timedelta(seconds=3)
    assert status["connectTimeoutSeconds"] == 2
    assert status["workflowRpcTimeoutSeconds"] == 3


def test_runtime_canary_runtime_disables_worker_heartbeating(monkeypatch):
    from demo_registry_operator.activities import runtime_canary

    created = []

    class FakeRuntime:
        def __init__(self, **kwargs):
            created.append(kwargs)

    monkeypatch.setattr(runtime_canary, "Runtime", FakeRuntime)
    monkeypatch.setattr(runtime_canary, "_runtime", None)

    first = runtime_canary.get_runtime()
    second = runtime_canary.get_runtime()

    assert first is second
    assert len(created) == 1
    assert created[0]["worker_heartbeat_interval"] is None


def test_runtime_canary_workflow_prepares_in_sandbox():
    async def prepare_workflow():
        SandboxedWorkflowRunner().prepare_workflow(
            workflow._Definition.must_from_class(
                temporal_canary.TemporalRuntimeCredentialCanary
            )
        )

    asyncio.run(prepare_workflow())


def test_runtime_canary_shutdown_timeout_fails_canary():
    class StuckWorker:
        async def shutdown(self):
            await asyncio.Event().wait()

    with pytest.raises(
        temporal_canary.TemporalRuntimeCanaryShutdownError,
        match="did not shut down",
    ):
        asyncio.run(
            temporal_canary.drain_canary_worker(
                StuckWorker(),
                timeout_seconds=0.001,
            )
        )


# --- External canary templates + mTLS env-var mapping (operator-canary gate) ---

from demo_registry_operator import resources as _resources  # noqa: E402
from demo_registry_operator.activities import canary as _canary  # noqa: E402


def _canary_template(name: str) -> dict:
    return {t["name"]: t for t in _canary.load_canary_templates()}[name]


def test_load_canary_templates_returns_external_self_contained_templates():
    templates = _canary.load_canary_templates()
    names = {t["name"] for t in templates}
    assert {"api-key", "mtls", "lambda", "nexus"} <= names
    for t in templates:
        assert t["authMode"] in ("apiKey", "mtls")
        assert t["demoProjectTemplate"]["kind"] == "DemoProject"


def test_nexus_canary_renders_unique_endpoint_and_cross_namespace_gate():
    nexus = _canary_template("nexus")["demoProjectTemplate"]
    project_name = "operator-canary-nexus-deadbeef"

    rendered = _canary.render_canary_demo_project(
        nexus,
        project_name=project_name,
        expected_source_sha=None,
        expected_images=None,
    )
    spec = rendered["spec"]
    endpoint = spec["temporal"]["nexusEndpoints"][project_name]
    assert endpoint == {
        "targetNamespace": "handler",
        "targetTaskQueue": "nexus-handler",
        "allowedCallerNamespaces": ["primary"],
    }
    assert spec["source"]["repo"] == (
        "https://github.com/temporal-sa/catalog-project-example-nexus"
    )

    caller = spec["components"]["caller"]
    caller_env = {item["name"]: item["value"] for item in caller["env"]}
    gate = caller["worker"]["versioning"]["rollout"]["gate"]
    assert caller_env["NEXUS_ENDPOINT"] == project_name
    assert gate["workflowType"] == "NexusE2ECanaryWorkflow"
    assert gate["input"]["endpoint"] == project_name
    assert caller["worker"]["versioning"]["enabled"] is True

    handler = spec["components"]["handler"]
    assert handler["worker"] is True
    assert handler["temporalNamespace"] == "handler"
    assert _canary.CANARY_NEXUS_ENDPOINT_PLACEHOLDER not in json.dumps(rendered)


def test_mtls_canary_template_worker_exposes_app_cert_env_and_no_api_key():
    # The fix: the mtls worker must expose TEMPORAL_CERT_PATH / TEMPORAL_KEY_PATH
    # (the names the banking app reads) pointing at the mounted cert, and NO API key
    # (else the app prefers api-key auth and never uses mTLS).
    mtls = _canary_template("mtls")["demoProjectTemplate"]
    worker = mtls["spec"]["components"]["worker"]
    spec = _resources.workload_pod_spec(
        mtls, "worker", worker, "ex.com/worker:git-x",
        image_overrides={"worker": "ex.com/worker:git-x"},
    )
    env = {e["name"]: e.get("value") for e in spec["containers"][0].get("env", [])}
    assert _resources.temporal_auth_mode(mtls) == "mtls"
    assert _resources.mounts_temporal_mtls_cert_files(mtls, worker)
    assert env.get("TEMPORAL_CERT_PATH", "").endswith("/tls.crt")
    assert env.get("TEMPORAL_KEY_PATH", "").endswith("/tls.key")
    assert "TEMPORAL_API_KEY" not in env


def test_lambda_canary_template_uses_serverless_worker_deployment_config():
    lambda_canary = _canary_template("lambda")["demoProjectTemplate"]
    worker = lambda_canary["spec"]["components"]["worker"]

    assert "worker" not in worker
    assert _resources.serverless_lambda_enabled(worker)
    assert not _resources.worker_versioning_enabled(worker)
    assert _resources.serverless_worker_deployment_name(
        "worker",
        worker,
    ) == "money-transfer"

    worker_deployment = worker["serverless"]["workerDeployment"]
    assert "buildId" not in worker_deployment
    assert "env" not in worker_deployment
    assert _resources.serverless_worker_deployment_env_names(worker) == {
        "deploymentName": (),
        "buildId": (),
    }


def test_api_key_canary_template_worker_has_no_cert_mount():
    apikey = _canary_template("api-key")["demoProjectTemplate"]
    worker = apikey["spec"]["components"]["worker"]
    assert _resources.temporal_auth_mode(apikey) != "mtls"
    assert not _resources.mounts_temporal_mtls_cert_files(apikey, worker)


def test_canary_failed_fails_fast_on_terminal_rollout_phases():
    # The gate's promotion wait must treat terminal rollout-failure phases as
    # failures so it fails fast instead of polling until the timeout.
    from demo_registry_operator.activities.canary import canary_failed

    for phase in ("CandidateNotReady", "SmokeCheckFailed", "WorkerRolloutFailed",
                  "Error", "FinalizationError"):
        assert canary_failed({"phase": phase}) is True, phase
    # in-progress / success phases must NOT be treated as failed
    for phase in ("Reconciling", "SmokeCheckPending", "Promoted", "Ready"):
        assert canary_failed({"phase": phase}) is False, phase
    # candidate-level terminal failures also count
    assert canary_failed({"candidateRolloutStatus": {"phase": "SmokeCheckFailed"}}) is True
