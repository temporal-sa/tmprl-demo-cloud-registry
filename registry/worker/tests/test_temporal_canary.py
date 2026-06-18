from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from types import SimpleNamespace

from demo_registry_operator.activities import temporal_canary


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


def test_runtime_canary_attempt_sets_client_and_workflow_timeouts(monkeypatch):
    calls = {}

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

    monkeypatch.setattr(
        temporal_canary,
        "Client",
        SimpleNamespace(connect=fake_connect),
    )
    monkeypatch.setattr(temporal_canary, "Worker", FakeWorker)

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
    assert calls["workflow"]["execution_timeout"] == timedelta(seconds=5)
    assert calls["workflow"]["run_timeout"] == timedelta(seconds=5)
    assert calls["workflow"]["task_timeout"] == timedelta(seconds=5)
    assert calls["workflow"]["rpc_timeout"] == timedelta(seconds=3)
    assert status["connectTimeoutSeconds"] == 2
    assert status["workflowRpcTimeoutSeconds"] == 3
