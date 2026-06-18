"""Credential canary worker for project Temporal runtime auth."""

from __future__ import annotations

import asyncio
import math
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker


DEFAULT_RUNTIME_CANARY_TIMEOUT_SECONDS = 60.0
DEFAULT_RUNTIME_CANARY_ATTEMPT_TIMEOUT_SECONDS = 20.0
DEFAULT_RUNTIME_CANARY_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_RUNTIME_CANARY_WORKFLOW_RPC_TIMEOUT_SECONDS = 10.0
DEFAULT_RUNTIME_CANARY_WORKFLOW_TASK_TIMEOUT_SECONDS = 10.0
MIN_RUNTIME_CANARY_TIMEOUT_SECONDS = 0.1


@workflow.defn(name="TemporalRuntimeCredentialCanary")
class TemporalRuntimeCredentialCanary:
    """Tiny workflow used to prove a candidate runtime credential can work."""

    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": True,
            "nonce": request.get("nonce"),
            "checkedAt": workflow.now().isoformat(),
        }


async def run_temporal_runtime_canary(
    credential: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_RUNTIME_CANARY_TIMEOUT_SECONDS,
    attempt_timeout_seconds: float = DEFAULT_RUNTIME_CANARY_ATTEMPT_TIMEOUT_SECONDS,
    retry_delay_seconds: float = 2.0,
) -> dict[str, Any]:
    """Start a one-off worker and workflow using candidate runtime credentials.

    The caller must pass the candidate credential before it is written to the
    runtime secret. A successful return means the new API key or client
    certificate authenticated, polled a task, and completed a workflow in the
    target namespace.
    """

    total_timeout = positive_timeout_seconds(
        timeout_seconds,
        field="timeout_seconds",
    )
    per_attempt_timeout = positive_timeout_seconds(
        attempt_timeout_seconds,
        field="attempt_timeout_seconds",
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total_timeout
    address = string_field(credential, "address")
    namespace = string_field(credential, "namespace")
    auth_mode = credential_auth_mode(credential)
    attempt = 0
    while True:
        attempt += 1
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(
                "Temporal runtime credential canary did not pass within "
                f"{total_timeout:g} seconds."
            )
        attempt_budget = bounded_timeout_seconds(per_attempt_timeout, remaining)
        try:
            status = await asyncio.wait_for(
                run_temporal_runtime_canary_attempt(
                    credential,
                    address=address,
                    namespace=namespace,
                    auth_mode=auth_mode,
                    timeout_seconds=attempt_budget,
                ),
                timeout=attempt_budget,
            )
            status["attempts"] = attempt
            status["canaryTimeoutSeconds"] = total_timeout
            status["attemptTimeoutSeconds"] = attempt_budget
            return status
        except asyncio.CancelledError as error:
            if current_task_cancel_requested():
                raise
            if loop.time() >= deadline:
                raise TimeoutError(
                    "Temporal runtime credential canary did not pass within "
                    f"{total_timeout:g} seconds after {attempt} attempts."
                ) from error
            await sleep_before_retry(
                deadline=deadline,
                retry_delay_seconds=retry_delay_seconds,
            )
        except Exception as error:  # noqa: BLE001 - retry propagation/transient auth lag
            if loop.time() >= deadline:
                if isinstance(error, TimeoutError):
                    raise TimeoutError(
                        "Temporal runtime credential canary did not pass within "
                        f"{total_timeout:g} seconds after {attempt} attempts."
                    ) from error
                raise
            await sleep_before_retry(
                deadline=deadline,
                retry_delay_seconds=retry_delay_seconds,
            )


async def run_temporal_runtime_canary_attempt(
    credential: dict[str, Any],
    *,
    address: str,
    namespace: str,
    auth_mode: str,
    timeout_seconds: float,
    connect_timeout_seconds: float = DEFAULT_RUNTIME_CANARY_CONNECT_TIMEOUT_SECONDS,
    workflow_rpc_timeout_seconds: float = (
        DEFAULT_RUNTIME_CANARY_WORKFLOW_RPC_TIMEOUT_SECONDS
    ),
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    attempt_timeout = positive_timeout_seconds(
        timeout_seconds,
        field="timeout_seconds",
    )
    connect_timeout = bounded_timeout_seconds(
        connect_timeout_seconds,
        attempt_timeout,
    )
    workflow_rpc_timeout = bounded_timeout_seconds(
        workflow_rpc_timeout_seconds,
        attempt_timeout,
    )
    workflow_task_timeout = bounded_timeout_seconds(
        DEFAULT_RUNTIME_CANARY_WORKFLOW_TASK_TIMEOUT_SECONDS,
        attempt_timeout,
    )
    nonce = uuid.uuid4().hex
    task_queue = f"tmprl-auth-canary-{nonce[:16]}"
    workflow_id = f"tmprl-demo-cloud-auth-canary/{auth_mode}/{nonce}"
    client = await asyncio.wait_for(
        Client.connect(
            address,
            namespace=namespace,
            identity=f"tmprl-demo-cloud-auth-canary-{nonce[:12]}",
            **client_auth_kwargs(credential, auth_mode),
        ),
        timeout=connect_timeout,
    )

    original_error: Exception | None = None
    try:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[TemporalRuntimeCredentialCanary],
        ):
            try:
                result = await asyncio.wait_for(
                    client.execute_workflow(
                        TemporalRuntimeCredentialCanary.run,
                        {"nonce": nonce, "authMode": auth_mode},
                        id=workflow_id,
                        task_queue=task_queue,
                        execution_timeout=timeout_delta(attempt_timeout),
                        run_timeout=timeout_delta(attempt_timeout),
                        task_timeout=timeout_delta(workflow_task_timeout),
                        rpc_timeout=timeout_delta(workflow_rpc_timeout),
                    ),
                    timeout=attempt_timeout,
                )
            except Exception as error:  # noqa: BLE001 - preserve through worker shutdown
                original_error = error
                raise
    except asyncio.CancelledError:
        if original_error is not None:
            raise original_error
        raise

    return {
        "phase": "Passed",
        "ok": True,
        "authMode": auth_mode,
        "namespace": namespace,
        "taskQueue": task_queue,
        "workflowID": workflow_id,
        "startedAt": started_at.isoformat(),
        "completedAt": datetime.now(UTC).isoformat(),
        "connectTimeoutSeconds": connect_timeout,
        "workflowRpcTimeoutSeconds": workflow_rpc_timeout,
        "workflowTaskTimeoutSeconds": workflow_task_timeout,
        "result": result,
    }


def credential_auth_mode(credential: dict[str, Any]) -> str:
    if credential.get("apiKey"):
        return "apiKey"
    if credential.get("clientCert") and credential.get("clientKey"):
        return "mtls"
    raise ValueError("Temporal runtime credential must include apiKey or client certs.")


def client_auth_kwargs(
    credential: dict[str, Any],
    auth_mode: str,
) -> dict[str, Any]:
    if auth_mode == "apiKey":
        return {
            "api_key": string_field(credential, "apiKey"),
            "tls": string_field(credential, "tls").lower() == "true",
        }
    if auth_mode == "mtls":
        return {
            "tls": TLSConfig(
                client_cert=string_field(credential, "clientCert").encode("utf-8"),
                client_private_key=string_field(credential, "clientKey").encode(
                    "utf-8"
                ),
            )
        }
    raise ValueError(f"Unsupported Temporal runtime auth mode {auth_mode!r}.")


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field:
        return field
    raise ValueError(f"Temporal runtime canary credential is missing {key}.")


def positive_timeout_seconds(value: float, *, field: str) -> float:
    timeout = float(value)
    if timeout <= 0:
        raise ValueError(f"{field} must be positive.")
    return timeout


def bounded_timeout_seconds(value: float, maximum: float) -> float:
    return max(MIN_RUNTIME_CANARY_TIMEOUT_SECONDS, min(float(value), float(maximum)))


def timeout_delta(seconds: float) -> timedelta:
    return timedelta(seconds=max(1, math.ceil(seconds)))


def current_task_cancel_requested() -> bool:
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


async def sleep_before_retry(
    *,
    deadline: float,
    retry_delay_seconds: float,
) -> None:
    await asyncio.sleep(
        min(
            retry_delay_seconds,
            max(
                MIN_RUNTIME_CANARY_TIMEOUT_SECONDS,
                deadline - asyncio.get_running_loop().time(),
            ),
        )
    )
