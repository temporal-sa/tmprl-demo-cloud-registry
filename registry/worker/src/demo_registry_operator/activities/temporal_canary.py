"""Credential canary worker for project Temporal runtime auth."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker


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
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    """Start a one-off worker and workflow using candidate runtime credentials.

    The caller must pass the candidate credential before it is written to the
    runtime secret. A successful return means the new API key or client
    certificate authenticated, polled a task, and completed a workflow in the
    target namespace.
    """

    started_at = datetime.now(UTC)
    address = string_field(credential, "address")
    namespace = string_field(credential, "namespace")
    auth_mode = credential_auth_mode(credential)
    nonce = uuid.uuid4().hex
    task_queue = f"tmprl-auth-canary-{nonce[:16]}"
    workflow_id = f"tmprl-demo-cloud-auth-canary/{auth_mode}/{nonce}"
    client = await Client.connect(
        address,
        namespace=namespace,
        identity=f"tmprl-demo-cloud-auth-canary-{nonce[:12]}",
        **client_auth_kwargs(credential, auth_mode),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TemporalRuntimeCredentialCanary],
    ):
        result = await client.execute_workflow(
            TemporalRuntimeCredentialCanary.run,
            {"nonce": nonce, "authMode": auth_mode},
            id=workflow_id,
            task_queue=task_queue,
            execution_timeout=timedelta(seconds=timeout_seconds),
        )

    return {
        "phase": "Passed",
        "ok": True,
        "authMode": auth_mode,
        "namespace": namespace,
        "taskQueue": task_queue,
        "workflowID": workflow_id,
        "startedAt": started_at.isoformat(),
        "completedAt": datetime.now(UTC).isoformat(),
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
