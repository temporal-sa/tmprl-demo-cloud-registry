"""Runtime authentication reconciliation for Temporal Cloud namespaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from demo_registry_operator.kube import read_namespace, upsert_secret_data
from demo_registry_operator.resources import (
    STANDARD_TEMPORAL_ENV,
    TEMPORAL_SECRET_NAME,
    TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
    common_labels,
    project_name,
    project_namespace,
    temporal_components,
    temporal_proxy_components,
    temporal_secret_remote_path,
    unversioned_temporal_components,
    versioned_worker_components,
)


@dataclass
class ApiKeyResult:
    key_id: str
    token: str
    expiry_time: str | None = None


class RuntimeNamespace(Protocol):
    qualified_name: str
    grpc_address: str


class ApiKeyAuthReconciler(Protocol):
    async def find_service_account(self, name: str) -> str | None: ...

    async def create_service_account(
        self, name: str, qualified_namespace: str, description: str
    ) -> str: ...

    async def create_api_key(
        self,
        service_account_id: str,
        display_name: str,
        description: str,
        ttl_days: int,
    ) -> ApiKeyResult: ...


class SecretStore(Protocol):
    def read_json(self, path: str) -> dict[str, Any] | None: ...

    def write_json(
        self, path: str, value: dict[str, Any], description: str | None = None
    ) -> None: ...


async def reconcile_api_key_auth(
    demo_project: dict[str, Any],
    *,
    namespace: RuntimeNamespace,
    reconciler: ApiKeyAuthReconciler,
    secret_store: SecretStore,
    checked_at: str,
    api_key_ttl_days: int,
    rotate_before_days: int,
    mirror_kubernetes_secrets: bool = True,
) -> dict[str, Any]:
    """Create-or-verify API-key runtime auth for one Temporal Cloud namespace.

    Namespace lifecycle is intentionally owned by ``temporal_cloud``. This
    function owns the auth-side resources that depend on the namespace: the
    namespace-scoped service account, its short-lived API key, and the runtime
    secret projected into project workloads.
    """

    project = project_name(demo_project)
    service_account_name = f"{project}-runtime"
    service_account_id = await reconciler.find_service_account(service_account_name)
    service_account_created = False
    if service_account_id is None:
        service_account_id = await reconciler.create_service_account(
            service_account_name,
            namespace.qualified_name,
            f"tmprl-demo.cloud runtime service account for {project}",
        )
        service_account_created = True

    runtime_path = temporal_secret_remote_path(demo_project)
    existing = secret_store.read_json(runtime_path)
    rotation = temporal_api_key_rotation_decision(
        existing,
        checked_at=checked_at,
        rotate_before_days=rotate_before_days,
    )
    if isinstance(existing, dict) and existing.get("apiKey") and not rotation["rotate"]:
        key_id = existing.get("keyId")
        expiry_time = existing.get("expiryTime")
        key_action = "verified"
        created_at = existing.get("createdAt")
        previous_key_id = None
    else:
        result = await reconciler.create_api_key(
            service_account_id,
            service_account_name,
            f"tmprl-demo.cloud runtime key for {project}",
            api_key_ttl_days,
        )
        key_id = result.key_id
        expiry_time = result.expiry_time
        previous_key_id = existing.get("keyId") if isinstance(existing, dict) else None
        previous_expiry_time = (
            existing.get("expiryTime") if isinstance(existing, dict) else None
        )
        key_action = (
            "rotated"
            if isinstance(existing, dict) and existing.get("apiKey")
            else "created"
        )
        created_at = checked_at
        secret_value: dict[str, Any] = {
            "address": namespace.grpc_address,
            "namespace": namespace.qualified_name,
            "apiKey": result.token,
            "tls": "true",
            "keyId": result.key_id,
            "serviceAccountId": service_account_id,
            "createdAt": checked_at,
            "rotationReason": rotation["reason"],
        }
        if expiry_time:
            secret_value["expiryTime"] = expiry_time
        if previous_key_id:
            secret_value["previousKeyId"] = previous_key_id
        if previous_expiry_time:
            secret_value["previousExpiryTime"] = previous_expiry_time
        secret_store.write_json(
            runtime_path,
            secret_value,
            f"Temporal Cloud runtime credentials for {project}",
        )
        if mirror_kubernetes_secrets:
            mirror_runtime_kubernetes_secrets(demo_project, secret_value)

    api_key_status: dict[str, Any] = {
        "keyId": key_id,
        "action": key_action,
        "scope": "namespace",
        "namespace": namespace.qualified_name,
        "ttlDays": api_key_ttl_days,
        "rotateBeforeExpiryDays": rotate_before_days,
        "secretRemotePath": runtime_path,
        "expiryTime": expiry_time,
        "createdAt": created_at,
        "components": sorted(temporal_components(demo_project)),
    }
    if previous_key_id:
        api_key_status["previousKeyId"] = previous_key_id
    if rotation.get("reason"):
        api_key_status["rotationReason"] = rotation["reason"]

    return {
        "serviceAccount": {
            "id": service_account_id,
            "name": service_account_name,
            "action": "created" if service_account_created else "verified",
        },
        "apiKey": api_key_status,
    }


def temporal_api_key_rotation_decision(
    existing: Any,
    *,
    checked_at: str,
    rotate_before_days: int,
) -> dict[str, Any]:
    if not isinstance(existing, dict) or not existing.get("apiKey"):
        return {"rotate": True, "reason": "MissingRuntimeSecret"}
    if not existing.get("keyId"):
        return {"rotate": True, "reason": "MissingKeyId"}

    expiry_time = existing.get("expiryTime")
    if not isinstance(expiry_time, str) or not expiry_time:
        return {"rotate": True, "reason": "MissingExpiryTime"}

    expiry = parse_datetime(expiry_time)
    checked = parse_datetime(checked_at)
    if expiry is None:
        return {"rotate": True, "reason": "InvalidExpiryTime"}
    if checked is None:
        checked = datetime.now(UTC)

    rotate_at = expiry - timedelta(days=rotate_before_days)
    if checked >= rotate_at:
        return {
            "rotate": True,
            "reason": "RotateBeforeExpiry",
            "rotateAt": rotate_at.isoformat(),
        }

    return {
        "rotate": False,
        "reason": "ExistingKeyStillFresh",
        "rotateAt": rotate_at.isoformat(),
    }


def parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def mirror_runtime_kubernetes_secrets(
    demo_project: dict[str, Any],
    secret_value: dict[str, Any],
) -> dict[str, str]:
    namespace = project_namespace(demo_project)
    if read_namespace(namespace) is None:
        return {"phase": "Skipped", "reason": "NamespaceMissing"}

    labels = common_labels(demo_project)
    annotations = {
        "platform.temporal.io/secret-source": temporal_secret_remote_path(demo_project),
        "platform.temporal.io/temporal-api-key-id": str(secret_value["keyId"]),
    }
    actions: dict[str, str] = {}
    if unversioned_temporal_components(demo_project) or temporal_proxy_components(
        demo_project
    ):
        actions[TEMPORAL_SECRET_NAME] = upsert_secret_data(
            namespace,
            TEMPORAL_SECRET_NAME,
            {
                STANDARD_TEMPORAL_ENV["endpoint"]: str(secret_value["address"]),
                STANDARD_TEMPORAL_ENV["namespace"]: str(secret_value["namespace"]),
                STANDARD_TEMPORAL_ENV["api_key"]: str(secret_value["apiKey"]),
                STANDARD_TEMPORAL_ENV["tls"]: str(secret_value["tls"]),
            },
            labels=labels,
            annotations=annotations,
        )
    if versioned_worker_components(demo_project):
        actions[TEMPORAL_WORKER_RUNTIME_SECRET_NAME] = upsert_secret_data(
            namespace,
            TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
            {
                STANDARD_TEMPORAL_ENV["api_key"]: str(secret_value["apiKey"]),
                STANDARD_TEMPORAL_ENV["tls"]: str(secret_value["tls"]),
            },
            labels=labels,
            annotations=annotations,
        )
    return actions
