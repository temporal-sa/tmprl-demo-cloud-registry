"""Temporal Cloud control-plane reconciliation via the Cloud Operations API.

This module creates or verifies the per-project Temporal Cloud namespace, a
namespace-scoped service account, and a runtime API key, then writes the runtime
credential to AWS Secrets Manager so External Secrets can project it into the
project pods.

It uses the Temporal Cloud Operations API through the Python SDK's
``CloudOperationsClient``. The SDK returns typed responses (including the
one-time API-key token), needs no extra binary in the worker image, and maps
async control-plane operations onto activity heartbeats and retries.

The orchestration logic in :func:`reconcile_temporal_cloud` is deliberately
decoupled from gRPC/boto3 I/O (the ``reconciler`` and ``secret_store`` are
injected) so it can be unit-tested with fakes. The real adapters
(:class:`CloudApiReconciler`, :class:`SecretsManagerStore`) wrap the SDK and
boto3.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from demo_registry_operator import derive
from demo_registry_operator.activities.temporal_auth import (
    ApiKeyResult,
    mirror_runtime_kubernetes_secrets,
    parse_datetime,
    reconcile_api_key_auth,
    temporal_api_key_rotation_decision,
)
from demo_registry_operator.resources import (
    TEMPORAL_AUTH_MODE_API_KEY,
    project_name,
    temporal_auth_mode,
    temporal_auth_runtime_status,
    temporal_auth_status,
    temporal_components,
    temporal_secret_remote_path,
)


# Minted runtime keys are short-lived (Temporal's default is 30 days). The entity
# workflow rotates them on a durable timer ~10 days before expiry (i.e. every
# ~20 days), re-minting and updating the runtime secret.
TEMPORAL_API_KEY_TTL_DAYS = 30
TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS = 10
# Demo namespaces keep short retention; demos do not need long history retention
# and retained storage is a cost driver.
DEFAULT_NAMESPACE_RETENTION_DAYS = 7
DEFAULT_OPS_TARGET_HOST = "saas-api.tmprl.cloud:443"
# Region demo namespaces are created in when the bootstrap credential does not
# specify one. Must be a region the Temporal Cloud account can provision into.
DEFAULT_TEMPORAL_CLOUD_REGION = "gcp-us-central1"
# A DELETED namespace frees its name, so it is treated as absent (recreate).
DELETED_NAMESPACE_STATES = {"RESOURCE_STATE_DELETED"}
# Reusable as-is.
REUSABLE_NAMESPACE_STATES = {"RESOURCE_STATE_ACTIVE", "RESOURCE_STATE_ACTIVATING"}
# Present but not safe to adopt or recreate over — surface an error.
UNUSABLE_NAMESPACE_STATES = {
    "RESOURCE_STATE_ACTIVATION_FAILED",
    "RESOURCE_STATE_DELETE_FAILED",
    "RESOURCE_STATE_DELETING",
    "RESOURCE_STATE_SUSPENDED",
    "RESOURCE_STATE_EXPIRED",
}
ASYNC_OPERATION_POLL_SECONDS = 2.0
ASYNC_OPERATION_TIMEOUT_SECONDS = 300.0


@dataclass
class NamespaceInfo:
    qualified_name: str
    grpc_address: str
    web_address: str | None = None
    state: str | None = None
    created: bool = False


class TemporalCloudReconciler(Protocol):
    async def list_regions(self) -> list[str]: ...

    async def find_namespace(self, name: str) -> NamespaceInfo | None: ...

    async def disable_namespace_delete_protection(self, qualified_name: str) -> str: ...

    async def delete_namespace(self, qualified_name: str) -> str: ...

    async def delete_api_key(self, key_id: str) -> str: ...

    async def delete_service_account(self, service_account_id: str) -> str: ...

    async def create_namespace(
        self,
        name: str,
        region: str,
        retention_days: int,
        enable_delete_protection: bool = True,
    ) -> NamespaceInfo: ...

    async def find_service_account(self, name: str) -> str | None: ...

    async def create_service_account(
        self, name: str, qualified_namespace: str, description: str
    ) -> str: ...

    async def create_api_key(
        self, service_account_id: str, display_name: str, description: str, ttl_days: int
    ) -> ApiKeyResult: ...


class SecretStore(Protocol):
    def read_json(self, path: str) -> dict[str, Any] | None: ...

    def write_json(
        self, path: str, value: dict[str, Any], description: str | None = None
    ) -> None: ...

    def delete(self, path: str) -> str: ...


def desired_namespace_name(demo_project: dict[str, Any]) -> str:
    return derive.temporal_namespace(project_name(demo_project))


async def reconcile_temporal_cloud(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    account_id: str | None,
    region: str,
    checked_at: str,
    api_key_ttl_days: int = TEMPORAL_API_KEY_TTL_DAYS,
    rotate_before_days: int = TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS,
    retention_days: int = DEFAULT_NAMESPACE_RETENTION_DAYS,
    enable_delete_protection: bool = True,
    mirror_kubernetes_secrets: bool = True,
) -> dict[str, Any]:
    """Create-or-verify the namespace, scoped service account, and runtime key.

    Idempotency rules:

    - Namespace and service account are looked up first and only created when
      absent (create-or-verify).
    - A new API key is minted only when the runtime secret is absent or has no
      ``apiKey``. API-key tokens cannot be read back after creation, so
      re-minting on every reconcile would sprawl keys; instead the existing
      runtime secret is treated as the source of truth for an active key.
    """

    if not temporal_components(demo_project):
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "message": "No component currently requires Temporal credentials.",
        }

    auth_mode = temporal_auth_mode(demo_project)
    if auth_mode != TEMPORAL_AUTH_MODE_API_KEY:
        return {
            "phase": "Error",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "reason": "UnsupportedTemporalAuthMode",
            "message": (
                "Temporal Cloud mTLS auth is accepted in project configuration "
                "but is not implemented by this operator version."
            ),
            "auth": temporal_auth_status(demo_project),
            **temporal_auth_runtime_status(demo_project),
        }

    short_name = desired_namespace_name(demo_project)

    namespace = await reconciler.find_namespace(short_name)
    # A deleted namespace has freed its name; treat it as absent so we recreate.
    if namespace is not None and namespace.state in DELETED_NAMESPACE_STATES:
        namespace = None
    # An existing namespace in a non-reusable, non-deleted state must not be
    # silently adopted (nor recreated over) — surface it for attention.
    if (
        namespace is not None
        and namespace.state in UNUSABLE_NAMESPACE_STATES
    ):
        return {
            "phase": "Error",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "reason": "NamespaceUnusable",
            "message": (
                f"Temporal Cloud namespace {namespace.qualified_name!r} exists in "
                f"state {namespace.state}; not reusing or recreating."
            ),
            "namespace": {"name": namespace.qualified_name, "state": namespace.state},
        }

    namespace_created = False
    if namespace is None:
        # Fail fast if the requested region cannot be provisioned by this account
        # rather than creating a namespace that will get stuck in
        # ACTIVATION_FAILED (and cannot then be deleted via the API).
        available_regions = await reconciler.list_regions()
        if region not in available_regions:
            return {
                "phase": "Error",
                "provider": "temporal-cloud",
                "mode": "cloud-api",
                "checkedAt": checked_at,
                "reason": "RegionUnavailable",
                "message": (
                    f"Temporal Cloud region {region!r} is not available to this "
                    "account; choose one of the available regions."
                ),
                "region": region,
                "availableRegions": sorted(available_regions),
            }
        namespace = await reconciler.create_namespace(
            short_name,
            region,
            retention_days,
            enable_delete_protection=enable_delete_protection,
        )
        namespace_created = True

    auth_status = await reconcile_api_key_auth(
        demo_project,
        namespace=namespace,
        reconciler=reconciler,
        secret_store=secret_store,
        checked_at=checked_at,
        api_key_ttl_days=api_key_ttl_days,
        rotate_before_days=rotate_before_days,
        mirror_kubernetes_secrets=mirror_kubernetes_secrets,
    )

    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "accountId": account_id,
        "region": region,
        "auth": temporal_auth_status(demo_project),
        "namespace": {
            "name": namespace.qualified_name,
            "action": "created" if namespace_created else "verified",
            "deletionPolicy": "retain-delete-protected",
            "grpcAddress": namespace.grpc_address,
            "webAddress": namespace.web_address,
            "state": namespace.state,
        },
        "serviceAccount": auth_status["serviceAccount"],
        "apiKey": auth_status["apiKey"],
    }


async def finalize_temporal_cloud(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    """Delete the project's Temporal Cloud namespace, service account, API key,
    and runtime secret. Idempotent: anything already gone is treated as deleted.

    Driven by project removal from git (the explicit deletion approval), so this
    is allowed to destroy the namespace (disabling delete protection first)."""

    if not temporal_components(demo_project):
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "message": "No Temporal Cloud resources to delete.",
        }

    runtime_path = temporal_secret_remote_path(demo_project)
    existing = secret_store.read_json(runtime_path)
    existing = existing if isinstance(existing, dict) else {}
    key_id = existing.get("keyId")
    service_account_id = existing.get("serviceAccountId")
    qualified = existing.get("namespace")

    deleted: dict[str, Any] = {}
    if key_id:
        deleted["apiKey"] = await reconciler.delete_api_key(key_id)
    if service_account_id:
        deleted["serviceAccount"] = await reconciler.delete_service_account(
            service_account_id
        )

    if not qualified:
        found = await reconciler.find_namespace(desired_namespace_name(demo_project))
        qualified = found.qualified_name if found else None
    if qualified:
        deleted["namespaceDeleteProtection"] = (
            await reconciler.disable_namespace_delete_protection(qualified)
        )
        deleted["namespace"] = await reconciler.delete_namespace(qualified)

    if existing:
        deleted["secret"] = secret_store.delete(runtime_path)

    return {
        "phase": "Deleted",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "namespace": {"name": qualified},
        "deleted": deleted,
    }


async def delete_temporal_cloud_api_key(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    base = finalization_step_base(demo_project, checked_at)
    if base is not None:
        return base

    existing = temporal_cloud_runtime_secret(demo_project, secret_store)
    key_id = existing.get("keyId")
    action = (
        await reconciler.delete_api_key(key_id)
        if isinstance(key_id, str)
        else "not-found"
    )
    return temporal_cloud_step_status(
        checked_at,
        deleted={"apiKey": action},
    )


async def delete_temporal_cloud_service_account(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    base = finalization_step_base(demo_project, checked_at)
    if base is not None:
        return base

    existing = temporal_cloud_runtime_secret(demo_project, secret_store)
    service_account_id = existing.get("serviceAccountId")
    action = (
        await reconciler.delete_service_account(service_account_id)
        if isinstance(service_account_id, str)
        else "not-found"
    )
    return temporal_cloud_step_status(
        checked_at,
        deleted={"serviceAccount": action},
    )


async def disable_temporal_cloud_namespace_delete_protection(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    base = finalization_step_base(demo_project, checked_at)
    if base is not None:
        return base

    qualified = await resolve_temporal_cloud_namespace(
        demo_project,
        reconciler=reconciler,
        secret_store=secret_store,
    )
    action = (
        await reconciler.disable_namespace_delete_protection(qualified)
        if isinstance(qualified, str)
        else "not-found"
    )
    return temporal_cloud_step_status(
        checked_at,
        namespace=qualified,
        deleted={"namespaceDeleteProtection": action},
    )


async def delete_temporal_cloud_namespace(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    base = finalization_step_base(demo_project, checked_at)
    if base is not None:
        return base

    qualified = await resolve_temporal_cloud_namespace(
        demo_project,
        reconciler=reconciler,
        secret_store=secret_store,
    )
    action = (
        await reconciler.delete_namespace(qualified)
        if isinstance(qualified, str)
        else "not-found"
    )
    return temporal_cloud_step_status(
        checked_at,
        namespace=qualified,
        deleted={"namespace": action},
    )


def delete_temporal_cloud_runtime_secret(
    demo_project: dict[str, Any],
    *,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    base = finalization_step_base(demo_project, checked_at)
    if base is not None:
        return base

    runtime_path = temporal_secret_remote_path(demo_project)
    action = secret_store.delete(runtime_path)
    return temporal_cloud_step_status(
        checked_at,
        deleted={"secret": action},
    )


def finalization_step_base(
    demo_project: dict[str, Any],
    checked_at: str,
) -> dict[str, Any] | None:
    if temporal_components(demo_project):
        return None
    return {
        "phase": "NotRequired",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "message": "No Temporal Cloud resources to delete.",
    }


def temporal_cloud_runtime_secret(
    demo_project: dict[str, Any],
    secret_store: SecretStore,
) -> dict[str, Any]:
    existing = secret_store.read_json(temporal_secret_remote_path(demo_project))
    return existing if isinstance(existing, dict) else {}


async def resolve_temporal_cloud_namespace(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
) -> str | None:
    existing = temporal_cloud_runtime_secret(demo_project, secret_store)
    qualified = existing.get("namespace")
    if isinstance(qualified, str) and qualified:
        return qualified
    found = await reconciler.find_namespace(desired_namespace_name(demo_project))
    return found.qualified_name if found else None


def temporal_cloud_step_status(
    checked_at: str,
    *,
    namespace: str | None = None,
    deleted: dict[str, str],
) -> dict[str, Any]:
    return {
        "phase": "Deleted",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "namespace": {"name": namespace},
        "deleted": deleted,
    }


# ---------------------------------------------------------------------------
# Real adapters (gRPC + boto3). Imported lazily so the module loads without the
# SDK/boto3 present and so unit tests can use fakes.
# ---------------------------------------------------------------------------


class CloudApiReconciler:
    """Thin async adapter over the Temporal Cloud Operations API."""

    def __init__(
        self,
        cloud_service: Any,
        *,
        account_id: str | None,
        heartbeat: Any = None,
        poll_seconds: float = ASYNC_OPERATION_POLL_SECONDS,
        timeout_seconds: float = ASYNC_OPERATION_TIMEOUT_SECONDS,
    ) -> None:
        self._cs = cloud_service
        self._account_id = account_id
        self._heartbeat = heartbeat
        self._poll_seconds = poll_seconds
        self._timeout_seconds = timeout_seconds

    async def list_regions(self) -> list[str]:
        from temporalio.api.cloud.cloudservice.v1 import GetRegionsRequest

        response = await self._cs.get_regions(GetRegionsRequest())
        return [region.id for region in response.regions]

    async def find_namespace(self, name: str) -> NamespaceInfo | None:
        from temporalio.api.cloud.cloudservice.v1 import GetNamespacesRequest

        page_token = ""
        while True:
            response = await self._cs.get_namespaces(
                GetNamespacesRequest(page_size=100, page_token=page_token)
            )
            for namespace in response.namespaces:
                if namespace.spec.name != name:
                    continue
                info = self._namespace_info(namespace, created=False)
                # Skip a deleted entry whose name has been freed; keep scanning
                # in case an active namespace reuses the same name.
                if info.state in DELETED_NAMESPACE_STATES:
                    continue
                return info
            page_token = response.next_page_token
            if not page_token:
                return None

    async def create_namespace(
        self,
        name: str,
        region: str,
        retention_days: int,
        enable_delete_protection: bool = True,
    ) -> NamespaceInfo:
        from temporalio.api.cloud.cloudservice.v1 import (
            CreateNamespaceRequest,
            GetNamespaceRequest,
        )
        from temporalio.api.cloud.namespace.v1 import (
            ApiKeyAuthSpec,
            CapacitySpec,
            LifecycleSpec,
            NamespaceSpec,
        )

        spec = NamespaceSpec(
            name=name,
            regions=[region],
            retention_days=retention_days,
            api_key_auth=ApiKeyAuthSpec(enabled=True),
            lifecycle=LifecycleSpec(enable_delete_protection=enable_delete_protection),
            # On-demand capacity must be specified explicitly; a namespace created
            # without a capacity spec fails activation ("Error loading capacity
            # information").
            capacity_spec=CapacitySpec(on_demand=CapacitySpec.OnDemand()),
        )
        response = await self._cs.create_namespace(CreateNamespaceRequest(spec=spec))
        await self._await_operation(response.async_operation, label=f"create-namespace:{name}")
        got = await self._cs.get_namespace(
            GetNamespaceRequest(namespace=response.namespace)
        )
        return self._namespace_info(got.namespace, created=True)

    async def find_service_account(self, name: str) -> str | None:
        from temporalio.api.cloud.cloudservice.v1 import GetServiceAccountsRequest

        page_token = ""
        while True:
            response = await self._cs.get_service_accounts(
                GetServiceAccountsRequest(page_size=100, page_token=page_token)
            )
            for service_account in response.service_account:
                if service_account.spec.name == name:
                    return service_account.id
            page_token = response.next_page_token
            if not page_token:
                return None

    async def create_service_account(
        self, name: str, qualified_namespace: str, description: str
    ) -> str:
        from temporalio.api.cloud.cloudservice.v1 import CreateServiceAccountRequest
        from temporalio.api.cloud.identity.v1 import (
            NamespaceAccess,
            NamespaceScopedAccess,
            ServiceAccountSpec,
        )

        spec = ServiceAccountSpec(
            name=name,
            description=description,
            namespace_scoped_access=NamespaceScopedAccess(
                namespace=qualified_namespace,
                access=NamespaceAccess(permission=NamespaceAccess.PERMISSION_WRITE),
            ),
        )
        response = await self._cs.create_service_account(
            CreateServiceAccountRequest(spec=spec)
        )
        await self._await_operation(
            response.async_operation, label=f"create-service-account:{name}"
        )
        return response.service_account_id

    async def create_api_key(
        self, service_account_id: str, display_name: str, description: str, ttl_days: int
    ) -> ApiKeyResult:
        from google.protobuf.timestamp_pb2 import Timestamp

        from temporalio.api.cloud.cloudservice.v1 import CreateApiKeyRequest
        from temporalio.api.cloud.identity.v1 import ApiKeySpec, OwnerType

        expiry = datetime.now(UTC) + timedelta(days=ttl_days)
        expiry_ts = Timestamp()
        expiry_ts.FromDatetime(expiry)
        spec = ApiKeySpec(
            owner_id=service_account_id,
            owner_type=OwnerType.OWNER_TYPE_SERVICE_ACCOUNT,
            display_name=display_name,
            description=description,
            expiry_time=expiry_ts,
            disabled=False,
        )
        response = await self._cs.create_api_key(CreateApiKeyRequest(spec=spec))
        await self._await_operation(
            response.async_operation, label="create-api-key"
        )
        return ApiKeyResult(
            key_id=response.key_id,
            token=response.token,
            expiry_time=expiry.isoformat(),
        )

    async def disable_namespace_delete_protection(self, qualified_name: str) -> str:
        from temporalio.api.cloud.cloudservice.v1 import (
            GetNamespaceRequest,
            UpdateNamespaceRequest,
        )

        try:
            got = await self._cs.get_namespace(
                GetNamespaceRequest(namespace=qualified_name)
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise

        namespace = got.namespace
        if not namespace.spec.lifecycle.enable_delete_protection:
            return "already-disabled"

        spec = namespace.spec
        spec.lifecycle.enable_delete_protection = False
        response = await self._cs.update_namespace(
            UpdateNamespaceRequest(
                namespace=qualified_name,
                spec=spec,
                resource_version=namespace.resource_version,
            )
        )
        await self._await_operation(
            response.async_operation,
            label=f"disable-delete-protection:{qualified_name}",
        )
        return "disabled"

    async def delete_namespace(self, qualified_name: str) -> str:
        from temporalio.api.cloud.cloudservice.v1 import (
            DeleteNamespaceRequest,
            GetNamespaceRequest,
        )

        try:
            got = await self._cs.get_namespace(
                GetNamespaceRequest(namespace=qualified_name)
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise

        namespace = got.namespace

        try:
            response = await self._cs.delete_namespace(
                DeleteNamespaceRequest(
                    namespace=qualified_name,
                    resource_version=namespace.resource_version,
                )
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted/stale version
            if _is_not_found(error):
                return "not-found"
            if not _is_invalid_resource_version(error):
                raise
            got = await self._cs.get_namespace(
                GetNamespaceRequest(namespace=qualified_name)
            )
            namespace = got.namespace
            response = await self._cs.delete_namespace(
                DeleteNamespaceRequest(
                    namespace=qualified_name,
                    resource_version=namespace.resource_version,
                )
            )
        await self._await_operation(
            response.async_operation, label=f"delete-namespace:{qualified_name}"
        )
        return "deleted"

    async def delete_api_key(self, key_id: str) -> str:
        from temporalio.api.cloud.cloudservice.v1 import (
            DeleteApiKeyRequest,
            GetApiKeyRequest,
        )

        try:
            got = await self._cs.get_api_key(GetApiKeyRequest(key_id=key_id))
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise

        try:
            response = await self._cs.delete_api_key(
                DeleteApiKeyRequest(
                    key_id=key_id,
                    resource_version=got.api_key.resource_version,
                )
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise
        await self._await_operation(response.async_operation, label=f"delete-api-key:{key_id}")
        return "deleted"

    async def delete_service_account(self, service_account_id: str) -> str:
        from temporalio.api.cloud.cloudservice.v1 import (
            DeleteServiceAccountRequest,
            GetServiceAccountRequest,
        )

        try:
            got = await self._cs.get_service_account(
                GetServiceAccountRequest(service_account_id=service_account_id)
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise

        try:
            response = await self._cs.delete_service_account(
                DeleteServiceAccountRequest(
                    service_account_id=service_account_id,
                    resource_version=got.service_account.resource_version,
                )
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise
        await self._await_operation(
            response.async_operation, label=f"delete-service-account:{service_account_id}"
        )
        return "deleted"

    async def _await_operation(self, async_operation: Any, *, label: str) -> None:
        from temporalio.api.cloud.cloudservice.v1 import GetAsyncOperationRequest
        from temporalio.api.cloud.operation.v1 import AsyncOperation

        state = async_operation.state
        operation_id = async_operation.id
        if state == AsyncOperation.STATE_FULFILLED or not operation_id:
            return

        loop = asyncio.get_event_loop()
        deadline = loop.time() + self._timeout_seconds
        while True:
            if self._heartbeat is not None:
                self._heartbeat({"operation": label, "operationId": operation_id})
            response = await self._cs.get_async_operation(
                GetAsyncOperationRequest(async_operation_id=operation_id)
            )
            current = response.async_operation
            if current.state == AsyncOperation.STATE_FULFILLED:
                return
            if current.state in (
                AsyncOperation.STATE_FAILED,
                AsyncOperation.STATE_CANCELLED,
                AsyncOperation.STATE_REJECTED,
            ):
                raise RuntimeError(
                    f"Temporal Cloud operation {label} did not succeed: "
                    f"{current.failure_reason or current.state}"
                )
            if loop.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for Temporal Cloud operation {label}"
                )
            await asyncio.sleep(self._poll_seconds)

    def _namespace_info(self, namespace: Any, *, created: bool) -> NamespaceInfo:
        from temporalio.api.cloud.resource.v1 import ResourceState

        endpoints = namespace.endpoints
        state = ResourceState.Name(namespace.state) if namespace.state else None
        # Runtime workers must connect to the per-namespace gRPC endpoint
        # (`<namespace>.tmprl.cloud:7233`), not the shared regional endpoint
        # reported by `endpoints.grpc_address`. The per-namespace endpoint
        # authorizes namespace-scoped API keys directly and immediately.
        return NamespaceInfo(
            qualified_name=namespace.namespace,
            grpc_address=f"{namespace.namespace}.tmprl.cloud:7233",
            web_address=endpoints.web_address or None,
            state=state,
            created=created,
        )


class SecretsManagerStore:
    """Adapter writing/reading JSON secrets in AWS Secrets Manager."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def read_json(self, path: str) -> dict[str, Any] | None:
        try:
            response = self._client.get_secret_value(SecretId=path)
        except Exception as error:  # noqa: BLE001 - normalize boto error code
            if _error_code(error) == "ResourceNotFoundException":
                return None
            raise
        secret_string = response.get("SecretString")
        if not isinstance(secret_string, str):
            return None
        try:
            decoded = json.loads(secret_string)
        except json.JSONDecodeError:
            return None
        return decoded if isinstance(decoded, dict) else None

    def write_json(
        self, path: str, value: dict[str, Any], description: str | None = None
    ) -> None:
        body = json.dumps(value)
        try:
            self._client.put_secret_value(SecretId=path, SecretString=body)
        except Exception as error:  # noqa: BLE001 - create on first write
            if _error_code(error) != "ResourceNotFoundException":
                raise
            kwargs: dict[str, Any] = {"Name": path, "SecretString": body}
            if description:
                kwargs["Description"] = description
            kwargs["Tags"] = [{"Key": "managed-by", "Value": "demo-registry"}]
            self._client.create_secret(**kwargs)

    def delete(self, path: str) -> str:
        # Schedule deletion with a recovery window rather than force-deleting, so
        # a credential torn down by mistake can still be restored.
        try:
            self._client.delete_secret(SecretId=path, RecoveryWindowInDays=7)
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _error_code(error) == "ResourceNotFoundException":
                return "not-found"
            raise
        return "scheduled-deletion"


async def run_cloud_api_reconcile(
    demo_project: dict[str, Any],
    *,
    bootstrap: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    target_host: str = DEFAULT_OPS_TARGET_HOST,
    api_version: str | None = None,
    heartbeat: Any = None,
    enable_delete_protection: bool = True,
) -> dict[str, Any]:
    """Connect the Cloud Operations client and run the reconcile orchestration."""

    from temporalio.client import CloudOperationsClient

    api_key = bootstrap.get("TEMPORAL_CLOUD_API_KEY")
    account_id = bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID")
    region = bootstrap.get("TEMPORAL_CLOUD_REGION") or DEFAULT_TEMPORAL_CLOUD_REGION
    if not api_key:
        raise ValueError(
            "Temporal Cloud bootstrap secret must include TEMPORAL_CLOUD_API_KEY."
        )

    connect_kwargs: dict[str, Any] = {"api_key": api_key, "target_host": target_host}
    if api_version:
        connect_kwargs["version"] = api_version
    client = await CloudOperationsClient.connect(**connect_kwargs)

    reconciler = CloudApiReconciler(
        client.cloud_service,
        account_id=account_id,
        heartbeat=heartbeat,
    )
    return await reconcile_temporal_cloud(
        demo_project,
        reconciler=reconciler,
        secret_store=secret_store,
        account_id=account_id,
        region=region,
        checked_at=checked_at,
        enable_delete_protection=enable_delete_protection,
    )


async def run_cloud_api_finalize(
    demo_project: dict[str, Any],
    *,
    bootstrap: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    target_host: str = DEFAULT_OPS_TARGET_HOST,
    api_version: str | None = None,
    heartbeat: Any = None,
) -> dict[str, Any]:
    """Connect the Cloud Operations client and tear down Temporal Cloud resources."""

    from temporalio.client import CloudOperationsClient

    api_key = bootstrap.get("TEMPORAL_CLOUD_API_KEY")
    if not api_key:
        raise ValueError(
            "Temporal Cloud bootstrap secret must include TEMPORAL_CLOUD_API_KEY."
        )

    connect_kwargs: dict[str, Any] = {"api_key": api_key, "target_host": target_host}
    if api_version:
        connect_kwargs["version"] = api_version
    client = await CloudOperationsClient.connect(**connect_kwargs)

    reconciler = CloudApiReconciler(
        client.cloud_service,
        account_id=bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID"),
        heartbeat=heartbeat,
    )
    return await finalize_temporal_cloud(
        demo_project,
        reconciler=reconciler,
        secret_store=secret_store,
        checked_at=checked_at,
    )


async def run_cloud_api_delete_api_key(
    demo_project: dict[str, Any],
    *,
    bootstrap: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    target_host: str = DEFAULT_OPS_TARGET_HOST,
    api_version: str | None = None,
    heartbeat: Any = None,
) -> dict[str, Any]:
    return await run_cloud_api_finalization_step(
        demo_project,
        bootstrap=bootstrap,
        secret_store=secret_store,
        checked_at=checked_at,
        target_host=target_host,
        api_version=api_version,
        heartbeat=heartbeat,
        step=delete_temporal_cloud_api_key,
    )


async def run_cloud_api_delete_service_account(
    demo_project: dict[str, Any],
    *,
    bootstrap: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    target_host: str = DEFAULT_OPS_TARGET_HOST,
    api_version: str | None = None,
    heartbeat: Any = None,
) -> dict[str, Any]:
    return await run_cloud_api_finalization_step(
        demo_project,
        bootstrap=bootstrap,
        secret_store=secret_store,
        checked_at=checked_at,
        target_host=target_host,
        api_version=api_version,
        heartbeat=heartbeat,
        step=delete_temporal_cloud_service_account,
    )


async def run_cloud_api_disable_namespace_delete_protection(
    demo_project: dict[str, Any],
    *,
    bootstrap: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    target_host: str = DEFAULT_OPS_TARGET_HOST,
    api_version: str | None = None,
    heartbeat: Any = None,
) -> dict[str, Any]:
    return await run_cloud_api_finalization_step(
        demo_project,
        bootstrap=bootstrap,
        secret_store=secret_store,
        checked_at=checked_at,
        target_host=target_host,
        api_version=api_version,
        heartbeat=heartbeat,
        step=disable_temporal_cloud_namespace_delete_protection,
    )


async def run_cloud_api_delete_namespace(
    demo_project: dict[str, Any],
    *,
    bootstrap: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    target_host: str = DEFAULT_OPS_TARGET_HOST,
    api_version: str | None = None,
    heartbeat: Any = None,
) -> dict[str, Any]:
    return await run_cloud_api_finalization_step(
        demo_project,
        bootstrap=bootstrap,
        secret_store=secret_store,
        checked_at=checked_at,
        target_host=target_host,
        api_version=api_version,
        heartbeat=heartbeat,
        step=delete_temporal_cloud_namespace,
    )


async def run_cloud_api_finalization_step(
    demo_project: dict[str, Any],
    *,
    bootstrap: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    target_host: str,
    api_version: str | None,
    heartbeat: Any,
    step: Any,
) -> dict[str, Any]:
    from temporalio.client import CloudOperationsClient

    api_key = bootstrap.get("TEMPORAL_CLOUD_API_KEY")
    if not api_key:
        raise ValueError(
            "Temporal Cloud bootstrap secret must include TEMPORAL_CLOUD_API_KEY."
        )

    connect_kwargs: dict[str, Any] = {"api_key": api_key, "target_host": target_host}
    if api_version:
        connect_kwargs["version"] = api_version
    client = await CloudOperationsClient.connect(**connect_kwargs)

    reconciler = CloudApiReconciler(
        client.cloud_service,
        account_id=bootstrap.get("TEMPORAL_CLOUD_ACCOUNT_ID"),
        heartbeat=heartbeat,
    )
    return await step(
        demo_project,
        reconciler=reconciler,
        secret_store=secret_store,
        checked_at=checked_at,
    )


def _is_not_found(error: Exception) -> bool:
    try:
        from temporalio.service import RPCError, RPCStatusCode
    except ImportError:
        return False
    return isinstance(error, RPCError) and error.status == RPCStatusCode.NOT_FOUND


def _is_invalid_resource_version(error: Exception) -> bool:
    return "invalid resource version" in str(error).lower()


def _error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        info = response.get("Error")
        if isinstance(info, dict) and isinstance(info.get("Code"), str):
            return info["Code"]
    return None
