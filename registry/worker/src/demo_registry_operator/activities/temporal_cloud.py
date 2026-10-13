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
import logging
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from demo_registry_operator import derive, resources
from demo_registry_operator.activities import temporal_auth


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
DEFAULT_TEMPORAL_CLOUD_REGION = "aws-ca-central-1"
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
TEMPORAL_RUNTIME_CANDIDATE_SECRET_SUFFIX = "candidate"
TEMPORAL_MTLS_CA_CANDIDATE_SECRET_SUFFIX = "ca-candidate"

ApiKeyResult = temporal_auth.ApiKeyResult

logger = logging.getLogger(__name__)

# The readers group holds exactly this on every managed namespace: never more,
# so membership can't leak write or admin access into demo namespaces.
READERS_GROUP_PERMISSION = "read"
# Every demo reconcile updates the same group (one namespace each), so group
# updates race on its resource_version. Re-read and retry with jitter.
USER_GROUP_UPDATE_ATTEMPTS = 8
USER_GROUP_RETRY_DELAY_SECONDS = 0.25


@dataclass
class NamespaceInfo:
    qualified_name: str
    grpc_address: str
    web_address: str | None = None
    state: str | None = None
    auth_mode: str | None = None
    # The region the namespace is actually provisioned in (from the Cloud API
    # namespace spec). None when unknown; region drift is only acted on when the
    # current region is known, so an unknown region never triggers a teardown.
    region: str | None = None
    created: bool = False


@dataclass
class NexusEndpointInfo:
    endpoint_id: str
    name: str
    # Qualified "<namespace>.<account-id>" ids, which is what the Cloud API stores
    # and compares against -- not the short namespace names a project declares.
    target_namespace_id: str
    target_task_queue: str
    allowed_caller_namespace_ids: tuple[str, ...] = ()
    resource_version: str | None = None
    state: str | None = None
    created: bool = False

    def matches(
        self,
        *,
        target_namespace_id: str,
        target_task_queue: str,
        allowed_caller_namespace_ids: tuple[str, ...],
    ) -> bool:
        return (
            self.target_namespace_id == target_namespace_id
            and self.target_task_queue == target_task_queue
            and set(self.allowed_caller_namespace_ids) == set(allowed_caller_namespace_ids)
        )


@dataclass
class UserGroupInfo:
    group_id: str
    display_name: str
    resource_version: str
    # Qualified namespace name -> "read" | "write" | "admin".
    namespace_permissions: dict[str, str] = field(default_factory=dict)


class NamespaceReadersReconciler(Protocol):
    """The Cloud Ops calls behind the namespace readers group."""

    async def find_user_group(self, display_name: str) -> UserGroupInfo | None: ...

    async def set_user_group_namespace_access(
        self,
        group: UserGroupInfo,
        qualified_namespace: str,
        permission: str | None,
    ) -> None: ...

    async def find_user_by_email(self, email: str) -> str | None: ...

    async def user_group_has_member(self, group_id: str, user_id: str) -> bool: ...

    async def add_user_group_member(self, group_id: str, user_id: str) -> str: ...

    async def list_tagged_namespaces(self, key: str, value: str) -> list[str]: ...


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
        auth_mode: str = resources.TEMPORAL_AUTH_MODE_API_KEY,
        accepted_client_ca: str | None = None,
        certificate_common_name: str | None = None,
        enable_delete_protection: bool = True,
    ) -> NamespaceInfo: ...

    async def configure_namespace_mtls(
        self,
        qualified_name: str,
        accepted_client_ca: str,
        certificate_common_name: str,
    ) -> str: ...

    async def create_or_verify_search_attribute(
        self,
        qualified_name: str,
        name: str,
        attribute_type: str,
    ) -> dict[str, str | None]: ...

    async def delete_search_attribute(
        self,
        qualified_name: str,
        name: str,
    ) -> dict[str, str | None]: ...

    async def update_namespace_tags(
        self,
        qualified_name: str,
        tags: dict[str, str],
    ) -> dict[str, Any]: ...

    async def find_service_account(self, name: str) -> str | None: ...

    async def create_service_account(
        self, name: str, qualified_namespace: str, description: str
    ) -> str: ...

    async def create_api_key(
        self, service_account_id: str, display_name: str, description: str, ttl_days: int
    ) -> temporal_auth.ApiKeyResult: ...

    async def find_nexus_endpoint(self, name: str) -> NexusEndpointInfo | None: ...

    async def create_nexus_endpoint(
        self,
        name: str,
        target_namespace_id: str,
        target_task_queue: str,
        allowed_caller_namespace_ids: tuple[str, ...],
    ) -> NexusEndpointInfo: ...

    async def update_nexus_endpoint(
        self,
        endpoint_id: str,
        name: str,
        target_namespace_id: str,
        target_task_queue: str,
        allowed_caller_namespace_ids: tuple[str, ...],
    ) -> NexusEndpointInfo: ...

    async def delete_nexus_endpoint(self, endpoint_id: str) -> str: ...


class SecretStore(Protocol):
    def read_json(self, path: str) -> dict[str, Any] | None: ...

    def write_json(
        self, path: str, value: dict[str, Any], description: str | None = None
    ) -> None: ...

    def delete(self, path: str) -> str: ...


def desired_namespace_name(demo_project: dict[str, Any]) -> str:
    return resources.project_temporal_namespace(demo_project)


def desired_service_account_name(demo_project: dict[str, Any]) -> str:
    """Keep the primary name stable and isolate additional namespaces.

    Temporal Cloud service accounts are account-scoped, while the role created
    for each account is scoped to one namespace. Reusing ``<project>-runtime``
    for an additional namespace would therefore adopt the primary namespace's
    account instead of creating one with access to the additional namespace.
    """
    project = resources.project_name(demo_project)
    namespace = resources.project_temporal_namespace(demo_project)
    if namespace == derive.temporal_namespace(project):
        return f"{project}-runtime"
    return f"{namespace}-runtime"


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
    force_auth_rotation: bool = False,
    runtime_canary: Any = None,
    previous_managed_search_attributes: dict[str, str] | None = None,
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

    if not resources.temporal_namespace_required(demo_project):
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "message": "No Temporal Cloud namespace resources are required.",
        }

    auth_mode = resources.temporal_auth_mode(demo_project)
    short_name = desired_namespace_name(demo_project)
    mtls_ca_material: dict[str, Any] | None = None
    mtls_ca_rotation: dict[str, Any] | None = None
    mtls_common_name: str | None = None
    if auth_mode == resources.TEMPORAL_AUTH_MODE_MTLS:
        mtls_ca_material, mtls_ca_rotation = temporal_auth.prepare_mtls_ca_material(
            demo_project=demo_project,
            secret_store=secret_store,
            checked_at=checked_at,
        )
        mtls_common_name = temporal_auth.temporal_mtls_client_common_name(demo_project)

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
    if namespace is not None and namespace.auth_mode and namespace.auth_mode != auth_mode:
        return {
            "phase": "Error",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "reason": "TemporalAuthModeSwitchRequiresFinalize",
            "message": (
                "Temporal Cloud namespaces cannot support API-key and mTLS auth "
                "at the same time. Finalize this project to delete the existing "
                "namespace, then deploy it again with the new auth mode."
            ),
            "auth": resources.temporal_auth_status(demo_project),
            "namespace": {
                "name": namespace.qualified_name,
                "state": namespace.state,
                "authMode": namespace.auth_mode,
            },
            **resources.temporal_auth_runtime_status(demo_project),
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
            auth_mode=auth_mode,
            accepted_client_ca=(
                temporal_auth.accepted_client_ca_bundle(mtls_ca_material)
                if isinstance(mtls_ca_material, dict)
                else None
            ),
            certificate_common_name=mtls_common_name,
            enable_delete_protection=enable_delete_protection,
        )
        namespace_created = True

    search_attributes_status = await reconcile_temporal_cloud_search_attributes(
        demo_project,
        namespace=namespace,
        reconciler=reconciler,
        checked_at=checked_at,
        previous_managed=previous_managed_search_attributes or {},
    )
    if search_attributes_status.get("phase") == "Error":
        return {
            "phase": "Error",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "accountId": account_id,
            "region": region,
            "auth": resources.temporal_auth_status(demo_project),
            "namespace": namespace_status(namespace, namespace_created),
            "searchAttributes": search_attributes_status,
            "reason": search_attributes_status.get("reason"),
            "message": search_attributes_status.get("message"),
        }

    auth_status: dict[str, Any] = {}
    if not resources.temporal_components(demo_project):
        auth_status = {}
    elif auth_mode == resources.TEMPORAL_AUTH_MODE_MTLS:
        auth_status = await temporal_auth.reconcile_mtls_auth(
            demo_project,
            namespace=namespace,
            reconciler=reconciler,
            secret_store=secret_store,
            checked_at=checked_at,
            mirror_kubernetes_secrets=mirror_kubernetes_secrets,
            force_client_rotation=force_auth_rotation,
            runtime_canary=runtime_canary,
            ca_material=mtls_ca_material,
            ca_rotation=mtls_ca_rotation,
        )
    else:
        auth_status = await temporal_auth.reconcile_api_key_auth(
            demo_project,
            namespace=namespace,
            reconciler=reconciler,
            secret_store=secret_store,
            checked_at=checked_at,
            api_key_ttl_days=api_key_ttl_days,
            rotate_before_days=rotate_before_days,
            mirror_kubernetes_secrets=mirror_kubernetes_secrets,
            force_rotation=force_auth_rotation,
            runtime_canary=runtime_canary,
        )

    status: dict[str, Any] = {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "accountId": account_id,
        "region": region,
        "auth": resources.temporal_auth_status(demo_project),
        "namespace": {
            "name": namespace.qualified_name,
            "action": "created" if namespace_created else "verified",
            "deletionPolicy": "retain-delete-protected",
            "grpcAddress": namespace.grpc_address,
            "webAddress": namespace.web_address,
            "state": namespace.state,
            "authMode": auth_mode,
            "region": namespace.region,
        },
        "searchAttributes": search_attributes_status,
    }
    status.update(auth_status)
    return status


NEXUS_PRIMARY_NAMESPACE_REF = "primary"


class CallerNamespaceNotReady(Exception):
    """The other demo's primary namespace is not active yet.

    The message contains the transient marker the activity wrapper already
    retries, so the target demo waits without failing the rollout.
    """

    def __init__(self, project: str, namespace: str) -> None:
        super().__init__(
            f"caller project {project} namespace is not ready ({namespace})"
        )


def caller_namespace_ready(info: NamespaceInfo | None) -> bool:
    if info is None or not info.state:
        return False
    return info.state.strip() in {"RESOURCE_STATE_ACTIVE", "active"}


def qualified_temporal_namespace(name: str, account_id: str | None) -> str:
    return f"{name}.{account_id}" if account_id else name


def nexus_namespace_id(
    demo_project: dict[str, Any],
    reference: str,
    account_id: str | None,
) -> str:
    """Resolve a declared namespace reference to the Cloud API's namespace_id.

    ``reference`` is a label from spec.temporal.namespaces, or "primary" for the
    project's original namespace. The Cloud API keys off the qualified
    ``<namespace>.<account-id>`` form, which is the same value NamespaceInfo
    carries as ``qualified_name``.
    """
    label = None if reference == NEXUS_PRIMARY_NAMESPACE_REF else reference
    namespace = resources.project_temporal_namespace_for_label(demo_project, label)
    return f"{namespace}.{account_id}" if account_id else namespace


async def _allow_caller_projects(
    endpoint: Any,
    *,
    reconciler: TemporalCloudReconciler,
    account_id: str | None,
    live_project_names: set[str] | None,
    allowed_ids: list[str],
) -> list[str]:
    """Append each other demo's primary namespace id to ``allowed_ids``.

    Returns the project names dropped because their DemoProject CR is gone.
    """
    omitted: list[str] = []
    for project_name in getattr(endpoint, "allowed_caller_projects", ()):
        if live_project_names is not None and project_name not in live_project_names:
            omitted.append(project_name)
            continue
        namespace = derive.temporal_namespace(project_name)
        info = await reconciler.find_namespace(namespace)
        if not caller_namespace_ready(info):
            raise CallerNamespaceNotReady(project_name, namespace)
        allowed_ids.append(qualified_temporal_namespace(namespace, account_id))
    return omitted


async def reconcile_nexus_endpoints(
    demo_project: dict[str, Any],
    *,
    endpoints: tuple[Any, ...],
    reconciler: TemporalCloudReconciler,
    checked_at: str,
    account_id: str | None,
    live_project_names: set[str] | None = None,
) -> dict[str, Any]:
    """Create-or-verify every declared endpoint, correcting drift in place.

    Endpoints are looked up by name first and only created when absent. An
    existing endpoint whose target or caller allowlist has drifted is updated
    rather than recreated, because the endpoint id is what callers resolve.

    ``live_project_names`` is the set of DemoProject names currently in the
    cluster. A caller whose CR is gone is dropped from the allowlist. A caller
    whose CR exists while its primary namespace is not active yet raises
    CallerNamespaceNotReady so the activity retries. None means every declared
    caller project is treated as present, which is the unit-test default.
    """
    statuses: list[dict[str, Any]] = []
    endpoint_ids: dict[str, str] = {}

    for endpoint in endpoints:
        target_namespace_id = nexus_namespace_id(
            demo_project, endpoint.target_namespace, account_id
        )
        target_task_queue = endpoint.target_task_queue
        allowed_ids = [
            nexus_namespace_id(demo_project, reference, account_id)
            for reference in endpoint.allowed_caller_namespaces
        ]
        omitted = await _allow_caller_projects(
            endpoint,
            reconciler=reconciler,
            account_id=account_id,
            live_project_names=live_project_names,
            allowed_ids=allowed_ids,
        )
        allowed_id_tuple = tuple(allowed_ids)

        existing = await reconciler.find_nexus_endpoint(endpoint.name)
        if existing is None:
            info = await reconciler.create_nexus_endpoint(
                endpoint.name,
                target_namespace_id,
                target_task_queue,
                allowed_id_tuple,
            )
            action = "created"
        elif existing.matches(
            target_namespace_id=target_namespace_id,
            target_task_queue=target_task_queue,
            allowed_caller_namespace_ids=allowed_id_tuple,
        ):
            info = existing
            action = "verified"
        else:
            info = await reconciler.update_nexus_endpoint(
                existing.endpoint_id,
                endpoint.name,
                target_namespace_id,
                target_task_queue,
                allowed_id_tuple,
            )
            action = "updated"

        endpoint_ids[endpoint.name] = info.endpoint_id
        entry: dict[str, Any] = {
            "name": endpoint.name,
            "action": action,
            "endpointId": info.endpoint_id,
            "targetNamespace": target_namespace_id,
            "targetTaskQueue": target_task_queue,
            "allowedCallerNamespaces": list(allowed_id_tuple),
        }
        if omitted:
            entry["omittedCallerProjects"] = omitted
        statuses.append(entry)

    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "endpoints": statuses,
        "endpointIds": endpoint_ids,
    }


async def delete_nexus_endpoints(
    demo_project: dict[str, Any],
    *,
    endpoints: tuple[Any, ...],
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    """Delete this project's endpoints, tolerating ones already gone.

    Runs before any namespace teardown: an endpoint outlives the namespace it
    targets, and endpoint names are account-wide, so a leaked one blocks the name
    for every other project.
    """
    statuses: list[dict[str, Any]] = []
    for endpoint in endpoints:
        existing = await reconciler.find_nexus_endpoint(endpoint.name)
        if existing is None:
            statuses.append({"name": endpoint.name, "action": "not-found"})
            continue
        action = await reconciler.delete_nexus_endpoint(existing.endpoint_id)
        statuses.append(
            {
                "name": endpoint.name,
                "action": action,
                "endpointId": existing.endpoint_id,
            }
        )
    return {
        "phase": "Deleted",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "endpoints": statuses,
    }


async def reconcile_temporal_cloud_search_attributes(
    demo_project: dict[str, Any],
    *,
    namespace: NamespaceInfo,
    reconciler: TemporalCloudReconciler,
    checked_at: str,
    previous_managed: dict[str, str],
) -> dict[str, Any]:
    desired = resources.temporal_search_attributes(demo_project)
    delete_names = sorted(set(previous_managed) - set(desired))
    operations = [
        ensure_temporal_cloud_search_attribute(
            demo_project,
            namespace_status_value={"namespace": namespace_status(namespace, False)},
            search_attribute={"name": name, "type": attribute_type},
            reconciler=reconciler,
            checked_at=checked_at,
        )
        for name, attribute_type in desired.items()
    ]
    operations.extend(
        delete_temporal_cloud_search_attribute_value(
            demo_project,
            namespace_status_value={"namespace": namespace_status(namespace, False)},
            search_attribute={"name": name, "type": previous_managed[name]},
            reconciler=reconciler,
            checked_at=checked_at,
        )
        for name in delete_names
    )
    statuses = list(await asyncio.gather(*operations)) if operations else []
    return temporal_cloud_search_attributes_status(
        checked_at,
        namespace=namespace.qualified_name,
        statuses=statuses,
        previous_managed=previous_managed,
    )


def temporal_cloud_search_attributes_status(
    checked_at: str,
    *,
    namespace: str | None,
    statuses: list[dict[str, Any]],
    previous_managed: dict[str, str],
) -> dict[str, Any]:
    attributes: list[dict[str, Any]] = []
    managed_names: set[str] = set()
    deleted_names: list[str] = []
    phases: set[str] = set()
    reason: str | None = None
    message: str | None = None

    for status in statuses:
        phase = status.get("phase")
        if isinstance(phase, str):
            phases.add(phase)
        source = status.get("searchAttribute")
        if not isinstance(source, dict):
            continue
        name = source.get("name")
        if not isinstance(name, str) or not name:
            continue
        action = source.get("action")
        entry = dict(source)
        managed = (
            action == "created"
            or (
                name in previous_managed
                and action in {"verified", "created", "drifted"}
            )
        )
        entry["managed"] = managed
        attributes.append(entry)
        if action == "deleted":
            deleted_names.append(name)
        elif managed:
            managed_names.add(name)
        if status.get("reason") and reason is None:
            reason = str(status["reason"])
        if status.get("message") and message is None:
            message = str(status["message"])

    if "Error" in phases:
        phase = "Error"
    elif "ManualSetupRequired" in phases:
        phase = "ManualSetupRequired"
    elif statuses:
        phase = "Ready"
    else:
        phase = "NotRequired"

    rendered: dict[str, Any] = {
        "phase": phase,
        "checkedAt": checked_at,
        "namespace": namespace,
        "ownershipPolicy": "delete-only-previously-managed",
        "attributes": sorted(attributes, key=lambda item: str(item.get("name", ""))),
        "managedNames": sorted(managed_names),
    }
    if deleted_names:
        rendered["deletedNames"] = sorted(deleted_names)
    if reason:
        rendered["reason"] = reason
    if message:
        rendered["message"] = message
    return rendered


def temporal_runtime_candidate_secret_path(
    demo_project: dict[str, Any],
    candidate_secret_token: str | None = None,
) -> str:
    suffix = TEMPORAL_RUNTIME_CANDIDATE_SECRET_SUFFIX
    token = normalized_candidate_secret_token(candidate_secret_token)
    if token:
        suffix = f"{suffix}-{token}"
    return (
        f"{resources.temporal_secret_remote_path(demo_project)}/"
        f"{suffix}"
    )


def temporal_mtls_ca_candidate_secret_path(
    demo_project: dict[str, Any],
    candidate_secret_token: str | None = None,
) -> str:
    suffix = TEMPORAL_MTLS_CA_CANDIDATE_SECRET_SUFFIX
    token = normalized_candidate_secret_token(candidate_secret_token)
    if token:
        suffix = f"{suffix}-{token}"
    return (
        f"{resources.temporal_secret_remote_path(demo_project)}/"
        f"{suffix}"
    )


def normalized_candidate_secret_token(value: str | None) -> str | None:
    if value is None:
        return None
    token = value.strip().lower()
    if not token:
        return None
    if len(token) < 8 or len(token) > 64:
        raise ValueError("Temporal candidate secret token must be 8-64 hex characters.")
    if any(char not in "0123456789abcdef" for char in token):
        raise ValueError("Temporal candidate secret token must be hex.")
    return token


async def ensure_temporal_cloud_namespace(
    demo_project: dict[str, Any],
    *,
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    account_id: str | None,
    region: str,
    checked_at: str,
    retention_days: int = DEFAULT_NAMESPACE_RETENTION_DAYS,
    enable_delete_protection: bool = True,
    candidate_ca_secret_path: str | None = None,
    candidate_secret_token: str | None = None,
) -> dict[str, Any]:
    if not resources.temporal_namespace_required(demo_project):
        return temporal_cloud_not_required_status(checked_at)

    auth_mode = resources.temporal_auth_mode(demo_project)
    short_name = desired_namespace_name(demo_project)
    namespace = await reconciler.find_namespace(short_name)
    if namespace is not None and namespace.state in DELETED_NAMESPACE_STATES:
        namespace = None
    if namespace is not None and namespace.state in UNUSABLE_NAMESPACE_STATES:
        return {
            **temporal_cloud_base_status(
                demo_project,
                checked_at=checked_at,
                account_id=account_id,
                region=region,
                phase="Error",
            ),
            "reason": "NamespaceUnusable",
            "message": (
                f"Temporal Cloud namespace {namespace.qualified_name!r} exists in "
                f"state {namespace.state}; not reusing or recreating."
            ),
            "namespace": {
                "name": namespace.qualified_name,
                "state": namespace.state,
            },
        }
    if namespace is not None and namespace.auth_mode and namespace.auth_mode != auth_mode:
        return {
            **temporal_cloud_base_status(
                demo_project,
                checked_at=checked_at,
                account_id=account_id,
                region=region,
                phase="Error",
            ),
            "reason": "TemporalAuthModeSwitchRequiresFinalize",
            "message": (
                "Temporal Cloud namespaces cannot support API-key and mTLS auth "
                "at the same time. Finalize this project to delete the existing "
                "namespace, then deploy it again with the new auth mode."
            ),
            "namespace": {
                "name": namespace.qualified_name,
                "state": namespace.state,
                "authMode": namespace.auth_mode,
            },
            **resources.temporal_auth_runtime_status(demo_project),
        }
    # Region is immutable on a Temporal Cloud namespace, so a region change
    # requires a destructive teardown + recreate (driven by the workflow). Only
    # signal drift when the current region is known and differs; never act on an
    # unknown (None) region.
    if namespace is not None and namespace.region and namespace.region != region:
        return {
            **temporal_cloud_base_status(
                demo_project,
                checked_at=checked_at,
                account_id=account_id,
                region=region,
                phase="RegionDrift",
            ),
            "reason": "RegionDrift",
            "message": (
                f"Temporal Cloud namespace {namespace.qualified_name!r} is in "
                f"region {namespace.region!r}, but the desired region is "
                f"{region!r}; it must be torn down and recreated."
            ),
            "namespace": namespace_status(namespace, False),
            "currentRegion": namespace.region,
            "desiredRegion": region,
        }

    namespace_created = False
    if namespace is None:
        available_regions = await reconciler.list_regions()
        if region not in available_regions:
            return {
                **temporal_cloud_base_status(
                    demo_project,
                    checked_at=checked_at,
                    account_id=account_id,
                    region=region,
                    phase="Error",
                ),
                "reason": "RegionUnavailable",
                "message": (
                    f"Temporal Cloud region {region!r} is not available to this "
                    "account; choose one of the available regions."
                ),
                "availableRegions": sorted(available_regions),
            }
        accepted_client_ca = None
        certificate_common_name = None
        if auth_mode == resources.TEMPORAL_AUTH_MODE_MTLS:
            ca_candidate_path = candidate_ca_secret_path
            if not ca_candidate_path:
                ca_candidate_path = temporal_mtls_ca_candidate_secret_path(
                    demo_project,
                    candidate_secret_token,
                )
            ca_material = read_mtls_ca_material_for_reconcile(
                demo_project,
                secret_store,
                allow_candidate=True,
                candidate_ca_secret_path=ca_candidate_path,
            )
            accepted_client_ca = temporal_auth.accepted_client_ca_bundle(ca_material)
            certificate_common_name = temporal_auth.temporal_mtls_client_common_name(demo_project)
        namespace = await reconciler.create_namespace(
            short_name,
            region,
            retention_days,
            auth_mode=auth_mode,
            accepted_client_ca=accepted_client_ca,
            certificate_common_name=certificate_common_name,
            enable_delete_protection=enable_delete_protection,
        )
        namespace_created = True

    return {
        **temporal_cloud_base_status(
            demo_project,
            checked_at=checked_at,
            account_id=account_id,
            region=region,
            phase="Ready",
        ),
        "namespace": namespace_status(namespace, namespace_created),
    }


def stage_temporal_mtls_ca(
    demo_project: dict[str, Any],
    *,
    secret_store: SecretStore,
    checked_at: str,
    candidate_secret_token: str | None = None,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_MTLS:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)

    ca_path = resources.temporal_mtls_ca_secret_remote_path(demo_project)
    candidate_path = temporal_mtls_ca_candidate_secret_path(
        demo_project,
        candidate_secret_token,
    )
    token = normalized_candidate_secret_token(candidate_secret_token)
    existing = secret_store.read_json(ca_path)
    rotation = temporal_auth.temporal_mtls_ca_rotation_decision(
        existing,
        checked_at=checked_at,
        rotate_every_days=resources.TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
    )
    promote_required = False
    if isinstance(existing, dict) and existing.get("caCert") and not rotation["rotate"]:
        cleanup = temporal_auth.temporal_mtls_ca_cleanup_decision(existing, checked_at=checked_at)
        if cleanup["cleanup"]:
            material = {
                key: value
                for key, value in existing.items()
                if key
                not in {
                    "previousCaCert",
                    "previousCaSerialNumber",
                    "previousCaRemoveAt",
                }
            }
            if token:
                material["candidateSecretToken"] = token
            rotation = cleanup
            action = "cleaned-up"
            promote_required = True
            secret_store.write_json(
                candidate_path,
                material,
                f"Temporal Cloud mTLS candidate CA for {resources.project_name(demo_project)}",
            )
        else:
            material = existing
            action = "verified"
    else:
        material = temporal_auth.generate_mtls_ca_certificate(checked_at=checked_at)
        if isinstance(existing, dict) and existing.get("caCert"):
            checked = temporal_auth.parse_datetime(checked_at) or datetime.now(UTC)
            material.update(
                {
                    "previousCaCert": existing["caCert"],
                    "previousCaSerialNumber": existing.get("serialNumber"),
                    "previousCaRemoveAt": (
                        checked + timedelta(days=temporal_auth.MTLS_CA_OVERLAP_DAYS)
                    ).isoformat(),
                }
            )
            action = "rotated"
        else:
            action = "created"
        if token:
            material["candidateSecretToken"] = token
        promote_required = True
        secret_store.write_json(
            candidate_path,
            material,
            f"Temporal Cloud mTLS candidate CA for {resources.project_name(demo_project)}",
        )

    ca_status = mtls_ca_certificate_status(
        demo_project,
        material,
        rotation,
        action=action,
        promote_required=promote_required,
        candidate_path=candidate_path,
    )
    status = {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "authMode": resources.TEMPORAL_AUTH_MODE_MTLS,
        "promoteRequired": promote_required,
        "mtls": {
            **resources.temporal_mtls_status(demo_project),
            "action": "created" if action == "created" else action,
            "caCertificate": ca_status,
        },
    }
    if token and promote_required:
        status["candidateSecretToken"] = token
    return status


async def configure_temporal_cloud_namespace_mtls(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
    candidate_ca_secret_path: str | None = None,
    candidate_secret_token: str | None = None,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_MTLS:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    ca_candidate_path = candidate_ca_secret_path
    if not ca_candidate_path:
        ca_candidate_path = temporal_mtls_ca_candidate_secret_path(
            demo_project,
            candidate_secret_token,
        )
    ca_material = read_mtls_ca_material_for_reconcile(
        demo_project,
        secret_store,
        allow_candidate=True,
        candidate_ca_secret_path=ca_candidate_path,
    )
    common_name = temporal_auth.temporal_mtls_client_common_name(demo_project)
    action = await reconciler.configure_namespace_mtls(
        namespace.qualified_name,
        temporal_auth.accepted_client_ca_bundle(ca_material),
        common_name,
    )
    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "authMode": resources.TEMPORAL_AUTH_MODE_MTLS,
        "namespace": {
            "name": namespace.qualified_name,
            "grpcAddress": namespace.grpc_address,
            "authMode": resources.TEMPORAL_AUTH_MODE_MTLS,
        },
        "mtls": {
            "namespace": namespace.qualified_name,
            "namespaceConfigAction": action,
            "clientCertificate": {
                "commonName": common_name,
            },
        },
    }


async def ensure_temporal_cloud_service_account(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_API_KEY:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    project = resources.project_name(demo_project)
    name = desired_service_account_name(demo_project)
    service_account_id = await reconciler.find_service_account(name)
    created = False
    if service_account_id is None:
        service_account_id = await reconciler.create_service_account(
            name,
            namespace.qualified_name,
            f"tmprl-demo.cloud runtime service account for {project}",
        )
        created = True
    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "authMode": resources.TEMPORAL_AUTH_MODE_API_KEY,
        "serviceAccount": {
            "id": service_account_id,
            "name": name,
            "action": "created" if created else "verified",
        },
    }


async def ensure_temporal_cloud_namespace_tags(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    tags: tuple[tuple[str, str], ...],
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_namespace_required(demo_project):
        return temporal_cloud_not_required_status(checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    desired = {key: value for key, value in tags}
    result = await reconciler.update_namespace_tags(namespace.qualified_name, desired)
    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "namespace": {"name": namespace.qualified_name},
        "tags": {
            "action": result.get("action"),
            "desired": desired,
        },
    }


async def ensure_temporal_cloud_namespace_readers_access(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    group_name: str,
    reconciler: NamespaceReadersReconciler,
    checked_at: str,
) -> dict[str, Any]:
    """Reconcile step: make sure the readers group can read this namespace."""
    if not resources.temporal_namespace_required(demo_project):
        return temporal_cloud_not_required_status(checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    action = await grant_namespace_readers_access(
        reconciler,
        group_name=group_name,
        qualified_namespace=namespace.qualified_name,
    )
    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "namespace": {"name": namespace.qualified_name},
        "readersGroup": {"name": group_name, "action": action},
    }


async def grant_namespace_readers_access(
    reconciler: NamespaceReadersReconciler,
    *,
    group_name: str,
    qualified_namespace: str,
    retry_delay_seconds: float = USER_GROUP_RETRY_DELAY_SECONDS,
) -> str:
    """Make the readers group's access to one namespace exactly Read.

    Returns "granted" (it had none), "restricted" (it had more than Read, e.g.
    set by hand, which membership would otherwise leak), "verified", or
    "group-missing" (an account admin has not created the group yet).
    """
    for attempt in range(USER_GROUP_UPDATE_ATTEMPTS):
        group = await reconciler.find_user_group(group_name)
        if group is None:
            return "group-missing"
        current = group.namespace_permissions.get(qualified_namespace)
        if current == READERS_GROUP_PERMISSION:
            return "verified"
        try:
            await reconciler.set_user_group_namespace_access(
                group, qualified_namespace, READERS_GROUP_PERMISSION
            )
        except Exception as error:  # noqa: BLE001 - retry stale group versions
            if not _is_stale_resource_version(error) or attempt == USER_GROUP_UPDATE_ATTEMPTS - 1:
                raise
            await _group_retry_sleep(attempt, retry_delay_seconds)
            continue
        return "granted" if current is None else "restricted"
    raise RuntimeError(f"Granting {group_name!r} read on {qualified_namespace} exhausted retries.")


async def revoke_namespace_readers_access(
    reconciler: NamespaceReadersReconciler,
    *,
    group_name: str,
    qualified_namespace: str,
    retry_delay_seconds: float = USER_GROUP_RETRY_DELAY_SECONDS,
) -> str:
    """Drop the readers group's access to a namespace that is being deleted."""
    for attempt in range(USER_GROUP_UPDATE_ATTEMPTS):
        group = await reconciler.find_user_group(group_name)
        if group is None:
            return "group-missing"
        if qualified_namespace not in group.namespace_permissions:
            return "not-granted"
        try:
            await reconciler.set_user_group_namespace_access(
                group, qualified_namespace, None
            )
        except Exception as error:  # noqa: BLE001 - retry stale group versions
            if _is_not_found(error):
                return "not-granted"
            if not _is_stale_resource_version(error) or attempt == USER_GROUP_UPDATE_ATTEMPTS - 1:
                raise
            await _group_retry_sleep(attempt, retry_delay_seconds)
            continue
        return "revoked"
    raise RuntimeError(f"Removing {group_name!r} from {qualified_namespace} exhausted retries.")


async def sync_namespace_readers_group(
    reconciler: NamespaceReadersReconciler,
    *,
    group_name: str,
    owner_tag: tuple[str, str],
    retry_delay_seconds: float = USER_GROUP_RETRY_DELAY_SECONDS,
) -> dict[str, Any]:
    """Grant the readers group exactly Read on every managed namespace.

    Managed namespaces are the ones carrying the operator's owner tag. Demo
    reconciles keep new namespaces covered afterwards; this backfills the ones
    that existed before the group did. The group itself is created once by an
    account admin; until then there is nothing to grant to.
    """
    group = await reconciler.find_user_group(group_name)
    if group is None:
        return {"group": {"name": group_name, "action": "missing"}, "namespaces": {}}
    namespaces = await reconciler.list_tagged_namespaces(*owner_tag)
    results: dict[str, str] = {}
    for qualified_namespace in sorted(namespaces):
        results[qualified_namespace] = await grant_namespace_readers_access(
            reconciler,
            group_name=group_name,
            qualified_namespace=qualified_namespace,
            retry_delay_seconds=retry_delay_seconds,
        )
    return {
        "group": {"name": group_name, "id": group.group_id, "action": "verified"},
        "namespaces": results,
    }


async def grant_user_namespace_read_access(
    reconciler: NamespaceReadersReconciler,
    *,
    email: str,
    group_name: str,
    owner_tag: tuple[str, str],
) -> dict[str, Any]:
    """Self-service: add the Temporal Cloud user with this email to the readers group.

    Membership is the only thing this changes: no account role and no
    namespace permission of the user is touched. Returns a status of "added",
    "already-member", "user-not-found", or "group-missing".
    """
    user_id = await reconciler.find_user_by_email(email)
    if user_id is None:
        return {"status": "user-not-found", "email": email, "group": group_name}
    group = await reconciler.find_user_group(group_name)
    if group is None:
        return {
            "status": "group-missing",
            "email": email,
            "group": group_name,
            "message": (
                f"The {group_name} group doesn't exist yet. A Temporal Cloud "
                "account admin needs to create it once."
            ),
        }
    if await reconciler.user_group_has_member(group.group_id, user_id):
        return {"status": "already-member", "email": email, "group": group_name}
    action = await reconciler.add_user_group_member(group.group_id, user_id)
    status = "already-member" if action == "already-member" else "added"
    return {"status": status, "email": email, "group": group_name}


async def _group_retry_sleep(attempt: int, base_seconds: float) -> None:
    if base_seconds <= 0:
        await asyncio.sleep(0)
        return
    delay = min(base_seconds * (2**attempt), 4.0)
    await asyncio.sleep(delay + random.uniform(0, base_seconds))


async def ensure_temporal_cloud_search_attribute(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    search_attribute: dict[str, str],
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_namespace_required(demo_project):
        return temporal_cloud_not_required_status(checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    name = string_value(search_attribute, "name")
    attribute_type = resources.temporal_search_attribute_type(
        search_attribute.get("type")
    )
    result = await reconciler.create_or_verify_search_attribute(
        namespace.qualified_name,
        name,
        attribute_type,
    )
    action = result.get("action")
    current_type = result.get("currentType")
    phase = "Error" if action == "drifted" else "Ready"
    status = {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "namespace": {"name": namespace.qualified_name},
        "searchAttribute": {
            "name": name,
            "type": attribute_type,
            "action": action,
        },
    }
    if current_type:
        status["searchAttribute"]["currentType"] = current_type
    if action == "drifted":
        status.update(
            {
                "reason": "SearchAttributeTypeDrift",
                "message": (
                    f"Temporal Cloud search attribute {name!r} exists with type "
                    f"{current_type!r}, but project config requires "
                    f"{attribute_type!r}."
                ),
            }
        )
    return status


async def ensure_temporal_cloud_search_attributes(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    desired: list[dict[str, str]],
    delete_names: list[str],
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> list[dict[str, Any]]:
    if not resources.temporal_namespace_required(demo_project):
        return [temporal_cloud_not_required_status(checked_at)]

    namespace = namespace_info_from_status(namespace_status_value)
    desired_types = {
        entry["name"]: resources.temporal_search_attribute_type(entry.get("type"))
        for entry in desired
    }

    results = await reconciler.reconcile_search_attributes(
        namespace.qualified_name,
        desired=list(desired_types.items()),
        delete_names=list(delete_names),
    )

    statuses: list[dict[str, Any]] = []
    for name, attribute_type in desired_types.items():
        outcome = results.get(name, {})
        action = outcome.get("action")
        current_type = outcome.get("currentType")
        phase = "Error" if action == "drifted" else "Ready"
        status = {
            "phase": phase,
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "namespace": {"name": namespace.qualified_name},
            "searchAttribute": {
                "name": name,
                "type": attribute_type,
                "action": action,
            },
        }
        if current_type:
            status["searchAttribute"]["currentType"] = current_type
        if action == "drifted":
            status.update(
                {
                    "reason": "SearchAttributeTypeDrift",
                    "message": (
                        f"Temporal Cloud search attribute {name!r} exists with "
                        f"type {current_type!r}, but project config requires "
                        f"{attribute_type!r}."
                    ),
                }
            )
        statuses.append(status)

    for name in delete_names:
        outcome = results.get(name, {})
        current_type = outcome.get("currentType")
        statuses.append(
            {
                "phase": "Deleted",
                "provider": "temporal-cloud",
                "mode": "cloud-api",
                "checkedAt": checked_at,
                "namespace": {"name": namespace.qualified_name},
                "searchAttribute": {
                    "name": name,
                    "type": current_type,
                    "action": outcome.get("action"),
                },
            }
        )

    return statuses


async def delete_temporal_cloud_search_attribute_value(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    search_attribute: dict[str, str],
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_namespace_required(demo_project):
        return temporal_cloud_not_required_status(checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    name = string_value(search_attribute, "name")
    attribute_type = resources.temporal_search_attribute_type(
        search_attribute.get("type")
    )
    result = await reconciler.delete_search_attribute(namespace.qualified_name, name)
    action = result.get("action")
    current_type = result.get("currentType")
    status = {
        "phase": "Deleted",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "namespace": {"name": namespace.qualified_name},
        "searchAttribute": {
            "name": name,
            "type": attribute_type,
            "action": action,
        },
    }
    if current_type:
        status["searchAttribute"]["currentType"] = current_type
    return status


async def stage_temporal_api_key_candidate(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    service_account_status: dict[str, Any],
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
    api_key_ttl_days: int = TEMPORAL_API_KEY_TTL_DAYS,
    rotate_before_days: int = TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS,
    force_rotation: bool = False,
    candidate_secret_token: str | None = None,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_API_KEY:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)

    runtime = check_temporal_api_key_runtime_secret(
        demo_project,
        namespace_status_value=namespace_status_value,
        secret_store=secret_store,
        checked_at=checked_at,
        api_key_ttl_days=api_key_ttl_days,
        rotate_before_days=rotate_before_days,
        force_rotation=force_rotation,
    )
    if not bool(runtime.get("rotate")):
        return runtime

    return await create_temporal_api_key_candidate_secret(
        demo_project,
        namespace_status_value=namespace_status_value,
        service_account_status=service_account_status,
        runtime_secret_status=runtime,
        reconciler=reconciler,
        secret_store=secret_store,
        checked_at=checked_at,
        api_key_ttl_days=api_key_ttl_days,
        rotate_before_days=rotate_before_days,
        candidate_secret_token=candidate_secret_token,
    )


def check_temporal_api_key_runtime_secret(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    api_key_ttl_days: int = TEMPORAL_API_KEY_TTL_DAYS,
    rotate_before_days: int = TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS,
    force_rotation: bool = False,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_API_KEY:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    runtime_path = resources.temporal_secret_remote_path(demo_project)
    existing, state = read_runtime_secret_for_rotation(secret_store, runtime_path)
    rotation = temporal_auth.temporal_api_key_rotation_decision(
        existing,
        checked_at=checked_at,
        rotate_before_days=rotate_before_days,
    )
    if (
        force_rotation
        and isinstance(existing, dict)
        and existing.get("apiKey")
    ):
        rotation = {"rotate": True, "reason": temporal_auth.MANUAL_ROTATION_REASON}

    checked = {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "authMode": resources.TEMPORAL_AUTH_MODE_API_KEY,
        "promoteRequired": False,
        "rotate": bool(rotation["rotate"]),
        "rotation": rotation,
        "runtimeSecret": {
            "secretRemotePath": runtime_path,
            "state": state,
            "exists": isinstance(existing, dict),
        },
    }
    if state == "MarkedForDeletion":
        checked["message"] = (
            "Temporal runtime secret is marked for deletion; treating it as absent."
        )

    if isinstance(existing, dict) and existing.get("apiKey") and not rotation["rotate"]:
        key_id = string_optional(existing.get("keyId"))
        expiry_time = string_optional(existing.get("expiryTime"))
        checked["apiKey"] = api_key_candidate_status(
            demo_project,
            namespace=namespace,
            key_id=key_id,
            action="verified",
            ttl_days=api_key_ttl_days,
            rotate_before_days=rotate_before_days,
            rotation=rotation,
            expiry_time=expiry_time,
            created_at=string_optional(existing.get("createdAt")),
            canary_status=existing.get("canary"),
        )
    return checked


async def create_temporal_api_key_candidate_secret(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    service_account_status: dict[str, Any],
    runtime_secret_status: dict[str, Any],
    reconciler: TemporalCloudReconciler,
    secret_store: SecretStore,
    checked_at: str,
    api_key_ttl_days: int = TEMPORAL_API_KEY_TTL_DAYS,
    rotate_before_days: int = TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS,
    candidate_secret_token: str | None = None,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_API_KEY:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    service_account = service_account_status.get("serviceAccount")
    service_account = service_account if isinstance(service_account, dict) else {}
    service_account_id = string_value(service_account, "id")
    service_account_name = string_value(service_account, "name")
    rotation = runtime_secret_status.get("rotation")
    rotation = rotation if isinstance(rotation, dict) else {
        "rotate": True,
        "reason": "MissingRuntimeSecret",
    }
    api_key = runtime_secret_status.get("apiKey")
    api_key = api_key if isinstance(api_key, dict) else {}

    result = await reconciler.create_api_key(
        service_account_id,
        service_account_name,
        f"tmprl-demo.cloud runtime key for {resources.project_name(demo_project)}",
        api_key_ttl_days,
    )
    previous_key_id = string_optional(api_key.get("keyId"))
    previous_expiry_time = string_optional(api_key.get("expiryTime"))
    candidate_path = temporal_runtime_candidate_secret_path(
        demo_project,
        candidate_secret_token,
    )
    token = normalized_candidate_secret_token(candidate_secret_token)
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
    if result.expiry_time:
        secret_value["expiryTime"] = result.expiry_time
    if previous_key_id:
        secret_value["previousKeyId"] = previous_key_id
    if previous_expiry_time:
        secret_value["previousExpiryTime"] = previous_expiry_time
    if token:
        secret_value["candidateSecretToken"] = token
    secret_store.write_json(
        candidate_path,
        secret_value,
        f"Temporal Cloud candidate runtime credentials for {resources.project_name(demo_project)}",
    )
    status = {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "authMode": resources.TEMPORAL_AUTH_MODE_API_KEY,
        "promoteRequired": True,
        "candidateSecretRemotePath": candidate_path,
        "candidateApiKeyId": result.key_id,
        "apiKey": api_key_candidate_status(
            demo_project,
            namespace=namespace,
            key_id=result.key_id,
            action="rotated" if previous_key_id else "created",
            ttl_days=api_key_ttl_days,
            rotate_before_days=rotate_before_days,
            rotation=rotation,
            expiry_time=result.expiry_time,
            created_at=checked_at,
            previous_key_id=string_optional(previous_key_id),
        ),
    }
    if token:
        status["candidateSecretToken"] = token
    return status


def read_runtime_secret_for_rotation(
    secret_store: SecretStore,
    runtime_path: str,
) -> tuple[dict[str, Any] | None, str]:
    try:
        existing = secret_store.read_json(runtime_path)
    except Exception as error:  # noqa: BLE001 - normalize provider deletion state
        if secret_marked_for_deletion(error):
            return None, "MarkedForDeletion"
        raise
    if isinstance(existing, dict):
        return existing, "Ready"
    return None, "Missing"


def secret_marked_for_deletion(error: Exception) -> bool:
    if _error_code(error) != "InvalidRequestException":
        return False
    return "marked for deletion" in str(error).lower()


def stage_temporal_mtls_client_certificate(
    demo_project: dict[str, Any],
    *,
    namespace_status_value: dict[str, Any],
    ca_status: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
    force_client_rotation: bool = False,
    candidate_secret_token: str | None = None,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_MTLS:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)

    namespace = namespace_info_from_status(namespace_status_value)
    runtime_path = resources.temporal_secret_remote_path(demo_project)
    candidate_path = temporal_runtime_candidate_secret_path(
        demo_project,
        candidate_secret_token,
    )
    existing = secret_store.read_json(runtime_path)
    ca_certificate = nested_dict(ca_status, "mtls", "caCertificate")
    ca_material = read_mtls_ca_material_for_reconcile(
        demo_project,
        secret_store,
        allow_candidate=True,
        candidate_ca_secret_path=ca_status_candidate_path(ca_certificate),
    )
    ca_promote_required = bool(ca_status.get("promoteRequired"))
    common_name = temporal_auth.temporal_mtls_client_common_name(demo_project)
    client_rotation = temporal_auth.temporal_mtls_client_rotation_decision(
        existing,
        checked_at=checked_at,
        ca_serial_number=str(ca_material["serialNumber"]),
        rotate_every_days=resources.TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
    )
    if (
        force_client_rotation
        and isinstance(existing, dict)
        and existing.get("clientCert")
    ):
        client_rotation = {"rotate": True, "reason": temporal_auth.MANUAL_ROTATION_REASON}

    if (
        isinstance(existing, dict)
        and existing.get("clientCert")
        and existing.get("clientKey")
        and not client_rotation["rotate"]
    ):
        client_status = {
            "action": "verified",
            "identityScope": "namespace",
            "namespace": namespace.qualified_name,
            "commonName": common_name,
            "serialNumber": existing.get("clientCertSerialNumber"),
            "notBefore": existing.get("clientCertNotBefore"),
            "notAfter": existing.get("clientCertNotAfter"),
            "createdAt": existing.get("createdAt")
            or existing.get("clientCertNotBefore"),
            "rotateAt": client_rotation.get("rotateAt"),
            "rotation": {
                "strategy": "staggered",
                "rotateEveryDays": resources.TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
            },
            "rotationReason": client_rotation.get("reason"),
        }
        if isinstance(existing.get("canary"), dict):
            client_status["canary"] = existing["canary"]
        return {
            "phase": "Ready",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "authMode": resources.TEMPORAL_AUTH_MODE_MTLS,
            "promoteRequired": ca_promote_required,
            "candidateCaSecretRemotePath": ca_status_candidate_path(ca_certificate),
            "mtls": {
                **resources.temporal_mtls_status(demo_project),
                "action": (
                    "rotated" if ca_certificate.get("action") == "rotated"
                    else "created" if ca_certificate.get("action") == "created"
                    else "verified"
                ),
                "namespace": namespace.qualified_name,
                "caCertificate": ca_certificate,
                "clientCertificate": client_status,
            },
        }

    client_cert = temporal_auth.generate_mtls_client_certificate(
        ca_material,
        common_name=common_name,
        checked_at=checked_at,
    )
    previous_client_serial = (
        existing.get("clientCertSerialNumber") if isinstance(existing, dict) else None
    )
    token = normalized_candidate_secret_token(candidate_secret_token)
    secret_value: dict[str, Any] = {
        "authMode": resources.TEMPORAL_AUTH_MODE_MTLS,
        "address": namespace.grpc_address,
        "namespace": namespace.qualified_name,
        "tls": "true",
        "caCert": ca_material["caCert"],
        "clientCert": client_cert["clientCert"],
        "clientKey": client_cert["clientKey"],
        "caSerialNumber": ca_material["serialNumber"],
        "clientCertSerialNumber": client_cert["serialNumber"],
        "clientCertCommonName": common_name,
        "clientCertNotBefore": client_cert["notBefore"],
        "clientCertNotAfter": client_cert["notAfter"],
        "clientCertRotateAt": temporal_auth.mtls_rotate_at(
            client_cert["notBefore"],
            resources.TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
        ),
        "createdAt": checked_at,
        "rotationReason": client_rotation["reason"],
    }
    if previous_client_serial:
        secret_value["previousClientCertSerialNumber"] = previous_client_serial
    if token:
        secret_value["candidateSecretToken"] = token
    secret_store.write_json(
        candidate_path,
        secret_value,
        f"Temporal Cloud candidate mTLS credentials for {resources.project_name(demo_project)}",
    )
    client_status = {
        "action": (
            "rotated"
            if isinstance(existing, dict) and existing.get("clientCert")
            else "created"
        ),
        "identityScope": "namespace",
        "namespace": namespace.qualified_name,
        "commonName": common_name,
        "serialNumber": client_cert.get("serialNumber"),
        "notBefore": client_cert.get("notBefore"),
        "notAfter": client_cert.get("notAfter"),
        "createdAt": checked_at,
        "rotateAt": temporal_auth.mtls_rotate_at(
            str(client_cert.get("notBefore", checked_at)),
            resources.TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
        ),
        "rotation": {
            "strategy": "staggered",
            "rotateEveryDays": resources.TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
        },
        "rotationReason": client_rotation.get("reason"),
    }
    if previous_client_serial:
        client_status["previousClientCertSerialNumber"] = previous_client_serial
    status = {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "authMode": resources.TEMPORAL_AUTH_MODE_MTLS,
        "promoteRequired": True,
        "candidateSecretRemotePath": candidate_path,
        "candidateCaSecretRemotePath": ca_status_candidate_path(ca_certificate),
        "mtls": {
            **resources.temporal_mtls_status(demo_project),
            "action": (
                "rotated"
                if client_status["action"] == "rotated"
                or ca_certificate.get("action") == "rotated"
                else "created"
            ),
            "namespace": namespace.qualified_name,
            "caCertificate": ca_certificate,
            "clientCertificate": client_status,
        },
    }
    if token:
        status["candidateSecretToken"] = token
    return status


async def run_temporal_runtime_candidate_canary(
    demo_project: dict[str, Any],
    *,
    stage_status: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if not stage_status.get("promoteRequired"):
        return {
            "phase": "Skipped",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "reason": "NoCandidateCredential",
        }

    candidate_path = stage_status.get("candidateSecretRemotePath")
    if isinstance(candidate_path, str) and candidate_path:
        credential = secret_store.read_json(candidate_path)
    else:
        credential = secret_store.read_json(resources.temporal_secret_remote_path(demo_project))
    if not isinstance(credential, dict):
        raise ValueError("Temporal runtime canary credential secret is missing.")
    return await temporal_auth.run_runtime_canary(credential, None)


def promote_temporal_runtime_secret(
    demo_project: dict[str, Any],
    *,
    stage_status: dict[str, Any],
    canary_status: dict[str, Any] | None,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)

    promoted_runtime = False
    runtime_path = resources.temporal_secret_remote_path(demo_project)
    candidate_path = stage_status.get("candidateSecretRemotePath")
    if isinstance(candidate_path, str) and candidate_path:
        credential = secret_store.read_json(candidate_path)
        if not isinstance(credential, dict):
            raise ValueError("Temporal runtime candidate secret is missing.")
        if isinstance(canary_status, dict):
            credential["canary"] = canary_status
        secret_store.write_json(
            runtime_path,
            credential,
            f"Temporal Cloud runtime credentials for {resources.project_name(demo_project)}",
        )
        promoted_runtime = True

    rendered = {
        **stage_status,
        "phase": "Ready",
        "checkedAt": checked_at,
        "promoted": {
            "runtimeSecret": promoted_runtime,
        },
    }
    attach_canary_to_status(rendered, canary_status)
    return rendered


def promote_temporal_mtls_ca_secret(
    demo_project: dict[str, Any],
    *,
    stage_status: dict[str, Any],
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)

    promoted_ca = False
    candidate_ca_path = stage_status.get("candidateCaSecretRemotePath")
    if isinstance(candidate_ca_path, str) and candidate_ca_path:
        ca_material = secret_store.read_json(candidate_ca_path)
        if not isinstance(ca_material, dict):
            raise ValueError("Temporal mTLS CA candidate secret is missing.")
        secret_store.write_json(
            resources.temporal_mtls_ca_secret_remote_path(demo_project),
            ca_material,
            f"Temporal Cloud mTLS CA for {resources.project_name(demo_project)}",
        )
        promoted_ca = True

    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "promoted": {
            "caSecret": promoted_ca,
        },
    }


def mirror_temporal_runtime_secret(
    demo_project: dict[str, Any],
    *,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    credential = secret_store.read_json(resources.temporal_secret_remote_path(demo_project))
    if not isinstance(credential, dict):
        raise ValueError("Temporal runtime secret is missing; cannot mirror.")
    actions = temporal_auth.mirror_runtime_kubernetes_secrets(demo_project, credential)
    return {
        "phase": "Ready",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "mirroredSecrets": actions,
    }


def delete_temporal_runtime_candidate_secret(
    demo_project: dict[str, Any],
    *,
    secret_store: SecretStore,
    checked_at: str,
    candidate_secret_path: str | None = None,
    candidate_ca_secret_path: str | None = None,
    candidate_secret_token: str | None = None,
) -> dict[str, Any]:
    runtime_candidate_path = (
        candidate_secret_path
        if isinstance(candidate_secret_path, str) and candidate_secret_path
        else temporal_runtime_candidate_secret_path(
            demo_project,
            candidate_secret_token,
        )
    )
    mtls_ca_candidate_path = (
        candidate_ca_secret_path
        if isinstance(candidate_ca_secret_path, str) and candidate_ca_secret_path
        else temporal_mtls_ca_candidate_secret_path(
            demo_project,
            candidate_secret_token,
        )
    )
    deleted = {
        "runtimeCandidateSecret": secret_store.delete(runtime_candidate_path),
        "mtlsCaCandidateSecret": secret_store.delete(mtls_ca_candidate_path),
    }
    return {
        "phase": "Deleted",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "deleted": deleted,
        "paths": {
            "runtimeCandidateSecret": runtime_candidate_path,
            "mtlsCaCandidateSecret": mtls_ca_candidate_path,
        },
    }


def delete_temporal_cloud_mtls_ca_secret(
    demo_project: dict[str, Any],
    *,
    secret_store: SecretStore,
    checked_at: str,
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return temporal_cloud_not_required_status(checked_at)
    if resources.temporal_auth_mode(demo_project) != resources.TEMPORAL_AUTH_MODE_MTLS:
        return temporal_cloud_auth_skipped_status(demo_project, checked_at)
    action = secret_store.delete(resources.temporal_mtls_ca_secret_remote_path(demo_project))
    return {
        "phase": "Deleted",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "deleted": {"mtlsCaSecret": action},
    }


async def delete_temporal_cloud_api_key_id(
    key_id: str,
    *,
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    action = await reconciler.delete_api_key(key_id) if key_id else "not-found"
    return temporal_cloud_step_status(
        checked_at,
        deleted={"apiKey": action},
    )


async def delete_temporal_cloud_service_account_id(
    service_account_id: str,
    *,
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    action = (
        await reconciler.delete_service_account(service_account_id)
        if service_account_id
        else "not-found"
    )
    return temporal_cloud_step_status(
        checked_at,
        deleted={"serviceAccount": action},
    )


async def disable_temporal_cloud_namespace_delete_protection_name(
    qualified_name: str,
    *,
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    action = (
        await reconciler.disable_namespace_delete_protection(qualified_name)
        if qualified_name
        else "not-found"
    )
    return temporal_cloud_step_status(
        checked_at,
        namespace=qualified_name or None,
        deleted={"namespaceDeleteProtection": action},
    )


async def delete_temporal_cloud_namespace_name(
    qualified_name: str,
    *,
    reconciler: TemporalCloudReconciler,
    checked_at: str,
) -> dict[str, Any]:
    action = (
        await reconciler.delete_namespace(qualified_name)
        if qualified_name
        else "not-found"
    )
    return temporal_cloud_step_status(
        checked_at,
        namespace=qualified_name or None,
        deleted={"namespace": action},
    )


def temporal_cloud_base_status(
    demo_project: dict[str, Any],
    *,
    checked_at: str,
    account_id: str | None,
    region: str,
    phase: str,
) -> dict[str, Any]:
    return {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "accountId": account_id,
        "region": region,
        "auth": resources.temporal_auth_status(demo_project),
    }


def temporal_cloud_not_required_status(checked_at: str) -> dict[str, Any]:
    return {
        "phase": "NotRequired",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "message": "No component currently requires Temporal credentials.",
    }


def temporal_cloud_auth_skipped_status(
    demo_project: dict[str, Any],
    checked_at: str,
) -> dict[str, Any]:
    return {
        "phase": "Skipped",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "auth": resources.temporal_auth_status(demo_project),
    }


def namespace_status(namespace: NamespaceInfo, created: bool) -> dict[str, Any]:
    return {
        "name": namespace.qualified_name,
        "action": "created" if created else "verified",
        "deletionPolicy": "retain-delete-protected",
        "grpcAddress": namespace.grpc_address,
        "webAddress": namespace.web_address,
        "state": namespace.state,
        "authMode": namespace.auth_mode,
        "region": namespace.region,
    }


def namespace_info_from_status(status: dict[str, Any]) -> NamespaceInfo:
    namespace = status.get("namespace")
    namespace = namespace if isinstance(namespace, dict) else {}
    return NamespaceInfo(
        qualified_name=string_value(namespace, "name"),
        grpc_address=string_value(namespace, "grpcAddress"),
        web_address=string_optional(namespace.get("webAddress")),
        state=string_optional(namespace.get("state")),
        auth_mode=string_optional(namespace.get("authMode")),
        region=string_optional(namespace.get("region")),
        created=namespace.get("action") == "created",
    )


def read_mtls_ca_material_for_reconcile(
    demo_project: dict[str, Any],
    secret_store: SecretStore,
    *,
    allow_candidate: bool,
    candidate_ca_secret_path: str | None = None,
) -> dict[str, Any]:
    material = None
    if allow_candidate:
        if isinstance(candidate_ca_secret_path, str) and candidate_ca_secret_path:
            material = secret_store.read_json(candidate_ca_secret_path)
        if not isinstance(material, dict):
            material = secret_store.read_json(
                temporal_mtls_ca_candidate_secret_path(demo_project)
            )
    if not isinstance(material, dict):
        material = secret_store.read_json(resources.temporal_mtls_ca_secret_remote_path(demo_project))
    if not isinstance(material, dict) or not material.get("caCert"):
        raise ValueError("Temporal mTLS CA material is missing.")
    return material


def mtls_ca_certificate_status(
    demo_project: dict[str, Any],
    material: dict[str, Any],
    rotation: dict[str, Any],
    *,
    action: str,
    promote_required: bool,
    candidate_path: str | None = None,
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "action": action,
        "scope": "namespace",
        "serialNumber": str(material.get("serialNumber", "")),
        "secretRemotePath": resources.temporal_mtls_ca_secret_remote_path(demo_project),
        "notBefore": material.get("notBefore"),
        "notAfter": material.get("notAfter"),
        "rotateAt": rotation.get("rotateAt"),
        "rotation": {
            "strategy": "staggered",
            "rotateEveryDays": resources.TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
        },
    }
    if promote_required:
        status["candidateSecretRemotePath"] = (
            candidate_path or temporal_mtls_ca_candidate_secret_path(demo_project)
        )
    if rotation.get("reason"):
        status["rotationReason"] = rotation["reason"]
    previous_ca_remove_at = material.get("previousCaRemoveAt")
    if isinstance(previous_ca_remove_at, str) and previous_ca_remove_at:
        status["previousCaRemoveAt"] = previous_ca_remove_at
        status["previousCaSerialNumber"] = material.get("previousCaSerialNumber")
        status["overlapDays"] = temporal_auth.MTLS_CA_OVERLAP_DAYS
    return status


def api_key_candidate_status(
    demo_project: dict[str, Any],
    *,
    namespace: NamespaceInfo,
    key_id: str | None,
    action: str,
    ttl_days: int,
    rotate_before_days: int,
    rotation: dict[str, Any],
    expiry_time: str | None,
    created_at: str | None,
    previous_key_id: str | None = None,
    canary_status: Any = None,
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "keyId": key_id,
        "action": action,
        "scope": "namespace",
        "namespace": namespace.qualified_name,
        "ttlDays": ttl_days,
        "rotateBeforeExpiryDays": rotate_before_days,
        "secretRemotePath": resources.temporal_secret_remote_path(demo_project),
        "expiryTime": expiry_time,
        "createdAt": created_at,
        "components": sorted(resources.temporal_components(demo_project)),
    }
    if previous_key_id:
        status["previousKeyId"] = previous_key_id
    if rotation.get("reason"):
        status["rotationReason"] = rotation["reason"]
    if isinstance(canary_status, dict):
        status["canary"] = canary_status
    return status


def nested_dict(value: dict[str, Any], *keys: str) -> dict[str, Any]:
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def ca_status_candidate_path(ca_certificate: dict[str, Any]) -> str | None:
    candidate_path = ca_certificate.get("candidateSecretRemotePath")
    return candidate_path if isinstance(candidate_path, str) and candidate_path else None


def attach_canary_to_status(
    status: dict[str, Any],
    canary_status: dict[str, Any] | None,
) -> None:
    if not isinstance(canary_status, dict):
        return
    api_key = status.get("apiKey")
    if isinstance(api_key, dict):
        api_key["canary"] = canary_status
    mtls = status.get("mtls")
    mtls = mtls if isinstance(mtls, dict) else {}
    client = mtls.get("clientCertificate")
    if isinstance(client, dict):
        client["canary"] = canary_status


def string_value(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field:
        return field
    raise ValueError(f"Temporal Cloud status is missing {key}.")


def string_optional(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


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

    if not resources.temporal_components(demo_project):
        return {
            "phase": "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "checkedAt": checked_at,
            "message": "No Temporal Cloud resources to delete.",
        }

    runtime_path, existing = temporal_cloud_runtime_secret_with_path(
        demo_project,
        secret_store,
    )
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

    runtime_path = temporal_cloud_runtime_secret_path(demo_project)
    action = secret_store.delete(runtime_path)
    return temporal_cloud_step_status(
        checked_at,
        deleted={"secret": action},
    )


def finalization_step_base(
    demo_project: dict[str, Any],
    checked_at: str,
) -> dict[str, Any] | None:
    if resources.temporal_components(demo_project):
        return None
    return {
        "phase": "NotRequired",
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": checked_at,
        "message": "No Temporal Cloud resources to delete.",
    }


def temporal_cloud_runtime_secret_path(demo_project: dict[str, Any]) -> str:
    return resources.temporal_secret_remote_path(demo_project)


def temporal_cloud_runtime_secret_with_path(
    demo_project: dict[str, Any],
    secret_store: SecretStore,
) -> tuple[str, dict[str, Any]]:
    path = temporal_cloud_runtime_secret_path(demo_project)
    try:
        existing = secret_store.read_json(path)
    except Exception as error:  # noqa: BLE001 - normalize completed compensation
        if not secret_marked_for_deletion(error):
            raise
        existing = None
    return path, existing if isinstance(existing, dict) else {}


def temporal_cloud_runtime_secret(
    demo_project: dict[str, Any],
    secret_store: SecretStore,
) -> dict[str, Any]:
    _path, existing = temporal_cloud_runtime_secret_with_path(
        demo_project,
        secret_store,
    )
    return existing


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
        readers_group: str | None = None,
    ) -> None:
        self._cs = cloud_service
        self._account_id = account_id
        self._heartbeat = heartbeat
        self._poll_seconds = poll_seconds
        self._timeout_seconds = timeout_seconds
        # Display name of the namespace readers group; when set, deleting a
        # namespace first removes that group's access to it.
        self._readers_group = readers_group or None

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
        auth_mode: str = resources.TEMPORAL_AUTH_MODE_API_KEY,
        accepted_client_ca: str | None = None,
        certificate_common_name: str | None = None,
        enable_delete_protection: bool = True,
    ) -> NamespaceInfo:
        from temporalio.api.cloud.cloudservice.v1 import (
            CreateNamespaceRequest,
            GetNamespaceRequest,
        )
        from temporalio.api.cloud.namespace.v1 import (
            ApiKeyAuthSpec,
            CapacitySpec,
            CertificateFilterSpec,
            LifecycleSpec,
            MtlsAuthSpec,
            NamespaceSpec,
        )

        auth_spec: dict[str, Any]
        if auth_mode == resources.TEMPORAL_AUTH_MODE_MTLS:
            if not accepted_client_ca or not certificate_common_name:
                raise ValueError(
                    "mTLS namespace creation requires accepted_client_ca and "
                    "certificate_common_name."
                )
            auth_spec = {
                "api_key_auth": ApiKeyAuthSpec(enabled=False),
                "mtls_auth": MtlsAuthSpec(
                    enabled=True,
                    accepted_client_ca=accepted_client_ca.encode("utf-8"),
                    certificate_filters=[
                        CertificateFilterSpec(common_name=certificate_common_name)
                    ],
                ),
            }
        else:
            auth_spec = {"api_key_auth": ApiKeyAuthSpec(enabled=True)}

        spec = NamespaceSpec(
            name=name,
            regions=[region],
            retention_days=retention_days,
            **auth_spec,
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

    async def configure_namespace_mtls(
        self,
        qualified_name: str,
        accepted_client_ca: str,
        certificate_common_name: str,
    ) -> str:
        from temporalio.api.cloud.cloudservice.v1 import (
            GetNamespaceRequest,
            UpdateNamespaceRequest,
        )
        from temporalio.api.cloud.namespace.v1 import (
            ApiKeyAuthSpec,
            CertificateFilterSpec,
            MtlsAuthSpec,
        )

        got = await self._cs.get_namespace(GetNamespaceRequest(namespace=qualified_name))
        namespace = got.namespace
        spec = namespace.spec
        desired_ca = accepted_client_ca.encode("utf-8")
        current_mtls = spec.mtls_auth
        current_filters = [
            item.common_name for item in current_mtls.certificate_filters
        ]
        if (
            current_mtls.enabled
            and current_mtls.accepted_client_ca == desired_ca
            and current_filters == [certificate_common_name]
            and not spec.api_key_auth.enabled
        ):
            return "verified"

        spec.api_key_auth = ApiKeyAuthSpec(enabled=False)
        spec.mtls_auth = MtlsAuthSpec(
            enabled=True,
            accepted_client_ca=desired_ca,
            certificate_filters=[
                CertificateFilterSpec(common_name=certificate_common_name)
            ],
        )
        response = await self._cs.update_namespace(
            UpdateNamespaceRequest(
                namespace=qualified_name,
                spec=spec,
                resource_version=namespace.resource_version,
            )
        )
        await self._await_operation(
            response.async_operation,
            label=f"configure-mtls:{qualified_name}",
        )
        return "updated"

    async def create_or_verify_search_attribute(
        self,
        qualified_name: str,
        name: str,
        attribute_type: str,
    ) -> dict[str, str | None]:
        desired_type = resources.temporal_search_attribute_type(attribute_type)
        desired_value = cloud_search_attribute_type_value(desired_type)

        def mutate(spec: Any) -> tuple[bool, dict[str, str | None]]:
            current_type = cloud_search_attribute_status_type(spec, name)
            if current_type == desired_type:
                return False, {
                    "action": "verified",
                    "currentType": current_type,
                }
            if current_type is not None:
                return False, {
                    "action": "drifted",
                    "currentType": current_type,
                }
            spec.search_attributes[name] = desired_value
            return True, {
                "action": "created",
                "currentType": desired_type,
            }

        return await self._update_namespace_spec(
            qualified_name,
            label=f"create-search-attribute:{qualified_name}:{name}",
            mutate=mutate,
        )

    async def delete_search_attribute(
        self,
        qualified_name: str,
        name: str,
    ) -> dict[str, str | None]:
        def mutate(spec: Any) -> tuple[bool, dict[str, str | None]]:
            current_type = cloud_search_attribute_status_type(spec, name)
            if current_type is None:
                return False, {
                    "action": "not-found",
                    "currentType": None,
                }
            del spec.search_attributes[name]
            return True, {
                "action": "deleted",
                "currentType": current_type,
            }

        return await self._update_namespace_spec(
            qualified_name,
            label=f"delete-search-attribute:{qualified_name}:{name}",
            mutate=mutate,
        )

    async def reconcile_search_attributes(
        self,
        qualified_name: str,
        *,
        desired: list[tuple[str, str]],
        delete_names: list[str],
    ) -> dict[str, dict[str, str | None]]:
        # Apply every create/verify/delete in a SINGLE namespace-spec update.
        # Search attributes all live on one namespace spec guarded by
        # resource_version optimistic concurrency, so per-attribute updates run
        # concurrently would conflict ("resource version mismatch"). Batching
        # makes the reconcile one atomic, conflict-free operation.
        def mutate(spec: Any) -> tuple[bool, dict[str, dict[str, str | None]]]:
            results: dict[str, dict[str, str | None]] = {}
            changed = False
            for name, attribute_type in desired:
                desired_type = resources.temporal_search_attribute_type(attribute_type)
                current_type = cloud_search_attribute_status_type(spec, name)
                if current_type == desired_type:
                    results[name] = {"action": "verified", "currentType": current_type}
                elif current_type is not None:
                    results[name] = {"action": "drifted", "currentType": current_type}
                else:
                    spec.search_attributes[name] = cloud_search_attribute_type_value(
                        desired_type
                    )
                    results[name] = {"action": "created", "currentType": desired_type}
                    changed = True
            for name in delete_names:
                current_type = cloud_search_attribute_status_type(spec, name)
                if current_type is None:
                    results[name] = {"action": "not-found", "currentType": None}
                else:
                    del spec.search_attributes[name]
                    results[name] = {"action": "deleted", "currentType": current_type}
                    changed = True
            return changed, results

        return await self._update_namespace_spec(
            qualified_name,
            label=f"reconcile-search-attributes:{qualified_name}",
            mutate=mutate,
        )

    async def update_namespace_tags(
        self,
        qualified_name: str,
        tags: dict[str, str],
    ) -> dict[str, Any]:
        # Namespace tags live on the Namespace (not its spec) and are managed by a
        # dedicated RPC, so this does not go through _update_namespace_spec. Read
        # first and upsert only the drifted keys so a steady-state reconcile makes
        # no change (and starts no async operation).
        from temporalio.api.cloud.cloudservice.v1 import (
            GetNamespaceRequest,
            UpdateNamespaceTagsRequest,
        )

        got = await self._cs.get_namespace(GetNamespaceRequest(namespace=qualified_name))
        current = dict(got.namespace.tags)
        to_upsert = {key: value for key, value in tags.items() if current.get(key) != value}
        if not to_upsert:
            return {"action": "verified", "tags": dict(tags)}
        response = await self._cs.update_namespace_tags(
            UpdateNamespaceTagsRequest(
                namespace=qualified_name,
                tags_to_upsert=to_upsert,
            )
        )
        await self._await_operation(
            response.async_operation,
            label=f"update-namespace-tags:{qualified_name}",
        )
        return {"action": "updated", "tags": to_upsert}

    async def _update_namespace_spec(
        self,
        qualified_name: str,
        *,
        label: str,
        mutate: Any,
        max_attempts: int = 5,
    ) -> dict[str, str | None]:
        from temporalio.api.cloud.cloudservice.v1 import (
            GetNamespaceRequest,
            UpdateNamespaceRequest,
        )

        for attempt in range(max_attempts):
            got = await self._cs.get_namespace(
                GetNamespaceRequest(namespace=qualified_name)
            )
            namespace = got.namespace
            changed, result = mutate(namespace.spec)
            if not changed:
                return result
            try:
                response = await self._cs.update_namespace(
                    UpdateNamespaceRequest(
                        namespace=qualified_name,
                        spec=namespace.spec,
                        resource_version=namespace.resource_version,
                    )
                )
                # Kept inside the try: the resource_version conflict can surface
                # here (async operation FAILED with "resource version mismatch")
                # rather than at update_namespace, so both paths must retry.
                await self._await_operation(response.async_operation, label=label)
            except Exception as error:  # noqa: BLE001 - retry stale versions
                if not _is_stale_resource_version(error) or attempt == max_attempts - 1:
                    raise
                await asyncio.sleep(0)
                continue
            return result

        raise RuntimeError(
            f"Temporal Cloud namespace update {label} exhausted retries."
        )

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
    ) -> temporal_auth.ApiKeyResult:
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
        return temporal_auth.ApiKeyResult(
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
        await self._revoke_readers_access_before_delete(qualified_name)

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

    async def _revoke_readers_access_before_delete(self, qualified_name: str) -> None:
        # BEST-EFFORT: a stale grant on a deleted namespace is harmless, but a
        # failed delete is not, so this never blocks the delete itself.
        if not self._readers_group:
            return
        try:
            await revoke_namespace_readers_access(
                self,
                group_name=self._readers_group,
                qualified_namespace=qualified_name,
            )
        except Exception as error:  # noqa: BLE001
            logger.warning(
                "Could not remove user group %r from namespace %s before deleting it: %s",
                self._readers_group,
                qualified_name,
                error,
            )

    async def find_user_group(self, display_name: str) -> UserGroupInfo | None:
        from temporalio.api.cloud.cloudservice.v1 import GetUserGroupsRequest

        matches = []
        page_token = ""
        while True:
            response = await self._cs.get_user_groups(
                GetUserGroupsRequest(
                    page_size=100,
                    page_token=page_token,
                    display_name=display_name,
                )
            )
            for group in response.groups:
                if group.spec.display_name != display_name:
                    continue
                if _resource_state_name(group.state) in GONE_RESOURCE_STATES:
                    continue
                matches.append(group)
            page_token = response.next_page_token
            if not page_token:
                break
        if not matches:
            return None
        # Oldest first, so callers converge on one group even if a duplicate exists.
        oldest = min(
            matches,
            key=lambda group: (
                group.created_time.seconds,
                group.created_time.nanos,
                group.id,
            ),
        )
        return _user_group_info(oldest)

    async def set_user_group_namespace_access(
        self,
        group: UserGroupInfo,
        qualified_namespace: str,
        permission: str | None,
    ) -> None:
        from temporalio.api.cloud.cloudservice.v1 import (
            SetUserGroupNamespaceAccessRequest,
        )
        from temporalio.api.cloud.identity.v1 import NamespaceAccess

        request = SetUserGroupNamespaceAccessRequest(
            namespace=qualified_namespace,
            group_id=group.group_id,
            resource_version=group.resource_version,
        )
        # Leaving access unset removes the group from the namespace.
        if permission is not None:
            request.access.CopyFrom(
                NamespaceAccess(permission=_namespace_permission_value(permission))
            )
        response = await self._cs.set_user_group_namespace_access(request)
        await self._await_operation(
            response.async_operation,
            label=f"set-user-group-namespace-access:{group.display_name}:{qualified_namespace}",
        )

    async def find_user_by_email(self, email: str) -> str | None:
        from temporalio.api.cloud.cloudservice.v1 import GetUsersRequest

        wanted = email.strip().lower()
        if not wanted:
            return None
        # Ask the API to filter first; fall back to a case-insensitive scan in case
        # the stored address differs from the sign-in address only by case.
        for filter_email in (email.strip(), None):
            page_token = ""
            while True:
                request = GetUsersRequest(page_size=100, page_token=page_token)
                if filter_email:
                    request.email = filter_email
                response = await self._cs.get_users(request)
                for user in response.users:
                    if _resource_state_name(user.state) in GONE_RESOURCE_STATES:
                        continue
                    if user.spec.email.strip().lower() == wanted:
                        return user.id
                page_token = response.next_page_token
                if not page_token:
                    break
        return None

    async def user_group_has_member(self, group_id: str, user_id: str) -> bool:
        from temporalio.api.cloud.cloudservice.v1 import GetUserGroupMembersRequest

        page_token = ""
        while True:
            response = await self._cs.get_user_group_members(
                GetUserGroupMembersRequest(
                    page_size=100, page_token=page_token, group_id=group_id
                )
            )
            if any(member.member_id.user_id == user_id for member in response.members):
                return True
            page_token = response.next_page_token
            if not page_token:
                return False

    async def add_user_group_member(self, group_id: str, user_id: str) -> str:
        from temporalio.api.cloud.cloudservice.v1 import AddUserGroupMemberRequest
        from temporalio.api.cloud.identity.v1 import UserGroupMemberId

        try:
            response = await self._cs.add_user_group_member(
                AddUserGroupMemberRequest(
                    group_id=group_id,
                    member_id=UserGroupMemberId(user_id=user_id),
                )
            )
        except Exception as error:  # noqa: BLE001 - joined in the meantime
            if _is_already_exists(error):
                return "already-member"
            raise
        await self._await_operation(
            response.async_operation, label=f"add-user-group-member:{group_id}"
        )
        return "added"

    async def list_tagged_namespaces(self, key: str, value: str) -> list[str]:
        from temporalio.api.cloud.cloudservice.v1 import GetNamespacesRequest

        names: list[str] = []
        page_token = ""
        while True:
            response = await self._cs.get_namespaces(
                GetNamespacesRequest(page_size=100, page_token=page_token)
            )
            for namespace in response.namespaces:
                if _resource_state_name(namespace.state) in GONE_RESOURCE_STATES:
                    continue
                if dict(namespace.tags).get(key) == value:
                    names.append(namespace.namespace)
            page_token = response.next_page_token
            if not page_token:
                return names

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

    # --- Nexus Endpoints ---
    #
    # Endpoint names are unique account-wide, not per namespace, so find-by-name is
    # a server-side filter rather than a scan. The spec carries the target and the
    # caller allowlist together, so an update rewrites both -- a changed
    # targetTaskQueue drifts back just like a changed allowlist does.

    @staticmethod
    def _nexus_endpoint_info(endpoint: Any, *, created: bool = False) -> NexusEndpointInfo:
        target = endpoint.spec.target_spec.worker_target_spec
        return NexusEndpointInfo(
            endpoint_id=endpoint.id,
            name=endpoint.spec.name,
            target_namespace_id=target.namespace_id,
            target_task_queue=target.task_queue,
            allowed_caller_namespace_ids=tuple(
                policy.allowed_cloud_namespace_policy_spec.namespace_id
                for policy in endpoint.spec.policy_specs
                if policy.HasField("allowed_cloud_namespace_policy_spec")
            ),
            resource_version=endpoint.resource_version,
            state=str(endpoint.state),
            created=created,
        )

    @staticmethod
    def _nexus_endpoint_spec(
        name: str,
        target_namespace_id: str,
        target_task_queue: str,
        allowed_caller_namespace_ids: tuple[str, ...],
    ) -> Any:
        from temporalio.api.cloud.nexus.v1 import (
            AllowedCloudNamespacePolicySpec,
            EndpointPolicySpec,
            EndpointSpec,
            EndpointTargetSpec,
            WorkerTargetSpec,
        )

        return EndpointSpec(
            name=name,
            target_spec=EndpointTargetSpec(
                worker_target_spec=WorkerTargetSpec(
                    namespace_id=target_namespace_id,
                    task_queue=target_task_queue,
                )
            ),
            # Temporal permits no callers by default, even from the target's own
            # namespace, so every allowed caller needs its own policy spec.
            policy_specs=[
                EndpointPolicySpec(
                    allowed_cloud_namespace_policy_spec=AllowedCloudNamespacePolicySpec(
                        namespace_id=namespace_id,
                    )
                )
                for namespace_id in allowed_caller_namespace_ids
            ],
        )

    async def find_nexus_endpoint(self, name: str) -> NexusEndpointInfo | None:
        from temporalio.api.cloud.cloudservice.v1 import GetNexusEndpointsRequest

        response = await self._cs.get_nexus_endpoints(
            GetNexusEndpointsRequest(page_size=100, name=name)
        )
        for endpoint in response.endpoints:
            if endpoint.spec.name == name:
                return self._nexus_endpoint_info(endpoint)
        return None

    async def create_nexus_endpoint(
        self,
        name: str,
        target_namespace_id: str,
        target_task_queue: str,
        allowed_caller_namespace_ids: tuple[str, ...],
    ) -> NexusEndpointInfo:
        from temporalio.api.cloud.cloudservice.v1 import (
            CreateNexusEndpointRequest,
            GetNexusEndpointRequest,
        )

        response = await self._cs.create_nexus_endpoint(
            CreateNexusEndpointRequest(
                spec=self._nexus_endpoint_spec(
                    name,
                    target_namespace_id,
                    target_task_queue,
                    allowed_caller_namespace_ids,
                )
            )
        )
        await self._await_operation(
            response.async_operation, label=f"create-nexus-endpoint:{name}"
        )
        got = await self._cs.get_nexus_endpoint(
            GetNexusEndpointRequest(endpoint_id=response.endpoint_id)
        )
        return self._nexus_endpoint_info(got.endpoint, created=True)

    async def update_nexus_endpoint(
        self,
        endpoint_id: str,
        name: str,
        target_namespace_id: str,
        target_task_queue: str,
        allowed_caller_namespace_ids: tuple[str, ...],
    ) -> NexusEndpointInfo:
        from temporalio.api.cloud.cloudservice.v1 import (
            GetNexusEndpointRequest,
            UpdateNexusEndpointRequest,
        )

        got = await self._cs.get_nexus_endpoint(
            GetNexusEndpointRequest(endpoint_id=endpoint_id)
        )
        response = await self._cs.update_nexus_endpoint(
            UpdateNexusEndpointRequest(
                endpoint_id=endpoint_id,
                spec=self._nexus_endpoint_spec(
                    name,
                    target_namespace_id,
                    target_task_queue,
                    allowed_caller_namespace_ids,
                ),
                resource_version=got.endpoint.resource_version,
            )
        )
        await self._await_operation(
            response.async_operation, label=f"update-nexus-endpoint:{name}"
        )
        got = await self._cs.get_nexus_endpoint(
            GetNexusEndpointRequest(endpoint_id=endpoint_id)
        )
        return self._nexus_endpoint_info(got.endpoint)

    async def delete_nexus_endpoint(self, endpoint_id: str) -> str:
        from temporalio.api.cloud.cloudservice.v1 import (
            DeleteNexusEndpointRequest,
            GetNexusEndpointRequest,
        )

        try:
            got = await self._cs.get_nexus_endpoint(
                GetNexusEndpointRequest(endpoint_id=endpoint_id)
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise

        try:
            response = await self._cs.delete_nexus_endpoint(
                DeleteNexusEndpointRequest(
                    endpoint_id=endpoint_id,
                    resource_version=got.endpoint.resource_version,
                )
            )
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _is_not_found(error):
                return "not-found"
            raise
        await self._await_operation(
            response.async_operation, label=f"delete-nexus-endpoint:{endpoint_id}"
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
            auth_mode=namespace_auth_mode(namespace),
            region=namespace_region(namespace),
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

    def create_json_if_missing(
        self,
        path: str,
        value: dict[str, Any],
        description: str | None = None,
    ) -> bool:
        kwargs: dict[str, Any] = {"Name": path, "SecretString": json.dumps(value)}
        if description:
            kwargs["Description"] = description
        kwargs["Tags"] = [{"Key": "managed-by", "Value": "demo-registry"}]
        try:
            self._client.create_secret(**kwargs)
        except Exception as error:  # noqa: BLE001 - normalize boto error code
            if _error_code(error) == "ResourceExistsException":
                return False
            raise
        return True

    def delete(self, path: str) -> str:
        # Schedule deletion with a recovery window rather than force-deleting, so
        # a credential torn down by mistake can still be restored.
        try:
            self._client.delete_secret(SecretId=path, RecoveryWindowInDays=7)
        except Exception as error:  # noqa: BLE001 - tolerate already-deleted
            if _error_code(error) == "ResourceNotFoundException":
                return "not-found"
            if secret_marked_for_deletion(error):
                return "scheduled-deletion"
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
    force_auth_rotation: bool = False,
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
        force_auth_rotation=force_auth_rotation,
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


# Resources in these states are on their way out and are ignored when looking
# things up (a deleting group or user must not be reused).
GONE_RESOURCE_STATES = {"RESOURCE_STATE_DELETED", "RESOURCE_STATE_DELETING"}


def _resource_state_name(state: int) -> str | None:
    from temporalio.api.cloud.resource.v1 import ResourceState

    try:
        return ResourceState.Name(state) if state else None
    except ValueError:
        return None


_NAMESPACE_PERMISSION_NAMES = {
    "PERMISSION_READ": "read",
    "PERMISSION_WRITE": "write",
    "PERMISSION_ADMIN": "admin",
}


def _namespace_permission_value(permission: str) -> int:
    from temporalio.api.cloud.identity.v1 import NamespaceAccess

    return NamespaceAccess.Permission.Value(f"PERMISSION_{permission.upper()}")


def _user_group_info(group: Any) -> UserGroupInfo:
    from temporalio.api.cloud.identity.v1 import NamespaceAccess

    permissions: dict[str, str] = {}
    for namespace, access in group.spec.access.namespace_accesses.items():
        name = _NAMESPACE_PERMISSION_NAMES.get(
            NamespaceAccess.Permission.Name(access.permission)
        )
        if name:
            permissions[namespace] = name
    return UserGroupInfo(
        group_id=group.id,
        display_name=group.spec.display_name,
        resource_version=group.resource_version,
        namespace_permissions=permissions,
    )


def _is_already_exists(error: Exception) -> bool:
    if "already exists" in str(error).lower():
        return True
    try:
        from temporalio.service import RPCError, RPCStatusCode
    except ImportError:
        return False
    return isinstance(error, RPCError) and error.status == RPCStatusCode.ALREADY_EXISTS


def _is_not_found(error: Exception) -> bool:
    try:
        from temporalio.service import RPCError, RPCStatusCode
    except ImportError:
        return False
    return isinstance(error, RPCError) and error.status == RPCStatusCode.NOT_FOUND


def _is_invalid_resource_version(error: Exception) -> bool:
    lowered = str(error).lower()
    # "invalid resource version" surfaces synchronously from UpdateNamespace;
    # "resource version mismatch" surfaces asynchronously as a failed async
    # operation. Both mean an optimistic-concurrency conflict on the namespace
    # spec and must be retried against a freshly-read resource_version.
    return (
        "invalid resource version" in lowered
        or "resource version mismatch" in lowered
    )


def _is_stale_resource_version(error: Exception) -> bool:
    if _is_invalid_resource_version(error):
        return True
    try:
        from temporalio.service import RPCError, RPCStatusCode
    except ImportError:
        return False
    return isinstance(error, RPCError) and error.status == RPCStatusCode.ABORTED


def _error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        info = response.get("Error")
        if isinstance(info, dict) and isinstance(info.get("Code"), str):
            return info["Code"]
    return None


def cloud_search_attribute_type_value(attribute_type: str) -> int:
    from temporalio.api.cloud.namespace.v1 import NamespaceSpec

    values = NamespaceSpec.SearchAttributeType
    return {
        resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_TEXT: (
            values.SEARCH_ATTRIBUTE_TYPE_TEXT
        ),
        resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_KEYWORD: (
            values.SEARCH_ATTRIBUTE_TYPE_KEYWORD
        ),
        resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_INT: values.SEARCH_ATTRIBUTE_TYPE_INT,
        resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_DOUBLE: (
            values.SEARCH_ATTRIBUTE_TYPE_DOUBLE
        ),
        resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_BOOL: (
            values.SEARCH_ATTRIBUTE_TYPE_BOOL
        ),
        resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_DATETIME: (
            values.SEARCH_ATTRIBUTE_TYPE_DATETIME
        ),
        resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST: (
            values.SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST
        ),
    }[resources.temporal_search_attribute_type(attribute_type)]


def cloud_search_attribute_status_type(spec: Any, name: str) -> str | None:
    value = getattr(spec, "search_attributes", {}).get(name)
    if value is None:
        return None
    return cloud_search_attribute_type_name(value)


def cloud_search_attribute_type_name(value: Any) -> str | None:
    from temporalio.api.cloud.namespace.v1 import NamespaceSpec

    values = NamespaceSpec.SearchAttributeType
    return {
        values.SEARCH_ATTRIBUTE_TYPE_TEXT: resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_TEXT,
        values.SEARCH_ATTRIBUTE_TYPE_KEYWORD: (
            resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_KEYWORD
        ),
        values.SEARCH_ATTRIBUTE_TYPE_INT: resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_INT,
        values.SEARCH_ATTRIBUTE_TYPE_DOUBLE: (
            resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_DOUBLE
        ),
        values.SEARCH_ATTRIBUTE_TYPE_BOOL: (
            resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_BOOL
        ),
        values.SEARCH_ATTRIBUTE_TYPE_DATETIME: (
            resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_DATETIME
        ),
        values.SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST: (
            resources.TEMPORAL_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST
        ),
    }.get(value)


def namespace_auth_mode(namespace: Any) -> str | None:
    spec = getattr(namespace, "spec", None)
    if spec is None:
        return None
    mtls_auth = getattr(spec, "mtls_auth", None)
    if mtls_auth is not None and getattr(mtls_auth, "enabled", False):
        return resources.TEMPORAL_AUTH_MODE_MTLS
    api_key_auth = getattr(spec, "api_key_auth", None)
    if api_key_auth is not None and getattr(api_key_auth, "enabled", False):
        return resources.TEMPORAL_AUTH_MODE_API_KEY
    return None


def namespace_region(namespace: Any) -> str | None:
    # A demo namespace is single-region; NamespaceSpec.regions carries the region
    # the namespace was created in (symmetric with create_namespace's write).
    spec = getattr(namespace, "spec", None)
    if spec is None:
        return None
    regions = list(getattr(spec, "regions", None) or [])
    return regions[0] if regions else None
