"""Activities behind the namespace readers user group.

Every operator-managed demo namespace grants Read to one Temporal Cloud user
group (``WorkerConfig.namespace_readers_group``). Demo reconciles keep each new
namespace covered and namespace deletes remove the grant; these activities do the
account-wide parts:

- ``sync_namespace_readers_group`` backfills exactly Read on every namespace
  carrying the operator's owner tag (the group is created once by an account
  admin; the operator never creates groups or changes account roles).
- ``grant_user_namespace_read_access`` is the catalog's self-service action: it
  adds the signed-in user, looked up by email, to the group.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import activity
from temporalio.common import RetryPolicy

from demo_registry_operator import errors, resources
from demo_registry_operator.activities import temporal_cloud
from demo_registry_operator.activities.external_resources import (
    temporal as cloud_activities,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.workflows.namespace_readers_messages import (
    GrantNamespaceReadAccessRequest,
    GrantNamespaceReadAccessResult,
    SyncNamespaceReadersRequest,
    SyncNamespaceReadersResult,
)

# Someone is waiting on the catalog for this, so fail fast.
GRANT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
SYNC_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=6,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


def not_configured_reason(config: WorkerConfig) -> str | None:
    if not config.namespace_readers_group:
        return "The operator has no namespace readers group configured."
    if config.temporal_cloud_mode != "cloud-api" or config.external_resource_mode != "aws":
        return "The operator is not managing Temporal Cloud through the Cloud Ops API."
    return None


@activity.defn
def grant_user_namespace_read_access(
    request: GrantNamespaceReadAccessRequest,
) -> GrantNamespaceReadAccessResult:
    email = request.email.strip()
    config = WorkerConfig.from_env()
    group = config.namespace_readers_group or None
    reason = not_configured_reason(config)
    if reason:
        return GrantNamespaceReadAccessResult(
            status="not-configured", email=email, group=group, message=reason
        )
    if not email:
        return GrantNamespaceReadAccessResult(
            status="error", email=email, group=group, message="No email address to look up."
        )

    checked_at = datetime.now(UTC).isoformat()
    bootstrap = cloud_activities.temporal_bootstrap_secret(config, checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return GrantNamespaceReadAccessResult(
            status="not-configured",
            email=email,
            group=group,
            message="The operator has no Temporal Cloud API key.",
        )

    async def run() -> dict[str, Any]:
        reconciler = await cloud_activities.cloud_reconciler(config, bootstrap)
        return await temporal_cloud.grant_user_namespace_read_access(
            reconciler,
            email=email,
            group_name=config.namespace_readers_group,
            owner_tag=resources.namespace_owner_tag(),
        )

    try:
        outcome = asyncio.run(run())
    except Exception as error:  # noqa: BLE001
        app_error = cloud_activities.temporal_cloud_application_error(
            error, operation="Grant namespace read access"
        )
        # Permanent failures (e.g. the operator's key may not manage groups) go
        # back to the catalog as a readable message instead of a failed workflow.
        if getattr(app_error, "non_retryable", False):
            return GrantNamespaceReadAccessResult(
                status="error", email=email, group=group, message=str(app_error)
            )
        raise app_error from error
    return GrantNamespaceReadAccessResult(
        status=str(outcome.get("status")),
        email=email,
        group=group,
        message=outcome.get("message"),
    )


@activity.defn
def sync_namespace_readers_group(
    request: SyncNamespaceReadersRequest,
) -> SyncNamespaceReadersResult:
    config = WorkerConfig.from_env()
    checked_at = datetime.now(UTC).isoformat()
    reason = not_configured_reason(config)
    if reason:
        return SyncNamespaceReadersResult(
            status={"phase": "NotRequired", "checkedAt": checked_at, "message": reason}
        )
    bootstrap = cloud_activities.temporal_bootstrap_secret(config, checked_at)
    if not bootstrap.get("TEMPORAL_CLOUD_API_KEY"):
        return SyncNamespaceReadersResult(status=bootstrap)

    async def run() -> dict[str, Any]:
        reconciler = await cloud_activities.cloud_reconciler(config, bootstrap)
        return await temporal_cloud.sync_namespace_readers_group(
            reconciler,
            group_name=config.namespace_readers_group,
            owner_tag=resources.namespace_owner_tag(),
        )

    try:
        result = asyncio.run(run())
    except Exception as error:  # noqa: BLE001
        raise cloud_activities.temporal_cloud_application_error(
            error, operation="Sync namespace readers group"
        ) from error
    phase = "GroupMissing" if result["group"]["action"] == "missing" else "Ready"
    return SyncNamespaceReadersResult(
        status={"phase": phase, "checkedAt": checked_at, "reason": request.reason, **result}
    )
