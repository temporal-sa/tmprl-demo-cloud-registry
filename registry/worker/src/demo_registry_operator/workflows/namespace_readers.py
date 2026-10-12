"""Standalone workflows for the namespace readers user group.

Both are short-lived: each runs one activity (see activities/namespace_readers.py).
``GrantNamespaceReadAccessWorkflow`` is started per click by the catalog web
server; ``SyncNamespaceReadersWorkflow`` is started once per operator activation
under a fixed, cluster-agnostic id, so concurrent starts share a single run.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, is_cancelled_exception

from demo_registry_operator import errors
from demo_registry_operator.activities import namespace_readers as activities
from demo_registry_operator.workflows.namespace_readers_messages import (
    GrantNamespaceReadAccessRequest,
    GrantNamespaceReadAccessResult,
    SyncNamespaceReadersRequest,
    SyncNamespaceReadersResult,
)


@workflow.defn
class GrantNamespaceReadAccessWorkflow:
    """Self-service: add one user to the namespace readers group."""

    @workflow.run
    async def run(
        self, request: GrantNamespaceReadAccessRequest
    ) -> GrantNamespaceReadAccessResult:
        try:
            return await workflow.execute_activity(
                activities.grant_user_namespace_read_access,
                request,
                start_to_close_timeout=timedelta(minutes=3),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=activities.GRANT_RETRY_POLICY,
            )
        except ActivityError as error:
            if is_cancelled_exception(error):
                raise
            status = errors.activity_failure_status(error, fallback_phase="Error")
            return GrantNamespaceReadAccessResult(
                status="error",
                email=status.get("email") or request.email.strip(),
                group=status.get("group"),
                message=status.get("message"),
                activity_failure=status["activityFailure"],
            )


@workflow.defn
class SyncNamespaceReadersWorkflow:
    """Backfill exactly Read for the readers group on every demo namespace."""

    @workflow.run
    async def run(
        self, request: SyncNamespaceReadersRequest
    ) -> SyncNamespaceReadersResult:
        return await workflow.execute_activity(
            activities.sync_namespace_readers_group,
            request,
            start_to_close_timeout=timedelta(minutes=20),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=activities.SYNC_RETRY_POLICY,
        )
