from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest
from demo_registry_operator import errors
from demo_registry_operator.activities import namespace_readers as grants
from demo_registry_operator.workflows import crashable_workspace, namespace_readers
from demo_registry_operator.workflows.namespace_readers_messages import (
    GrantNamespaceReadAccessRequest,
)
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError
from temporalio.testing import ActivityEnvironment


def activity_failure(cause, activity_type="grant_user_namespace_read_access"):
    error = ActivityError(
        "Activity task failed",
        scheduled_event_id=10,
        started_event_id=11,
        identity="test-worker",
        activity_type=activity_type,
        activity_id="1",
        retry_state=None,
    )
    error.__cause__ = cause
    return error


def test_grant_invalid_input_fails_before_provider_call(monkeypatch):
    config = SimpleNamespace(
        namespace_readers_group="readers",
        temporal_cloud_mode="cloud-api",
        external_resource_mode="aws",
    )
    monkeypatch.setattr(grants.WorkerConfig, "from_env", lambda: config)
    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(
            grants.grant_user_namespace_read_access,
            GrantNamespaceReadAccessRequest(" "),
        )
    assert raised.value.type == errors.VALIDATION_ERROR
    assert raised.value.non_retryable
    assert (
        raised.value.details[0]["status"]["message"] == "No email address to look up."
    )


@pytest.mark.parametrize(
    ("provider_error", "error_type", "non_retryable"),
    [
        (RuntimeError("permission denied"), errors.PROVIDER_AUTHENTICATION_ERROR, True),
        (
            RuntimeError("invalid group configuration"),
            errors.PROVIDER_CONFIGURATION_ERROR,
            True,
        ),
        (TimeoutError("Cloud Ops timed out"), errors.PROVIDER_TRANSIENT_ERROR, False),
    ],
)
def test_grant_provider_failures_are_native_and_keep_retry_classification(
    monkeypatch, provider_error, error_type, non_retryable
):
    config = SimpleNamespace(
        namespace_readers_group="readers",
        temporal_cloud_mode="cloud-api",
        external_resource_mode="aws",
    )
    monkeypatch.setattr(grants.WorkerConfig, "from_env", lambda: config)
    monkeypatch.setattr(
        grants.cloud_activities,
        "temporal_bootstrap_secret",
        lambda *_: {"TEMPORAL_CLOUD_API_KEY": "test"},
    )

    async def reconciler(*_):
        raise provider_error

    monkeypatch.setattr(grants.cloud_activities, "cloud_reconciler", reconciler)
    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(
            grants.grant_user_namespace_read_access,
            GrantNamespaceReadAccessRequest(" user@temporal.io "),
        )
    error = raised.value
    assert error.type == error_type
    assert error.non_retryable is non_retryable
    assert error.details[0]["status"]["email"] == "user@temporal.io"
    assert error.details[0]["status"]["group"] == "readers"
    assert str(provider_error) in error.details[0]["status"]["message"]


@pytest.mark.parametrize("structured", [False, True])
def test_grant_workflow_recovers_readable_result_with_activity_identity(
    monkeypatch, structured
):
    details = (
        (
            {
                "status": {
                    "email": "user@temporal.io",
                    "group": "readers",
                    "message": "grant denied",
                }
            },
        )
        if structured
        else ()
    )
    error = activity_failure(
        ApplicationError("grant denied", *details, type="GrantDenied")
    )

    async def execute(*_, **__):
        raise error

    monkeypatch.setattr(namespace_readers.workflow, "execute_activity", execute)
    result = asyncio.run(
        namespace_readers.GrantNamespaceReadAccessWorkflow().run(
            GrantNamespaceReadAccessRequest(" user@temporal.io ")
        )
    )
    assert result.status == "error"
    assert result.email == "user@temporal.io"
    assert result.message == "grant denied"
    assert result.group == ("readers" if structured else None)
    assert result.activity_failure == {
        "message": "grant denied",
        "type": "GrantDenied",
        "activityType": "grant_user_namespace_read_access",
        "activityId": "1",
        "scheduledEventId": 10,
    }


def test_grant_preserves_cancellation(monkeypatch):
    error = activity_failure(CancelledError("cancelled"))

    async def execute(*_, **__):
        raise error

    monkeypatch.setattr(namespace_readers.workflow, "execute_activity", execute)
    with pytest.raises(ActivityError) as raised:
        asyncio.run(
            namespace_readers.GrantNamespaceReadAccessWorkflow().run(
                GrantNamespaceReadAccessRequest("user@temporal.io")
            )
        )
    assert raised.value is error


def test_workspace_compensations_continue_in_reverse_order_and_retain_cleanup_error(
    monkeypatch,
):
    calls = []

    async def first():
        calls.append("first")

    async def last():
        calls.append("last")
        raise activity_failure(
            ApplicationError("delete denied", type="DeleteDenied"),
            "delete_workspace_namespace",
        )

    monkeypatch.setattr(
        crashable_workspace.workflow, "logger", logging.getLogger(__name__)
    )
    failures = asyncio.run(crashable_workspace._run_compensations([first, last]))
    assert calls == ["last", "first"]
    assert failures == [
        {
            "message": "delete denied",
            "type": "DeleteDenied",
            "activityType": "delete_workspace_namespace",
            "activityId": "1",
            "scheduledEventId": 10,
        }
    ]
