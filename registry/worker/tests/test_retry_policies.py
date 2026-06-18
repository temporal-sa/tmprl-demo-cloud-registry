from __future__ import annotations

from datetime import timedelta

from demo_registry_operator.activities import external_resources
from demo_registry_operator.errors import (
    NON_RETRYABLE_ERROR_TYPES,
    PROVIDER_AUTHENTICATION_ERROR,
    PROVIDER_CONFIGURATION_ERROR,
    PROVIDER_TRANSIENT_ERROR,
    aws_application_error,
)
from demo_registry_operator.workflows import demo_project, demo_project_runtime


class FakeAwsError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def test_retry_policies_include_non_retryable_error_types():
    policies = [
        demo_project.COMMON_RETRY_POLICY,
        demo_project.STATUS_RETRY_POLICY,
        demo_project.CHECK_RETRY_POLICY,
        demo_project_runtime.COMMON_RETRY_POLICY,
        demo_project_runtime.BUILD_WAIT_RETRY_POLICY,
        demo_project_runtime.CHECK_RETRY_POLICY,
    ]

    for policy in policies:
        assert policy.non_retryable_error_types == NON_RETRYABLE_ERROR_TYPES


def test_temporal_runtime_candidate_canary_has_rollout_sized_timeouts():
    assert (
        demo_project.TEMPORAL_RUNTIME_CANDIDATE_CANARY_START_TO_CLOSE_TIMEOUT
        >= timedelta(minutes=10)
    )
    assert (
        demo_project.TEMPORAL_RUNTIME_CANDIDATE_CANARY_HEARTBEAT_TIMEOUT
        >= timedelta(minutes=2)
    )


def test_aws_access_denied_is_non_retryable_provider_authentication_error():
    error = aws_application_error(
        FakeAwsError("AccessDeniedException"),
        operation="Call AWS",
    )

    assert error.type == PROVIDER_AUTHENTICATION_ERROR
    assert error.non_retryable is True


def test_unknown_aws_error_is_retryable_provider_transient_error():
    error = aws_application_error(
        FakeAwsError("InternalFailure"),
        operation="Call AWS",
    )

    assert error.type == PROVIDER_TRANSIENT_ERROR
    assert error.non_retryable is False


def test_temporal_cloud_cleanup_timeout_is_retryable_provider_error():
    error = external_resources.temporal_cloud_application_error(
        TimeoutError("timed out waiting for namespace delete"),
        operation="Temporal Cloud teardown",
    )

    assert error.type == PROVIDER_TRANSIENT_ERROR
    assert error.non_retryable is False


def test_temporal_cloud_runtime_canary_timeout_is_retryable_provider_error():
    error = external_resources.temporal_cloud_application_error(
        TimeoutError(
            "Temporal runtime credential canary did not pass within "
            "60 seconds after 4 attempts."
        ),
        operation="Temporal runtime candidate canary",
    )

    assert error.type == PROVIDER_TRANSIENT_ERROR
    assert error.non_retryable is False


def test_temporal_cloud_cleanup_validation_error_is_terminal():
    error = external_resources.temporal_cloud_application_error(
        RuntimeError("invalid namespace retention policy"),
        operation="Temporal Cloud teardown",
    )

    assert error.type == PROVIDER_CONFIGURATION_ERROR
    assert error.non_retryable is True
