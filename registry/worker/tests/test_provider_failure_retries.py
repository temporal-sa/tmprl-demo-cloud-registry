from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from demo_registry_operator import errors
from demo_registry_operator.activities import instruqt
from demo_registry_operator.activities.external_resources import aws_lambda, temporal
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode


@pytest.mark.parametrize(
    "status",
    [
        RPCStatusCode.INTERNAL,
        RPCStatusCode.ABORTED,
        RPCStatusCode.RESOURCE_EXHAUSTED,
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.UNAVAILABLE,
        RPCStatusCode.UNKNOWN,
    ],
)
def test_cloud_rpc_status_controls_retries_even_when_message_mentions_auth(status):
    error = temporal.temporal_cloud_application_error(
        RPCError("authentication service failed", status, b""),
        operation="Create namespace",
    )
    assert error.type == errors.PROVIDER_TRANSIENT_ERROR
    assert not error.non_retryable


@pytest.mark.parametrize(
    "cause",
    [
        TimeoutError("authentication request timed out"),
        RuntimeError("authentication service temporarily unavailable"),
        RuntimeError("unexpected downstream fault"),
        EndpointConnectionError(endpoint_url="https://secretsmanager.example"),
        ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
            "PutSecretValue",
        ),
    ],
)
def test_cloud_transport_and_unknown_provider_failures_retry(cause):
    error = temporal.temporal_cloud_application_error(
        cause, operation="Publish credentials"
    )
    assert error.type == errors.PROVIDER_TRANSIENT_ERROR
    assert not error.non_retryable


@pytest.mark.parametrize(
    "status,error_type",
    [
        (RPCStatusCode.UNAUTHENTICATED, errors.PROVIDER_AUTHENTICATION_ERROR),
        (RPCStatusCode.PERMISSION_DENIED, errors.PROVIDER_AUTHENTICATION_ERROR),
        (RPCStatusCode.INVALID_ARGUMENT, errors.PROVIDER_CONFIGURATION_ERROR),
        (RPCStatusCode.FAILED_PRECONDITION, errors.PROVIDER_CONFIGURATION_ERROR),
        (RPCStatusCode.NOT_FOUND, errors.PROVIDER_CONFIGURATION_ERROR),
        (RPCStatusCode.UNIMPLEMENTED, errors.PROVIDER_CONFIGURATION_ERROR),
    ],
)
def test_cloud_permanent_rpc_status_does_not_depend_on_message(status, error_type):
    error = temporal.temporal_cloud_application_error(
        RPCError("request unavailable", status, b""), operation="Update namespace"
    )
    assert error.type == error_type
    assert error.non_retryable


@pytest.mark.parametrize("non_retryable", [False, True])
def test_cloud_keeps_explicit_application_error_and_diagnostics(non_retryable):
    cause = ApplicationError(
        "original failure",
        {"status": {"phase": "Error"}},
        type="OriginalFailure",
        non_retryable=non_retryable,
    )
    assert (
        temporal.temporal_cloud_application_error(cause, operation="Reconcile") is cause
    )


class MissingSecret:
    def get_secret_value(self, **kwargs):
        raise ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
            "GetSecretValue",
        )

    def describe_secret(self, **kwargs):
        raise ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "missing"}},
            "DescribeSecret",
        )


def test_new_runtime_secret_read_retries():
    with pytest.raises(ApplicationError) as raised:
        aws_lambda.read_json_secret(MissingSecret(), "managed/temporal")
    assert raised.value.type == errors.PROVIDER_TRANSIENT_ERROR
    assert not raised.value.non_retryable


@pytest.mark.parametrize("managed", [False, True])
def test_missing_managed_secret_retries_but_user_supplied_secret_is_terminal(managed):
    with pytest.raises(ApplicationError) as raised:
        aws_lambda.describe_secret_reference(
            MissingSecret(), "project/secret", retry_if_missing=managed
        )
    assert raised.value.non_retryable is not managed
    assert raised.value.type == (
        errors.PROVIDER_TRANSIENT_ERROR if managed else errors.CONFIGURATION_ERROR
    )


def test_lambda_creation_retries_new_runtime_secret_visibility(monkeypatch):
    monkeypatch.setattr(
        aws_lambda.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(aws_region="us-west-1"),
    )
    monkeypatch.setattr(
        boto3.session,
        "Session",
        lambda **kwargs: SimpleNamespace(client=lambda name: MissingSecret()),
    )
    with pytest.raises(ApplicationError) as raised:
        aws_lambda.ensure_lambda_function(
            SimpleNamespace(temporal_secret_path="managed/temporal")
        )
    assert raised.value.type == errors.PROVIDER_TRANSIENT_ERROR
    assert not raised.value.non_retryable


@pytest.mark.parametrize("value", ["not-json", "[]"])
def test_invalid_runtime_secret_stays_terminal(value):
    secret = SimpleNamespace(get_secret_value=lambda **kwargs: {"SecretString": value})
    with pytest.raises(ApplicationError) as raised:
        aws_lambda.read_json_secret(secret, "managed/temporal")
    assert raised.value.type == errors.CONFIGURATION_ERROR
    assert raised.value.non_retryable


@pytest.mark.parametrize(
    "fn,request_type,phase",
    [
        (
            instruqt.push_instruqt_course_track,
            instruqt.InstruqtCourseArtifactRequest,
            "PushFailed",
        ),
        (
            instruqt.test_instruqt_course_track,
            instruqt.InstruqtCourseArtifactRequest,
            "TestFailed",
        ),
        (
            instruqt.deploy_instruqt_course_track,
            instruqt.InstruqtTrackDeployRequest,
            "DeployFailed",
        ),
    ],
)
def test_instruqt_temporary_cli_failure_retries_and_preserves_status(
    monkeypatch, fn, request_type, phase
):
    monkeypatch.setattr(
        instruqt,
        "run_instruqt_track_operation",
        lambda *args, **kwargs: {
            "phase": "Ran",
            "channel": "production",
            "target": "example/course",
            "artifact": {"phase": "Rendered"},
            "command": {
                "phase": "Failed",
                "exitCode": 1,
                "output": "HTTP 503 Service Unavailable",
            },
        },
    )
    with pytest.raises(ApplicationError) as raised:
        fn(request_type({}))
    assert raised.value.type == errors.PROVIDER_TRANSIENT_ERROR
    assert not raised.value.non_retryable
    assert "HTTP 503" in raised.value.message
    assert raised.value.details[0]["status"]["phase"] == phase


def test_instruqt_delete_temporary_failure_retries(monkeypatch):
    monkeypatch.setattr(
        instruqt, "instruqt_track_target", lambda *args: "example/course"
    )
    monkeypatch.setattr(
        instruqt,
        "run_instruqt_command",
        lambda *args, **kwargs: {"phase": "Failed", "output": "HTTP 503"},
    )
    with pytest.raises(ApplicationError) as raised:
        instruqt.delete_instruqt_course_track(instruqt.InstruqtTrackDeleteRequest({}))
    assert not raised.value.non_retryable
    assert raised.value.details[0]["status"]["phase"] == "DeleteFailed"


@pytest.mark.parametrize(
    "cause,retryable",
    [
        (
            subprocess.CalledProcessError(
                128, ["git", "clone"], stderr="Could not resolve host: github.com"
            ),
            True,
        ),
        (ValueError("Instruqt track.yml is required"), False),
    ],
)
def test_instruqt_artifact_errors_distinguish_transport_from_invalid_track(
    monkeypatch, tmp_path, cause, retryable
):
    def clone(*args, **kwargs):
        raise cause

    monkeypatch.setattr(instruqt, "clone_course_source", clone)
    status = instruqt.prepare_managed_instruqt_artifact(
        {}, tmp_path, {}, validate=False
    )
    monkeypatch.setattr(
        instruqt, "run_instruqt_track_operation", lambda *args, **kwargs: status
    )
    with pytest.raises(ApplicationError) as raised:
        instruqt.push_instruqt_course_track(instruqt.InstruqtCourseArtifactRequest({}))
    assert raised.value.non_retryable is not retryable
    assert raised.value.details[0]["status"]["reason"] == "ArtifactPreparationFailed"


def test_instruqt_command_timeout_retries_with_command_diagnostics(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(
        instruqt,
        "instruqt_cli_status",
        lambda: {"ready": True, "executable": "instruqt"},
    )

    def run(*args, **kwargs):
        raise subprocess.TimeoutExpired("instruqt track push", 300)

    monkeypatch.setattr(instruqt.subprocess, "run", run)
    command = instruqt.run_instruqt_command(
        Path(tmp_path), ["track", "push", "--force"], timeout_seconds=300
    )
    monkeypatch.setattr(
        instruqt,
        "run_instruqt_track_operation",
        lambda *args, **kwargs: {
            "phase": "Ran",
            "channel": "production",
            "target": "example/course",
            "artifact": {},
            "command": command,
        },
    )
    with pytest.raises(ApplicationError) as raised:
        instruqt.push_instruqt_course_track(instruqt.InstruqtCourseArtifactRequest({}))
    assert not raised.value.non_retryable
    assert raised.value.details[0]["status"]["push"]["command"][-1] == "--force"


def test_instruqt_cli_preflight_transport_failure_retries(monkeypatch, tmp_path):
    monkeypatch.setattr(instruqt.shutil, "which", lambda name: "instruqt")
    monkeypatch.setenv("INSTRUQT_TOKEN", "test")
    monkeypatch.setattr(
        instruqt.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="HTTP 503 Service Unavailable"
        ),
    )
    command = instruqt.run_instruqt_command(
        tmp_path, ["track", "push", "--force"], timeout_seconds=300
    )
    monkeypatch.setattr(
        instruqt,
        "run_instruqt_track_operation",
        lambda *args, **kwargs: {
            "phase": "Ran",
            "channel": "production",
            "target": "example/course",
            "artifact": {},
            "command": command,
        },
    )
    with pytest.raises(ApplicationError) as raised:
        instruqt.push_instruqt_course_track(instruqt.InstruqtCourseArtifactRequest({}))
    assert not raised.value.non_retryable
    assert "HTTP 503" in raised.value.message
