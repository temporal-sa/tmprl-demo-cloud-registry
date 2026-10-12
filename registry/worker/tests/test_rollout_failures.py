from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.error import HTTPError

import boto3
import pytest
from demo_registry_operator import errors
from demo_registry_operator.activities import reconcile
from demo_registry_operator.activities.builds.codebuild import execution
from demo_registry_operator.workflows import demo_project, messages
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError
from temporalio.testing import ActivityEnvironment

PROJECT = {"metadata": {"name": "demo"}}


def wrap_activity_error(cause: BaseException) -> ActivityError:
    error = ActivityError(
        "Activity task failed",
        scheduled_event_id=10,
        started_event_id=11,
        identity="test-worker",
        activity_type="failed_gate",
        activity_id="gate-1",
        retry_state=None,
    )
    error.__cause__ = cause
    return error


def expire_polling_window(monkeypatch):
    times = iter([0, 121])
    monkeypatch.setattr(reconcile.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(
        reconcile.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(readiness_timeout_seconds=30, readiness_poll_seconds=1),
    )


def test_smoke_failure_is_non_retryable_and_keeps_http_details(monkeypatch):
    expire_polling_window(monkeypatch)

    def failed_http(*args, **kwargs):
        raise HTTPError("https://demo/health", 503, "Unavailable", {}, None)

    monkeypatch.setattr(reconcile, "urlopen", failed_http)
    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(
            reconcile.run_smoke_check,
            reconcile.SmokeCheckRequest(
                project="demo",
                host="demo",
                smoke_check={"path": "/health", "expectedStatus": 200},
            ),
        )

    assert raised.value.type == errors.SMOKE_CHECK_FAILED
    assert raised.value.non_retryable
    assert "HTTP 503" in raised.value.message
    status = raised.value.details[0]["status"]
    assert status["actualStatus"] == 503
    assert status["expectedStatus"] == 200
    assert status["ok"] is False


def test_readiness_timeout_is_a_failed_activity_with_deployment_details(monkeypatch):
    expire_polling_window(monkeypatch)
    deployments = {
        "worker": {"name": "worker-new", "ready": False, "availableReplicas": 0}
    }
    monkeypatch.setattr(
        reconcile, "read_candidate_deployments", lambda *args: deployments
    )

    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(
            reconcile.wait_for_candidate_readiness,
            reconcile.CandidateReadinessRequest(
                PROJECT, {"deployments": {"worker": "worker-new"}}
            ),
        )

    assert raised.value.type == errors.CANDIDATE_NOT_READY
    assert raised.value.non_retryable
    assert raised.value.details[0]["status"]["deployments"] == deployments


@pytest.mark.parametrize("timed_out", [False, True])
def test_worker_rollout_failure_is_a_failed_activity(monkeypatch, timed_out):
    expire_polling_window(monkeypatch)
    status = {
        "name": "worker-new",
        "conditions": [
            {
                "type": "Ready",
                "status": "False",
                "reason": "Waiting" if timed_out else "InvalidSpec",
            }
        ],
    }
    monkeypatch.setattr(
        reconcile, "read_worker_deployments", lambda *args: {"worker": status}
    )

    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(
            reconcile.wait_for_worker_deployment_rollouts,
            reconcile.WorkerDeploymentRolloutRequest(
                PROJECT, {"worker": "worker-new"}, timeout_seconds=30
            ),
        )

    assert raised.value.type == errors.WORKER_ROLLOUT_FAILED
    assert raised.value.non_retryable
    payload = raised.value.details[0]["status"]
    assert payload["phase"] == ("TimedOut" if timed_out else "Failed")
    assert payload["resources"] == [status]


def test_failed_codebuild_activity_keeps_log_references(monkeypatch):
    config = SimpleNamespace(
        aws_region="us-west-1",
        build_mode="aws-codebuild",
        codebuild_project="build-demo",
    )
    monkeypatch.setattr(execution.WorkerConfig, "from_env", lambda: config)
    monkeypatch.setattr(
        boto3.session,
        "Session",
        lambda **kwargs: SimpleNamespace(client=lambda *args, **kwargs: object()),
    )
    failed_build = {
        "id": "build-demo:123",
        "buildStatus": "FAILED",
        "resolvedSourceVersion": "a" * 40,
        "logs": {"deepLink": "https://console.aws.amazon.com/logs/demo"},
    }
    monkeypatch.setattr(
        execution, "wait_for_codebuild_build", lambda *args, **kwargs: failed_build
    )
    plan = {
        "demoProject": {
            **PROJECT,
            "spec": {
                "builds": {"worker": {"context": ".", "dockerfile": "Dockerfile"}}
            },
        },
        "startedAt": "2026-10-10T00:00:00Z",
        "imageTemplates": {"worker": "registry/worker:{sourceCommitSha}"},
        "sourceVersion": "a" * 40,
        "codebuildBuildId": "build-demo:123",
    }

    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(
            execution.wait_for_project_image_build,
            execution.WaitForProjectImageBuildRequest(plan),
        )

    assert raised.value.type == errors.IMAGE_BUILD_FAILED
    assert raised.value.non_retryable
    status = raised.value.details[0]["status"]
    assert status["phase"] == "Error"
    assert status["codebuild"]["buildId"] == "build-demo:123"
    assert status["codebuild"]["logs"]["deepLink"] == failed_build["logs"]["deepLink"]


def test_workflow_recovers_status_without_losing_activity_error_location():
    status = {
        "ok": False,
        "phase": "TimedOut",
        "deployments": {"worker": {"ready": False}},
    }
    error = wrap_activity_error(
        errors.terminal_activity_error(status, error_type=errors.CANDIDATE_NOT_READY)
    )
    recovered = errors.activity_failure_status(error)
    assert recovered["deployments"] == status["deployments"]
    assert recovered["activityFailure"]["activityId"] == "gate-1"
    assert recovered["activityFailure"]["scheduledEventId"] == 10


def test_workflow_does_not_convert_unexpected_errors_into_gate_results():
    error = wrap_activity_error(ApplicationError("unexpected provider failure"))
    with pytest.raises(ActivityError) as raised:
        errors.activity_failure_status(error)
    assert raised.value is error


def test_exhausted_gate_failure_enters_cleanup_with_its_original_error():
    error = wrap_activity_error(ApplicationError("provider retries exhausted"))
    status = errors.activity_failure_status(error, fallback_phase="Error")
    assert status["ok"] is False
    assert status["message"] == "provider retries exhausted"
    assert status["activityFailure"]["activityId"] == "gate-1"


def test_cancelled_gate_is_not_treated_as_a_rollout_failure():
    error = wrap_activity_error(CancelledError("cancelled"))
    with pytest.raises(ActivityError) as raised:
        errors.activity_failure_status(error, fallback_phase="Error")
    assert raised.value is error


@pytest.mark.parametrize(
    "established,compensation_fails", [(False, False), (False, True), (True, False)]
)
def test_entity_records_failure_and_stays_available(
    monkeypatch, established, compensation_fails
):
    entity = demo_project.DemoProjectWorkflow(
        messages.DemoProjectRunRequest(project_name="demo")
    )
    entity._compensate_first_time_external_resources = AsyncMock(
        side_effect=wrap_activity_error(ApplicationError("cleanup denied"))
        if compensation_fails
        else None,
    )
    entity._record_status = AsyncMock()
    project = {
        **PROJECT,
        "status": {"activeImageVersion": "git-old"} if established else {},
    }
    failure = wrap_activity_error(
        ApplicationError("deployment rejected", type="InvalidSpec")
    )

    result = asyncio.run(entity._handle_reconcile_failure("demo", project, failure))

    assert result is True
    assert entity._status["phase"] == "WorkerRolloutFailed"
    assert entity._status["message"] == "deployment rejected"
    assert entity._status["rolloutFailure"]["type"] == "InvalidSpec"
    assert entity._status["compensationStatus"]["phase"] == (
        "NotRequired" if established else "Error" if compensation_fails else "Completed"
    )
    entity._record_status.assert_awaited_once()
    if established:
        entity._compensate_first_time_external_resources.assert_not_awaited()
        assert entity._status["activeImageVersion"] == "git-old"
    else:
        entity._compensate_first_time_external_resources.assert_awaited_once()
    if compensation_fails:
        assert (
            entity._status["compensationStatus"]["failure"]["message"]
            == "cleanup denied"
        )
