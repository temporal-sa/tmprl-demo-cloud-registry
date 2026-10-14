from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import boto3
import pytest
from botocore.exceptions import ClientError
from demo_registry_operator import errors, resources
from demo_registry_operator.activities import canary, instruqt
from demo_registry_operator.workflows import (
    instruqt_course,
    messages,
    operator_auth_canary,
    operator_canary,
)
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import ActivityEnvironment

COURSE = {
    "metadata": {
        "name": "course",
        "annotations": {
            resources.SOURCE_COMMIT_ANNOTATION: "a" * 40,
            resources.SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:abc",
        },
    },
    "spec": {
        "source": {"repo": "https://github.com/example/course", "branch": "main"},
        "track": {"organization": "example", "slug": "course"},
    },
}


@pytest.fixture(autouse=True)
def workflow_logger(monkeypatch):
    monkeypatch.setattr(
        operator_auth_canary.workflow, "logger", logging.getLogger("test-canary")
    )


READY_CLI = {"phase": "Ready", "ready": True}


def canary_plan(**overrides):
    return {
        "project": "canary",
        "namespace": "canary-ns",
        "demoProject": {},
        "authMode": "apiKey",
        "targetDeploymentName": "operator",
        "targetBuildID": "new",
        "gitRepository": "canary-source",
        "sourceWatchNamespace": "operator",
        "catalogStatusNamespace": "operator",
        "catalogStatusMirrorNamespace": "operator",
        "catalogStatusConfigMap": "catalog",
        "timeoutSeconds": 0,
        "pollSeconds": 1,
        "cleanupTimeoutSeconds": 0,
        **overrides,
    }


def activity_error(name, message="provider denied the request"):
    error = ActivityError(
        "Activity task failed",
        scheduled_event_id=10,
        started_event_id=11,
        identity="test",
        activity_type=name,
        activity_id="activity-1",
        retry_state=None,
    )
    error.__cause__ = errors.terminal_activity_error(
        {"phase": "Failed", "ok": False, "message": message},
        error_type="ProviderDenied",
    )
    return error


@pytest.mark.parametrize(
    "fn,phase",
    [
        (instruqt.push_instruqt_course_track, "PushFailed"),
        (instruqt.test_instruqt_course_track, "TestFailed"),
        (instruqt.deploy_instruqt_course_track, "DeployFailed"),
    ],
)
def test_instruqt_cli_failures_raise_with_command_output(monkeypatch, fn, phase):
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
                "message": "instruqt exited with 1",
                "output": "permission denied",
                "exitCode": 1,
            },
        },
    )
    request = (
        instruqt.InstruqtTrackDeployRequest(COURSE)
        if fn is instruqt.deploy_instruqt_course_track
        else instruqt.InstruqtCourseArtifactRequest(COURSE)
    )
    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(fn, request)
    assert raised.value.non_retryable
    assert "permission denied" in raised.value.message
    assert raised.value.details[0]["status"]["phase"] == phase


def test_instruqt_artifact_preparation_failure_raises(monkeypatch):
    monkeypatch.setattr(
        instruqt,
        "run_instruqt_track_operation",
        lambda *args, **kwargs: {
            "phase": "Error",
            "reason": "ArtifactPreparationFailed",
            "message": "source checkout failed",
        },
    )
    with pytest.raises(ApplicationError, match="source checkout failed"):
        instruqt.push_instruqt_course_track(
            instruqt.InstruqtCourseArtifactRequest(COURSE)
        )


def test_instruqt_missing_build_configuration_raises(monkeypatch):
    monkeypatch.setattr(
        instruqt.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(codebuild_project=None, build_mode="aws-codebuild"),
    )
    course = {
        **COURSE,
        "spec": {
            **COURSE["spec"],
            "images": {"workstation": {"build": {"context": "."}}},
        },
    }
    with pytest.raises(ApplicationError) as raised:
        instruqt.resolve_instruqt_course_image_build_plan(
            instruqt.InstruqtCourseImageBuildPlanRequest(course)
        )
    assert raised.value.type == errors.IMAGE_BUILD_FAILED
    assert raised.value.details[0]["status"]["phase"] == "Error"


def test_instruqt_codebuild_failure_keeps_build_logs(monkeypatch):
    monkeypatch.setattr(
        instruqt.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(
            aws_region="us-west-1",
            codebuild_project="images",
            build_mode="aws-codebuild",
        ),
    )
    monkeypatch.setattr(
        boto3.session,
        "Session",
        lambda **kwargs: SimpleNamespace(client=lambda *args, **kwargs: object()),
    )
    monkeypatch.setattr(
        instruqt,
        "wait_for_instruqt_codebuild_build",
        lambda *args, **kwargs: {
            "id": "images:failed",
            "buildStatus": "FAILED",
            "currentPhase": "BUILD",
            "logs": {"deepLink": "https://logs.example/failed"},
            "phases": [
                {
                    "phaseType": "BUILD",
                    "phaseStatus": "FAILED",
                    "contexts": [{"message": "docker build failed"}],
                }
            ],
        },
    )
    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(
            instruqt.wait_for_instruqt_course_image_build,
            instruqt.InstruqtCourseImageBuildWaitRequest(
                {
                    "instruqtCourse": COURSE,
                    "startedAt": "now",
                    "channel": "production",
                    "imageTemplates": {"workstation": "test:${IMAGE_VERSION}"},
                    "codebuildBuildId": "images:failed",
                }
            ),
        )
    status = raised.value.details[0]["status"]
    assert "docker build failed" in raised.value.message
    assert "https://logs.example/failed" in str(status["codebuild"])


def test_instruqt_repository_cleanup_attempts_all_and_raises(monkeypatch):
    calls = []

    def delete(**kwargs):
        calls.append(kwargs["repositoryName"])
        if len(calls) == 1:
            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                "DeleteRepository",
            )

    monkeypatch.setattr(
        boto3.session,
        "Session",
        lambda **kwargs: SimpleNamespace(
            client=lambda *args: SimpleNamespace(delete_repository=delete)
        ),
    )
    course = {
        **COURSE,
        "spec": {**COURSE["spec"], "images": {"first": {}, "second": {}}},
    }
    with pytest.raises(ApplicationError, match="denied") as raised:
        instruqt.delete_instruqt_course_ecr_repositories(
            instruqt.InstruqtCourseFinalizationPlan({"instruqtCourse": course})
        )
    assert len(calls) == 2
    assert [r["action"] for r in raised.value.details[0]["status"]["repositories"]] == [
        "error",
        "deleted",
    ]


@pytest.mark.parametrize("stage", ["promotion", "rotation", "cleanup"])
def test_canary_deadline_is_a_terminal_activity_failure(monkeypatch, stage):
    clock = iter([0, 1])
    monkeypatch.setattr(canary.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(
        canary.kube, "read_demo_project", lambda _: {"status": {"phase": "Starting"}}
    )
    monkeypatch.setattr(canary, "canary_workflow_terminal_failure", lambda _: None)
    monkeypatch.setattr(
        canary,
        "canary_cleanup_snapshot",
        lambda _: {"phase": "CleanupPending", "ok": False, "waitingOn": ["namespace"]},
    )
    if stage == "promotion":
        fn, request = (
            canary.wait_for_registry_operator_canary_promotion,
            canary.RegistryOperatorCanaryPromotionWaitRequest(
                "canary", "canary-ns", timeout_seconds=0
            ),
        )
    elif stage == "rotation":
        fn, request = (
            canary.wait_for_registry_operator_canary_rotation,
            canary.RegistryOperatorCanaryRotationWaitRequest(
                "canary", "apiKey", "token", timeout_seconds=0
            ),
        )
    else:
        fn, request = (
            canary.wait_registry_operator_canary_cleanup,
            canary.WaitRegistryOperatorCanaryCleanupRequest(
                "canary",
                "canary-ns",
                "source",
                "operator",
                "operator",
                "operator",
                "catalog",
                cleanup_timeout_seconds=0,
            ),
        )
    with pytest.raises(ApplicationError) as raised:
        ActivityEnvironment().run(fn, request)
    assert raised.value.non_retryable
    assert raised.value.details[0]["status"]["ok"] is False


CANARY_STEPS = [
    "apply_registry_operator_canary_project",
    "signal_registry_operator_canary_reconcile",
    "wait_for_registry_operator_canary_promotion",
    "rotate_registry_operator_canary_credentials",
    "wait_for_registry_operator_canary_rotation",
    "delete_registry_operator_canary_project",
    "signal_registry_operator_canary_finalize",
    "wait_registry_operator_canary_cleanup",
]


@pytest.mark.parametrize("failed_step", CANARY_STEPS + [None])
def test_canary_saga_cleans_up_early_failures_and_keeps_cleanup_errors(
    monkeypatch, failed_step
):
    calls = []

    async def execute(fn, request, **kwargs):
        name = fn.__name__
        if name == "prepare_registry_operator_canary_plan":
            return canary.RegistryOperatorCanaryPlan(canary_plan())
        calls.append((name, kwargs.get("summary")))
        if name == failed_step:
            raise activity_error(name)
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Ready", "ok": True, "token": "token"}
        )

    monkeypatch.setattr(operator_auth_canary.workflow, "execute_activity", execute)
    result = asyncio.run(
        operator_auth_canary._run_registry_operator_auth_canary(
            messages.RegistryOperatorAuthCanaryRequest("wf", "run", "apiKey"), "apiKey"
        )
    )
    assert [name for name, _ in calls][-3:] == CANARY_STEPS[-3:]
    prefix = "Compensate" if failed_step in CANARY_STEPS[:5] else "Clean up"
    assert all(summary.startswith(prefix) for _, summary in calls[-3:])
    assert result.phase == ("Succeeded" if failed_step is None else "Failed")
    if failed_step in CANARY_STEPS[-3:]:
        assert result.compensationFailures[0]["activityType"] == failed_step
    elif failed_step:
        assert result.failure["activityType"] == failed_step
        assert result.failure["message"] == "provider denied the request"


def test_canary_saga_preserves_original_failure_when_cleanup_also_fails(monkeypatch):
    async def execute(fn, request, **kwargs):
        name = fn.__name__
        if name in {CANARY_STEPS[0], CANARY_STEPS[-3], CANARY_STEPS[-2]}:
            raise activity_error(name, name)
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Deleted", "ok": True}
        )

    monkeypatch.setattr(operator_auth_canary.workflow, "execute_activity", execute)
    result = asyncio.run(
        operator_auth_canary._run_registry_operator_auth_canary_saga(
            canary_plan(), "apiKey"
        )
    )
    assert result.failure["message"] == CANARY_STEPS[0]
    assert len(result.compensationFailures) == 2
    assert result.cleanup["ok"] is True
    assert result.phase == "Failed"


@pytest.mark.parametrize("never", [False, True])
def test_canary_saga_cancellation_runs_cleanup_and_respects_policy(monkeypatch, never):
    calls = []

    async def execute(fn, request, **kwargs):
        name = fn.__name__
        calls.append(name)
        if name == CANARY_STEPS[0]:
            raise asyncio.CancelledError()
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Skipped" if never else "Deleted", "ok": True}
        )

    monkeypatch.setattr(operator_auth_canary.workflow, "execute_activity", execute)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            operator_auth_canary._run_registry_operator_auth_canary_saga(
                canary_plan(cleanupPolicy="Never" if never else "Always"), "apiKey"
            )
        )
    assert calls[1:] == (CANARY_STEPS[-3:-2] if never else CANARY_STEPS[-3:])


def test_canary_parent_waits_for_other_branches_on_child_failure(monkeypatch):
    completed = []

    async def execute_activity(*args, **kwargs):
        return [{"name": "bad"}, {"name": "good"}]

    async def execute_child(fn, request, **kwargs):
        if request.workflowID.endswith("bad"):
            raise ApplicationError("plan invalid", non_retryable=True)
        await asyncio.sleep(0.01)
        completed.append("good")
        assert (
            kwargs["parent_close_policy"]
            == operator_canary.workflow.ParentClosePolicy.REQUEST_CANCEL
        )
        return messages.RegistryOperatorAuthCanaryResult(phase="Succeeded")

    monkeypatch.setattr(operator_canary.workflow, "execute_activity", execute_activity)
    monkeypatch.setattr(
        operator_canary.workflow, "execute_child_workflow", execute_child
    )
    monkeypatch.setattr(
        operator_canary.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="gate", run_id="run", task_queue="queue"),
    )
    with pytest.raises(ApplicationError) as raised:
        asyncio.run(
            operator_canary.RegistryOperatorCanaryWorkflow().run(
                messages.RegistryOperatorCanaryRequest()
            )
        )
    assert completed == ["good"]
    assert raised.value.details[0].canaries["bad"].failure["message"] == "plan invalid"


def test_instruqt_failure_is_recorded_and_resources_are_retained(monkeypatch):
    recorded = []

    async def execute(fn, request=None, **kwargs):
        if fn is instruqt.check_instruqt_cli:
            return instruqt.InstruqtActivityResult(READY_CLI)
        if fn is instruqt.reconcile_instruqt_course_ecr_repositories:
            raise activity_error(fn.__name__)
        assert fn is instruqt.record_instruqt_course_status
        recorded.append(request.status)
        return instruqt.InstruqtActivityResult(request.status)

    monkeypatch.setattr(instruqt_course.workflow, "execute_activity", execute)
    monkeypatch.setattr(
        instruqt_course.workflow,
        "now",
        lambda: SimpleNamespace(isoformat=lambda: "now"),
    )
    entity = instruqt_course.InstruqtCourseWorkflow(
        messages.InstruqtCourseRunRequest("course")
    )
    assert asyncio.run(
        entity._run_instruqt_course_reconcile(
            "course", instruqt_course.InstruqtCourseReconcileInput(COURSE)
        )
    )
    assert recorded[0]["phase"] == "Error"
    assert recorded[0]["rolloutFailure"]["scheduledEventId"] == 10
    assert recorded[0]["compensationStatus"]["phase"] == "NotRequired"


def test_instruqt_status_write_failure_preserves_publish_failure(monkeypatch):
    async def execute(fn, request=None, **kwargs):
        if fn is instruqt.check_instruqt_cli:
            return instruqt.InstruqtActivityResult(READY_CLI)
        raise activity_error(
            fn.__name__,
            "publish denied"
            if fn is instruqt.push_instruqt_course_track
            else "status write denied",
        )

    async def ecr(*args):
        return {"phase": "NotRequired"}

    async def images(*args, **kwargs):
        return {"phase": "NotRequired", "images": {}}

    monkeypatch.setattr(instruqt_course.workflow, "execute_activity", execute)
    monkeypatch.setattr(
        instruqt_course.workflow,
        "now",
        lambda: SimpleNamespace(isoformat=lambda: "now"),
    )
    entity = instruqt_course.InstruqtCourseWorkflow(
        messages.InstruqtCourseRunRequest("course")
    )
    monkeypatch.setattr(entity, "_reconcile_instruqt_ecr", ecr)
    monkeypatch.setattr(entity, "_resolve_images", images)
    asyncio.run(
        entity._run_instruqt_course_reconcile(
            "course", instruqt_course.InstruqtCourseReconcileInput(COURSE)
        )
    )
    assert entity.status()["rolloutFailure"]["message"] == "publish denied"
    assert entity.status()["statusRecordFailure"]["message"] == "status write denied"


def test_instruqt_failed_finalization_keeps_finalizer_and_accepts_retry(monkeypatch):
    calls = []
    actions = iter(["finalize", "finalize"])
    entity = instruqt_course.InstruqtCourseWorkflow(
        messages.InstruqtCourseRunRequest("course")
    )
    entity._finalize_spec = COURSE

    async def next_action():
        next(actions)
        entity._finalize = True

    async def execute(fn, request, **kwargs):
        calls.append(fn.__name__)
        if fn is instruqt.load_instruqt_course_finalization_plan:
            return instruqt.InstruqtCourseFinalizationPlan(
                {
                    "instruqtCourse": COURSE,
                    "course": "course",
                    "gitRepository": "source",
                    "sourceWatchNamespace": "operator",
                    "checkedAt": "now",
                }
            )
        if (
            fn is instruqt.delete_instruqt_course_track
            and calls.count(fn.__name__) == 1
        ):
            raise activity_error(fn.__name__, "delete denied")
        if fn is instruqt.remove_instruqt_course_finalizer_resource:
            return None
        return SimpleNamespace(action="deleted", status={"phase": "Deleted"})

    monkeypatch.setattr(entity, "_wait_for_next_action", next_action)
    monkeypatch.setattr(instruqt_course.workflow, "execute_activity", execute)
    asyncio.run(entity.run(messages.InstruqtCourseRunRequest("course")))
    assert calls.count("load_instruqt_course_finalization_plan") == 2
    assert calls.count("remove_instruqt_course_finalizer_resource") == 1
    assert calls.index("remove_instruqt_course_finalizer_resource") > calls.index(
        "delete_instruqt_course_ecr_repositories"
    )


def test_cancellation_during_canary_cleanup_waits_for_remaining_steps(monkeypatch):
    calls = []

    async def verify():
        delete_started, finish_delete = asyncio.Event(), asyncio.Event()

        async def execute(fn, request, **kwargs):
            calls.append(fn.__name__)
            if fn is canary.delete_registry_operator_canary_project:
                delete_started.set()
                await finish_delete.wait()
            return canary.RegistryOperatorCanaryActivityResult(
                {"phase": "Ready", "ok": True, "token": "token"}
            )

        monkeypatch.setattr(operator_auth_canary.workflow, "execute_activity", execute)
        task = asyncio.create_task(
            operator_auth_canary._run_registry_operator_auth_canary_saga(
                canary_plan(), "apiKey"
            )
        )
        await delete_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        finish_delete.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(verify())
    assert calls[-3:] == CANARY_STEPS[-3:]
