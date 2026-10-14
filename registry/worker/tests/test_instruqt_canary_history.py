"""Run with DEMO_REGISTRY_TEST_TEMPORAL_HISTORY=1 and an installed Temporal CLI."""

from __future__ import annotations

import asyncio
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from demo_registry_operator import errors, resources
from demo_registry_operator.activities import canary, instruqt, workspace_canary
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows import (
    instruqt_course,
    messages,
    operator_auth_canary,
    operator_canary,
)
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

pytestmark = pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal dev server",
)


def scheduled_activities(history):
    return [
        event
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


def failure_events(history):
    return [
        event
        for event in history.events
        if event.HasField("activity_task_failed_event_attributes")
    ]


def test_canary_failure_compensation_cancellation_and_replay(monkeypatch):
    scenario = {
        "failure": "promotion",
        "created": False,
        "workspace_created": False,
    }
    apply_started = None
    monkeypatch.setattr(
        canary.kube,
        "read_demo_project",
        lambda _: {"status": {"phase": "Error", "message": "worker could not start"}},
    )

    @activity.defn(name="prepare_registry_operator_canary_plan")
    async def prepare(
        request: canary.RegistryOperatorCanaryPlanRequest,
    ) -> canary.RegistryOperatorCanaryPlan:
        if scenario["failure"] == "parent" and request.project_name_prefix.endswith(
            "bad"
        ):
            raise ApplicationError(
                "invalid canary plan", type="InvalidPlan", non_retryable=True
            )
        return canary.RegistryOperatorCanaryPlan(
            {
                "project": "canary",
                "namespace": "canary-ns",
                "demoProject": {},
                "authMode": "apiKey",
                "targetDeploymentName": "operator",
                "targetBuildID": "new",
                "gitRepository": "source",
                "sourceWatchNamespace": "operator",
                "catalogStatusNamespace": "operator",
                "catalogStatusMirrorNamespace": "operator",
                "catalogStatusConfigMap": "catalog",
                "timeoutSeconds": 1,
                "pollSeconds": 1,
                "cleanupTimeoutSeconds": 1,
                **(
                    {
                        "workspaceMode": "isolated-lambda"
                        if scenario["failure"] == "workspace-lambda-success"
                        else "crashable"
                    }
                    if scenario["failure"].startswith("workspace")
                    else {}
                ),
            }
        )

    @activity.defn(name="apply_registry_operator_canary_project")
    async def apply(
        request: canary.ApplyRegistryOperatorCanaryProjectRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        scenario["created"] = True
        if scenario["failure"] == "apply":
            raise ApplicationError(
                "response lost after project creation",
                type="ApplyFailed",
                non_retryable=True,
            )
        if scenario["failure"] == "cancel":
            apply_started.set()
            await asyncio.Event().wait()
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Applied", "ok": True}
        )

    @activity.defn(name="signal_registry_operator_canary_reconcile")
    async def signal(
        request: canary.SignalRegistryOperatorCanaryReconcileRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Signaled", "workflowID": "project", "runID": "run"}
        )

    @activity.defn(name="wait_for_registry_operator_canary_promotion")
    def promotion(
        request: canary.RegistryOperatorCanaryPromotionWaitRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        if scenario["failure"] in {"cleanup", "parent"} or scenario[
            "failure"
        ].startswith("workspace"):
            return canary.RegistryOperatorCanaryActivityResult(
                {"phase": "Promoted", "ok": True}
            )
        return canary.wait_for_registry_operator_canary_promotion(request)

    @activity.defn(name="rotate_registry_operator_canary_credentials")
    async def rotate(
        request: canary.RotateRegistryOperatorCanaryCredentialsRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Signaled", "token": "token"}
        )

    @activity.defn(name="wait_for_registry_operator_canary_rotation")
    async def rotation(
        request: canary.RegistryOperatorCanaryRotationWaitRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Rotated", "ok": True}
        )

    @activity.defn(name="delete_registry_operator_canary_project")
    async def delete(
        request: canary.DeleteRegistryOperatorCanaryProjectRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        scenario["created"] = False
        if scenario["failure"] == "promotion-and-delete":
            raise ApplicationError(
                "delete denied", type="DeleteFailed", non_retryable=True
            )
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "DeleteRequested", "ok": True}
        )

    @activity.defn(name="signal_registry_operator_canary_finalize")
    async def finalize(
        request: canary.SignalRegistryOperatorCanaryFinalizeRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Signaled", "ok": True}
        )

    @activity.defn(name="wait_registry_operator_canary_cleanup")
    async def cleanup(
        request: canary.WaitRegistryOperatorCanaryCleanupRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        if scenario["failure"] == "cleanup":
            raise errors.terminal_activity_error(
                {
                    "phase": "CleanupFailed",
                    "ok": False,
                    "message": "namespace still exists",
                },
                error_type="RegistryOperatorCanaryCleanupFailed",
            )
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Deleted", "ok": True}
        )

    @activity.defn(name="load_canary_templates")
    async def templates() -> list[dict]:
        return [
            {"name": "bad", "authMode": "apiKey"},
            {"name": "good", "authMode": "apiKey"},
        ]

    @activity.defn(name="begin_workspace_canary")
    async def begin_workspace(
        request: workspace_canary.WorkspaceCanaryRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        scenario["workspace_created"] = True
        if scenario["failure"] == "workspace-start":
            raise ApplicationError(
                "lost workspace start acknowledgement",
                type="WorkspaceStartFailed",
                non_retryable=True,
            )
        if scenario["failure"] == "workspace-cancel":
            apply_started.set()
            await asyncio.Event().wait()
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Started", "ok": True, "source": {"namespace": "source"}}
        )

    @activity.defn(name="check_workspace_canary")
    async def check_workspace(
        request: workspace_canary.WorkspaceCanaryRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        if (
            scenario["failure"] in {"workspace-check", "workspace-cleanup"}
            and request.step == "exercise"
        ):
            raise ApplicationError(
                "workspace secret mismatch",
                type="WorkspaceCheckFailed",
                non_retryable=True,
            )
        return canary.RegistryOperatorCanaryActivityResult(
            {**request.workspace, "phase": "Succeeded", "ok": True}
        )

    @activity.defn(name="cleanup_workspace_canary")
    async def cleanup_workspace(
        request: workspace_canary.WorkspaceCanaryRequest,
    ) -> canary.RegistryOperatorCanaryActivityResult:
        assert scenario["created"], "source must still exist during workspace cleanup"
        if scenario["failure"] == "workspace-cleanup":
            raise ApplicationError(
                "workspace Lambda remains",
                type="WorkspaceCleanupFailed",
                non_retryable=True,
            )
        scenario["workspace_created"] = False
        return canary.RegistryOperatorCanaryActivityResult(
            {"phase": "Deleted", "ok": True}
        )

    async def verify():
        nonlocal apply_started
        apply_started = asyncio.Event()
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with ThreadPoolExecutor(max_workers=3) as executor:
                cls = operator_auth_canary.RegistryOperatorAuthCanaryWorkflow
                for failed_step in (
                    "apply",
                    "promotion",
                    "promotion-and-delete",
                    "cleanup",
                    "cancel",
                    "workspace-success",
                    "workspace-lambda-success",
                    "workspace-start",
                    "workspace-check",
                    "workspace-cleanup",
                    "workspace-cancel",
                ):
                    scenario.update(
                        failure=failed_step,
                        created=False,
                        workspace_created=False,
                    )
                    apply_started.clear()
                    async with Worker(
                        env.client,
                        task_queue="canary-saga-history",
                        workflows=[cls],
                        workflow_runner=create_workflow_runner(),
                        activities=[
                            prepare,
                            apply,
                            signal,
                            promotion,
                            rotate,
                            rotation,
                            delete,
                            finalize,
                            cleanup,
                            begin_workspace,
                            check_workspace,
                            cleanup_workspace,
                        ],
                        activity_executor=executor,
                    ):
                        handle = await env.client.start_workflow(
                            cls.run,
                            messages.RegistryOperatorAuthCanaryRequest(
                                "gate", "run", "apiKey"
                            ),
                            id=f"canary-{failed_step}",
                            task_queue="canary-saga-history",
                        )
                        if failed_step in {"cancel", "workspace-cancel"}:
                            await asyncio.wait_for(apply_started.wait(), 10)
                            await handle.cancel()
                            with pytest.raises(Exception) as raised:
                                await asyncio.wait_for(handle.result(), 15)
                            assert isinstance(raised.value.__cause__, CancelledError)
                            assert (
                                await handle.describe()
                            ).status == WorkflowExecutionStatus.CANCELED
                        else:
                            result = await asyncio.wait_for(handle.result(), 15)
                            assert result.phase == (
                                "Succeeded"
                                if failed_step
                                in {"workspace-success", "workspace-lambda-success"}
                                else "Failed"
                            )
                            if failed_step.startswith("workspace"):
                                assert result.workspaceCleanup["phase"] == (
                                    "CleanupFailed"
                                    if failed_step == "workspace-cleanup"
                                    else "Deleted"
                                )
                                assert scenario["created"] == (
                                    failed_step == "workspace-cleanup"
                                )
                                assert scenario["workspace_created"] == (
                                    failed_step == "workspace-cleanup"
                                )
                                if failed_step == "workspace-cleanup":
                                    assert result.compensationFailures
                                    assert (
                                        "secret mismatch" in result.failure["message"]
                                    )
                            if failed_step == "promotion-and-delete":
                                assert (
                                    "worker could not start"
                                    in result.failure["message"]
                                )
                                assert (
                                    result.compensationFailures[0]["message"]
                                    == "delete denied"
                                )
                            if failed_step == "cleanup":
                                assert (
                                    result.compensationFailures[0]["message"]
                                    == "namespace still exists"
                                )
                        assert scenario["created"] == (
                            failed_step == "workspace-cleanup"
                        )
                        history = await handle.fetch_history()
                        scheduled = scheduled_activities(history)
                        names = [
                            event.activity_task_scheduled_event_attributes.activity_type.name
                            for event in scheduled
                        ]
                        if failed_step == "workspace-cleanup":
                            assert names[-1] == "cleanup_workspace_canary"
                            assert (
                                "delete_registry_operator_canary_project" not in names
                            )
                        else:
                            assert names[-3:] == [
                                "delete_registry_operator_canary_project",
                                "signal_registry_operator_canary_finalize",
                                "wait_registry_operator_canary_cleanup",
                            ]
                        if failed_step.startswith("workspace"):
                            assert "cleanup_workspace_canary" in names
                            if failed_step != "workspace-cleanup":
                                assert names[-4] == "cleanup_workspace_canary"
                        if failed_step in {
                            "workspace-success",
                            "workspace-lambda-success",
                        }:
                            steps = []
                            for event in scheduled:
                                attributes = (
                                    event.activity_task_scheduled_event_attributes
                                )
                                if (
                                    attributes.activity_type.name
                                    == "check_workspace_canary"
                                ):
                                    decoded = await env.client.data_converter.decode(
                                        attributes.input.payloads
                                    )
                                    steps.append(decoded[0]["step"])
                            assert steps == (
                                ["ready", "exercise"]
                                if failed_step == "workspace-lambda-success"
                                else [
                                    "ready",
                                    "exercise",
                                    "crash",
                                    "scale_down",
                                    "scale_up",
                                    "exercise",
                                ]
                            )
                        for event in (
                            []
                            if failed_step == "workspace-cleanup"
                            else scheduled[-3:]
                        ):
                            summary = await env.client.data_converter.decode(
                                [event.user_metadata.summary]
                            )
                            assert summary[0].startswith(
                                "Clean up operator canary:"
                                if failed_step
                                in {
                                    "cleanup",
                                    "workspace-success",
                                    "workspace-lambda-success",
                                }
                                else "Compensate operator canary:"
                            )
                        if failed_step not in {
                            "cancel",
                            "workspace-cancel",
                            "workspace-success",
                            "workspace-lambda-success",
                        }:
                            assert failure_events(history)
                        assert not any(
                            event.HasField("marker_recorded_event_attributes")
                            for event in history.events
                        )
                        await Replayer(
                            workflows=[cls],
                            workflow_runner=create_workflow_runner(),
                        ).replay_workflow(history)
                scenario.update(failure="parent", created=False)
                parent_cls = operator_canary.RegistryOperatorCanaryWorkflow
                async with Worker(
                    env.client,
                    task_queue="canary-parent-history",
                    workflows=[parent_cls, cls],
                    workflow_runner=create_workflow_runner(),
                    activities=[
                        templates,
                        prepare,
                        apply,
                        signal,
                        promotion,
                        rotate,
                        rotation,
                        delete,
                        finalize,
                        cleanup,
                    ],
                    activity_executor=executor,
                ):
                    handle = await env.client.start_workflow(
                        parent_cls.run,
                        messages.RegistryOperatorCanaryRequest(),
                        id="parent",
                        task_queue="canary-parent-history",
                    )
                    with pytest.raises(Exception) as raised:
                        await asyncio.wait_for(handle.result(), 15)
                    result = raised.value.__cause__.details[0]
                    assert result["canaries"]["bad"]["phase"] == "Failed"
                    assert result["canaries"]["good"]["phase"] == "Succeeded"
                    assert not scenario["created"]
                    history = await handle.fetch_history()
                    assert not any(
                        event.HasField("marker_recorded_event_attributes")
                        for event in history.events
                    )
                    await Replayer(
                        workflows=[parent_cls],
                        workflow_runner=create_workflow_runner(),
                    ).replay_workflow(history)

    asyncio.run(verify())


@pytest.mark.parametrize("transient", [False, True])
def test_instruqt_remains_running_after_failure_and_replays_legacy_results(
    monkeypatch, transient
):
    scenario = {"legacy": False, "generation": 1, "attempts": 0}
    monkeypatch.setattr(
        instruqt,
        "run_instruqt_track_operation",
        lambda *args, **kwargs: (
            {
                "phase": "Ran",
                "channel": "production",
                "target": "example/course",
                "artifact": {"phase": "Rendered"},
                "command": {
                    "phase": "Failed",
                    "message": "instruqt exited with 1",
                    "output": (
                        "HTTP 503 Service Unavailable"
                        if transient
                        else "publish denied"
                    ),
                    "exitCode": 1,
                },
            }
            if scenario["generation"] == 1
            else {
                "phase": "Ran",
                "channel": "production",
                "target": "example/course",
                "artifact": {"phase": "Rendered"},
                "command": {"phase": "Succeeded"},
            }
        ),
    )

    @activity.defn(name="check_instruqt_cli")
    async def cli() -> instruqt.InstruqtActivityResult:
        return instruqt.InstruqtActivityResult({"phase": "Ready", "ready": True})

    @activity.defn(name="reconcile_instruqt_course_ecr_repositories")
    async def ecr(
        request: instruqt.InstruqtCourseRequest,
    ) -> instruqt.InstruqtActivityResult:
        return instruqt.InstruqtActivityResult({"phase": "NotRequired"})

    @activity.defn(name="resolve_instruqt_course_image_build_plan")
    async def plan(
        request: instruqt.InstruqtCourseImageBuildPlanRequest,
    ) -> instruqt.InstruqtImageBuildPlan:
        return instruqt.InstruqtImageBuildPlan(
            instruqt.completed_instruqt_image_plan(
                {"phase": "NotRequired", "images": {}}
            )
        )

    @activity.defn(name="push_instruqt_course_track")
    def push(
        request: instruqt.InstruqtCourseArtifactRequest,
    ) -> instruqt.InstruqtActivityResult:
        scenario["attempts"] += 1
        try:
            return instruqt.push_instruqt_course_track(request)
        except ApplicationError as error:
            if scenario["legacy"]:
                return instruqt.InstruqtActivityResult(error.details[0]["status"])
            if not error.non_retryable:
                # Keep the real workflow retry policy, but shorten test delays.
                raise ApplicationError(
                    error.message,
                    *error.details,
                    type=error.type,
                    next_retry_delay=timedelta(milliseconds=10),
                ) from error
            raise

    @activity.defn(name="record_instruqt_course_status")
    async def record(
        request: instruqt.InstruqtCourseStatusRecordRequest,
    ) -> instruqt.InstruqtActivityResult:
        return instruqt.InstruqtActivityResult(request.status)

    async def verify():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with ThreadPoolExecutor(max_workers=2) as executor:
                async with Worker(
                    env.client,
                    task_queue="instruqt-failure-history",
                    workflows=[instruqt_course.InstruqtCourseWorkflow],
                    workflow_runner=create_workflow_runner(),
                    activities=[cli, ecr, plan, push, record],
                    activity_executor=executor,
                ):
                    for legacy in (True, False):
                        scenario.update(legacy=legacy, generation=1, attempts=0)
                        handle = await env.client.start_workflow(
                            instruqt_course.InstruqtCourseWorkflow.run,
                            messages.InstruqtCourseRunRequest("course"),
                            id=f"instruqt-{legacy}",
                            task_queue="instruqt-failure-history",
                        )
                        for generation, expected_phase in [
                            (1, "PushFailed"),
                            (2, "Ready"),
                        ]:
                            scenario["generation"] = generation
                            scenario["attempts"] = 0
                            course = {
                                "metadata": {
                                    "name": "course",
                                    "generation": generation,
                                    "annotations": {
                                        resources.SOURCE_COMMIT_ANNOTATION: "a" * 40,
                                        resources.SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:abc",
                                    },
                                },
                                "spec": {
                                    "source": {
                                        "repo": "https://github.com/example/course",
                                        "branch": "main",
                                    },
                                    "track": {
                                        "organization": "example",
                                        "slug": "course",
                                    },
                                },
                            }
                            await handle.signal(
                                "reconcile",
                                messages.InstruqtCourseReconcileSignal(course),
                            )

                            async def settled(
                                handle=handle, expected_phase=expected_phase
                            ):
                                while True:
                                    status = await handle.query("status")
                                    if status.get("phase") == expected_phase:
                                        return status
                                    await asyncio.sleep(0.02)

                            status = await asyncio.wait_for(settled(), 10)
                            assert (
                                await handle.describe()
                            ).status == WorkflowExecutionStatus.RUNNING
                            assert scenario["attempts"] == (
                                8 if generation == 1 and transient and not legacy else 1
                            )
                            if generation == 1 and not legacy:
                                assert (
                                    "HTTP 503" if transient else "publish denied"
                                ) in status["rolloutFailure"]["message"]
                                assert (
                                    status["compensationStatus"]["phase"]
                                    == "NotRequired"
                                )
                        await handle.signal("shutdown", messages.ShutdownSignal())
                        await asyncio.wait_for(handle.result(), 10)
                        history = await handle.fetch_history()
                        assert len(failure_events(history)) == (0 if legacy else 1)
                        assert not any(
                            "delete"
                            in event.activity_task_scheduled_event_attributes.activity_type.name
                            for event in scheduled_activities(history)
                        )
                        await Replayer(
                            workflows=[instruqt_course.InstruqtCourseWorkflow],
                            workflow_runner=create_workflow_runner(),
                        ).replay_workflow(history)

    asyncio.run(verify())
