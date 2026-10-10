"""Run with DEMO_REGISTRY_TEST_TEMPORAL_HISTORY=1 and an installed Temporal CLI."""

from __future__ import annotations

import asyncio
import os
import shutil
from concurrent.futures import ThreadPoolExecutor

import pytest
from demo_registry_operator import errors, resources
from demo_registry_operator.activities import canary, instruqt
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows import (
    instruqt_course,
    messages,
    operator_auth_canary,
    operator_canary,
)
from temporalio import activity, workflow
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

pytestmark = pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal dev server",
)


@workflow.defn(name="RegistryOperatorAuthCanaryWorkflow")
class LegacyAuthCanaryWorkflow:
    """The pre-patch command sequence, for genuine unpatched history replay."""

    @workflow.run
    async def run(
        self, request: messages.RegistryOperatorAuthCanaryRequest
    ) -> messages.RegistryOperatorAuthCanaryResult:
        plan = await operator_auth_canary._prepare_registry_operator_canary_plan(
            request
        )
        applied = await operator_auth_canary._apply_registry_operator_canary_project(
            plan
        )
        signaled = (
            await operator_auth_canary._signal_registry_operator_canary_reconcile(plan)
        )
        state = operator_auth_canary.RegistryOperatorAuthCanaryState(
            plan,
            request.authMode,
            applied,
            signaled,
            str(signaled.get("workflowID") or ""),
            str(signaled.get("runID") or ""),
        )
        try:
            await operator_auth_canary._wait_for_registry_operator_canary_promotion(
                state
            )
            if state.promotion_ok:
                await operator_auth_canary._run_registry_operator_canary_rotation(state)
        finally:
            await operator_auth_canary._cleanup_registry_operator_canary(state)
        return operator_auth_canary._registry_operator_auth_canary_result(state)


@workflow.defn(name="RegistryOperatorCanaryWorkflow")
class LegacyOperatorCanaryWorkflow:
    @workflow.run
    async def run(
        self, request: messages.RegistryOperatorCanaryRequest
    ) -> messages.RegistryOperatorCanaryResult:
        info = workflow.info()
        templates = await workflow.execute_activity(
            canary.load_canary_templates,
            start_to_close_timeout=operator_canary.timedelta(seconds=30),
            retry_policy=operator_canary.LOAD_CANARY_TEMPLATES_RETRY_POLICY,
        )
        results = await asyncio.gather(
            *(
                workflow.execute_child_workflow(
                    operator_auth_canary.RegistryOperatorAuthCanaryWorkflow.run,
                    branch,
                    id=branch.workflowID,
                    task_queue=info.task_queue,
                    parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
                    versioning_intent=workflow.VersioningIntent.COMPATIBLE,
                )
                for branch in (
                    operator_canary.registry_operator_canary_branch_request(
                        request,
                        template=template,
                        workflow_id=info.workflow_id,
                        run_id=info.run_id,
                    )
                    for template in templates
                )
            )
        )
        return messages.RegistryOperatorCanaryResult(
            phase="Succeeded",
            canaries=operator_canary.canary_results_by_template_name(
                templates, results
            ),
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


def test_canary_failure_compensation_cancellation_and_legacy_replay(monkeypatch):
    scenario = {"failure": "promotion", "legacy": False, "created": False}
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
        if scenario["failure"] in {"cleanup", "parent", "parent-legacy"}:
            return canary.RegistryOperatorCanaryActivityResult(
                {"phase": "Promoted", "ok": True}
            )
        try:
            return canary.wait_for_registry_operator_canary_promotion(request)
        except ApplicationError as error:
            if scenario["legacy"]:
                return canary.RegistryOperatorCanaryActivityResult(
                    error.details[0]["status"]
                )
            raise

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

    async def verify():
        nonlocal apply_started
        apply_started = asyncio.Event()
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with ThreadPoolExecutor(max_workers=3) as executor:
                for failed_step, legacy in [
                    ("promotion", True),
                    ("apply", False),
                    ("promotion", False),
                    ("promotion-and-delete", False),
                    ("cleanup", False),
                    ("cancel", False),
                ]:
                    scenario.update(failure=failed_step, legacy=legacy, created=False)
                    cls = (
                        LegacyAuthCanaryWorkflow
                        if legacy
                        else operator_auth_canary.RegistryOperatorAuthCanaryWorkflow
                    )
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
                        ],
                        activity_executor=executor,
                    ):
                        handle = await env.client.start_workflow(
                            cls.run,
                            messages.RegistryOperatorAuthCanaryRequest(
                                "gate", "run", "apiKey"
                            ),
                            id=f"canary-{failed_step}-{legacy}",
                            task_queue="canary-saga-history",
                        )
                        if failed_step == "cancel":
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
                            assert result.phase == "Failed"
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
                        assert not scenario["created"]
                        history = await handle.fetch_history()
                        scheduled = scheduled_activities(history)
                        names = [
                            event.activity_task_scheduled_event_attributes.activity_type.name
                            for event in scheduled
                        ]
                        assert names[-3:] == [
                            "delete_registry_operator_canary_project",
                            "signal_registry_operator_canary_finalize",
                            "wait_registry_operator_canary_cleanup",
                        ]
                        if not legacy:
                            for event in scheduled[-3:]:
                                summary = await env.client.data_converter.decode(
                                    [event.user_metadata.summary]
                                )
                                assert summary[0].startswith(
                                    "Clean up operator canary:"
                                    if failed_step == "cleanup"
                                    else "Compensate operator canary:"
                                )
                            if failed_step != "cancel":
                                assert failure_events(history)
                        else:
                            assert not failure_events(history)
                            assert not any(
                                event.HasField("marker_recorded_event_attributes")
                                for event in history.events
                            )
                        await Replayer(
                            workflows=[
                                operator_auth_canary.RegistryOperatorAuthCanaryWorkflow
                            ],
                            workflow_runner=create_workflow_runner(),
                        ).replay_workflow(history)
                for legacy in (True, False):
                    scenario.update(
                        failure="parent-legacy" if legacy else "parent", created=False
                    )
                    cls = (
                        LegacyOperatorCanaryWorkflow
                        if legacy
                        else operator_canary.RegistryOperatorCanaryWorkflow
                    )
                    async with Worker(
                        env.client,
                        task_queue="canary-parent-history",
                        workflows=[
                            cls,
                            operator_auth_canary.RegistryOperatorAuthCanaryWorkflow,
                        ],
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
                            cls.run,
                            messages.RegistryOperatorCanaryRequest(),
                            id=f"parent-{legacy}",
                            task_queue="canary-parent-history",
                        )
                        if legacy:
                            assert (
                                await asyncio.wait_for(handle.result(), 15)
                            ).phase == "Succeeded"
                        else:
                            with pytest.raises(Exception) as raised:
                                await asyncio.wait_for(handle.result(), 15)
                            result = raised.value.__cause__.details[0]
                            assert result["canaries"]["bad"]["phase"] == "Failed"
                            assert result["canaries"]["good"]["phase"] == "Succeeded"
                            assert not scenario["created"]
                        history = await handle.fetch_history()
                        if legacy:
                            assert not any(
                                event.HasField("marker_recorded_event_attributes")
                                for event in history.events
                            )
                        await Replayer(
                            workflows=[operator_canary.RegistryOperatorCanaryWorkflow],
                            workflow_runner=create_workflow_runner(),
                        ).replay_workflow(history)

    asyncio.run(verify())


def test_instruqt_remains_running_after_failure_and_replays_legacy_results(monkeypatch):
    scenario = {"legacy": False, "generation": 1}
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
                    "output": "publish denied",
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
        try:
            return instruqt.push_instruqt_course_track(request)
        except ApplicationError as error:
            if scenario["legacy"]:
                return instruqt.InstruqtActivityResult(error.details[0]["status"])
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
                    activities=[ecr, plan, push, record],
                    activity_executor=executor,
                ):
                    for legacy in (True, False):
                        scenario.update(legacy=legacy, generation=1)
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
                            if generation == 1 and not legacy:
                                assert (
                                    "publish denied"
                                    in status["rolloutFailure"]["message"]
                                )
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
