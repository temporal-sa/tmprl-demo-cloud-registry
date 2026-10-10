"""Run with DEMO_REGISTRY_TEST_TEMPORAL_HISTORY=1 and an installed Temporal CLI."""

from __future__ import annotations

import asyncio
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from demo_registry_operator.activities import crashable_workspace as activities
from demo_registry_operator.activities import namespace_readers as grants
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows import crashable_workspace, namespace_readers
from demo_registry_operator.workflows import crashable_workspace_messages as messages
from demo_registry_operator.workflows.namespace_readers_messages import (
    GrantNamespaceReadAccessRequest,
    GrantNamespaceReadAccessResult,
)
from temporalio import activity, workflow
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

pytestmark = pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal dev server",
)


def events(history, attributes):
    return [event for event in history.events if event.HasField(attributes)]


@workflow.defn(name="GrantNamespaceReadAccessWorkflow")
class LegacyGrantWorkflow:
    @workflow.run
    async def run(
        self, request: GrantNamespaceReadAccessRequest
    ) -> GrantNamespaceReadAccessResult:
        return await workflow.execute_activity(
            grants.grant_user_namespace_read_access,
            request,
            start_to_close_timeout=timedelta(minutes=3),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=grants.GRANT_RETRY_POLICY,
        )


@workflow.defn(name="CrashableWorkspaceWorkflow")
class LegacyWorkspaceWorkflow(crashable_workspace.CrashableWorkspaceWorkflow):
    @workflow.init
    def __init__(self, request: messages.EnsureWorkspaceRequest):
        super().__init__(request)

    @workflow.run
    async def run(self, request: messages.EnsureWorkspaceRequest) -> None:
        # Original provisioning commands and cancellation behavior, without patches.
        compensations = []
        try:
            images = await workflow.execute_activity(
                activities.resolve_source_images,
                messages.ResolveSourceImagesRequest(request.demo_name),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=activities.PROVISIONING_RETRY_POLICY,
            )
            plan = await workflow.execute_activity(
                activities.build_workspace_plan,
                messages.BuildWorkspacePlanRequest(
                    request.demo_name,
                    request.owner_email,
                    self._workspace_id,
                    images.images,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=activities.PROVISIONING_RETRY_POLICY,
            )
            compensations.append(
                lambda: workflow.execute_activity(
                    activities.delete_workspace_namespace,
                    messages.DeleteWorkspaceNamespaceRequest(self._namespace),
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=activities.CLEANUP_RETRY_POLICY,
                )
            )
            for manifest in plan.resources:
                await workflow.execute_activity(
                    activities.apply_workspace_resource,
                    messages.ApplyWorkspaceResourceRequest(manifest),
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=activities.PROVISIONING_RETRY_POLICY,
                )
            await workflow.execute_activity(
                activities.wait_workspace_ready,
                messages.WaitWorkspaceReadyRequest(
                    self._namespace, plan.deployment_names, plan.worker_deployment_names
                ),
                start_to_close_timeout=timedelta(minutes=6),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=activities.WAIT_READY_RETRY_POLICY,
            )
        except asyncio.CancelledError:
            await asyncio.shield(
                asyncio.ensure_future(
                    crashable_workspace._run_compensations(compensations)
                )
            )
            raise
        except Exception:
            await crashable_workspace._run_compensations(compensations)
            raise


def test_grant_native_failures_bounded_retries_and_legacy_replay(monkeypatch):
    scenario = {"mode": "permanent", "attempts": 0}
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
        scenario["attempts"] += 1
        if scenario["mode"] == "transient":
            raise RPCError("internal server error", RPCStatusCode.INTERNAL, b"")
        if scenario["mode"] == "legacy-failed":
            raise TimeoutError("Cloud Ops timed out")
        raise RuntimeError("permission denied")

    monkeypatch.setattr(grants.cloud_activities, "cloud_reconciler", reconciler)

    @activity.defn(name="grant_user_namespace_read_access")
    def grant(
        request: GrantNamespaceReadAccessRequest,
    ) -> GrantNamespaceReadAccessResult:
        try:
            return grants.grant_user_namespace_read_access(request)
        except ApplicationError as error:
            if scenario["mode"] == "legacy-result":
                return GrantNamespaceReadAccessResult(**error.details[0]["status"])
            raise

    async def verify():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with ThreadPoolExecutor(max_workers=2) as executor:
                for mode in (
                    "legacy-result",
                    "legacy-failed",
                    "permanent",
                    "transient",
                ):
                    scenario.update(mode=mode, attempts=0)
                    cls = (
                        LegacyGrantWorkflow
                        if mode.startswith("legacy")
                        else namespace_readers.GrantNamespaceReadAccessWorkflow
                    )
                    async with Worker(
                        env.client,
                        task_queue="grant-history",
                        workflows=[cls],
                        activities=[grant],
                        activity_executor=executor,
                        workflow_runner=create_workflow_runner(),
                    ):
                        handle = await env.client.start_workflow(
                            cls.run,
                            GrantNamespaceReadAccessRequest("user@temporal.io"),
                            id=mode,
                            task_queue="grant-history",
                        )
                        if mode == "legacy-failed":
                            with pytest.raises(WorkflowFailureError):
                                await asyncio.wait_for(handle.result(), 15)
                        else:
                            result = await asyncio.wait_for(handle.result(), 15)
                            assert result.status == "error"
                            assert result.group == "readers"
                            assert result.email == "user@temporal.io"
                            assert bool(result.activity_failure) == (
                                not mode.startswith("legacy")
                            )
                            assert (
                                await handle.describe()
                            ).status == WorkflowExecutionStatus.COMPLETED
                        assert scenario["attempts"] == (
                            3 if mode in {"transient", "legacy-failed"} else 1
                        )
                        history = await handle.fetch_history()
                        failures = events(
                            history, "activity_task_failed_event_attributes"
                        )
                        assert len(failures) == (0 if mode == "legacy-result" else 1)
                        if mode in {"permanent", "transient"}:
                            assert (
                                result.activity_failure["scheduledEventId"]
                                == failures[
                                    0
                                ].activity_task_failed_event_attributes.scheduled_event_id
                            )
                        if mode.startswith("legacy"):
                            assert not events(
                                history, "marker_recorded_event_attributes"
                            )
                        await Replayer(
                            workflows=[
                                namespace_readers.GrantNamespaceReadAccessWorkflow
                            ],
                            workflow_runner=create_workflow_runner(),
                        ).replay_workflow(history)

    asyncio.run(verify())


def test_workspace_action_failures_remain_running_and_teardown_errors_are_visible():
    scenario = {}

    @activity.defn(name="crash_workspace_pods")
    async def crash(
        request: messages.CrashWorkspacePodsRequest,
    ) -> messages.CrashWorkspacePodsResult:
        raise ApplicationError(
            "pod deletion denied",
            type="ProviderAuthenticationError",
            non_retryable=True,
        )

    @activity.defn(name="scale_workspace_workloads")
    async def scale(
        request: messages.ScaleWorkspaceWorkloadsRequest,
    ) -> messages.ScaleWorkspaceWorkloadsResult:
        raise ApplicationError(
            "workload scaling denied",
            type="ProviderAuthenticationError",
            non_retryable=True,
        )

    @activity.defn(name="delete_workspace_namespace")
    async def delete(
        request: messages.DeleteWorkspaceNamespaceRequest,
    ) -> messages.DeleteWorkspaceNamespaceResult:
        if scenario["action"] == "delete":
            raise ApplicationError(
                "namespace deletion denied",
                type="ProviderAuthenticationError",
                non_retryable=True,
            )
        return messages.DeleteWorkspaceNamespaceResult("deleted")

    async def verify():
        cls = crashable_workspace.CrashableWorkspaceWorkflow
        async with (
            await WorkflowEnvironment.start_local(
                dev_server_existing_path=shutil.which("temporal")
            ) as env,
            Worker(
                env.client,
                task_queue="workspace-action-history",
                workflows=[cls],
                activities=[crash, scale, delete],
                workflow_runner=create_workflow_runner(),
            ),
        ):
            for action in ("crash", "scale_down", "scale_up", "delete"):
                scenario["action"] = action
                handle = await env.client.start_workflow(
                    cls.run,
                    messages.EnsureWorkspaceRequest(
                        "demo",
                        "user@temporal.io",
                        carried_already_provisioned=True,
                        carried_expires_at=(
                            datetime.now(UTC) + timedelta(hours=1)
                        ).isoformat(),
                        carried_scaled_down=action == "scale_up",
                    ),
                    id=action,
                    task_queue="workspace-action-history",
                )

                async def active(handle=handle, action=action):
                    while True:
                        status = await handle.query("status")
                        if status["phase"] == (
                            "ScaledDown" if action == "scale_up" else "Active"
                        ):
                            return
                        await asyncio.sleep(0.02)

                await asyncio.wait_for(active(), 10)
                await handle.execute_update(action)
                if action == "delete":
                    with pytest.raises(WorkflowFailureError):
                        await asyncio.wait_for(handle.result(), 10)
                    status = await handle.query("status")
                    assert status["phase"] == "Failed"
                    assert status["last_error"] == "namespace deletion denied"
                    assert status["step"] == "Environment cleanup failed"
                else:

                    async def settled(handle=handle):
                        while True:
                            status = await handle.query("status")
                            if status["last_error"]:
                                return status
                            await asyncio.sleep(0.02)

                    status = await asyncio.wait_for(settled(), 10)
                    assert status["phase"] == (
                        "ScaledDown" if action == "scale_up" else "Active"
                    )
                    assert (
                        await handle.describe()
                    ).status == WorkflowExecutionStatus.RUNNING
                    assert status["failure"]["message"] in status["last_error"]
                    await handle.execute_update("delete")
                    await asyncio.wait_for(handle.result(), 10)
                history = await handle.fetch_history()
                failures = events(history, "activity_task_failed_event_attributes")
                assert len(failures) == 1
                assert (
                    status["failure"]["scheduledEventId"]
                    == failures[
                        0
                    ].activity_task_failed_event_attributes.scheduled_event_id
                )
                scheduled = events(history, "activity_task_scheduled_event_attributes")
                assert (
                    scheduled[
                        -1
                    ].activity_task_scheduled_event_attributes.activity_type.name
                    == "delete_workspace_namespace"
                )
                assert not scheduled[-1].HasField("user_metadata")
                await Replayer(
                    workflows=[cls], workflow_runner=create_workflow_runner()
                ).replay_workflow(history)

    asyncio.run(verify())


def test_workspace_failure_compensation_cancellation_and_legacy_replay():
    scenario = {}
    apply_started = cleanup_started = cleanup_release = None

    @activity.defn(name="resolve_source_images")
    async def images(
        request: messages.ResolveSourceImagesRequest,
    ) -> messages.ResolveSourceImagesResult:
        if scenario["mode"] == "images":
            raise ApplicationError(
                "no running source images",
                type="ConfigurationError",
                non_retryable=True,
            )
        return messages.ResolveSourceImagesResult({"worker": "image"})

    @activity.defn(name="build_workspace_plan")
    async def plan(
        request: messages.BuildWorkspacePlanRequest,
    ) -> messages.BuildWorkspacePlanResult:
        return messages.BuildWorkspacePlanResult(
            resources=[{"kind": "Namespace", "metadata": {"name": "workspace"}}],
            task_queue="workspace",
        )

    @activity.defn(name="apply_workspace_resource")
    async def apply(
        request: messages.ApplyWorkspaceResourceRequest,
    ) -> messages.ApplyWorkspaceResourceResult:
        scenario["created"] = True
        if scenario["mode"] == "apply":
            raise ApplicationError(
                "response lost after namespace creation",
                type="ApplyFailed",
                non_retryable=True,
            )
        if scenario["mode"] == "cancel-provision":
            apply_started.set()
            await asyncio.Event().wait()
        return messages.ApplyWorkspaceResourceResult("created")

    @activity.defn(name="wait_workspace_ready")
    async def ready(
        request: messages.WaitWorkspaceReadyRequest,
    ) -> messages.WaitWorkspaceReadyResult:
        raise ApplicationError(
            "worker pods never became ready",
            type="ConfigurationError",
            non_retryable=True,
        )

    @activity.defn(name="delete_workspace_namespace")
    async def delete(
        request: messages.DeleteWorkspaceNamespaceRequest,
    ) -> messages.DeleteWorkspaceNamespaceResult:
        scenario["deleted"] += 1
        if scenario["mode"] in {"cancel-cleanup", "legacy-cancel-cleanup"}:
            cleanup_started.set()
            try:
                await cleanup_release.wait()
            except asyncio.CancelledError:
                scenario["cleanup_cancelled"] = True
                raise
        if scenario["mode"] == "cleanup-failed":
            raise ApplicationError(
                "namespace delete denied", type="DeleteDenied", non_retryable=True
            )
        scenario["created"] = False
        return messages.DeleteWorkspaceNamespaceResult("deleted")

    async def verify():
        nonlocal apply_started, cleanup_started, cleanup_release
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            for mode in (
                "images",
                "apply",
                "ready",
                "cleanup-failed",
                "cancel-provision",
                "cancel-cleanup",
                "legacy-ready",
                "legacy-cancel-cleanup",
            ):
                scenario.update(
                    mode=mode, created=False, deleted=0, cleanup_cancelled=False
                )
                apply_started, cleanup_started, cleanup_release = (
                    asyncio.Event(),
                    asyncio.Event(),
                    asyncio.Event(),
                )
                legacy = mode.startswith("legacy")
                cls = (
                    LegacyWorkspaceWorkflow
                    if legacy
                    else crashable_workspace.CrashableWorkspaceWorkflow
                )
                async with Worker(
                    env.client,
                    task_queue="workspace-history",
                    workflows=[cls],
                    activities=[images, plan, apply, ready, delete],
                    workflow_runner=create_workflow_runner(),
                ):
                    handle = await env.client.start_workflow(
                        cls.run,
                        messages.EnsureWorkspaceRequest("demo", "user@temporal.io"),
                        id=mode,
                        task_queue="workspace-history",
                    )
                    if mode == "cancel-provision":
                        await asyncio.wait_for(apply_started.wait(), 10)
                        await handle.cancel()
                    if mode in {"cancel-cleanup", "legacy-cancel-cleanup"}:
                        await asyncio.wait_for(cleanup_started.wait(), 10)
                        await handle.cancel()
                        # Query processing follows the cancellation workflow task.
                        await handle.query("status")
                        if not legacy:
                            assert (
                                await handle.describe()
                            ).status == WorkflowExecutionStatus.RUNNING
                        cleanup_release.set()
                    with pytest.raises(WorkflowFailureError) as raised:
                        await asyncio.wait_for(handle.result(), 10)
                    status = await handle.query("status")
                    if not legacy:
                        assert status["compensation_status"]["phase"] == (
                            "NotRequired"
                            if mode == "images"
                            else "Error"
                            if mode == "cleanup-failed"
                            else "Completed"
                        )
                        if mode != "cancel-provision":
                            assert status["failure"]["message"] == status["last_error"]
                        if "cancel" not in mode:
                            assert status["failure"]["message"] in str(
                                raised.value.__cause__.__cause__
                            )
                        if mode == "cleanup-failed":
                            assert (
                                status["failure"]["message"]
                                == "worker pods never became ready"
                            )
                            assert (
                                status["compensation_status"]["failures"][0]["message"]
                                == "namespace delete denied"
                            )
                        if "cancel" in mode:
                            assert (
                                await handle.describe()
                            ).status == WorkflowExecutionStatus.CANCELED
                            assert not scenario["cleanup_cancelled"]
                    assert scenario["deleted"] == (0 if mode == "images" else 1)
                    if mode not in {"cleanup-failed", "legacy-cancel-cleanup"}:
                        assert not scenario["created"]
                    history = await handle.fetch_history()
                    scheduled = events(
                        history, "activity_task_scheduled_event_attributes"
                    )
                    if mode != "images":
                        assert (
                            scheduled[
                                -1
                            ].activity_task_scheduled_event_attributes.activity_type.name
                            == "delete_workspace_namespace"
                        )
                        if not legacy:
                            summary = await env.client.data_converter.decode(
                                [scheduled[-1].user_metadata.summary]
                            )
                            assert summary == [
                                "Compensate failed workspace provision: delete namespace"
                            ]
                            cancel_requests = events(
                                history,
                                "activity_task_cancel_requested_event_attributes",
                            )
                            cleanup_id = scheduled[-1].event_id
                            assert any(
                                event.activity_task_cancel_requested_event_attributes.scheduled_event_id
                                == cleanup_id
                                for event in cancel_requests
                            ) == (mode == "legacy-cancel-cleanup")
                    if mode not in {"cancel-provision", "legacy-cancel-cleanup"}:
                        assert len(
                            events(history, "activity_task_failed_event_attributes")
                        ) == (2 if mode == "cleanup-failed" else 1)
                    if legacy:
                        assert not events(history, "marker_recorded_event_attributes")
                    await Replayer(
                        workflows=[crashable_workspace.CrashableWorkspaceWorkflow],
                        workflow_runner=create_workflow_runner(),
                    ).replay_workflow(history)

    asyncio.run(verify())
