"""Run with DEMO_REGISTRY_TEST_TEMPORAL_HISTORY=1 and an installed Temporal CLI."""

from __future__ import annotations

import asyncio
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest
from demo_registry_operator.activities import reconcile, revision, rollout_diagnostics
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows import demo_project, messages
from demo_registry_operator.workflows.demo_project_kubernetes_runtime import (
    DemoProjectKubernetesRuntimeWorkflow,
)
from demo_registry_operator.workflows.messages import (
    DemoProjectKubernetesRuntimeRequest,
)
from temporalio import activity, workflow
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker


@workflow.defn(sandboxed=False)
class ProjectFailureHistoryWorkflow(demo_project.DemoProjectWorkflow):
    """Exercise the entity loop with isolated provisioning and compensation."""

    @workflow.init
    def __init__(self, request: messages.DemoProjectRunRequest) -> None:
        super().__init__(request)

    @workflow.run
    async def run(self, request: messages.DemoProjectRunRequest) -> None:
        await super().run(request)

    async def _reconcile_external_resources(self, demo_project, **kwargs):
        await workflow.execute_activity(
            "test_provision", start_to_close_timeout=timedelta(seconds=5)
        )

    async def _compensate_first_time_external_resources(self):
        await workflow.execute_activity(
            "test_compensate", start_to_close_timeout=timedelta(seconds=5)
        )


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal dev server",
)
def test_failed_gates_have_failure_events_then_compensation_and_legacy_histories_replay(
    monkeypatch,
):
    scenario = {"gate": "worker", "legacy": False}
    monkeypatch.setattr(
        reconcile.WorkerConfig,
        "from_env",
        lambda: SimpleNamespace(readiness_timeout_seconds=0, readiness_poll_seconds=1),
    )
    monkeypatch.setattr(reconcile, "smoke_check_retry_window_seconds", lambda _: 0)
    monkeypatch.setattr(
        reconcile,
        "read_candidate_deployments",
        lambda namespace, deployments: {
            "worker": {"name": "worker-new", "ready": scenario["gate"] != "readiness"}
        },
    )
    monkeypatch.setattr(
        reconcile,
        "read_worker_deployments",
        lambda namespace, deployments: {
            "worker": {
                "name": "worker-new",
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "False",
                        "reason": "InvalidSpec",
                        "message": "Invalid worker configuration",
                    }
                ],
            }
        },
    )

    def failed_http(*args, **kwargs):
        raise HTTPError("https://demo/health", 503, "Unavailable", {}, None)

    monkeypatch.setattr(reconcile, "urlopen", failed_http)

    @activity.defn(name="plan_candidate_revision")
    async def plan(
        request: revision.PlanCandidateRevisionRequest,
    ) -> revision.CandidateRevisionPlan:
        return revision.CandidateRevisionPlan(
            ready=False, phase="Skipped", message="Revision setup omitted in gate test"
        )

    @activity.defn(name="apply_demo_project_resources")
    async def apply(
        request: reconcile.ApplyDemoProjectResourcesRequest,
    ) -> reconcile.DemoProjectStatusResult:
        return reconcile.DemoProjectStatusResult(
            {
                "phase": "WorkerRolloutPending"
                if scenario["gate"] == "worker"
                else "SmokeCheckPending",
                "desired": {"host": "demo"},
                "candidateRolloutStatus": {
                    "imageVersion": "git-new",
                    "runtimeNamespace": "candidate",
                    "deployments": {"worker": "worker-new"},
                    "workerDeployments": {"worker": "worker-new"},
                },
            }
        )

    @activity.defn(name="capture_rollout_diagnostics")
    async def capture(
        request: rollout_diagnostics.CaptureRolloutDiagnosticsRequest,
    ) -> reconcile.ActivityStatusResult:
        return reconcile.ActivityStatusResult(
            {"phase": "Captured", "s3Uri": "s3://test/failure.json.gz"}
        )

    @activity.defn(name="restore_active_runtime_routes")
    async def restore(
        request: reconcile.RestoreActiveRuntimeRoutesRequest,
    ) -> reconcile.ActivityStatusResult:
        return reconcile.ActivityStatusResult({"phase": "Restored"})

    @activity.defn(name="delete_runtime_candidate_resources")
    async def delete(
        request: reconcile.DeleteRuntimeCandidateResourcesRequest,
    ) -> reconcile.ActivityStatusResult:
        return reconcile.ActivityStatusResult({"phase": "Deleted", "resources": []})

    @activity.defn(name="rollback_worker_deployments")
    async def rollback(
        request: reconcile.WorkerDeploymentRollbackRequest,
    ) -> reconcile.ActivityStatusResult:
        return reconcile.ActivityStatusResult({"phase": "RolledBack"})

    def run_gate(fn, request):
        try:
            return fn(request)
        except ApplicationError as error:
            if not scenario["legacy"]:
                raise
            return reconcile.ActivityStatusResult(error.details[0]["status"])

    @activity.defn(name="wait_for_worker_deployment_rollouts")
    def worker_gate(
        request: reconcile.WorkerDeploymentRolloutRequest,
    ) -> reconcile.ActivityStatusResult:
        return run_gate(reconcile.wait_for_worker_deployment_rollouts, request)

    @activity.defn(name="wait_for_candidate_readiness")
    def readiness_gate(
        request: reconcile.CandidateReadinessRequest,
    ) -> reconcile.ActivityStatusResult:
        return run_gate(reconcile.wait_for_candidate_readiness, request)

    @activity.defn(name="run_smoke_check")
    def smoke_gate(
        request: reconcile.SmokeCheckRequest,
    ) -> reconcile.ActivityStatusResult:
        return run_gate(reconcile.run_smoke_check, request)

    async def verify():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with ThreadPoolExecutor(max_workers=5) as executor:
                async with Worker(
                    env.client,
                    task_queue="rollout-failure-history-test",
                    workflows=[DemoProjectKubernetesRuntimeWorkflow],
                    workflow_runner=create_workflow_runner(),
                    activities=[
                        plan,
                        apply,
                        capture,
                        restore,
                        delete,
                        rollback,
                        worker_gate,
                        readiness_gate,
                        smoke_gate,
                    ],
                    activity_executor=executor,
                ):
                    for gate, failure_phase, activity_name in (
                        (
                            "worker",
                            "WorkerRolloutFailed",
                            "wait_for_worker_deployment_rollouts",
                        ),
                        (
                            "readiness",
                            "CandidateNotReady",
                            "wait_for_candidate_readiness",
                        ),
                        ("smoke", "SmokeCheckFailed", "run_smoke_check"),
                    ):
                        for legacy in (False, True):
                            scenario.update(gate=gate, legacy=legacy)
                            handle = await env.client.start_workflow(
                                DemoProjectKubernetesRuntimeWorkflow.run,
                                DemoProjectKubernetesRuntimeRequest(
                                    project="demo",
                                    demoProject={
                                        "metadata": {"name": "demo"},
                                        "spec": {
                                            "smokeCheck": {
                                                "path": "/health",
                                                "expectedStatus": 200,
                                            }
                                        },
                                    },
                                    externalResources={},
                                    buildStatus={},
                                    previousStatus={
                                        "activeImageVersion": "git-old",
                                        "activeImages": {
                                            "worker": "registry/worker:old"
                                        },
                                    },
                                ),
                                id=f"rollout-{gate}-{legacy}",
                                task_queue="rollout-failure-history-test",
                            )
                            result = await asyncio.wait_for(handle.result(), 20)
                            assert result.status["phase"] == failure_phase
                            assert result.status["pruneStatus"]["phase"] == "Deleted"
                            history = await handle.fetch_history()
                            failures = [
                                event
                                for event in history.events
                                if event.HasField(
                                    "activity_task_failed_event_attributes"
                                )
                            ]
                            assert len(failures) == (0 if legacy else 1)
                            if not legacy:
                                failed = failures[0]
                                scheduled_id = failed.activity_task_failed_event_attributes.scheduled_event_id
                                scheduled = next(
                                    event
                                    for event in history.events
                                    if event.event_id == scheduled_id
                                )
                                assert (
                                    scheduled.activity_task_scheduled_event_attributes.activity_type.name
                                    == activity_name
                                )
                                cleanup = next(
                                    event
                                    for event in history.events
                                    if event.HasField(
                                        "activity_task_scheduled_event_attributes"
                                    )
                                    and event.activity_task_scheduled_event_attributes.activity_type.name
                                    == "restore_active_runtime_routes"
                                )
                                assert cleanup.event_id > failed.event_id
                                summary = await env.client.data_converter.decode(
                                    [cleanup.user_metadata.summary]
                                )
                                assert summary == [
                                    "Compensate failed rollout: restore active routes"
                                ]
                            await Replayer(
                                workflows=[DemoProjectKubernetesRuntimeWorkflow],
                                workflow_runner=create_workflow_runner(),
                            ).replay_workflow(history)

    asyncio.run(verify())


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal dev server",
)
def test_entity_accepts_another_reconcile_after_compensation():
    attempts = 0
    compensations = 0

    @activity.defn(name="test_provision")
    async def provision():
        nonlocal attempts
        attempts += 1
        raise ApplicationError(
            f"provision attempt {attempts} failed",
            type="ProvisionDenied",
            non_retryable=True,
        )

    @activity.defn(name="test_compensate")
    async def compensate():
        nonlocal compensations
        compensations += 1

    @activity.defn(name="record_demo_project_status")
    async def record_project(
        request: reconcile.RecordDemoProjectStatusRequest,
    ) -> reconcile.DemoProjectStatusResult:
        return reconcile.DemoProjectStatusResult(request.status)

    @activity.defn(name="record_catalog_project_status")
    async def record_catalog(
        request: reconcile.RecordDemoProjectStatusRequest,
    ) -> reconcile.DemoProjectStatusResult:
        return reconcile.DemoProjectStatusResult(request.status)

    async def verify():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env, Worker(
            env.client,
            task_queue="entity-failure-history-test",
            workflows=[ProjectFailureHistoryWorkflow],
            activities=[provision, compensate, record_project, record_catalog],
        ):
            handle = await env.client.start_workflow(
                ProjectFailureHistoryWorkflow.run,
                messages.DemoProjectRunRequest(project_name="demo"),
                id="entity-failure-history",
                task_queue="entity-failure-history-test",
            )
            for attempt in (1, 2):
                await handle.signal(
                    "reconcile",
                    messages.DemoProjectReconcileSignal(
                        demoProject={
                            "metadata": {"name": "demo", "generation": attempt},
                            "spec": {},
                        }
                    ),
                )

                async def settled(expected_attempt=attempt):
                    while True:
                        status = await handle.query("status")
                        if (
                            status.get("message")
                            == f"provision attempt {expected_attempt} failed"
                        ):
                            return status
                        await asyncio.sleep(0.02)

                status = await asyncio.wait_for(settled(), 10)
                assert status["phase"] == "WorkerRolloutFailed"
                assert status["compensationStatus"]["phase"] == "Completed"
                assert (
                    await handle.describe()
                ).status == WorkflowExecutionStatus.RUNNING
            assert compensations == 2
            await handle.signal("shutdown", messages.ShutdownSignal())
            await asyncio.wait_for(handle.result(), 10)
            history = await handle.fetch_history()
            assert (
                sum(
                    event.HasField("activity_task_failed_event_attributes")
                    for event in history.events
                )
                == 2
            )
            await Replayer(
                workflows=[ProjectFailureHistoryWorkflow]
            ).replay_workflow(history)

    asyncio.run(verify())
