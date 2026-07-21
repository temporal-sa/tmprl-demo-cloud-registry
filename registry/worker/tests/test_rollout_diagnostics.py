from __future__ import annotations

import asyncio
import gzip
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import boto3

from demo_registry_operator import kube, resources
from demo_registry_operator.activities import rollout_diagnostics
import demo_registry_operator.workflows.demo_project_kubernetes_runtime as runtime_workflow


def state(*, waiting=None, running=None, terminated=None):
    return SimpleNamespace(
        waiting=waiting,
        running=running,
        terminated=terminated,
    )


def test_collect_namespace_diagnostics_captures_previous_crash_log(monkeypatch):
    now = datetime(2026, 7, 20, 18, 6, tzinfo=UTC)
    container_status = SimpleNamespace(
        name="worker",
        ready=False,
        restart_count=4,
        image="example/worker:git-failed",
        image_id="sha256:image",
        container_id="containerd://current",
        state=state(
            waiting=SimpleNamespace(reason="CrashLoopBackOff", message="back-off")
        ),
        last_state=state(
            terminated=SimpleNamespace(
                exit_code=1,
                signal=None,
                reason="Error",
                message=None,
                started_at=now,
                finished_at=now,
                container_id="containerd://previous",
            )
        ),
    )
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name="worker-failed",
            uid="pod-uid",
            namespace="candidate-ns",
            creation_timestamp=now,
            labels={"platform.temporal.io/rollout-version": "git-failed"},
        ),
        spec=SimpleNamespace(
            node_name="node-1",
            service_account_name="worker",
            init_containers=[],
            containers=[
                SimpleNamespace(
                    name="worker",
                    image="example/worker:git-failed",
                    env=[SimpleNamespace(name="SECRET", value="must-not-leak")],
                )
            ],
            ephemeral_containers=[],
        ),
        status=SimpleNamespace(
            phase="Running",
            reason=None,
            message=None,
            host_ip="10.0.0.1",
            pod_ip="10.0.0.2",
            start_time=now,
            conditions=[],
            init_container_statuses=[],
            container_statuses=[container_status],
            ephemeral_container_statuses=[],
        ),
    )
    event = SimpleNamespace(
        metadata=SimpleNamespace(creation_timestamp=now),
        type="Warning",
        reason="BackOff",
        message="Back-off restarting failed container worker",
        count=4,
        event_time=None,
        first_timestamp=now,
        last_timestamp=now,
        action=None,
        reporting_controller="kubelet",
        source=SimpleNamespace(component="kubelet", host="node-1"),
        involved_object=SimpleNamespace(
            api_version="v1",
            kind="Pod",
            namespace="candidate-ns",
            name="worker-failed",
            uid="pod-uid",
        ),
    )

    class CoreApi:
        def list_namespaced_pod(self, namespace, **kwargs):
            assert namespace == "candidate-ns"
            return SimpleNamespace(
                items=[pod], metadata=SimpleNamespace(_continue=None)
            )

        def read_namespaced_pod_log(self, name, namespace, **kwargs):
            assert name == "worker-failed"
            assert namespace == "candidate-ns"
            return (
                "ValueError: DATABRICKS_EMBEDDING_ENDPOINT is required\n"
                if kwargs["previous"]
                else "starting worker\n"
            )

        def list_namespaced_event(self, namespace, **kwargs):
            assert namespace == "candidate-ns"
            return SimpleNamespace(
                items=[event], metadata=SimpleNamespace(_continue=None)
            )

    monkeypatch.setattr(
        kube,
        "_client_module",
        lambda: SimpleNamespace(CoreV1Api=lambda: CoreApi()),
    )
    monkeypatch.setattr(kube, "load_kubernetes_config", lambda: None)
    progress = []

    result = kube.collect_namespace_diagnostics(
        "candidate-ns",
        label_selector="platform.temporal.io/rollout-version=git-failed",
        progress_callback=progress.append,
    )

    assert result["summary"] == {
        "podCount": 1,
        "eventCount": 1,
        "logEntryCount": 2,
        "logErrorCount": 0,
        "podsTruncated": False,
        "eventsTruncated": False,
        "logsTruncated": False,
        "partial": False,
    }
    pod_result = result["pods"][0]
    assert pod_result["containerStatuses"][0]["restartCount"] == 4
    assert pod_result["containerStatuses"][0]["lastState"]["exitCode"] == 1
    assert [entry["stream"] for entry in pod_result["logs"]] == [
        "current",
        "previous",
    ]
    assert "DATABRICKS_EMBEDDING_ENDPOINT" in pod_result["logs"][1]["log"]
    assert "must-not-leak" not in json.dumps(result)
    assert result["events"][0]["reason"] == "BackOff"
    assert progress[-1]["phase"] == "CollectingPodLogs"


def test_capture_rollout_diagnostics_uploads_private_gzipped_artifact(monkeypatch):
    config = SimpleNamespace(
        cluster="blue",
        rollout_diagnostics_bucket="tmprl-dem-cld-rollout-diagnostics",
        aws_region="us-west-1",
    )
    monkeypatch.setattr(
        rollout_diagnostics.WorkerConfig,
        "from_env",
        lambda: config,
    )
    monkeypatch.setattr(rollout_diagnostics.activity, "heartbeat", lambda *_: None)
    monkeypatch.setattr(
        rollout_diagnostics.kube,
        "collect_namespace_diagnostics",
        lambda namespace, **kwargs: {
            "namespace": namespace,
            "pods": [],
            "events": [],
            "summary": {
                "podCount": 0,
                "eventCount": 0,
                "logEntryCount": 0,
                "partial": False,
            },
        },
    )
    uploaded = {}

    class S3:
        def put_object(self, **kwargs):
            uploaded.update(kwargs)
            return {"ETag": '"etag"'}

    monkeypatch.setattr(
        boto3.session,
        "Session",
        lambda **kwargs: SimpleNamespace(
            client=lambda service, **client_kwargs: S3()
        ),
    )

    result = rollout_diagnostics.capture_rollout_diagnostics(
        rollout_diagnostics.CaptureRolloutDiagnosticsRequest(
            project="golden-retriever",
            candidate_version="git-failed-key-1234",
            namespace="tmprl-dem-cld-golden-retriever-failed",
            failure_phase="WorkerRolloutFailed",
            failure_status={"workerDeploymentStatus": {"phase": "TimedOut"}},
        )
    )

    assert result.status["phase"] == "Captured"
    assert result.status["s3Uri"].startswith(
        "s3://tmprl-dem-cld-rollout-diagnostics/clusters/blue/projects/"
    )
    assert uploaded["ServerSideEncryption"] == "AES256"
    assert uploaded["ContentEncoding"] == "gzip"
    artifact = json.loads(gzip.decompress(uploaded["Body"]))
    assert artifact["failure"]["phase"] == "WorkerRolloutFailed"
    assert artifact["failure"]["status"]["workerDeploymentStatus"]["phase"] == (
        "TimedOut"
    )
    assert artifact["kubernetes"]["namespace"] == (
        "tmprl-dem-cld-golden-retriever-failed"
    )


def test_workflow_captures_diagnostics_for_candidate_namespace(monkeypatch):
    seen = {}

    async def fake_execute_activity(activity, request, **kwargs):
        seen["activity"] = activity
        seen["request"] = request
        seen["kwargs"] = kwargs
        return SimpleNamespace(
            status={"phase": "Captured", "s3Uri": "s3://bucket/key"}
        )

    monkeypatch.setattr(
        runtime_workflow.workflow,
        "execute_activity",
        fake_execute_activity,
    )
    runtime = runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()
    result = asyncio.run(
        runtime._capture_failed_candidate_diagnostics(
            "golden-retriever",
            {"metadata": {"name": "golden-retriever"}},
            {
                "candidateRolloutStatus": {
                    "imageVersion": "git-failed",
                    "runtimeNamespace": "candidate-ns",
                }
            },
            failure_phase="WorkerRolloutFailed",
            failure_status={"workerDeploymentStatus": {"phase": "TimedOut"}},
        )
    )

    assert result["phase"] == "Captured"
    assert seen["activity"] is rollout_diagnostics.capture_rollout_diagnostics
    assert seen["request"].namespace == "candidate-ns"
    assert seen["request"].candidate_version == "git-failed"
    assert seen["request"].label_selector is None
    assert seen["kwargs"]["heartbeat_timeout"].total_seconds() == 30


def test_failed_status_exposes_diagnostics_reference():
    runtime = runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()
    status = runtime._candidate_not_ready_status(
        {"candidateRolloutStatus": {"imageVersion": "git-failed"}},
        {"phase": "TimedOut", "ok": False},
        {
            "diagnosticsStatus": {
                "phase": "Captured",
                "s3Uri": "s3://bucket/key.json.gz",
            },
            "rollbackStatus": {"phase": "Restored"},
            "pruneStatus": {"phase": "Deleted", "resources": []},
        },
    )

    assert status["diagnosticsStatus"] == {
        "phase": "Captured",
        "s3Uri": "s3://bucket/key.json.gz",
    }
    assert status["lastFailedDiagnostics"] == status["diagnosticsStatus"]


def test_last_failed_diagnostics_survives_later_successful_reconcile():
    last_failed = {
        "phase": "Captured",
        "s3Uri": "s3://bucket/old-failure.json.gz",
    }

    status = runtime_workflow.carry_forward_last_failed_diagnostics(
        {"phase": "Ready"},
        {"lastFailedDiagnostics": last_failed},
    )

    assert status == {
        "phase": "Ready",
        "lastFailedDiagnostics": last_failed,
    }


def test_self_rendered_operator_inherits_diagnostics_bucket(monkeypatch):
    monkeypatch.setenv(
        "DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET",
        "tmprl-dem-cld-rollout-diagnostics",
    )
    demo_project = {
        "metadata": {
            "name": "registry-operator",
            "annotations": {"registry.tmprl-demo.cloud/catalog": "true"},
        }
    }

    env = resources.platform_env_vars(
        demo_project,
        "registry-operator",
        "example/operator:git-new",
        {},
    )

    assert {
        "name": "DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET",
        "value": "tmprl-dem-cld-rollout-diagnostics",
    } in env


def test_catalog_status_exposes_diagnostics_reference():
    entry = resources.catalog_status_entry(
        {"metadata": {"name": "golden-retriever"}},
        {
            "phase": "WorkerRolloutFailed",
            "diagnosticsStatus": {
                "phase": "Captured",
                "s3Uri": "s3://bucket/key.json.gz",
            },
        },
    )

    assert entry["diagnostics"] == {
        "phase": "Captured",
        "s3Uri": "s3://bucket/key.json.gz",
    }
