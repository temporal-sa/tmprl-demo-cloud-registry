from __future__ import annotations

import inspect
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import demo_registry_operator.workflows.demo_project as demo_project_workflow
import demo_registry_operator.workflows.operator_canary as operator_canary_workflow
from demo_registry_operator.resources import (
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
)
from demo_registry_operator.workflows.demo_project import (
    DemoProjectWorkflow,
    demo_project_with_temporal_api_key,
    remember_completed_reconcile_identity,
    reconcile_identity,
    status_reconcile_identity,
    temporal_api_key_rotation_state,
    temporal_candidate_secret_token,
)
from demo_registry_operator.workflows.operator_canary import (
    registry_operator_canary_branch_request,
)
from demo_registry_operator.workflows.messages import (
    DemoProjectReconcileSignal,
    DemoProjectRunRequest,
    ForceReconcileSignal,
    RegistryOperatorCanaryRequest,
)


def project(
    *,
    generation: int = 1,
    source_commit_sha: str = "abcdef1234567890abcdef1234567890abcdef12",
    source_artifact_digest: str = "sha256:aaaabbbb",
) -> dict[str, Any]:
    return {
        "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
        "kind": "DemoProject",
        "metadata": {
            "name": "demo",
            "generation": generation,
            "annotations": {
                SOURCE_COMMIT_ANNOTATION: source_commit_sha,
                SOURCE_ARTIFACT_DIGEST_ANNOTATION: source_artifact_digest,
            },
        },
        "spec": {
            "components": {
                "worker": {
                    "image": "example.com/demo/worker:latest",
                    "worker": True,
                },
            },
        },
    }


def workflow_instance(name: str = "demo") -> DemoProjectWorkflow:
    return DemoProjectWorkflow(DemoProjectRunRequest(project_name=name))


def reconcile_signal(demo_project: dict[str, Any]) -> DemoProjectReconcileSignal:
    return DemoProjectReconcileSignal(demoProject=demo_project)


def test_reconcile_signal_noops_when_same_identity_is_pending():
    demo_project = project()
    duplicate = deepcopy(demo_project)
    workflow = workflow_instance()

    workflow.reconcile(reconcile_signal(demo_project))
    workflow.reconcile(reconcile_signal(duplicate))

    assert workflow._pending_spec is demo_project
    assert workflow._pending_reconcile_identity == reconcile_identity(demo_project)


def test_reconcile_signal_noops_when_same_identity_is_currently_pursued():
    demo_project = project()
    workflow = workflow_instance()
    workflow._current_reconcile_identity = reconcile_identity(demo_project)

    workflow.reconcile(reconcile_signal(deepcopy(demo_project)))

    assert workflow._pending_spec is None
    assert workflow._pending_reconcile_identity is None


def test_reconcile_signal_noops_when_same_identity_is_completed():
    demo_project = project()
    workflow = workflow_instance()
    workflow._completed_reconcile_identity = reconcile_identity(demo_project)

    workflow.reconcile(reconcile_signal(deepcopy(demo_project)))

    assert workflow._pending_spec is None
    assert workflow._pending_reconcile_identity is None


def test_force_reconcile_signal_sets_pending_request_and_dedupes():
    workflow = workflow_instance()

    workflow.force_reconcile(
        ForceReconcileSignal(token=" token-1 ", reason=" manual retry ")
    )
    workflow.force_reconcile(
        ForceReconcileSignal(token="token-1", reason="ignored duplicate")
    )

    assert workflow._pending_force_reconcile == {
        "token": "token-1",
        "reason": "manual retry",
    }

    workflow._pending_force_reconcile = None
    workflow._completed_force_reconcile_token = "token-1"
    workflow.force_reconcile(
        ForceReconcileSignal(token="token-1", reason="already completed")
    )

    assert workflow._pending_force_reconcile is None


def test_failed_reconcile_identity_is_not_remembered_as_completed():
    demo_project = project()
    identity = reconcile_identity(demo_project)
    workflow = workflow_instance()
    workflow._current_reconcile_identity = identity
    workflow._current_force_reconcile_token = "manual-1"
    workflow._status = {"phase": "Error"}

    workflow._complete_current_reconcile()
    workflow.reconcile(reconcile_signal(deepcopy(demo_project)))

    assert workflow._completed_reconcile_identity is None
    assert workflow._completed_force_reconcile_token is None
    assert workflow._pending_spec is not None
    assert workflow._pending_reconcile_identity == identity


def test_successful_force_reconcile_token_is_remembered_as_completed():
    demo_project = project()
    workflow = workflow_instance()
    workflow._current_reconcile_identity = reconcile_identity(demo_project)
    workflow._current_force_reconcile_token = "manual-1"
    workflow._status = {"phase": "Promoted"}

    workflow._complete_current_reconcile()

    assert workflow._completed_reconcile_identity == reconcile_identity(demo_project)
    assert workflow._completed_force_reconcile_token == "manual-1"


def test_failed_status_is_not_a_rememberable_completed_identity():
    assert remember_completed_reconcile_identity({"phase": "Error"}) is False
    assert (
        remember_completed_reconcile_identity({"phase": "SmokeCheckFailed"}) is False
    )
    assert remember_completed_reconcile_identity({"phase": "Promoted"}) is True


def test_reconcile_signal_accepts_same_digest_with_different_sha():
    current = project(source_commit_sha="1111111111111111111111111111111111111111")
    updated = project(source_commit_sha="2222222222222222222222222222222222222222")
    workflow = workflow_instance()
    workflow._current_reconcile_identity = reconcile_identity(current)

    workflow.reconcile(reconcile_signal(updated))

    assert workflow._pending_spec is updated
    assert workflow._pending_reconcile_identity == reconcile_identity(updated)


def test_reconcile_signal_accepts_same_source_with_changed_spec_key():
    current = project(generation=1)
    updated = project(generation=2)
    workflow = workflow_instance()
    workflow._completed_reconcile_identity = reconcile_identity(current)

    workflow.reconcile(reconcile_signal(updated))

    assert workflow._pending_spec is updated
    assert workflow._pending_reconcile_identity == reconcile_identity(updated)


def test_status_reconcile_identity_recovers_completed_source_identity():
    demo_project = project()
    identity = reconcile_identity(demo_project)

    recovered = status_reconcile_identity(
        {
            "phase": "Promoted",
            "reconcileKey": identity["reconcileKey"],
            "buildStatus": {
                "sourceCommitSha": identity["sourceCommitSha"],
                "sourceArtifactDigest": identity["sourceArtifactDigest"],
            },
        }
    )

    assert recovered == identity


def test_temporal_api_key_status_sets_rotation_state_and_annotations():
    demo_project = project()
    temporal_status = {
        "apiKey": {
            "keyId": "key-abc",
            "expiryTime": "2026-07-13T00:00:00+00:00",
            "createdAt": "2026-06-13T00:00:00+00:00",
            "rotateBeforeExpiryDays": 10,
        }
    }

    annotated = demo_project_with_temporal_api_key(demo_project, temporal_status)
    state = temporal_api_key_rotation_state(temporal_status)

    assert annotated["metadata"]["annotations"][
        "registry.tmprl-demo.cloud/temporal-api-key-id"
    ] == "key-abc"
    assert state == {
        "authMode": "apiKey",
        "keyId": "key-abc",
        "expiryTime": "2026-07-13T00:00:00+00:00",
        "rotateAt": "2026-07-03T00:00:00+00:00",
        "rotateBeforeExpiryDays": 10,
    }


def test_temporal_candidate_secret_token_is_deterministic_and_sequence_scoped():
    temporal_project = SimpleNamespace(
        name="demo",
        namespace="tmprl-dem-cld-demo.acct1",
        auth_mode="apiKey",
        runtime_secret_path="tmprl-dem-cld/demo/temporal",
        mtls_ca_secret_path="tmprl-dem-cld/demo/temporal/ca",
    )
    plan = SimpleNamespace(
        checked_at="2026-06-17T00:00:00+00:00",
        temporal_project=temporal_project,
    )

    token = temporal_candidate_secret_token(
        plan,
        workflow_id="DemoProject/demo",
        run_id="run-1",
        reconcile_sequence=1,
    )

    assert token == temporal_candidate_secret_token(
        plan,
        workflow_id="DemoProject/demo",
        run_id="run-1",
        reconcile_sequence=1,
    )
    assert token != temporal_candidate_secret_token(
        plan,
        workflow_id="DemoProject/demo",
        run_id="run-1",
        reconcile_sequence=2,
    )
    assert len(token) == 20
    int(token, 16)


def test_temporal_cloud_reconcile_state_merges_status_sections():
    state = demo_project_workflow.TemporalCloudReconcileState(
        namespace=demo_project_workflow.external_resources.TemporalCloudNamespaceResult(
            status={
                "phase": "Ready",
                "namespace": {"name": "tmprl-dem-cld-demo", "action": "verified"},
            },
            namespace="tmprl-dem-cld-demo.acct1",
            grpc_address="tmprl-dem-cld-demo.acct1.tmprl.cloud:7233",
            auth_mode="apiKey",
            created=False,
        ),
        service_account=demo_project_workflow.external_resources.TemporalCloudServiceAccountResult(
            status={
                "phase": "Ready",
                "serviceAccount": {"id": "sa-1", "action": "created"},
            },
            service_account_id="sa-1",
            service_account_name="demo-runtime",
            created=True,
        ),
        promote=demo_project_workflow.external_resources.TemporalCloudPromotionResult(
            status={
                "phase": "Ready",
                "apiKey": {"keyId": "key-1"},
                "promoted": {"runtimeSecret": True},
            },
            promoted=True,
        ),
        mirror=demo_project_workflow.external_resources.TemporalCloudStatusResult(
            status={"phase": "Ready", "action": "mirrored"},
        ),
    )

    status = state.merged_status()

    assert status["namespace"]["name"] == "tmprl-dem-cld-demo"
    assert status["serviceAccount"]["id"] == "sa-1"
    assert status["apiKey"]["keyId"] == "key-1"
    assert status["promoted"]["runtimeSecret"] is True
    assert status["runtimeSecretMirror"]["action"] == "mirrored"


def test_registry_operator_canary_branch_request_splits_auth_modes():
    request = RegistryOperatorCanaryRequest(
        projectNamePrefix="operator-canary",
        projectName="manual-canary",
        demoProjectTemplate={"metadata": {"name": "template"}, "spec": {}},
    )

    api_key = registry_operator_canary_branch_request(
        request,
        auth_mode="apiKey",
        workflow_id="gate/run",
        run_id="run-1",
    )
    mtls = registry_operator_canary_branch_request(
        request,
        auth_mode="mtls",
        workflow_id="gate/run",
        run_id="run-1",
    )

    assert api_key.authMode == "apiKey"
    assert api_key.workflowID == "gate/run/apiKey"
    assert api_key.projectNamePrefix == "operator-canary-api-key"
    assert api_key.projectName == "manual-canary-api-key"
    assert mtls.authMode == "mtls"
    assert mtls.workflowID == "gate/run/mtls"
    assert mtls.projectNamePrefix == "operator-canary-mtls"
    assert mtls.projectName == "manual-canary-mtls"


def test_registry_operator_canary_parent_uses_branch_dataclass_fields():
    source = inspect.getsource(
        operator_canary_workflow.RegistryOperatorCanaryWorkflow.run
    )

    assert 'branch_request["workflowID"]' not in source
    assert "branch_request.workflowID" in source


def test_check_worker_version_signal_requests_continue_as_new_when_target_changed(
    monkeypatch,
):
    workflow = workflow_instance()
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(
            is_target_worker_deployment_version_changed=lambda: True
        ),
    )

    workflow.check_worker_version()

    assert workflow._continue_as_new_for_worker_version is True


def test_check_worker_version_signal_noops_when_target_unchanged(monkeypatch):
    workflow = workflow_instance()
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(
            is_target_worker_deployment_version_changed=lambda: False
        ),
    )

    workflow.check_worker_version()

    assert workflow._continue_as_new_for_worker_version is False


def test_worker_update_continue_as_new_uses_initial_auto_upgrade(monkeypatch):
    calls = []
    workflow = workflow_instance()
    workflow._continue_as_new_for_worker_version = True

    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(is_continue_as_new_suggested=lambda: False),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "continue_as_new",
        lambda **kwargs: calls.append(kwargs),
    )

    workflow._continue_as_new_if_needed("demo")

    assert len(calls) == 1
    assert calls[0]["initial_versioning_behavior"] == (
        demo_project_workflow.workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
    )


def test_child_workflow_id_is_scoped_to_reconcile_sequence(monkeypatch):
    workflow = workflow_instance()
    workflow._reconcile_sequence = 7
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="DemoProject/demo"),
    )

    assert (
        workflow._child_workflow_id("artifact-build")
        == "DemoProject/demo/reconcile-7/artifact-build"
    )


def test_demo_project_reconcile_delegates_runtime_paths_to_child_workflows():
    source = inspect.getsource(DemoProjectWorkflow.run)

    assert "DemoProjectArtifactBuildWorkflow.run" in source
    assert "DemoProjectKubernetesRuntimeWorkflow.run" in source
    assert "workflow.execute_child_workflow" in source


def test_temporal_cloud_reconcile_delegates_to_named_steps():
    source = inspect.getsource(DemoProjectWorkflow._reconcile_temporal_cloud)

    assert "workflow.execute_activity" not in source
    assert "_stage_temporal_mtls_ca_if_needed" in source
    assert "_create_or_verify_temporal_cloud_namespace" in source
    assert "_stage_temporal_runtime_credential" in source
    assert "_promote_temporal_runtime_credential_if_needed" in source
    assert "_mirror_temporal_runtime_secret" in source

    compensation_source = inspect.getsource(
        DemoProjectWorkflow._compensate_temporal_cloud_reconcile
    )
    assert "workflow.execute_activity" not in compensation_source
    assert "_compensate_temporal_candidate_api_key" in compensation_source
    assert "_delete_temporal_candidate_secret" in compensation_source
    assert "_compensate_temporal_service_account" in compensation_source
    assert "_compensate_temporal_namespace" in compensation_source
