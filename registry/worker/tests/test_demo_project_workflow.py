from __future__ import annotations

import asyncio
import inspect
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import demo_registry_operator.workflows.demo_project as demo_project_workflow
import demo_registry_operator.workflows.demo_project_kubernetes_runtime as kubernetes_runtime_workflow
import demo_registry_operator.workflows.operator_canary as operator_canary_workflow
from demo_registry_operator.resources import (
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
)
from demo_registry_operator.workflows.demo_project import (
    DemoProjectWorkflow,
    demo_project_with_temporal_api_key,
    demo_project_with_temporal_runtime_secret_path,
    project_has_active_version,
    remember_completed_reconcile_identity,
    previous_reconcile_status,
    previous_temporal_search_attributes,
    reconcile_identity,
    status_reconcile_identity,
    status_with_temporal_runtime_secret_path,
    temporal_api_key_rotation_state,
    temporal_candidate_secret_token,
    temporal_search_attributes_reconcile_status,
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


def test_terminal_rollout_failures_are_remembered_to_settle():
    # SmokeCheckFailed / WorkerRolloutFailed are terminal: the rollout was
    # attempted and failed definitively. Settle (remember the identity) so the
    # entity workflow does not re-attempt the identical rollout; recovery needs
    # a new reconcile identity (new source) or an explicit force_reconcile.
    assert (
        remember_completed_reconcile_identity({"phase": "SmokeCheckFailed"}) is True
    )
    assert (
        remember_completed_reconcile_identity({"phase": "WorkerRolloutFailed"}) is True
    )


def test_transient_failures_are_not_a_rememberable_completed_identity():
    # Non-terminal failures keep reconciling on every signal.
    assert remember_completed_reconcile_identity({"phase": "Error"}) is False
    assert (
        remember_completed_reconcile_identity({"phase": "CandidateNotReady"}) is False
    )
    assert remember_completed_reconcile_identity({"phase": "DeployFailed"}) is False


def test_successful_status_is_a_rememberable_completed_identity():
    assert remember_completed_reconcile_identity({"phase": "Promoted"}) is True
    assert remember_completed_reconcile_identity({"phase": "Ready"}) is True


def test_kubernetes_runtime_failed_status_includes_compensation_status():
    workflow = kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow()

    status = workflow._candidate_not_ready_status(
        {
            "candidateRolloutStatus": {
                "imageVersion": "git-failed",
            },
        },
        {
            "phase": "TimedOut",
            "ok": False,
        },
        {
            "rollbackStatus": {
                "phase": "Restored",
                "appliedResources": [],
            },
            "pruneStatus": {
                "phase": "Deleted",
                "resources": [
                    {
                        "kind": "Deployment",
                        "name": "backend-git-failed",
                    }
                ],
            },
        },
    )

    assert status["phase"] == "CandidateNotReady"
    assert status["rollbackStatus"]["phase"] == "Restored"
    assert status["pruneStatus"]["phase"] == "Deleted"
    assert status["prunedResources"] == [
        {
            "kind": "Deployment",
            "name": "backend-git-failed",
        }
    ]


def test_kubernetes_runtime_failure_paths_cleanup_failed_candidate():
    source = inspect.getsource(
        kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow.run
    )

    assert source.count("_cleanup_failed_candidate(") == 3


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


def test_previous_temporal_search_attributes_reads_managed_names_from_status():
    status = {
        "externalResources": {
            "temporalCloud": {
                "searchAttributes": {
                    "managedNames": ["CreatedByOperator"],
                    "attributes": [
                        {
                            "name": "CreatedByOperator",
                            "type": "double",
                            "managed": True,
                        },
                        {
                            "name": "VerifiedExternal",
                            "type": "keyword",
                            "managed": False,
                        },
                    ],
                }
            }
        }
    }

    assert previous_temporal_search_attributes(status) == {
        "CreatedByOperator": "double"
    }


def test_temporal_search_attribute_status_tracks_owned_creates_and_deletes():
    statuses = [
        {
            "phase": "Ready",
            "searchAttribute": {
                "name": "NewAttribute",
                "type": "keyword",
                "action": "created",
            },
        },
        {
            "phase": "Ready",
            "searchAttribute": {
                "name": "ExistingExternal",
                "type": "double",
                "action": "verified",
            },
        },
        {
            "phase": "Deleted",
            "searchAttribute": {
                "name": "RemovedManaged",
                "type": "bool",
                "action": "deleted",
            },
        },
    ]

    status = temporal_search_attributes_reconcile_status(
        "2026-06-18T00:00:00+00:00",
        namespace="tmprl-dem-cld-demo.acct1",
        desired={
            "ExistingExternal": demo_project_workflow.external_resources.TemporalCloudSearchAttribute(
                name="ExistingExternal",
                type="double",
            ),
            "NewAttribute": demo_project_workflow.external_resources.TemporalCloudSearchAttribute(
                name="NewAttribute",
                type="keyword",
            ),
        },
        previous_managed={"RemovedManaged": "bool"},
        statuses=statuses,
    )

    assert status["phase"] == "Ready"
    assert status["managedNames"] == ["NewAttribute"]
    assert status["deletedNames"] == ["RemovedManaged"]
    by_name = {item["name"]: item for item in status["attributes"]}
    assert by_name["NewAttribute"]["managed"] is True
    assert by_name["ExistingExternal"]["managed"] is False
    assert by_name["RemovedManaged"]["managed"] is False


def test_registry_operator_canary_branch_request_uses_per_template_auth():
    # Each branch is built from a self-contained external template ({name, authMode,
    # demoProjectTemplate}); the template carries its own auth, no shared template.
    request = RegistryOperatorCanaryRequest(
        projectNamePrefix="operator-canary",
        projectName="manual-canary",
    )

    api_key = registry_operator_canary_branch_request(
        request,
        template={
            "name": "api-key",
            "authMode": "apiKey",
            "demoProjectTemplate": {"metadata": {"name": "t"}, "spec": {}},
        },
        workflow_id="gate/run",
        run_id="run-1",
    )
    mtls = registry_operator_canary_branch_request(
        request,
        template={
            "name": "mtls",
            "authMode": "mtls",
            "demoProjectTemplate": {
                "metadata": {"name": "t"},
                "spec": {"temporal": {"auth": {"mode": "mtls"}}},
            },
        },
        workflow_id="gate/run",
        run_id="run-1",
    )

    assert api_key.authMode == "apiKey"
    assert api_key.workflowID == "gate/run/api-key"
    assert api_key.projectNamePrefix == "operator-canary-api-key"
    assert api_key.projectName == "manual-canary-api-key"
    assert api_key.demoProjectTemplate["spec"] == {}
    assert mtls.authMode == "mtls"
    assert mtls.workflowID == "gate/run/mtls"
    assert mtls.projectNamePrefix == "operator-canary-mtls"
    assert mtls.projectName == "manual-canary-mtls"
    assert (
        mtls.demoProjectTemplate["spec"]["temporal"]["auth"]["mode"] == "mtls"
    )


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
    workflow._temporal_runtime_secret_suffix = "stable123"

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
    assert (
        calls[0]["args"][0].carried_temporal_runtime_secret_suffix
        == "stable123"
    )


def test_runtime_secret_suffix_is_carried_from_run_request():
    workflow = DemoProjectWorkflow(
        DemoProjectRunRequest(
            project_name="demo",
            carried_temporal_runtime_secret_suffix="stable123",
        )
    )

    assert workflow._temporal_runtime_secret_suffix == "stable123"


def test_runtime_secret_suffix_recovers_from_persisted_status():
    workflow = workflow_instance()
    demo_project = project()
    demo_project["status"] = {
        "temporalRuntimeSecretPath": "tmprl-dem-cld/demo/temporal-stable123",
    }

    workflow._recover_temporal_runtime_secret_suffix(demo_project)

    assert workflow._temporal_runtime_secret_suffix == "stable123"
    rendered = workflow._demo_project_with_known_runtime_secret_path(demo_project)
    assert rendered["spec"]["temporal"]["runtimeSecretPath"] == (
        "tmprl-dem-cld/demo/temporal-stable123"
    )


def test_runtime_secret_suffix_recovers_from_legacy_nested_status():
    workflow = workflow_instance()
    demo_project = project()
    demo_project["status"] = {
        "externalResources": {
            "secretsManager": {
                "temporalCredentials": {
                    "path": "tmprl-dem-cld/demo/temporal-legacy123",
                }
            }
        }
    }

    workflow._recover_temporal_runtime_secret_suffix(demo_project)

    assert workflow._temporal_runtime_secret_suffix == "legacy123"


def test_previous_reconcile_status_recovers_persisted_active_runtime():
    demo_project = project()
    demo_project["status"] = {
        "phase": "Promoted",
        "activeRuntimeNamespace": "tmprl-dem-cld-demo-old12",
        "activeRevision": {"name": "demo-old12"},
    }

    previous = previous_reconcile_status(
        {"phase": "Starting", "message": "new workflow"},
        demo_project,
    )

    assert previous["phase"] == "Starting"
    assert previous["activeRuntimeNamespace"] == "tmprl-dem-cld-demo-old12"
    assert previous["activeRevision"] == {"name": "demo-old12"}


def test_status_persists_temporal_runtime_secret_path():
    demo_project = demo_project_with_temporal_runtime_secret_path(
        project(),
        "tmprl-dem-cld/demo/temporal-stable123",
    )

    status = status_with_temporal_runtime_secret_path(
        {"phase": "Ready"},
        demo_project,
    )

    assert status["temporalRuntimeSecretPath"] == (
        "tmprl-dem-cld/demo/temporal-stable123"
    )


def test_runtime_secret_path_moves_from_missing_legacy_to_workflow_suffix(
    monkeypatch,
):
    workflow = workflow_instance()
    demo_project = project()
    external_plan = demo_project_workflow.external_resources.plan_external_resources(
        demo_project,
        checked_at="2026-06-20T00:00:00+00:00",
    )
    runtime_secret = (
        demo_project_workflow.external_resources.TemporalRuntimeSecretCheckResult(
            status={},
            runtime_secret_path=external_plan.temporal_project.runtime_secret_path,
            state="MarkedForDeletion",
            exists=False,
            rotate=True,
            rotation_reason="MissingRuntimeSecret",
        )
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "uuid4",
        lambda: SimpleNamespace(hex="abcdef1234567890abcdef1234567890"),
    )

    assert workflow._runtime_secret_path_can_move(external_plan, runtime_secret)

    moved = workflow._external_plan_with_new_runtime_secret_path(external_plan)

    assert workflow._temporal_runtime_secret_suffix == "abcdef123456"
    assert moved.temporal_project.runtime_secret_path.endswith("-abcdef123456")
    assert moved.demo_project["spec"]["temporal"]["runtimeSecretPath"].endswith(
        "-abcdef123456"
    )


def test_runtime_secret_path_does_not_move_after_suffix_is_owned():
    workflow = workflow_instance()
    workflow._temporal_runtime_secret_suffix = "stable123"
    demo_project = demo_project_with_temporal_runtime_secret_path(
        project(),
        "tmprl-dem-cld/demo/temporal-stable123",
    )
    external_plan = demo_project_workflow.external_resources.plan_external_resources(
        demo_project,
        checked_at="2026-06-20T00:00:00+00:00",
    )
    runtime_secret = (
        demo_project_workflow.external_resources.TemporalRuntimeSecretCheckResult(
            status={},
            runtime_secret_path=external_plan.temporal_project.runtime_secret_path,
            state="Missing",
            exists=False,
            rotate=True,
        )
    )

    assert not workflow._runtime_secret_path_can_move(external_plan, runtime_secret)


def test_external_resources_uses_moved_runtime_secret_plan(monkeypatch):
    workflow = workflow_instance()
    source = project()
    checked_at = "2026-06-20T00:00:00+00:00"
    moved_path = "tmprl-dem-cld/demo/temporal-stable123"
    moved_project = demo_project_with_temporal_runtime_secret_path(
        source,
        moved_path,
    )
    moved_plan = demo_project_workflow.external_resources.plan_external_resources(
        moved_project,
        checked_at=checked_at,
    )
    captured: dict[str, Any] = {}

    async def fake_reconcile_ecr_repositories(external_plan):
        captured["ecrPlan"] = external_plan
        return {"phase": "NotRequired"}

    async def fake_reconcile_temporal_cloud(external_plan, *, previous_status):
        captured["temporalInputPlan"] = external_plan
        workflow._tc_plan = moved_plan
        return {
            "phase": "Ready",
            "apiKey": {
                "keyId": "key-1",
                "expiryTime": "2026-07-20T00:00:00+00:00",
            },
        }

    async def fake_execute_activity(activity, request, **kwargs):
        captured["secretsRequest"] = request
        return SimpleNamespace(status={"phase": "Ready"})

    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "now",
        lambda: datetime(2026, 6, 20, tzinfo=UTC),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "execute_activity",
        fake_execute_activity,
    )
    workflow._reconcile_ecr_repositories = fake_reconcile_ecr_repositories
    workflow._reconcile_temporal_cloud = fake_reconcile_temporal_cloud

    result = asyncio.run(
        workflow._reconcile_external_resources(
            source,
            previous_status={},
            force_temporal_auth_rotation=False,
        )
    )

    assert result.demo_project["spec"]["temporal"]["runtimeSecretPath"] == moved_path
    assert captured["secretsRequest"].temporal_credentials.path == moved_path
    assert result.status["secretsManager"]["phase"] == "Ready"


def test_finalization_external_plan_uses_carried_runtime_secret_path():
    workflow = workflow_instance()
    workflow._temporal_runtime_secret_suffix = "stable123"

    plan = workflow._project_finalization_external_plan(
        project(),
        {"checkedAt": "2026-06-20T00:00:00+00:00"},
    )

    assert plan.temporal_project.runtime_secret_path == (
        "tmprl-dem-cld/demo/temporal-stable123"
    )
    assert plan.secrets_manager.temporal_credentials.path == (
        "tmprl-dem-cld/demo/temporal-stable123"
    )
    assert plan.demo_project["spec"]["temporal"]["runtimeSecretPath"] == (
        "tmprl-dem-cld/demo/temporal-stable123"
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
    run_source = inspect.getsource(DemoProjectWorkflow.run)
    build_source = inspect.getsource(DemoProjectWorkflow._build_project_artifact)
    runtime_source = inspect.getsource(DemoProjectWorkflow._reconcile_kubernetes_runtime)
    serverless_source = inspect.getsource(
        DemoProjectWorkflow._reconcile_serverless_lambda_if_needed
    )

    assert "_run_demo_project_reconcile" in run_source
    assert "DemoProjectArtifactBuildWorkflow.run" in build_source
    assert "DemoProjectKubernetesRuntimeWorkflow.run" in runtime_source
    assert "DemoProjectServerlessLambdaWorkflow.run" in serverless_source
    assert "workflow.execute_child_workflow" in (
        build_source + runtime_source + serverless_source
    )


def test_temporal_cloud_reconcile_delegates_to_named_steps():
    source = inspect.getsource(DemoProjectWorkflow._reconcile_temporal_cloud)

    assert "workflow.execute_activity" not in source
    assert "_stage_temporal_mtls_ca_if_needed" in source
    assert "_create_or_verify_temporal_cloud_namespace" in source
    assert "_stage_temporal_runtime_credential" in source
    assert "_promote_temporal_runtime_credential_if_needed" in source
    assert "_mirror_temporal_runtime_secret" in source
    # Region drift is detected from the create-or-verify result and handled by a
    # dedicated teardown step (whose activity calls stay out of this method).
    assert "RegionDrift" in source
    assert "_teardown_temporal_cloud_namespace_on_region_drift" in source

    teardown_source = inspect.getsource(
        DemoProjectWorkflow._teardown_temporal_cloud_namespace_on_region_drift
    )
    assert "delete_temporal_cloud_namespace" in teardown_source
    assert "disable_temporal_cloud_namespace_delete_protection" in teardown_source

    compensation_source = inspect.getsource(
        DemoProjectWorkflow._compensate_temporal_cloud_reconcile
    )
    assert "workflow.execute_activity" not in compensation_source
    assert "_compensate_temporal_candidate_api_key" in compensation_source
    assert "_delete_temporal_candidate_secret" in compensation_source
    assert "_compensate_temporal_service_account" in compensation_source
    assert "_compensate_temporal_namespace" in compensation_source


# --- Task 4.0: first-failure-safe delete-target helper + guard ---


def test_compute_delete_targets_never_deletes_active():
    decision = kubernetes_runtime_workflow.compute_delete_targets(
        active_revision="rev-active",
        candidate_revision="rev-active",  # candidate == active
        promoted=False,
        previous_active_revision=None,
    )
    assert decision["delete"] == []
    assert decision["refused"] == ["rev-active"]


def test_compute_delete_targets_failed_first_rollout_deletes_candidate():
    # No active revision yet (first rollout) -> still clean up the failed candidate.
    decision = kubernetes_runtime_workflow.compute_delete_targets(
        active_revision=None,
        candidate_revision="rev-cand",
        promoted=False,
        previous_active_revision=None,
    )
    assert decision["delete"] == ["rev-cand"]
    assert decision["refused"] == []


def test_compute_delete_targets_promotion_refuses_until_active_recorded():
    decision = kubernetes_runtime_workflow.compute_delete_targets(
        active_revision=None,  # new active not yet recorded
        candidate_revision="rev-new",
        promoted=True,
        previous_active_revision="rev-old",
    )
    assert decision["delete"] == []
    assert decision.get("reason") == "active-not-recorded"


def test_compute_delete_targets_promotion_deletes_previous_when_active_recorded():
    decision = kubernetes_runtime_workflow.compute_delete_targets(
        active_revision="rev-new",
        candidate_revision="rev-new",
        promoted=True,
        previous_active_revision="rev-old",
    )
    assert decision["delete"] == ["rev-old"]
    assert decision["refused"] == []


def test_require_allowed_revision_delete_passes_allowed_target():
    decision = {"delete": ["rev-old"], "refused": []}
    assert (
        kubernetes_runtime_workflow.require_allowed_revision_delete("rev-old", decision)
        == "rev-old"
    )


def test_require_allowed_revision_delete_raises_on_unexpected_target():
    decision = {"delete": ["rev-old"], "refused": []}
    import pytest

    with pytest.raises(RuntimeError, match="not in computed delete targets"):
        kubernetes_runtime_workflow.require_allowed_revision_delete(
            "rev-active", decision
        )


def test_revision_summary_from_delete_decision_returns_listed_revision():
    decision = {
        "delete": ["rev-old"],
        "revisions": {
            "rev-old": {
                "name": "rev-old",
                "rolloutVersion": "git-old",
                "runtimeNamespace": "runtime-old",
                "runtimeNamespaceEnabled": True,
            }
        },
    }

    assert kubernetes_runtime_workflow.revision_summary_from_delete_decision(
        decision, "rev-old"
    ) == decision["revisions"]["rev-old"]


# --- Task 4.1-4.3: revision lifecycle helpers ---

from demo_registry_operator.activities.revision import RevisionRef  # noqa: E402


def _revref():
    return RevisionRef(
        name="rev-1",
        uid="uid-9",
        rollout_version="git-v1",
        runtime_namespace="ns-x",
        runtime_namespace_enabled=True,
    )


def test_revision_owner_reference_from_ref():
    owner = kubernetes_runtime_workflow.revision_owner_reference_from_ref(_revref())
    assert owner["kind"] == "DemoProjectRevision"
    assert owner["apiVersion"] == "registry.tmprl-demo.cloud/v1alpha1"
    assert owner["name"] == "rev-1"
    assert owner["uid"] == "uid-9"
    assert owner["controller"] is True
    assert owner["blockOwnerDeletion"] is True


def test_revision_ref_summary():
    assert kubernetes_runtime_workflow.revision_ref_summary(_revref()) == {
        "name": "rev-1",
        "uid": "uid-9",
        "rolloutVersion": "git-v1",
        "runtimeNamespace": "ns-x",
        "runtimeNamespaceEnabled": True,
    }


def test_revision_cleanup_mode():
    assert (
        kubernetes_runtime_workflow.revision_cleanup_mode(
            {"runtimeNamespaceEnabled": True}
        )
        == "runtime-namespace"
    )
    assert (
        kubernetes_runtime_workflow.revision_cleanup_mode(
            {"runtimeNamespaceEnabled": False}
        )
        == "control-plane"
    )
    assert kubernetes_runtime_workflow.revision_cleanup_mode(None) == "control-plane"


def test_revision_phase_for_reconcile():
    assert (
        kubernetes_runtime_workflow._revision_phase_for_reconcile(
            {"phase": "WorkerRolloutPending"}
        )
        == "WorkerRolloutPending"
    )
    assert (
        kubernetes_runtime_workflow._revision_phase_for_reconcile(
            {"phase": "SmokeCheckPending"}
        )
        == "Applied"
    )


def test_revision_name_of():
    assert kubernetes_runtime_workflow._revision_name_of({"name": "rev-1"}) == "rev-1"
    assert kubernetes_runtime_workflow._revision_name_of(None) is None
    assert kubernetes_runtime_workflow._revision_name_of({}) is None


# --- Task 5.1: entity dedupe settles a terminal failure; force_reconcile retries ---


def test_smoke_check_failed_settles_then_force_reconcile_retries():
    demo_project = project()
    identity = reconcile_identity(demo_project)
    wf = workflow_instance()
    wf._current_reconcile_identity = identity
    wf._status = {"phase": "SmokeCheckFailed"}
    wf._complete_current_reconcile()
    # 0.2: terminal rollout failure is settled (remembered)
    assert wf._completed_reconcile_identity == identity
    # 5.1: re-signalling the SAME identity does not queue a new attempt
    wf.reconcile(reconcile_signal(deepcopy(demo_project)))
    assert wf._pending_spec is None
    assert wf._pending_reconcile_identity is None
    # 5.1: force_reconcile queues exactly one new attempt
    wf.force_reconcile(ForceReconcileSignal(token="retry-1", reason="manual"))
    assert wf._pending_force_reconcile == {"token": "retry-1", "reason": "manual"}


# --- Saga: first-time-failure compensation vs established-project settle ---


def test_project_has_active_version_distinguishes_first_time():
    assert project_has_active_version({"status": {"activeImageVersion": "git-x"}}) is True
    assert project_has_active_version({"status": {}}) is False
    assert project_has_active_version({"status": {"activeImageVersion": ""}}) is False
    assert project_has_active_version({}) is False
    assert project_has_active_version(None) is False


def test_failed_rollout_status_preserves_active_and_settles():
    wf = workflow_instance("demo")
    demo_project = {
        "status": {
            "activeImageVersion": "git-active",
            "activeRuntimeNamespace": "ns-active",
            "activeImages": {"worker": "git-active"},
        }
    }
    status = wf._failed_rollout_status(demo_project)
    assert status["phase"] == "WorkerRolloutFailed"
    # active/production fields are preserved so the catalog is not blanked
    assert status["activeImageVersion"] == "git-active"
    assert status["activeRuntimeNamespace"] == "ns-active"
    # WorkerRolloutFailed is a terminal phase -> the entity settles (keeps running)
    assert remember_completed_reconcile_identity(status) is True


def test_reconcile_wraps_children_in_saga_compensation_boundary():
    source = inspect.getsource(DemoProjectWorkflow._run_demo_project_reconcile)
    # external resources + child workflows run inside one try/except that routes
    # to the saga failure handler.
    assert "try:" in source
    assert "except Exception" in source
    assert "_handle_reconcile_failure" in source
    # the per-reconcile temporal-cloud capture is reset each pass
    assert "self._tc_state = None" in source


def test_handle_reconcile_failure_splits_first_time_vs_existing():
    source = inspect.getsource(DemoProjectWorkflow._handle_reconcile_failure)
    assert "project_has_active_version" in source
    # first-time: compensate then re-raise (fail the entity workflow)
    assert "_compensate_first_time_external_resources" in source
    assert "raise error" in source
    # existing: settle to a failed rollout, keep running (no destructive teardown)
    assert "_failed_rollout_status" in source
    assert "_record_status" in source


def test_first_time_compensation_tears_down_full_temporal_stack():
    source = inspect.getsource(
        DemoProjectWorkflow._compensate_first_time_external_resources
    )
    # no `promoted` guard here — a failed first provision tears down everything,
    # including the promoted runtime secret.
    assert "_compensate_temporal_candidate_api_key" in source
    assert "_compensate_temporal_service_account" in source
    assert "_compensate_temporal_namespace" in source
    assert "_delete_temporal_runtime_secret" in source
    assert "_delete_temporal_candidate_secret" in source


def test_temporal_cloud_reconcile_state_reports_region_migration():
    state = demo_project_workflow.TemporalCloudReconcileState(
        namespace=demo_project_workflow.external_resources.TemporalCloudNamespaceResult(
            status={
                "phase": "Ready",
                "namespace": {
                    "name": "tmprl-dem-cld-demo",
                    "action": "created",
                    "region": "aws-ca-central-1",
                },
            },
            namespace="tmprl-dem-cld-demo.acct1",
            grpc_address="tmprl-dem-cld-demo.acct1.tmprl.cloud:7233",
            auth_mode="apiKey",
            created=True,
            region="aws-ca-central-1",
        ),
        region_recreated=True,
        previous_region="gcp-us-central1",
        desired_region="aws-ca-central-1",
    )

    merged = state.merged_status()

    assert merged["regionMigration"] == {
        "recreated": True,
        "fromRegion": "gcp-us-central1",
        "toRegion": "aws-ca-central-1",
    }


def test_temporal_cloud_reconcile_state_omits_region_migration_without_recreate():
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
    )

    assert "regionMigration" not in state.merged_status()
