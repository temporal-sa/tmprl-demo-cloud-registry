from __future__ import annotations

import asyncio
from types import SimpleNamespace

from temporalio.common import AutoUpgradeVersioningOverride

import demo_registry_operator.watch as watch
from demo_registry_operator.activities.reconcile import status_with_reconcile_key
from demo_registry_operator.resources import (
    CANARY_KIND_LABEL,
    CANARY_LABEL,
    FINALIZATION_POLICY_ANNOTATION,
    IMAGE_OVERRIDES_ANNOTATION,
    OPERATOR_ROLLOUT_CANARY_KIND,
    PLATFORM_SOURCE_AUTH_SECRET_NAME,
    PLATFORM_SOURCE_AUTH_SECRET_PATH,
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
    demo_project_reconcile_key,
    source_auth_kubernetes_secret_name,
    source_git_repository,
    source_git_repository_name,
    source_secret_ref,
    source_watch_resources,
    source_watch_required,
)
from demo_registry_operator.watch import (
    git_repository_source_state,
    reconcile_key,
    source_commit_from_revision,
    workflow_signal_payload,
)


def project(
    *,
    name: str = "banking-money-transfer",
    annotations: dict[str, str] | None = None,
    source: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
        "kind": "DemoProject",
        "metadata": {
            "name": name,
            "generation": 4,
            "managedFields": [{"large": "field"}],
            "annotations": annotations or {},
        },
        "spec": {
            "source": source
            or {
                "repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
            },
            "builds": {
                "frontend": {
                    "context": ".",
                    "dockerfile": "docker/frontend.Dockerfile",
                },
            },
            "components": {
                "frontend": {
                    "build": "frontend",
                },
            },
        },
        "status": {
            "phase": "Promoted",
        },
    }


def test_source_git_repository_uses_public_repo_defaults():
    resource = source_git_repository(
        project(),
        namespace="tmprl-dem-cld-registry-operator",
        interval="1m",
    )

    assert resource["apiVersion"] == "source.toolkit.fluxcd.io/v1"
    assert resource["kind"] == "GitRepository"
    assert resource["metadata"]["name"] == "banking-money-transfer-source"
    assert resource["metadata"]["namespace"] == "tmprl-dem-cld-registry-operator"
    assert resource["metadata"]["labels"]["platform.temporal.io/project"] == (
        "banking-money-transfer"
    )
    assert resource["spec"] == {
        "interval": "1m",
        "url": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        "ref": {"branch": "main"},
        "secretRef": {"name": PLATFORM_SOURCE_AUTH_SECRET_NAME},
    }


def test_source_git_repository_keeps_branch_interval_and_secret_ref():
    demo_project = project(
        source={
            "repo": "ssh://git@github.com/temporal-sa/private-demo",
            "branch": "develop",
            "interval": "2m",
            "secretRef": {"name": "private-demo-auth"},
        }
    )

    resource = source_git_repository(
        demo_project,
        namespace="tmprl-dem-cld-registry-operator",
        interval="1m",
    )

    assert resource["spec"] == {
        "interval": "2m",
        "url": "ssh://git@github.com/temporal-sa/private-demo",
        "ref": {"branch": "develop"},
        "secretRef": {"name": "private-demo-auth"},
    }
    assert source_secret_ref(demo_project) == {"name": "private-demo-auth"}


def test_source_git_repository_renders_sparse_checkout_as_flux_ignore():
    demo_project = project(
        source={
            "repo": "https://github.com/temporal-sa/private-demo",
            "gitRepository": {
                "sparseCheckout": ["registry/web", "registry/worker"],
                "ignore": "/registry/worker/tests\n",
            },
        }
    )

    resource = source_git_repository(
        demo_project,
        namespace="tmprl-dem-cld-registry-operator",
        interval="1m",
    )

    assert resource["spec"] == {
        "interval": "1m",
        "url": "https://github.com/temporal-sa/private-demo",
        "ref": {"branch": "main"},
        "secretRef": {"name": PLATFORM_SOURCE_AUTH_SECRET_NAME},
        "ignore": "\n".join(
            [
                "/*",
                "!/registry/",
                "/registry/*",
                "!/registry/web/",
                "!/registry/web/**",
                "!/registry/worker/",
                "!/registry/worker/**",
                "/registry/worker/tests",
            ]
        ),
    }


def test_source_watch_resources_sync_shared_github_pat_for_temporal_sa_https():
    resources = source_watch_resources(
        project(),
        namespace="tmprl-dem-cld-registry-operator",
        interval="1m",
    )

    assert len(resources) == 2
    external_secret, git_repository = resources
    assert external_secret["kind"] == "ExternalSecret"
    assert external_secret["metadata"]["name"] == PLATFORM_SOURCE_AUTH_SECRET_NAME
    assert external_secret["metadata"]["namespace"] == (
        "tmprl-dem-cld-registry-operator"
    )
    assert external_secret["metadata"]["annotations"] == {
        "platform.temporal.io/secret-source": PLATFORM_SOURCE_AUTH_SECRET_PATH
    }
    assert external_secret["spec"]["target"]["name"] == PLATFORM_SOURCE_AUTH_SECRET_NAME
    assert external_secret["spec"]["dataFrom"] == [
        {"extract": {"key": PLATFORM_SOURCE_AUTH_SECRET_PATH}}
    ]
    assert git_repository["kind"] == "GitRepository"
    assert git_repository["spec"]["secretRef"] == {
        "name": PLATFORM_SOURCE_AUTH_SECRET_NAME
    }


def test_source_watch_resources_sync_private_repo_auth_from_aws_secret():
    demo_project = project(
        name="registry-operator",
        source={
            "repo": "https://github.com/temporal-sa/private-demo",
            "gitRepository": {
                "url": "ssh://git@github.com/temporal-sa/private-demo.git",
                "authSecret": "source-auth",
            },
        },
    )

    resources = source_watch_resources(
        demo_project,
        namespace="tmprl-dem-cld-registry-operator",
        interval="1m",
    )

    assert len(resources) == 2
    external_secret, git_repository = resources
    assert source_auth_kubernetes_secret_name(demo_project) == (
        "registry-operator-source-auth"
    )
    assert external_secret["kind"] == "ExternalSecret"
    assert external_secret["metadata"]["name"] == "registry-operator-source-auth"
    assert external_secret["metadata"]["namespace"] == (
        "tmprl-dem-cld-registry-operator"
    )
    assert external_secret["spec"]["target"]["name"] == "registry-operator-source-auth"
    assert external_secret["spec"]["dataFrom"] == [
        {"extract": {"key": "tmprl-dem-cld/registry-operator/source-auth"}}
    ]
    assert git_repository["kind"] == "GitRepository"
    assert git_repository["spec"] == {
        "interval": "1m",
        "url": "ssh://git@github.com/temporal-sa/private-demo.git",
        "ref": {"branch": "main"},
        "secretRef": {"name": "registry-operator-source-auth"},
    }


def test_source_watch_required_skips_pinned_and_fully_overridden_projects():
    assert source_watch_required(project()) is True
    assert (
        source_watch_required(
            project(
                annotations={
                    SOURCE_COMMIT_ANNOTATION: (
                        "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
                    )
                }
            )
        )
        is False
    )
    assert (
        source_watch_required(
            project(
                annotations={
                    IMAGE_OVERRIDES_ANNOTATION: '{"frontend":"example/app:git-test"}'
                }
            )
        )
        is False
    )


def test_source_commit_from_flux_revision():
    assert source_commit_from_revision(
        "main@sha1:e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
    ) == "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
    assert source_commit_from_revision("main") is None
    assert source_commit_from_revision("main@sha1:not-a-sha") is None


def test_git_repository_source_state_requires_observed_ready_revision():
    resource = {
        "metadata": {"generation": 2},
        "status": {
            "observedGeneration": 1,
            "conditions": [{"type": "Ready", "status": "True"}],
            "artifact": {
                "revision": (
                    "main@sha1:e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
                ),
                "digest": "sha256:aaaabbbb",
            },
        },
    }
    assert git_repository_source_state(resource)["reason"] == "GitRepositoryNotObserved"

    resource["status"]["observedGeneration"] = 2
    state = git_repository_source_state(resource)

    assert state["phase"] == "Resolved"
    assert state["sourceCommitSha"] == (
        "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
    )
    assert state["sourceArtifactDigest"] == "sha256:aaaabbbb"

    resource["status"]["artifact"].pop("digest")
    assert git_repository_source_state(resource)["reason"] == (
        "ArtifactDigestUnavailable"
    )


def test_workflow_signal_payload_is_trimmed_and_can_inject_source_commit():
    payload = workflow_signal_payload(
        project(),
        source_commit_sha="e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        source_artifact_digest="sha256:aaaabbbb",
    )

    assert payload["metadata"] == {
        "name": "banking-money-transfer",
        "generation": 4,
        "annotations": {
            SOURCE_COMMIT_ANNOTATION: (
                "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
            ),
            SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:aaaabbbb",
        },
    }
    assert "status" not in payload
    assert "managedFields" not in payload["metadata"]


def test_reconcile_key_changes_with_flux_source_commit_without_artifact_digest():
    base = workflow_signal_payload(project())
    updated = workflow_signal_payload(
        project(),
        source_commit_sha="e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
    )

    assert source_git_repository_name(project()) == "banking-money-transfer-source"
    assert reconcile_key(base) != reconcile_key(updated)


def test_reconcile_key_uses_artifact_digest_over_flux_source_commit():
    first = workflow_signal_payload(
        project(),
        source_commit_sha="e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        source_artifact_digest="sha256:aaaabbbb",
    )
    second = workflow_signal_payload(
        project(),
        source_commit_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        source_artifact_digest="sha256:aaaabbbb",
    )
    changed_artifact = workflow_signal_payload(
        project(),
        source_commit_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        source_artifact_digest="sha256:ccccdddd",
    )

    assert reconcile_key(first) == reconcile_key(second)
    assert reconcile_key(first) != reconcile_key(changed_artifact)


def test_signal_workflow_starts_with_auto_upgrade_versioning():
    calls = []

    class TemporalClient:
        async def start_workflow(self, *args, **kwargs):
            calls.append((args, kwargs))

    asyncio.run(
        watch.signal_workflow(
            TemporalClient(),
            SimpleNamespace(task_queue="registry-management"),
            project(),
            "reconcile",
        )
    )

    assert len(calls) == 1
    _, kwargs = calls[0]
    assert kwargs["id"] == "demo-project/banking-money-transfer"
    assert kwargs["task_queue"] == "registry-management"
    assert kwargs["start_signal"] == "reconcile"
    assert isinstance(kwargs["versioning_override"], AutoUpgradeVersioningOverride)


def test_watch_skips_gate_owned_operator_canary_reconcile(monkeypatch):
    calls = []
    canary_project = project(name="operator-canary-1234")
    canary_project["metadata"]["labels"] = {
        CANARY_LABEL: "true",
        CANARY_KIND_LABEL: OPERATOR_ROLLOUT_CANARY_KIND,
    }

    monkeypatch.setattr(
        watch,
        "ensure_demo_project_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "reconcile_source_watch",
        lambda resource, config: calls.append(("source", resource["metadata"]["name"])),
    )

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args, kwargs))

    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            canary_project,
            {},
        )
    )

    assert calls == [("finalizer", "operator-canary-1234")]


def test_watch_does_not_adopt_control_plane_project(monkeypatch):
    calls = []
    control_plane_project = project(
        name="registry-operator",
        annotations={FINALIZATION_POLICY_ANNOTATION: "retain"},
    )

    monkeypatch.setattr(
        watch,
        "ensure_demo_project_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "remove_demo_project_finalizer",
        lambda name: calls.append(("remove-finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "reconcile_source_watch",
        lambda resource, config: {
            "sourceCommitSha": "abc1234",
            "sourceArtifactDigest": "sha256:aaaabbbb",
        },
    )

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args[2]["metadata"]["name"]))

    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            control_plane_project,
            {},
        )
    )

    assert calls == [
        ("remove-finalizer", "registry-operator"),
        ("signal", "registry-operator"),
    ]


def test_watch_skips_source_digest_already_observed_in_status(monkeypatch):
    calls = []
    demo_project = project()
    demo_project["status"] = {
        "observedGeneration": 4,
        "phase": "Promoted",
        "buildStatus": {"sourceArtifactDigest": "sha256:aaaabbbb"},
        "conditions": [
            {
                "type": "Reconciled",
                "status": "True",
                "observedGeneration": 4,
            }
        ],
    }

    monkeypatch.setattr(
        watch,
        "ensure_demo_project_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "reconcile_source_watch",
        lambda resource, config: {
            "sourceCommitSha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "sourceArtifactDigest": "sha256:aaaabbbb",
        },
    )

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args[2]["metadata"]["name"]))

    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    async def workflow_running(*args, **kwargs):
        return True

    monkeypatch.setattr(watch, "entity_workflow_running", workflow_running)

    seen = {}
    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            demo_project,
            seen,
        )
    )

    assert calls == [("finalizer", "banking-money-transfer")]
    expected_key = reconcile_key(
        workflow_signal_payload(
            demo_project,
            source_commit_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            source_artifact_digest="sha256:aaaabbbb",
        )
    )
    assert seen == {"banking-money-transfer": expected_key}


def test_watch_skips_reconcile_key_already_observed_in_status(monkeypatch):
    calls = []
    source_commit_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    source_artifact_digest = "sha256:newdigest"
    demo_project = project()
    expected_key = reconcile_key(
        workflow_signal_payload(
            demo_project,
            source_commit_sha=source_commit_sha,
            source_artifact_digest=source_artifact_digest,
        )
    )
    demo_project["status"] = {
        "observedGeneration": 4,
        "phase": "Promoted",
        "reconcileKey": expected_key,
        "buildStatus": {"sourceArtifactDigest": "sha256:olddigest"},
        "conditions": [
            {
                "type": "Reconciled",
                "status": "True",
                "observedGeneration": 4,
            }
        ],
    }

    monkeypatch.setattr(
        watch,
        "ensure_demo_project_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "reconcile_source_watch",
        lambda resource, config: {
            "sourceCommitSha": source_commit_sha,
            "sourceArtifactDigest": source_artifact_digest,
        },
    )

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args[2]["metadata"]["name"]))

    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    async def workflow_running(*args, **kwargs):
        return True

    monkeypatch.setattr(watch, "entity_workflow_running", workflow_running)

    seen = {}
    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            demo_project,
            seen,
        )
    )

    assert calls == [("finalizer", "banking-money-transfer")]
    assert seen == {"banking-money-transfer": expected_key}


def test_watch_resignals_current_status_when_workflow_is_missing(monkeypatch):
    calls = []
    source_commit_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    source_artifact_digest = "sha256:newdigest"
    demo_project = project()
    expected_key = reconcile_key(
        workflow_signal_payload(
            demo_project,
            source_commit_sha=source_commit_sha,
            source_artifact_digest=source_artifact_digest,
        )
    )
    demo_project["status"] = {
        "observedGeneration": 4,
        "phase": "Promoted",
        "reconcileKey": expected_key,
        "conditions": [
            {
                "type": "Reconciled",
                "status": "True",
                "observedGeneration": 4,
            }
        ],
    }

    monkeypatch.setattr(
        watch,
        "ensure_demo_project_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "reconcile_source_watch",
        lambda resource, config: {
            "sourceCommitSha": source_commit_sha,
            "sourceArtifactDigest": source_artifact_digest,
        },
    )

    async def workflow_running(*args, **kwargs):
        return False

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args[2]["metadata"]["name"]))

    monkeypatch.setattr(watch, "entity_workflow_running", workflow_running)
    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            demo_project,
            seen,
        )
    )

    assert calls == [
        ("finalizer", "banking-money-transfer"),
        ("signal", "banking-money-transfer"),
    ]
    assert seen == {"banking-money-transfer": expected_key}


def test_watch_resignals_failed_observed_reconcile(monkeypatch):
    calls = []
    source_commit_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    source_artifact_digest = "sha256:newdigest"
    demo_project = project()
    expected_key = reconcile_key(
        workflow_signal_payload(
            demo_project,
            source_commit_sha=source_commit_sha,
            source_artifact_digest=source_artifact_digest,
        )
    )
    demo_project["status"] = {
        "observedGeneration": 4,
        "phase": "CandidateNotReady",
        "reconcileKey": expected_key,
        "buildStatus": {"sourceArtifactDigest": source_artifact_digest},
        "conditions": [
            {
                "type": "Reconciled",
                "status": "True",
                "observedGeneration": 4,
            }
        ],
    }

    monkeypatch.setattr(
        watch,
        "ensure_demo_project_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "reconcile_source_watch",
        lambda resource, config: {
            "sourceCommitSha": source_commit_sha,
            "sourceArtifactDigest": source_artifact_digest,
        },
    )

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args[2]["metadata"]["name"]))

    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            demo_project,
            seen,
        )
    )

    assert calls == [
        ("finalizer", "banking-money-transfer"),
        ("signal", "banking-money-transfer"),
    ]
    assert seen == {"banking-money-transfer": expected_key}


def test_status_with_reconcile_key_replaces_stale_key():
    demo_project = project()
    status = {
        "observedGeneration": 4,
        "phase": "Promoted",
        "reconcileKey": "stale",
    }

    updated = status_with_reconcile_key(demo_project, status)

    assert updated["reconcileKey"] == demo_project_reconcile_key(demo_project)
    assert updated["reconcileKey"] != "stale"
    assert status["reconcileKey"] == "stale"


def test_watch_removes_control_plane_finalizer_on_delete(monkeypatch):
    calls = []
    control_plane_project = project(
        name="registry-operator",
        annotations={FINALIZATION_POLICY_ANNOTATION: "retain"},
    )
    control_plane_project["metadata"]["deletionTimestamp"] = "2026-06-14T20:00:00Z"

    monkeypatch.setattr(
        watch,
        "remove_demo_project_finalizer",
        lambda name: calls.append(("remove-finalizer", name)),
    )

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args, kwargs))

    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            control_plane_project,
            seen,
        )
    )

    assert calls == [("remove-finalizer", "registry-operator")]
    assert seen == {"registry-operator": "retain:2026-06-14T20:00:00Z"}


def test_watch_still_finalizes_gate_owned_operator_canary(monkeypatch):
    calls = []
    canary_project = project(name="operator-canary-1234")
    canary_project["metadata"]["labels"] = {
        CANARY_LABEL: "true",
        CANARY_KIND_LABEL: OPERATOR_ROLLOUT_CANARY_KIND,
    }
    canary_project["metadata"]["deletionTimestamp"] = "2026-06-14T20:00:00Z"

    async def signal_workflow(temporal_client, config, demo_project, signal_name):
        calls.append((demo_project["metadata"]["name"], signal_name))

    monkeypatch.setattr(watch, "signal_workflow", signal_workflow)

    asyncio.run(
        watch.reconcile_one(
            None,
            SimpleNamespace(),
            canary_project,
            {},
        )
    )

    assert calls == [("operator-canary-1234", "finalize")]


def test_watch_auto_mode_runs_only_for_current_ready_worker(monkeypatch):
    config = SimpleNamespace(
        watch_mode="current",
        source_watch_namespace="tmprl-dem-cld-registry-operator",
    )
    monkeypatch.setenv(
        "TEMPORAL_DEPLOYMENT_NAME",
        "tmprl-dem-cld-registry-operator/registry-operator",
    )
    monkeypatch.setenv("TEMPORAL_WORKER_BUILD_ID", "git-new")

    monkeypatch.setattr(
        watch,
        "read_worker_deployment_status",
        lambda namespace, name: {
            "currentVersion": {"buildID": "git-old"},
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    )

    status = watch.watch_activation_status(config)

    assert status["active"] is False
    assert status["reason"] == "NotCurrentWorkerVersion"

    monkeypatch.setattr(
        watch,
        "read_worker_deployment_status",
        lambda namespace, name: {
            "currentVersion": {"buildID": "git-new"},
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    )

    status = watch.watch_activation_status(config)

    assert status["active"] is True
    assert status["reason"] == "CurrentWorkerVersion"


def test_watch_bootstrap_mode_idles_after_handoff_is_ready(monkeypatch):
    config = SimpleNamespace(
        watch_mode="bootstrap",
        source_watch_namespace="tmprl-dem-cld-registry-operator",
        watch_handoff_deployment="tmprl-dem-cld-registry-operator/registry-operator",
    )
    monkeypatch.setattr(
        watch,
        "read_worker_deployment_status",
        lambda namespace, name: {
            "currentVersion": {"buildID": "git-new"},
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    )

    status = watch.watch_activation_status(config)

    assert status["active"] is False
    assert status["reason"] == "HandoffComplete"


def test_watch_bootstrap_mode_stays_active_until_handoff_exists(monkeypatch):
    class NotFound(Exception):
        status = 404

    config = SimpleNamespace(
        watch_mode="bootstrap",
        source_watch_namespace="tmprl-dem-cld-registry-operator",
        watch_handoff_deployment="tmprl-dem-cld-registry-operator/registry-operator",
    )
    monkeypatch.setattr(
        watch,
        "read_worker_deployment_status",
        lambda namespace, name: (_ for _ in ()).throw(NotFound()),
    )

    status = watch.watch_activation_status(config)

    assert status["active"] is True
    assert status["reason"] == "WorkerDeploymentMissing"
