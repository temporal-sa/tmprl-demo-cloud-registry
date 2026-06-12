from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import demo_registry_operator.workflows.demo_project as demo_project_workflow
from demo_registry_operator.resources import (
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
)
from demo_registry_operator.workflows.demo_project import (
    DemoProjectWorkflow,
    remember_completed_reconcile_identity,
    reconcile_identity,
    status_reconcile_identity,
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


def test_reconcile_signal_noops_when_same_identity_is_pending():
    demo_project = project()
    duplicate = deepcopy(demo_project)
    workflow = DemoProjectWorkflow("demo")

    workflow.reconcile(demo_project)
    workflow.reconcile(duplicate)

    assert workflow._pending_spec is demo_project
    assert workflow._pending_reconcile_identity == reconcile_identity(demo_project)


def test_reconcile_signal_noops_when_same_identity_is_currently_pursued():
    demo_project = project()
    workflow = DemoProjectWorkflow("demo")
    workflow._current_reconcile_identity = reconcile_identity(demo_project)

    workflow.reconcile(deepcopy(demo_project))

    assert workflow._pending_spec is None
    assert workflow._pending_reconcile_identity is None


def test_reconcile_signal_noops_when_same_identity_is_completed():
    demo_project = project()
    workflow = DemoProjectWorkflow("demo")
    workflow._completed_reconcile_identity = reconcile_identity(demo_project)

    workflow.reconcile(deepcopy(demo_project))

    assert workflow._pending_spec is None
    assert workflow._pending_reconcile_identity is None


def test_failed_reconcile_identity_is_not_remembered_as_completed():
    demo_project = project()
    identity = reconcile_identity(demo_project)
    workflow = DemoProjectWorkflow("demo")
    workflow._current_reconcile_identity = identity
    workflow._status = {"phase": "Error"}

    workflow._complete_current_reconcile()
    workflow.reconcile(deepcopy(demo_project))

    assert workflow._completed_reconcile_identity is None
    assert workflow._pending_spec is not None
    assert workflow._pending_reconcile_identity == identity


def test_failed_status_is_not_a_rememberable_completed_identity():
    assert remember_completed_reconcile_identity({"phase": "Error"}) is False
    assert (
        remember_completed_reconcile_identity({"phase": "SmokeCheckFailed"}) is False
    )
    assert remember_completed_reconcile_identity({"phase": "Promoted"}) is True


def test_reconcile_signal_accepts_same_digest_with_different_sha():
    current = project(source_commit_sha="1111111111111111111111111111111111111111")
    updated = project(source_commit_sha="2222222222222222222222222222222222222222")
    workflow = DemoProjectWorkflow("demo")
    workflow._current_reconcile_identity = reconcile_identity(current)

    workflow.reconcile(updated)

    assert workflow._pending_spec is updated
    assert workflow._pending_reconcile_identity == reconcile_identity(updated)


def test_reconcile_signal_accepts_same_source_with_changed_spec_key():
    current = project(generation=1)
    updated = project(generation=2)
    workflow = DemoProjectWorkflow("demo")
    workflow._completed_reconcile_identity = reconcile_identity(current)

    workflow.reconcile(updated)

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


def test_check_worker_version_signal_requests_continue_as_new_when_target_changed(
    monkeypatch,
):
    workflow = DemoProjectWorkflow("demo")
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
    workflow = DemoProjectWorkflow("demo")
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(
            is_target_worker_deployment_version_changed=lambda: False
        ),
    )

    workflow.check_worker_version()

    assert workflow._continue_as_new_for_worker_version is False
