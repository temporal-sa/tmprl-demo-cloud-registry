from __future__ import annotations

from pathlib import Path

import yaml

from demo_registry_operator import kube

ROOT = Path(__file__).resolve().parents[3]
CRD_DIR = ROOT / "clusters" / "sa-demo" / "bootstrap" / "crds"
DEMOPROJECTS_CRD = CRD_DIR / "demoprojects.registry.tmprl-demo.cloud.yaml"
REVISIONS_CRD = CRD_DIR / "demoprojectrevisions.registry.tmprl-demo.cloud.yaml"
KUSTOMIZATION = CRD_DIR / "kustomization.yaml"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _v1_schema(crd: dict) -> dict:
    version = crd["spec"]["versions"][0]
    return version["schema"]["openAPIV3Schema"]["properties"]


# --- Task 1.2: DemoProject status persists revision bookkeeping (closed schema) ---


def test_demoproject_status_persists_revision_bookkeeping_keys():
    crd = _load(DEMOPROJECTS_CRD)
    status_props = _v1_schema(crd)["status"]["properties"]
    for key in (
        "activeRevision",
        "candidateRevision",
        "lastFailedRevision",
        "workerDeploymentRollbackStatus",
        "revisionCleanupStatus",
    ):
        assert key in status_props, f"{key} missing from DemoProject status schema"
        assert status_props[key]["type"] == "object"
        assert status_props[key].get("x-kubernetes-preserve-unknown-fields") is True


def test_demoproject_status_keeps_existing_enumerated_keys():
    crd = _load(DEMOPROJECTS_CRD)
    status_props = _v1_schema(crd)["status"]["properties"]
    # Must not drop the keys that already exist (closed schema prunes unlisted keys).
    for key in (
        "phase",
        "activeImageVersion",
        "candidateRolloutStatus",
        "workerDeploymentStatus",
        "reconcileKey",
    ):
        assert key in status_props


# --- Task 1.1: cluster-scoped DemoProjectRevision CRD ---


def test_demoprojectrevision_crd_is_cluster_scoped_with_expected_shape():
    crd = _load(REVISIONS_CRD)
    assert crd["kind"] == "CustomResourceDefinition"
    assert crd["spec"]["scope"] == "Cluster"
    names = crd["spec"]["names"]
    assert names["plural"] == "demoprojectrevisions"
    assert names["kind"] == "DemoProjectRevision"
    assert "dpr" in names.get("shortNames", [])

    version = crd["spec"]["versions"][0]
    assert version["subresources"].get("status") == {}

    schema = _v1_schema(crd)
    spec_props = schema["spec"]["properties"]
    for field in (
        "projectName",
        "rolloutVersion",
        "reconcileKey",
        "sourceCommitSha",
        "sourceArtifactDigest",
        "runtimeNamespace",
        "runtimeNamespaceEnabled",
        "host",
        "images",
        "components",
        "workerDeployments",
        "smokeCheck",
    ):
        assert field in spec_props, f"spec.{field} missing from DemoProjectRevision"

    status = schema["status"]
    assert status.get("x-kubernetes-preserve-unknown-fields") is True
    phase_enum = status["properties"]["phase"]["enum"]
    for phase in (
        "Pending",
        "Applied",
        "WorkerRolloutPending",
        "WorkerRolloutReady",
        "Smoking",
        "Promoted",
        "Failed",
        "Superseded",
        "Deleting",
        "Deleted",
    ):
        assert phase in phase_enum, f"phase {phase} missing from enum"


def test_demoprojectrevision_crd_registered_in_kustomization():
    resources = _load(KUSTOMIZATION).get("resources", [])
    assert "demoprojectrevisions.registry.tmprl-demo.cloud.yaml" in resources


# --- Task 1.3: RBAC (cluster-scoped + finalizers subresource) ---

RBAC = ROOT / "clusters" / "sa-demo" / "bootstrap" / "registry-operator" / "rbac.yaml"


def _cluster_role_rules() -> list[dict]:
    docs = [d for d in yaml.safe_load_all(RBAC.read_text()) if d]
    cluster_role = next(d for d in docs if d.get("kind") == "ClusterRole")
    return cluster_role["rules"]


def _verbs_for(rules: list[dict], group: str, resource: str) -> set:
    verbs: set = set()
    for rule in rules:
        if group in rule["apiGroups"] and resource in rule["resources"]:
            verbs |= set(rule["verbs"])
    return verbs


def test_rbac_grants_demoprojectrevision_access():
    rules = _cluster_role_rules()
    group = "registry.tmprl-demo.cloud"
    assert {
        "get",
        "list",
        "watch",
        "create",
        "update",
        "patch",
        "delete",
    } <= _verbs_for(rules, group, "demoprojectrevisions")
    assert {"get", "patch", "update"} <= _verbs_for(
        rules, group, "demoprojectrevisions/status"
    )
    # finalizers subresource: required by OwnerReferencesPermissionEnforcement to set
    # blockOwnerDeletion referencing a Revision.
    assert {"update", "patch"} <= _verbs_for(
        rules, group, "demoprojectrevisions/finalizers"
    )


def test_rbac_covers_ownerreference_patch_targets():
    rules = _cluster_role_rules()
    assert {"get", "patch", "delete"} <= _verbs_for(rules, "", "namespaces")
    assert {"get", "patch", "delete"} <= _verbs_for(rules, "apps", "deployments")
    assert {"get", "patch", "delete"} <= _verbs_for(rules, "", "services")
    assert {"get", "create", "patch"} <= _verbs_for(
        rules, "temporal.io", "workerdeployments"
    )


# --- Task 1.4: kube helpers + merge-patch content-type guard ---


def test_revision_kube_constants():
    assert kube.DEMO_PROJECT_REVISION_GROUP == "registry.tmprl-demo.cloud"
    assert kube.DEMO_PROJECT_REVISION_VERSION == "v1alpha1"
    assert kube.DEMO_PROJECT_REVISION_PLURAL == "demoprojectrevisions"


def test_status_patch_preserves_unrelated_keys_and_uses_json_merge_patch(monkeypatch):
    sent = {}
    monkeypatch.setattr(
        kube,
        "_patch_demo_project_status_raw",
        lambda name, body, *, content_type: sent.update(
            name=name, body=body, content_type=content_type
        ),
    )
    kube.patch_demo_project_status(
        "p", {"candidateRevision": {"name": "x"}}, dry_run=False
    )
    # merge-patch body contains ONLY the changed key; server preserves phase/etc.
    assert sent["body"] == {"status": {"candidateRevision": {"name": "x"}}}
    assert sent["content_type"] == "application/merge-patch+json"


def test_status_patch_dry_run_skips_raw_call(monkeypatch):
    called = []
    monkeypatch.setattr(
        kube,
        "_patch_demo_project_status_raw",
        lambda *a, **k: called.append(1),
    )
    kube.patch_demo_project_status("p", {"phase": "Promoted"}, dry_run=True)
    assert called == []


def test_revision_status_patch_uses_json_merge_patch(monkeypatch):
    sent = {}
    monkeypatch.setattr(
        kube,
        "_patch_demo_project_revision_status_raw",
        lambda name, body, *, content_type: sent.update(
            name=name, body=body, content_type=content_type
        ),
    )
    kube.patch_demo_project_revision_status("r", {"phase": "Applied"}, dry_run=False)
    assert sent["body"] == {"status": {"phase": "Applied"}}
    assert sent["content_type"] == "application/merge-patch+json"


def test_revision_owner_reference_shape():
    ref = kube.revision_owner_reference("rev-1", "uid-123")
    assert ref["apiVersion"] == "registry.tmprl-demo.cloud/v1alpha1"
    assert ref["kind"] == "DemoProjectRevision"
    assert ref["name"] == "rev-1"
    assert ref["uid"] == "uid-123"
    assert ref["controller"] is True
    assert ref["blockOwnerDeletion"] is True
