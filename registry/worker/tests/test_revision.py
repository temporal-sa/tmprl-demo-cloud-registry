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


# --- Task 2.1: revision_name + revision_resource ---

from demo_registry_operator import resources  # noqa: E402


def _ordinary_project() -> dict:
    return {
        "metadata": {
            "name": "banking-money-transfer",
            "annotations": {
                resources.SOURCE_COMMIT_ANNOTATION: "a" * 40,
                resources.SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:dead",
            },
        },
        "spec": {
            "components": {
                "backend": {"build": "backend"},
                "worker": {
                    "build": "worker",
                    "worker": {"versioning": {"enabled": True}},
                },
            },
            "smokeCheck": {"path": "/api/health", "expectedStatus": 200},
            "ingress": {"subdomain": "banking"},
        },
    }


def _control_plane_project() -> dict:
    return {
        "metadata": {
            "name": "registry-operator",
            "annotations": {
                resources.FINALIZATION_POLICY_ANNOTATION: resources.FINALIZATION_POLICY_RETAIN,
            },
        },
        "spec": {"components": {"web": {"build": "web"}}},
    }


def test_revision_name_is_dns_safe_and_bounded():
    dp = {"metadata": {"name": "banking-money-transfer"}}
    name = resources.revision_name(dp, "git-388c26cb1e5e-key-7aa36083")
    assert len(name) <= 63
    assert name.startswith("banking-money-transfer-")


def test_revision_resource_ordinary_demo_enables_runtime_namespace():
    dp = _ordinary_project()
    images = {
        "backend": "example.com/backend:git-v1",
        "worker": "example.com/worker:git-v1",
    }
    rev = resources.revision_resource(dp, "git-v1", images=images, reconcile_key="rk1")
    assert rev["kind"] == "DemoProjectRevision"
    assert rev["apiVersion"].startswith("registry.tmprl-demo.cloud/")
    assert rev["metadata"]["name"] == resources.revision_name(dp, "git-v1")
    assert (
        rev["metadata"]["labels"]["platform.temporal.io/rollout-version"] == "git-v1"
    )
    spec = rev["spec"]
    assert spec["projectName"] == "banking-money-transfer"
    assert spec["rolloutVersion"] == "git-v1"
    assert spec["reconcileKey"] == "rk1"
    assert spec["sourceCommitSha"] == "a" * 40
    assert spec["sourceArtifactDigest"] == "sha256:dead"
    assert spec["runtimeNamespaceEnabled"] is True
    assert spec["runtimeNamespace"] == resources.runtime_namespace(dp, "git-v1")
    assert spec["images"] == images
    assert "backend" in spec["components"]


def test_revision_resource_control_plane_disables_runtime_namespace():
    dp = _control_plane_project()
    rev = resources.revision_resource(
        dp, "git-v1", images={"web": "example.com/web:git-v1"}
    )
    spec = rev["spec"]
    assert spec["runtimeNamespaceEnabled"] is False
    assert spec.get("runtimeNamespace") in (None, "")


# --- Task 2.2: runtime-namespace mode owns the Namespace ---


def test_namespace_resource_carries_revision_owner_reference():
    owner = kube.revision_owner_reference("rev-x", "uid-x")
    ns = resources.namespace_resource(
        {"metadata": {"name": "banking-money-transfer"}},
        namespace="tmprl-dem-cld-banking-money-transfer-abcde",
        rollout_version="git-v1",
        owner_references=[owner],
    )
    assert ns["kind"] == "Namespace"
    assert ns["metadata"]["ownerReferences"] == [owner]
    assert ns["metadata"]["labels"][resources.RUNTIME_NAMESPACE_LABEL] == "true"


def test_runtime_resources_owns_only_the_namespace():
    dp = {
        "metadata": {
            "name": "banking-money-transfer",
            "annotations": {resources.SOURCE_COMMIT_ANNOTATION: "a" * 40},
        },
        "spec": {"components": {"backend": {"build": "backend"}}},
    }
    images = {"backend": "example.com/backend:git-v1"}
    owner = kube.revision_owner_reference("rev-x", "uid-x")
    res = resources.runtime_resources(dp, images, owner_references=[owner])

    namespace = next(r for r in res if r["kind"] == "Namespace")
    assert namespace["metadata"]["ownerReferences"] == [owner]
    assert namespace["metadata"]["name"] == resources.candidate_runtime_namespace(
        dp, images
    )
    # The Namespace is the cleanup boundary; nothing inside it carries an ownerRef.
    for resource in res:
        if resource["kind"] != "Namespace":
            assert "ownerReferences" not in resource.get("metadata", {})


# --- Task 2.3: no-runtime-namespace mode owns candidate Deploy/Svc only ---


def _control_plane_web_project() -> dict:
    return {
        "metadata": {
            "name": "registry-operator",
            "annotations": {
                resources.FINALIZATION_POLICY_ANNOTATION: resources.FINALIZATION_POLICY_RETAIN,
                resources.SOURCE_COMMIT_ANNOTATION: "b" * 40,
            },
        },
        "spec": {
            "components": {
                "web": {
                    "build": "web",
                    "ports": [{"containerPort": 80, "servicePort": 80}],
                },
                "registry-operator": {
                    "build": "worker",
                    "worker": {
                        "versioning": {
                            "enabled": True,
                            "deploymentName": "registry-operator",
                        }
                    },
                },
            },
        },
    }


def test_control_plane_owns_candidate_deploy_and_service_not_worker_or_stable():
    dp = _control_plane_web_project()
    images = {
        "web": "example.com/web:git-v1",
        "worker": "example.com/worker:git-v1",
    }
    owner = kube.revision_owner_reference("rev-op", "uid-op")
    res = resources.runtime_resources(dp, images, owner_references=[owner])

    # Control-plane renders no runtime Namespace.
    assert not any(r["kind"] == "Namespace" for r in res)

    web_deploy = next(r for r in res if r["kind"] == "Deployment")
    assert web_deploy["metadata"]["name"].startswith("web-git-")
    assert web_deploy["metadata"]["ownerReferences"] == [owner]

    web_svc = next(r for r in res if r["kind"] == "Service")
    assert web_svc["metadata"]["ownerReferences"] == [owner]

    # WorkerDeployment is reconciled by the Worker Controller; never owned by a Revision.
    for resource in res:
        if resource["kind"] == "WorkerDeployment":
            assert "ownerReferences" not in resource["metadata"]


def test_promotion_stable_service_is_never_owned():
    dp = _control_plane_web_project()
    promoted = resources.promotion_resources(
        dp,
        image_overrides={"web": "example.com/web:git-v1"},
        rollout_version="git-v1",
    )
    for resource in promoted:
        if resource["kind"] == "Service":
            assert "ownerReferences" not in resource["metadata"]


# --- Task 3.1: PARITY TEST — planner readiness == apply renders workloads ---

import pytest  # noqa: E402

from demo_registry_operator import candidate_plan  # noqa: E402
from demo_registry_operator.activities import reconcile  # noqa: E402


def _apply_req(dp, ext, build):
    return reconcile.ApplyDemoProjectResourcesRequest(
        demo_project=dp, external_resources=ext, build_status=build
    )


def _parity_base_project():
    return {
        "metadata": {
            "name": "parity-demo",
            "annotations": {resources.SOURCE_COMMIT_ANNOTATION: "c" * 40},
        },
        "spec": {
            "builds": {"backend": {"context": ".", "dockerfile": "docker/b"}},
            "components": {
                "backend": {
                    "build": "backend",
                    "ports": [{"containerPort": 8000, "servicePort": 8000}],
                }
            },
        },
    }


def _parity_worker_only_project():
    return {
        "metadata": {
            "name": "parity-worker",
            "annotations": {resources.SOURCE_COMMIT_ANNOTATION: "d" * 40},
        },
        "spec": {
            "builds": {"worker": {"context": ".", "dockerfile": "docker/w"}},
            "components": {
                "worker": {
                    "build": "worker",
                    "worker": {
                        "versioning": {"enabled": True, "deploymentName": "parity-worker"}
                    },
                }
            },
        },
    }


def _parity_fixture(case):
    if case == "worker_only":
        dp = _parity_worker_only_project()
        return dp, None, {"phase": "Pushed", "images": {"worker": "ex.com/w:git-d"}}
    dp = _parity_base_project()
    ok = {"phase": "Pushed", "images": {"backend": "ex.com/b:git-c"}}
    if case == "missing_images":
        return dp, None, {"phase": "Pushed", "images": {}}
    if case == "build_error":
        return dp, None, {"phase": "Error", "images": {"backend": "ex.com/b:git-c"}}
    if case == "external_block":
        return dp, {"temporalCloud": {"phase": "Error", "reason": "Boom"}}, ok
    if case == "all_clear":
        return dp, None, ok
    raise ValueError(case)


@pytest.mark.parametrize(
    "case",
    ["missing_images", "build_error", "external_block", "all_clear", "worker_only"],
)
def test_planner_matches_apply(case, monkeypatch):
    dp, ext, build = _parity_fixture(case)
    rendered: list[str] = []
    monkeypatch.setattr(
        reconcile.kube,
        "apply_resources",
        lambda res, dry_run: rendered.extend(r["kind"] for r in res) or [],
    )
    reconcile.apply_demo_project_resources(_apply_req(dp, ext, build))
    workloads_rendered = any(
        kind in rendered for kind in ("Deployment", "Service", "WorkerDeployment")
    )
    assert (
        candidate_plan.candidate_renderability(dp, ext, build).ready
        == workloads_rendered
    )


def test_worker_only_renders_workerdeployment_not_deployment(monkeypatch):
    dp, ext, build = _parity_fixture("worker_only")
    rendered: list[str] = []
    monkeypatch.setattr(
        reconcile.kube,
        "apply_resources",
        lambda res, dry_run: rendered.extend(r["kind"] for r in res) or [],
    )
    reconcile.apply_demo_project_resources(_apply_req(dp, ext, build))
    assert "WorkerDeployment" in rendered
    assert "Deployment" not in rendered


# --- Task 3.1: plan_candidate_revision activity + neutral-module guard ---

from demo_registry_operator.activities import revision as revision_activities  # noqa: E402


def test_plan_candidate_revision_wraps_renderability():
    dp, ext, build = _parity_fixture("all_clear")
    plan = revision_activities.plan_candidate_revision(
        revision_activities.PlanCandidateRevisionRequest(
            demo_project=dp, external_resources=ext, build_status=build
        )
    )
    expected = candidate_plan.candidate_renderability(dp, ext, build)
    assert plan.ready == expected.ready is True
    assert plan.phase == expected.phase == "Ready"
    assert plan.rollout_version == expected.rollout_version
    assert plan.images == expected.images


def test_revision_activities_do_not_import_reconcile():
    # activities/revision.py must depend on the neutral candidate_plan module, never
    # on activities/reconcile.py (one-directional dependency).
    source = Path(revision_activities.__file__).read_text()
    assert "activities.reconcile" not in source
    assert "import reconcile" not in source


# --- Task 3.2: revision lifecycle activities ---


def test_ensure_revision_applies_and_returns_ref(monkeypatch):
    applied = {}
    monkeypatch.setattr(
        revision_activities.kube,
        "apply_demo_project_revision",
        lambda resource, dry_run: applied.update(resource=resource) or "applied",
    )
    monkeypatch.setattr(
        revision_activities.kube,
        "read_demo_project_revision",
        lambda name: {"metadata": {"uid": "uid-42"}},
    )
    dp = _ordinary_project()
    ref = revision_activities.ensure_revision(
        revision_activities.EnsureRevisionRequest(
            demo_project=dp,
            rollout_version="git-v1",
            images={"backend": "x", "worker": "y"},
            reconcile_key="rk",
        )
    )
    assert ref.name == resources.revision_name(dp, "git-v1")
    assert ref.uid == "uid-42"
    assert ref.rollout_version == "git-v1"
    assert ref.runtime_namespace_enabled is True
    assert applied["resource"]["kind"] == "DemoProjectRevision"
    assert applied["resource"]["spec"]["reconcileKey"] == "rk"


def test_set_revision_phase_patches_status(monkeypatch):
    sent = {}
    monkeypatch.setattr(
        revision_activities.kube,
        "patch_demo_project_revision_status",
        lambda name, status, dry_run: sent.update(name=name, status=status),
    )
    revision_activities.set_revision_phase(
        revision_activities.SetRevisionPhaseRequest(
            name="r", phase="Applied", message="ok"
        )
    )
    assert sent["name"] == "r"
    assert sent["status"] == {"phase": "Applied", "message": "ok"}


def test_delete_revision_uses_foreground_propagation(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        revision_activities.kube,
        "delete_demo_project_revision",
        lambda name, propagation_policy, dry_run: captured.update(
            name=name, policy=propagation_policy
        )
        or "deleting",
    )
    result = revision_activities.delete_revision(
        revision_activities.DeleteRevisionRequest(name="r")
    )
    assert captured["policy"] == "Foreground"
    assert result["action"] == "deleting"


def test_patch_revision_refs_only_patches_ref_keys(monkeypatch):
    sent = {}
    monkeypatch.setattr(
        revision_activities.kube,
        "patch_demo_project_status",
        lambda project, status, dry_run: sent.update(project=project, status=status),
    )
    revision_activities.patch_demo_project_revision_refs(
        revision_activities.PatchRevisionRefsRequest(
            project="p",
            refs={
                "candidateRevision": {"name": "x"},
                "phase": "Promoted",  # not a ref key -> dropped
                "bogus": 1,
            },
        )
    )
    assert sent["status"] == {"candidateRevision": {"name": "x"}}


# --- Task 3.3: bounded cleanup wait ---


def test_wait_cleanup_runtime_namespace_cleaned_when_namespace_gone(monkeypatch):
    monkeypatch.setattr(revision_activities.kube, "read_namespace", lambda name: None)
    result = revision_activities.wait_for_revision_cleanup_impl(
        "rev",
        mode="runtime-namespace",
        boundary={"namespace": "ns-x"},
        sleep=lambda *_: None,
        now=lambda: 0.0,
    )
    assert result["phase"] == "Cleaned"


def test_wait_cleanup_control_plane_cleaned_when_no_candidate(monkeypatch):
    monkeypatch.setattr(
        revision_activities.kube,
        "list_deployments",
        lambda ns, *, label_selector: [],
    )
    monkeypatch.setattr(
        revision_activities.kube,
        "list_services",
        lambda ns, *, label_selector: [],
    )
    result = revision_activities.wait_for_revision_cleanup_impl(
        "rev",
        mode="control-plane",
        boundary={"namespace": "p", "rolloutVersion": "git-v1"},
        sleep=lambda *_: None,
        now=lambda: 0.0,
    )
    assert result["phase"] == "Cleaned"


def test_wait_cleanup_terminating_on_deadline_and_heartbeats(monkeypatch):
    monkeypatch.setattr(
        revision_activities.kube,
        "read_namespace",
        lambda name: {"metadata": {"name": name}},  # never disappears
    )
    times = iter([0.0, 0.5, 2.0])  # deadline=1.0; 0.5 < deadline, then 2.0 >= deadline
    beats: list = []
    slept: list = []
    result = revision_activities.wait_for_revision_cleanup_impl(
        "rev",
        mode="runtime-namespace",
        boundary={"namespace": "ns-x"},
        timeout_seconds=1,
        poll_seconds=5,
        sleep=lambda s: slept.append(s),
        heartbeat=lambda *a: beats.append(a),
        now=lambda: next(times),
    )
    assert result["phase"] == "Terminating"
    assert beats  # heartbeated while waiting
    assert slept == [5]
