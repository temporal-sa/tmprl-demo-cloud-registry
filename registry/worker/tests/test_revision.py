from __future__ import annotations

from pathlib import Path

import yaml

from demo_registry_operator import kube

ROOT = Path(__file__).resolve().parents[3]
CRD_DIR = ROOT / "clusters" / "blue" / "bootstrap" / "crds"
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
        "temporalRuntimeSecretPath",
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

RBAC = ROOT / "clusters" / "blue" / "bootstrap" / "registry-operator" / "rbac.yaml"
REGISTRY_OPERATOR_RBACS = tuple(
    ROOT / "clusters" / cluster / "bootstrap" / "registry-operator" / "rbac.yaml"
    for cluster in ("blue", "green")
)


def _cluster_role_rules(path: Path = RBAC) -> list[dict]:
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d]
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


def test_all_registry_operator_rbac_covers_temporal_teardown():
    required = {"get", "list", "create", "patch", "delete"}
    for rbac in REGISTRY_OPERATOR_RBACS:
        rules = _cluster_role_rules(rbac)
        assert required <= _verbs_for(rules, "temporal.io", "workerdeployments")
        assert required <= _verbs_for(rules, "temporal.io", "connections")


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


def test_plan_revision_deletes_protects_active_and_deletes_stale(monkeypatch):
    monkeypatch.setattr(
        revision_activities.kube,
        "list_demo_project_revisions",
        lambda label_selector: [
            {
                "metadata": {"name": "rev-active", "uid": "uid-active"},
                "spec": {
                    "rolloutVersion": "git-new",
                    "runtimeNamespace": "runtime-new",
                    "runtimeNamespaceEnabled": True,
                },
            },
            {
                "metadata": {"name": "rev-old", "uid": "uid-old"},
                "spec": {
                    "rolloutVersion": "git-old",
                    "runtimeNamespace": "runtime-old",
                    "runtimeNamespaceEnabled": True,
                },
            },
        ],
    )

    decision = revision_activities.plan_revision_deletes(
        revision_activities.PlanRevisionDeletesRequest(
            project="demo",
            protected_revisions=["rev-active"],
        )
    )

    assert decision["delete"] == ["rev-old"]
    assert decision["protected"] == ["rev-active"]
    assert decision["refused"] == ["rev-active"]
    assert decision["revisions"]["rev-old"]["runtimeNamespace"] == "runtime-old"


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


# --- Task 4.1: apply threads revision ownerReference into candidate rendering ---


def test_apply_threads_revision_owner_reference_to_candidate_namespace(monkeypatch):
    dp = {
        "metadata": {
            "name": "banking-money-transfer",
            "annotations": {resources.SOURCE_COMMIT_ANNOTATION: "a" * 40},
        },
        "spec": {"components": {"backend": {"build": "backend"}}},
    }
    owner = kube.revision_owner_reference("rev-x", "uid-x")
    rendered: list = []
    monkeypatch.setattr(
        reconcile.kube,
        "apply_resources",
        lambda res, dry_run: rendered.extend(res) or [],
    )
    reconcile.apply_demo_project_resources(
        reconcile.ApplyDemoProjectResourcesRequest(
            demo_project=dp,
            build_status={"phase": "Pushed", "images": {"backend": "ex/b:git-v1"}},
            revision_owner_reference=owner,
        )
    )
    runtime_ns = next(
        r
        for r in rendered
        if r["kind"] == "Namespace"
        and r["metadata"].get("labels", {}).get(resources.RUNTIME_NAMESPACE_LABEL)
        == "true"
    )
    assert runtime_ns["metadata"]["ownerReferences"] == [owner]


def test_apply_without_revision_owner_reference_leaves_namespace_unowned(monkeypatch):
    dp = {
        "metadata": {
            "name": "banking-money-transfer",
            "annotations": {resources.SOURCE_COMMIT_ANNOTATION: "a" * 40},
        },
        "spec": {"components": {"backend": {"build": "backend"}}},
    }
    rendered: list = []
    monkeypatch.setattr(
        reconcile.kube,
        "apply_resources",
        lambda res, dry_run: rendered.extend(res) or [],
    )
    reconcile.apply_demo_project_resources(
        reconcile.ApplyDemoProjectResourcesRequest(
            demo_project=dp,
            build_status={"phase": "Pushed", "images": {"backend": "ex/b:git-v1"}},
        )
    )
    runtime_ns = next(
        r
        for r in rendered
        if r["kind"] == "Namespace"
        and r["metadata"].get("labels", {}).get(resources.RUNTIME_NAMESPACE_LABEL)
        == "true"
    )
    assert "ownerReferences" not in runtime_ns["metadata"]


# --- Task 4.4: one Revision identity per attempt ---


def test_one_revision_identity_version_is_consistent():
    dp = {
        "metadata": {
            "name": "banking-money-transfer",
            "annotations": {resources.SOURCE_COMMIT_ANNOTATION: "c" * 40},
        },
        "spec": {"components": {"backend": {"build": "backend"}}},
    }
    build = {"phase": "Pushed", "images": {"backend": "ex/b:git-c"}}
    plan = candidate_plan.candidate_renderability(dp, None, build)
    # The planner's rolloutVersion is exactly the candidate version apply renders with
    # (single identity) and the Revision records that same version + runtime namespace.
    assert plan.ready is True
    assert plan.rollout_version == resources.candidate_version(dp, plan.images)
    assert plan.runtime_namespace == resources.candidate_runtime_namespace(
        dp, plan.images
    )
    rev = resources.revision_resource(dp, plan.rollout_version, images=plan.images)
    assert rev["spec"]["rolloutVersion"] == plan.rollout_version
    assert rev["spec"]["runtimeNamespace"] == plan.runtime_namespace


# --- Task 3.4: adoption + repair ---


def test_adopt_skipped_when_no_active_revision():
    result = revision_activities.adopt_active_revision_resources(
        revision_activities.AdoptActiveRevisionRequest(
            demo_project={"metadata": {"name": "p"}}, active_revision=None
        )
    )
    assert result["phase"] == "Skipped"


def test_adopt_runtime_namespace_verified_when_owner_present(monkeypatch):
    ref = {"name": "rev-1", "uid": "uid-1", "rolloutVersion": "git-v1"}
    monkeypatch.setattr(
        revision_activities.kube, "read_demo_project_revision",
        lambda name: {"metadata": {"uid": "uid-1"}},
    )
    monkeypatch.setattr(
        revision_activities.kube, "read_namespace",
        lambda name: {
            "name": name,
            "ownerReferences": [{"kind": "DemoProjectRevision", "name": "rev-1"}],
        },
    )
    patched = []
    monkeypatch.setattr(
        revision_activities.kube, "patch_namespace_owner_references",
        lambda *a, **k: patched.append(a),
    )
    result = revision_activities.adopt_active_revision_resources(
        revision_activities.AdoptActiveRevisionRequest(
            demo_project={"metadata": {"name": "banking-money-transfer"}},
            active_revision=ref,
            active_runtime_namespace="tmprl-dem-cld-banking-money-transfer-abcde",
        )
    )
    assert result["phase"] == "Verified"
    assert patched == []


def test_adopt_runtime_namespace_patches_when_owner_missing(monkeypatch):
    ref = {"name": "rev-1", "uid": "uid-1"}
    monkeypatch.setattr(
        revision_activities.kube, "read_demo_project_revision",
        lambda name: {"metadata": {"uid": "uid-1"}},
    )
    monkeypatch.setattr(
        revision_activities.kube, "read_namespace",
        lambda name: {"name": name, "ownerReferences": []},
    )
    patched = []
    monkeypatch.setattr(
        revision_activities.kube, "patch_namespace_owner_references",
        lambda ns, owners, *, dry_run: patched.append((ns, owners)),
    )
    result = revision_activities.adopt_active_revision_resources(
        revision_activities.AdoptActiveRevisionRequest(
            demo_project={"metadata": {"name": "banking-money-transfer"}},
            active_revision=ref,
            active_runtime_namespace="ns-x",
        )
    )
    assert result["phase"] == "Adopted"
    assert patched[0][0] == "ns-x"
    assert patched[0][1][0]["name"] == "rev-1"


def test_adopt_control_plane_patches_candidate_deploy_svc_only(monkeypatch):
    ref = {"name": "rev-1", "uid": "uid-1", "rolloutVersion": "git-v1"}
    monkeypatch.setattr(
        revision_activities.kube, "read_demo_project_revision",
        lambda name: {"metadata": {"uid": "uid-1"}},
    )
    monkeypatch.setattr(
        revision_activities.kube, "list_deployments",
        lambda ns, *, label_selector: [{"name": "web-git-v1"}],
    )
    monkeypatch.setattr(
        revision_activities.kube, "list_services",
        lambda ns, *, label_selector: [{"name": "web-git-v1"}],
    )
    monkeypatch.setattr(
        revision_activities.kube, "read_deployment_owner_references",
        lambda ns, name: [],
    )
    monkeypatch.setattr(
        revision_activities.kube, "read_service_owner_references",
        lambda ns, name: [],
    )
    dep_patched, svc_patched = [], []
    monkeypatch.setattr(
        revision_activities.kube, "patch_deployment_owner_references",
        lambda ns, name, owners, *, dry_run: dep_patched.append(name),
    )
    monkeypatch.setattr(
        revision_activities.kube, "patch_service_owner_references",
        lambda ns, name, owners, *, dry_run: svc_patched.append(name),
    )
    result = revision_activities.adopt_active_revision_resources(
        revision_activities.AdoptActiveRevisionRequest(
            demo_project={"metadata": {"name": "registry-operator"}},
            active_revision=ref,
            active_version="git-v1",
        )
    )
    assert result["phase"] == "Adopted"
    assert dep_patched == ["web-git-v1"]
    assert svc_patched == ["web-git-v1"]


# --- Task 8: finalization-policy footgun coupling (regression doc) ---


def test_finalization_policy_couples_control_plane_and_runtime_namespace():
    # Documents the footgun: finalization-policy=retain drives BOTH control-plane
    # teardown-retain AND runtime-namespace disable. If this coupling is ever split,
    # update this test alongside.
    operator = {
        "metadata": {
            "name": "registry-operator",
            "annotations": {
                resources.FINALIZATION_POLICY_ANNOTATION: resources.FINALIZATION_POLICY_RETAIN
            },
        }
    }
    ordinary = {"metadata": {"name": "banking-money-transfer", "annotations": {}}}

    assert resources.is_control_plane_project(operator) is True
    assert resources.runtime_namespace_rollout_enabled(operator) is False

    assert resources.is_control_plane_project(ordinary) is False
    assert resources.runtime_namespace_rollout_enabled(ordinary) is True


# --- Task 6.2: canary shape preserved via external, self-contained templates ---

REGISTRY_PROJECT = ROOT / "projects" / "operator" / "registry.yaml"


def _operator_canary_gate() -> dict:
    spec = _load(REGISTRY_PROJECT)["spec"]
    for component in spec.get("components", {}).values():
        worker = component.get("worker") if isinstance(component, dict) else None
        versioning = worker.get("versioning") if isinstance(worker, dict) else None
        gate = (
            versioning.get("rollout", {}).get("gate")
            if isinstance(versioning, dict)
            else None
        )
        if isinstance(gate, dict):
            return gate
    raise AssertionError("no worker rollout gate found in registry.yaml")


def test_operator_canary_gate_input_carries_policy_only_not_template():
    # The canary project templates are now external YAML files loaded by the
    # workflow; the gate input must NOT inline a demoProjectTemplate, only the
    # shared run policy (naming, timeouts, cleanup).
    gate = _operator_canary_gate()
    assert gate["workflowType"] == "RegistryOperatorCanaryWorkflow"
    gate_input = gate["input"]
    assert "demoProjectTemplate" not in gate_input
    assert gate_input["projectNamePrefix"] == "operator-canary"
    assert gate_input["cleanupPolicy"] == "Always"
    assert {"timeoutSeconds", "cleanupTimeoutSeconds", "pollSeconds"} <= set(gate_input)


def test_operator_canary_external_templates_preserve_banking_shape():
    # Moving the template out to files must not change what the canary exercises:
    # both auth-mode templates still build/run the full banking app end-to-end.
    from demo_registry_operator.activities import canary as canary_activities

    templates = {t["name"]: t for t in canary_activities.load_canary_templates()}
    assert {"api-key", "mtls"} <= set(templates)
    for name in ("api-key", "mtls"):
        spec = templates[name]["demoProjectTemplate"]["spec"]
        assert (
            spec["source"]["repo"]
            == "https://github.com/temporal-sa/demo-banking-app-money-transfer"
        )
        assert set(spec["builds"]) == {"frontend", "backend", "worker"}
        assert set(spec["components"]) == {"frontend", "backend", "worker"}
        assert spec["smokeCheck"]["path"] == "/api/health"
        assert spec["smokeCheck"]["expectedStatus"] == 200


# --- Task 6.1: operator self-rollout behaviors are separate ---


def test_operator_self_rollout_behaviors_are_separate():
    # (1) WorkerDeployment rollback, (2) candidate Deploy/Svc cleanup via Revision
    # ownership, (3) operator-canary cleanup are distinct. Pin (1) vs (2): the
    # control-plane candidate render owns web Deploy/Svc but NEVER the WorkerDeployment
    # (its rollback is the separate rollback_worker_deployments activity).
    dp = _control_plane_web_project()
    images = {"web": "ex/web:git-v1", "worker": "ex/worker:git-v1"}
    owner = kube.revision_owner_reference("rev-op", "uid-op")
    res = resources.runtime_resources(dp, images, owner_references=[owner])
    owned_kinds = {
        r["kind"] for r in res if "ownerReferences" in r.get("metadata", {})
    }
    worker_deployments = [r for r in res if r["kind"] == "WorkerDeployment"]
    assert worker_deployments
    assert all(
        "ownerReferences" not in wd["metadata"] for wd in worker_deployments
    )
    assert {"Deployment", "Service"} <= owned_kinds


# --- Task 7: label-prune is a managed-only, version-targeted migration fallback ---


def test_prune_only_targets_managed_candidate_resources(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        reconcile.kube,
        "list_deployments",
        lambda ns, *, label_selector: (captured.update(dep=label_selector), [])[1],
    )
    monkeypatch.setattr(
        reconcile.kube,
        "list_services",
        lambda ns, *, label_selector: (captured.update(svc=label_selector), [])[1],
    )
    dp = {
        "metadata": {"name": "banking-money-transfer"},
        "spec": {"components": {"backend": {"build": "backend"}}},
    }
    reconcile.prune_stale_runtime_candidates(
        reconcile.PruneStaleRuntimeCandidatesRequest(
            demo_project=dp, active_version="git-new"
        )
    )
    for selector in (captured["dep"], captured["svc"]):
        assert "app.kubernetes.io/managed-by=demo-registry" in selector
        assert "platform.temporal.io/rollout-role=candidate" in selector


def test_delete_candidate_resources_targets_candidate_version_and_managed(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        reconcile.kube,
        "list_deployments",
        lambda ns, *, label_selector: (captured.update(dep=label_selector), [])[1],
    )
    monkeypatch.setattr(
        reconcile.kube,
        "list_services",
        lambda ns, *, label_selector: (captured.update(svc=label_selector), [])[1],
    )
    dp = {
        "metadata": {"name": "banking-money-transfer"},
        "spec": {"components": {"backend": {"build": "backend"}}},
    }
    reconcile.delete_runtime_candidate_resources(
        reconcile.DeleteRuntimeCandidateResourcesRequest(
            demo_project=dp, candidate_version="git-failed"
        )
    )
    for selector in (captured["dep"], captured["svc"]):
        assert "app.kubernetes.io/managed-by=demo-registry" in selector
        assert "platform.temporal.io/rollout-version=git-failed" in selector


# --- Regression: patch helpers must drive the REAL kubernetes client correctly ---
# The pinned client rejects a `_content_type` kwarg (ApiTypeError), and built-in
# patch methods negotiate application/json-patch+json by default — which cannot
# carry a merge-patch dict body. Mocked `kube` tests never exercised this; drive
# the real client (mock only call_api, so kwarg-validation + content-type
# negotiation run for real) and assert a merge-patch is sent.

def test_status_and_ownerref_patches_drive_real_client_with_merge_patch(monkeypatch):
    from kubernetes import client as k8s
    from demo_registry_operator import kube as kubemod

    captured = []

    def fake_call_api(self, resource_path, method, path_params=None,
                      query_params=None, header_params=None, body=None,
                      *args, **kwargs):
        captured.append(
            {"method": method, "ct": (header_params or {}).get("Content-Type")}
        )
        return {}

    monkeypatch.setattr(k8s.ApiClient, "call_api", fake_call_api, raising=True)
    monkeypatch.setattr(kubemod, "load_kubernetes_config", lambda: None)
    k8s.Configuration.set_default(k8s.Configuration())

    # custom-object status (default merge-patch) + built-in owner-ref (default
    # json-patch — must be forced to merge-patch).
    kubemod.patch_demo_project_status("proj", {"phase": "Ready"}, dry_run=False)
    kubemod.patch_demo_project_revision_status("rev", {"phase": "Ready"}, dry_run=False)
    kubemod.patch_namespace_owner_references(
        "ns", [{"apiVersion": "v1", "kind": "X", "name": "n", "uid": "u"}], dry_run=False
    )

    assert len(captured) == 3, captured
    for c in captured:
        assert c["method"] == "PATCH", c
        assert c["ct"] == "application/merge-patch+json", c


# --- DNS: workloads resolve external FQDNs absolute-first (ndots:2) to avoid the
#     conntrack-race that wedges the worker's gRPC DNS resolver ---

def test_workload_dns_config_uses_ndots2_and_single_request_reopen():
    opts = {o["name"]: o.get("value") for o in resources.workload_dns_config()["options"]}
    assert opts["ndots"] == "2"
    assert "single-request-reopen" in opts


def test_rendered_pods_set_dns_config_for_deployment_and_workerdeployment():
    dp = _control_plane_web_project()
    images = {"web": "ex/web:git-v1", "worker": "ex/worker:git-v1"}
    res = resources.runtime_resources(dp, images)
    checked = set()
    for r in res:
        if r["kind"] in ("Deployment", "WorkerDeployment"):
            spec = r["spec"]["template"]["spec"]
            opts = {o["name"]: o.get("value") for o in spec.get("dnsConfig", {}).get("options", [])}
            assert opts.get("ndots") == "2", f"{r['kind']} missing ndots:2"
            assert "single-request-reopen" in opts, f"{r['kind']} missing single-request-reopen"
            checked.add(r["kind"])
    assert {"Deployment", "WorkerDeployment"} <= checked, checked


# --- Worker liveness probe (heartbeat-driven; recovers a Running-but-wedged worker) ---

def test_worker_liveness_probe_enabled_is_opt_in():
    assert resources.worker_liveness_probe_enabled({"worker": {"livenessProbe": True}}) is True
    assert resources.worker_liveness_probe_enabled(
        {"worker": {"versioning": {"enabled": True}, "livenessProbe": True}}
    ) is True
    assert resources.worker_liveness_probe_enabled(
        {"worker": {"versioning": {"enabled": True}}}
    ) is False
    assert resources.worker_liveness_probe_enabled({"worker": True}) is False
    assert resources.worker_liveness_probe_enabled({}) is False


def test_worker_liveness_probe_checks_heartbeat_freshness():
    probe = resources.worker_liveness_probe()
    cmd = probe["exec"]["command"]
    assert cmd[0] == "sh"
    assert resources.WORKER_LIVENESS_FILE in cmd[2]
    assert str(resources.WORKER_LIVENESS_MAX_STALE_SECONDS) in cmd[2]
    assert probe["failureThreshold"] >= 1 and probe["periodSeconds"] >= 1


def test_container_renders_liveness_probe_only_when_opted_in():
    dp = {"metadata": {"name": "x"}}
    opted = resources.container(
        dp, "worker",
        {"build": "worker", "worker": {"livenessProbe": True, "versioning": {"enabled": True}}},
        "img:git-x",
    )
    assert "livenessProbe" in opted
    plain = resources.container(dp, "worker", {"build": "worker", "worker": True}, "img:git-x")
    assert "livenessProbe" not in plain
