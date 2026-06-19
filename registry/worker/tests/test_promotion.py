from __future__ import annotations

from demo_registry_operator import resources
from demo_registry_operator.activities import reconcile
from demo_registry_operator.resources import HTTPS_REDIRECT_MIDDLEWARE_NAME


def project(name: str, **spec):
    return {"metadata": {"name": name}, "spec": spec}


def banking_project():
    return project(
        "banking-money-transfer",
        builds={
            "backend": {
                "context": ".",
                "dockerfile": "docker/backend.Dockerfile",
            },
            "frontend": {
                "context": ".",
                "dockerfile": "docker/frontend.Dockerfile",
            },
            "worker": {
                "context": ".",
                "dockerfile": "docker/worker-python.Dockerfile",
            },
        },
        components={
            "backend": {
                "build": "backend",
                "ports": [{"containerPort": 8000, "servicePort": 8000}],
            },
            "frontend": {
                "build": "frontend",
                "ports": [{"containerPort": 5173, "servicePort": 80}],
            },
            "worker": {"build": "worker", "worker": True},
        },
        ingress={"subdomain": "banking-app"},
        smokeCheck={"path": "/api/health", "expectedStatus": 200},
    )


def banking_images(version: str = "git-e12b0ed9d450") -> dict[str, str]:
    return {
        "backend": f"example.com/backend:{version}",
        "frontend": f"example.com/frontend:{version}",
        "worker": f"example.com/worker:{version}",
    }


def applied_summary(rendered):
    summaries = []
    for resource in rendered:
        metadata = resource.get("metadata", {})
        summaries.append(
            {
                "kind": resource["kind"],
                "namespace": metadata.get("namespace"),
                "name": metadata.get("name"),
            }
        )
    return summaries


def test_plan_project_finalization_uses_source_watch_namespace(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_SOURCE_WATCH_NAMESPACE", "source-watch")
    monkeypatch.setattr(
        reconcile.kube,
        "list_namespaces",
        lambda *, label_selector: [
            {"name": "tmprl-dem-cld-banking-money-transfer-a1b2c"},
            {"name": "tmprl-dem-cld-banking-money-transfer"},
        ],
    )

    plan = reconcile.load_project_finalization_plan(
        reconcile.ProjectFinalizationRequest(banking_project())
    ).plan

    assert plan["action"] == "finalize"
    assert plan["project"] == "banking-money-transfer"
    assert plan["namespace"] == "tmprl-dem-cld-banking-money-transfer"
    assert plan["runtimeNamespaces"] == [
        "tmprl-dem-cld-banking-money-transfer-a1b2c"
    ]
    assert plan["sourceWatchNamespace"] == "source-watch"
    assert plan["gitRepository"] == "banking-money-transfer-source"
    assert set(plan) == {
        "action",
        "demoProject",
        "project",
        "namespace",
        "runtimeNamespaces",
        "sourceWatchNamespace",
        "gitRepository",
        "sourceAuthSecret",
        "checkedAt",
    }


def test_control_plane_waiting_for_images_reconciles_stable_runtime_secrets(
    monkeypatch,
):
    applied = []

    def capture_apply(rendered, *, dry_run):
        applied.extend(rendered)
        assert dry_run is False
        return applied_summary(rendered)

    monkeypatch.setattr(reconcile.kube, "apply_resources", capture_apply)
    monkeypatch.setattr(
        reconcile.kube,
        "read_external_secret_status",
        lambda namespace, name: {
            "name": name,
            "namespace": namespace,
            "phase": "Ready",
            "ready": True,
        },
    )

    demo_project = banking_project()
    demo_project["metadata"]["annotations"] = {
        resources.FINALIZATION_POLICY_ANNOTATION: (
            resources.FINALIZATION_POLICY_RETAIN
        )
    }

    status = reconcile.apply_demo_project_resources(
        reconcile.ApplyDemoProjectResourcesRequest(demo_project=demo_project)
    ).status

    external_secrets = [
        resource
        for resource in applied
        if resource["kind"] == "ExternalSecret"
    ]
    assert status["phase"] == "WaitingForImages"
    assert "ExternalSecrets reconciled" in status["message"]
    assert {secret["metadata"]["namespace"] for secret in external_secrets} == {
        "tmprl-dem-cld-banking-money-transfer"
    }


def test_runtime_namespace_waiting_for_images_defers_runtime_secrets(monkeypatch):
    applied = []

    def capture_apply(rendered, *, dry_run):
        applied.extend(rendered)
        assert dry_run is False
        return applied_summary(rendered)

    monkeypatch.setattr(reconcile.kube, "apply_resources", capture_apply)

    status = reconcile.apply_demo_project_resources(
        reconcile.ApplyDemoProjectResourcesRequest(demo_project=banking_project())
    ).status

    assert status["phase"] == "WaitingForImages"
    assert status["message"] == (
        "Base resources reconciled; waiting for immutable images."
    )
    assert [
        resource for resource in applied if resource["kind"] == "ExternalSecret"
    ] == []


def test_candidate_runtime_apply_does_not_promote_stable_services():
    demo_project = banking_project()
    images = banking_images()
    version = resources.candidate_version(demo_project, images)
    runtime_namespace = resources.runtime_namespace(demo_project, version)

    rendered = resources.runtime_workload_resources(
        demo_project,
        images,
        include_routes=False,
    )

    services = [
        resource
        for resource in rendered
        if resource["kind"] == "Service"
    ]
    service_names = {service["metadata"]["name"] for service in services}

    assert service_names == {"backend", "frontend"}
    assert {service["metadata"]["namespace"] for service in services} == {
        runtime_namespace
    }
    backend = next(
        service for service in services if service["metadata"]["name"] == "backend"
    )
    assert backend["spec"]["selector"] == resources.rollout_selector_labels(
        demo_project,
        "backend",
        version,
    )


def test_runtime_resources_include_versioned_runtime_namespace():
    demo_project = banking_project()
    images = banking_images()
    version = resources.candidate_version(demo_project, images)
    runtime_namespace = resources.runtime_namespace(demo_project, version)

    rendered = resources.runtime_resources(
        demo_project,
        images,
        include_routes=False,
    )

    namespace = next(
        resource
        for resource in rendered
        if resource["kind"] == "Namespace"
        and resource["metadata"]["name"] == runtime_namespace
    )
    assert namespace["metadata"]["labels"][
        resources.RUNTIME_NAMESPACE_LABEL
    ] == "true"

    namespaced = [
        resource
        for resource in rendered
        if resource["kind"] in {"Service", "Deployment", "ExternalSecret"}
    ]
    assert {resource["metadata"]["namespace"] for resource in namespaced} == {
        runtime_namespace
    }


def test_promotion_resources_point_routes_at_runtime_namespace():
    demo_project = banking_project()
    images = banking_images()

    rendered = resources.promotion_resources(
        demo_project,
        image_overrides=images,
        rollout_version="git-active",
        service_namespace="tmprl-dem-cld-banking-money-transfer-a1b2c",
    )

    services = [
        resource
        for resource in rendered
        if resource["kind"] == "Service"
    ]
    ingress_route = next(
        resource
        for resource in rendered
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "banking-money-transfer"
    )

    assert services == []
    assert ingress_route["spec"]["routes"][0]["services"][0] == {
        "name": "frontend",
        "namespace": "tmprl-dem-cld-banking-money-transfer-a1b2c",
        "passHostHeader": True,
        "port": 80,
    }


def test_promotion_resources_without_runtime_namespace_promotes_stable_services():
    demo_project = banking_project()
    images = banking_images()

    rendered = resources.promotion_resources(
        demo_project,
        image_overrides=images,
        rollout_version="git-active",
    )

    backend = next(
        resource
        for resource in rendered
        if resource["kind"] == "Service"
        and resource["metadata"]["name"] == "backend"
    )
    ingress_route = next(
        resource
        for resource in rendered
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "banking-money-transfer"
    )

    assert backend["metadata"]["namespace"] == (
        "tmprl-dem-cld-banking-money-transfer"
    )
    assert backend["spec"]["selector"] == resources.rollout_selector_labels(
        demo_project,
        "backend",
        "git-active",
    )
    assert (
        ingress_route["spec"]["routes"][0]["services"][0]["name"]
        == "frontend-git-active"
    )


def test_external_resource_blocker_blocks_temporal_cloud_errors():
    blocker = reconcile.external_resource_blocker(
        {
            "temporalCloud": {
                "phase": "Error",
                "reason": "TemporalAuthModeSwitchRequiresFinalize",
                "message": "Finalize before switching auth mode.",
                "auth": {"mode": "mtls"},
                "namespace": {"name": "demo.acct1", "authMode": "apiKey"},
            },
            "secretsManager": {"phase": "Ready"},
        }
    )

    assert blocker == {
        "phase": "Error",
        "reason": "TemporalAuthModeSwitchRequiresFinalize",
        "message": "Finalize before switching auth mode.",
        "temporalCloud": {
            "phase": "Error",
            "reason": "TemporalAuthModeSwitchRequiresFinalize",
            "message": "Finalize before switching auth mode.",
            "auth": {"mode": "mtls"},
            "namespace": {"name": "demo.acct1", "authMode": "apiKey"},
        },
    }


def test_promote_candidate_uses_candidate_source_commit_for_route_version(
    monkeypatch,
):
    applied = []

    def capture_apply(resources, *, dry_run):
        applied.extend(resources)
        assert dry_run is False
        return [
            {
                "kind": resource["kind"],
                "name": resource["metadata"]["name"],
            }
            for resource in resources
        ]

    monkeypatch.setattr(reconcile.kube, "apply_resources", capture_apply)
    monkeypatch.setattr(reconcile.kube, "list_deployments", lambda *args, **kwargs: [])
    monkeypatch.setattr(reconcile.kube, "list_services", lambda *args, **kwargs: [])

    images = {
        "backend": "example.com/backend:git-e12b0ed9d450",
        "frontend": "example.com/frontend:git-e12b0ed9d450",
        "worker": "example.com/worker:git-e12b0ed9d450",
    }
    result = reconcile.promote_candidate_routes(
        reconcile.PromoteCandidateRoutesRequest(
            project="banking-money-transfer",
            demo_project=banking_project(),
            candidate={
                "images": images,
                "imageVersion": "git-e12b0ed9d450",
                "sourceCommitSha": "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
                "runtimeNamespace": "tmprl-dem-cld-banking-money-transfer-a1b2c",
                "services": {
                    "backend": "backend",
                    "frontend": "frontend",
                },
            },
        )
    ).status

    assert result["phase"] == "Promoted"

    ingress_route = next(
        resource
        for resource in applied
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "banking-money-transfer"
    )
    assert (
        ingress_route["metadata"]["annotations"][
            "platform.temporal.io/rollout-version"
        ]
        == "git-e12b0ed9d450"
    )
    assert (
        ingress_route["spec"]["routes"][0]["services"][0]["name"]
        == "frontend"
    )
    assert (
        ingress_route["spec"]["routes"][0]["services"][0]["namespace"]
        == "tmprl-dem-cld-banking-money-transfer-a1b2c"
    )

    http_route = next(
        resource
        for resource in applied
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "banking-money-transfer-http"
    )
    assert http_route["spec"]["entryPoints"] == ["web"]
    assert "tls" not in http_route["spec"]
    assert http_route["spec"]["routes"][0]["middlewares"] == [
        {
            "name": HTTPS_REDIRECT_MIDDLEWARE_NAME,
            "namespace": "tmprl-dem-cld-banking-money-transfer",
        }
    ]

    redirect_middleware = next(
        resource
        for resource in applied
        if resource["kind"] == "Middleware"
        and resource["metadata"]["name"] == HTTPS_REDIRECT_MIDDLEWARE_NAME
    )
    assert redirect_middleware["spec"]["redirectScheme"] == {
        "scheme": "https",
        "permanent": True,
    }


def test_promote_candidate_prunes_stale_plain_runtime_candidates(monkeypatch):
    deleted = []

    monkeypatch.setattr(reconcile.kube, "apply_resources",
        lambda resources, *, dry_run: [],
    )
    monkeypatch.setattr(reconcile.kube, "list_deployments",
        lambda namespace, *, label_selector: [
            {
                "name": "backend-git-old",
                "labels": {
                    "app.kubernetes.io/component": "backend",
                    "platform.temporal.io/rollout-version": "git-old",
                },
                "annotations": {},
            },
            {
                "name": "frontend-git-new",
                "labels": {
                    "app.kubernetes.io/component": "frontend",
                    "platform.temporal.io/rollout-version": "git-new",
                },
                "annotations": {},
            },
            {
                "name": "versioned-worker-git-old",
                "labels": {
                    "app.kubernetes.io/component": "versioned-worker",
                    "platform.temporal.io/rollout-version": "git-old",
                },
                "annotations": {},
            },
        ],
    )
    monkeypatch.setattr(reconcile.kube, "list_services",
        lambda namespace, *, label_selector: [
            {
                "name": "backend-git-old",
                "labels": {
                    "app.kubernetes.io/component": "backend",
                    "platform.temporal.io/rollout-version": "git-old",
                },
                "annotations": {
                    "platform.temporal.io/stable-service": "backend",
                },
            },
            {
                "name": "backend",
                "labels": {
                    "app.kubernetes.io/component": "backend",
                    "platform.temporal.io/rollout-version": "git-old",
                },
                "annotations": {},
            },
            {
                "name": "frontend-git-new",
                "labels": {
                    "app.kubernetes.io/component": "frontend",
                    "platform.temporal.io/rollout-version": "git-new",
                },
                "annotations": {
                    "platform.temporal.io/stable-service": "frontend",
                },
            },
        ],
    )

    def delete_deployment(namespace, name, *, dry_run):
        deleted.append(("Deployment", namespace, name, dry_run))
        return "deleting"

    def delete_service(namespace, name, *, dry_run):
        deleted.append(("Service", namespace, name, dry_run))
        return "deleting"

    monkeypatch.setattr(reconcile.kube, "delete_deployment", delete_deployment)
    monkeypatch.setattr(reconcile.kube, "delete_service", delete_service)

    demo_project = banking_project()
    demo_project["spec"]["components"]["versioned-worker"] = {
        "build": "worker",
        "worker": {
            "versioning": {
                "enabled": True,
                "deploymentName": "versioned-worker",
            }
        },
    }
    images = {
        "backend": "example.com/backend:git-new",
        "frontend": "example.com/frontend:git-new",
        "worker": "example.com/worker:git-new",
    }

    result = reconcile.prune_stale_runtime_candidates(
        reconcile.PruneStaleRuntimeCandidatesRequest(
            demo_project=demo_project,
            active_version="git-new",
        )
    ).status

    assert deleted == [
        (
            "Deployment",
            "tmprl-dem-cld-banking-money-transfer",
            "backend-git-old",
            False,
        ),
        (
            "Service",
            "tmprl-dem-cld-banking-money-transfer",
            "backend-git-old",
            False,
        ),
    ]
    assert result["phase"] == "Pruned"
    assert [resource["name"] for resource in result["resources"]] == [
        "backend-git-old",
        "backend-git-old",
    ]


def test_promote_candidate_prunes_previous_runtime_namespace(monkeypatch):
    deleted = []

    def delete_namespace(name, *, dry_run):
        deleted.append((name, dry_run))
        return "deleting"

    monkeypatch.setattr(reconcile.kube, "delete_namespace", delete_namespace)

    result = reconcile.prune_stale_runtime_candidates(
        reconcile.PruneStaleRuntimeCandidatesRequest(
            demo_project=banking_project(),
            active_version="git-new",
            active_runtime_namespace="tmprl-dem-cld-banking-money-transfer-new12",
            previous_runtime_namespace="tmprl-dem-cld-banking-money-transfer-old12",
        )
    ).status

    assert deleted == [("tmprl-dem-cld-banking-money-transfer-old12", False)]
    assert result["phase"] == "Pruned"
    assert result["resources"] == [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "name": "tmprl-dem-cld-banking-money-transfer-old12",
            "rolloutVersion": "git-new",
            "action": "deleting",
        }
    ]


def test_restore_active_runtime_routes_uses_explicit_active_version(monkeypatch):
    applied = []

    def capture_apply(rendered, *, dry_run):
        applied.extend(rendered)
        assert dry_run is False
        return []

    monkeypatch.setattr(reconcile.kube, "apply_resources", capture_apply)

    demo_project = banking_project()
    demo_project["metadata"] = {
        **demo_project["metadata"],
        "annotations": {
            resources.SOURCE_COMMIT_ANNOTATION: "f" * 40,
        }
    }

    result = reconcile.restore_active_runtime_routes(
        reconcile.RestoreActiveRuntimeRoutesRequest(
            project="banking-money-transfer",
            demo_project=demo_project,
            active_images=banking_images("git-old"),
            active_version="git-old",
            active_runtime_namespace="tmprl-dem-cld-banking-money-transfer-old12",
        )
    ).status

    ingress_route = next(
        resource
        for resource in applied
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "banking-money-transfer"
    )

    assert result["phase"] == "Restored"
    assert (
        ingress_route["spec"]["routes"][0]["services"][0]["name"]
        == "frontend"
    )
    assert (
        ingress_route["spec"]["routes"][0]["services"][0]["namespace"]
        == "tmprl-dem-cld-banking-money-transfer-old12"
    )


def test_delete_runtime_candidate_resources_deletes_only_failed_candidate(
    monkeypatch,
):
    deleted = []

    monkeypatch.setattr(
        reconcile.kube,
        "list_deployments",
        lambda namespace, *, label_selector: [
            {
                "name": "backend-git-failed",
                "labels": {
                    "app.kubernetes.io/component": "backend",
                    "platform.temporal.io/rollout-version": "git-failed",
                },
                "annotations": {},
            },
            {
                "name": "backend-git-active",
                "labels": {
                    "app.kubernetes.io/component": "backend",
                    "platform.temporal.io/rollout-version": "git-active",
                },
                "annotations": {},
            },
        ],
    )
    monkeypatch.setattr(
        reconcile.kube,
        "list_services",
        lambda namespace, *, label_selector: [
            {
                "name": "backend-git-failed",
                "labels": {
                    "app.kubernetes.io/component": "backend",
                    "platform.temporal.io/rollout-version": "git-failed",
                },
                "annotations": {
                    "platform.temporal.io/stable-service": "backend",
                },
            },
            {
                "name": "backend",
                "labels": {
                    "app.kubernetes.io/component": "backend",
                    "platform.temporal.io/rollout-version": "git-failed",
                },
                "annotations": {},
            },
        ],
    )

    def delete_deployment(namespace, name, *, dry_run):
        deleted.append(("Deployment", namespace, name, dry_run))
        return "deleting"

    def delete_service(namespace, name, *, dry_run):
        deleted.append(("Service", namespace, name, dry_run))
        return "deleting"

    monkeypatch.setattr(reconcile.kube, "delete_deployment", delete_deployment)
    monkeypatch.setattr(reconcile.kube, "delete_service", delete_service)

    result = reconcile.delete_runtime_candidate_resources(
        reconcile.DeleteRuntimeCandidateResourcesRequest(
            demo_project=banking_project(),
            candidate_version="git-failed",
        )
    ).status

    assert deleted == [
        (
            "Deployment",
            "tmprl-dem-cld-banking-money-transfer",
            "backend-git-failed",
            False,
        ),
        (
            "Service",
            "tmprl-dem-cld-banking-money-transfer",
            "backend-git-failed",
            False,
        ),
    ]
    assert result["phase"] == "Deleted"
    assert [resource["name"] for resource in result["resources"]] == [
        "backend-git-failed",
        "backend-git-failed",
    ]


def test_delete_runtime_candidate_resources_deletes_runtime_namespace(monkeypatch):
    deleted = []

    def delete_namespace(name, *, dry_run):
        deleted.append((name, dry_run))
        return "deleting"

    monkeypatch.setattr(reconcile.kube, "delete_namespace", delete_namespace)

    result = reconcile.delete_runtime_candidate_resources(
        reconcile.DeleteRuntimeCandidateResourcesRequest(
            demo_project=banking_project(),
            candidate_version="git-failed",
            candidate_runtime_namespace="tmprl-dem-cld-banking-money-transfer-bad12",
        )
    ).status

    assert deleted == [("tmprl-dem-cld-banking-money-transfer-bad12", False)]
    assert result["phase"] == "Deleted"
    assert result["resources"] == [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "name": "tmprl-dem-cld-banking-money-transfer-bad12",
            "rolloutVersion": "git-failed",
            "action": "deleting",
        }
    ]
