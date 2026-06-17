from __future__ import annotations

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


def test_plan_project_finalization_uses_source_watch_namespace(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_SOURCE_WATCH_NAMESPACE", "source-watch")

    plan = reconcile.load_project_finalization_plan(
        reconcile.ProjectFinalizationRequest(banking_project())
    ).plan

    assert plan["action"] == "finalize"
    assert plan["project"] == "banking-money-transfer"
    assert plan["namespace"] == "tmprl-dem-cld-banking-money-transfer"
    assert plan["sourceWatchNamespace"] == "source-watch"
    assert plan["gitRepository"] == "banking-money-transfer-source"
    assert set(plan) == {
        "action",
        "demoProject",
        "project",
        "namespace",
        "sourceWatchNamespace",
        "gitRepository",
        "sourceAuthSecret",
        "checkedAt",
    }


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

    monkeypatch.setattr(reconcile, "apply_resources", capture_apply)
    monkeypatch.setattr(reconcile, "list_deployments", lambda *args, **kwargs: [])
    monkeypatch.setattr(reconcile, "list_services", lambda *args, **kwargs: [])

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
                "services": {
                    "backend": "backend-git-e12b0ed9d450",
                    "frontend": "frontend-git-e12b0ed9d450",
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
        == "frontend-git-e12b0ed9d450"
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

    monkeypatch.setattr(
        reconcile,
        "apply_resources",
        lambda resources, *, dry_run: [],
    )
    monkeypatch.setattr(
        reconcile,
        "list_deployments",
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
    monkeypatch.setattr(
        reconcile,
        "list_services",
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

    monkeypatch.setattr(reconcile, "delete_deployment", delete_deployment)
    monkeypatch.setattr(reconcile, "delete_service", delete_service)

    demo_project = banking_project()
    demo_project["spec"]["components"]["versioned-worker"] = {
        "build": "worker",
        "worker": {
            "versioning": {
                "enabled": True,
                "taskQueue": "versioned-worker",
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
