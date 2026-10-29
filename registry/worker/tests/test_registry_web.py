from __future__ import annotations

import pytest

from demo_registry_operator.resources import (
    AUTH_HEADER_STRIP_MIDDLEWARE_NAME,
    AUTH_MIDDLEWARE_NAME,
    HTTPS_REDIRECT_MIDDLEWARE_NAME,
    REGISTRY_AUTH_PUBLIC_PATH_PREFIXES,
    REGISTRY_CATALOG_ANNOTATION,
    REGISTRY_LINK_PREVIEW_USER_AGENT_PATTERN,
    SOURCE_COMMIT_ANNOTATION,
    auth_required,
    auth_header_strip_middleware,
    auth_middleware,
    deployment_resources,
    ingress_resources,
    runtime_secret_resources,
)


def registry_project(*, temporal_auth_required=False):
    return {
        "metadata": {
            "name": "registry-operator",
            "annotations": {
                REGISTRY_CATALOG_ANNOTATION: "true",
                SOURCE_COMMIT_ANNOTATION: "abcdef1234567890abcdef1234567890abcdef12",
            },
        },
        "spec": {
            "builds": {
                "web": {
                    "context": ".",
                    "dockerfile": "registry/web/Dockerfile",
                },
            },
            "components": {
                "web": {
                    "build": "web",
                    "env": [
                        {
                            "name": "AUTH_MODE",
                            "value": "google",
                        },
                    ],
                    "ports": [
                        {
                            "containerPort": 8080,
                            "servicePort": 80,
                        },
                    ],
                },
            },
            "ingress": {
                "subdomain": "catalog",
                "temporalAuthRequired": temporal_auth_required,
            },
        },
    }


def test_registry_web_does_not_request_temporal_credentials():
    secrets = runtime_secret_resources(registry_project())

    assert [secret["metadata"]["name"] for secret in secrets] == ["shared-auth"]
    assert (
        secrets[0]["spec"]["dataFrom"][0]["extract"]["key"]
        == "tmprl-dem-cld/platform/shared-auth"
    )


def test_registry_catalog_route_is_not_forward_auth_protected():
    resources = ingress_resources(
        registry_project(),
        image_overrides={"web": "example.com/registry-web:git-abcdef123456"},
    )

    assert auth_required(registry_project()) is False
    https = next(
        resource
        for resource in resources
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "registry-operator"
    )
    http = next(
        resource
        for resource in resources
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "registry-operator-http"
    )

    assert "middlewares" not in https["spec"]["routes"][0]
    assert http["spec"]["routes"][0]["middlewares"] == [
        {
            "name": HTTPS_REDIRECT_MIDDLEWARE_NAME,
            "namespace": "tmprl-dem-cld-registry-operator",
        }
    ]


def test_registry_auth_endpoints_stay_public_when_catalog_is_protected():
    resources = ingress_resources(
        registry_project(temporal_auth_required=True),
        image_overrides={"web": "example.com/registry-web:git-abcdef123456"},
    )

    https = next(
        resource
        for resource in resources
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "registry-operator"
    )
    route_by_match = {
        route["match"]: route
        for route in https["spec"]["routes"]
    }

    for path_prefix in REGISTRY_AUTH_PUBLIC_PATH_PREFIXES:
        public_route = route_by_match[
            f"Host(`catalog.tmprl-demo.cloud`) && PathPrefix(`{path_prefix}`)"
        ]
        assert public_route["priority"] == 2000
        assert "middlewares" not in public_route

    catchall = route_by_match["Host(`catalog.tmprl-demo.cloud`)"]
    assert catchall["priority"] == 1000
    assert catchall["middlewares"] == [
        {
            "name": AUTH_HEADER_STRIP_MIDDLEWARE_NAME,
            "namespace": "tmprl-dem-cld-registry-operator",
        },
        {
            "name": AUTH_MIDDLEWARE_NAME,
            "namespace": "tmprl-dem-cld-registry-operator",
        },
    ]


@pytest.mark.parametrize(
    ("cluster", "header_matcher"),
    [("blue", "HeaderRegexp"), (None, "HeadersRegexp")],
)
def test_registry_link_preview_crawlers_reach_page_shells_without_auth(
    monkeypatch, cluster, header_matcher
):
    if cluster:
        monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", cluster)
    else:
        monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    resources = ingress_resources(
        registry_project(temporal_auth_required=True),
        image_overrides={"web": "example.com/registry-web:git-abcdef123456"},
    )
    https = next(
        resource
        for resource in resources
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "registry-operator"
    )
    preview = [
        route for route in https["spec"]["routes"] if "User-Agent" in route["match"]
    ]

    assert len(preview) == 1
    assert preview[0]["match"] == (
        "Host(`catalog.tmprl-demo.cloud`) && (Path(`/`) || Path(`/courses`) || "
        "PathPrefix(`/demos/`) || PathPrefix(`/courses/`)) && "
        f"{header_matcher}(`User-Agent`, `{REGISTRY_LINK_PREVIEW_USER_AGENT_PATTERN}`)"
    )
    assert preview[0]["priority"] == 2000
    assert "middlewares" not in preview[0]


def test_registry_web_mounts_catalog_configmaps():
    deployments = deployment_resources(
        registry_project(),
        {"web": "example.com/registry-web:git-abcdef123456"},
    )
    assert len(deployments) == 1

    pod_spec = deployments[0]["spec"]["template"]["spec"]
    volume_names = {volume["name"] for volume in pod_spec["volumes"]}
    assert volume_names == {"catalog", "catalog-status"}

    container = pod_spec["containers"][0]
    assert {"name": "AUTH_MODE", "value": "google"} in container["env"]
    assert {"secretRef": {"name": "shared-auth"}} in container["envFrom"]


def test_forward_auth_uses_internal_catalog_service():
    middleware = auth_middleware(
        {
            "metadata": {"name": "banking-money-transfer"},
            "spec": {},
        }
    )

    assert (
        middleware["spec"]["forwardAuth"]["address"]
        == "http://web.tmprl-dem-cld-registry-operator.svc.cluster.local/_auth/verify"
    )
    assert middleware["spec"]["forwardAuth"]["authRequestHeaders"] == [
        "Cookie",
        "X-Forwarded-Host",
        "X-Forwarded-Method",
        "X-Forwarded-Proto",
        "X-Forwarded-Uri",
    ]
    assert "X-Temporal-Auth-Name" in middleware["spec"]["forwardAuth"][
        "authResponseHeaders"
    ]


def test_protected_routes_strip_identity_headers_before_forward_auth():
    resources = ingress_resources(
        {
            "metadata": {"name": "banking-money-transfer"},
            "spec": {
                "components": {
                    "frontend": {
                        "ports": [{"containerPort": 8080, "servicePort": 80}],
                    }
                },
                "ingress": {"subdomain": "banking-app"},
            },
        },
        image_overrides={},
    )

    route = next(
        resource
        for resource in resources
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "banking-money-transfer"
    )
    assert route["spec"]["routes"][0]["middlewares"] == [
        {
            "name": AUTH_HEADER_STRIP_MIDDLEWARE_NAME,
            "namespace": "tmprl-dem-cld-banking-money-transfer",
        },
        {
            "name": AUTH_MIDDLEWARE_NAME,
            "namespace": "tmprl-dem-cld-banking-money-transfer",
        },
    ]

    middleware = auth_header_strip_middleware(
        {
            "metadata": {"name": "banking-money-transfer"},
            "spec": {},
        }
    )
    assert middleware["spec"]["headers"]["customRequestHeaders"] == {
        "X-Temporal-Auth-JWT": "",
        "X-Temporal-Auth-Subject": "",
        "X-Temporal-Auth-Email": "",
        "X-Temporal-Auth-Name": "",
    }


def webhook_project(*, temporal_auth_required=True):
    return {
        "metadata": {"name": "agentic-sdlc"},
        "spec": {
            "components": {
                "web": {
                    "ports": [{"containerPort": 8080, "servicePort": 80}],
                },
            },
            "ingress": {
                "subdomain": "agentic-sdlc",
                "temporalAuthRequired": temporal_auth_required,
                "routes": [
                    {
                        "pathPrefix": "/webhooks/github",
                        "component": "web",
                        "port": "http",
                        "public": True,
                    },
                    {"pathPrefix": "/", "component": "web", "port": "http"},
                ],
            },
        },
    }


def https_routes_by_match(demo_project):
    resources = ingress_resources(demo_project, image_overrides={})
    https = next(
        resource
        for resource in resources
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == demo_project["metadata"]["name"]
    )
    return {route["match"]: route for route in https["spec"]["routes"]}


def test_public_route_skips_forward_auth_but_still_strips_identity_headers():
    routes = https_routes_by_match(webhook_project())
    namespace = "tmprl-dem-cld-agentic-sdlc"

    webhook = routes[
        "Host(`agentic-sdlc.tmprl-demo.cloud`) && PathPrefix(`/webhooks/github`)"
    ]
    assert webhook["middlewares"] == [
        {"name": AUTH_HEADER_STRIP_MIDDLEWARE_NAME, "namespace": namespace},
    ]
    root = routes["Host(`agentic-sdlc.tmprl-demo.cloud`) && PathPrefix(`/`)"]
    assert root["middlewares"] == [
        {"name": AUTH_HEADER_STRIP_MIDDLEWARE_NAME, "namespace": namespace},
        {"name": AUTH_MIDDLEWARE_NAME, "namespace": namespace},
    ]
    # Explicit priorities disable Traefik's rule-length ordering, so the longer
    # prefix has to outrank the root route itself.
    assert webhook["priority"] > root["priority"]


def test_public_route_has_no_middlewares_when_project_auth_is_off():
    routes = https_routes_by_match(webhook_project(temporal_auth_required=False))

    assert all("middlewares" not in route for route in routes.values())


def test_ingress_route_host_header_passthrough_defaults_and_override():
    resources = ingress_resources(
        {
            "metadata": {"name": "banking-money-transfer"},
            "spec": {
                "components": {
                    "api": {
                        "ports": [
                            {"containerPort": 8000, "servicePort": 8000},
                        ],
                    },
                    "web": {
                        "ports": [
                            {"containerPort": 5173, "servicePort": 80},
                        ],
                    },
                },
                "ingress": {
                    "subdomain": "banking-app",
                    "temporalAuthRequired": False,
                    "routes": [
                        {
                            "pathPrefix": "/api",
                            "component": "api",
                            "port": "http",
                        },
                        {
                            "pathPrefix": "/",
                            "component": "web",
                            "port": "http",
                            "passHostHeader": False,
                        },
                    ],
                },
            },
        },
        image_overrides={},
    )

    ingress_route = next(
        resource
        for resource in resources
        if resource["kind"] == "IngressRoute"
        and resource["metadata"]["name"] == "banking-money-transfer"
    )
    services_by_match = {
        route["match"]: route["services"][0]
        for route in ingress_route["spec"]["routes"]
    }

    assert (
        services_by_match[
            "Host(`banking-app.tmprl-demo.cloud`) && PathPrefix(`/api`)"
        ]["passHostHeader"]
        is True
    )
    assert (
        services_by_match[
            "Host(`banking-app.tmprl-demo.cloud`) && PathPrefix(`/`)"
        ]["passHostHeader"]
        is False
    )
