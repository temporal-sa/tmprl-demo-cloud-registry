from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


AUTH_MIDDLEWARE_NAME = "temporal-auth-required"
IMAGE_OVERRIDES_ANNOTATION = "registry.tmprl-demo.cloud/image-overrides"
SOURCE_COMMIT_ANNOTATION = "registry.tmprl-demo.cloud/source-commit-sha"
STANDARD_TEMPORAL_ENV = {
    "endpoint": "TEMPORAL_ADDRESS",
    "namespace": "TEMPORAL_NAMESPACE",
    "api_key": "TEMPORAL_API_KEY",
    "tls": "TEMPORAL_TLS",
}


def project_name(demo_project: dict[str, Any]) -> str:
    metadata = demo_project.get("metadata", {})
    if isinstance(metadata, dict) and isinstance(metadata.get("name"), str):
        return metadata["name"]
    return "unknown"


def project_namespace(demo_project: dict[str, Any]) -> str:
    spec = spec_of(demo_project)
    derived = spec.get("derived", {})
    if isinstance(derived, dict) and isinstance(derived.get("kubernetesNamespace"), str):
        return derived["kubernetesNamespace"]
    return f"tmprl-cloud-demo-{project_name(demo_project)}"


def project_host(demo_project: dict[str, Any]) -> str:
    spec = spec_of(demo_project)
    derived = spec.get("derived", {})
    if isinstance(derived, dict) and isinstance(derived.get("host"), str):
        return derived["host"]

    ingress = spec.get("ingress", {})
    if isinstance(ingress, dict) and isinstance(ingress.get("subdomain"), str):
        return f"{ingress['subdomain']}.tmprl-demo.cloud"

    return f"{project_name(demo_project)}.tmprl-demo.cloud"


def project_secret_prefix(demo_project: dict[str, Any]) -> str:
    spec = spec_of(demo_project)
    derived = spec.get("derived", {})
    if isinstance(derived, dict) and isinstance(derived.get("secretPrefix"), str):
        return derived["secretPrefix"]
    return f"tmprl-cloud-demo/{project_name(demo_project)}"


def spec_of(demo_project: dict[str, Any]) -> dict[str, Any]:
    spec = demo_project.get("spec")
    return spec if isinstance(spec, dict) else {}


def metadata_annotations(demo_project: dict[str, Any]) -> dict[str, str]:
    metadata = demo_project.get("metadata", {})
    if not isinstance(metadata, dict):
        return {}
    annotations = metadata.get("annotations", {})
    if not isinstance(annotations, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in annotations.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def parse_image_overrides(demo_project: dict[str, Any]) -> dict[str, str]:
    raw = metadata_annotations(demo_project).get(IMAGE_OVERRIDES_ANNOTATION)
    if not raw:
        return {}

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {}

    if not isinstance(decoded, dict):
        return {}

    return {
        str(build_name): str(image)
        for build_name, image in decoded.items()
        if isinstance(build_name, str) and isinstance(image, str) and image
    }


def source_commit_sha(demo_project: dict[str, Any]) -> str | None:
    value = metadata_annotations(demo_project).get(SOURCE_COMMIT_ANNOTATION)
    return value if value else None


def missing_image_overrides(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> list[str]:
    return [
        build_name
        for build_name in sorted(builds(demo_project))
        if build_name not in image_overrides
    ]


def base_resources(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        namespace_resource(demo_project),
        *rbac_resources(demo_project),
        platform_metadata_configmap(demo_project),
    ]


def runtime_resources(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
    *,
    include_routes: bool = True,
) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    resources.extend(service_resources(demo_project))

    if temporal_components(demo_project):
        resources.append(external_secret(demo_project))

    resources.extend(user_external_secrets(demo_project))
    resources.extend(deployment_resources(demo_project, image_overrides))

    if include_routes:
        resources.extend(promotion_resources(demo_project))
    return resources


def promotion_resources(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    resources = []
    if auth_required(demo_project):
        resources.append(auth_middleware(demo_project))
    resources.extend(ingress_resources(demo_project))
    return resources


def namespace_resource(demo_project: dict[str, Any]) -> dict[str, Any]:
    name = project_name(demo_project)
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/project": name,
            },
        },
    }


def rbac_resources(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    namespace = project_namespace(demo_project)
    role = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": "tenant-baseline",
            "namespace": namespace,
            "labels": common_labels(demo_project),
        },
        "rules": [],
    }

    resources = [role]
    for component_name in sorted(components(demo_project)):
        service_account_name = service_account_for_component(component_name)
        resources.append(
            {
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "name": service_account_name,
                    "namespace": namespace,
                    "labels": component_labels(demo_project, component_name),
                    "annotations": {
                        "platform.temporal.io/component": component_name,
                    },
                },
                "automountServiceAccountToken": False,
            }
        )
        resources.append(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": f"{service_account_name}-tenant-baseline",
                    "namespace": namespace,
                    "labels": component_labels(demo_project, component_name),
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": service_account_name,
                        "namespace": namespace,
                    }
                ],
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "tenant-baseline",
                },
            }
        )

    return resources


def platform_metadata_configmap(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "platform-project-metadata",
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
        },
        "data": {
            "project.json": json.dumps(project_metadata(demo_project), sort_keys=True),
            "registry.json": json.dumps(registry_entry(demo_project), sort_keys=True),
        },
    }


def service_resources(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    namespace = project_namespace(demo_project)
    for component_name, component in sorted(components(demo_project).items()):
        ports = component_ports(component)
        if not ports:
            continue

        resources.append(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": effective_service_name(component_name, component),
                    "namespace": namespace,
                    "labels": component_labels(demo_project, component_name),
                    "annotations": {
                        "platform.temporal.io/component": component_name,
                    },
                },
                "spec": {
                    "type": "ClusterIP",
                    "selector": selector_labels(demo_project, component_name),
                    "ports": [
                        {
                            "name": port["name"],
                            "port": port["servicePort"],
                            "targetPort": port["containerPort"],
                        }
                        for port in ports
                    ],
                },
            }
        )
    return resources


def deployment_resources(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    namespace = project_namespace(demo_project)
    for component_name, component in sorted(components(demo_project).items()):
        build_name = component.get("build")
        if not isinstance(build_name, str) or build_name not in image_overrides:
            continue

        resources.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": component_name,
                    "namespace": namespace,
                    "labels": component_labels(demo_project, component_name),
                    "annotations": {
                        "platform.temporal.io/build": build_name,
                    },
                },
                "spec": {
                    "replicas": component.get("replicas", 1),
                    "selector": {
                        "matchLabels": selector_labels(demo_project, component_name),
                    },
                    "template": {
                        "metadata": {
                            "labels": component_labels(demo_project, component_name),
                        },
                        "spec": {
                            "serviceAccountName": service_account_for_component(
                                component_name
                            ),
                            "automountServiceAccountToken": False,
                            "containers": [
                                container(
                                    demo_project,
                                    component_name,
                                    component,
                                    image_overrides[build_name],
                                )
                            ],
                        },
                    },
                },
            }
        )
    return resources


def container(
    demo_project: dict[str, Any],
    component_name: str,
    component: dict[str, Any],
    image: str,
) -> dict[str, Any]:
    rendered: dict[str, Any] = {
        "name": component_name,
        "image": image,
        "imagePullPolicy": "IfNotPresent",
    }

    if component.get("command"):
        rendered["command"] = component["command"]
    if component.get("args"):
        rendered["args"] = component["args"]

    ports = component_ports(component)
    if ports:
        rendered["ports"] = [
            {
                "name": port["name"],
                "containerPort": port["containerPort"],
            }
            for port in ports
        ]

    env_from = env_from_refs(demo_project, component)
    if env_from:
        rendered["envFrom"] = env_from

    env = env_vars(component)
    if env:
        rendered["env"] = env

    return rendered


def env_from_refs(
    demo_project: dict[str, Any],
    component: dict[str, Any],
) -> list[dict[str, Any]]:
    refs = []
    if component_needs_temporal(component):
        refs.append({"secretRef": {"name": "temporal-credentials"}})

    references = component.get("secrets")
    if isinstance(references, list):
        for secret_name in sorted({item for item in references if isinstance(item, str)}):
            refs.append(
                {
                    "secretRef": {
                        "name": user_secret_kubernetes_name(secret_name),
                    }
                }
            )

    return refs


def env_vars(component: dict[str, Any]) -> list[dict[str, str]]:
    rendered = [
        {"name": item["name"], "value": str(item["value"])}
        for item in component.get("env", [])
        if isinstance(item, dict)
    ]

    worker = component.get("worker")
    if isinstance(worker, dict):
        rendered.extend(worker_versioning_env(worker))

    return rendered


def worker_versioning_env(worker: dict[str, Any]) -> list[dict[str, str]]:
    versioning = worker.get("versioning")
    if not isinstance(versioning, dict):
        return []

    env = versioning.get("env")
    if not isinstance(env, dict):
        return []

    rendered = []
    if isinstance(env.get("enabled"), str):
        rendered.append(
            {
                "name": env["enabled"],
                "value": str(versioning.get("enabled", False)).lower(),
            }
        )
    if isinstance(env.get("buildId"), str):
        rendered.append(
            {
                "name": env["buildId"],
                "value": "PLACEHOLDER_BUILD_ID",
            }
        )
    if isinstance(env.get("deploymentName"), str):
        rendered.append(
            {
                "name": env["deploymentName"],
                "value": worker["taskQueue"],
            }
        )
    return rendered


def ingress_resources(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    route_specs = ingress_routes(demo_project)
    if not route_specs:
        return []

    routes = [ingress_route(demo_project, route_spec) for route_spec in route_specs]
    base = {
        "apiVersion": "traefik.io/v1alpha1",
        "kind": "IngressRoute",
        "metadata": {
            "name": project_name(demo_project),
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/host": project_host(demo_project),
                "platform.temporal.io/temporal-auth-required": str(
                    auth_required(demo_project)
                ).lower(),
            },
        },
        "spec": {
            "entryPoints": ["websecure"],
            "routes": routes,
            "tls": {
                "secretName": f"{project_name(demo_project)}-tls",
            },
        },
    }

    http = {
        **base,
        "metadata": {
            **base["metadata"],
            "name": f"{project_name(demo_project)}-http",
        },
        "spec": {
            **base["spec"],
            "entryPoints": ["web"],
        },
    }
    http["spec"].pop("tls", None)
    return [base, http]


def ingress_route(
    demo_project: dict[str, Any],
    route_spec: dict[str, Any],
) -> dict[str, Any]:
    match = f"Host(`{project_host(demo_project)}`)"
    path_prefix = route_spec.get("pathPrefix")
    if path_prefix:
        match = f"{match} && PathPrefix(`{path_prefix}`)"

    component_name = route_spec["component"]
    component = components(demo_project)[component_name]
    route: dict[str, Any] = {
        "kind": "Rule",
        "match": match,
        "priority": 1000,
        "services": [
            {
                "name": effective_service_name(component_name, component),
                "namespace": project_namespace(demo_project),
                "passHostHeader": True,
                "port": service_port(component, route_spec["port"]),
            }
        ],
    }
    if auth_required(demo_project):
        route["middlewares"] = [
            {
                "name": AUTH_MIDDLEWARE_NAME,
                "namespace": project_namespace(demo_project),
            }
        ]
    return route


def auth_middleware(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "traefik.io/v1alpha1",
        "kind": "Middleware",
        "metadata": {
            "name": AUTH_MIDDLEWARE_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/description": "Shared Temporal employee auth forward-auth middleware.",
            },
        },
        "spec": {
            "forwardAuth": {
                "address": "https://auth.tmprl-demo.cloud/traefik/verify",
                "trustForwardHeader": True,
                "authResponseHeaders": [
                    "X-Temporal-Auth-JWT",
                    "X-Temporal-Auth-Subject",
                    "X-Temporal-Auth-Email",
                ],
            }
        },
    }


def external_secret(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {
            "name": "temporal-credentials",
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/secret-prefix": project_secret_prefix(demo_project),
            },
        },
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {
                "name": "project-secrets",
                "kind": "SecretStore",
            },
            "target": {
                "name": "temporal-credentials",
                "creationPolicy": "Owner",
            },
            "data": [
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["endpoint"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(demo_project),
                        "property": "address",
                    },
                },
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["namespace"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(demo_project),
                        "property": "namespace",
                    },
                },
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["api_key"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(demo_project),
                        "property": "apiKey",
                    },
                },
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["tls"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(demo_project),
                        "property": "tls",
                    },
                },
            ],
        },
    }


def user_external_secrets(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    resources = []
    for secret_name in user_secret_names(demo_project):
        resources.append(
            {
                "apiVersion": "external-secrets.io/v1",
                "kind": "ExternalSecret",
                "metadata": {
                    "name": user_secret_kubernetes_name(secret_name),
                    "namespace": project_namespace(demo_project),
                    "labels": common_labels(demo_project),
                    "annotations": {
                        "platform.temporal.io/secret-source": user_secret_remote_path(
                            demo_project,
                            secret_name,
                        ),
                    },
                },
                "spec": {
                    "refreshInterval": "1h",
                    "secretStoreRef": {
                        "name": "project-secrets",
                        "kind": "SecretStore",
                    },
                    "target": {
                        "name": user_secret_kubernetes_name(secret_name),
                        "creationPolicy": "Owner",
                    },
                    "dataFrom": [
                        {
                            "extract": {
                                "key": user_secret_remote_path(demo_project, secret_name),
                            }
                        }
                    ],
                },
            }
        )
    return resources


def build_reconcile_status(
    demo_project: dict[str, Any],
    *,
    phase: str,
    message: str,
    mode: str,
    applied_resources: list[dict[str, Any]],
    image_overrides: dict[str, str],
    missing_images: list[str],
    error: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    metadata = demo_project.get("metadata", {})
    generation = metadata.get("generation", 0) if isinstance(metadata, dict) else 0
    candidate_status: dict[str, Any] = {
        "phase": "WaitingForImages" if missing_images else "CandidateApplied",
        "images": image_overrides,
        "message": (
            f"Missing image override(s): {', '.join(missing_images)}"
            if missing_images
            else "Image overrides are present for every build."
        ),
    }
    commit_sha = source_commit_sha(demo_project)
    if commit_sha:
        candidate_status["sourceCommitSha"] = commit_sha
    candidate_url = smoke_check_internal_url(demo_project)
    if candidate_url and not missing_images:
        candidate_status["url"] = candidate_url

    status: dict[str, Any] = {
        "observedGeneration": generation if isinstance(generation, int) else 0,
        "phase": phase,
        "message": message,
        "externalResources": external_resource_status(demo_project),
        "candidateRolloutStatus": candidate_status,
        "smokeCheckStatus": smoke_check_status(demo_project, phase),
        "conditions": [
            {
                "type": "Reconciled",
                "status": "False" if error else "True",
                "observedGeneration": generation if isinstance(generation, int) else 0,
                "reason": "Error" if error else phase,
                "message": error or message,
                "lastTransitionTime": now,
            },
            {
                "type": "ImagesReady",
                "status": "False" if missing_images else "True",
                "observedGeneration": generation if isinstance(generation, int) else 0,
                "reason": "MissingImageOverrides" if missing_images else "ImagesResolved",
                "message": (
                    f"Missing image override(s): {', '.join(missing_images)}"
                    if missing_images
                    else "Every build has an immutable image override."
                ),
                "lastTransitionTime": now,
            },
        ],
        "appliedResources": applied_resources,
        "reconcileMode": mode,
    }

    if image_overrides and not missing_images:
        version = source_commit_sha(demo_project) or "manual-image-overrides"
        status["activeImageVersion"] = version
        status["activeImages"] = image_overrides

    if error:
        status["lastError"] = {
            "reason": "ReconcileFailed",
            "message": error,
            "lastTransitionTime": now,
        }

    return status


def external_resource_status(demo_project: dict[str, Any]) -> dict[str, Any]:
    prefix = project_secret_prefix(demo_project)
    build_names = sorted(builds(demo_project))
    return {
        "ecr": {
            "phase": "ManualSetupRequired",
            "repositories": [
                f"tmprl-cloud-demo/{project_name(demo_project)}/{build_name}"
                for build_name in build_names
            ],
        },
        "secretsManager": {
            "phase": "ManualSetupRequired",
            "temporalCredentials": temporal_secret_remote_path(demo_project),
            "userManaged": [
                user_secret_remote_path(demo_project, name)
                for name in user_secret_names(demo_project)
            ],
        },
        "temporalCloud": {
            "phase": "ManualSetupRequired",
            "namespace": project_name(demo_project),
            "credentialsSecret": temporal_secret_remote_path(demo_project),
        },
    }


def smoke_check_status(demo_project: dict[str, Any], phase: str) -> dict[str, Any]:
    smoke_check = spec_of(demo_project).get("smokeCheck")
    if not isinstance(smoke_check, dict):
        return {
            "phase": "NotConfigured",
            "message": "No smoke check configured.",
        }

    return {
        "phase": "Pending" if phase in {"CandidateApplied", "SmokeCheckPending"} else phase,
        "path": smoke_check.get("path"),
        "expectedStatus": smoke_check.get("expectedStatus"),
        "message": "Smoke check is ready to run after candidate deployment.",
    }


def smoke_check_internal_url(demo_project: dict[str, Any]) -> str | None:
    smoke_check = spec_of(demo_project).get("smokeCheck")
    if not isinstance(smoke_check, dict) or not isinstance(smoke_check.get("path"), str):
        return None

    path = smoke_check["path"]
    route = best_route_for_path(demo_project, path)
    if route is None:
        return None

    component_name = route.get("component")
    port_name = route.get("port")
    if not isinstance(component_name, str) or not isinstance(port_name, str):
        return None

    component = components(demo_project).get(component_name)
    if component is None:
        return None

    service_name = effective_service_name(component_name, component)
    port = service_port(component, port_name)
    namespace = project_namespace(demo_project)
    return f"http://{service_name}.{namespace}.svc.cluster.local:{port}{path}"


def best_route_for_path(
    demo_project: dict[str, Any],
    path: str,
) -> dict[str, Any] | None:
    catchall = None
    prefixed: list[tuple[int, dict[str, Any]]] = []
    for route in ingress_routes(demo_project):
        prefix = route.get("pathPrefix")
        if isinstance(prefix, str):
            if path.startswith(prefix):
                prefixed.append((len(prefix), route))
        else:
            catchall = route

    if prefixed:
        return sorted(prefixed, key=lambda item: item[0], reverse=True)[0][1]
    return catchall


def registry_entry(demo_project: dict[str, Any]) -> dict[str, Any]:
    spec = spec_of(demo_project)
    registry = spec.get("registry", {})
    if not isinstance(registry, dict):
        registry = {}
    return {
        "name": project_name(demo_project),
        "title": registry.get("title", project_name(demo_project)),
        "summary": registry.get("summary", ""),
        "host": project_host(demo_project),
        "appUrl": f"https://{project_host(demo_project)}",
        "sourceRepo": spec.get("source", {}).get("repo") if isinstance(spec.get("source"), dict) else None,
        "temporalNamespace": project_name(demo_project),
        "authRequired": auth_required(demo_project),
        "tags": registry.get("tags", []),
        "temporalSdks": registry.get("temporalSdks", []),
        "links": registry.get("links", {}),
        "assets": registry.get("assets", {}),
    }


def project_metadata(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": project_name(demo_project),
        "namespace": project_namespace(demo_project),
        "host": project_host(demo_project),
        "source": spec_of(demo_project).get("source", {}),
        "builds": sorted(builds(demo_project)),
        "components": sorted(components(demo_project)),
        "userSecrets": {
            secret_name: {
                "remotePath": user_secret_remote_path(demo_project, secret_name),
                "kubernetesSecret": user_secret_kubernetes_name(secret_name),
                "components": sorted(
                    components_for_user_secret(demo_project, secret_name)
                ),
            }
            for secret_name in user_secret_names(demo_project)
        },
        "temporal": {
            "namespace": project_name(demo_project),
            "secretPrefix": project_secret_prefix(demo_project),
            "remotePath": temporal_secret_remote_path(demo_project),
            "standardEnv": STANDARD_TEMPORAL_ENV,
            "components": sorted(temporal_components(demo_project)),
        },
    }


def builds(demo_project: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = spec_of(demo_project).get("builds")
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def components(demo_project: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = spec_of(demo_project).get("components")
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def component_ports(component: dict[str, Any]) -> list[dict[str, Any]]:
    ports = component.get("ports")
    if not isinstance(ports, list):
        return []
    return [port for port in ports if isinstance(port, dict)]


def ingress_routes(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    ingress = spec_of(demo_project).get("ingress")
    if not isinstance(ingress, dict):
        return []

    routes = ingress.get("routes")
    if isinstance(routes, list):
        return [route for route in routes if isinstance(route, dict)]

    component = ingress.get("component")
    port = ingress.get("port")
    if isinstance(component, str) and isinstance(port, str):
        return [{"component": component, "port": port}]

    return []


def auth_required(demo_project: dict[str, Any]) -> bool:
    ingress = spec_of(demo_project).get("ingress")
    if not isinstance(ingress, dict):
        return True
    return bool(ingress.get("temporalAuthRequired", True))


def component_needs_temporal(component: dict[str, Any]) -> bool:
    return (
        isinstance(component.get("worker"), dict)
        or component.get("temporalAccess") is True
    )


def temporal_components(demo_project: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        name: component
        for name, component in components(demo_project).items()
        if component_needs_temporal(component)
    }


def components_for_user_secret(
    demo_project: dict[str, Any],
    secret_name: str,
) -> list[str]:
    names = []
    for component_name, component in components(demo_project).items():
        references = component.get("secrets")
        if isinstance(references, list) and secret_name in references:
            names.append(component_name)
    return names


def user_secret_names(demo_project: dict[str, Any]) -> list[str]:
    names: set[str] = set()
    for component in components(demo_project).values():
        references = component.get("secrets")
        if not isinstance(references, list):
            continue
        names.update(secret for secret in references if isinstance(secret, str))
    return sorted(names)


def temporal_secret_remote_path(demo_project: dict[str, Any]) -> str:
    return f"{project_secret_prefix(demo_project)}/temporal"


def user_secret_remote_path(demo_project: dict[str, Any], secret_name: str) -> str:
    return f"{project_secret_prefix(demo_project)}/{secret_name}"


def user_secret_kubernetes_name(secret_name: str) -> str:
    return f"user-secret-{secret_name}"


def effective_service_name(component_name: str, component: dict[str, Any]) -> str:
    service_name = component.get("serviceName")
    if isinstance(service_name, str):
        return service_name
    return component_name


def service_port(component: dict[str, Any], port_name: str) -> int:
    for port in component_ports(component):
        if port["name"] == port_name:
            return int(port["servicePort"])
    raise ValueError(f"component is missing port {port_name!r}")


def service_account_for_component(component_name: str) -> str:
    return component_name


def common_labels(demo_project: dict[str, Any]) -> dict[str, str]:
    name = project_name(demo_project)
    return {
        "app.kubernetes.io/name": name,
        "app.kubernetes.io/managed-by": "demo-registry",
        "platform.temporal.io/project": name,
    }


def component_labels(
    demo_project: dict[str, Any],
    component_name: str,
) -> dict[str, str]:
    return {
        **common_labels(demo_project),
        "app.kubernetes.io/component": component_name,
    }


def selector_labels(
    demo_project: dict[str, Any],
    component_name: str,
) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": project_name(demo_project),
        "app.kubernetes.io/component": component_name,
    }
