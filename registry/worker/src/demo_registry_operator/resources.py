from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import UTC, datetime
from typing import Any

from demo_registry_operator import derive


AUTH_MIDDLEWARE_NAME = "temporal-auth-required"
AUTH_HEADER_STRIP_MIDDLEWARE_NAME = "temporal-auth-strip-headers"
HTTPS_REDIRECT_MIDDLEWARE_NAME = "https-redirect"
IMAGE_OVERRIDES_ANNOTATION = "registry.tmprl-demo.cloud/image-overrides"
SOURCE_COMMIT_ANNOTATION = "registry.tmprl-demo.cloud/source-commit-sha"
SOURCE_ARTIFACT_DIGEST_ANNOTATION = "registry.tmprl-demo.cloud/source-artifact-digest"
RECONCILE_KEY_STATUS_FIELD = "reconcileKey"
CANARY_LABEL = "registry.tmprl-demo.cloud/canary"
CANARY_KIND_LABEL = "registry.tmprl-demo.cloud/canary-kind"
OPERATOR_ROLLOUT_CANARY_KIND = "operator-rollout"
FLUX_GIT_REPOSITORY_API_VERSION = "source.toolkit.fluxcd.io/v1"
TEMPORAL_WORKER_CONTROLLER_API_VERSION = "temporal.io/v1alpha1"
TRAEFIK_API_VERSION = "traefik.containo.us/v1alpha1"
CATALOG_AUTH_VERIFY_URL = (
    "http://web.tmprl-dem-cld-registry-operator.svc.cluster.local/_auth/verify"
)
CATALOG_AUTH_REQUEST_HEADERS = [
    "Cookie",
    "X-Forwarded-Host",
    "X-Forwarded-Method",
    "X-Forwarded-Proto",
    "X-Forwarded-Uri",
]
CATALOG_STATUS_NAMESPACE = "tmprl-dem-cld-registry-operator"
CATALOG_STATUS_CONFIGMAP = "registry-catalog-status"
CATALOG_CONFIGMAP = "registry-catalog"
CATALOG_STATUS_FIELDS = [
    "phase",
    "message",
    "kubernetesNamespaceStatus",
    "externalResources",
    "externalResourceBlock",
    "externalSecrets",
    "build",
    "candidate",
    "readiness",
    "workerDeployments",
    "smokeCheck",
    "prune",
    "activeImageVersion",
    "finalizeStatus",
    "lastError",
]
CATALOG_STATUS_MOUNT_PATH = "/app/status"
CATALOG_STATUS_VOLUME_NAME = "catalog-status"
CATALOG_MOUNT_PATH = "/app/data"
CATALOG_VOLUME_NAME = "catalog"
REGISTRY_WEB_COMPONENT_NAME = "web"
FINALIZATION_POLICY_ANNOTATION = "registry.tmprl-demo.cloud/finalization-policy"
FINALIZATION_POLICY_RETAIN = "retain"
REGISTRY_CATALOG_ANNOTATION = "registry.tmprl-demo.cloud/catalog"
REGISTRY_SHARED_AUTH_SECRET_NAME = "shared-auth"
REGISTRY_SHARED_AUTH_SECRET_PATH = "tmprl-dem-cld/platform/shared-auth"
SOURCE_AUTH_SECRET_SUFFIX = "auth"
REGISTRY_AUTH_PUBLIC_PATH_PREFIXES = [
    "/auth",
    "/oauth/google",
    "/api/auth",
    "/static",
    "/_auth",
    "/traefik",
]
TEMPORAL_BOOTSTRAP_SECRET_PATH = "tmprl-dem-cld/platform/temporal-cloud-bootstrap"
TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES = [
    "TEMPORAL_CLOUD_API_KEY",
    "TEMPORAL_CLOUD_ACCOUNT_ID",
    "TEMPORAL_CLOUD_REGION",
]
TEMPORAL_SECRET_PROPERTIES = ["address", "namespace", "apiKey", "tls"]
STANDARD_TEMPORAL_ENV = {
    "endpoint": "TEMPORAL_ADDRESS",
    "namespace": "TEMPORAL_NAMESPACE",
    "api_key": "TEMPORAL_API_KEY",
    "tls": "TEMPORAL_TLS",
}
TEMPORAL_SECRET_NAME = "temporal-credentials"
TEMPORAL_WORKER_RUNTIME_SECRET_NAME = "temporal-worker-runtime"
TEMPORAL_CONNECTION_NAME = "temporal"
TEMPORAL_PROXY_BUILD_NAME = "temporal-cloud-proxy"
TEMPORAL_PROXY_CONTAINER_NAME = "temporal-cloud-proxy"
TEMPORAL_PROXY_IMAGE_ENV = "DEMO_REGISTRY_TEMPORAL_PROXY_IMAGE"
TEMPORAL_PROXY_LOCAL_ADDRESS = "127.0.0.1:7233"
TEMPORAL_PROXY_DEFAULT_ADDRESS_ENVS = ["TEMPORAL_ADDRESS", "TEMPORAL_HOST"]
TEMPORAL_WORKER_DEFAULT_ROLLOUT_STRATEGY = "AllAtOnce"
TEMPORAL_WORKER_DEFAULT_SCALEDOWN_DELAY = "1h"
TEMPORAL_WORKER_DEFAULT_DELETE_DELAY = "24h"
TEMPORAL_WORKER_ROLLOUT_STRATEGIES = {"AllAtOnce", "Manual", "Progressive"}
MAX_DNS_LABEL_LENGTH = 63


def project_name(demo_project: dict[str, Any]) -> str:
    metadata = demo_project.get("metadata", {})
    if isinstance(metadata, dict) and isinstance(metadata.get("name"), str):
        return metadata["name"]
    return "unknown"


def is_control_plane_project(demo_project: dict[str, Any]) -> bool:
    """Control-plane projects opt out of ordinary managed-demo finalization."""
    return (
        metadata_annotations(demo_project).get(FINALIZATION_POLICY_ANNOTATION)
        == FINALIZATION_POLICY_RETAIN
    )


def is_registry_catalog_project(demo_project: dict[str, Any]) -> bool:
    return (
        metadata_annotations(demo_project).get(REGISTRY_CATALOG_ANNOTATION) == "true"
    )


def project_namespace(demo_project: dict[str, Any]) -> str:
    return derive.kubernetes_namespace(project_name(demo_project))


def project_temporal_namespace(demo_project: dict[str, Any]) -> str:
    return derive.temporal_namespace(project_name(demo_project))


def project_host(demo_project: dict[str, Any]) -> str:
    ingress = spec_of(demo_project).get("ingress", {})
    subdomain = ingress.get("subdomain") if isinstance(ingress, dict) else None
    return derive.host(
        subdomain if isinstance(subdomain, str) else None,
        project_name(demo_project),
    )


def project_secret_prefix(demo_project: dict[str, Any]) -> str:
    return derive.secret_prefix(project_name(demo_project))


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


def metadata_labels(demo_project: dict[str, Any]) -> dict[str, str]:
    metadata = demo_project.get("metadata", {})
    if not isinstance(metadata, dict):
        return {}
    labels = metadata.get("labels", {})
    if not isinstance(labels, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in labels.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def is_operator_rollout_canary(demo_project: dict[str, Any]) -> bool:
    labels = metadata_labels(demo_project)
    return (
        labels.get(CANARY_LABEL) == "true"
        and labels.get(CANARY_KIND_LABEL) == OPERATOR_ROLLOUT_CANARY_KIND
    )


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


def source_artifact_digest(demo_project: dict[str, Any]) -> str | None:
    value = metadata_annotations(demo_project).get(SOURCE_ARTIFACT_DIGEST_ANNOTATION)
    return value if value else None


def demo_project_reconcile_key(demo_project: dict[str, Any]) -> str:
    annotations = metadata_annotations(demo_project)
    source_artifact = annotations.get(SOURCE_ARTIFACT_DIGEST_ANNOTATION)
    payload = {
        "generation": demo_project.get("metadata", {}).get("generation")
        if isinstance(demo_project.get("metadata"), dict)
        else None,
        "spec": spec_of(demo_project),
        "imageOverrides": annotations.get(IMAGE_OVERRIDES_ANNOTATION),
        "sourceArtifactDigest": source_artifact,
    }
    if not source_artifact:
        payload["sourceCommitSha"] = annotations.get(SOURCE_COMMIT_ANNOTATION)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def source_repo(demo_project: dict[str, Any]) -> str | None:
    source = spec_of(demo_project).get("source")
    if not isinstance(source, dict):
        return None
    repo = source.get("repo")
    return repo.strip() if isinstance(repo, str) and repo.strip() else None


def source_git_repository_config(demo_project: dict[str, Any]) -> dict[str, Any]:
    source = spec_of(demo_project).get("source")
    if not isinstance(source, dict):
        return {}
    git_repository = source.get("gitRepository")
    return git_repository if isinstance(git_repository, dict) else {}


def source_git_repository_url(demo_project: dict[str, Any]) -> str | None:
    url = source_git_repository_config(demo_project).get("url")
    if isinstance(url, str) and url.strip():
        return url.strip()
    return source_repo(demo_project)


def source_branch(demo_project: dict[str, Any]) -> str:
    source = spec_of(demo_project).get("source")
    if not isinstance(source, dict):
        return "main"
    branch = source.get("branch")
    return branch.strip() if isinstance(branch, str) and branch.strip() else "main"


def source_watch_interval(demo_project: dict[str, Any], default: str) -> str:
    source = spec_of(demo_project).get("source")
    if isinstance(source, dict):
        interval = source.get("interval")
        if isinstance(interval, str) and interval.strip():
            return interval.strip()
    return default


def source_secret_ref(demo_project: dict[str, Any]) -> dict[str, str] | None:
    source = spec_of(demo_project).get("source")
    if not isinstance(source, dict):
        return None

    for key in ("secretRef", "fluxSecretRef"):
        ref = source.get(key)
        if isinstance(ref, dict) and isinstance(ref.get("name"), str):
            name = ref["name"].strip()
            if name:
                return {"name": name}

    secret_name = source.get("secretName")
    if isinstance(secret_name, str) and secret_name.strip():
        return {"name": secret_name.strip()}

    return None


def source_auth_secret_name(demo_project: dict[str, Any]) -> str | None:
    name = source_git_repository_config(demo_project).get("authSecret")
    return name.strip() if isinstance(name, str) and name.strip() else None


def source_auth_kubernetes_secret_name(demo_project: dict[str, Any]) -> str | None:
    if not source_auth_secret_name(demo_project):
        return None
    return suffixed_dns_name(
        source_git_repository_name(demo_project),
        SOURCE_AUTH_SECRET_SUFFIX,
    )


def source_git_repository_secret_ref(demo_project: dict[str, Any]) -> dict[str, str] | None:
    auth_secret_name = source_auth_kubernetes_secret_name(demo_project)
    if auth_secret_name:
        return {"name": auth_secret_name}
    return source_secret_ref(demo_project)


def source_git_repository_sparse_checkout(demo_project: dict[str, Any]) -> list[str]:
    raw = source_git_repository_config(demo_project).get("sparseCheckout")
    if not isinstance(raw, list):
        return []
    paths: list[str] = []
    for value in raw:
        if isinstance(value, str) and value.strip():
            paths.append(value.strip())
    return paths


def source_git_repository_ignore(demo_project: dict[str, Any]) -> str | None:
    raw = source_git_repository_config(demo_project).get("ignore")
    explicit_ignore = raw.strip() if isinstance(raw, str) else ""
    sparse_ignore = sparse_checkout_as_gitignore(
        source_git_repository_sparse_checkout(demo_project)
    )
    if explicit_ignore and sparse_ignore:
        return f"{sparse_ignore}\n{explicit_ignore}"
    return explicit_ignore or sparse_ignore


def sparse_checkout_as_gitignore(paths: list[str]) -> str | None:
    lines: list[str] = []

    def add(line: str) -> None:
        if line not in lines:
            lines.append(line)

    for raw_path in paths:
        parts = [part for part in raw_path.strip("/").split("/") if part and part != "."]
        if not parts:
            continue
        add("/*")
        for depth in range(1, len(parts)):
            parent = "/".join(parts[:depth])
            add(f"!/{parent}/")
            add(f"/{parent}/*")
        path = "/".join(parts)
        add(f"!/{path}/")
        add(f"!/{path}/**")

    return "\n".join(lines) if lines else None


def source_watch_required(demo_project: dict[str, Any]) -> bool:
    source = spec_of(demo_project).get("source")
    if isinstance(source, dict) and source.get("watch") is False:
        return False
    if source_commit_sha(demo_project):
        return False
    if not source_repo(demo_project) or not builds(demo_project):
        return False
    return bool(
        missing_image_overrides(
            demo_project,
            parse_image_overrides(demo_project),
        )
    )


def source_git_repository_name(demo_project: dict[str, Any]) -> str:
    return dns_label(f"{project_name(demo_project)}-source")


def source_watch_resources(
    demo_project: dict[str, Any],
    *,
    namespace: str,
    interval: str,
) -> list[dict[str, Any]]:
    resources = source_auth_external_secret(demo_project, namespace=namespace)
    return [
        *([resources] if resources else []),
        source_git_repository(
            demo_project,
            namespace=namespace,
            interval=interval,
        ),
    ]


def source_git_repository(
    demo_project: dict[str, Any],
    *,
    namespace: str,
    interval: str,
) -> dict[str, Any]:
    url = source_git_repository_url(demo_project)
    if not url:
        raise ValueError("DemoProject spec.source.repo is required for source watch")

    spec: dict[str, Any] = {
        "interval": source_watch_interval(demo_project, interval),
        "url": url,
        "ref": {
            "branch": source_branch(demo_project),
        },
    }
    secret_ref = source_git_repository_secret_ref(demo_project)
    if secret_ref:
        spec["secretRef"] = secret_ref
    ignore = source_git_repository_ignore(demo_project)
    if ignore:
        spec["ignore"] = ignore

    return {
        "apiVersion": FLUX_GIT_REPOSITORY_API_VERSION,
        "kind": "GitRepository",
        "metadata": {
            "name": source_git_repository_name(demo_project),
            "namespace": namespace,
            "labels": {
                **common_labels(demo_project),
                "app.kubernetes.io/component": "source-watch",
            },
        },
        "spec": spec,
    }


def source_auth_external_secret(
    demo_project: dict[str, Any],
    *,
    namespace: str,
) -> dict[str, Any] | None:
    auth_secret_name = source_auth_secret_name(demo_project)
    kubernetes_secret_name = source_auth_kubernetes_secret_name(demo_project)
    if not auth_secret_name or not kubernetes_secret_name:
        return None

    remote_path = user_secret_remote_path(demo_project, auth_secret_name)
    return {
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {
            "name": kubernetes_secret_name,
            "namespace": namespace,
            "labels": {
                **common_labels(demo_project),
                "app.kubernetes.io/component": "source-watch",
            },
            "annotations": {
                "platform.temporal.io/secret-source": remote_path,
            },
        },
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {
                "name": "platform-secrets",
                "kind": "ClusterSecretStore",
            },
            "target": {
                "name": kubernetes_secret_name,
                "creationPolicy": "Owner",
            },
            "dataFrom": [
                {
                    "extract": {
                        "key": remote_path,
                    }
                }
            ],
        },
    }


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
    external_resources: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return [
        *runtime_secret_resources(demo_project),
        *runtime_workload_resources(
            demo_project,
            image_overrides,
            include_routes=include_routes,
            external_resources=external_resources,
        ),
    ]


def runtime_secret_resources(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    if unversioned_temporal_components(demo_project) or temporal_proxy_components(
        demo_project
    ):
        resources.append(external_secret(demo_project))
    if versioned_worker_components(demo_project):
        resources.append(worker_runtime_external_secret(demo_project))
    if is_registry_catalog_project(demo_project):
        resources.append(registry_shared_auth_external_secret(demo_project))
    resources.extend(user_external_secrets(demo_project))
    return resources


def runtime_workload_resources(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
    *,
    include_routes: bool = True,
    external_resources: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    resources.extend(service_resources(demo_project, image_overrides))
    resources.extend(deployment_resources(demo_project, image_overrides))
    resources.extend(
        worker_controller_resources(
            demo_project,
            image_overrides,
            external_resources=external_resources,
        )
    )

    if include_routes:
        resources.extend(
            promotion_resources(
                demo_project,
                image_overrides=image_overrides,
            )
        )
    return resources


def promotion_resources(
    demo_project: dict[str, Any],
    *,
    image_overrides: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    resources = []
    resolved_images = (
        image_overrides
        if image_overrides is not None
        else parse_image_overrides(demo_project)
    )
    if ingress_routes(demo_project):
        resources.append(certificate_resource(demo_project))
        resources.append(https_redirect_middleware(demo_project))
    if auth_required(demo_project):
        resources.append(auth_header_strip_middleware(demo_project))
        resources.append(auth_middleware(demo_project))
    resources.extend(ingress_resources(demo_project, image_overrides=resolved_images))
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
    project_components = components(demo_project)
    for component_name in sorted(project_components):
        component = project_components[component_name]
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
                "automountServiceAccountToken": automount_service_account_token(
                    component
                ),
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


def service_resources(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    namespace = project_namespace(demo_project)
    version = candidate_version(demo_project, image_overrides or {})
    for component_name, component in sorted(components(demo_project).items()):
        ports = component_ports(component)
        if not ports:
            continue

        base_name = effective_service_name(component_name, component)
        versioned_worker = worker_versioning_enabled(component)
        selector = selector_labels(demo_project, component_name)
        if version:
            selector = rollout_selector_labels(demo_project, component_name, version)
        annotations = {
            "platform.temporal.io/component": component_name,
        }
        labels = component_labels(demo_project, component_name)
        if version:
            annotations["platform.temporal.io/rollout-version"] = version
            labels = rollout_labels(demo_project, component_name, version)

        resources.append(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": base_name,
                    "namespace": namespace,
                    "labels": labels,
                    "annotations": annotations,
                },
                "spec": {
                    "type": "ClusterIP",
                    "selector": selector,
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

        if version and not versioned_worker:
            resources.append(
                {
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {
                        "name": candidate_service_name(
                            demo_project,
                            component_name,
                            component,
                            version,
                        ),
                        "namespace": namespace,
                        "labels": labels,
                        "annotations": {
                            **annotations,
                            "platform.temporal.io/stable-service": base_name,
                        },
                    },
                    "spec": {
                        "type": "ClusterIP",
                        "selector": selector,
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
    version = candidate_version(demo_project, image_overrides)
    for component_name, component in sorted(components(demo_project).items()):
        if worker_versioning_enabled(component):
            continue

        build_name = component.get("build")
        if not isinstance(build_name, str) or build_name not in image_overrides:
            continue

        pod_spec = workload_pod_spec(
            demo_project,
            component_name,
            component,
            image_overrides[build_name],
            image_overrides=image_overrides,
        )

        resources.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": candidate_deployment_name(
                        demo_project,
                        component_name,
                        version,
                    ),
                    "namespace": namespace,
                    "labels": rollout_labels(demo_project, component_name, version),
                    "annotations": {
                        "platform.temporal.io/build": build_name,
                        "platform.temporal.io/component": component_name,
                        "platform.temporal.io/rollout-version": version,
                    },
                },
                "spec": {
                    "replicas": component.get("replicas", 1),
                    "selector": {
                        "matchLabels": rollout_selector_labels(
                            demo_project,
                            component_name,
                            version,
                        ),
                    },
                    "template": {
                        "metadata": {
                            "labels": rollout_labels(
                                demo_project,
                                component_name,
                                version,
                            ),
                        },
                        "spec": pod_spec,
                    },
                },
            }
        )
    return resources


def workload_pod_spec(
    demo_project: dict[str, Any],
    component_name: str,
    component: dict[str, Any],
    image: str,
    *,
    image_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    containers = [
        container(
            demo_project,
            component_name,
            component,
            image,
            image_overrides=image_overrides or {},
        )
    ]
    if temporal_proxy_enabled(component):
        containers.append(temporal_proxy_container(demo_project, image_overrides or {}))

    pod_spec: dict[str, Any] = {
        "serviceAccountName": service_account_for_component(component_name),
        "automountServiceAccountToken": automount_service_account_token(component),
        "containers": containers,
    }
    if mounts_catalog_status(demo_project, component_name):
        pod_spec["volumes"] = [catalog_status_volume(), catalog_volume()]
    return pod_spec


def worker_controller_resources(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
    *,
    external_resources: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    workers = versioned_worker_components(demo_project)
    if not workers:
        return []

    resources = [worker_connection_resource(demo_project, external_resources)]
    version = candidate_version(demo_project, image_overrides)
    for component_name, component in sorted(workers.items()):
        build_name = component.get("build")
        if (
            not isinstance(build_name, str)
            or build_name not in image_overrides
            or not version
        ):
            continue
        resources.append(
            worker_deployment_resource(
                demo_project,
                component_name,
                component,
                image_overrides[build_name],
                version=version,
                external_resources=external_resources,
                image_overrides=image_overrides,
            )
        )
    return resources


def worker_connection_resource(
    demo_project: dict[str, Any],
    external_resources: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "apiVersion": TEMPORAL_WORKER_CONTROLLER_API_VERSION,
        "kind": "Connection",
        "metadata": {
            "name": TEMPORAL_CONNECTION_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/secret": TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
                "platform.temporal.io/secret-prefix": project_secret_prefix(demo_project),
            },
        },
        "spec": {
            "hostPort": temporal_connection_host_port(
                demo_project,
                external_resources,
            ),
            "apiKeySecretRef": {
                "name": TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
                "key": STANDARD_TEMPORAL_ENV["api_key"],
            },
        },
    }


def worker_deployment_resource(
    demo_project: dict[str, Any],
    component_name: str,
    component: dict[str, Any],
    image: str,
    *,
    version: str,
    external_resources: dict[str, Any] | None,
    image_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    versioning = worker_versioning(component)
    if versioning is None:
        raise ValueError(f"component {component_name!r} is missing worker.versioning")

    pod_spec = workload_pod_spec(
        demo_project,
        component_name,
        component,
        image,
        image_overrides=image_overrides or {},
    )
    name = worker_deployment_name(component_name, component)
    return {
        "apiVersion": TEMPORAL_WORKER_CONTROLLER_API_VERSION,
        "kind": "WorkerDeployment",
        "metadata": {
            "name": name,
            "namespace": project_namespace(demo_project),
            "labels": component_labels(demo_project, component_name),
            "annotations": {
                "platform.temporal.io/build": str(component.get("build")),
                "platform.temporal.io/component": component_name,
                "platform.temporal.io/rollout-version": version,
            },
        },
        "spec": {
            "replicas": component.get("replicas", 1),
            "workerOptions": {
                "connectionRef": {"name": TEMPORAL_CONNECTION_NAME},
                "temporalNamespace": temporal_worker_namespace(
                    demo_project,
                    external_resources,
                ),
            },
            "rollout": worker_rollout(versioning),
            "sunset": worker_sunset(versioning),
            "template": {
                "metadata": {
                    "labels": {
                        **rollout_labels(demo_project, component_name, version),
                        "temporal.io/worker-deployment": name,
                    },
                },
                "spec": pod_spec,
            },
        },
    }


def temporal_connection_host_port(
    demo_project: dict[str, Any],
    external_resources: dict[str, Any] | None,
) -> str:
    namespace_status = temporal_cloud_namespace_status(external_resources)
    if isinstance(namespace_status.get("grpcAddress"), str):
        return namespace_status["grpcAddress"]

    namespace_name = namespace_status.get("name")
    if isinstance(namespace_name, str) and namespace_name:
        return f"{namespace_name}.tmprl.cloud:7233"

    return f"{project_temporal_namespace(demo_project)}.tmprl.cloud:7233"


def temporal_worker_namespace(
    demo_project: dict[str, Any],
    external_resources: dict[str, Any] | None,
) -> str:
    namespace_name = temporal_cloud_namespace_status(external_resources).get("name")
    return (
        namespace_name
        if isinstance(namespace_name, str) and namespace_name
        else project_temporal_namespace(demo_project)
    )


def temporal_cloud_namespace_status(
    external_resources: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(external_resources, dict):
        return {}
    temporal_cloud = external_resources.get("temporalCloud")
    if not isinstance(temporal_cloud, dict):
        return {}
    namespace = temporal_cloud.get("namespace")
    return namespace if isinstance(namespace, dict) else {}


def worker_rollout(versioning: dict[str, Any]) -> dict[str, Any]:
    configured = versioning.get("rollout")
    configured = configured if isinstance(configured, dict) else {}
    strategy = configured.get("strategy", TEMPORAL_WORKER_DEFAULT_ROLLOUT_STRATEGY)
    if strategy not in TEMPORAL_WORKER_ROLLOUT_STRATEGIES:
        raise ValueError(f"Unsupported worker rollout strategy {strategy!r}")

    rollout: dict[str, Any] = {"strategy": strategy}
    gate = configured.get("gate")
    if isinstance(gate, dict):
        rollout["gate"] = worker_rollout_gate(gate)
    steps = configured.get("steps")
    if isinstance(steps, list):
        rollout["steps"] = [step for step in steps if isinstance(step, dict)]
    if strategy == "Progressive" and not rollout.get("steps"):
        raise ValueError("Progressive worker rollout requires at least one step")
    return rollout


def worker_rollout_gate(gate: dict[str, Any]) -> dict[str, Any]:
    workflow_type = gate.get("workflowType")
    if not isinstance(workflow_type, str) or not workflow_type:
        raise ValueError("worker rollout gate requires workflowType")

    rendered: dict[str, Any] = {"workflowType": workflow_type}
    input_from = gate.get("inputFrom")
    if "input" in gate and isinstance(input_from, dict):
        raise ValueError("worker rollout gate supports only one of input or inputFrom")
    if "input" in gate:
        rendered["input"] = gate["input"]
    if isinstance(input_from, dict):
        rendered["inputFrom"] = {
            key: value
            for key, value in input_from.items()
            if key in {"configMapKeyRef", "secretKeyRef"} and isinstance(value, dict)
        }
        if len(rendered["inputFrom"]) != 1:
            raise ValueError(
                "worker rollout gate inputFrom requires exactly one source"
            )
    return rendered


def worker_sunset(versioning: dict[str, Any]) -> dict[str, str]:
    sunset = versioning.get("sunset")
    sunset = sunset if isinstance(sunset, dict) else {}
    scaledown_delay = sunset.get(
        "scaledownDelay",
        TEMPORAL_WORKER_DEFAULT_SCALEDOWN_DELAY,
    )
    delete_delay = sunset.get("deleteDelay", TEMPORAL_WORKER_DEFAULT_DELETE_DELAY)
    return {
        "scaledownDelay": str(scaledown_delay),
        "deleteDelay": str(delete_delay),
    }


def worker_deployment_rollout_wait_seconds(
    demo_project: dict[str, Any],
    *,
    base_timeout_seconds: int,
) -> int:
    base = max(1, int(base_timeout_seconds))
    wait_seconds = base
    for component in versioned_worker_components(demo_project).values():
        wait_seconds = max(
            wait_seconds,
            base + worker_component_rollout_seconds(component),
        )
    return wait_seconds


def worker_component_rollout_seconds(component: dict[str, Any]) -> int:
    versioning = worker_versioning(component)
    rollout = versioning.get("rollout") if isinstance(versioning, dict) else None
    rollout = rollout if isinstance(rollout, dict) else {}

    total = 0
    gate = rollout.get("gate")
    gate = gate if isinstance(gate, dict) else {}
    gate_input = gate.get("input")
    if isinstance(gate_input, dict):
        total += positive_int_seconds(gate_input.get("timeoutSeconds"))
        total += positive_int_seconds(gate_input.get("cleanupTimeoutSeconds"))

    steps = rollout.get("steps")
    if isinstance(steps, list):
        for step in steps:
            if isinstance(step, dict):
                total += go_duration_seconds(step.get("pauseDuration"))
    return total


def positive_int_seconds(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0


def go_duration_seconds(value: Any) -> int:
    if not isinstance(value, str):
        return 0
    for suffix, multiplier in (
        ("ns", 0.000000001),
        ("us", 0.000001),
        ("ms", 0.001),
        ("s", 1),
        ("m", 60),
        ("h", 3600),
    ):
        if value.endswith(suffix):
            amount = value[: -len(suffix)]
            if amount.isdigit():
                numeric = int(amount)
                if numeric <= 0:
                    return 0
                return max(1, math.ceil(numeric * multiplier))
    return 0


def container(
    demo_project: dict[str, Any],
    component_name: str,
    component: dict[str, Any],
    image: str,
    *,
    image_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    rendered: dict[str, Any] = {
        "name": component_name,
        "image": image,
        "imagePullPolicy": "IfNotPresent",
    }

    rendered["command"] = component["command"] if "command" in component else None
    rendered["args"] = component["args"] if "args" in component else None

    ports = component_ports(component)
    if ports:
        rendered["ports"] = [
            {
                "name": port["name"],
                "containerPort": port["containerPort"],
            }
            for port in ports
        ]
        # Gate readiness on the port actually accepting connections, so a
        # candidate is only "ready" once it is serving (not merely started).
        # This closes the window where an app that builds/initializes on boot
        # would pass the readiness gate and fail the smoke check.
        rendered["readinessProbe"] = {
            "tcpSocket": {"port": ports[0]["containerPort"]},
            "initialDelaySeconds": 5,
            "periodSeconds": 5,
            "failureThreshold": 30,
        }

    env_from = env_from_refs(demo_project, component_name, component)
    if env_from:
        rendered["envFrom"] = env_from

    env = env_vars(component)
    env.extend(
        platform_env_vars(demo_project, component_name, image, image_overrides or {})
    )
    if temporal_proxy_enabled(component):
        env = env_with_overrides(env, temporal_proxy_app_env(component))
    if env:
        rendered["env"] = env
    if mounts_catalog_status(demo_project, component_name):
        rendered.setdefault("env", []).append(
            {
                "name": "CATALOG_STATUS_DIR",
                "value": CATALOG_STATUS_MOUNT_PATH,
            }
        )
        rendered["volumeMounts"] = [
            {
                "name": CATALOG_STATUS_VOLUME_NAME,
                "mountPath": CATALOG_STATUS_MOUNT_PATH,
                "readOnly": True,
            },
            {
                "name": CATALOG_VOLUME_NAME,
                "mountPath": CATALOG_MOUNT_PATH,
                "readOnly": True,
            },
        ]

    return rendered


def temporal_proxy_container(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> dict[str, Any]:
    return {
        "name": TEMPORAL_PROXY_CONTAINER_NAME,
        "image": temporal_proxy_image(demo_project, image_overrides),
        "imagePullPolicy": "IfNotPresent",
        "envFrom": [{"secretRef": {"name": TEMPORAL_SECRET_NAME}}],
        "env": [
            {
                "name": "TEMPORAL_PROXY_LISTEN_ADDRESS",
                "value": TEMPORAL_PROXY_LOCAL_ADDRESS,
            },
            {
                "name": "TEMPORAL_PROXY_NAMESPACE_ALIASES",
                "value": temporal_proxy_namespace_aliases(demo_project),
            },
        ],
        "resources": {
            "requests": {"cpu": "25m", "memory": "32Mi"},
            "limits": {"cpu": "100m", "memory": "128Mi"},
        },
    }


def temporal_proxy_image(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> str:
    image = image_overrides.get(TEMPORAL_PROXY_BUILD_NAME)
    if is_registry_catalog_project(demo_project) and image:
        return image

    configured = os.getenv(TEMPORAL_PROXY_IMAGE_ENV, "").strip()
    if configured:
        return configured
    raise ValueError(
        f"{TEMPORAL_PROXY_IMAGE_ENV} is required when temporalProxy is enabled"
    )


def platform_env_vars(
    demo_project: dict[str, Any],
    component_name: str,
    image: str,
    image_overrides: dict[str, str],
) -> list[dict[str, str]]:
    env: list[dict[str, str]] = []
    if is_registry_catalog_project(demo_project) and component_name == "registry-operator":
        env.append({"name": "DEMO_REGISTRY_OPERATOR_IMAGE", "value": image})
        proxy_image = image_overrides.get(TEMPORAL_PROXY_BUILD_NAME)
        if proxy_image:
            env.append({"name": TEMPORAL_PROXY_IMAGE_ENV, "value": proxy_image})
    return env


def temporal_proxy_app_env(component: dict[str, Any]) -> list[dict[str, str]]:
    names = temporal_proxy_address_env_names(component)
    env = [{"name": name, "value": TEMPORAL_PROXY_LOCAL_ADDRESS} for name in names]
    env.extend(
        [
            {"name": "TEMPORAL_NAMESPACE", "value": "default"},
            {"name": "TEMPORAL_TLS", "value": "false"},
        ]
    )
    return env


def temporal_proxy_address_env_names(component: dict[str, Any]) -> list[str]:
    proxy = temporal_proxy_config(component)
    configured = proxy.get("addressEnv") if isinstance(proxy, dict) else None
    names = (
        [item for item in configured if isinstance(item, str)]
        if isinstance(configured, list)
        else list(TEMPORAL_PROXY_DEFAULT_ADDRESS_ENVS)
    )
    deduped: list[str] = []
    for name in names:
        if name not in deduped:
            deduped.append(name)
    return deduped


def temporal_proxy_namespace_aliases(demo_project: dict[str, Any]) -> str:
    # The first compatibility target is local dev samples that use the SDK's
    # default namespace. The platform namespace is always injected separately.
    return "default"


def env_with_overrides(
    existing: list[dict[str, str]],
    overrides: list[dict[str, str]],
) -> list[dict[str, str]]:
    override_names = {item["name"] for item in overrides}
    return [
        item for item in existing if item.get("name") not in override_names
    ] + overrides


def mounts_catalog_status(demo_project: dict[str, Any], component_name: str) -> bool:
    return is_registry_catalog_project(demo_project) and (
        component_name == REGISTRY_WEB_COMPONENT_NAME
    )


def catalog_status_volume() -> dict[str, Any]:
    return {
        "name": CATALOG_STATUS_VOLUME_NAME,
        "configMap": {
            "name": CATALOG_STATUS_CONFIGMAP,
            "optional": True,
        },
    }


def catalog_volume() -> dict[str, Any]:
    return {
        "name": CATALOG_VOLUME_NAME,
        "configMap": {
            "name": CATALOG_CONFIGMAP,
            "optional": True,
        },
    }


def env_from_refs(
    demo_project: dict[str, Any],
    component_name: str,
    component: dict[str, Any],
) -> list[dict[str, Any]]:
    refs = []
    if temporal_proxy_enabled(component):
        pass
    elif worker_versioning_enabled(component):
        refs.append({"secretRef": {"name": TEMPORAL_WORKER_RUNTIME_SECRET_NAME}})
    elif component_needs_temporal(component):
        refs.append({"secretRef": {"name": TEMPORAL_SECRET_NAME}})
    if is_registry_catalog_project(demo_project) and (
        component_name == REGISTRY_WEB_COMPONENT_NAME
    ):
        refs.append({"secretRef": {"name": REGISTRY_SHARED_AUTH_SECRET_NAME}})

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
    return [
        {"name": item["name"], "value": str(item["value"])}
        for item in component.get("env", [])
        if isinstance(item, dict)
    ]


def ingress_resources(
    demo_project: dict[str, Any],
    *,
    image_overrides: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    route_specs = ingress_routes(demo_project)
    if not route_specs:
        return []

    version = candidate_version(demo_project, image_overrides or {})
    routes = [
        ingress_route(
            demo_project,
            route_spec,
            image_overrides=image_overrides or {},
        )
        for route_spec in route_specs
    ]
    if registry_auth_bypass_required(demo_project):
        routes = [
            *registry_auth_bypass_routes(
                demo_project,
                route_specs,
                image_overrides=image_overrides or {},
            ),
            *routes,
        ]
    annotations = {
        "platform.temporal.io/host": project_host(demo_project),
        "platform.temporal.io/temporal-auth-required": str(
            auth_required(demo_project)
        ).lower(),
    }
    if version:
        annotations["platform.temporal.io/rollout-version"] = version
    base = {
        "apiVersion": TRAEFIK_API_VERSION,
        "kind": "IngressRoute",
        "metadata": {
            "name": project_name(demo_project),
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": annotations,
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
            "routes": http_redirect_routes(routes, demo_project),
        },
    }
    http["spec"].pop("tls", None)
    return [base, http]


def http_redirect_routes(
    routes: list[dict[str, Any]],
    demo_project: dict[str, Any],
) -> list[dict[str, Any]]:
    rendered = []
    for route in routes:
        http_route = {**route}
        http_route["middlewares"] = [
            {
                "name": HTTPS_REDIRECT_MIDDLEWARE_NAME,
                "namespace": project_namespace(demo_project),
            }
        ]
        rendered.append(http_route)
    return rendered


def registry_auth_bypass_required(demo_project: dict[str, Any]) -> bool:
    return is_registry_catalog_project(demo_project) and auth_required(demo_project)


def registry_auth_bypass_routes(
    demo_project: dict[str, Any],
    route_specs: list[dict[str, Any]],
    *,
    image_overrides: dict[str, str],
) -> list[dict[str, Any]]:
    routes = []
    for route_spec in route_specs:
        for path_prefix in REGISTRY_AUTH_PUBLIC_PATH_PREFIXES:
            routes.append(
                ingress_route(
                    demo_project,
                    {
                        **route_spec,
                        "pathPrefix": path_prefix,
                    },
                    image_overrides=image_overrides,
                    use_auth=False,
                    priority=2000,
                )
            )
    return routes


def certificate_resource(demo_project: dict[str, Any]) -> dict[str, Any]:
    name = project_name(demo_project)
    return {
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": f"{name}-tls",
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
        },
        "spec": {
            "dnsNames": [project_host(demo_project)],
            "issuerRef": {
                "group": "cert-manager.io",
                "kind": "ClusterIssuer",
                "name": "letsencrypt",
            },
            "privateKey": {
                "rotationPolicy": "Always",
            },
            "secretName": f"{name}-tls",
        },
    }


def ingress_route(
    demo_project: dict[str, Any],
    route_spec: dict[str, Any],
    *,
    image_overrides: dict[str, str] | None = None,
    use_auth: bool | None = None,
    priority: int = 1000,
) -> dict[str, Any]:
    match = f"Host(`{project_host(demo_project)}`)"
    path_prefix = route_spec.get("pathPrefix")
    if path_prefix:
        match = f"{match} && PathPrefix(`{path_prefix}`)"

    component_name = route_spec["component"]
    component = components(demo_project)[component_name]
    version = candidate_version(demo_project, image_overrides or {})
    service_name = effective_service_name(component_name, component)
    if version and not worker_versioning_enabled(component):
        service_name = candidate_service_name(
            demo_project,
            component_name,
            component,
            version,
        )
    route: dict[str, Any] = {
        "kind": "Rule",
        "match": match,
        "priority": priority,
        "services": [
            {
                "name": service_name,
                "namespace": project_namespace(demo_project),
                "passHostHeader": pass_host_header(route_spec),
                "port": service_port(component, route_spec["port"]),
            }
        ],
    }
    if use_auth is None:
        use_auth = auth_required(demo_project)
    if use_auth:
        route["middlewares"] = [
            {
                "name": AUTH_HEADER_STRIP_MIDDLEWARE_NAME,
                "namespace": project_namespace(demo_project),
            },
            {
                "name": AUTH_MIDDLEWARE_NAME,
                "namespace": project_namespace(demo_project),
            }
        ]
    return route


def pass_host_header(route_spec: dict[str, Any]) -> bool:
    value = route_spec.get("passHostHeader")
    return value if isinstance(value, bool) else True


def auth_middleware(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": TRAEFIK_API_VERSION,
        "kind": "Middleware",
        "metadata": {
            "name": AUTH_MIDDLEWARE_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/description": "Temporal employee catalog auth forward-auth middleware.",
            },
        },
        "spec": {
            "forwardAuth": {
                "address": CATALOG_AUTH_VERIFY_URL,
                "authRequestHeaders": CATALOG_AUTH_REQUEST_HEADERS,
                "authResponseHeaders": [
                    "X-Temporal-Auth-JWT",
                    "X-Temporal-Auth-Subject",
                    "X-Temporal-Auth-Email",
                    "X-Temporal-Auth-Name",
                ],
            }
        },
    }


def https_redirect_middleware(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": TRAEFIK_API_VERSION,
        "kind": "Middleware",
        "metadata": {
            "name": HTTPS_REDIRECT_MIDDLEWARE_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/description": "Redirect all HTTP demo traffic to HTTPS.",
            },
        },
        "spec": {
            "redirectScheme": {
                "scheme": "https",
                "permanent": True,
            }
        },
    }


def auth_header_strip_middleware(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": TRAEFIK_API_VERSION,
        "kind": "Middleware",
        "metadata": {
            "name": AUTH_HEADER_STRIP_MIDDLEWARE_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/description": "Remove untrusted client-supplied Temporal auth headers before forward-auth runs.",
            },
        },
        "spec": {
            "headers": {
                "customRequestHeaders": {
                    "X-Temporal-Auth-JWT": "",
                    "X-Temporal-Auth-Subject": "",
                    "X-Temporal-Auth-Email": "",
                    "X-Temporal-Auth-Name": "",
                }
            }
        },
    }


def external_secret(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {
            "name": TEMPORAL_SECRET_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/secret-prefix": project_secret_prefix(demo_project),
            },
        },
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {
                "name": "platform-secrets",
                "kind": "ClusterSecretStore",
            },
            "target": {
                "name": TEMPORAL_SECRET_NAME,
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


def worker_runtime_external_secret(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {
            "name": TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/secret-prefix": project_secret_prefix(demo_project),
            },
        },
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {
                "name": "platform-secrets",
                "kind": "ClusterSecretStore",
            },
            "target": {
                "name": TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
                "creationPolicy": "Owner",
            },
            "data": [
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


def registry_shared_auth_external_secret(demo_project: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {
            "name": REGISTRY_SHARED_AUTH_SECRET_NAME,
            "namespace": project_namespace(demo_project),
            "labels": common_labels(demo_project),
            "annotations": {
                "platform.temporal.io/secret-source": REGISTRY_SHARED_AUTH_SECRET_PATH,
            },
        },
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {
                "name": "platform-secrets",
                "kind": "ClusterSecretStore",
            },
            "target": {
                "name": REGISTRY_SHARED_AUTH_SECRET_NAME,
                "creationPolicy": "Owner",
            },
            "dataFrom": [
                {
                    "extract": {
                        "key": REGISTRY_SHARED_AUTH_SECRET_PATH,
                    }
                }
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
                        "name": "platform-secrets",
                        "kind": "ClusterSecretStore",
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
    external_resources: dict[str, Any] | None = None,
    build_status: dict[str, Any] | None = None,
    external_secret_status: dict[str, Any] | None = None,
    worker_deployments_status: dict[str, Any] | None = None,
    external_resource_block: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    metadata = demo_project.get("metadata", {})
    generation = metadata.get("generation", 0) if isinstance(metadata, dict) else 0
    build_failed = bool(build_status and build_status.get("phase") == "Error")
    candidate_status: dict[str, Any] = {
        "phase": "WaitingForImages" if missing_images else "CandidateApplied",
        "images": image_overrides,
        "message": (
            f"Missing image(s): {', '.join(missing_images)}"
            if missing_images
            else "Candidate resources are versioned and ready for smoke testing."
        ),
    }
    commit_sha = source_commit_sha(demo_project)
    if commit_sha:
        candidate_status["sourceCommitSha"] = commit_sha
    artifact_digest = source_artifact_digest(demo_project)
    if artifact_digest:
        candidate_status["sourceArtifactDigest"] = artifact_digest
    if build_failed:
        candidate_status["phase"] = "BuildFailed"
        candidate_status["message"] = build_failure_message(build_status)
    if external_resource_block and not missing_images and not build_failed:
        candidate_status["phase"] = "WaitingForExternalResources"
        candidate_status["message"] = external_resource_block["message"]
    candidate_url = smoke_check_internal_url(
        demo_project,
        image_overrides=image_overrides,
    )
    if candidate_url and not missing_images and not build_failed:
        candidate_status["url"] = candidate_url
    if image_overrides and not missing_images and not build_failed:
        version = candidate_version(demo_project, image_overrides)
        candidate_status["imageVersion"] = version
        candidate_status["services"] = candidate_service_status(
            demo_project,
            image_overrides,
        )
        candidate_status["deployments"] = candidate_deployment_status(
            demo_project,
            image_overrides,
        )
        candidate_status["workerDeployments"] = worker_deployment_status(
            demo_project,
            image_overrides,
        )
    images_ready_reason = (
        "BuildFailed"
        if build_failed
        else "MissingImages"
        if missing_images
        else "ImagesResolved"
    )
    images_ready_message = (
        build_failure_message(build_status)
        if build_failed
        else f"Missing image(s): {', '.join(missing_images)}"
        if missing_images
        else "Every build has an immutable image."
    )

    status: dict[str, Any] = {
        "observedGeneration": generation if isinstance(generation, int) else 0,
        RECONCILE_KEY_STATUS_FIELD: demo_project_reconcile_key(demo_project),
        "phase": phase,
        "message": message,
        "externalResources": external_resources or external_resource_status(demo_project),
        "buildStatus": build_status or fallback_build_status(
            demo_project,
            image_overrides,
            missing_images,
        ),
        "kubernetesNamespaceStatus": kubernetes_namespace_status(
            demo_project,
            applied_resources,
            mode=mode,
            error=error,
        ),
        "candidateRolloutStatus": candidate_status,
        "smokeCheckStatus": smoke_check_status(demo_project, phase),
        "conditions": [
            {
                "type": "Reconciled",
                "status": "False" if error or phase == "Error" else "True",
                "observedGeneration": generation if isinstance(generation, int) else 0,
                "reason": "Error" if error else phase,
                "message": error or message,
                "lastTransitionTime": now,
            },
            {
                "type": "ImagesReady",
                "status": "False" if missing_images or build_failed else "True",
                "observedGeneration": generation if isinstance(generation, int) else 0,
                "reason": images_ready_reason,
                "message": images_ready_message,
                "lastTransitionTime": now,
            },
            {
                "type": "ExternalResourcesReady",
                "status": "False" if external_resource_block else "True",
                "observedGeneration": generation if isinstance(generation, int) else 0,
                "reason": (
                    external_resource_block.get("reason")
                    if external_resource_block
                    else "ExternalResourcesReady"
                ),
                "message": (
                    external_resource_block.get("message")
                    if external_resource_block
                    else "Required external resources are ready or not required."
                ),
                "lastTransitionTime": now,
            },
        ],
        "appliedResources": applied_resources,
        "reconcileMode": mode,
    }
    if external_secret_status:
        status["externalSecretStatus"] = external_secret_status
    if worker_deployments_status:
        status["workerDeploymentStatus"] = worker_deployments_status
        status["conditions"].append(
            worker_deployments_condition(
                worker_deployments_status,
                generation if isinstance(generation, int) else 0,
                now,
            )
        )
    if external_resource_block:
        status["externalResourceBlock"] = external_resource_block

    if image_overrides and not missing_images and phase in {"Promoted", "Ready"}:
        version = candidate_version(demo_project, image_overrides)
        status["activeImageVersion"] = version
        status["activeImages"] = image_overrides
        status["activeRouteServices"] = candidate_service_status(
            demo_project,
            image_overrides,
        )

    if error:
        status["lastError"] = {
            "reason": "ReconcileFailed",
            "message": error,
            "lastTransitionTime": now,
        }

    return status


def build_failure_message(build_status: dict[str, Any] | None) -> str:
    if not isinstance(build_status, dict):
        return "Image build failed."
    last_error = build_status.get("lastError")
    if isinstance(last_error, dict) and isinstance(last_error.get("message"), str):
        return last_error["message"]
    message = build_status.get("message")
    return message if isinstance(message, str) and message else "Image build failed."


def worker_deployments_condition(
    status: dict[str, Any],
    generation: int,
    now: str,
) -> dict[str, Any]:
    phase = status.get("phase")
    condition_status = "Unknown"
    if phase in {"Ready", "NotRequired", "Planned", "Skipped"}:
        condition_status = "True"
    elif phase in {"Failed", "TimedOut", "Error"}:
        condition_status = "False"

    return {
        "type": "WorkerDeploymentsReady",
        "status": condition_status,
        "observedGeneration": generation,
        "reason": str(phase or "Unknown"),
        "message": str(
            status.get("message")
            or "WorkerDeployment rollout status has been mirrored."
        ),
        "lastTransitionTime": now,
    }


def kubernetes_namespace_status(
    demo_project: dict[str, Any],
    applied_resources: list[dict[str, Any]],
    *,
    mode: str,
    error: str | None = None,
) -> dict[str, Any]:
    namespace = project_namespace(demo_project)
    namespace_summary = next(
        (
            resource
            for resource in applied_resources
            if resource.get("kind") == "Namespace"
            and resource.get("name") == namespace
        ),
        None,
    )
    action = (
        namespace_summary.get("action")
        if isinstance(namespace_summary, dict)
        else None
    )
    status: dict[str, Any] = {
        "name": namespace,
        "mode": mode,
    }

    if error:
        return {
            **status,
            "phase": "Error",
            "message": "Namespace reconcile did not complete.",
            "error": error,
        }

    if mode == "plan":
        return {
            **status,
            "phase": "Planned",
            "action": action or "planned",
            "message": "Namespace would be created or patched.",
        }

    if action == "created":
        return {
            **status,
            "phase": "Ready",
            "action": action,
            "message": "Namespace was created.",
        }

    if action == "patched":
        return {
            **status,
            "phase": "Ready",
            "action": action,
            "message": "Namespace was patched.",
        }

    return {
        **status,
        "phase": "Unknown",
        "message": "Namespace was not present in the applied resource summary.",
    }


def fallback_build_status(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
    missing_images: list[str],
) -> dict[str, Any]:
    commit_sha = source_commit_sha(demo_project)
    phase = "Resolved" if image_overrides and not missing_images else "WaitingForImages"
    status: dict[str, Any] = {
        "phase": phase,
        "mode": "image-overrides",
        "images": image_overrides,
        "missingImages": missing_images,
        "message": (
            "Immutable image overrides are present for every build."
            if phase == "Resolved"
            else "Waiting for immutable image overrides or build output."
        ),
    }
    if commit_sha:
        status["sourceCommitSha"] = commit_sha
        status["imageVersion"] = f"git-{commit_sha[:12]}"
    artifact_digest = source_artifact_digest(demo_project)
    if artifact_digest:
        status["sourceArtifactDigest"] = artifact_digest
    return status


def external_resource_status(demo_project: dict[str, Any]) -> dict[str, Any]:
    prefix = project_secret_prefix(demo_project)
    build_names = sorted(builds(demo_project))
    temporal_required = bool(temporal_components(demo_project))
    return {
        "phase": "ManualSetupRequired",
        "kubernetesNamespace": project_namespace(demo_project),
        "ecr": {
            "phase": "ManualSetupRequired",
            "repositories": [
                f"tmprl-dem-cld/{project_name(demo_project)}/{build_name}"
                for build_name in build_names
            ],
        },
        "secretsManager": {
            "phase": "ManualSetupRequired",
            "secretPrefix": prefix,
            "temporalBootstrap": {
                "path": TEMPORAL_BOOTSTRAP_SECRET_PATH,
                "required": temporal_required,
                "managedBy": "platform",
                "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
                "phase": "ManualSetupRequired" if temporal_required else "NotRequired",
            },
            "temporalCredentials": {
                "path": temporal_secret_remote_path(demo_project),
                "required": temporal_required,
                "managedBy": "platform",
                "requiredProperties": TEMPORAL_SECRET_PROPERTIES,
                "phase": "ManualSetupRequired" if temporal_required else "NotRequired",
            },
            "userManaged": [
                {
                    "path": user_secret_remote_path(demo_project, name),
                    "required": True,
                    "managedBy": "project-owner",
                    "requiredProperties": user_secret_required_properties(
                        demo_project,
                        name,
                    ),
                    "components": sorted(
                        components_for_user_secret(demo_project, name)
                    ),
                    "phase": "ManualSetupRequired",
                }
                for name in user_secret_names(demo_project)
            ],
        },
        "temporalCloud": {
            "phase": "ManualSetupRequired" if temporal_required else "NotRequired",
            "provider": "temporal-cloud",
            "mode": "cloud-api",
            "message": (
                "The operator manages Temporal Cloud resources through the "
                "Cloud Operations API when external resource reconciliation runs."
                if temporal_required
                else "No component currently requires Temporal credentials."
            ),
            "namespace": {
                "name": project_temporal_namespace(demo_project),
                "action": "create-or-verify",
                "deletionPolicy": "retain-until-explicitly-approved",
            },
            "apiKey": {
                "action": "create-or-verify-or-rotate",
                "scope": "namespace",
                "namespace": project_temporal_namespace(demo_project),
                "ttlDays": 30,
                "rotateBeforeExpiryDays": 10,
                "secretRemotePath": temporal_secret_remote_path(demo_project),
                "secretProperties": TEMPORAL_SECRET_PROPERTIES,
                "components": sorted(temporal_components(demo_project)),
            },
            "bootstrapCredential": {
                "secretRemotePath": TEMPORAL_BOOTSTRAP_SECRET_PATH,
                "kubernetesSecret": "temporal-cloud-bootstrap",
                "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
            },
        },
    }


def catalog_status_configmap(
    demo_project: dict[str, Any],
    status: dict[str, Any],
    *,
    namespace: str,
    name: str,
) -> dict[str, Any]:
    project = project_name(demo_project)
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "demo-registry",
                "app.kubernetes.io/component": "catalog-status",
                "platform.temporal.io/managed-by": "registry-operator",
            },
            "annotations": {
                "platform.temporal.io/catalog-status": "true",
            },
        },
        "data": {
            f"{project}.json": json.dumps(
                catalog_status_entry(demo_project, status),
                sort_keys=True,
            ),
        },
    }


def humanized_title(name: str) -> str:
    return name.replace("-", " ").replace("_", " ").title()


def catalog_entry(demo_project: dict[str, Any]) -> dict[str, Any]:
    """One catalog card built from a DemoProject CR. The ``registry`` block is
    optional metadata; when omitted, the title falls back to a humanized name so a
    thin config still lists in the catalog. Live status is overlaid by the web app
    from the catalog-status ConfigMap referenced here."""
    spec = spec_of(demo_project)
    registry = spec.get("registry") if isinstance(spec.get("registry"), dict) else {}
    source = spec.get("source") if isinstance(spec.get("source"), dict) else {}
    name = project_name(demo_project)
    host = project_host(demo_project)
    return {
        "name": name,
        "title": registry.get("title") or humanized_title(name),
        "summary": registry.get("summary", ""),
        "host": host,
        "appUrl": f"https://{host}",
        "sourceRepo": source.get("repo", ""),
        "temporalNamespace": project_temporal_namespace(demo_project),
        "authRequired": auth_required(demo_project),
        "catalogStatus": {
            "source": "DemoProject.status",
            "demoProject": name,
            "configMap": {
                "namespace": CATALOG_STATUS_NAMESPACE,
                "name": CATALOG_STATUS_CONFIGMAP,
                "key": f"{name}.json",
            },
            "fields": list(CATALOG_STATUS_FIELDS),
        },
        "tags": registry.get("tags", []),
        "temporalSdks": registry.get("temporalSdks", []),
        "links": registry.get("links", {}),
        "assets": registry.get("assets", {}),
    }


def catalog_payload(demo_projects: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "baseDomain": derive.BASE_DOMAIN,
        "projects": [
            catalog_entry(demo_project)
            for demo_project in sorted(demo_projects, key=project_name)
        ],
    }


def catalog_configmap(
    demo_projects: list[dict[str, Any]],
    *,
    namespace: str,
    name: str = CATALOG_CONFIGMAP,
) -> dict[str, Any]:
    """ConfigMap the registry web app mounts at /app/data/registry.json. The
    operator rebuilds it from the full set of DemoProject CRs (see watch loop)."""
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "demo-registry",
                "app.kubernetes.io/component": "catalog",
                "platform.temporal.io/managed-by": "registry-operator",
            },
            "annotations": {
                "platform.temporal.io/catalog": "true",
            },
        },
        "data": {
            "registry.json": json.dumps(
                catalog_payload(demo_projects), indent=2, sort_keys=True
            ),
        },
    }


def catalog_status_entry(
    demo_project: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    return {
        "apiVersion": "tmprl-demo.cloud/v1alpha1",
        "kind": "DemoProjectCatalogStatus",
        "project": project_name(demo_project),
        "updatedAt": datetime.now(UTC).isoformat(),
        "phase": status.get("phase"),
        "message": status.get("message"),
        "host": project_host(demo_project),
        "appUrl": f"https://{project_host(demo_project)}",
        "kubernetesNamespace": project_namespace(demo_project),
        "kubernetesNamespaceStatus": status.get("kubernetesNamespaceStatus"),
        "temporalNamespace": temporal_namespace_for_status(demo_project, status),
        "activeImageVersion": status.get("activeImageVersion"),
        "activeImages": status.get("activeImages", {}),
        "activeRouteServices": status.get("activeRouteServices", {}),
        "externalResources": external_resource_summary(
            status.get("externalResources")
        ),
        "externalResourceBlock": status.get("externalResourceBlock"),
        "externalSecrets": status.get("externalSecretStatus"),
        "build": build_summary(status.get("buildStatus")),
        "candidate": candidate_summary(status.get("candidateRolloutStatus")),
        "readiness": status.get("readinessStatus"),
        "workerDeployments": worker_deployments_summary(
            status.get("workerDeploymentStatus")
        ),
        "smokeCheck": status.get("smokeCheckStatus"),
        "prune": status.get("pruneStatus"),
        "prunedResources": status.get("prunedResources", []),
        "finalizeStatus": status.get("finalizeStatus"),
        "lastError": status.get("lastError"),
    }


def temporal_namespace_for_status(
    demo_project: dict[str, Any],
    status: dict[str, Any],
) -> str:
    external_resources = status.get("externalResources")
    if isinstance(external_resources, dict):
        temporal_cloud = external_resources.get("temporalCloud")
        if isinstance(temporal_cloud, dict):
            namespace = temporal_cloud.get("namespace")
            if isinstance(namespace, dict) and isinstance(namespace.get("name"), str):
                return namespace["name"]
    return project_name(demo_project)


def external_resource_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        "phase": value.get("phase"),
        "mode": value.get("mode"),
        "checkedAt": value.get("checkedAt"),
        "ecr": phase_summary(value.get("ecr")),
        "secretsManager": phase_summary(value.get("secretsManager")),
        "temporalCloud": phase_summary(value.get("temporalCloud")),
    }


def build_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        "phase": value.get("phase"),
        "mode": value.get("mode"),
        "imageVersion": value.get("imageVersion"),
        "sourceArtifactDigest": value.get("sourceArtifactDigest"),
        "images": value.get("images", {}),
        "missingImages": value.get("missingImages", []),
        "codebuild": value.get("codebuild"),
        "lastError": value.get("lastError"),
    }


def candidate_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        "phase": value.get("phase"),
        "message": value.get("message"),
        "sourceCommitSha": value.get("sourceCommitSha"),
        "sourceArtifactDigest": value.get("sourceArtifactDigest"),
        "imageVersion": value.get("imageVersion"),
        "images": value.get("images", {}),
        "services": value.get("services", {}),
        "deployments": value.get("deployments", {}),
        "workerDeployments": value.get("workerDeployments", {}),
        "url": value.get("url"),
    }


def worker_deployments_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        "phase": value.get("phase"),
        "checkedAt": value.get("checkedAt"),
        "resources": value.get("resources", []),
        "message": value.get("message"),
    }


def phase_summary(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        key: value.get(key)
        for key in ("phase", "message", "checkedAt")
        if key in value
    }


def smoke_check_status(demo_project: dict[str, Any], phase: str) -> dict[str, Any]:
    smoke_check = spec_of(demo_project).get("smokeCheck")
    if not isinstance(smoke_check, dict):
        return {
            "phase": "NotConfigured",
            "message": "No smoke check configured.",
        }

    if phase == "WaitingForExternalResources":
        return {
            "phase": phase,
            "path": smoke_check.get("path"),
            "expectedStatus": smoke_check.get("expectedStatus"),
            "message": "Smoke check is blocked until required external resources are ready.",
        }

    if phase in {"WorkerRolloutPending", "WorkerRolloutFailed"}:
        return {
            "phase": phase,
            "path": smoke_check.get("path"),
            "expectedStatus": smoke_check.get("expectedStatus"),
            "message": "Smoke check is blocked until WorkerDeployment rollout succeeds.",
        }

    return {
        "phase": "Pending" if phase in {"CandidateApplied", "SmokeCheckPending"} else phase,
        "path": smoke_check.get("path"),
        "expectedStatus": smoke_check.get("expectedStatus"),
        "message": "Smoke check is ready to run after candidate deployment.",
    }


def smoke_check_internal_url(
    demo_project: dict[str, Any],
    *,
    image_overrides: dict[str, str],
) -> str | None:
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
    if image_overrides and not missing_image_overrides(demo_project, image_overrides):
        version = candidate_version(demo_project, image_overrides)
        service_name = candidate_service_name(
            demo_project,
            component_name,
            component,
            version,
        )
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


def candidate_version(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> str | None:
    if missing_image_overrides(demo_project, image_overrides):
        return None

    commit_sha = source_commit_sha(demo_project)
    if commit_sha:
        return dns_label(f"git-{commit_sha[:12]}")

    encoded = json.dumps(image_overrides, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12]
    return f"images-{digest}"


def candidate_service_status(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> dict[str, str]:
    version = candidate_version(demo_project, image_overrides)
    if not version:
        return {}

    return {
        component_name: (
            effective_service_name(component_name, component)
            if worker_versioning_enabled(component)
            else candidate_service_name(
                demo_project,
                component_name,
                component,
                version,
            )
        )
        for component_name, component in sorted(components(demo_project).items())
        if component_ports(component)
    }


def candidate_deployment_status(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> dict[str, str]:
    version = candidate_version(demo_project, image_overrides)
    if not version:
        return {}

    return {
        component_name: candidate_deployment_name(
            demo_project,
            component_name,
            version,
        )
        for component_name, component in sorted(components(demo_project).items())
        if isinstance(component.get("build"), str)
        and component["build"] in image_overrides
        and not worker_versioning_enabled(component)
    }


def worker_deployment_status(
    demo_project: dict[str, Any],
    image_overrides: dict[str, str],
) -> dict[str, str]:
    version = candidate_version(demo_project, image_overrides)
    if not version:
        return {}

    return {
        component_name: worker_deployment_name(component_name, component)
        for component_name, component in sorted(
            versioned_worker_components(demo_project).items()
        )
        if isinstance(component.get("build"), str)
        and component["build"] in image_overrides
    }


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
        "catalogStatus": {
            "source": "DemoProject.status",
            "demoProject": project_name(demo_project),
            "configMap": {
                "namespace": CATALOG_STATUS_NAMESPACE,
                "name": CATALOG_STATUS_CONFIGMAP,
                "key": f"{project_name(demo_project)}.json",
            },
            "fields": [
                "phase",
                "message",
                "kubernetesNamespaceStatus",
                "externalResources",
                "externalResourceBlock",
                "externalSecrets",
                "build",
                "candidate",
                "readiness",
                "workerDeployments",
                "smokeCheck",
                "activeImageVersion",
                "finalizeStatus",
                "lastError",
            ],
        },
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
                "requiredProperties": user_secret_required_properties(
                    demo_project,
                    secret_name,
                ),
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
    raw = [port for port in ports if isinstance(port, dict)]
    # Defaults that thin the common case: a lone port needs no name (it becomes
    # "http"), and servicePort falls back to the container port.
    single = len(raw) == 1
    normalized: list[dict[str, Any]] = []
    for port in raw:
        port = dict(port)
        if "name" not in port and single:
            port["name"] = "http"
        if "servicePort" not in port and "containerPort" in port:
            port["servicePort"] = port["containerPort"]
        normalized.append(port)
    return normalized


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

    return default_ingress_routes(demo_project)


def default_ingress_routes(demo_project: dict[str, Any]) -> list[dict[str, Any]]:
    """When ``ingress.routes`` is omitted, expose the public web component as a
    single catch-all. The public component is the one serving servicePort 80 (the
    HTTP convention); other components (APIs, workers) are reached internally. If
    no component serves 80, routing is ambiguous and must be declared explicitly.
    """
    for name, component in sorted(components(demo_project).items()):
        for port in component_ports(component):
            if port.get("servicePort") == 80 and isinstance(port.get("name"), str):
                return [{"component": name, "port": port["name"]}]
    return []


def auth_required(demo_project: dict[str, Any]) -> bool:
    ingress = spec_of(demo_project).get("ingress")
    if not isinstance(ingress, dict):
        return True
    return bool(ingress.get("temporalAuthRequired", True))


def component_needs_temporal(component: dict[str, Any]) -> bool:
    worker = component.get("worker")
    return (
        worker is True
        or isinstance(worker, dict)
        or component.get("temporalAccess") is True
    )


def temporal_proxy_config(component: dict[str, Any]) -> dict[str, Any]:
    proxy = component.get("temporalProxy")
    return proxy if isinstance(proxy, dict) else {}


def temporal_proxy_enabled(component: dict[str, Any]) -> bool:
    return temporal_proxy_config(component).get("enabled") is True


def worker_versioning(component: dict[str, Any]) -> dict[str, Any] | None:
    worker = component.get("worker")
    if not isinstance(worker, dict):
        return None
    versioning = worker.get("versioning")
    return versioning if isinstance(versioning, dict) else None


def worker_versioning_enabled(component: dict[str, Any]) -> bool:
    versioning = worker_versioning(component)
    return bool(versioning and versioning.get("enabled") is True)


def versioned_worker_components(
    demo_project: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        name: component
        for name, component in components(demo_project).items()
        if worker_versioning_enabled(component)
    }


def worker_deployment_name(component_name: str, component: dict[str, Any]) -> str:
    versioning = worker_versioning(component)
    task_queue = versioning.get("taskQueue") if isinstance(versioning, dict) else None
    return dns_label(task_queue if isinstance(task_queue, str) else component_name)


def temporal_components(demo_project: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        name: component
        for name, component in components(demo_project).items()
        if component_needs_temporal(component)
    }


def temporal_proxy_components(
    demo_project: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        name: component
        for name, component in components(demo_project).items()
        if temporal_proxy_enabled(component)
    }


def unversioned_temporal_components(
    demo_project: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        name: component
        for name, component in temporal_components(demo_project).items()
        if not worker_versioning_enabled(component)
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


def project_secret_definitions(
    demo_project: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    value = spec_of(demo_project).get("secrets")
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def user_secret_required_properties(
    demo_project: dict[str, Any],
    secret_name: str,
) -> list[str]:
    secret = project_secret_definitions(demo_project).get(secret_name)
    if not isinstance(secret, dict):
        return []
    required_properties = secret.get("requiredProperties")
    if not isinstance(required_properties, list):
        return []
    return sorted(name for name in required_properties if isinstance(name, str))


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


def candidate_service_name(
    demo_project: dict[str, Any],
    component_name: str,
    component: dict[str, Any],
    version: str,
) -> str:
    base_name = effective_service_name(component_name, component)
    return suffixed_dns_name(base_name, version)


def candidate_deployment_name(
    demo_project: dict[str, Any],
    component_name: str,
    version: str,
) -> str:
    return suffixed_dns_name(component_name, version)


def suffixed_dns_name(base_name: str, suffix: str) -> str:
    candidate = f"{base_name}-{suffix}"
    if len(candidate) <= MAX_DNS_LABEL_LENGTH:
        return candidate

    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()[:8]
    suffix_part = f"{suffix}-{digest}"
    stem_length = MAX_DNS_LABEL_LENGTH - len(suffix_part) - 1
    stem = base_name[:stem_length].rstrip("-")
    return f"{stem}-{suffix_part}"


def dns_label(value: str) -> str:
    rendered = "".join(
        character if character.isalnum() or character == "-" else "-"
        for character in value.lower()
    ).strip("-")
    if not rendered:
        rendered = "unknown"
    if len(rendered) <= MAX_DNS_LABEL_LENGTH:
        return rendered

    digest = hashlib.sha256(rendered.encode("utf-8")).hexdigest()[:8]
    stem = rendered[: MAX_DNS_LABEL_LENGTH - len(digest) - 1].rstrip("-")
    return f"{stem}-{digest}"


def effective_service_name(component_name: str, component: dict[str, Any]) -> str:
    service_name = component.get("serviceName")
    if isinstance(service_name, str):
        return service_name
    return component_name


def automount_service_account_token(component: dict[str, Any]) -> bool:
    return bool(component.get("automountServiceAccountToken") is True)


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


def rollout_labels(
    demo_project: dict[str, Any],
    component_name: str,
    version: str,
) -> dict[str, str]:
    return {
        **component_labels(demo_project, component_name),
        "platform.temporal.io/rollout-role": "candidate",
        "platform.temporal.io/rollout-version": version,
    }


def selector_labels(
    demo_project: dict[str, Any],
    component_name: str,
) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": project_name(demo_project),
        "app.kubernetes.io/component": component_name,
    }


def rollout_selector_labels(
    demo_project: dict[str, Any],
    component_name: str,
    version: str,
) -> dict[str, str]:
    return {
        **selector_labels(demo_project, component_name),
        "platform.temporal.io/rollout-version": version,
    }
