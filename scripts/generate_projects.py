#!/usr/bin/env python3
"""Generate cluster manifests and registry data from project configs."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from validate_projects import (
    DEFAULT_SCHEMA,
    ProjectDocument,
    builds,
    component_ports,
    components,
    derive_namespace,
    discover_project_paths,
    effective_service_name,
    ingress_routes,
    load_schema,
    load_yaml_document,
    relative_path,
    validate_schema,
    validate_semantics,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = ROOT / "clusters" / "sa-demo" / "generated"
BASE_DOMAIN = "tmprl-demo.cloud"
AUTH_MIDDLEWARE_NAME = "temporal-auth-required"
TRAEFIK_API_VERSION = "traefik.containo.us/v1alpha1"
CATALOG_AUTH_VERIFY_URL = "https://catalog.tmprl-demo.cloud/_auth/verify"
IMAGE_OVERRIDES_ANNOTATION = "registry.tmprl-demo.cloud/image-overrides"
SOURCE_COMMIT_ANNOTATION = "registry.tmprl-demo.cloud/source-commit-sha"
IMAGE_REPOSITORY_PREFIX = "tmprl-cloud-demo"
IMAGE_TAG_TEMPLATE = "git-${sourceCommitSha}"
DEMO_PROJECT_API_VERSION = "registry.tmprl-demo.cloud/v1alpha1"
TEMPORAL_API_KEY_TTL_DAYS = 365
TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS = 60
TEMPORAL_BOOTSTRAP_SECRET_PATH = "tmprl-cloud-demo/platform/temporal-cloud-bootstrap"
CATALOG_STATUS_NAMESPACE = "demo-registry-system"
CATALOG_STATUS_CONFIGMAP = "demo-registry-catalog-status"
CATALOG_STATUS_MOUNT_PATH = "/app/status"
CATALOG_STATUS_VOLUME_NAME = "catalog-status"
REGISTRY_PROJECT_NAME = "demo-registry"
REGISTRY_WEB_COMPONENT_NAME = "web"
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


@dataclass(frozen=True)
class Project:
    path: Path
    data: dict[str, Any]
    name: str
    namespace: str
    host: str


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    output_root = Path(args.output).resolve()

    projects = load_projects(args.paths)
    desired_files = render_all(projects)

    if args.check:
        return check_output(output_root, desired_files)

    write_output(output_root, desired_files)
    print(f"Generated {len(desired_files)} file(s) under {relative_path(output_root)}.")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic cluster output from project configs.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Project YAML files to generate. Defaults to projects/*.yaml.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_ROOT),
        help=f"Generated output directory. Defaults to {relative_path(DEFAULT_OUTPUT_ROOT)}.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check that generated files are current without writing changes.",
    )
    return parser.parse_args(argv)


def load_projects(paths: list[str]) -> list[Project]:
    schema = load_schema(DEFAULT_SCHEMA)
    validator = Draft202012Validator(schema)
    documents: list[ProjectDocument] = []
    errors = []

    for path in discover_project_paths(paths):
        data, load_errors = load_yaml_document(path)
        errors.extend(load_errors)
        if data is not None:
            document = ProjectDocument(path=path, data=data)
            documents.append(document)
            errors.extend(validate_schema(validator, document))

    errors.extend(validate_semantics(documents))
    if errors:
        print("Cannot generate project output because validation failed:", file=sys.stderr)
        for error in sorted(errors, key=lambda item: (relative_path(item.path), item.message)):
            print(f"- {error.render()}", file=sys.stderr)
        raise SystemExit(1)

    projects: list[Project] = []
    for document in sorted(
        documents,
        key=lambda item: str(item.data["metadata"]["name"]),
    ):
        name = document.data["metadata"]["name"]
        subdomain = document.data["ingress"]["subdomain"]
        projects.append(
            Project(
                path=document.path,
                data=document.data,
                name=name,
                namespace=derive_namespace(name),
                host=f"{subdomain}.{BASE_DOMAIN}",
            )
        )
    return projects


def render_all(projects: list[Project]) -> dict[Path, str]:
    files: dict[Path, str] = {
        Path("build-plan.json"): render_json(build_plan_payload(projects)),
        Path("demoprojects.yaml"): render_yaml_documents(
            [
                demo_project_resource(project, review_artifact=False)
                for project in projects
            ],
        ),
        Path("external-resources-plan.json"): render_json(
            external_resources_plan_payload(projects)
        ),
        Path("kustomization.yaml"): render_kustomization(
            ["demoprojects.yaml"]
        ),
        Path("registry.json"): render_json(registry_payload(projects)),
    }

    for project in projects:
        for relative_path, content in render_project(project).items():
            files[Path(project.name) / relative_path] = content

    return files


def render_project(project: Project) -> dict[Path, str]:
    resources = [
        "namespace.yaml",
        "rbac.yaml",
        "platform-metadata.yaml",
    ]

    rendered = {
        Path("namespace.yaml"): render_yaml_documents(
            [namespace_resource(project)],
        ),
        Path("rbac.yaml"): render_yaml_documents(rbac_resources(project)),
        Path("platform-metadata.yaml"): render_yaml_documents(
            [platform_metadata_configmap(project)],
        ),
    }

    service_resources = services(project)
    if service_resources:
        resources.append("services.yaml")
        rendered[Path("services.yaml")] = render_yaml_documents(service_resources)

    deployment_resource_list = deployments(project)
    if deployment_resource_list:
        rendered[Path("deployment-templates.yaml")] = render_yaml_documents(
            deployment_resource_list
        )

    rendered[Path("demoproject-template.yaml")] = render_yaml_documents(
        [demo_project_resource(project, review_artifact=True)]
    )

    ingress_resource_list = ingress_resources(project)
    if ingress_resource_list:
        resources.append("certificate.yaml")
        rendered[Path("certificate.yaml")] = render_yaml_documents(
            [certificate_resource(project)]
        )
        if auth_required(project):
            resources.append("auth-middleware.yaml")
            rendered[Path("auth-middleware.yaml")] = render_yaml_documents(
                [auth_middleware(project)]
            )
        resources.append("ingressroute.yaml")
        rendered[Path("ingressroute.yaml")] = render_yaml_documents(ingress_resource_list)

    if temporal_components(project):
        resources.append("external-secret.yaml")
        rendered[Path("external-secret.yaml")] = render_yaml_documents(
            [external_secret(project)]
        )

    user_secret_resource_list = user_external_secrets(project)
    if user_secret_resource_list:
        resources.append("user-external-secrets.yaml")
        rendered[Path("user-external-secrets.yaml")] = render_yaml_documents(
            user_secret_resource_list
        )

    workers = worker_components(project)
    if workers:
        resources.append("worker-controller-placeholder.yaml")
        rendered[Path("worker-controller-placeholder.yaml")] = render_yaml_documents(
            [worker_controller_placeholder(project, workers)]
        )

    rendered[Path("kustomization.yaml")] = render_kustomization(resources)
    return rendered


def demo_project_resource(project: Project, *, review_artifact: bool) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "source": project.data["source"],
        "registry": project.data["registry"],
        "builds": project.data["builds"],
        "components": project.data["components"],
        "ingress": project.data["ingress"],
        "derived": {
            "host": project.host,
            "kubernetesNamespace": project.namespace,
            "temporalNamespace": project.name,
            "secretPrefix": project_secret_prefix(project),
            "imageRepositoryPrefix": IMAGE_REPOSITORY_PREFIX,
        },
    }
    smoke_check = project.data.get("smokeCheck")
    if isinstance(smoke_check, dict):
        spec["smokeCheck"] = smoke_check

    annotations = {
        "platform.temporal.io/source": relative_path(project.path),
    }
    rollout = project.data.get("rollout")
    if isinstance(rollout, dict):
        image_overrides = rollout.get("imageOverrides")
        if isinstance(image_overrides, dict) and image_overrides:
            annotations[IMAGE_OVERRIDES_ANNOTATION] = json.dumps(
                image_overrides,
                sort_keys=True,
                separators=(",", ":"),
            )
        source_commit_sha = rollout.get("sourceCommitSha")
        if isinstance(source_commit_sha, str) and source_commit_sha:
            annotations[SOURCE_COMMIT_ANNOTATION] = source_commit_sha
    if review_artifact:
        annotations["platform.temporal.io/review-artifact"] = "true"

    return {
        "apiVersion": DEMO_PROJECT_API_VERSION,
        "kind": "DemoProject",
        "metadata": {
            "name": project.name,
            "labels": common_labels(project),
            "annotations": annotations,
        },
        "spec": spec,
    }


def namespace_resource(project: Project) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": project.namespace,
            "labels": common_labels(project),
            "annotations": {
                "platform.temporal.io/project": project.name,
                "platform.temporal.io/source": relative_path(project.path),
            },
        },
    }


def rbac_resources(project: Project) -> list[dict[str, Any]]:
    service_accounts = []
    role_bindings = []

    for component_name in sorted(components(project_document(project))):
        service_account_name = service_account_for_component(component_name)
        service_accounts.append(
            {
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "name": service_account_name,
                    "namespace": project.namespace,
                    "labels": component_labels(project, component_name),
                    "annotations": {
                        "platform.temporal.io/component": component_name,
                    },
                },
                "automountServiceAccountToken": False,
            }
        )
        role_bindings.append(
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": f"{service_account_name}-tenant-baseline",
                    "namespace": project.namespace,
                    "labels": component_labels(project, component_name),
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": service_account_name,
                        "namespace": project.namespace,
                    }
                ],
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "tenant-baseline",
                },
            }
        )

    role = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": "tenant-baseline",
            "namespace": project.namespace,
            "labels": common_labels(project),
        },
        "rules": [],
    }
    return [*service_accounts, role, *role_bindings]


def services(project: Project) -> list[dict[str, Any]]:
    resources = []
    project_components = components(project_document(project))

    for component_name, component in sorted(project_components.items()):
        ports = component_ports(component)
        if not ports:
            continue

        resources.append(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": effective_service_name(component_name, component),
                    "namespace": project.namespace,
                    "labels": component_labels(project, component_name),
                    "annotations": {
                        "platform.temporal.io/component": component_name,
                    },
                },
                "spec": {
                    "type": "ClusterIP",
                    "selector": selector_labels(project, component_name),
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


def deployments(project: Project) -> list[dict[str, Any]]:
    resources = []

    for component_name, component in sorted(
        components(project_document(project)).items()
    ):
        resources.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": component_name,
                    "namespace": project.namespace,
                    "labels": component_labels(project, component_name),
                    "annotations": {
                        "platform.temporal.io/placeholder": "true",
                        "platform.temporal.io/build": component["build"],
                    },
                },
                "spec": {
                    "replicas": component.get("replicas", 1),
                    "selector": {
                        "matchLabels": selector_labels(project, component_name),
                    },
                    "template": {
                        "metadata": {
                            "labels": component_labels(project, component_name),
                        },
                        "spec": deployment_pod_spec(
                            project,
                            component_name,
                            component,
                        ),
                    },
                },
            }
        )

    return resources


def container(
    project: Project,
    component_name: str,
    component: dict[str, Any],
) -> dict[str, Any]:
    rendered: dict[str, Any] = {
        "name": component_name,
        "image": placeholder_image(project, component["build"]),
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

    env_from = env_from_refs(project, component)
    if env_from:
        rendered["envFrom"] = env_from

    env = env_vars(project, component)
    if env:
        rendered["env"] = env
    if mounts_catalog_status(project, component_name):
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
            }
        ]

    return rendered


def deployment_pod_spec(
    project: Project,
    component_name: str,
    component: dict[str, Any],
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "serviceAccountName": service_account_for_component(component_name),
        "automountServiceAccountToken": False,
        "containers": [
            container(project, component_name, component),
        ],
    }
    if mounts_catalog_status(project, component_name):
        spec["volumes"] = [catalog_status_volume()]
    return spec


def mounts_catalog_status(project: Project, component_name: str) -> bool:
    return (
        project.name == REGISTRY_PROJECT_NAME
        and component_name == REGISTRY_WEB_COMPONENT_NAME
    )


def catalog_status_volume() -> dict[str, Any]:
    return {
        "name": CATALOG_STATUS_VOLUME_NAME,
        "configMap": {
            "name": CATALOG_STATUS_CONFIGMAP,
            "optional": True,
        },
    }


def env_from_refs(project: Project, component: dict[str, Any]) -> list[dict[str, Any]]:
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


def component_needs_temporal(component: dict[str, Any]) -> bool:
    return (
        isinstance(component.get("worker"), dict)
        or component.get("temporalAccess") is True
    )


def env_vars(project: Project, component: dict[str, Any]) -> list[dict[str, str]]:
    rendered = [
        render_env_var(item)
        for item in component.get("env", [])
        if isinstance(item, dict)
    ]

    worker = component.get("worker")
    if isinstance(worker, dict):
        rendered.extend(worker_versioning_env(project, worker))

    return rendered


def render_env_var(item: dict[str, Any]) -> dict[str, str]:
    return {"name": item["name"], "value": str(item["value"])}


def worker_versioning_env(project: Project, worker: dict[str, Any]) -> list[dict[str, str]]:
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


def placeholder_image(project: Project, build_name: str) -> str:
    return f"registry.invalid/{image_repository_name(project, build_name)}:git-sha"


def image_repository_name(project: Project, build_name: str) -> str:
    return f"{IMAGE_REPOSITORY_PREFIX}/{project.name}/{build_name}"


def image_uri_template(project: Project, build_name: str) -> str:
    repository = image_repository_name(project, build_name)
    return (
        "${awsAccountId}.dkr.ecr.${awsRegion}.amazonaws.com/"
        f"{repository}:{IMAGE_TAG_TEMPLATE}"
    )


def ingress_resources(project: Project) -> list[dict[str, Any]]:
    ingress = project.data["ingress"]
    route_specs = ingress_routes(ingress)
    if not route_specs:
        return []

    routes = [ingress_route(project, route_spec) for route_spec in route_specs]
    base = {
        "apiVersion": TRAEFIK_API_VERSION,
        "kind": "IngressRoute",
        "metadata": {
            "name": project.name,
            "namespace": project.namespace,
            "labels": common_labels(project),
            "annotations": {
                "platform.temporal.io/host": project.host,
                "platform.temporal.io/temporal-auth-required": str(
                    auth_required(project)
                ).lower(),
            },
        },
        "spec": {
            "entryPoints": ["websecure"],
            "routes": routes,
            "tls": {
                "secretName": f"{project.name}-tls",
            },
        },
    }

    http = {
        **base,
        "metadata": {
            **base["metadata"],
            "name": f"{project.name}-http",
        },
        "spec": {
            **base["spec"],
            "entryPoints": ["web"],
        },
    }
    http["spec"].pop("tls", None)
    return [base, http]


def certificate_resource(project: Project) -> dict[str, Any]:
    return {
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": f"{project.name}-tls",
            "namespace": project.namespace,
            "labels": common_labels(project),
        },
        "spec": {
            "dnsNames": [project.host],
            "issuerRef": {
                "group": "cert-manager.io",
                "kind": "ClusterIssuer",
                "name": "letsencrypt",
            },
            "privateKey": {
                "rotationPolicy": "Always",
            },
            "secretName": f"{project.name}-tls",
        },
    }


def ingress_route(project: Project, route_spec: dict[str, Any]) -> dict[str, Any]:
    match = f"Host(`{project.host}`)"
    path_prefix = route_spec.get("pathPrefix")
    if path_prefix:
        match = f"{match} && PathPrefix(`{path_prefix}`)"

    component_name = route_spec["component"]
    component = components(project_document(project))[component_name]
    route: dict[str, Any] = {
        "kind": "Rule",
        "match": match,
        "priority": 1000,
        "services": [
            {
                "name": effective_service_name(component_name, component),
                "namespace": project.namespace,
                "passHostHeader": True,
                "port": service_port(component, route_spec["port"]),
            }
        ],
    }
    if auth_required(project):
        route["middlewares"] = [
            {
                "name": AUTH_MIDDLEWARE_NAME,
                "namespace": project.namespace,
            }
        ]
    return route


def auth_middleware(project: Project) -> dict[str, Any]:
    return {
        "apiVersion": TRAEFIK_API_VERSION,
        "kind": "Middleware",
        "metadata": {
            "name": AUTH_MIDDLEWARE_NAME,
            "namespace": project.namespace,
            "labels": common_labels(project),
            "annotations": {
                "platform.temporal.io/placeholder": "true",
                "platform.temporal.io/description": "Temporal employee catalog auth forward-auth middleware.",
            },
        },
        "spec": {
            "forwardAuth": {
                "address": CATALOG_AUTH_VERIFY_URL,
                "authResponseHeaders": [
                    "X-Temporal-Auth-JWT",
                    "X-Temporal-Auth-Subject",
                    "X-Temporal-Auth-Email",
                ],
            }
        },
    }


def external_secret(project: Project) -> dict[str, Any]:
    return {
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {
            "name": "temporal-credentials",
            "namespace": project.namespace,
            "labels": common_labels(project),
            "annotations": {
                "platform.temporal.io/placeholder": "true",
                "platform.temporal.io/secret-prefix": project_secret_prefix(project),
            },
        },
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {
                "name": "platform-secrets",
                "kind": "ClusterSecretStore",
            },
            "target": {
                "name": "temporal-credentials",
                "creationPolicy": "Owner",
            },
            "data": [
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["endpoint"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(project),
                        "property": "address",
                    },
                },
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["namespace"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(project),
                        "property": "namespace",
                    },
                },
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["api_key"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(project),
                        "property": "apiKey",
                    },
                },
                {
                    "secretKey": STANDARD_TEMPORAL_ENV["tls"],
                    "remoteRef": {
                        "key": temporal_secret_remote_path(project),
                        "property": "tls",
                    },
                },
            ],
        },
    }


def user_external_secrets(project: Project) -> list[dict[str, Any]]:
    resources = []
    for secret_name in user_secret_names(project):
        resources.append(
            {
                "apiVersion": "external-secrets.io/v1",
                "kind": "ExternalSecret",
                "metadata": {
                    "name": user_secret_kubernetes_name(secret_name),
                    "namespace": project.namespace,
                    "labels": common_labels(project),
                    "annotations": {
                        "platform.temporal.io/secret-source": user_secret_remote_path(
                            project,
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
                                "key": user_secret_remote_path(project, secret_name),
                            }
                        }
                    ],
                },
            }
        )

    return resources


def user_secret_kubernetes_name(secret_name: str) -> str:
    return f"user-secret-{secret_name}"


def user_secret_remote_path(project: Project, secret_name: str) -> str:
    return f"{project_secret_prefix(project)}/{secret_name}"


def temporal_secret_remote_path(project: Project) -> str:
    return f"{project_secret_prefix(project)}/temporal"


def project_secret_prefix(project: Project) -> str:
    return f"tmprl-cloud-demo/{project.name}"


def platform_metadata_configmap(project: Project) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "platform-project-metadata",
            "namespace": project.namespace,
            "labels": common_labels(project),
        },
        "data": {
            "project.json": json.dumps(project_metadata(project), sort_keys=True),
            "registry.json": json.dumps(registry_entry(project), sort_keys=True),
        },
    }


def worker_controller_placeholder(
    project: Project,
    workers: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "worker-controller-placeholder",
            "namespace": project.namespace,
            "labels": common_labels(project),
            "annotations": {
                "platform.temporal.io/placeholder": "true",
                "platform.temporal.io/description": "Replace with Temporal Worker Controller resources after CRD shape is verified.",
            },
        },
        "data": {
            "workers.yaml": yaml.safe_dump(
                {
                    component_name: {
                        "taskQueue": component["worker"]["taskQueue"],
                        "temporalNamespace": project.name,
                        "versioning": component["worker"].get(
                            "versioning",
                            {"enabled": False},
                        ),
                    }
                    for component_name, component in sorted(workers.items())
                },
                sort_keys=True,
            ),
        },
    }


def registry_payload(projects: list[Project]) -> dict[str, Any]:
    return {
        "baseDomain": BASE_DOMAIN,
        "projects": [registry_entry(project) for project in projects],
    }


def build_plan_payload(projects: list[Project]) -> dict[str, Any]:
    return {
        "apiVersion": "tmprl-demo.cloud/v1alpha1",
        "kind": "BuildPlan",
        "imageRepositoryPrefix": IMAGE_REPOSITORY_PREFIX,
        "tagPolicy": {
            "source": "sourceCommitSha",
            "template": IMAGE_TAG_TEMPLATE,
        },
        "projects": [project_build_plan(project) for project in projects],
    }


def external_resources_plan_payload(projects: list[Project]) -> dict[str, Any]:
    return {
        "apiVersion": "tmprl-demo.cloud/v1alpha1",
        "kind": "ExternalResourcesPlan",
        "projects": [project_external_resources(project) for project in projects],
    }


def project_external_resources(project: Project) -> dict[str, Any]:
    secret_prefix = project_secret_prefix(project)
    ecr_repositories = [
        {
            "name": image_repository_name(project, build_name),
            "build": build_name,
            "components": components_for_build(project, build_name),
        }
        for build_name in sorted(builds(project_document(project)))
    ]

    return {
        "name": project.name,
        "kubernetesNamespace": project.namespace,
        "secretPrefix": secret_prefix,
        "ecrRepositories": ecr_repositories,
        "temporalCloud": temporal_cloud_resources(project),
        "secretsManager": secrets_manager_resources(project),
        "iam": {
            "externalSecretsReadPath": f"{secret_prefix}/*",
            "buildPushRepositories": [
                repository["name"] for repository in ecr_repositories
            ],
        },
    }


def temporal_cloud_resources(project: Project) -> dict[str, Any]:
    components_requiring_temporal = sorted(temporal_components(project))
    return {
        "provider": "temporal-cloud",
        "mode": "manual-until-provider-contract-is-implemented",
        "required": bool(components_requiring_temporal),
        "namespace": {
            "name": project.name,
            "action": "create-or-verify",
            "deletionPolicy": "retain-until-explicitly-approved",
        },
        "apiKey": {
            "action": "create-or-verify-or-rotate",
            "scope": "namespace",
            "namespace": project.name,
            "ttlDays": TEMPORAL_API_KEY_TTL_DAYS,
            "rotateBeforeExpiryDays": TEMPORAL_API_KEY_ROTATE_BEFORE_EXPIRY_DAYS,
            "secretRemotePath": temporal_secret_remote_path(project),
            "secretProperties": TEMPORAL_SECRET_PROPERTIES,
            "components": components_requiring_temporal,
        },
        "tcldCommandPlan": temporal_cloud_tcld_command_plan(project),
        "bootstrapCredential": {
            "secretRemotePath": TEMPORAL_BOOTSTRAP_SECRET_PATH,
            "kubernetesSecret": "temporal-cloud-bootstrap",
            "requiredProperties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
        },
    }


def temporal_cloud_tcld_command_plan(project: Project) -> list[dict[str, Any]]:
    service_account_name = f"{project.name}-runtime"
    return [
        {
            "step": "verify-namespace",
            "command": [
                "tcld",
                "namespace",
                "get",
                "--namespace",
                project.name,
            ],
        },
        {
            "step": "create-namespace",
            "command": [
                "tcld",
                "namespace",
                "create",
                "--namespace",
                project.name,
                "--region",
                "<temporal-region>",
                "--auth-method",
                "api_key",
                "--enable-delete-protection",
            ],
        },
        {
            "step": "create-scoped-service-account",
            "command": [
                "tcld",
                "service-account",
                "create-scoped",
                "--name",
                service_account_name,
                "--namespace-permission",
                f"{project.name}=Write",
            ],
        },
        {
            "step": "create-service-account-api-key",
            "command": [
                "tcld",
                "apikey",
                "create",
                "--name",
                f"{project.name}-runtime",
                "--description",
                f"tmprl-demo.cloud runtime key for {project.name}",
                "--duration",
                f"{TEMPORAL_API_KEY_TTL_DAYS}d",
                "--service-account-id",
                "<service-account-id>",
            ],
        },
        {
            "step": "list-service-account-api-keys",
            "command": [
                "tcld",
                "apikey",
                "list",
                "--owner-type",
                "service-account",
                "--owner-id",
                "<service-account-id>",
            ],
        },
    ]


def secrets_manager_resources(project: Project) -> dict[str, Any]:
    return {
        "temporalBootstrap": {
            "managedBy": "platform",
            "remotePath": TEMPORAL_BOOTSTRAP_SECRET_PATH,
            "required": bool(temporal_components(project)),
            "properties": TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
        },
        "temporalCredentials": {
            "managedBy": "platform",
            "remotePath": temporal_secret_remote_path(project),
            "required": bool(temporal_components(project)),
            "properties": TEMPORAL_SECRET_PROPERTIES,
        },
        "userManaged": [
            {
                "name": secret_name,
                "managedBy": "project-owner",
                "remotePath": user_secret_remote_path(project, secret_name),
                "components": sorted(components_for_user_secret(project, secret_name)),
            }
            for secret_name in user_secret_names(project)
        ],
    }


def project_build_plan(project: Project) -> dict[str, Any]:
    return {
        "name": project.name,
        "source": project.data["source"],
        "builds": [
            build_plan_entry(project, build_name, build)
            for build_name, build in sorted(builds(project_document(project)).items())
        ],
    }


def build_plan_entry(
    project: Project,
    build_name: str,
    build: dict[str, Any],
) -> dict[str, Any]:
    return {
        "name": build_name,
        "context": build["context"],
        "dockerfile": build["dockerfile"],
        "image": {
            "repositoryName": image_repository_name(project, build_name),
            "tagTemplate": IMAGE_TAG_TEMPLATE,
            "uriTemplate": image_uri_template(project, build_name),
            "placeholder": placeholder_image(project, build_name),
        },
        "components": components_for_build(project, build_name),
    }


def components_for_build(project: Project, build_name: str) -> list[str]:
    return [
        component_name
        for component_name, component in sorted(
            components(project_document(project)).items()
        )
        if component.get("build") == build_name
    ]


def registry_entry(project: Project) -> dict[str, Any]:
    registry = project.data["registry"]
    return {
        "name": project.name,
        "title": registry["title"],
        "summary": registry["summary"],
        "host": project.host,
        "appUrl": f"https://{project.host}",
        "sourceRepo": project.data["source"]["repo"],
        "temporalNamespace": project.name,
        "authRequired": auth_required(project),
        "catalogStatus": {
            "source": "DemoProject.status",
            "demoProject": project.name,
            "configMap": {
                "namespace": CATALOG_STATUS_NAMESPACE,
                "name": CATALOG_STATUS_CONFIGMAP,
                "key": f"{project.name}.json",
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
                "smokeCheck",
                "activeImageVersion",
                "lastError",
            ],
        },
        "tags": registry.get("tags", []),
        "temporalSdks": registry.get("temporalSdks", []),
        "links": registry.get("links", {}),
        "assets": registry.get("assets", {}),
    }


def project_metadata(project: Project) -> dict[str, Any]:
    return {
        "name": project.name,
        "namespace": project.namespace,
        "host": project.host,
        "source": project.data["source"],
        "builds": sorted(builds(project_document(project))),
        "components": sorted(components(project_document(project))),
        "userSecrets": {
            secret_name: {
                "remotePath": user_secret_remote_path(project, secret_name),
                "kubernetesSecret": user_secret_kubernetes_name(secret_name),
                "components": sorted(components_for_user_secret(project, secret_name)),
            }
            for secret_name in user_secret_names(project)
        },
        "temporal": {
            "namespace": project.name,
            "secretPrefix": project_secret_prefix(project),
            "remotePath": temporal_secret_remote_path(project),
            "standardEnv": STANDARD_TEMPORAL_ENV,
            "components": sorted(temporal_components(project)),
        },
    }


def components_for_user_secret(project: Project, secret_name: str) -> list[str]:
    names = []
    for component_name, component in components(project_document(project)).items():
        references = component.get("secrets")
        if isinstance(references, list) and secret_name in references:
            names.append(component_name)
    return names


def user_secret_names(project: Project) -> list[str]:
    names: set[str] = set()
    for component in components(project_document(project)).values():
        references = component.get("secrets")
        if not isinstance(references, list):
            continue
        names.update(secret for secret in references if isinstance(secret, str))
    return sorted(names)


def auth_required(project: Project) -> bool:
    return bool(project.data["ingress"].get("temporalAuthRequired", True))


def worker_components(project: Project) -> dict[str, dict[str, Any]]:
    return {
        name: component
        for name, component in components(project_document(project)).items()
        if isinstance(component.get("worker"), dict)
    }


def temporal_components(project: Project) -> dict[str, dict[str, Any]]:
    return {
        name: component
        for name, component in components(project_document(project)).items()
        if isinstance(component.get("worker"), dict)
        or component.get("temporalAccess") is True
    }


def service_port(component: dict[str, Any], port_name: str) -> int:
    for port in component_ports(component):
        if port["name"] == port_name:
            return int(port["servicePort"])
    raise ValueError(f"component is missing port {port_name!r}")


def service_account_for_component(component_name: str) -> str:
    return component_name


def project_document(project: Project) -> ProjectDocument:
    return ProjectDocument(path=project.path, data=project.data)


def common_labels(project: Project) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": project.name,
        "app.kubernetes.io/managed-by": "demo-registry",
        "platform.temporal.io/project": project.name,
    }


def component_labels(project: Project, component_name: str) -> dict[str, str]:
    return {
        **common_labels(project),
        "app.kubernetes.io/component": component_name,
    }


def selector_labels(project: Project, component_name: str) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": project.name,
        "app.kubernetes.io/component": component_name,
    }


def render_yaml_documents(documents: list[dict[str, Any]]) -> str:
    rendered = ["# Generated by scripts/generate_projects.py. Do not edit by hand.\n"]
    for index, document in enumerate(documents):
        if index:
            rendered.append("---\n")
        rendered.append(yaml.safe_dump(document, sort_keys=False))
    return "".join(rendered)


def render_kustomization(resources: list[str]) -> str:
    body = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "resources": resources,
    }
    return render_yaml_documents([body])


def render_json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def write_output(output_root: Path, desired_files: dict[Path, str]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    for path in output_root.iterdir():
        if path.name == ".gitkeep":
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    for relative_path, content in desired_files.items():
        destination = output_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content, encoding="utf-8")


def check_output(output_root: Path, desired_files: dict[Path, str]) -> int:
    existing_files = {
        path.relative_to(output_root)
        for path in output_root.rglob("*")
        if path.is_file() and path.name != ".gitkeep"
    }
    desired_paths = set(desired_files)

    missing = sorted(desired_paths - existing_files)
    extra = sorted(existing_files - desired_paths)
    changed = sorted(
        path
        for path in desired_paths & existing_files
        if (output_root / path).read_text(encoding="utf-8") != desired_files[path]
    )

    if not missing and not extra and not changed:
        print(f"Generated output is current under {relative_path(output_root)}.")
        return 0

    print("Generated output is out of date.", file=sys.stderr)
    for path in missing:
        print(f"- missing: {relative_path(output_root / path)}", file=sys.stderr)
    for path in extra:
        print(f"- extra: {relative_path(output_root / path)}", file=sys.stderr)
    for path in changed:
        print(f"- changed: {relative_path(output_root / path)}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
