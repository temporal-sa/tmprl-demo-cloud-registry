from __future__ import annotations

from typing import Any


DEMO_PROJECT_GROUP = "registry.tmprl-demo.cloud"
DEMO_PROJECT_VERSION = "v1alpha1"
DEMO_PROJECT_PLURAL = "demoprojects"

CUSTOM_RESOURCE_PLURALS = {
    ("cert-manager.io", "v1", "Certificate"): "certificates",
    ("external-secrets.io", "v1", "ExternalSecret"): "externalsecrets",
    ("traefik.containo.us", "v1alpha1", "IngressRoute"): "ingressroutes",
    ("traefik.containo.us", "v1alpha1", "Middleware"): "middlewares",
}


def list_demo_projects() -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    response = api.list_cluster_custom_object(
        group=DEMO_PROJECT_GROUP,
        version=DEMO_PROJECT_VERSION,
        plural=DEMO_PROJECT_PLURAL,
    )
    items = response.get("items", []) if isinstance(response, dict) else []
    return [item for item in items if isinstance(item, dict)]


def apply_resources(
    resources: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> list[dict[str, Any]]:
    results = []
    for resource in resources:
        metadata = resource.get("metadata", {})
        name = metadata.get("name") if isinstance(metadata, dict) else None
        namespace = metadata.get("namespace") if isinstance(metadata, dict) else None
        summary = {
            "apiVersion": resource.get("apiVersion"),
            "kind": resource.get("kind"),
            "name": name,
            "action": "planned" if dry_run else "applied",
        }
        if namespace:
            summary["namespace"] = namespace
        if not dry_run:
            apply_resource(resource)
        results.append(summary)
    return results


def apply_resource(resource: dict[str, Any]) -> None:
    client = _client_module()
    load_kubernetes_config()
    kind = resource.get("kind")
    metadata = resource.get("metadata", {})
    if not isinstance(metadata, dict) or not isinstance(metadata.get("name"), str):
        raise ValueError(f"{kind} resource is missing metadata.name")

    name = metadata["name"]
    namespace = metadata.get("namespace")

    if kind == "Namespace":
        core = client.CoreV1Api()
        _create_or_patch(
            read=lambda: core.read_namespace(name),
            create=lambda: core.create_namespace(resource),
            patch=lambda: core.patch_namespace(name, resource),
        )
        return

    if not isinstance(namespace, str):
        raise ValueError(f"{kind} {name} is missing metadata.namespace")

    if kind == "ServiceAccount":
        core = client.CoreV1Api()
        _create_or_patch(
            read=lambda: core.read_namespaced_service_account(name, namespace),
            create=lambda: core.create_namespaced_service_account(namespace, resource),
            patch=lambda: core.patch_namespaced_service_account(
                name,
                namespace,
                resource,
            ),
        )
        return

    if kind == "ConfigMap":
        core = client.CoreV1Api()
        _create_or_patch(
            read=lambda: core.read_namespaced_config_map(name, namespace),
            create=lambda: core.create_namespaced_config_map(namespace, resource),
            patch=lambda: core.patch_namespaced_config_map(name, namespace, resource),
        )
        return

    if kind == "Service":
        core = client.CoreV1Api()
        _create_or_patch(
            read=lambda: core.read_namespaced_service(name, namespace),
            create=lambda: core.create_namespaced_service(namespace, resource),
            patch=lambda: core.patch_namespaced_service(name, namespace, resource),
        )
        return

    if kind == "Deployment":
        apps = client.AppsV1Api()
        _create_or_patch(
            read=lambda: apps.read_namespaced_deployment(name, namespace),
            create=lambda: apps.create_namespaced_deployment(namespace, resource),
            patch=lambda: apps.patch_namespaced_deployment(name, namespace, resource),
        )
        return

    if kind == "Role":
        rbac = client.RbacAuthorizationV1Api()
        _create_or_patch(
            read=lambda: rbac.read_namespaced_role(name, namespace),
            create=lambda: rbac.create_namespaced_role(namespace, resource),
            patch=lambda: rbac.patch_namespaced_role(name, namespace, resource),
        )
        return

    if kind == "RoleBinding":
        rbac = client.RbacAuthorizationV1Api()
        _create_or_patch(
            read=lambda: rbac.read_namespaced_role_binding(name, namespace),
            create=lambda: rbac.create_namespaced_role_binding(namespace, resource),
            patch=lambda: rbac.patch_namespaced_role_binding(
                name,
                namespace,
                resource,
            ),
        )
        return

    if isinstance(resource.get("apiVersion"), str):
        apply_custom_resource(resource, name=name, namespace=namespace)
        return

    raise ValueError(f"Unsupported resource kind {kind!r}")


def apply_custom_resource(
    resource: dict[str, Any],
    *,
    name: str,
    namespace: str,
) -> None:
    api_version = resource["apiVersion"]
    if "/" not in api_version:
        raise ValueError(f"Unsupported custom apiVersion {api_version!r}")

    group, version = api_version.split("/", 1)
    kind = resource.get("kind")
    plural = CUSTOM_RESOURCE_PLURALS.get((group, version, str(kind)))
    if not plural:
        raise ValueError(f"Unsupported custom resource {api_version}/{kind}")

    client = _client_module()
    custom = client.CustomObjectsApi()
    _create_or_patch(
        read=lambda: custom.get_namespaced_custom_object(
            group,
            version,
            namespace,
            plural,
            name,
        ),
        create=lambda: custom.create_namespaced_custom_object(
            group,
            version,
            namespace,
            plural,
            resource,
        ),
        patch=lambda: custom.patch_namespaced_custom_object(
            group,
            version,
            namespace,
            plural,
            name,
            resource,
        ),
    )


def patch_demo_project_status(name: str, status: dict[str, Any], *, dry_run: bool) -> None:
    if dry_run:
        return

    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    api.patch_cluster_custom_object_status(
        group=DEMO_PROJECT_GROUP,
        version=DEMO_PROJECT_VERSION,
        plural=DEMO_PROJECT_PLURAL,
        name=name,
        body={"status": status},
    )


def _create_or_patch(read: Any, create: Any, patch: Any) -> None:
    try:
        read()
    except Exception as error:
        if _api_status(error) == 404:
            create()
            return
        raise
    patch()


def _api_status(error: Exception) -> int | None:
    status = getattr(error, "status", None)
    return int(status) if isinstance(status, int) else None


def load_kubernetes_config() -> None:
    config = _config_module()
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()


def _client_module() -> Any:
    try:
        from kubernetes import client
    except ImportError as error:
        raise RuntimeError(
            "The kubernetes package is required for operator reconcile mode."
        ) from error
    return client


def _config_module() -> Any:
    try:
        from kubernetes import config
    except ImportError as error:
        raise RuntimeError(
            "The kubernetes package is required for operator reconcile mode."
        ) from error
    return config
