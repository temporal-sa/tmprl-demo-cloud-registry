from __future__ import annotations

import base64
from typing import Any


DEMO_PROJECT_GROUP = "registry.tmprl-demo.cloud"
DEMO_PROJECT_VERSION = "v1alpha1"
DEMO_PROJECT_PLURAL = "demoprojects"
INSTRUQT_COURSE_PLURAL = "instruqtcourses"
FLUX_SOURCE_GROUP = "source.toolkit.fluxcd.io"
FLUX_SOURCE_VERSION = "v1"
FLUX_GIT_REPOSITORY_PLURAL = "gitrepositories"
EXTERNAL_SECRETS_GROUP = "external-secrets.io"
EXTERNAL_SECRETS_VERSION = "v1"
EXTERNAL_SECRET_PLURAL = "externalsecrets"
TEMPORAL_WORKER_CONTROLLER_GROUP = "temporal.io"
TEMPORAL_WORKER_CONTROLLER_VERSION = "v1alpha1"
TEMPORAL_CONNECTION_PLURAL = "connections"
TEMPORAL_WORKER_DEPLOYMENT_PLURAL = "workerdeployments"

CUSTOM_RESOURCE_PLURALS = {
    ("cert-manager.io", "v1", "Certificate"): "certificates",
    (EXTERNAL_SECRETS_GROUP, EXTERNAL_SECRETS_VERSION, "ExternalSecret"): (
        EXTERNAL_SECRET_PLURAL
    ),
    (FLUX_SOURCE_GROUP, FLUX_SOURCE_VERSION, "GitRepository"): FLUX_GIT_REPOSITORY_PLURAL,
    (
        TEMPORAL_WORKER_CONTROLLER_GROUP,
        TEMPORAL_WORKER_CONTROLLER_VERSION,
        "Connection",
    ): TEMPORAL_CONNECTION_PLURAL,
    (
        TEMPORAL_WORKER_CONTROLLER_GROUP,
        TEMPORAL_WORKER_CONTROLLER_VERSION,
        "WorkerDeployment",
    ): TEMPORAL_WORKER_DEPLOYMENT_PLURAL,
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


def list_instruqt_courses() -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    response = api.list_cluster_custom_object(
        group=DEMO_PROJECT_GROUP,
        version=DEMO_PROJECT_VERSION,
        plural=INSTRUQT_COURSE_PLURAL,
    )
    items = response.get("items", []) if isinstance(response, dict) else []
    return [item for item in items if isinstance(item, dict)]


def read_demo_project(name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        resource = client.CustomObjectsApi().get_cluster_custom_object(
            DEMO_PROJECT_GROUP,
            DEMO_PROJECT_VERSION,
            DEMO_PROJECT_PLURAL,
            name,
        )
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return resource if isinstance(resource, dict) else None


def read_instruqt_course(name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        resource = client.CustomObjectsApi().get_cluster_custom_object(
            DEMO_PROJECT_GROUP,
            DEMO_PROJECT_VERSION,
            INSTRUQT_COURSE_PLURAL,
            name,
        )
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return resource if isinstance(resource, dict) else None


def apply_demo_project(resource: dict[str, Any], *, dry_run: bool) -> str:
    if dry_run:
        return "planned"
    metadata = resource.get("metadata", {})
    name = metadata.get("name") if isinstance(metadata, dict) else None
    if not isinstance(name, str):
        raise ValueError("DemoProject resource is missing metadata.name")

    client = _client_module()
    load_kubernetes_config()
    custom = client.CustomObjectsApi()
    return _create_or_patch(
        read=lambda: custom.get_cluster_custom_object(
            DEMO_PROJECT_GROUP,
            DEMO_PROJECT_VERSION,
            DEMO_PROJECT_PLURAL,
            name,
        ),
        create=lambda: custom.create_cluster_custom_object(
            DEMO_PROJECT_GROUP,
            DEMO_PROJECT_VERSION,
            DEMO_PROJECT_PLURAL,
            resource,
        ),
        patch=lambda: custom.patch_cluster_custom_object(
            DEMO_PROJECT_GROUP,
            DEMO_PROJECT_VERSION,
            DEMO_PROJECT_PLURAL,
            name,
            resource,
        ),
    )


def delete_demo_project(name: str, *, dry_run: bool = False) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().delete_cluster_custom_object(
            DEMO_PROJECT_GROUP,
            DEMO_PROJECT_VERSION,
            DEMO_PROJECT_PLURAL,
            name,
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def read_namespace(name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        namespace = client.CoreV1Api().read_namespace(name)
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    metadata = getattr(namespace, "metadata", None)
    status = getattr(namespace, "status", None)
    return {
        "name": name,
        "phase": getattr(status, "phase", None),
        "deletionTimestamp": getattr(metadata, "deletion_timestamp", None),
    }


def list_namespaces(*, label_selector: str | None = None) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CoreV1Api().list_namespace(label_selector=label_selector)
    summaries = [
        object_metadata_summary(item)
        for item in getattr(response, "items", [])
    ]
    return [item for item in summaries if item.get("name")]


def read_config_map(namespace: str, name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        configmap = client.CoreV1Api().read_namespaced_config_map(name, namespace)
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    metadata = getattr(configmap, "metadata", None)
    return {
        "name": name,
        "namespace": namespace,
        "resourceVersion": getattr(metadata, "resource_version", None),
        "data": getattr(configmap, "data", None) or {},
    }


def read_secret_data(namespace: str, name: str) -> dict[str, str] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        secret = client.CoreV1Api().read_namespaced_secret(name, namespace)
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise

    data = getattr(secret, "data", None)
    if not isinstance(data, dict):
        return {}

    decoded: dict[str, str] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        try:
            decoded[key] = base64.b64decode(value).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            continue
    return decoded


def upsert_secret_data(
    namespace: str,
    name: str,
    data: dict[str, str],
    *,
    labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
    dry_run: bool = False,
) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    core = client.CoreV1Api()
    body: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "type": "Opaque",
        "stringData": data,
    }
    if labels:
        body["metadata"]["labels"] = labels
    if annotations:
        body["metadata"]["annotations"] = annotations

    return _create_or_patch(
        read=lambda: core.read_namespaced_secret(name, namespace),
        create=lambda: core.create_namespaced_secret(namespace, body),
        patch=lambda: core.patch_namespaced_secret(name, namespace, body),
    )


def read_service(namespace: str, name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        service = client.CoreV1Api().read_namespaced_service(name, namespace)
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    metadata = getattr(service, "metadata", None)
    spec = getattr(service, "spec", None)
    return {
        "name": name,
        "namespace": namespace,
        "resourceVersion": getattr(metadata, "resource_version", None),
        "selector": getattr(spec, "selector", None),
    }


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
            summary["action"] = apply_resource(resource)
        results.append(summary)
    return results


def apply_resource(resource: dict[str, Any]) -> str:
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
        return _create_or_patch(
            read=lambda: core.read_namespace(name),
            create=lambda: core.create_namespace(resource),
            patch=lambda: core.patch_namespace(name, resource),
        )

    if kind == "DemoProject":
        return apply_demo_project(resource, dry_run=False)

    if not isinstance(namespace, str):
        raise ValueError(f"{kind} {name} is missing metadata.namespace")

    if kind == "ServiceAccount":
        core = client.CoreV1Api()
        return _create_or_patch(
            read=lambda: core.read_namespaced_service_account(name, namespace),
            create=lambda: core.create_namespaced_service_account(namespace, resource),
            patch=lambda: core.patch_namespaced_service_account(
                name,
                namespace,
                resource,
            ),
        )

    if kind == "ConfigMap":
        core = client.CoreV1Api()
        return _create_or_patch(
            read=lambda: core.read_namespaced_config_map(name, namespace),
            create=lambda: core.create_namespaced_config_map(namespace, resource),
            patch=lambda: core.patch_namespaced_config_map(name, namespace, resource),
        )

    if kind == "Service":
        core = client.CoreV1Api()
        return _create_or_patch(
            read=lambda: core.read_namespaced_service(name, namespace),
            create=lambda: core.create_namespaced_service(namespace, resource),
            patch=lambda: core.patch_namespaced_service(name, namespace, resource),
        )

    if kind == "Deployment":
        apps = client.AppsV1Api()
        return _create_or_patch(
            read=lambda: apps.read_namespaced_deployment(name, namespace),
            create=lambda: apps.create_namespaced_deployment(namespace, resource),
            patch=lambda: apps.patch_namespaced_deployment(name, namespace, resource),
        )

    if kind == "Role":
        rbac = client.RbacAuthorizationV1Api()
        return _create_or_patch(
            read=lambda: rbac.read_namespaced_role(name, namespace),
            create=lambda: rbac.create_namespaced_role(namespace, resource),
            patch=lambda: rbac.patch_namespaced_role(name, namespace, resource),
        )

    if kind == "RoleBinding":
        rbac = client.RbacAuthorizationV1Api()
        return _create_or_patch(
            read=lambda: rbac.read_namespaced_role_binding(name, namespace),
            create=lambda: rbac.create_namespaced_role_binding(namespace, resource),
            patch=lambda: rbac.patch_namespaced_role_binding(
                name,
                namespace,
                resource,
            ),
        )

    if isinstance(resource.get("apiVersion"), str):
        return apply_custom_resource(resource, name=name, namespace=namespace)

    raise ValueError(f"Unsupported resource kind {kind!r}")


def apply_custom_resource(
    resource: dict[str, Any],
    *,
    name: str,
    namespace: str,
) -> str:
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
    return _create_or_patch(
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


DEMO_PROJECT_FINALIZER = "registry.tmprl-demo.cloud/finalize"


def demo_project_deletion_timestamp(demo_project: dict[str, Any]) -> str | None:
    metadata = demo_project.get("metadata", {})
    if not isinstance(metadata, dict):
        return None
    value = metadata.get("deletionTimestamp")
    return value if isinstance(value, str) and value else None


def instruqt_course_deletion_timestamp(
    instruqt_course: dict[str, Any],
) -> str | None:
    metadata = instruqt_course.get("metadata", {})
    if not isinstance(metadata, dict):
        return None
    value = metadata.get("deletionTimestamp")
    return value if isinstance(value, str) and value else None


def demo_project_finalizers(demo_project: dict[str, Any]) -> list[str]:
    metadata = demo_project.get("metadata", {})
    finalizers = metadata.get("finalizers") if isinstance(metadata, dict) else None
    return [f for f in finalizers if isinstance(f, str)] if isinstance(finalizers, list) else []


def instruqt_course_finalizers(instruqt_course: dict[str, Any]) -> list[str]:
    metadata = instruqt_course.get("metadata", {})
    finalizers = metadata.get("finalizers") if isinstance(metadata, dict) else None
    return (
        [f for f in finalizers if isinstance(f, str)]
        if isinstance(finalizers, list)
        else []
    )


def ensure_demo_project_finalizer(name: str, *, dry_run: bool = False) -> bool:
    """Add the operator finalizer to a DemoProject if absent. Returns True if added."""
    if dry_run:
        return False
    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    obj = api.get_cluster_custom_object(
        DEMO_PROJECT_GROUP, DEMO_PROJECT_VERSION, DEMO_PROJECT_PLURAL, name
    )
    finalizers = demo_project_finalizers(obj)
    if DEMO_PROJECT_FINALIZER in finalizers:
        return False
    api.patch_cluster_custom_object(
        DEMO_PROJECT_GROUP,
        DEMO_PROJECT_VERSION,
        DEMO_PROJECT_PLURAL,
        name,
        {"metadata": {"finalizers": [*finalizers, DEMO_PROJECT_FINALIZER]}},
    )
    return True


def ensure_instruqt_course_finalizer(name: str, *, dry_run: bool = False) -> bool:
    """Add the operator finalizer to an InstruqtCourse if absent."""
    if dry_run:
        return False
    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    obj = api.get_cluster_custom_object(
        DEMO_PROJECT_GROUP,
        DEMO_PROJECT_VERSION,
        INSTRUQT_COURSE_PLURAL,
        name,
    )
    finalizers = instruqt_course_finalizers(obj)
    if DEMO_PROJECT_FINALIZER in finalizers:
        return False
    api.patch_cluster_custom_object(
        DEMO_PROJECT_GROUP,
        DEMO_PROJECT_VERSION,
        INSTRUQT_COURSE_PLURAL,
        name,
        {"metadata": {"finalizers": [*finalizers, DEMO_PROJECT_FINALIZER]}},
    )
    return True


def remove_demo_project_finalizer(name: str, *, dry_run: bool = False) -> None:
    """Remove the operator finalizer so Kubernetes can complete deletion."""
    if dry_run:
        return
    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    try:
        obj = api.get_cluster_custom_object(
            DEMO_PROJECT_GROUP, DEMO_PROJECT_VERSION, DEMO_PROJECT_PLURAL, name
        )
    except Exception as error:
        if _api_status(error) == 404:
            return
        raise
    finalizers = [f for f in demo_project_finalizers(obj) if f != DEMO_PROJECT_FINALIZER]
    api.patch_cluster_custom_object(
        DEMO_PROJECT_GROUP,
        DEMO_PROJECT_VERSION,
        DEMO_PROJECT_PLURAL,
        name,
        {"metadata": {"finalizers": finalizers}},
    )


def remove_instruqt_course_finalizer(name: str, *, dry_run: bool = False) -> None:
    """Remove the operator finalizer so Kubernetes can complete deletion."""
    if dry_run:
        return
    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    try:
        obj = api.get_cluster_custom_object(
            DEMO_PROJECT_GROUP,
            DEMO_PROJECT_VERSION,
            INSTRUQT_COURSE_PLURAL,
            name,
        )
    except Exception as error:
        if _api_status(error) == 404:
            return
        raise
    finalizers = [
        f for f in instruqt_course_finalizers(obj) if f != DEMO_PROJECT_FINALIZER
    ]
    api.patch_cluster_custom_object(
        DEMO_PROJECT_GROUP,
        DEMO_PROJECT_VERSION,
        INSTRUQT_COURSE_PLURAL,
        name,
        {"metadata": {"finalizers": finalizers}},
    )


def delete_namespace(name: str, *, dry_run: bool = False) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    core = client.CoreV1Api()
    try:
        core.delete_namespace(name)
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


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


def patch_instruqt_course_status(
    name: str,
    status: dict[str, Any],
    *,
    dry_run: bool,
) -> None:
    if dry_run:
        return

    client = _client_module()
    load_kubernetes_config()
    api = client.CustomObjectsApi()
    api.patch_cluster_custom_object_status(
        group=DEMO_PROJECT_GROUP,
        version=DEMO_PROJECT_VERSION,
        plural=INSTRUQT_COURSE_PLURAL,
        name=name,
        body={"status": status},
    )


def read_git_repository(namespace: str, name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        resource = client.CustomObjectsApi().get_namespaced_custom_object(
            FLUX_SOURCE_GROUP,
            FLUX_SOURCE_VERSION,
            namespace,
            FLUX_GIT_REPOSITORY_PLURAL,
            name,
        )
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return resource if isinstance(resource, dict) else None


def delete_git_repository(namespace: str, name: str, *, dry_run: bool = False) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().delete_namespaced_custom_object(
            FLUX_SOURCE_GROUP,
            FLUX_SOURCE_VERSION,
            namespace,
            FLUX_GIT_REPOSITORY_PLURAL,
            name,
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def delete_external_secret(namespace: str, name: str, *, dry_run: bool = False) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().delete_namespaced_custom_object(
            EXTERNAL_SECRETS_GROUP,
            EXTERNAL_SECRETS_VERSION,
            namespace,
            EXTERNAL_SECRET_PLURAL,
            name,
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def list_deployments(
    namespace: str,
    *,
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.AppsV1Api().list_namespaced_deployment(
        namespace,
        label_selector=label_selector,
    )
    summaries = [
        object_metadata_summary(item)
        for item in getattr(response, "items", [])
    ]
    return [item for item in summaries if item.get("name")]


def delete_deployment(namespace: str, name: str, *, dry_run: bool = False) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.AppsV1Api().delete_namespaced_deployment(name, namespace)
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def list_services(
    namespace: str,
    *,
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CoreV1Api().list_namespaced_service(
        namespace,
        label_selector=label_selector,
    )
    summaries = [
        object_metadata_summary(item)
        for item in getattr(response, "items", [])
    ]
    return [item for item in summaries if item.get("name")]


def delete_service(namespace: str, name: str, *, dry_run: bool = False) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CoreV1Api().delete_namespaced_service(name, namespace)
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def read_deployment_status(namespace: str, name: str) -> dict[str, Any]:
    client = _client_module()
    load_kubernetes_config()
    deployment = client.AppsV1Api().read_namespaced_deployment(name, namespace)
    metadata = getattr(deployment, "metadata", None)
    spec = getattr(deployment, "spec", None)
    status = getattr(deployment, "status", None)
    return {
        "name": name,
        "namespace": namespace,
        "generation": getattr(metadata, "generation", None),
        "replicas": getattr(spec, "replicas", None),
        "observedGeneration": getattr(status, "observed_generation", None),
        "replicasStatus": getattr(status, "replicas", None),
        "readyReplicas": getattr(status, "ready_replicas", None),
        "availableReplicas": getattr(status, "available_replicas", None),
        "updatedReplicas": getattr(status, "updated_replicas", None),
        "conditions": deployment_conditions(status),
    }


def read_external_secret_status(namespace: str, name: str) -> dict[str, Any]:
    client = _client_module()
    load_kubernetes_config()
    resource = client.CustomObjectsApi().get_namespaced_custom_object(
        "external-secrets.io",
        "v1",
        namespace,
        "externalsecrets",
        name,
    )
    status = resource.get("status", {}) if isinstance(resource, dict) else {}
    conditions = status.get("conditions", []) if isinstance(status, dict) else []
    rendered_conditions = [
        external_secret_condition(condition)
        for condition in conditions
        if isinstance(condition, dict)
    ]
    ready_condition = next(
        (
            condition
            for condition in rendered_conditions
            if condition.get("type") == "Ready"
        ),
        {},
    )
    ready = ready_condition.get("status") == "True"
    return {
        "name": name,
        "namespace": namespace,
        "phase": "Ready" if ready else "Waiting",
        "ready": ready,
        "reason": ready_condition.get("reason"),
        "message": ready_condition.get("message"),
        "refreshTime": status.get("refreshTime") if isinstance(status, dict) else None,
        "syncedResourceVersion": (
            status.get("syncedResourceVersion") if isinstance(status, dict) else None
        ),
        "conditions": rendered_conditions,
    }


def read_worker_deployment_status(namespace: str, name: str) -> dict[str, Any]:
    client = _client_module()
    load_kubernetes_config()
    resource = client.CustomObjectsApi().get_namespaced_custom_object(
        TEMPORAL_WORKER_CONTROLLER_GROUP,
        TEMPORAL_WORKER_CONTROLLER_VERSION,
        namespace,
        TEMPORAL_WORKER_DEPLOYMENT_PLURAL,
        name,
    )
    metadata = resource.get("metadata", {}) if isinstance(resource, dict) else {}
    status = resource.get("status", {}) if isinstance(resource, dict) else {}
    return {
        "name": name,
        "namespace": namespace,
        "generation": metadata.get("generation") if isinstance(metadata, dict) else None,
        "targetVersion": (
            status.get("targetVersion") if isinstance(status, dict) else None
        ),
        "currentVersion": (
            status.get("currentVersion") if isinstance(status, dict) else None
        ),
        "deprecatedVersions": (
            status.get("deprecatedVersions", []) if isinstance(status, dict) else []
        ),
        "versionCount": status.get("versionCount") if isinstance(status, dict) else None,
        "conditions": worker_deployment_conditions(status),
    }


def _create_or_patch(read: Any, create: Any, patch: Any) -> str:
    try:
        read()
    except Exception as error:
        if _api_status(error) == 404:
            create()
            return "created"
        raise
    patch()
    return "patched"


def _api_status(error: Exception) -> int | None:
    status = getattr(error, "status", None)
    if isinstance(status, int):
        return status
    if isinstance(status, str) and status.isdigit():
        return int(status)
    return None


def object_metadata_summary(item: Any) -> dict[str, Any]:
    metadata = getattr(item, "metadata", None)
    labels = getattr(metadata, "labels", None)
    annotations = getattr(metadata, "annotations", None)
    return {
        "name": getattr(metadata, "name", None),
        "namespace": getattr(metadata, "namespace", None),
        "labels": labels if isinstance(labels, dict) else {},
        "annotations": annotations if isinstance(annotations, dict) else {},
    }


def deployment_conditions(status: Any) -> list[dict[str, Any]]:
    conditions = getattr(status, "conditions", None)
    if not conditions:
        return []
    return [
        {
            "type": getattr(condition, "type", None),
            "status": getattr(condition, "status", None),
            "reason": getattr(condition, "reason", None),
            "message": getattr(condition, "message", None),
        }
        for condition in conditions
    ]


def external_secret_condition(condition: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": condition.get("type"),
        "status": condition.get("status"),
        "reason": condition.get("reason"),
        "message": condition.get("message"),
        "lastTransitionTime": condition.get("lastTransitionTime"),
    }


def worker_deployment_conditions(status: Any) -> list[dict[str, Any]]:
    conditions = status.get("conditions") if isinstance(status, dict) else None
    if not isinstance(conditions, list):
        return []
    return [
        {
            "type": condition.get("type"),
            "status": condition.get("status"),
            "reason": condition.get("reason"),
            "message": condition.get("message"),
            "lastTransitionTime": condition.get("lastTransitionTime"),
            "observedGeneration": condition.get("observedGeneration"),
        }
        for condition in conditions
        if isinstance(condition, dict)
    ]


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
            "The kubernetes package is required for the registry operator."
        ) from error
    return client


def _config_module() -> Any:
    try:
        from kubernetes import config
    except ImportError as error:
        raise RuntimeError(
            "The kubernetes package is required for the registry operator."
        ) from error
    return config
