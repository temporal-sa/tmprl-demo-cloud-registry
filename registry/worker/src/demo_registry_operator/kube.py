from __future__ import annotations

import base64
from collections.abc import Callable
from typing import Any

DEMO_PROJECT_GROUP = "registry.tmprl-demo.cloud"
DEMO_PROJECT_VERSION = "v1alpha1"
DEMO_PROJECT_PLURAL = "demoprojects"
DEMO_PROJECT_REVISION_GROUP = "registry.tmprl-demo.cloud"
DEMO_PROJECT_REVISION_VERSION = "v1alpha1"
DEMO_PROJECT_REVISION_PLURAL = "demoprojectrevisions"
DEMO_PROJECT_REVISION_KIND = "DemoProjectRevision"
INSTRUQT_COURSE_PLURAL = "instruqtcourses"
JSON_MERGE_PATCH_CONTENT_TYPE = "application/merge-patch+json"
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
TEMPORAL_WORKER_RESOURCE_TEMPLATE_PLURAL = "workerresourcetemplates"
KEDA_GROUP = "keda.sh"
KEDA_VERSION = "v1alpha1"
KEDA_SCALED_OBJECT_PLURAL = "scaledobjects"

CUSTOM_RESOURCE_PLURALS = {
    ("cert-manager.io", "v1", "Certificate"): "certificates",
    (EXTERNAL_SECRETS_GROUP, EXTERNAL_SECRETS_VERSION, "ExternalSecret"): (
        EXTERNAL_SECRET_PLURAL
    ),
    (
        FLUX_SOURCE_GROUP,
        FLUX_SOURCE_VERSION,
        "GitRepository",
    ): FLUX_GIT_REPOSITORY_PLURAL,
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
    (
        TEMPORAL_WORKER_CONTROLLER_GROUP,
        TEMPORAL_WORKER_CONTROLLER_VERSION,
        "WorkerResourceTemplate",
    ): TEMPORAL_WORKER_RESOURCE_TEMPLATE_PLURAL,
    (KEDA_GROUP, KEDA_VERSION, "ScaledObject"): KEDA_SCALED_OBJECT_PLURAL,
    # Traefik v2 (traefik.containo.us) is served on the pet; v3 (traefik.io) on the
    # blue/green stack. apply_custom_resource looks up the plural by the emitted
    # (group, version, kind), so BOTH groups must map or a color's applies raise
    # "unsupported custom resource". Keep both — the operator emits whichever group
    # matches the cluster it runs on (see resources.traefik_api_version).
    ("traefik.containo.us", "v1alpha1", "IngressRoute"): "ingressroutes",
    ("traefik.containo.us", "v1alpha1", "Middleware"): "middlewares",
    ("traefik.io", "v1alpha1", "IngressRoute"): "ingressroutes",
    ("traefik.io", "v1alpha1", "Middleware"): "middlewares",
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
        "ownerReferences": _owner_reference_summaries(metadata),
    }


def _owner_reference_summaries(metadata: Any) -> list[dict[str, Any]]:
    owner_references = getattr(metadata, "owner_references", None)
    if not owner_references:
        return []
    summaries = []
    for ref in owner_references:
        summaries.append(
            {
                "apiVersion": getattr(ref, "api_version", None),
                "kind": getattr(ref, "kind", None),
                "name": getattr(ref, "name", None),
                "uid": getattr(ref, "uid", None),
                "controller": getattr(ref, "controller", None),
                "blockOwnerDeletion": getattr(ref, "block_owner_deletion", None),
            }
        )
    return summaries


def read_deployment_owner_references(
    namespace: str, name: str
) -> list[dict[str, Any]] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        deployment = client.AppsV1Api().read_namespaced_deployment(name, namespace)
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return _owner_reference_summaries(getattr(deployment, "metadata", None))


def read_service_owner_references(
    namespace: str, name: str
) -> list[dict[str, Any]] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        service = client.CoreV1Api().read_namespaced_service(name, namespace)
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return _owner_reference_summaries(getattr(service, "metadata", None))


def list_namespaces(*, label_selector: str | None = None) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CoreV1Api().list_namespace(label_selector=label_selector)
    summaries = [
        object_metadata_summary(item) for item in getattr(response, "items", [])
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
        # None marks a field the merge patch must clear (e.g. a WorkerDeployment's
        # replicas once KEDA owns scaling); a create simply omits it.
        create=lambda: custom.create_namespaced_custom_object(
            group,
            version,
            namespace,
            plural,
            _without_none(resource),
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
    return (
        [f for f in finalizers if isinstance(f, str)]
        if isinstance(finalizers, list)
        else []
    )


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
    finalizers = [
        f for f in demo_project_finalizers(obj) if f != DEMO_PROJECT_FINALIZER
    ]
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


def _content_type_api_client(content_type: str) -> Any:
    """Return an ApiClient that forces ``content_type`` for the request.

    The pinned kubernetes client's generated patch methods do NOT accept a
    ``_content_type`` kwarg (passing one raises ApiTypeError), and the built-in
    (Core/Apps) patch methods negotiate ``application/json-patch+json`` by
    default — which cannot carry a merge-patch dict body. Override the
    content-type negotiation on a dedicated client instead of relying on the
    per-method default.
    """
    api_client = _client_module().ApiClient()
    api_client.select_header_content_type = (
        lambda _content_types, *_request_args: content_type
    )
    return api_client


def patch_demo_project_status(
    name: str, status: dict[str, Any], *, dry_run: bool
) -> None:
    if dry_run:
        return
    # JSON merge patch: keys absent from `status` are preserved by the server, so a
    # child mid-rollout patch and the parent's end-of-reconcile write don't clobber
    # each other. Force the content type explicitly; do not rely on the client default.
    _patch_demo_project_status_raw(
        name,
        {"status": status},
        content_type=JSON_MERGE_PATCH_CONTENT_TYPE,
    )


def _patch_demo_project_status_raw(
    name: str,
    body: dict[str, Any],
    *,
    content_type: str,
) -> None:
    client = _client_module()
    load_kubernetes_config()
    client.CustomObjectsApi(
        _content_type_api_client(content_type)
    ).patch_cluster_custom_object_status(
        group=DEMO_PROJECT_GROUP,
        version=DEMO_PROJECT_VERSION,
        plural=DEMO_PROJECT_PLURAL,
        name=name,
        body=body,
    )


# --- DemoProjectRevision (cluster-scoped) ---


def revision_owner_reference(name: str, uid: str) -> dict[str, Any]:
    return {
        "apiVersion": f"{DEMO_PROJECT_REVISION_GROUP}/{DEMO_PROJECT_REVISION_VERSION}",
        "kind": DEMO_PROJECT_REVISION_KIND,
        "name": name,
        "uid": uid,
        "controller": True,
        "blockOwnerDeletion": True,
    }


def read_demo_project_revision(name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        return client.CustomObjectsApi().get_cluster_custom_object(
            DEMO_PROJECT_REVISION_GROUP,
            DEMO_PROJECT_REVISION_VERSION,
            DEMO_PROJECT_REVISION_PLURAL,
            name,
        )
    except Exception as error:  # noqa: BLE001 - translate 404 to None
        if str(getattr(error, "status", None)) == "404":
            return None
        raise


def list_demo_project_revisions(
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CustomObjectsApi().list_cluster_custom_object(
        group=DEMO_PROJECT_REVISION_GROUP,
        version=DEMO_PROJECT_REVISION_VERSION,
        plural=DEMO_PROJECT_REVISION_PLURAL,
        label_selector=label_selector,
    )
    items = response.get("items") if isinstance(response, dict) else None
    return items if isinstance(items, list) else []


def apply_demo_project_revision(
    resource: dict[str, Any],
    *,
    dry_run: bool,
) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    custom = client.CustomObjectsApi()
    name = resource["metadata"]["name"]
    return _create_or_patch(
        read=lambda: custom.get_cluster_custom_object(
            DEMO_PROJECT_REVISION_GROUP,
            DEMO_PROJECT_REVISION_VERSION,
            DEMO_PROJECT_REVISION_PLURAL,
            name,
        ),
        create=lambda: custom.create_cluster_custom_object(
            DEMO_PROJECT_REVISION_GROUP,
            DEMO_PROJECT_REVISION_VERSION,
            DEMO_PROJECT_REVISION_PLURAL,
            resource,
        ),
        patch=lambda: custom.patch_cluster_custom_object(
            DEMO_PROJECT_REVISION_GROUP,
            DEMO_PROJECT_REVISION_VERSION,
            DEMO_PROJECT_REVISION_PLURAL,
            name,
            resource,
        ),
    )


def patch_demo_project_revision_status(
    name: str,
    status: dict[str, Any],
    *,
    dry_run: bool,
) -> None:
    if dry_run:
        return
    _patch_demo_project_revision_status_raw(
        name,
        {"status": status},
        content_type=JSON_MERGE_PATCH_CONTENT_TYPE,
    )


def _patch_demo_project_revision_status_raw(
    name: str,
    body: dict[str, Any],
    *,
    content_type: str,
) -> None:
    client = _client_module()
    load_kubernetes_config()
    client.CustomObjectsApi(
        _content_type_api_client(content_type)
    ).patch_cluster_custom_object_status(
        group=DEMO_PROJECT_REVISION_GROUP,
        version=DEMO_PROJECT_REVISION_VERSION,
        plural=DEMO_PROJECT_REVISION_PLURAL,
        name=name,
        body=body,
    )


def delete_demo_project_revision(
    name: str,
    *,
    propagation_policy: str = "Foreground",
    dry_run: bool = False,
) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().delete_cluster_custom_object(
            DEMO_PROJECT_REVISION_GROUP,
            DEMO_PROJECT_REVISION_VERSION,
            DEMO_PROJECT_REVISION_PLURAL,
            name,
            body=client.V1DeleteOptions(propagation_policy=propagation_policy),
        )
        return "deleting"
    except Exception as error:  # noqa: BLE001 - already gone is success
        if str(getattr(error, "status", None)) == "404":
            return "absent"
        raise


def patch_namespace_owner_references(
    name: str,
    owner_references: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> str:
    return _patch_owner_references(
        lambda body, content_type: (
            _client_module()
            .CoreV1Api(_content_type_api_client(content_type))
            .patch_namespace(name, body)
        ),
        owner_references,
        dry_run=dry_run,
    )


def patch_deployment_owner_references(
    namespace: str,
    name: str,
    owner_references: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> str:
    return _patch_owner_references(
        lambda body, content_type: (
            _client_module()
            .AppsV1Api(_content_type_api_client(content_type))
            .patch_namespaced_deployment(name, namespace, body)
        ),
        owner_references,
        dry_run=dry_run,
    )


def patch_service_owner_references(
    namespace: str,
    name: str,
    owner_references: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> str:
    return _patch_owner_references(
        lambda body, content_type: (
            _client_module()
            .CoreV1Api(_content_type_api_client(content_type))
            .patch_namespaced_service(name, namespace, body)
        ),
        owner_references,
        dry_run=dry_run,
    )


def _patch_owner_references(
    patch: Any,
    owner_references: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> str:
    if dry_run:
        return "planned"
    load_kubernetes_config()
    body = {"metadata": {"ownerReferences": owner_references}}
    patch(body, JSON_MERGE_PATCH_CONTENT_TYPE)
    return "patched"


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


def list_external_secrets(namespace: str) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CustomObjectsApi().list_namespaced_custom_object(
        EXTERNAL_SECRETS_GROUP,
        EXTERNAL_SECRETS_VERSION,
        namespace,
        EXTERNAL_SECRET_PLURAL,
    )
    items = response.get("items", []) if isinstance(response, dict) else []
    return [item for item in items if isinstance(item, dict)]


def list_worker_deployments(
    namespace: str,
    *,
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CustomObjectsApi().list_namespaced_custom_object(
        TEMPORAL_WORKER_CONTROLLER_GROUP,
        TEMPORAL_WORKER_CONTROLLER_VERSION,
        namespace,
        TEMPORAL_WORKER_DEPLOYMENT_PLURAL,
        label_selector=label_selector,
    )
    items = response.get("items", []) if isinstance(response, dict) else []
    return [item for item in items if isinstance(item, dict)]


def scale_worker_deployment(namespace: str, name: str, replicas: int) -> None:
    if replicas < 0:
        raise ValueError("replicas must be non-negative")
    client = _client_module()
    load_kubernetes_config()
    client.CustomObjectsApi().patch_namespaced_custom_object(
        TEMPORAL_WORKER_CONTROLLER_GROUP,
        TEMPORAL_WORKER_CONTROLLER_VERSION,
        namespace,
        TEMPORAL_WORKER_DEPLOYMENT_PLURAL,
        name,
        {"spec": {"replicas": replicas}},
    )


def read_worker_deployment(namespace: str, name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        resource = client.CustomObjectsApi().get_namespaced_custom_object(
            TEMPORAL_WORKER_CONTROLLER_GROUP,
            TEMPORAL_WORKER_CONTROLLER_VERSION,
            namespace,
            TEMPORAL_WORKER_DEPLOYMENT_PLURAL,
            name,
        )
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return resource if isinstance(resource, dict) else None


def delete_worker_deployment(
    namespace: str,
    name: str,
    *,
    dry_run: bool = False,
) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().delete_namespaced_custom_object(
            TEMPORAL_WORKER_CONTROLLER_GROUP,
            TEMPORAL_WORKER_CONTROLLER_VERSION,
            namespace,
            TEMPORAL_WORKER_DEPLOYMENT_PLURAL,
            name,
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def list_connections(
    namespace: str,
    *,
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CustomObjectsApi().list_namespaced_custom_object(
        TEMPORAL_WORKER_CONTROLLER_GROUP,
        TEMPORAL_WORKER_CONTROLLER_VERSION,
        namespace,
        TEMPORAL_CONNECTION_PLURAL,
        label_selector=label_selector,
    )
    items = response.get("items", []) if isinstance(response, dict) else []
    return [item for item in items if isinstance(item, dict)]


def read_connection(namespace: str, name: str) -> dict[str, Any] | None:
    client = _client_module()
    load_kubernetes_config()
    try:
        resource = client.CustomObjectsApi().get_namespaced_custom_object(
            TEMPORAL_WORKER_CONTROLLER_GROUP,
            TEMPORAL_WORKER_CONTROLLER_VERSION,
            namespace,
            TEMPORAL_CONNECTION_PLURAL,
            name,
        )
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return resource if isinstance(resource, dict) else None


def delete_connection(
    namespace: str,
    name: str,
    *,
    dry_run: bool = False,
) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().delete_namespaced_custom_object(
            TEMPORAL_WORKER_CONTROLLER_GROUP,
            TEMPORAL_WORKER_CONTROLLER_VERSION,
            namespace,
            TEMPORAL_CONNECTION_PLURAL,
            name,
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def force_remove_worker_deployment_finalizers(
    namespace: str,
    name: str,
    *,
    dry_run: bool = False,
) -> str:
    return _force_remove_custom_object_finalizers(
        TEMPORAL_WORKER_DEPLOYMENT_PLURAL, namespace, name, dry_run=dry_run
    )


def force_remove_connection_finalizers(
    namespace: str,
    name: str,
    *,
    dry_run: bool = False,
) -> str:
    return _force_remove_custom_object_finalizers(
        TEMPORAL_CONNECTION_PLURAL, namespace, name, dry_run=dry_run
    )


def _force_remove_custom_object_finalizers(
    plural: str,
    namespace: str,
    name: str,
    *,
    dry_run: bool = False,
) -> str:
    """Clear finalizers on a wedged worker-controller CR so teardown can proceed.

    Used only as a last resort when the controller's `temporal.io/delete-protection`
    finalizer cannot clear (its Temporal Cloud cleanup is unreachable). Removing the
    finalizer lets Kubernetes garbage-collect the object; server-side cleanup is
    skipped, which is acceptable because the namespace/credentials are being deleted.
    """
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().patch_namespaced_custom_object(
            TEMPORAL_WORKER_CONTROLLER_GROUP,
            TEMPORAL_WORKER_CONTROLLER_VERSION,
            namespace,
            plural,
            name,
            {"metadata": {"finalizers": []}},
        )
        return "finalizers-removed"
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
        object_metadata_summary(item) for item in getattr(response, "items", [])
    ]
    return [item for item in summaries if item.get("name")]


def scale_deployment(namespace: str, name: str, replicas: int) -> None:
    if replicas < 0:
        raise ValueError("replicas must be non-negative")
    client = _client_module()
    load_kubernetes_config()
    client.AppsV1Api().patch_namespaced_deployment(
        name,
        namespace,
        {"spec": {"replicas": replicas}},
    )


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


def list_deployment_images(namespace: str) -> dict[str, str]:
    """Map each Deployment's platform build to its primary container image.

    One list call. Used to clone a demo's already-running images into a crashable
    workspace without rebuilding. The primary (first) container carries the build
    image; sidecars (e.g. the temporal proxy) are ignored.
    """
    client = _client_module()
    load_kubernetes_config()
    response = client.AppsV1Api().list_namespaced_deployment(namespace)
    images: dict[str, str] = {}
    for item in getattr(response, "items", []):
        metadata = getattr(item, "metadata", None)
        annotations = getattr(metadata, "annotations", None)
        build = (
            annotations.get("platform.temporal.io/build")
            if isinstance(annotations, dict)
            else None
        )
        spec = getattr(item, "spec", None)
        template = getattr(spec, "template", None)
        pod_spec = getattr(template, "spec", None)
        containers = getattr(pod_spec, "containers", None) or []
        if not isinstance(build, str) or not build or not containers:
            continue
        image = getattr(containers[0], "image", None)
        if isinstance(image, str) and image:
            images[build] = image
    return images


def list_worker_deployment_images(namespace: str) -> dict[str, str]:
    """Map each WorkerDeployment's platform build to its template image."""

    images: dict[str, str] = {}
    for item in list_worker_deployments(namespace):
        metadata = item.get("metadata")
        annotations = (
            metadata.get("annotations") if isinstance(metadata, dict) else None
        )
        build = (
            annotations.get("platform.temporal.io/build")
            if isinstance(annotations, dict)
            else None
        )
        spec = item.get("spec")
        template = spec.get("template") if isinstance(spec, dict) else None
        pod_spec = template.get("spec") if isinstance(template, dict) else None
        containers = pod_spec.get("containers") if isinstance(pod_spec, dict) else None
        if not isinstance(build, str) or not build or not isinstance(containers, list):
            continue
        primary = containers[0] if containers else None
        image = primary.get("image") if isinstance(primary, dict) else None
        if isinstance(image, str) and image:
            images[build] = image
    return images


def list_pods(
    namespace: str,
    *,
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    client = _client_module()
    load_kubernetes_config()
    response = client.CoreV1Api().list_namespaced_pod(
        namespace,
        label_selector=label_selector,
    )
    summaries = [
        object_metadata_summary(item) for item in getattr(response, "items", [])
    ]
    return [item for item in summaries if item.get("name")]


def collect_namespace_diagnostics(
    namespace: str,
    *,
    label_selector: str | None = None,
    max_pods: int = 20,
    max_events: int = 200,
    log_tail_lines: int = 500,
    max_log_bytes: int = 64 * 1024,
    max_total_log_bytes: int = 4 * 1024 * 1024,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Collect a bounded, secret-value-free Kubernetes failure snapshot.

    Pod specs are deliberately summarized rather than serialized wholesale so
    environment values and mounted Secret data never enter the artifact. Logs
    can themselves contain sensitive application output, so callers must store
    the returned snapshot in a private destination.
    """
    client = _client_module()
    load_kubernetes_config()
    core = client.CoreV1Api()
    pod_response = core.list_namespaced_pod(
        namespace,
        label_selector=label_selector,
        limit=max_pods,
        _request_timeout=(5, 30),
    )
    pods = sorted(
        list(getattr(pod_response, "items", []) or []),
        key=lambda item: str(getattr(getattr(item, "metadata", None), "name", "")),
    )
    remaining_log_bytes = max_total_log_bytes
    pod_diagnostics: list[dict[str, Any]] = []
    collection_errors: list[dict[str, Any]] = []
    log_entry_count = 0
    log_error_count = 0
    logs_truncated = False

    for index, pod in enumerate(pods):
        pod_summary = pod_diagnostic_summary(pod)
        pod_name = pod_summary.get("name")
        container_statuses = pod_container_statuses_by_name(pod)
        container_logs: list[dict[str, Any]] = []
        for container_name in pod_container_names(pod):
            status = container_statuses.get(container_name, {})
            attempts = [False]
            if int(status.get("restartCount") or 0) > 0:
                attempts.append(True)
            for previous in attempts:
                if remaining_log_bytes <= 0:
                    logs_truncated = True
                    break
                read_limit = min(max_log_bytes, remaining_log_bytes)
                try:
                    raw_log = core.read_namespaced_pod_log(
                        pod_name,
                        namespace,
                        container=container_name,
                        previous=previous,
                        timestamps=True,
                        tail_lines=log_tail_lines,
                        limit_bytes=read_limit,
                        _request_timeout=(5, 30),
                    )
                except Exception as error:
                    log_error_count += 1
                    container_logs.append(
                        {
                            "container": container_name,
                            "stream": "previous" if previous else "current",
                            "error": bounded_text(str(error), 2 * 1024),
                        }
                    )
                    continue

                log_text = raw_log if isinstance(raw_log, str) else str(raw_log or "")
                encoded = log_text.encode("utf-8", errors="replace")
                truncated = len(encoded) > read_limit
                if truncated:
                    encoded = encoded[-read_limit:]
                    log_text = encoded.decode("utf-8", errors="replace")
                remaining_log_bytes -= len(encoded)
                log_entry_count += 1
                container_logs.append(
                    {
                        "container": container_name,
                        "stream": "previous" if previous else "current",
                        "tailLines": log_tail_lines,
                        "truncated": truncated,
                        "bytes": len(encoded),
                        "log": log_text,
                    }
                )
                logs_truncated = logs_truncated or truncated
            if remaining_log_bytes <= 0:
                break
        pod_summary["logs"] = container_logs
        pod_diagnostics.append(pod_summary)
        if progress_callback is not None:
            progress_callback(
                {
                    "namespace": namespace,
                    "phase": "CollectingPodLogs",
                    "pod": pod_name,
                    "completedPods": index + 1,
                    "totalPods": len(pods),
                }
            )

    events: list[dict[str, Any]] = []
    events_truncated = False
    try:
        event_response = core.list_namespaced_event(
            namespace,
            limit=max_events,
            _request_timeout=(5, 30),
        )
        event_items = list(getattr(event_response, "items", []) or [])
        events = [event_diagnostic_summary(item) for item in event_items]
        events.sort(key=lambda item: str(item.get("lastObservedAt") or ""))
        events_truncated = bool(
            getattr(getattr(event_response, "metadata", None), "_continue", None)
        )
    except Exception as error:
        collection_errors.append(
            {
                "resource": "events",
                "error": bounded_text(str(error), 2 * 1024),
            }
        )

    pods_truncated = bool(
        getattr(getattr(pod_response, "metadata", None), "_continue", None)
    )
    return {
        "namespace": namespace,
        "labelSelector": label_selector,
        "pods": pod_diagnostics,
        "events": events,
        "limits": {
            "maxPods": max_pods,
            "maxEvents": max_events,
            "logTailLines": log_tail_lines,
            "maxLogBytesPerStream": max_log_bytes,
            "maxTotalLogBytes": max_total_log_bytes,
        },
        "summary": {
            "podCount": len(pod_diagnostics),
            "eventCount": len(events),
            "logEntryCount": log_entry_count,
            "logErrorCount": log_error_count,
            "podsTruncated": pods_truncated,
            "eventsTruncated": events_truncated,
            "logsTruncated": logs_truncated,
            "partial": bool(
                collection_errors
                or log_error_count
                or pods_truncated
                or events_truncated
                or logs_truncated
            ),
        },
        "collectionErrors": collection_errors,
    }


def pod_diagnostic_summary(pod: Any) -> dict[str, Any]:
    metadata = getattr(pod, "metadata", None)
    spec = getattr(pod, "spec", None)
    status = getattr(pod, "status", None)
    return {
        "name": getattr(metadata, "name", None),
        "uid": getattr(metadata, "uid", None),
        "namespace": getattr(metadata, "namespace", None),
        "createdAt": kubernetes_time(getattr(metadata, "creation_timestamp", None)),
        "labels": getattr(metadata, "labels", None) or {},
        "nodeName": getattr(spec, "node_name", None),
        "serviceAccountName": getattr(spec, "service_account_name", None),
        "containers": pod_container_specs(pod),
        "phase": getattr(status, "phase", None),
        "reason": getattr(status, "reason", None),
        "message": bounded_text(getattr(status, "message", None), 4 * 1024),
        "hostIP": getattr(status, "host_ip", None),
        "podIP": getattr(status, "pod_ip", None),
        "startedAt": kubernetes_time(getattr(status, "start_time", None)),
        "conditions": [
            {
                "type": getattr(condition, "type", None),
                "status": getattr(condition, "status", None),
                "reason": getattr(condition, "reason", None),
                "message": bounded_text(
                    getattr(condition, "message", None), 4 * 1024
                ),
                "lastTransitionTime": kubernetes_time(
                    getattr(condition, "last_transition_time", None)
                ),
            }
            for condition in (getattr(status, "conditions", None) or [])
        ],
        "containerStatuses": list(pod_container_statuses_by_name(pod).values()),
    }


def pod_container_specs(pod: Any) -> list[dict[str, Any]]:
    spec = getattr(pod, "spec", None)
    rendered: list[dict[str, Any]] = []
    for kind, attribute in (
        ("init", "init_containers"),
        ("app", "containers"),
        ("ephemeral", "ephemeral_containers"),
    ):
        for container in (getattr(spec, attribute, None) or []):
            rendered.append(
                {
                    "name": getattr(container, "name", None),
                    "image": getattr(container, "image", None),
                    "kind": kind,
                }
            )
    return rendered


def pod_container_names(pod: Any) -> list[str]:
    names: list[str] = []
    for container in pod_container_specs(pod):
        name = container.get("name")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def pod_container_statuses_by_name(pod: Any) -> dict[str, dict[str, Any]]:
    status = getattr(pod, "status", None)
    rendered: dict[str, dict[str, Any]] = {}
    for kind, attribute in (
        ("init", "init_container_statuses"),
        ("app", "container_statuses"),
        ("ephemeral", "ephemeral_container_statuses"),
    ):
        for container in (getattr(status, attribute, None) or []):
            name = getattr(container, "name", None)
            if not isinstance(name, str) or not name:
                continue
            rendered[name] = {
                "name": name,
                "kind": kind,
                "ready": getattr(container, "ready", None),
                "restartCount": getattr(container, "restart_count", None),
                "image": getattr(container, "image", None),
                "imageID": getattr(container, "image_id", None),
                "containerID": getattr(container, "container_id", None),
                "state": container_state_summary(getattr(container, "state", None)),
                "lastState": container_state_summary(
                    getattr(container, "last_state", None)
                ),
            }
    return rendered


def container_state_summary(state: Any) -> dict[str, Any]:
    if state is None:
        return {}
    waiting = getattr(state, "waiting", None)
    running = getattr(state, "running", None)
    terminated = getattr(state, "terminated", None)
    if waiting is not None:
        return {
            "phase": "waiting",
            "reason": getattr(waiting, "reason", None),
            "message": bounded_text(getattr(waiting, "message", None), 4 * 1024),
        }
    if running is not None:
        return {
            "phase": "running",
            "startedAt": kubernetes_time(getattr(running, "started_at", None)),
        }
    if terminated is not None:
        return {
            "phase": "terminated",
            "exitCode": getattr(terminated, "exit_code", None),
            "signal": getattr(terminated, "signal", None),
            "reason": getattr(terminated, "reason", None),
            "message": bounded_text(
                getattr(terminated, "message", None), 4 * 1024
            ),
            "startedAt": kubernetes_time(getattr(terminated, "started_at", None)),
            "finishedAt": kubernetes_time(
                getattr(terminated, "finished_at", None)
            ),
            "containerID": getattr(terminated, "container_id", None),
        }
    return {}


def event_diagnostic_summary(event: Any) -> dict[str, Any]:
    metadata = getattr(event, "metadata", None)
    involved = getattr(event, "involved_object", None)
    source = getattr(event, "source", None)
    observed_at = (
        getattr(event, "event_time", None)
        or getattr(event, "last_timestamp", None)
        or getattr(metadata, "creation_timestamp", None)
    )
    return {
        "type": getattr(event, "type", None),
        "reason": getattr(event, "reason", None),
        "message": bounded_text(getattr(event, "message", None), 8 * 1024),
        "count": getattr(event, "count", None),
        "firstObservedAt": kubernetes_time(
            getattr(event, "first_timestamp", None)
        ),
        "lastObservedAt": kubernetes_time(observed_at),
        "action": getattr(event, "action", None),
        "reportingController": getattr(event, "reporting_controller", None),
        "source": {
            "component": getattr(source, "component", None),
            "host": getattr(source, "host", None),
        },
        "involvedObject": {
            "apiVersion": getattr(involved, "api_version", None),
            "kind": getattr(involved, "kind", None),
            "namespace": getattr(involved, "namespace", None),
            "name": getattr(involved, "name", None),
            "uid": getattr(involved, "uid", None),
        },
    }


def kubernetes_time(value: Any) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


def bounded_text(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    rendered = str(value)
    return rendered if len(rendered) <= limit else rendered[:limit] + "…"


def delete_pod(
    namespace: str,
    name: str,
    *,
    grace_period_seconds: int = 0,
    dry_run: bool = False,
) -> str:
    if dry_run:
        return "planned"
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CoreV1Api().delete_namespaced_pod(
            name,
            namespace,
            grace_period_seconds=grace_period_seconds,
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def read_job(namespace: str, name: str) -> dict[str, Any] | None:
    """A Job as a plain dict (API field names), or None if it doesn't exist."""

    client = _client_module()
    load_kubernetes_config()
    try:
        job = client.BatchV1Api().read_namespaced_job(name, namespace)
    except Exception as error:
        if _api_status(error) == 404:
            return None
        raise
    return client.ApiClient().sanitize_for_serialization(job)


def create_job(job: dict[str, Any]) -> dict[str, Any]:
    client = _client_module()
    load_kubernetes_config()
    created = client.BatchV1Api().create_namespaced_job(
        job["metadata"]["namespace"], job
    )
    return client.ApiClient().sanitize_for_serialization(created)


def delete_job(namespace: str, name: str) -> str:
    """Deletes a Job and, in the background, its pods and owned objects."""

    client = _client_module()
    load_kubernetes_config()
    try:
        client.BatchV1Api().delete_namespaced_job(
            name, namespace, propagation_policy="Background"
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


def create_secret(secret: dict[str, Any]) -> str:
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CoreV1Api().create_namespaced_secret(
            secret["metadata"]["namespace"], secret
        )
        return "created"
    except Exception as error:
        if _api_status(error) == 409:
            return "exists"
        raise


def read_pod_log(namespace: str, name: str, *, container: str) -> str:
    client = _client_module()
    load_kubernetes_config()
    try:
        return client.CoreV1Api().read_namespaced_pod_log(
            name, namespace, container=container, _request_timeout=(5, 30)
        )
    except Exception as error:
        if _api_status(error) in {400, 404}:
            # Not started yet, or already gone.
            return ""
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
        object_metadata_summary(item) for item in getattr(response, "items", [])
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
        "generation": metadata.get("generation")
        if isinstance(metadata, dict)
        else None,
        "targetVersion": (
            status.get("targetVersion") if isinstance(status, dict) else None
        ),
        "currentVersion": (
            status.get("currentVersion") if isinstance(status, dict) else None
        ),
        "deprecatedVersions": (
            status.get("deprecatedVersions", []) if isinstance(status, dict) else []
        ),
        "versionCount": status.get("versionCount")
        if isinstance(status, dict)
        else None,
        "conditions": worker_deployment_conditions(status),
    }


def _without_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_none(item) for key, item in value.items() if item is not None
        }
    if isinstance(value, list):
        return [_without_none(item) for item in value]
    return value


def list_autoscalers(
    namespace: str,
    *,
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    """Operator-rendered autoscaling objects: ScaledObjects and WRTs.

    WRT-rendered ScaledObjects are owned by their WorkerResourceTemplate and
    carry no project label, so a project label selector skips them.
    """
    client = _client_module()
    load_kubernetes_config()
    custom = client.CustomObjectsApi()
    found: list[dict[str, Any]] = []
    for group, version, plural in (
        (KEDA_GROUP, KEDA_VERSION, KEDA_SCALED_OBJECT_PLURAL),
        (
            TEMPORAL_WORKER_CONTROLLER_GROUP,
            TEMPORAL_WORKER_CONTROLLER_VERSION,
            TEMPORAL_WORKER_RESOURCE_TEMPLATE_PLURAL,
        ),
    ):
        try:
            response = custom.list_namespaced_custom_object(
                group,
                version,
                namespace,
                plural,
                label_selector=label_selector,
            )
        except Exception as error:
            if _api_status(error) == 404:
                continue
            raise
        items = response.get("items", []) if isinstance(response, dict) else []
        found.extend(item for item in items if isinstance(item, dict))
    return found


def delete_autoscaler(namespace: str, kind: str, name: str) -> str:
    plural = {
        "ScaledObject": (KEDA_GROUP, KEDA_VERSION, KEDA_SCALED_OBJECT_PLURAL),
        "WorkerResourceTemplate": (
            TEMPORAL_WORKER_CONTROLLER_GROUP,
            TEMPORAL_WORKER_CONTROLLER_VERSION,
            TEMPORAL_WORKER_RESOURCE_TEMPLATE_PLURAL,
        ),
    }.get(kind)
    if plural is None:
        raise ValueError(f"Unsupported autoscaler kind {kind!r}")
    client = _client_module()
    load_kubernetes_config()
    try:
        client.CustomObjectsApi().delete_namespaced_custom_object(
            plural[0],
            plural[1],
            namespace,
            plural[2],
            name,
        )
        return "deleting"
    except Exception as error:
        if _api_status(error) == 404:
            return "not-found"
        raise


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
