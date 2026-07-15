"""Render one demo's Kubernetes environment into an ephemeral, isolated clone.

Pure functions only (no I/O) so this stays import-safe and deterministic. The
clone reuses the existing ``resources`` builders: we deep-copy the DemoProject CR,
point its ingress at a per-workspace subdomain, inject the per-workspace task
queue into the worker / temporal-access components, then render the same runtime
resource set the reconciler applies (namespace, RBAC, ExternalSecrets, services,
deployments, worker controller, ingress, certificate) pinned to the workspace
namespace.

Only Kubernetes objects are produced: no AWS or Temporal Cloud resources are
created, and the ExternalSecrets still reference the demo's existing Secrets
Manager paths (derived from the unchanged project name).
"""

from __future__ import annotations

import copy
from typing import Any

from demo_registry_operator import derive, resources


def build_workspace_manifests(
    demo_project: dict[str, Any],
    images: dict[str, str],
    workspace_id: str,
) -> tuple[list[dict[str, Any]], str, str, str, list[str]]:
    """Render the ordered manifests for one crashable workspace.

    Returns ``(ordered_resources, task_queue, host, namespace, deployment_names)``.
    """
    registry = _dict(_dict(demo_project.get("spec")).get("registry"))
    crashable = _dict(registry.get("crashable"))
    task_queue_env = crashable.get("taskQueueEnv")
    base_task_queue = crashable.get("baseTaskQueue")

    # Clone the demo and point its ingress at the per-workspace subdomain BEFORE
    # rendering, so project_host(cloned) — and every host/cert/route derived from
    # it — resolves to the workspace host instead of the always-on demo host.
    cloned = copy.deepcopy(demo_project)
    cloned_spec = cloned.get("spec")
    if not isinstance(cloned_spec, dict):
        cloned_spec = {}
        cloned["spec"] = cloned_spec
    ingress = cloned_spec.get("ingress")
    ingress = dict(ingress) if isinstance(ingress, dict) else {}
    ingress["subdomain"] = derive.crashable_workspace_subdomain(workspace_id)
    cloned_spec["ingress"] = ingress

    host = resources.project_host(cloned)
    namespace = derive.crashable_workspace_namespace(workspace_id)
    task_queue = derive.crashable_workspace_task_queue(
        base_task_queue
        if isinstance(base_task_queue, str) and base_task_queue
        else resources.project_name(cloned),
        workspace_id,
    )

    # The isolation seam: point every worker / temporal-access component at the
    # workspace task queue via a literal env override the workload renders.
    if isinstance(task_queue_env, str) and task_queue_env:
        _inject_task_queue_env(cloned, task_queue_env, task_queue)

    # Namespace FIRST (the single cleanup boundary), then the ServiceAccounts/RBAC
    # the workloads reference, then ExternalSecrets, then Services + Deployments +
    # worker-controller resources, then IngressRoutes + middlewares + Certificate.
    ordered: list[dict[str, Any]] = [
        _namespace_manifest(cloned, namespace, workspace_id),
    ]
    ordered.extend(resources.rbac_resources(cloned, namespace=namespace))
    ordered.extend(resources.runtime_secret_resources(cloned, namespace=namespace))
    ordered.extend(
        resources.runtime_workload_resources(
            cloned,
            images,
            include_routes=True,
            external_resources=None,
            namespace=namespace,
        )
    )

    deployment_names = [
        manifest["metadata"]["name"]
        for manifest in ordered
        if manifest.get("kind") == "Deployment"
    ]
    return ordered, task_queue, host, namespace, deployment_names


def _namespace_manifest(
    cloned: dict[str, Any],
    namespace: str,
    workspace_id: str,
) -> dict[str, Any]:
    manifest = resources.namespace_resource(cloned, namespace=namespace)
    labels = dict(manifest["metadata"].get("labels", {}))
    labels["platform.temporal.io/managed-by"] = "registry-operator"
    labels["platform.temporal.io/crashable-workspace"] = workspace_id
    manifest["metadata"]["labels"] = labels
    return manifest


def _inject_task_queue_env(
    cloned: dict[str, Any],
    task_queue_env: str,
    task_queue: str,
) -> None:
    components = _dict(_dict(cloned.get("spec")).get("components"))
    for component in components.values():
        if not isinstance(component, dict):
            continue
        if not resources.component_receives_default_temporal_env(component):
            continue
        existing = component.get("env")
        env = [
            item
            for item in (existing if isinstance(existing, list) else [])
            if not (isinstance(item, dict) and item.get("name") == task_queue_env)
        ]
        env.append({"name": task_queue_env, "value": task_queue})
        component["env"] = env


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
