"""Single source of truth for values derived from a project name.

These were previously precomputed by ``scripts/generate_projects.py`` and baked
into a ``spec.derived`` block on the DemoProject CR. The operator now computes
them at reconcile time so the project config (the CR the contributor authors) is
the only source of truth. Keep this module dependency-free so it can be imported
from activities and the validator alike.
"""

from __future__ import annotations

import hashlib

# Uniform platform prefix. k8s namespaces and derived Temporal namespaces use the
# dashed form; Secrets Manager / ECR paths use the slashable root.
NAMESPACE_PREFIX = "tmprl-dem-cld-"
TEMPORAL_NAMESPACE_PREFIX = "tmprl-dem-cld-"
IMAGE_REPOSITORY_PREFIX = "tmprl-dem-cld"
SECRET_PREFIX_ROOT = "tmprl-dem-cld"
BASE_DOMAIN = "tmprl-demo.cloud"

# The operator's Temporal task queue. Colored per cluster so two colors sharing
# one Temporal namespace never cross-route (registry-management-<color>); unset
# color = the original uncolored queue.
REGISTRY_TASK_QUEUE_BASE = "registry-management"

# Ephemeral "crashable" workspaces are per-(user, demo) clones of a demo's
# Kubernetes namespace. They get their own namespace + subdomain so they never
# collide with the always-on demo. The id is a short deterministic digest so the
# same user clicking the same demo always maps to the same workspace.
CRASHABLE_NAMESPACE_PREFIX = "tmprl-dem-cld-crash-"
CRASHABLE_SUBDOMAIN_PREFIX = "crash-"
CRASHABLE_WORKSPACE_ID_LENGTH = 10


# Kubernetes namespaces cap at 63 chars; Temporal Cloud namespaces (excluding the
# ``.<account>`` suffix) cap at 39. Longer names are hash-truncated deterministically.
KUBERNETES_NAMESPACE_MAX_LENGTH = 63
TEMPORAL_NAMESPACE_MAX_LENGTH = 39



def _prefixed_truncated(prefix: str, project_name: str, limit: int) -> str:
    raw = f"{prefix}{project_name}"
    if len(raw) <= limit:
        return raw

    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
    stem = raw[: limit - len(digest) - 1].rstrip("-")
    return f"{stem}-{digest}"


def kubernetes_namespace(project_name: str) -> str:
    return _prefixed_truncated(
        NAMESPACE_PREFIX, project_name, KUBERNETES_NAMESPACE_MAX_LENGTH
    )


def temporal_namespace(project_name: str) -> str:
    return _prefixed_truncated(
        TEMPORAL_NAMESPACE_PREFIX, project_name, TEMPORAL_NAMESPACE_MAX_LENGTH
    )


def secret_prefix(project_name: str) -> str:
    return f"{SECRET_PREFIX_ROOT}/{project_name}"


def image_repository_prefix() -> str:
    return IMAGE_REPOSITORY_PREFIX


def host(subdomain: str | None, project_name: str) -> str:
    return f"{subdomain or project_name}.{BASE_DOMAIN}"


# The Tofu-managed swing record (live.tmprl-demo.cloud A ALIAS -> active color's
# Traefik ELB). The *.tmprl-demo.cloud wildcard CNAMEs to it, so every demo host
# follows the active color atomically without a per-demo DNS record.
LIVE_SWING_HOST = f"live.{BASE_DOMAIN}"


def demo_project_workflow_id(project_name: str, cluster: str | None = None) -> str:
    """Entity workflow id for a DemoProject, optionally scoped to a cluster/color.

    Producers (watch signal-with-start) and consumers (canary signal/describe)
    MUST derive the id here so every site computes it identically; a mismatch
    silently misroutes signals to a different or nonexistent workflow. Unset
    cluster yields the original single-cluster id verbatim.
    """
    if cluster:
        return f"demo-project/{cluster}/{project_name}"
    return f"demo-project/{project_name}"


def instruqt_course_workflow_id(course_name: str, cluster: str | None = None) -> str:
    """Entity workflow id for an InstruqtCourse; see demo_project_workflow_id."""
    if cluster:
        return f"instruqt-course/{cluster}/{course_name}"
    return f"instruqt-course/{course_name}"


def catalog_onboarding_workflow_id(owner_email: str, cluster: str | None = None) -> str:
    """One onboarding per user, scoped to a cluster/color. The catalog web server
    (registry/web/server.py) builds the same id; keep them identical."""
    email = owner_email.strip().lower()
    if cluster:
        return f"catalog-onboarding/{cluster}/{email}"
    return f"catalog-onboarding/{email}"


def namespace_readers_sync_workflow_id() -> str:
    """Singleton id for the namespace readers sync. Deliberately not cluster-scoped:
    the readers group is account-wide, so blue and green share one run."""
    return "namespace-readers/sync"


def namespace_read_access_workflow_id(email: str) -> str:
    """Per-user id for the catalog's self-service grant. The catalog web server
    (registry/web/server.py) builds the same id; keep them identical."""
    return f"namespace-readers/grant/{email.strip().lower()}"


def registry_task_queue(cluster: str | None = None) -> str:
    """The operator's task queue, colored per cluster. Single source shared by the
    worker config (config.py) and the self-rendered operator/web env (resources.
    platform_env_vars) so a colored operator and its self-managed catalog web always
    poll the same registry-management-<color> queue; unset cluster = uncolored."""
    if cluster:
        return f"{REGISTRY_TASK_QUEUE_BASE}-{cluster}"
    return REGISTRY_TASK_QUEUE_BASE


def crashable_workspace_id(owner_email: str, project_name: str) -> str:
    """Short deterministic id for one user's crashable clone of one demo.

    Same (user, demo) always yields the same id so the entity workflow, its
    namespace, and its host are stable across repeated "Create" clicks.
    """

    seed = f"{owner_email.strip().lower()}\n{project_name}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return digest[:CRASHABLE_WORKSPACE_ID_LENGTH]


def crashable_workspace_namespace(workspace_id: str) -> str:
    return _prefixed_truncated(
        CRASHABLE_NAMESPACE_PREFIX, workspace_id, KUBERNETES_NAMESPACE_MAX_LENGTH
    )


def crashable_workspace_subdomain(workspace_id: str) -> str:
    return f"{CRASHABLE_SUBDOMAIN_PREFIX}{workspace_id}"


def crashable_workspace_host(workspace_id: str) -> str:
    return host(crashable_workspace_subdomain(workspace_id), workspace_id)


def crashable_workspace_task_queue(base_task_queue: str, workspace_id: str) -> str:
    return f"{base_task_queue}-{workspace_id}"
