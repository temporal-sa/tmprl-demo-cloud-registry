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
