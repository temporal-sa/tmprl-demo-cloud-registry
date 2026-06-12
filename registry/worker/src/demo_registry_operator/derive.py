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
