"""Pure candidate-renderability planning, shared by apply and the revision planner.

This module is intentionally neutral: it imports only ``resources`` (pure rendering
helpers) and must NOT import ``activities/reconcile.py`` or ``activities/revision.py``.
Both of those import *this* module so the planner and ``apply_demo_project_resources``
agree on exactly when a candidate is renderable (single source of truth).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from demo_registry_operator import resources


def images_from_build_status(build_status: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(build_status, dict):
        return {}
    images = build_status.get("images")
    if not isinstance(images, dict):
        return {}
    return {
        str(build_name): str(image)
        for build_name, image in images.items()
        if isinstance(build_name, str) and isinstance(image, str) and image
    }


def external_resource_blocker(
    external_resources: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(external_resources, dict):
        return None

    temporal_cloud = external_resources.get("temporalCloud")
    if isinstance(temporal_cloud, dict) and temporal_cloud.get("phase") == "Error":
        reason = str(temporal_cloud.get("reason") or "TemporalCloudError")
        return {
            "phase": "Error",
            "reason": reason,
            "message": temporal_cloud.get(
                "message",
                "Temporal Cloud external resource reconciliation failed.",
            ),
            "temporalCloud": {
                key: temporal_cloud.get(key)
                for key in ("phase", "reason", "message", "auth", "namespace")
                if key in temporal_cloud
            },
        }

    secrets_manager = external_resources.get("secretsManager")
    if not isinstance(secrets_manager, dict):
        return None

    phase = secrets_manager.get("phase")
    if phase not in {"ValuesRequired", "Error"}:
        return None

    blocking = blocking_secret_statuses(secrets_manager)
    return {
        "phase": phase,
        "reason": (
            "SecretValuesRequired"
            if phase == "ValuesRequired"
            else "SecretVerificationFailed"
        ),
        "message": (
            "Required AWS Secrets Manager values are not ready; runtime "
            "workloads were not deployed."
        ),
        "secretsManager": {
            "phase": phase,
            "blocking": blocking,
        },
    }


def blocking_secret_statuses(secrets_manager: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        secrets_manager.get("temporalBootstrap"),
        secrets_manager.get("temporalCredentials"),
        *(
            secrets_manager.get("userManaged")
            if isinstance(secrets_manager.get("userManaged"), list)
            else []
        ),
    ]
    blocking = []
    for secret in candidates:
        if not isinstance(secret, dict) or secret.get("required") is False:
            continue
        if secret.get("phase") in {"Ready", "NotRequired"}:
            continue
        blocking.append(
            {
                key: secret.get(key)
                for key in (
                    "path",
                    "phase",
                    "message",
                    "missingProperties",
                    "managedBy",
                    "components",
                )
                if key in secret
            }
        )
    return blocking


@dataclass(frozen=True)
class CandidateRenderability:
    ready: bool
    phase: str  # "WaitingForImages" | "BuildFailed" | "ExternalResourceBlocked" | "Ready"
    message: str
    rollout_version: str | None = None
    images: dict[str, str] | None = None
    runtime_namespace: str | None = None


def candidate_renderability(
    demo_project: dict[str, Any],
    external_resources: dict[str, Any] | None,
    build_status: dict[str, Any] | None,
) -> CandidateRenderability:
    """Single source of truth for whether candidate workloads can be rendered."""
    image_overrides = images_from_build_status(build_status) or (
        resources.parse_image_overrides(demo_project)
    )
    if build_status and build_status.get("phase") == "Error":
        return CandidateRenderability(False, "BuildFailed", "Image build failed.")
    missing = resources.missing_image_overrides(demo_project, image_overrides)
    if missing:
        return CandidateRenderability(
            False, "WaitingForImages", f"Missing images: {sorted(missing)}"
        )
    block = external_resource_blocker(external_resources)
    if block is not None:
        return CandidateRenderability(
            False, "ExternalResourceBlocked", str(block)
        )
    version = resources.candidate_version(demo_project, image_overrides)
    return CandidateRenderability(
        True,
        "Ready",
        "Candidate renderable.",
        rollout_version=version,
        images=image_overrides,
        runtime_namespace=resources.candidate_runtime_namespace(
            demo_project, image_overrides
        ),
    )
