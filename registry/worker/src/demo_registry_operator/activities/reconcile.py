from __future__ import annotations

from typing import Any

from temporalio import activity


@activity.defn
def reconcile_project(demo_project: dict[str, Any]) -> dict[str, Any]:
    """Plan project reconciliation.

    This is a non-mutating placeholder. Future versions will reconcile
    Kubernetes, AWS, Temporal Cloud, Traefik, and registry status.
    """
    metadata = demo_project.get("metadata", {})
    spec = demo_project.get("spec", {})
    derived = spec.get("derived", {})
    project_name = metadata.get("name", "unknown")

    activity.logger.info("Planning reconcile for DemoProject %s", project_name)
    return {
        "project": project_name,
        "phase": "Planned",
        "message": "Registry operator scaffold has not enabled mutations yet.",
        "desired": {
            "host": derived.get("host"),
            "kubernetesNamespace": derived.get("kubernetesNamespace"),
            "temporalNamespace": derived.get("temporalNamespace"),
        },
    }


@activity.defn
def plan_rollout(request: dict[str, Any]) -> dict[str, Any]:
    project_name = request.get("project", "unknown")
    source_commit_sha = request.get("sourceCommitSha")
    activity.logger.info(
        "Planning candidate rollout for project=%s sourceCommitSha=%s",
        project_name,
        source_commit_sha,
    )
    return {
        "project": project_name,
        "phase": "CandidatePlanned",
        "sourceCommitSha": source_commit_sha,
        "candidate": request.get("candidate", {}),
        "smokeCheck": request.get("smokeCheck"),
    }


@activity.defn
def run_smoke_check(plan: dict[str, Any]) -> dict[str, Any]:
    smoke_check = plan.get("smokeCheck")
    if not smoke_check:
        return {
            "ok": True,
            "skipped": True,
            "message": "No smoke check configured.",
        }

    return {
        "ok": True,
        "skipped": False,
        "message": "Smoke check execution is not implemented yet.",
        "smokeCheck": smoke_check,
    }


@activity.defn
def promote_candidate(plan: dict[str, Any]) -> dict[str, Any]:
    project_name = plan.get("project", "unknown")
    activity.logger.info("Planning candidate promotion for project=%s", project_name)
    return {
        "project": project_name,
        "phase": "PromotionPlanned",
        "message": "Traefik promotion is not implemented yet.",
        "candidate": plan.get("candidate", {}),
    }
