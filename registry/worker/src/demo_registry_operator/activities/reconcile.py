from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.kube import apply_resources, patch_demo_project_status
from demo_registry_operator.resources import (
    base_resources,
    build_reconcile_status,
    missing_image_overrides,
    parse_image_overrides,
    promotion_resources,
    project_host,
    project_name,
    runtime_resources,
    spec_of,
)


@activity.defn
def reconcile_project(demo_project: dict[str, Any]) -> dict[str, Any]:
    """Reconcile one DemoProject.

    The default mode is non-mutating `plan`. Set
    `DEMO_REGISTRY_RECONCILE_MODE=apply` in the operator deployment to create or
    patch Kubernetes resources.
    """
    config = WorkerConfig.from_env()
    name = project_name(demo_project)
    image_overrides = parse_image_overrides(demo_project)
    missing_images = missing_image_overrides(demo_project, image_overrides)
    dry_run = config.reconcile_mode != "apply"

    resources = base_resources(demo_project)
    smoke_check = spec_of(demo_project).get("smokeCheck")
    if not missing_images:
        resources.extend(
            runtime_resources(
                demo_project,
                image_overrides,
                include_routes=not bool(smoke_check),
            )
        )

    activity.logger.info(
        "Reconciling DemoProject project=%s mode=%s resources=%s missing_images=%s",
        name,
        config.reconcile_mode,
        len(resources),
        missing_images,
    )

    try:
        applied = apply_resources(resources, dry_run=dry_run)
        if config.reconcile_mode == "plan":
            phase = "Planned"
            message = "Reconcile plan rendered without mutating the cluster."
        elif missing_images:
            phase = "WaitingForImages"
            message = "Base resources reconciled; waiting for immutable image overrides."
        elif spec_of(demo_project).get("smokeCheck"):
            phase = "SmokeCheckPending"
            message = "Runtime resources reconciled; smoke check is ready to run."
        else:
            phase = "Ready"
            message = "Runtime resources reconciled."

        status = build_reconcile_status(
            demo_project,
            phase=phase,
            message=message,
            mode=config.reconcile_mode,
            applied_resources=applied,
            image_overrides=image_overrides,
            missing_images=missing_images,
        )
        patch_status(name, status, dry_run=dry_run)
        return status
    except Exception as error:
        activity.logger.exception("DemoProject reconcile failed project=%s", name)
        status = build_reconcile_status(
            demo_project,
            phase="Error",
            message="Reconcile failed.",
            mode=config.reconcile_mode,
            applied_resources=[],
            image_overrides=image_overrides,
            missing_images=missing_images,
            error=str(error),
        )
        patch_status(name, status, dry_run=dry_run)
        return status


@activity.defn
def plan_rollout(request: dict[str, Any]) -> dict[str, Any]:
    project_name = request.get("project", "unknown")
    source_commit_sha = request.get("sourceCommitSha")
    demo_project = request.get("demoProject")
    activity.logger.info(
        "Planning candidate rollout for project=%s sourceCommitSha=%s",
        project_name,
        source_commit_sha,
    )
    host = request.get("host")
    if not isinstance(host, str) and isinstance(demo_project, dict):
        host = project_host(demo_project)

    return {
        "project": project_name,
        "phase": "CandidatePlanned",
        "sourceCommitSha": source_commit_sha,
        "host": host,
        "demoProject": demo_project,
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

    url = smoke_check_url(plan, smoke_check)
    expected_status = int(smoke_check.get("expectedStatus", 200))
    timeout = int(smoke_check.get("timeoutSeconds", 10))
    checked_at = datetime.now(UTC).isoformat()

    try:
        request = Request(url, headers={"User-Agent": "demo-registry-operator/0"})
        with urlopen(request, timeout=timeout) as response:
            actual_status = int(response.status)
    except HTTPError as error:
        actual_status = int(error.code)
    except (OSError, URLError) as error:
        return {
            "ok": False,
            "skipped": False,
            "url": url,
            "checkedAt": checked_at,
            "message": f"Smoke check request failed: {error}",
            "smokeCheck": smoke_check,
        }

    ok = actual_status == expected_status
    return {
        "ok": ok,
        "skipped": False,
        "url": url,
        "expectedStatus": expected_status,
        "actualStatus": actual_status,
        "checkedAt": checked_at,
        "message": "Smoke check passed." if ok else "Smoke check status mismatch.",
        "smokeCheck": smoke_check,
    }


@activity.defn
def promote_candidate(plan: dict[str, Any]) -> dict[str, Any]:
    project_name = plan.get("project", "unknown")
    config = WorkerConfig.from_env()
    demo_project = plan.get("demoProject")
    applied = []
    if isinstance(demo_project, dict):
        applied = apply_resources(
            promotion_resources(demo_project),
            dry_run=config.reconcile_mode != "apply",
        )
    activity.logger.info("Promoting candidate for project=%s", project_name)
    return {
        "project": project_name,
        "phase": "Promoted",
        "message": "Candidate passed smoke check; active route resources were promoted.",
        "candidate": plan.get("candidate", {}),
        "appliedResources": applied,
    }


@activity.defn
def record_project_status(request: dict[str, Any]) -> dict[str, Any]:
    demo_project = request.get("demoProject")
    status = request.get("status")
    if not isinstance(demo_project, dict) or not isinstance(status, dict):
        raise ValueError("record_project_status requires demoProject and status objects")

    config = WorkerConfig.from_env()
    name = project_name(demo_project)
    patch_status(name, status, dry_run=config.reconcile_mode != "apply")
    return status


def smoke_check_url(plan: dict[str, Any], smoke_check: dict[str, Any]) -> str:
    if isinstance(smoke_check.get("url"), str):
        return smoke_check["url"]

    candidate = plan.get("candidate")
    if isinstance(candidate, dict) and isinstance(candidate.get("url"), str):
        return candidate["url"]

    host = plan.get("host")
    if not isinstance(host, str) and isinstance(plan.get("demoProject"), dict):
        host = project_host(plan["demoProject"])
    if not isinstance(host, str):
        host = f"{plan.get('project', 'unknown')}.tmprl-demo.cloud"

    path = smoke_check.get("path", "/")
    return f"https://{host}{path}"


def patch_status(name: str, status: dict[str, Any], *, dry_run: bool) -> None:
    try:
        patch_demo_project_status(name, status, dry_run=dry_run)
    except Exception:
        activity.logger.exception("DemoProject status patch failed project=%s", name)
