from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.kube import (
    apply_resources,
    delete_namespace,
    patch_demo_project_status,
    read_deployment_status,
    read_external_secret_status,
    remove_demo_project_finalizer,
)
from demo_registry_operator.resources import (
    SOURCE_COMMIT_ANNOTATION,
    base_resources,
    build_reconcile_status,
    catalog_status_configmap,
    is_control_plane_project,
    missing_image_overrides,
    parse_image_overrides,
    promotion_resources,
    project_host,
    project_name,
    project_namespace,
    runtime_secret_resources,
    runtime_workload_resources,
    spec_of,
)


@activity.defn
def reconcile_project(request: dict[str, Any]) -> dict[str, Any]:
    """Reconcile one DemoProject.

    The default mode is non-mutating `plan`. Set
    `DEMO_REGISTRY_RECONCILE_MODE=apply` in the operator deployment to create or
    patch Kubernetes resources.
    """
    demo_project = demo_project_from_request(request)
    external_resources = external_resources_from_request(request)
    build_status = build_status_from_request(request)
    demo_project = demo_project_with_build_source(demo_project, build_status)
    config = WorkerConfig.from_env()
    name = project_name(demo_project)
    image_overrides = images_from_build_status(build_status) or parse_image_overrides(demo_project)
    missing_images = missing_image_overrides(demo_project, image_overrides)
    build_failed = bool(build_status and build_status.get("phase") == "Error")
    external_resource_block = external_resource_blocker(external_resources)
    dry_run = config.reconcile_mode != "apply"

    resources = base_resources(demo_project)
    resources.extend(runtime_secret_resources(demo_project))
    smoke_check = spec_of(demo_project).get("smokeCheck")
    if not missing_images and not build_failed and external_resource_block is None:
        resources.extend(
            runtime_workload_resources(
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
        external_secret_status = external_secret_sync_status(
            applied,
            dry_run=dry_run,
        )
        if config.reconcile_mode == "plan":
            phase = "Planned"
            message = "Reconcile plan rendered without mutating the cluster."
        elif build_failed:
            phase = "Error"
            message = "Image build failed; workload resources were not reconciled."
        elif missing_images:
            phase = "WaitingForImages"
            message = "Base resources and ExternalSecrets reconciled; waiting for immutable images."
        elif external_resource_block:
            phase = "WaitingForExternalResources"
            message = external_resource_block["message"]
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
            external_resources=external_resources,
            build_status=build_status,
            external_secret_status=external_secret_status,
            external_resource_block=external_resource_block,
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
            external_resources=external_resources,
            build_status=build_status,
            error=str(error),
        )
        patch_status(name, status, dry_run=dry_run)
        return status


@activity.defn
def finalize_project(request: dict[str, Any]) -> dict[str, Any]:
    """Tear down a removed project: delete its Kubernetes namespace and external
    resources (Temporal Cloud namespace/SA/key/secret, ECR repos), then remove
    the operator finalizer so Kubernetes can complete the DemoProject deletion.

    Driven by the project file being removed from git (the explicit deletion
    approval). Idempotent and safe to retry."""

    from demo_registry_operator.activities.external_resources import (
        finalize_external_resources,
    )

    demo_project = demo_project_from_request(request)
    config = WorkerConfig.from_env()
    name = project_name(demo_project)
    namespace = project_namespace(demo_project)

    # Never tear down the control plane, even if a finalize somehow reached it.
    if is_control_plane_project(demo_project):
        activity.logger.warning(
            "Refusing to finalize control-plane project=%s", name
        )
        return {
            "phase": "Skipped",
            "project": name,
            "message": "Control-plane project is not finalized.",
        }

    dry_run = config.reconcile_mode != "apply"
    activity.logger.info(
        "Finalizing project=%s namespace=%s dry_run=%s", name, namespace, dry_run
    )

    namespace_action = delete_namespace(namespace, dry_run=dry_run)
    external = (
        finalize_external_resources(demo_project, config)
        if not dry_run
        else {"phase": "Planned"}
    )
    # Remove the finalizer last, so the DemoProject stays present (Terminating)
    # until teardown succeeds; on failure the activity retries and the CR is not
    # prematurely deleted.
    remove_demo_project_finalizer(name, dry_run=dry_run)

    return {
        "phase": "Finalized",
        "project": name,
        "checkedAt": datetime.now(UTC).isoformat(),
        "kubernetesNamespace": {"name": namespace, "action": namespace_action},
        "externalResources": external,
    }


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
        request = Request(url, headers={"User-Agent": "registry-operator/0"})
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
def wait_for_candidate_readiness(plan: dict[str, Any]) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    demo_project = plan.get("demoProject")
    candidate = plan.get("candidate")
    if not isinstance(demo_project, dict) or not isinstance(candidate, dict):
        raise ValueError("wait_for_candidate_readiness requires demoProject and candidate")

    deployments = candidate.get("deployments")
    if not isinstance(deployments, dict) or not deployments:
        return {
            "phase": "Skipped",
            "ok": True,
            "message": "Candidate has no Deployment resources to wait for.",
            "checkedAt": datetime.now(UTC).isoformat(),
            "deployments": {},
        }

    if config.reconcile_mode != "apply":
        return {
            "phase": "Planned",
            "ok": True,
            "message": "Readiness wait skipped because reconcile mode is plan.",
            "checkedAt": datetime.now(UTC).isoformat(),
            "deployments": {
                component: {
                    "name": deployment_name,
                    "phase": "Planned",
                }
                for component, deployment_name in sorted(deployments.items())
                if isinstance(component, str) and isinstance(deployment_name, str)
            },
        }

    namespace = project_namespace(demo_project)
    deadline = time.monotonic() + config.readiness_timeout_seconds
    last_status: dict[str, Any] = {}
    while True:
        last_status = read_candidate_deployments(namespace, deployments)
        if all(deployment.get("ready") for deployment in last_status.values()):
            return {
                "phase": "Ready",
                "ok": True,
                "message": "All candidate Deployments are available.",
                "checkedAt": datetime.now(UTC).isoformat(),
                "deployments": last_status,
            }

        if time.monotonic() >= deadline:
            return {
                "phase": "TimedOut",
                "ok": False,
                "message": "Timed out waiting for candidate Deployments to become available.",
                "checkedAt": datetime.now(UTC).isoformat(),
                "deployments": last_status,
            }

        activity.heartbeat(
            {
                "project": project_name(demo_project),
                "namespace": namespace,
                "deployments": last_status,
            }
        )
        time.sleep(config.readiness_poll_seconds)


@activity.defn
def promote_candidate(plan: dict[str, Any]) -> dict[str, Any]:
    project_name = plan.get("project", "unknown")
    config = WorkerConfig.from_env()
    demo_project = plan.get("demoProject")
    candidate = plan.get("candidate")
    candidate_images = (
        candidate.get("images")
        if isinstance(candidate, dict) and isinstance(candidate.get("images"), dict)
        else None
    )
    applied = []
    if isinstance(demo_project, dict):
        applied = apply_resources(
            promotion_resources(
                demo_project,
                image_overrides=candidate_images,
            ),
            dry_run=config.reconcile_mode != "apply",
        )
    activity.logger.info("Promoting candidate for project=%s", project_name)
    return {
        "project": project_name,
        "phase": "Promoted",
        "message": "Candidate passed smoke check; active route resources were promoted.",
        "candidate": candidate or {},
        "appliedResources": applied,
    }


def read_candidate_deployments(
    namespace: str,
    deployments: dict[str, Any],
) -> dict[str, Any]:
    statuses = {}
    for component, deployment_name in sorted(deployments.items()):
        if not isinstance(component, str) or not isinstance(deployment_name, str):
            continue
        try:
            status = read_deployment_status(namespace, deployment_name)
        except Exception as error:
            statuses[component] = {
                "name": deployment_name,
                "phase": "Missing",
                "ready": False,
                "message": str(error),
            }
            continue

        statuses[component] = {
            **status,
            "phase": "Ready" if deployment_ready(status) else "Waiting",
            "ready": deployment_ready(status),
        }
    return statuses


def deployment_ready(status: dict[str, Any]) -> bool:
    replicas = status.get("replicas")
    desired = int(replicas) if isinstance(replicas, int) else 1
    observed_generation = status.get("observedGeneration")
    generation = status.get("generation")
    if isinstance(generation, int) and isinstance(observed_generation, int):
        if observed_generation < generation:
            return False
    return (
        int(status.get("availableReplicas") or 0) >= desired
        and int(status.get("readyReplicas") or 0) >= desired
        and int(status.get("updatedReplicas") or 0) >= desired
    )


def external_resource_blocker(
    external_resources: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(external_resources, dict):
        return None

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


def external_secret_sync_status(
    applied_resources: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> dict[str, Any] | None:
    external_secrets = [
        resource
        for resource in applied_resources
        if resource.get("kind") == "ExternalSecret"
        and isinstance(resource.get("name"), str)
        and isinstance(resource.get("namespace"), str)
    ]
    if not external_secrets:
        return None

    checked_at = datetime.now(UTC).isoformat()
    if dry_run:
        resources = [
            {
                "name": resource["name"],
                "namespace": resource["namespace"],
                "phase": "Planned",
                "action": resource.get("action", "planned"),
                "ready": False,
                "message": "ExternalSecret would be created or patched.",
            }
            for resource in external_secrets
        ]
        return {
            "phase": "Planned",
            "checkedAt": checked_at,
            "resources": resources,
        }

    resources = []
    for resource in external_secrets:
        name = resource["name"]
        namespace = resource["namespace"]
        try:
            resources.append(read_external_secret_status(namespace, name))
        except Exception as error:
            resources.append(
                {
                    "name": name,
                    "namespace": namespace,
                    "phase": (
                        "Missing"
                        if getattr(error, "status", None) == 404
                        else "Error"
                    ),
                    "ready": False,
                    "message": str(error),
                }
            )

    return {
        "phase": aggregate_external_secret_phase(resources),
        "checkedAt": checked_at,
        "resources": resources,
    }


def aggregate_external_secret_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if phases <= {"Ready"}:
        return "Ready"
    if phases & {"Error", "Missing"}:
        return "Error"
    if "Waiting" in phases:
        return "Waiting"
    if phases <= {"Planned"}:
        return "Planned"
    return "Unknown"


@activity.defn
def record_project_status(request: dict[str, Any]) -> dict[str, Any]:
    demo_project = request.get("demoProject")
    status = request.get("status")
    if not isinstance(demo_project, dict) or not isinstance(status, dict):
        raise ValueError("record_project_status requires demoProject and status objects")

    config = WorkerConfig.from_env()
    name = project_name(demo_project)
    patch_status(name, status, dry_run=config.reconcile_mode != "apply")
    patch_catalog_status(demo_project, status, config)
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


def patch_catalog_status(
    demo_project: dict[str, Any],
    status: dict[str, Any],
    config: WorkerConfig,
) -> None:
    primary = catalog_status_configmap(
        demo_project,
        status,
        namespace=config.catalog_status_namespace,
        name=config.catalog_status_configmap,
    )
    try:
        apply_resources([primary], dry_run=config.reconcile_mode != "apply")
    except Exception:
        activity.logger.exception(
            "Catalog status patch failed project=%s",
            project_name(demo_project),
        )

    if config.catalog_status_mirror_namespace == config.catalog_status_namespace:
        return

    mirror = catalog_status_configmap(
        demo_project,
        status,
        namespace=config.catalog_status_mirror_namespace,
        name=config.catalog_status_configmap,
    )
    try:
        apply_resources([mirror], dry_run=config.reconcile_mode != "apply")
    except Exception:
        activity.logger.exception(
            "Catalog status mirror patch failed project=%s namespace=%s",
            project_name(demo_project),
            config.catalog_status_mirror_namespace,
        )


def demo_project_from_request(request: dict[str, Any]) -> dict[str, Any]:
    demo_project = request.get("demoProject")
    if isinstance(demo_project, dict):
        return demo_project
    return request


def external_resources_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    external_resources = request.get("externalResources")
    if isinstance(external_resources, dict):
        return external_resources
    return None


def build_status_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    build_status = request.get("buildStatus")
    if isinstance(build_status, dict):
        return build_status
    return None


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


def demo_project_with_build_source(
    demo_project: dict[str, Any],
    build_status: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(build_status, dict):
        return demo_project
    source_commit = build_status.get("sourceCommitSha")
    if not isinstance(source_commit, str) or not source_commit:
        return demo_project

    metadata = demo_project.get("metadata", {})
    annotations = metadata.get("annotations", {}) if isinstance(metadata, dict) else {}
    if isinstance(annotations, dict) and annotations.get(SOURCE_COMMIT_ANNOTATION):
        return demo_project

    rendered = {**demo_project}
    rendered_metadata = {**metadata} if isinstance(metadata, dict) else {}
    rendered_annotations = {**annotations} if isinstance(annotations, dict) else {}
    rendered_annotations[SOURCE_COMMIT_ANNOTATION] = source_commit
    rendered_metadata["annotations"] = rendered_annotations
    rendered["metadata"] = rendered_metadata
    return rendered
