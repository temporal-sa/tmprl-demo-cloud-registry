"""Activities behind onboarding test deploys (see workflows/onboarding_preview.py).

- ``deploy_onboarding_preview`` runs for the onboarding workflow: it pins the
  demo repo commit and starts or updates the user's preview workflow.
- ``apply_onboarding_preview`` renders the validated draft as ``pv-<name>`` and
  applies it. The watcher then reconciles it like any other DemoProject.
- ``read_onboarding_preview`` summarises the operator's status for the page.
- ``delete_onboarding_preview`` and ``wait_onboarding_preview_cleanup`` tear it
  down: the watcher sees the deletion and runs finalize, which removes the
  Temporal namespace, images, Lambda and Kubernetes resources.

A preview differs from the draft only where it must not collide with the real
project once that merges: its name and host, and the Temporal credentials the
operator writes under its own secret prefix. User secrets come from the real
project's path (``secrets-from``), which is where the onboarding page stores
them.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from temporalio import activity
from temporalio.client import WithStartWorkflowOperation, WorkflowUpdateFailedError
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy
from temporalio.service import RPCError, RPCStatusCode

from demo_registry_operator import derive, errors, kube, resources
from demo_registry_operator.activities import canary
from demo_registry_operator.activities.catalog_onboarding import (
    github_get,
    platform_github_token,
)
from demo_registry_operator.activities.builds import definitions as build_definitions
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.temporal_client import connect
from demo_registry_operator.workflows.onboarding_preview_messages import (
    AppliedPreview,
    ApplyPreviewRequest,
    DeployPreviewRequest,
    PreviewDeployRequest,
    PreviewHealth,
    PreviewNameRequest,
    PreviewStatus,
    ReadPreviewRequest,
    StartPreviewRequest,
)

PREVIEW_WORKFLOW_TYPE = "OnboardingPreviewWorkflow"
PREVIEW_DEPLOY_UPDATE = "deploy"
CLEANUP_TIMEOUT_SECONDS = 30 * 60
CLEANUP_POLL_SECONDS = 15

# Someone is waiting on the onboarding page; fail fast.
PREVIEW_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
# Teardown must eventually happen; keep trying.
CLEANUP_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


def preview_name(project: str) -> str:
    return resources.dns_label(f"pv-{project}")


def render_preview_project(
    document: dict[str, Any],
    *,
    owner_email: str,
    commit_sha: str,
) -> dict[str, Any]:
    """The DemoProject a preview applies for a validated draft.

    Metadata is rebuilt rather than copied, so labels and annotations in the
    draft can't steer the operator.
    """

    project = resources.project_name(document)
    name = preview_name(project)
    rendered = copy.deepcopy(document)
    rendered["metadata"] = {
        "name": name,
        "labels": {resources.PREVIEW_LABEL: "true"},
        "annotations": {
            resources.SECRETS_FROM_ANNOTATION: project,
            resources.PREVIEW_OWNER_ANNOTATION: owner_email,
            resources.SOURCE_COMMIT_ANNOTATION: commit_sha,
        },
    }
    spec = rendered.get("spec")
    if not isinstance(spec, dict):
        spec = {}
        rendered["spec"] = spec
    ingress = spec.get("ingress")
    if isinstance(ingress, dict):
        ingress["subdomain"] = name
        ingress["temporalAuthRequired"] = True
    temporal = spec.get("temporal")
    if isinstance(temporal, dict):
        # Credentials always live under the preview's own prefix.
        temporal.pop("runtimeSecretPath", None)
    return rendered


def preview_url(demo_project: dict[str, Any]) -> str:
    ingress = resources.spec_of(demo_project).get("ingress")
    if not isinstance(ingress, dict):
        return ""
    return f"https://{resources.project_host(demo_project)}"


def unsupported_preview_problem(document: dict[str, Any]) -> str | None:
    if resources.temporal_nexus_endpoints(document):
        # Endpoint names are unique across the Temporal Cloud account, so a live
        # preview would block the real project from creating them after merge.
        return "Test deploys don't support Nexus endpoints yet."
    return None


@activity.defn
def deploy_onboarding_preview(request: DeployPreviewRequest) -> PreviewStatus:
    """Pin the commit to build, then start or update the user's preview."""

    problem = unsupported_preview_problem(request.document)
    if problem:
        raise errors.non_retryable_error(problem, error_type=errors.VALIDATION_ERROR)
    project = resources.project_name(request.document)
    deploy = PreviewDeployRequest(
        project=project,
        document=request.document,
        revision=request.revision,
        commit_sha=resolve_source_commit(request.document),
    )
    return asyncio.run(start_or_update_preview(request.owner_email, deploy))


async def start_or_update_preview(
    owner_email: str, deploy: PreviewDeployRequest
) -> PreviewStatus:
    config = WorkerConfig.from_env()
    client = await connect(config)
    start = WithStartWorkflowOperation(
        PREVIEW_WORKFLOW_TYPE,
        StartPreviewRequest(owner_email=owner_email),
        id=derive.onboarding_preview_workflow_id(owner_email, config.cluster),
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        task_queue=config.task_queue,
    )
    try:
        return await client.execute_update_with_start_workflow(
            PREVIEW_DEPLOY_UPDATE,
            deploy,
            start_workflow_operation=start,
            result_type=PreviewStatus,
        )
    except WorkflowUpdateFailedError as error:
        cause = error.cause
        message = getattr(cause, "message", None) or str(cause or error)
        raise errors.non_retryable_error(
            message, error_type=errors.VALIDATION_ERROR
        ) from error


def resolve_source_commit(document: dict[str, Any]) -> str:
    """Head of the draft's source branch, the commit the preview builds."""

    branch = resources.source_branch(document)
    if build_definitions.looks_like_commit_sha(branch):
        return branch
    repo = canary.github_repo_from_url(resources.source_git_repository_url(document))
    if repo is None:
        raise errors.non_retryable_error(
            "Test deploys need a GitHub source repo.",
            error_type=errors.VALIDATION_ERROR,
        )
    owner, name = repo
    info = github_get(
        f"/repos/{quote(owner)}/{quote(name)}/branches/{quote(branch, safe='')}",
        platform_github_token(),
    )
    if info is None:
        raise errors.non_retryable_error(
            f"Branch {branch!r} was not found in github.com/{owner}/{name}. Set "
            "source.branch to the branch the demo builds from.",
            error_type=errors.VALIDATION_ERROR,
        )
    commit = info.get("commit") or {}
    sha = commit.get("sha") if isinstance(commit, dict) else None
    if not isinstance(sha, str) or not build_definitions.looks_like_commit_sha(sha):
        raise RuntimeError(f"GitHub returned no commit for {owner}/{name}@{branch}.")
    return sha


@activity.defn
def apply_onboarding_preview(request: ApplyPreviewRequest) -> AppliedPreview:
    rendered = render_preview_project(
        request.document,
        owner_email=request.owner_email,
        commit_sha=request.commit_sha,
    )
    name = resources.project_name(rendered)
    existing = kube.read_demo_project(name)
    if existing is not None:
        owner = resources.metadata_annotations(existing).get(
            resources.PREVIEW_OWNER_ANNOTATION
        )
        if not resources.is_onboarding_preview(existing) or owner != request.owner_email:
            raise errors.non_retryable_error(
                f"Another test deploy is already using the name {name!r}.",
                error_type=errors.VALIDATION_ERROR,
            )
        if kube.demo_project_deletion_timestamp(existing):
            # A previous preview of this project is still finalizing; retry.
            raise RuntimeError(f"The previous test deploy {name!r} is still being deleted.")
    try:
        kube.replace_demo_project(rendered)
        kube.ensure_demo_project_finalizer(name)
    except ValueError as error:
        raise errors.non_retryable_error(
            str(error), error_type=errors.VALIDATION_ERROR
        ) from error
    return AppliedPreview(name=name, url=preview_url(rendered))


@activity.defn
def read_onboarding_preview(request: ReadPreviewRequest) -> PreviewHealth:
    """Where the preview's rollout is, for the page.

    The operator writes the DemoProject status only when a reconcile ends, so
    it decides Ready and Failed, and the live step comes from what the
    project's workflows are running right now.
    """

    resource = kube.read_demo_project(request.name)
    health = preview_health(
        resource.get("status") if resource else None, request.commit_sha
    )
    if health.phase != "Deploying":
        return health
    try:
        progress = asyncio.run(rollout_progress(request.name))
    except Exception as error:  # noqa: BLE001 - progress is best effort
        activity.logger.warning("test deploy progress read failed: %s", error)
        return health
    return dataclasses.replace(health, **progress) if progress else health


def preview_health(status: Any, commit_sha: str) -> PreviewHealth:
    waiting = PreviewHealth(
        phase="Deploying", stage="resources", step="Waiting for the operator"
    )
    if not isinstance(status, dict) or not status.get("phase"):
        return waiting
    if status_commit(status) not in (None, commit_sha):
        # What the status describes is an earlier deploy.
        return waiting
    if canary.canary_failed(status):
        return PreviewHealth(
            phase="Failed",
            step=str(status["phase"]),
            message=canary.canary_failure_message(status).replace(
                "Canary project failed before promotion", "The test deploy failed"
            ),
            log_url=build_log_url(status),
        )
    if canary.canary_promoted({"expectedSourceCommitSha": commit_sha}, status):
        return PreviewHealth(phase="Ready", step="Ready")
    return waiting


def status_commit(status: dict[str, Any]) -> str | None:
    for block in ("candidateRolloutStatus", "buildStatus"):
        value = status.get(block)
        sha = value.get("sourceCommitSha") if isinstance(value, dict) else None
        if isinstance(sha, str) and sha:
            return sha
    return None


def build_log_url(status: dict[str, Any]) -> str | None:
    build = status.get("buildStatus")
    codebuild = build.get("codebuild") if isinstance(build, dict) else None
    logs = codebuild.get("logs") if isinstance(codebuild, dict) else None
    link = logs.get("deepLink") if isinstance(logs, dict) else None
    return link if isinstance(link, str) and link else None


# What each activity of a DemoProject reconcile means to someone watching, as
# (stage, step). Activities not listed fall back to their workflow's default.
PARENT_STEPS = {
    "load_current_demo_project_for_reconcile": ("resources", "Starting"),
    "create_or_verify_ecr_repository": ("resources", "Creating image repositories"),
    "create_or_verify_temporal_cloud_namespace": ("resources", "Creating the Temporal namespace"),
    "ensure_temporal_cloud_namespace_tags": ("resources", "Creating the Temporal namespace"),
    "wait_for_temporal_cloud_namespace_dns": ("resources", "Creating the Temporal namespace"),
    "ensure_temporal_cloud_namespace_readers_access": ("resources", "Creating the Temporal namespace"),
    "reconcile_temporal_cloud_search_attributes": ("resources", "Adding search attributes"),
    "reconcile_nexus_endpoints": ("resources", "Creating Nexus endpoints"),
    "verify_secrets_manager_resources": ("resources", "Checking secrets"),
    "record_demo_project_status": ("rollout", "Finishing up"),
    "record_catalog_project_status": ("rollout", "Finishing up"),
}
BUILD_STEPS = {
    "resolve_project_image_build_plan": ("build", "Planning image builds"),
    "start_project_image_build": ("build", "Starting the image build"),
    "wait_for_project_image_build": ("build", "Building images"),
}
RUNTIME_STEPS = {
    "wait_for_worker_deployment_rollouts": ("rollout", "Rolling out workers"),
    "wait_for_candidate_readiness": ("rollout", "Waiting for pods to start"),
    "run_smoke_check": ("rollout", "Waiting for the health check"),
    "promote_candidate_routes": ("rollout", "Switching traffic to the new version"),
    "prune_stale_runtime_candidates": ("rollout", "Cleaning up the previous version"),
    "capture_rollout_diagnostics": ("rollout", "Collecting rollout diagnostics"),
    "rollback_worker_deployments": ("rollout", "Rolling back a failed rollout"),
    "restore_active_runtime_routes": ("rollout", "Rolling back a failed rollout"),
    "delete_runtime_candidate_resources": ("rollout", "Rolling back a failed rollout"),
}
CHILD_STEPS = {
    "DemoProjectArtifactBuildWorkflow": (BUILD_STEPS, ("build", "Building images")),
    "DemoProjectServerlessLambdaWorkflow": ({}, ("deploy", "Deploying Lambda functions")),
    "DemoProjectKubernetesRuntimeWorkflow": (
        RUNTIME_STEPS,
        ("deploy", "Applying Kubernetes resources"),
    ),
}


def parent_step(activity_type: str) -> tuple[str, str]:
    if activity_type in PARENT_STEPS:
        return PARENT_STEPS[activity_type]
    if any(word in activity_type for word in ("api_key", "service_account", "mtls", "credential", "runtime_secret", "canary")):
        return ("resources", "Setting up Temporal credentials")
    if "temporal" in activity_type:
        return ("resources", "Setting up Temporal Cloud")
    return ("resources", "Preparing")


async def rollout_progress(name: str) -> dict[str, Any] | None:
    """The current stage, step and detail of the project's reconcile, or None
    when its workflow isn't reconciling."""

    config = WorkerConfig.from_env()
    client = await connect(config)
    handle = client.get_workflow_handle(
        derive.demo_project_workflow_id(name, config.cluster)
    )
    try:
        description = await handle.describe()
    except RPCError as error:
        if error.status == RPCStatusCode.NOT_FOUND:
            return {"stage": "resources", "step": "Waiting for the operator"}
        raise
    raw = description.raw_description
    for child in raw.pending_children:
        steps, default = CHILD_STEPS.get(child.workflow_type_name, ({}, None))
        if default is None:
            continue
        child_description = await client.get_workflow_handle(
            child.workflow_id, run_id=child.run_id or None
        ).describe()
        pending = list(child_description.raw_description.pending_activities)
        if not pending:
            return {"stage": default[0], "step": default[1]}
        stage, step = steps.get(pending[0].activity_type.name, default)
        return {
            "stage": stage,
            "step": step,
            **await activity_detail(client, config, pending[0]),
        }
    pending = list(raw.pending_activities)
    if not pending:
        return None
    stage, step = parent_step(pending[0].activity_type.name)
    return {"stage": stage, "step": step, **await activity_detail(client, config, pending[0])}


async def activity_detail(client: Any, config: WorkerConfig, pending: Any) -> dict[str, Any]:
    """A detail line, and the build log, from a pending activity's heartbeat
    and retry state."""

    heartbeat: dict[str, Any] = {}
    payloads = list(pending.heartbeat_details.payloads)
    if payloads:
        decoded = await client.data_converter.decode(payloads)
        if decoded and isinstance(decoded[-1], dict):
            heartbeat = decoded[-1]
    detail = None
    log_url = None
    build_id = heartbeat.get("codebuildBuildId")
    if isinstance(build_id, str) and build_id:
        detail, log_url = codebuild_progress(config, build_id)
    elif "attempt" in heartbeat and "smokeCheck" in heartbeat:
        detail = f"Check {heartbeat['attempt']}, last response {heartbeat.get('lastStatus') or 'none'}"
    if pending.attempt > 1 and pending.last_failure.message:
        retry = f"Retrying (attempt {pending.attempt}): {pending.last_failure.message}"
        detail = f"{detail}. {retry}" if detail else retry
    return {"detail": detail, "log_url": log_url}


def codebuild_progress(config: WorkerConfig, build_id: str) -> tuple[str | None, str | None]:
    """CodeBuild's current phase ("Build", "Post build") and the log link."""

    import boto3

    builds = (
        boto3.session.Session(region_name=config.aws_region)
        .client("codebuild")
        .batch_get_builds(ids=[build_id])
        .get("builds")
        or []
    )
    if not builds:
        return None, None
    build = builds[0]
    phase = build.get("currentPhase")
    detail = (
        f"CodeBuild phase: {str(phase).replace('_', ' ').capitalize()}"
        if isinstance(phase, str) and phase and phase != "COMPLETED"
        else None
    )
    link = (build.get("logs") or {}).get("deepLink")
    return detail, link if isinstance(link, str) and link else None


@activity.defn
def delete_onboarding_preview(request: PreviewNameRequest) -> str:
    return kube.delete_demo_project(request.name)


@activity.defn
def wait_onboarding_preview_cleanup(request: PreviewNameRequest) -> None:
    """Heartbeats until finalize has removed everything the preview created."""

    config = WorkerConfig.from_env()
    plan = {
        "project": request.name,
        "namespace": derive.kubernetes_namespace(request.name),
        "gitRepository": resources.source_git_repository_name(
            {"metadata": {"name": request.name}}
        ),
        "sourceWatchNamespace": config.source_watch_namespace,
        "catalogStatusNamespace": config.catalog_status_namespace,
        "catalogStatusMirrorNamespace": config.catalog_status_mirror_namespace,
        "catalogStatusConfigMap": config.catalog_status_configmap,
    }
    deadline = time.monotonic() + CLEANUP_TIMEOUT_SECONDS
    while True:
        snapshot = canary.canary_cleanup_snapshot(plan)
        if snapshot["ok"]:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Timed out waiting for test deploy {request.name!r} to clean up; "
                f"still waiting on {', '.join(snapshot.get('waitingOn', []))}."
            )
        activity.heartbeat(
            {
                "waitingOn": snapshot.get("waitingOn", []),
                "checkedAt": datetime.now(UTC).isoformat(),
            }
        )
        time.sleep(CLEANUP_POLL_SECONDS)
