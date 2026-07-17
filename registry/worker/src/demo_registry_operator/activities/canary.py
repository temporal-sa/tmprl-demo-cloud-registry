import asyncio
import copy
import hashlib
import importlib.resources
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import yaml
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.common import PinnedVersioningOverride, WorkerDeploymentVersion
from temporalio.service import RPCError, RPCStatusCode

from demo_registry_operator import derive, errors, kube, resources
from demo_registry_operator.activities.builds import definitions as build_definitions
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.temporal_client import connect
from demo_registry_operator.workflows import messages

CANARY_TEMPLATES_PACKAGE = "demo_registry_operator.canary_templates"


@activity.defn
def load_canary_templates() -> list[dict[str, Any]]:
    """Load the external operator-canary templates (one YAML file per type).

    Returns a sorted list of ``{name, authMode, demoProjectTemplate}``. Adding a new
    canary type is just a new YAML file in the canary_templates package — no workflow
    or registry.yaml change needed.
    """
    templates: list[dict[str, Any]] = []
    package_files = importlib.resources.files(CANARY_TEMPLATES_PACKAGE)
    for entry in sorted(package_files.iterdir(), key=lambda path: path.name):
        if not entry.name.endswith((".yaml", ".yml")):
            continue
        data = yaml.safe_load(entry.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("demoProjectTemplate"), dict):
            templates.append(
                {
                    "name": data.get("name") or entry.name.rsplit(".", 1)[0],
                    "authMode": data.get("authMode") or "apiKey",
                    "demoProjectTemplate": data["demoProjectTemplate"],
                }
            )
    if not templates:
        raise errors.non_retryable_error(
            "No operator-canary templates found in the canary_templates package.",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return templates


FORCE_TEMPORAL_AUTH_ROTATION_REASON = "rotate-temporal-auth"


@dataclass(frozen=True)
class RegistryOperatorCanaryPlanRequest:
    demo_project_template: dict[str, Any]
    workflow_id: str = "operator-canary"
    run_id: str = ""
    project_name_prefix: str = "operator-canary"
    project_name: str | None = None
    expected_source_commit_sha: str | None = None
    expected_images: dict[str, Any] | None = None
    auth_mode: str | None = None
    expected_operator_image: str | None = None
    expected_operator_source_sha: str | None = None
    timeout_seconds: int = 900
    poll_seconds: int = 10
    cleanup_timeout_seconds: int = 600
    cleanup_policy: str = "Always"


@dataclass(frozen=True)
class RegistryOperatorCanaryPlan:
    plan: dict[str, Any]


@dataclass(frozen=True)
class ApplyRegistryOperatorCanaryProjectRequest:
    project: str
    namespace: str
    demo_project: dict[str, Any]


@dataclass(frozen=True)
class SignalRegistryOperatorCanaryReconcileRequest:
    project: str
    demo_project: dict[str, Any]
    target_deployment_name: str
    target_build_id: str


@dataclass(frozen=True)
class SignalRegistryOperatorCanaryFinalizeRequest:
    project: str
    demo_project: dict[str, Any]


@dataclass(frozen=True)
class RegistryOperatorCanaryPromotionWaitRequest:
    project: str
    namespace: str
    expected_source_commit_sha: str | None = None
    expected_images: dict[str, Any] | None = None
    workflow_id: str | None = None
    run_id: str | None = None
    timeout_seconds: int = 900
    poll_seconds: int = 10

    def as_plan(self) -> dict[str, Any]:
        return {
            "project": self.project,
            "namespace": self.namespace,
            "expectedSourceCommitSha": self.expected_source_commit_sha,
            "expectedImages": self.expected_images or {},
            "workflowID": self.workflow_id,
            "runID": self.run_id,
            "timeoutSeconds": self.timeout_seconds,
            "pollSeconds": self.poll_seconds,
        }


@dataclass(frozen=True)
class RotateRegistryOperatorCanaryCredentialsRequest:
    project: str

    def as_plan(self) -> dict[str, Any]:
        return {"project": self.project}


@dataclass(frozen=True)
class RegistryOperatorCanaryRotationWaitRequest:
    project: str
    auth_mode: str
    rotation_token: str
    workflow_id: str | None = None
    run_id: str | None = None
    timeout_seconds: int = 900
    poll_seconds: int = 10

    def as_plan(self) -> dict[str, Any]:
        return {
            "project": self.project,
            "authMode": self.auth_mode,
            "rotationToken": self.rotation_token,
            "workflowID": self.workflow_id,
            "runID": self.run_id,
            "timeoutSeconds": self.timeout_seconds,
            "pollSeconds": self.poll_seconds,
        }


@dataclass(frozen=True)
class DeleteRegistryOperatorCanaryProjectRequest:
    project: str
    cleanup_policy: str = "Always"

    def as_plan(self) -> dict[str, Any]:
        return {"project": self.project, "cleanupPolicy": self.cleanup_policy}


@dataclass(frozen=True)
class WaitRegistryOperatorCanaryCleanupRequest:
    project: str
    namespace: str
    git_repository: str
    source_watch_namespace: str
    catalog_status_namespace: str
    catalog_status_mirror_namespace: str
    catalog_status_config_map: str
    cleanup_policy: str = "Always"
    cleanup_timeout_seconds: int = 600
    poll_seconds: int = 10
    delete_action: Any = None

    def as_plan(self) -> dict[str, Any]:
        return {
            "project": self.project,
            "namespace": self.namespace,
            "gitRepository": self.git_repository,
            "sourceWatchNamespace": self.source_watch_namespace,
            "catalogStatusNamespace": self.catalog_status_namespace,
            "catalogStatusMirrorNamespace": self.catalog_status_mirror_namespace,
            "catalogStatusConfigMap": self.catalog_status_config_map,
            "cleanupPolicy": self.cleanup_policy,
            "cleanupTimeoutSeconds": self.cleanup_timeout_seconds,
            "pollSeconds": self.poll_seconds,
            "deleteAction": self.delete_action,
        }


@dataclass(frozen=True)
class RegistryOperatorCanaryActivityResult:
    status: dict[str, Any]


@activity.defn
def prepare_registry_operator_canary_plan(
    request: RegistryOperatorCanaryPlanRequest,
) -> RegistryOperatorCanaryPlan:
    template = request.demo_project_template
    if not isinstance(template, dict):
        raise errors.non_retryable_error(
            "RegistryOperatorCanaryWorkflow requires demoProjectTemplate",
            error_type=errors.VALIDATION_ERROR,
        )

    workflow_id = request.workflow_id
    run_id = request.run_id
    prefix = request.project_name_prefix
    requested_name = request.project_name
    project_name = (
        requested_name
        if isinstance(requested_name, str) and requested_name
        else canary_project_name(prefix, workflow_id, run_id)
    )
    config = WorkerConfig.from_env()
    source_resolution = canary_source_resolution(
        template,
        config,
        expected_source_sha=request.expected_source_commit_sha,
        expected_images=request.expected_images,
    )
    expected_source_sha = source_resolution.get("sourceCommitSha")

    demo_project = render_canary_demo_project(
        template,
        project_name=project_name,
        expected_source_sha=expected_source_sha,
        expected_images=request.expected_images,
        auth_mode=request.auth_mode,
    )

    expected_operator_image = request.expected_operator_image
    actual_operator_image = os.getenv("DEMO_REGISTRY_OPERATOR_IMAGE")
    if (
        isinstance(expected_operator_image, str)
        and expected_operator_image
        and actual_operator_image != expected_operator_image
    ):
        raise errors.non_retryable_error(
            (
                "Registry operator canary is running on unexpected image "
                f"{actual_operator_image!r}; expected {expected_operator_image!r}"
            ),
            error_type=errors.VALIDATION_ERROR,
        )

    expected_operator_source_sha = request.expected_operator_source_sha
    actual_operator_source_sha = os.getenv("DEMO_REGISTRY_OPERATOR_SOURCE_SHA")
    if (
        isinstance(expected_operator_source_sha, str)
        and expected_operator_source_sha
        and actual_operator_source_sha != expected_operator_source_sha
    ):
        raise errors.non_retryable_error(
            (
                "Registry operator canary is running unexpected source SHA "
                f"{actual_operator_source_sha!r}; expected {expected_operator_source_sha!r}"
            ),
            error_type=errors.VALIDATION_ERROR,
        )

    target_deployment_name = os.getenv("TEMPORAL_DEPLOYMENT_NAME")
    target_build_id = os.getenv("TEMPORAL_WORKER_BUILD_ID")
    if not target_deployment_name or not target_build_id:
        raise errors.non_retryable_error(
            (
                "Registry operator canary requires TEMPORAL_DEPLOYMENT_NAME and "
                "TEMPORAL_WORKER_BUILD_ID so project reconciliation can be pinned "
                "to the target WorkerDeployment version"
            ),
            error_type=errors.CONFIGURATION_ERROR,
        )
    return RegistryOperatorCanaryPlan({
        "project": project_name,
        "namespace": resources.project_namespace(demo_project),
        "demoProject": demo_project,
        "authMode": resources.temporal_auth_mode(demo_project),
        "expectedOperatorImage": expected_operator_image,
        "actualOperatorImage": actual_operator_image,
        "expectedOperatorSourceSha": expected_operator_source_sha,
        "actualOperatorSourceSha": actual_operator_source_sha,
        "targetDeploymentName": target_deployment_name,
        "targetBuildID": target_build_id,
        "expectedSourceCommitSha": expected_source_sha,
        "sourceResolution": source_resolution,
        "expectedImages": request.expected_images or {},
        "timeoutSeconds": int(request.timeout_seconds),
        "pollSeconds": int(request.poll_seconds),
        "cleanupTimeoutSeconds": int(request.cleanup_timeout_seconds),
        "cleanupPolicy": request.cleanup_policy,
        "sourceWatchNamespace": config.source_watch_namespace,
        "gitRepository": resources.source_git_repository_name(demo_project),
        "catalogStatusNamespace": config.catalog_status_namespace,
        "catalogStatusMirrorNamespace": config.catalog_status_mirror_namespace,
        "catalogStatusConfigMap": config.catalog_status_configmap,
        "plannedAt": datetime.now(UTC).isoformat(),
    })


@activity.defn
def apply_registry_operator_canary_project(
    request: ApplyRegistryOperatorCanaryProjectRequest,
) -> RegistryOperatorCanaryActivityResult:
    demo_project = request.demo_project
    try:
        action = kube.apply_demo_project(
            demo_project,
            dry_run=False,
        )
        finalizer_added = kube.ensure_demo_project_finalizer(request.project)
    except ValueError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise errors.non_retryable_error(str(error), error_type=errors.CONFIGURATION_ERROR) from error

    return RegistryOperatorCanaryActivityResult({
        "project": request.project,
        "namespace": request.namespace,
        "phase": "Applied",
        "action": action,
        "finalizerAdded": finalizer_added,
        "checkedAt": datetime.now(UTC).isoformat(),
    })


def canary_source_resolution(
    template: dict[str, Any],
    config: WorkerConfig,
    *,
    expected_source_sha: Any,
    expected_images: Any,
) -> dict[str, Any]:
    explicit_source_sha = commit_sha_or_error(
        expected_source_sha,
        "expectedSourceCommitSha",
    )
    if explicit_source_sha:
        return {
            "phase": "Resolved",
            "source": "request",
            "sourceCommitSha": explicit_source_sha,
        }

    annotated_source_sha = commit_sha_or_error(
        resources.metadata_annotations(template).get(resources.SOURCE_COMMIT_ANNOTATION),
        resources.SOURCE_COMMIT_ANNOTATION,
    )
    if annotated_source_sha:
        return {
            "phase": "Resolved",
            "source": "annotation",
            "sourceCommitSha": annotated_source_sha,
        }

    if not canary_builds_require_source(template, expected_images):
        return {
            "phase": "Skipped",
            "reason": "ImagesAlreadyResolved",
            "message": "Canary image overrides cover every configured build.",
        }

    return resolve_canary_github_source_commit(template, config)


def commit_sha_or_error(value: Any, field: str) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    commit_sha = value.strip()
    if not build_definitions.looks_like_commit_sha(commit_sha):
        raise errors.non_retryable_error(
            f"Canary {field} must be a git commit SHA.",
            error_type=errors.VALIDATION_ERROR,
        )
    return commit_sha


def canary_builds_require_source(
    template: dict[str, Any],
    expected_images: Any,
) -> bool:
    build_names = set(resources.builds(template))
    if not build_names:
        return False
    if not isinstance(expected_images, dict):
        return True
    resolved_images = {
        str(build_name)
        for build_name, image in expected_images.items()
        if isinstance(build_name, str) and isinstance(image, str) and image
    }
    return not build_names.issubset(resolved_images)


def resolve_canary_github_source_commit(
    template: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, Any]:
    repo_url = resources.source_git_repository_url(template)
    repo = github_repo_from_url(repo_url)
    if repo is None:
        raise errors.non_retryable_error(
            (
                "Registry operator canary must use a GitHub HTTPS source repo "
                "or provide expectedSourceCommitSha."
            ),
            error_type=errors.VALIDATION_ERROR,
        )

    branch = resources.source_branch(template)
    if build_definitions.looks_like_commit_sha(branch):
        return {
            "phase": "Resolved",
            "source": "source.branch",
            "repo": repo_url,
            "branch": branch,
            "sourceCommitSha": branch,
        }

    owner, name = repo
    url = (
        f"https://api.github.com/repos/{quote(owner, safe='')}/"
        f"{quote(name, safe='')}/branches/{quote(branch, safe='')}"
    )
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "tmprl-demo-cloud-registry-operator",
    }
    token = github_token_for_canary_source(template, config)
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        with urlopen(Request(url, headers=headers), timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        if error.code == 404:
            raise errors.non_retryable_error(
                (
                    f"Canary source branch {branch!r} was not found or is not "
                    f"accessible in {owner}/{name}."
                ),
                error_type=errors.VALIDATION_ERROR,
            ) from error
        if error.code in {401, 403}:
            raise errors.non_retryable_error(
                (
                    f"Canary source branch lookup for {owner}/{name}@{branch} "
                    f"failed with HTTP {error.code}; verify source GitHub auth."
                ),
                error_type=errors.CONFIGURATION_ERROR,
            ) from error
        raise RuntimeError(
            f"Canary source branch lookup failed with HTTP {error.code}."
        ) from error
    except (OSError, URLError) as error:
        raise RuntimeError(
            f"Canary source branch lookup for {owner}/{name}@{branch} failed: {error}"
        ) from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise RuntimeError(
            f"Canary source branch lookup for {owner}/{name}@{branch} returned invalid JSON."
        ) from error

    commit = payload.get("commit") if isinstance(payload, dict) else None
    source_sha = commit.get("sha") if isinstance(commit, dict) else None
    if not isinstance(source_sha, str) or not build_definitions.looks_like_commit_sha(source_sha):
        raise RuntimeError(
            f"Canary source branch lookup for {owner}/{name}@{branch} did not return a commit SHA."
        )

    return {
        "phase": "Resolved",
        "source": "github",
        "repo": repo_url,
        "branch": branch,
        "sourceCommitSha": source_sha,
    }


def github_repo_from_url(url: str | None) -> tuple[str, str] | None:
    if not isinstance(url, str) or not url.strip():
        return None
    parsed = urlparse(url.strip())
    if parsed.scheme != "https" or parsed.netloc.lower() != "github.com":
        return None
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) < 2:
        return None
    repo = parts[1]
    if repo.endswith(".git"):
        repo = repo[:-4]
    return parts[0], repo


def github_token_for_canary_source(
    template: dict[str, Any],
    config: WorkerConfig,
) -> str | None:
    secret_ref = resources.source_git_repository_secret_ref(template)
    if not isinstance(secret_ref, dict):
        return None
    secret_name = secret_ref.get("name")
    if not isinstance(secret_name, str) or not secret_name:
        return None
    try:
        data = kube.read_secret_data(config.source_watch_namespace, secret_name)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    for key in ("password", "token", "githubToken", "GITHUB_TOKEN"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


@activity.defn
def wait_for_registry_operator_canary_promotion(
    request: RegistryOperatorCanaryPromotionWaitRequest,
) -> RegistryOperatorCanaryActivityResult:
    plan = request.as_plan()
    deadline = time.monotonic() + int(plan.get("timeoutSeconds", 900))
    poll_seconds = int(plan.get("pollSeconds", 10))
    last_status: dict[str, Any] | None = None
    while True:
        resource = kube.read_demo_project(string_field(plan, "project"))
        last_status = (
            resource.get("status", {}) if isinstance(resource, dict) else None
        )
        if isinstance(last_status, dict) and canary_promoted(plan, last_status):
            namespace_name = promoted_workload_namespace(plan, last_status)
            namespace_status = kube.read_namespace(namespace_name)
            if namespace_status is None:
                return RegistryOperatorCanaryActivityResult({
                    "project": string_field(plan, "project"),
                    "phase": "Failed",
                    "ok": False,
                    "message": "Canary promoted but namespace was not found.",
                    "namespace": namespace_name,
                    "status": last_status,
                    "checkedAt": datetime.now(UTC).isoformat(),
                })
            workload_status = verify_promoted_workload_resources(plan, last_status)
            if not workload_status["ok"]:
                return RegistryOperatorCanaryActivityResult({
                    "project": string_field(plan, "project"),
                    "phase": "Failed",
                    "ok": False,
                    "message": workload_status["message"],
                    "namespace": namespace_status,
                    "workloads": workload_status,
                    "status": last_status,
                    "checkedAt": datetime.now(UTC).isoformat(),
                })
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "Promoted",
                "ok": True,
                "message": "Canary project promoted successfully.",
                "namespace": namespace_status,
                "workloads": workload_status,
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        if isinstance(last_status, dict) and canary_failed(last_status):
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "Failed",
                "ok": False,
                "message": canary_failure_message(last_status),
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        workflow_failure = canary_workflow_terminal_failure(plan)
        if workflow_failure is not None:
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "Failed",
                "ok": False,
                "message": workflow_failure["message"],
                "workflow": workflow_failure,
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        if time.monotonic() >= deadline:
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "TimedOut",
                "ok": False,
                "message": "Timed out waiting for canary project promotion.",
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        activity.heartbeat(
            {
                "project": string_field(plan, "project"),
                "status": last_status,
            }
        )
        time.sleep(poll_seconds)


@activity.defn
def rotate_registry_operator_canary_credentials(
    request: RotateRegistryOperatorCanaryCredentialsRequest,
) -> RegistryOperatorCanaryActivityResult:
    plan = request.as_plan()
    token = f"operator-canary-credential-rotation:{string_field(plan, 'project')}:{uuid_token()}"
    workflow_signal = signal_registry_operator_canary_force_reconcile(
        plan,
        token=token,
        reason=FORCE_TEMPORAL_AUTH_ROTATION_REASON,
    )
    return RegistryOperatorCanaryActivityResult({
        "project": string_field(plan, "project"),
        "phase": "Signaled",
        "ok": True,
        "token": token,
        "reason": FORCE_TEMPORAL_AUTH_ROTATION_REASON,
        "workflow": workflow_signal,
        "checkedAt": datetime.now(UTC).isoformat(),
    })


@activity.defn
def wait_for_registry_operator_canary_rotation(
    request: RegistryOperatorCanaryRotationWaitRequest,
) -> RegistryOperatorCanaryActivityResult:
    plan = request.as_plan()
    deadline = time.monotonic() + int(plan.get("timeoutSeconds", 900))
    poll_seconds = int(plan.get("pollSeconds", 10))
    token = string_field(plan, "rotationToken")
    last_status: dict[str, Any] | None = None
    while True:
        resource = kube.read_demo_project(string_field(plan, "project"))
        last_status = (
            resource.get("status", {}) if isinstance(resource, dict) else None
        )
        if isinstance(last_status, dict) and canary_rotation_complete(
            token,
            last_status,
        ):
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "Rotated",
                "ok": True,
                "token": token,
                "authMode": string_field(plan, "authMode"),
                "message": "Canary credential rotation completed after worker canary.",
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        if isinstance(last_status, dict) and canary_failed(last_status):
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "Failed",
                "ok": False,
                "token": token,
                "message": canary_failure_message(last_status),
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        workflow_failure = canary_workflow_terminal_failure(plan)
        if workflow_failure is not None:
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "Failed",
                "ok": False,
                "token": token,
                "message": workflow_failure["message"],
                "workflow": workflow_failure,
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        if time.monotonic() >= deadline:
            return RegistryOperatorCanaryActivityResult({
                "project": string_field(plan, "project"),
                "phase": "TimedOut",
                "ok": False,
                "token": token,
                "message": "Timed out waiting for canary credential rotation.",
                "status": last_status,
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        activity.heartbeat(
            {
                "project": string_field(plan, "project"),
                "token": token,
                "status": last_status,
            }
        )
        time.sleep(poll_seconds)


def signal_registry_operator_canary_force_reconcile(
    plan: dict[str, Any],
    *,
    token: str,
    reason: str,
) -> dict[str, Any]:
    return asyncio.run(
        signal_registry_operator_canary_force_reconcile_async(
            plan,
            token=token,
            reason=reason,
        )
    )


async def signal_registry_operator_canary_force_reconcile_async(
    plan: dict[str, Any],
    *,
    token: str,
    reason: str,
) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    workflow_id = derive.demo_project_workflow_id(
        string_field(plan, "project"), config.cluster
    )
    client = await connect(config)
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(
        "force_reconcile",
        messages.ForceReconcileSignal(token=token, reason=reason),
    )
    return {
        "phase": "Signaled",
        "workflowID": workflow_id,
        "token": token,
        "reason": reason,
    }


FAILED_CANARY_WORKFLOW_STATUSES = {
    WorkflowExecutionStatus.FAILED,
    WorkflowExecutionStatus.CANCELED,
    WorkflowExecutionStatus.TERMINATED,
    WorkflowExecutionStatus.TIMED_OUT,
}


def canary_workflow_terminal_failure(
    plan: dict[str, Any],
) -> dict[str, Any] | None:
    workflow_id = plan.get("workflowID")
    if not isinstance(workflow_id, str) or not workflow_id:
        # Fallback when the plan carried no id: derive the same color-scoped id the
        # producer used, or describe() would target a nonexistent id, hit NOT_FOUND,
        # and silently report "not failed" for a genuinely failed canary.
        workflow_id = derive.demo_project_workflow_id(
            string_field(plan, "project"), WorkerConfig.from_env().cluster
        )
    run_id = plan.get("runID")
    run_id = run_id if isinstance(run_id, str) and run_id else None
    return asyncio.run(canary_workflow_terminal_failure_async(workflow_id, run_id))


async def canary_workflow_terminal_failure_async(
    workflow_id: str,
    run_id: str | None,
) -> dict[str, Any] | None:
    config = WorkerConfig.from_env()
    client = await connect(config)
    handle = client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe(rpc_timeout=timedelta(seconds=5))
    except RPCError as error:
        if error.status == RPCStatusCode.NOT_FOUND:
            return None
        activity.logger.warning(
            "Could not describe canary workflow %s/%s: %s",
            workflow_id,
            run_id,
            error,
        )
        return None

    status = description.status
    if status not in FAILED_CANARY_WORKFLOW_STATUSES:
        return None
    return {
        "phase": "Failed",
        "workflowID": workflow_id,
        "runID": description.run_id,
        "firstExecutionRunID": run_id,
        "executionStatus": status.name,
        "message": (
            "Canary DemoProject workflow reached terminal failed state "
            f"{status.name} before the gate completed."
        ),
    }


def canary_rotation_complete(token: str, status: dict[str, Any]) -> bool:
    if status.get("phase") not in {"Promoted", "Ready"}:
        return False
    force_reconcile = status.get("forceReconcile")
    if not isinstance(force_reconcile, dict):
        return False
    if force_reconcile.get("token") != token:
        return False
    if force_reconcile.get("reason") != FORCE_TEMPORAL_AUTH_ROTATION_REASON:
        return False

    temporal = (
        status.get("externalResources", {})
        .get("temporalCloud", {})
        if isinstance(status.get("externalResources"), dict)
        else {}
    )
    if not isinstance(temporal, dict):
        return False
    api_key = temporal.get("apiKey")
    if isinstance(api_key, dict):
        canary = api_key.get("canary")
        return (
            api_key.get("rotationReason") == "ManualRotation"
            and isinstance(canary, dict)
            and canary.get("ok") is True
        )
    mtls = temporal.get("mtls")
    mtls = mtls if isinstance(mtls, dict) else {}
    client = mtls.get("clientCertificate")
    if isinstance(client, dict):
        canary = client.get("canary")
        return (
            client.get("rotationReason") == "ManualRotation"
            and isinstance(canary, dict)
            and canary.get("ok") is True
        )
    return False


def canary_failed(status: dict[str, Any]) -> bool:
    # A canary is a single-shot rollout attempt: any terminal rollout-failure
    # phase means it will not promote on its own, so fail the gate fast instead
    # of polling until the (generous) promotion timeout.
    if status.get("phase") in {
        "Error",
        "FinalizationError",
        "CandidateNotReady",
        "SmokeCheckFailed",
        "WorkerRolloutFailed",
    }:
        return True
    external = status.get("externalResources")
    if isinstance(external, dict) and external.get("phase") == "Error":
        return True
    build_status = status.get("buildStatus")
    if isinstance(build_status, dict) and build_status.get("phase") == "Error":
        return True
    candidate_status = status.get("candidateRolloutStatus")
    return isinstance(candidate_status, dict) and candidate_status.get("phase") in {
        "BuildFailed",
        "WorkerRolloutFailed",
        "SmokeCheckFailed",
    }


def canary_failure_message(status: dict[str, Any]) -> str:
    message = status.get("message")
    if isinstance(message, str) and message:
        return "Canary project failed before promotion: " + message
    external = status.get("externalResources")
    if isinstance(external, dict):
        temporal = external.get("temporalCloud")
        if isinstance(temporal, dict) and temporal.get("message"):
            return "Canary project failed before promotion: " + str(
                temporal["message"]
            )
    build_status = status.get("buildStatus")
    if isinstance(build_status, dict):
        build_message = build_status.get("message")
        if isinstance(build_message, str) and build_message:
            return "Canary project failed before promotion: " + build_message
    return "Canary project failed before promotion."


@activity.defn
def delete_registry_operator_canary_project(
    request: DeleteRegistryOperatorCanaryProjectRequest,
) -> RegistryOperatorCanaryActivityResult:
    plan = request.as_plan()
    if plan.get("cleanupPolicy") == "Never":
        return RegistryOperatorCanaryActivityResult({
            "project": string_field(plan, "project"),
            "phase": "Skipped",
            "ok": True,
            "message": "Canary cleanup skipped by cleanupPolicy=Never.",
            "checkedAt": datetime.now(UTC).isoformat(),
        })

    action = kube.delete_demo_project(string_field(plan, "project"))
    return RegistryOperatorCanaryActivityResult({
        "project": string_field(plan, "project"),
        "phase": "DeleteRequested",
        "ok": True,
        "deleteAction": action,
        "message": "Canary DemoProject deletion was requested.",
        "checkedAt": datetime.now(UTC).isoformat(),
    })


@activity.defn
def signal_registry_operator_canary_finalize(
    request: SignalRegistryOperatorCanaryFinalizeRequest,
) -> RegistryOperatorCanaryActivityResult:
    return RegistryOperatorCanaryActivityResult(
        asyncio.run(signal_registry_operator_canary_finalize_async(request))
    )


async def signal_registry_operator_canary_finalize_async(
    request: SignalRegistryOperatorCanaryFinalizeRequest,
) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    workflow_id = derive.demo_project_workflow_id(request.project, config.cluster)
    client = await connect(config)
    handle = client.get_workflow_handle(workflow_id)
    try:
        await handle.signal(
            "finalize",
            messages.DemoProjectFinalizeSignal(demoProject=request.demo_project),
        )
    except RPCError as error:
        if error.status == RPCStatusCode.NOT_FOUND:
            return {
                "project": request.project,
                "phase": "Missing",
                "ok": True,
                "workflowID": workflow_id,
                "message": "Canary DemoProject workflow was already absent.",
                "checkedAt": datetime.now(UTC).isoformat(),
            }
        raise
    return {
        "project": request.project,
        "phase": "Signaled",
        "ok": True,
        "workflowID": workflow_id,
        "message": "Canary DemoProject workflow was signaled to finalize.",
        "checkedAt": datetime.now(UTC).isoformat(),
    }


@activity.defn
def wait_registry_operator_canary_cleanup(
    request: WaitRegistryOperatorCanaryCleanupRequest,
) -> RegistryOperatorCanaryActivityResult:
    plan = request.as_plan()
    if plan.get("cleanupPolicy") == "Never":
        return RegistryOperatorCanaryActivityResult({
            "project": string_field(plan, "project"),
            "phase": "Skipped",
            "ok": True,
            "message": "Canary cleanup skipped by cleanupPolicy=Never.",
            "checkedAt": datetime.now(UTC).isoformat(),
        })

    deadline = time.monotonic() + int(plan.get("cleanupTimeoutSeconds", 600))
    poll_seconds = max(1, int(plan.get("pollSeconds", 10)))
    delete_action = plan.get("deleteAction")
    last_status: dict[str, Any] | None = None
    while True:
        last_status = canary_cleanup_snapshot(plan)
        if delete_action is not None:
            last_status = {
                **last_status,
                "deleteAction": delete_action,
            }
        if last_status["ok"]:
            return RegistryOperatorCanaryActivityResult(last_status)

        if time.monotonic() >= deadline:
            return RegistryOperatorCanaryActivityResult({
                **last_status,
                "phase": "CleanupFailed",
                "ok": False,
                "message": "Timed out waiting for canary cleanup to complete.",
                "checkedAt": datetime.now(UTC).isoformat(),
            })

        activity.heartbeat(last_status)
        time.sleep(poll_seconds)


def canary_cleanup_snapshot(plan: dict[str, Any]) -> dict[str, Any]:
    namespace = string_field(plan, "namespace")
    git_repository = string_field(plan, "gitRepository")
    source_watch_namespace = string_field(plan, "sourceWatchNamespace")
    last_namespace = kube.read_namespace(namespace)
    last_runtime_namespaces = canary_runtime_namespaces(plan)
    last_source = kube.read_git_repository(source_watch_namespace, git_repository)
    last_project = kube.read_demo_project(string_field(plan, "project"))
    last_catalog = catalog_cleanup_status(plan)
    project_finalizer_removed = (
        last_project is None
        or not kube.demo_project_finalizers(last_project)
    )
    waiting_on = []
    if last_namespace is not None:
        waiting_on.append("namespace")
    if last_runtime_namespaces:
        waiting_on.append("runtimeNamespaces")
    if last_source is not None:
        waiting_on.append("source")
    if not project_finalizer_removed:
        waiting_on.append("finalizer")
    if not last_catalog["ok"]:
        waiting_on.append("catalog")

    if not waiting_on:
        return {
            "project": string_field(plan, "project"),
            "phase": "Deleted",
            "ok": True,
            "message": "Canary namespaces, source watch, finalizer, and catalog status were cleaned up.",
            "catalog": last_catalog,
            "checkedAt": datetime.now(UTC).isoformat(),
        }

    return {
        "project": string_field(plan, "project"),
        "phase": "CleanupPending",
        "ok": False,
        "message": "Canary cleanup is still waiting for resources to clear.",
        "waitingOn": waiting_on,
        "namespace": last_namespace,
        "runtimeNamespaces": last_runtime_namespaces,
        "demoProject": last_project,
        "source": last_source,
        "catalog": last_catalog,
        "checkedAt": datetime.now(UTC).isoformat(),
    }


def canary_runtime_namespaces(plan: dict[str, Any]) -> list[dict[str, Any]]:
    project = string_field(plan, "project")
    control_namespace = string_field(plan, "namespace")
    label_selector = ",".join(
        [
            f"app.kubernetes.io/name={project}",
            "app.kubernetes.io/managed-by=demo-registry",
            f"{resources.RUNTIME_NAMESPACE_LABEL}=true",
        ]
    )
    namespaces = []
    for namespace in kube.list_namespaces(label_selector=label_selector):
        name = namespace.get("name")
        if isinstance(name, str) and name and name != control_namespace:
            namespaces.append(namespace)
    return sorted(namespaces, key=lambda namespace: namespace.get("name", ""))


def render_canary_demo_project(
    template: dict[str, Any],
    *,
    project_name: str,
    expected_source_sha: Any,
    expected_images: Any,
    auth_mode: Any = None,
) -> dict[str, Any]:
    demo_project = copy.deepcopy(template)
    metadata = demo_project.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    annotations = metadata.get("annotations")
    if not isinstance(annotations, dict):
        annotations = {}
    labels = metadata.get("labels")
    if not isinstance(labels, dict):
        labels = {}
    if isinstance(expected_source_sha, str) and expected_source_sha:
        annotations[resources.SOURCE_COMMIT_ANNOTATION] = expected_source_sha
    if isinstance(expected_images, dict) and expected_images:
        annotations[resources.IMAGE_OVERRIDES_ANNOTATION] = json_dumps(expected_images)
    labels[resources.CANARY_LABEL] = "true"
    labels[resources.CANARY_KIND_LABEL] = resources.OPERATOR_ROLLOUT_CANARY_KIND
    metadata["name"] = project_name
    metadata["annotations"] = annotations
    metadata["labels"] = labels
    demo_project["metadata"] = metadata
    demo_project.setdefault("apiVersion", "registry.tmprl-demo.cloud/v1alpha1")
    demo_project.setdefault("kind", "DemoProject")
    spec = demo_project.get("spec")
    if isinstance(auth_mode, str) and auth_mode:
        if not isinstance(spec, dict):
            spec = {}
            demo_project["spec"] = spec
        temporal = spec.get("temporal")
        temporal = temporal if isinstance(temporal, dict) else {}
        auth = temporal.get("auth")
        auth = auth if isinstance(auth, dict) else {}
        auth["mode"] = auth_mode
        temporal["auth"] = auth
        spec["temporal"] = temporal
    ingress = spec.get("ingress") if isinstance(spec, dict) else None
    if isinstance(ingress, dict):
        ingress["subdomain"] = project_name
    return demo_project


@activity.defn
def signal_registry_operator_canary_reconcile(
    request: SignalRegistryOperatorCanaryReconcileRequest,
) -> RegistryOperatorCanaryActivityResult:
    return RegistryOperatorCanaryActivityResult(
        asyncio.run(signal_registry_operator_canary_reconcile_async(request))
    )


async def signal_registry_operator_canary_reconcile_async(
    request: SignalRegistryOperatorCanaryReconcileRequest,
) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    deployment_name = request.target_deployment_name
    build_id = request.target_build_id
    demo_project = request.demo_project
    name = resources.project_name(demo_project)
    workflow_id = derive.demo_project_workflow_id(name, config.cluster)
    client = await connect(config)
    handle = await client.start_workflow(
        "DemoProjectWorkflow",
        args=[messages.DemoProjectRunRequest(project_name=name)],
        id=workflow_id,
        task_queue=config.task_queue,
        start_signal="reconcile",
        start_signal_args=[
            messages.DemoProjectReconcileSignal(demoProject=demo_project),
        ],
        versioning_override=PinnedVersioningOverride(
            WorkerDeploymentVersion(
                deployment_name=deployment_name,
                build_id=build_id,
            )
        ),
    )
    return {
        "phase": "Signaled",
        "workflowID": workflow_id,
        "runID": handle.first_execution_run_id,
        "targetDeploymentName": deployment_name,
        "targetBuildID": build_id,
    }


def canary_promoted(plan: dict[str, Any], status: dict[str, Any]) -> bool:
    if status.get("phase") not in {"Promoted", "Ready"}:
        return False
    expected_source = plan.get("expectedSourceCommitSha")
    candidate = status.get("candidateRolloutStatus", {})
    if isinstance(expected_source, str) and expected_source:
        if not isinstance(candidate, dict):
            return False
        if candidate.get("sourceCommitSha") != expected_source:
            return False

    expected_images = plan.get("expectedImages")
    if isinstance(expected_images, dict) and expected_images:
        active_images = status.get("activeImages")
        if not isinstance(active_images, dict):
            return False
        for build_name, image in expected_images.items():
            if active_images.get(build_name) != image:
                return False
    return True


def verify_promoted_workload_resources(
    plan: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    namespace = promoted_workload_namespace(plan, status)
    candidate = status.get("candidateRolloutStatus")
    candidate = candidate if isinstance(candidate, dict) else {}
    deployments = candidate.get("deployments")
    services = status.get("activeRouteServices") or candidate.get("services")
    deployments = deployments if isinstance(deployments, dict) else {}
    services = services if isinstance(services, dict) else {}

    missing: list[dict[str, str]] = []
    deployment_statuses = {}
    for component, name in sorted(deployments.items()):
        if not isinstance(component, str) or not isinstance(name, str):
            continue
        try:
            deployment_statuses[component] = kube.read_deployment_status(namespace, name)
        except Exception as error:
            missing.append(
                {
                    "kind": "Deployment",
                    "component": component,
                    "name": name,
                    "message": str(error),
                }
            )

    service_statuses = {}
    for component, name in sorted(services.items()):
        if not isinstance(component, str) or not isinstance(name, str):
            continue
        try:
            service = kube.read_service(namespace, name)
        except Exception as error:
            service = None
            missing.append(
                {
                    "kind": "Service",
                    "component": component,
                    "name": name,
                    "message": str(error),
                }
            )
        if service is None:
            missing.append(
                {
                    "kind": "Service",
                    "component": component,
                    "name": name,
                    "message": "Service was not found.",
                }
            )
        else:
            service_statuses[component] = service

    return {
        "ok": not missing,
        "message": (
            "Promoted workload resources exist."
            if not missing
            else "Promoted workload resources are missing."
        ),
        "deployments": deployment_statuses,
        "services": service_statuses,
        "missing": missing,
    }


def promoted_workload_namespace(
    plan: dict[str, Any],
    status: dict[str, Any],
) -> str:
    candidate = status.get("candidateRolloutStatus")
    candidate = candidate if isinstance(candidate, dict) else {}
    namespace = status.get("activeRuntimeNamespace") or candidate.get("runtimeNamespace")
    return namespace if isinstance(namespace, str) and namespace else string_field(
        plan,
        "namespace",
    )


def catalog_cleanup_status(plan: dict[str, Any]) -> dict[str, Any]:
    namespaces = [
        string_field(plan, "catalogStatusNamespace"),
        string_field(plan, "catalogStatusMirrorNamespace"),
    ]
    configmap_name = string_field(plan, "catalogStatusConfigMap")
    key = f"{string_field(plan, 'project')}.json"
    entries = []
    for namespace in sorted(set(namespaces)):
        configmap = kube.read_config_map(namespace, configmap_name)
        entry = catalog_entry_from_configmap(configmap, key)
        entries.append(
            {
                "namespace": namespace,
                "configMap": configmap_name,
                "key": key,
                "phase": entry.get("phase") if isinstance(entry, dict) else None,
                "present": entry is not None,
                "ok": catalog_entry_finalized_or_absent(entry),
                **({"entry": entry} if isinstance(entry, dict) else {}),
            }
        )

    return {
        "ok": all(entry["ok"] for entry in entries),
        "entries": entries,
    }


def catalog_entry_from_configmap(
    configmap: dict[str, Any] | None,
    key: str,
) -> dict[str, Any] | None:
    data = configmap.get("data") if isinstance(configmap, dict) else None
    if not isinstance(data, dict) or key not in data:
        return None
    raw = data.get(key)
    if not isinstance(raw, str):
        return None
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {"phase": "Invalid", "message": "Catalog status JSON is invalid."}
    return decoded if isinstance(decoded, dict) else None


def catalog_entry_finalized_or_absent(entry: dict[str, Any] | None) -> bool:
    if entry is None:
        return True
    if entry.get("phase") != "Finalized":
        return False
    finalize_status = entry.get("finalizeStatus")
    if not isinstance(finalize_status, dict):
        return False
    external = finalize_status.get("externalResources")
    if not isinstance(external, dict):
        return False
    return external.get("phase") in {"Deleted", "Skipped"}


def canary_project_name(prefix: str, workflow_id: str, run_id: str) -> str:
    digest = hashlib.sha256(f"{workflow_id}:{run_id}".encode("utf-8")).hexdigest()[:8]
    return resources.dns_label(f"{prefix}-{digest}")


def demo_project_from_plan(plan: dict[str, Any]) -> dict[str, Any]:
    demo_project = plan.get("demoProject")
    if not isinstance(demo_project, dict):
        raise errors.non_retryable_error(
            "Canary plan is missing demoProject",
            error_type=errors.VALIDATION_ERROR,
        )
    return demo_project


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field:
        return field
    raise errors.non_retryable_error(
        f"Canary request is missing {key}",
        error_type=errors.VALIDATION_ERROR,
    )


def string_value(value: dict[str, Any], key: str, *, default: str) -> str:
    field = value.get(key)
    return field if isinstance(field, str) and field else default


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def uuid_token() -> str:
    return uuid.uuid4().hex
