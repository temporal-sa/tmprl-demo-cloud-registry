from __future__ import annotations

import time
from typing import Any

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    CONFIGURATION_ERROR,
    IMAGE_BUILD_TIMEOUT,
    NON_RETRYABLE_AWS_ERROR_CODES,
    VALIDATION_ERROR,
    aws_application_error,
    aws_error_code,
    non_retryable_error,
)
from demo_registry_operator.resources import builds, project_name, source_commit_sha

from demo_registry_operator.activities.builds.definitions import (
    looks_like_commit_sha,
    render_image_templates,
)
from demo_registry_operator.activities.builds.source import (
    demo_project_from_request,
    external_resources_from_request,
)
from demo_registry_operator.activities.builds.status import (
    build_entry,
    build_status,
    codebuild_build_entries,
    completed_build_plan,
    completed_build_status,
    image_templates_from_plan,
    optional_string_field,
    string_field,
)
from demo_registry_operator.activities.builds.codebuild.buildspec import (
    codebuild_start_build_args,
)


CODEBUILD_API_TIMEOUT_SECONDS = 10


@activity.defn
def start_project_image_build(plan: dict[str, Any]) -> dict[str, Any]:
    """Start CodeBuild or reattach to a build ID heartbeated by a prior attempt."""

    demo_project = demo_project_from_request(plan)
    external_resources = external_resources_from_request(plan)
    config = WorkerConfig.from_env()
    started_at = string_field(plan, "startedAt")
    image_templates = image_templates_from_plan(plan)
    source_repo = string_field(plan, "sourceRepo")
    source_version = string_field(plan, "sourceVersion")
    source_auth_secret_path = optional_string_field(plan, "sourceAuthSecretPath")
    codebuild_build: dict[str, Any] | None = None

    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return completed_build_plan(
            build_status(
                demo_project,
                phase="Error",
                mode=config.build_mode,
                started_at=started_at,
                images={},
                source_commit_sha=source_commit_sha(demo_project),
                message="AWS CodeBuild image build failed.",
                build_entries=[],
                error=f"boto3 is required for aws-codebuild mode: {error}",
            )
        )

    codebuild = boto3.session.Session(region_name=config.aws_region).client(
        "codebuild",
        config=Config(
            connect_timeout=CODEBUILD_API_TIMEOUT_SECONDS,
            read_timeout=CODEBUILD_API_TIMEOUT_SECONDS,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )
    build_id = codebuild_build_id_from_heartbeat()
    if build_id:
        activity.heartbeat(
            {
                "project": project_name(demo_project),
                "codebuildProject": config.codebuild_project,
                "codebuildBuildId": build_id,
                "phase": "Reattaching",
            }
        )
        return {**plan, "codebuildBuildId": build_id}

    start_args = codebuild_start_build_args(
        demo_project,
        config,
        image_templates=image_templates,
        source_repo=source_repo,
        source_version=source_version,
        source_auth_secret_path=source_auth_secret_path,
        idempotency_token_salt=started_at,
    )
    activity.heartbeat(
        {
            "project": project_name(demo_project),
            "codebuildProject": config.codebuild_project,
            "phase": "Starting",
        }
    )
    try:
        response = codebuild.start_build(**start_args)
    except (BotoCoreError, ClientError) as error:
        if aws_error_code(error) in NON_RETRYABLE_AWS_ERROR_CODES:
            return codebuild_api_error_status(
                plan,
                message="AWS CodeBuild image build failed.",
                error=str(error),
                codebuild_status=codebuild_status_summary(
                    codebuild_build,
                    project_name=config.codebuild_project,
                ),
            )
        raise aws_application_error(error, operation="Start CodeBuild build") from error

    build = response.get("build", {})
    codebuild_build = build if isinstance(build, dict) else None
    build_id = build.get("id") if isinstance(build, dict) else None
    if not isinstance(build_id, str):
        raise non_retryable_error(
            "CodeBuild start_build response did not include build.id",
            error_type=CONFIGURATION_ERROR,
        )
    activity.heartbeat(
        {
            "project": project_name(demo_project),
            "codebuildProject": config.codebuild_project,
            "codebuildBuildId": build_id,
            "phase": "Started",
        }
    )
    return {
        **plan,
        "codebuildBuildId": build_id,
        "codebuild": codebuild_status_summary(
            codebuild_build,
            project_name=config.codebuild_project,
        ),
    }


@activity.defn
def wait_for_project_image_build(started: dict[str, Any]) -> dict[str, Any]:
    """Wait for a specific CodeBuild build and render the immutable image status."""

    demo_project = demo_project_from_request(started)
    external_resources = external_resources_from_request(started)
    config = WorkerConfig.from_env()
    started_at = string_field(started, "startedAt")
    image_templates = image_templates_from_plan(started)
    build_id = string_field(started, "codebuildBuildId")
    requested_version = string_field(started, "sourceVersion")
    codebuild_build: dict[str, Any] | None = None

    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return build_status(
            demo_project,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images={},
            source_commit_sha=source_commit_sha(demo_project),
            message="AWS CodeBuild image build failed.",
            build_entries=[],
            error=f"boto3 is required for aws-codebuild mode: {error}",
        )

    codebuild = boto3.session.Session(region_name=config.aws_region).client(
        "codebuild",
        config=Config(
            connect_timeout=CODEBUILD_API_TIMEOUT_SECONDS,
            read_timeout=CODEBUILD_API_TIMEOUT_SECONDS,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )
    try:
        codebuild_build = wait_for_codebuild_build(
            codebuild,
            build_id,
            config=config,
            demo_project=demo_project,
        )
    except TimeoutError as error:
        raise non_retryable_error(str(error), error_type=IMAGE_BUILD_TIMEOUT) from error
    except (BotoCoreError, ClientError) as error:
        if aws_error_code(error) in NON_RETRYABLE_AWS_ERROR_CODES:
            status = completed_build_status(
                codebuild_api_error_status(
                    started,
                    message="AWS CodeBuild image build failed.",
                    error=str(error),
                    codebuild_status=codebuild_status_summary(
                        codebuild_build,
                        project_name=config.codebuild_project,
                    ),
                )
            )
            if status is not None:
                return status
            return build_status(
                demo_project,
                phase="Error",
                mode=config.build_mode,
                started_at=started_at,
                images={},
                source_commit_sha=source_commit_sha(demo_project),
                message="AWS CodeBuild image build failed.",
                build_entries=[],
                error=str(error),
            )
        raise aws_application_error(error, operation="Poll CodeBuild build") from error

    status = codebuild_build.get("buildStatus")
    resolved_commit = resolved_codebuild_source_version(
        codebuild_build,
        requested_version,
    )
    codebuild_status = codebuild_status_summary(
        codebuild_build,
        project_name=config.codebuild_project,
    )
    if status != "SUCCEEDED":
        return build_status(
            demo_project,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images={},
            source_commit_sha=resolved_commit,
            message="AWS CodeBuild image build failed.",
            build_entries=codebuild_build_entries(
                demo_project,
                image_templates,
                source_commit_sha=resolved_commit,
                external_resources=external_resources,
                phase="Failed",
            ),
            error=codebuild_error_message(codebuild_build),
            codebuild_status=codebuild_status,
        )

    if not resolved_commit or not looks_like_commit_sha(resolved_commit):
        return build_status(
            demo_project,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images={},
            source_commit_sha=resolved_commit,
            message="AWS CodeBuild image build did not return an immutable source commit.",
            build_entries=[],
            error=(
                "CodeBuild completed but resolvedSourceVersion was not a git "
                "commit SHA; annotate registry.tmprl-demo.cloud/source-commit-sha "
                "or verify the CodeBuild source provider."
            ),
            codebuild_status=codebuild_status,
        )

    images = render_image_templates(demo_project, image_templates, resolved_commit)
    return build_status(
        demo_project,
        phase="Pushed",
        mode=config.build_mode,
        started_at=started_at,
        images=images,
        source_commit_sha=resolved_commit,
        message="AWS CodeBuild built and pushed immutable images for every build.",
        build_entries=[
            build_entry(
                demo_project,
                build_name,
                image=images[build_name],
                phase="Pushed",
                source="aws-codebuild",
                external_resources=external_resources,
            )
            for build_name in sorted(builds(demo_project))
        ],
        codebuild_status=codebuild_status,
    )


def codebuild_api_error_status(
    plan: dict[str, Any],
    *,
    message: str,
    error: str,
    codebuild_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    demo_project = demo_project_from_request(plan)
    config = WorkerConfig.from_env()
    return completed_build_plan(
        build_status(
            demo_project,
            phase="Error",
            mode=str(plan.get("mode") or config.build_mode),
            started_at=string_field(plan, "startedAt"),
            images={},
            source_commit_sha=source_commit_sha(demo_project),
            message=message,
            build_entries=[],
            error=error,
            codebuild_status=codebuild_status,
        )
    )


def codebuild_build_id_from_heartbeat() -> str | None:
    details = activity.info().heartbeat_details
    if not details:
        return None
    latest = details[0]
    if not isinstance(latest, dict):
        return None
    build_id = latest.get("codebuildBuildId")
    return build_id if isinstance(build_id, str) and build_id else None


def codebuild_status_summary(
    build: dict[str, Any] | None,
    *,
    project_name: str | None,
) -> dict[str, Any] | None:
    if not isinstance(build, dict):
        if project_name:
            return {
                "projectName": project_name,
                "phase": "Unknown",
            }
        return None

    summary: dict[str, Any] = {
        "projectName": project_name,
        "buildId": build.get("id"),
        "arn": build.get("arn"),
        "buildNumber": build.get("buildNumber"),
        "phase": build.get("buildStatus"),
        "sourceVersion": build.get("sourceVersion"),
        "resolvedSourceVersion": build.get("resolvedSourceVersion"),
        "startedAt": isoformat_value(build.get("startTime")),
        "completedAt": isoformat_value(build.get("endTime")),
    }
    logs = build.get("logs")
    if isinstance(logs, dict):
        summary["logs"] = {
            "groupName": logs.get("groupName"),
            "streamName": logs.get("streamName"),
            "deepLink": logs.get("deepLink"),
        }
    return {
        key: value
        for key, value in summary.items()
        if value not in (None, {}, [])
    }


def isoformat_value(value: Any) -> str | None:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value if isinstance(value, str) else None


def wait_for_codebuild_build(
    codebuild: Any,
    build_id: str,
    *,
    config: WorkerConfig,
    demo_project: dict[str, Any],
) -> dict[str, Any]:
    deadline = time.monotonic() + (config.codebuild_timeout_minutes * 60)
    while True:
        response = codebuild.batch_get_builds(ids=[build_id])
        builds_response = response.get("builds", [])
        build = builds_response[0] if builds_response else {}
        status = build.get("buildStatus")
        activity.heartbeat(
            {
                "project": project_name(demo_project),
                "codebuildBuildId": build_id,
                "status": status,
            }
        )
        if status in {"SUCCEEDED", "FAILED", "FAULT", "STOPPED", "TIMED_OUT"}:
            return build
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for CodeBuild build {build_id}")
        time.sleep(config.codebuild_poll_seconds)


def resolved_codebuild_source_version(
    build: dict[str, Any],
    fallback: str | None,
) -> str | None:
    resolved = build.get("resolvedSourceVersion")
    if isinstance(resolved, str) and resolved:
        return resolved
    return fallback if fallback and looks_like_commit_sha(fallback) else None


def codebuild_error_message(build: dict[str, Any]) -> str:
    phases = build.get("phases")
    if isinstance(phases, list):
        for phase in reversed(phases):
            if not isinstance(phase, dict):
                continue
            contexts = phase.get("contexts")
            if not isinstance(contexts, list):
                continue
            for context in contexts:
                if isinstance(context, dict) and isinstance(context.get("message"), str):
                    return context["message"]
    status = build.get("buildStatus", "UNKNOWN")
    return f"CodeBuild build finished with status {status}."
