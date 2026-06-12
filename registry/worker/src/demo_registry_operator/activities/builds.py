from __future__ import annotations

import hashlib
import json
import shlex
import time
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from demo_registry_operator import derive
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
from demo_registry_operator.resources import (
    builds,
    components,
    missing_image_overrides,
    parse_image_overrides,
    project_name,
    source_artifact_digest,
    source_auth_secret_name,
    source_branch,
    source_commit_sha,
    source_git_repository_url,
    spec_of,
    user_secret_remote_path,
)


CODEBUILD_API_TIMEOUT_SECONDS = 10
DOCKERFILE_BUILD_STRATEGY = "dockerfile"
BUILDPACK_BUILD_STRATEGY = "buildpack"
PACK_VERSION = "0.40.6"
BUILDPACK_IMPLEMENTATION_VERSION = "buildpack-post-processes-v1"
GENERATED_PROCESS_BUILDPACK_ID = "tmprl-demo/generated-processes"


@activity.defn
def plan_project_images(request: dict[str, Any]) -> dict[str, Any]:
    """Resolve cached/override images or prepare an AWS CodeBuild request."""

    try:
        demo_project = demo_project_from_request(request)
    except ValueError as error:
        raise non_retryable_error(str(error), error_type=VALIDATION_ERROR) from error
    external_resources = external_resources_from_request(request)
    config = WorkerConfig.from_env()
    started_at = datetime.now(UTC).isoformat()

    image_overrides = parse_image_overrides(demo_project)
    missing_images = missing_image_overrides(demo_project, image_overrides)
    previous_build_status = previous_build_status_from_request(request)

    if not missing_images:
        status = build_status(
            demo_project,
            phase="Resolved",
            mode="image-overrides",
            started_at=started_at,
            images=image_overrides,
            source_commit_sha=source_commit_sha(demo_project),
            message="Immutable image overrides are present for every build.",
            build_entries=[
                build_entry(
                    demo_project,
                    build_name,
                    image=image_overrides[build_name],
                    phase="Resolved",
                    source="image-overrides",
                    external_resources=external_resources,
                )
                for build_name in sorted(builds(demo_project))
            ],
        )
        return completed_build_plan(status)

    reusable_build_status = reusable_previous_build_status(
        demo_project,
        previous_build_status,
        external_resources,
        started_at=started_at,
    )
    if reusable_build_status is not None:
        return completed_build_plan(reusable_build_status)

    if config.build_mode == "aws-codebuild":
        if not config.codebuild_project:
            return completed_build_plan(
                build_status(
                    demo_project,
                    phase="Error",
                    mode=config.build_mode,
                    started_at=started_at,
                    images={},
                    source_commit_sha=source_commit_sha(demo_project),
                    message="AWS CodeBuild project is not configured.",
                    build_entries=[],
                    error=(
                        "Set DEMO_REGISTRY_CODEBUILD_PROJECT to use "
                        "aws-codebuild mode."
                    ),
                )
            )

        try:
            source = source_config(demo_project)
            requested_version = source_commit_sha(demo_project) or source["branch"]
            image_templates = codebuild_image_templates(
                demo_project,
                external_resources,
                config,
            )
        except ValueError as error:
            return completed_build_plan(
                build_status(
                    demo_project,
                    phase="Error",
                    mode=config.build_mode,
                    started_at=started_at,
                    images={},
                    source_commit_sha=source_commit_sha(demo_project),
                    message="AWS CodeBuild image build cannot start.",
                    build_entries=[],
                    error=str(error),
                )
            )

        return {
            "action": "codebuild",
            "demoProject": demo_project,
            "externalResources": external_resources,
            "startedAt": started_at,
            "mode": config.build_mode,
            "codebuildProject": config.codebuild_project,
            "imageTemplates": image_templates,
            "sourceRepo": source.get("codebuildRepo", source["repo"]),
            "sourceVersion": requested_version,
            **(
                {"sourceAuthSecretPath": source["sourceAuthSecretPath"]}
                if "sourceAuthSecretPath" in source
                else {}
            ),
        }

    return completed_build_plan(
        build_status(
            demo_project,
            phase="Planned",
            mode=config.build_mode,
            started_at=started_at,
            images=image_overrides,
            source_commit_sha=source_commit_sha(demo_project),
            message=(
                "Build plan recorded without building images. Set "
                "DEMO_REGISTRY_BUILD_MODE=aws-codebuild to build and push images."
            ),
            missing_images=missing_images,
            build_entries=[
                build_entry(
                    demo_project,
                    build_name,
                    image=image_overrides.get(build_name),
                    phase="Resolved" if build_name in image_overrides else "Planned",
                    source=(
                        "image-overrides"
                        if build_name in image_overrides
                        else "build-plan"
                    ),
                    external_resources=external_resources,
                )
                for build_name in sorted(builds(demo_project))
            ],
        )
    )


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


def build_project_images(request: dict[str, Any]) -> dict[str, Any]:
    """Compatibility wrapper for direct callers; workflows use split activities."""

    plan = plan_project_images(request)
    if completed_status := completed_build_status(plan):
        return completed_status
    started = start_project_image_build(plan)
    if completed_status := completed_build_status(started):
        return completed_status
    return wait_for_project_image_build(started)


def completed_build_plan(status: dict[str, Any]) -> dict[str, Any]:
    return {"action": "complete", "status": status}


def completed_build_status(value: dict[str, Any]) -> dict[str, Any] | None:
    status = value.get("status")
    return (
        status
        if value.get("action") == "complete" and isinstance(status, dict)
        else None
    )


def previous_build_status_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    previous_build_status = request.get("previousBuildStatus")
    if isinstance(previous_build_status, dict):
        return previous_build_status
    return None


def reusable_previous_build_status(
    demo_project: dict[str, Any],
    previous_build_status: dict[str, Any] | None,
    external_resources: dict[str, Any] | None,
    *,
    started_at: str,
) -> dict[str, Any] | None:
    if not isinstance(previous_build_status, dict):
        return None
    if previous_build_status.get("phase") not in {"Pushed", "Resolved"}:
        return None
    if previous_build_status.get("lastError"):
        return None
    if previous_build_status.get("buildDefinitionHash") != build_definition_hash(
        demo_project
    ):
        return None

    requested_digest = source_artifact_digest(demo_project)
    previous_digest = previous_build_status.get("sourceArtifactDigest")
    if not (
        isinstance(requested_digest, str)
        and requested_digest
        and previous_digest == requested_digest
    ):
        requested_commit = source_commit_sha(demo_project)
        previous_commit = previous_build_status.get("sourceCommitSha")
        if not same_commit_sha(previous_commit, requested_commit):
            return None

    previous_images = previous_build_status.get("images")
    if not isinstance(previous_images, dict):
        return None

    images: dict[str, str] = {}
    for build_name in sorted(builds(demo_project)):
        image = previous_images.get(build_name)
        if not isinstance(image, str) or not image:
            return None
        images[build_name] = image

    return build_status(
        demo_project,
        phase="Resolved",
        mode="workflow-cache",
        started_at=started_at,
        images=images,
        source_commit_sha=source_commit_sha(demo_project),
        message=(
            "Source artifact was already built by this entity workflow; reusing "
            "immutable images."
        ),
        build_entries=[
            build_entry(
                demo_project,
                build_name,
                image=images[build_name],
                phase="Resolved",
                source="workflow-cache",
                external_resources=external_resources,
            )
            for build_name in sorted(builds(demo_project))
        ],
    )


def same_commit_sha(left: Any, right: Any) -> bool:
    if not isinstance(left, str) or not isinstance(right, str):
        return False
    if not looks_like_commit_sha(left) or not looks_like_commit_sha(right):
        return False
    left = left.lower()
    right = right.lower()
    return left == right or left.startswith(right) or right.startswith(left)


def codebuild_image_templates(
    demo_project: dict[str, Any],
    external_resources: dict[str, Any] | None,
    config: WorkerConfig,
) -> dict[str, str]:
    image_templates: dict[str, str] = {}
    for build_name in sorted(builds(demo_project)):
        repository_uri = repository_uri_for_build(build_name, external_resources)
        if not repository_uri:
            if not config.aws_account_id:
                raise ValueError(
                    "DEMO_REGISTRY_AWS_ACCOUNT_ID is required when ECR "
                    "repositoryUri is not available from external resource status."
                )
            repository_uri = (
                f"{config.aws_account_id}.dkr.ecr.{config.aws_region}.amazonaws.com/"
                f"{repository_name_for_build(demo_project, build_name, external_resources)}"
            )
        image_templates[build_name] = f"{repository_uri}:${{IMAGE_VERSION}}"
    return image_templates


def image_templates_from_plan(plan: dict[str, Any]) -> dict[str, str]:
    image_templates = plan.get("imageTemplates")
    if not isinstance(image_templates, dict):
        raise non_retryable_error(
            "CodeBuild image plan is missing imageTemplates",
            error_type=VALIDATION_ERROR,
        )
    rendered = {
        str(build_name): str(image_template)
        for build_name, image_template in image_templates.items()
        if isinstance(build_name, str)
        and isinstance(image_template, str)
        and image_template
    }
    if not rendered:
        raise non_retryable_error(
            "CodeBuild image plan has no image templates",
            error_type=VALIDATION_ERROR,
        )
    return rendered


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field:
        return field
    raise non_retryable_error(
        f"CodeBuild image plan is missing {key}",
        error_type=VALIDATION_ERROR,
    )


def optional_string_field(value: dict[str, Any], key: str) -> str | None:
    field = value.get(key)
    return field if isinstance(field, str) and field else None


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


def build_with_codebuild(
    demo_project: dict[str, Any],
    external_resources: dict[str, Any] | None,
    config: WorkerConfig,
    *,
    started_at: str,
) -> dict[str, Any]:
    if not config.codebuild_project:
        return build_status(
            demo_project,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images={},
            source_commit_sha=source_commit_sha(demo_project),
            message="AWS CodeBuild project is not configured.",
            build_entries=[],
            error="Set DEMO_REGISTRY_CODEBUILD_PROJECT to use aws-codebuild mode.",
        )

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

    source = source_config(demo_project)
    requested_version = source_commit_sha(demo_project) or source["branch"]
    image_templates: dict[str, str] = {}
    codebuild_build: dict[str, Any] | None = None
    try:
        for build_name in sorted(builds(demo_project)):
            repository_uri = repository_uri_for_build(build_name, external_resources)
            if not repository_uri:
                if not config.aws_account_id:
                    raise ValueError(
                        "DEMO_REGISTRY_AWS_ACCOUNT_ID is required when ECR "
                        "repositoryUri is not available from external resource status."
                    )
                repository_uri = (
                    f"{config.aws_account_id}.dkr.ecr.{config.aws_region}.amazonaws.com/"
                    f"{repository_name_for_build(demo_project, build_name, external_resources)}"
                )
            image_templates[build_name] = f"{repository_uri}:${{IMAGE_VERSION}}"

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
        else:
            start_args = codebuild_start_build_args(
                demo_project,
                config,
                image_templates=image_templates,
                source_repo=source.get("codebuildRepo", source["repo"]),
                source_version=requested_version,
                source_auth_secret_path=source.get("sourceAuthSecretPath"),
                idempotency_token_salt=started_at,
            )
            activity.heartbeat(
                {
                    "project": project_name(demo_project),
                    "codebuildProject": config.codebuild_project,
                    "phase": "Starting",
                }
            )
            response = codebuild.start_build(**start_args)
            build = response.get("build", {})
            codebuild_build = build if isinstance(build, dict) else None
            build_id = build.get("id")
            if not isinstance(build_id, str):
                raise RuntimeError(
                    "CodeBuild start_build response did not include build.id"
                )
            activity.heartbeat(
                {
                    "project": project_name(demo_project),
                    "codebuildProject": config.codebuild_project,
                    "codebuildBuildId": build_id,
                    "phase": "Started",
                }
            )

        codebuild_build = wait_for_codebuild_build(
            codebuild,
            build_id,
            config=config,
            demo_project=demo_project,
        )
    except (BotoCoreError, ClientError, Exception) as error:
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
            codebuild_status=codebuild_status_summary(
                codebuild_build,
                project_name=config.codebuild_project,
            ),
        )

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


def codebuild_build_id_from_heartbeat() -> str | None:
    details = activity.info().heartbeat_details
    if not details:
        return None
    latest = details[0]
    if not isinstance(latest, dict):
        return None
    build_id = latest.get("codebuildBuildId")
    return build_id if isinstance(build_id, str) and build_id else None


def build_status(
    demo_project: dict[str, Any],
    *,
    phase: str,
    mode: str,
    started_at: str,
    images: dict[str, str],
    source_commit_sha: str | None,
    message: str,
    build_entries: list[dict[str, Any]],
    missing_images: list[str] | None = None,
    error: str | None = None,
    codebuild_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    completed_at = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "phase": phase,
        "mode": mode,
        "startedAt": started_at,
        "completedAt": completed_at,
        "message": message,
        "images": images,
        "missingImages": missing_images or [],
        "builds": build_entries,
        "buildDefinitionHash": build_definition_hash(demo_project),
    }
    if source_commit_sha:
        status["sourceCommitSha"] = source_commit_sha
        status["imageVersion"] = image_version(demo_project, source_commit_sha)
    artifact_digest = source_artifact_digest(demo_project)
    if artifact_digest:
        status["sourceArtifactDigest"] = artifact_digest
    if error:
        status["lastError"] = {
            "reason": "BuildFailed",
            "message": error,
            "lastTransitionTime": completed_at,
        }
    if codebuild_status:
        status["codebuild"] = codebuild_status
    return status


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


def codebuild_start_build_args(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    image_templates: dict[str, str],
    source_repo: str,
    source_version: str,
    source_auth_secret_path: str | None = None,
    idempotency_token_salt: str | None = None,
) -> dict[str, Any]:
    if not config.codebuild_project:
        raise ValueError("DEMO_REGISTRY_CODEBUILD_PROJECT is required")

    environment_variables = [
        {
            "name": "DEMO_PROJECT_NAME",
            "value": project_name(demo_project),
            "type": "PLAINTEXT",
        },
        {
            "name": "DEMO_REGISTRY_BUILD_MODE",
            "value": "aws-codebuild",
            "type": "PLAINTEXT",
        },
    ]
    start_args: dict[str, Any] = {
        "projectName": config.codebuild_project,
        "buildspecOverride": codebuild_buildspec(
            demo_project,
            image_templates,
            aws_region=config.aws_region,
            manual_source_checkout=bool(source_auth_secret_path),
        ),
        "environmentVariablesOverride": environment_variables,
        "privilegedModeOverride": True,
        "timeoutInMinutesOverride": config.codebuild_timeout_minutes,
        "idempotencyToken": codebuild_idempotency_token(
            demo_project,
            source_repo=source_repo,
            source_version=source_version,
            source_auth_secret_path=source_auth_secret_path,
            idempotency_token_salt=idempotency_token_salt,
            image_templates=image_templates,
        ),
    }

    if source_auth_secret_path:
        start_args["sourceTypeOverride"] = "NO_SOURCE"
        environment_variables.extend(
            [
                {
                    "name": "DEMO_SOURCE_REPO",
                    "value": source_repo,
                    "type": "PLAINTEXT",
                },
                {
                    "name": "DEMO_SOURCE_VERSION",
                    "value": source_version,
                    "type": "PLAINTEXT",
                },
                {
                    "name": "DEMO_SOURCE_AUTH_JSON",
                    "value": source_auth_secret_path,
                    "type": "SECRETS_MANAGER",
                },
            ]
        )
    else:
        start_args.update(
            {
                "sourceTypeOverride": "GITHUB",
                "sourceLocationOverride": source_repo,
                "sourceVersion": source_version,
            }
        )

    return start_args


def codebuild_buildspec(
    demo_project: dict[str, Any],
    image_templates: dict[str, str],
    *,
    aws_region: str,
    manual_source_checkout: bool = False,
) -> str:
    registries = sorted(
        {
            image_template.split("/", 1)[0]
            for image_template in image_templates.values()
            if "/" in image_template
        }
    )
    pre_build_commands = (
        manual_source_checkout_commands()
        if manual_source_checkout
        else [
            "SOURCE_SHA=${CODEBUILD_RESOLVED_SOURCE_VERSION:-${CODEBUILD_SOURCE_VERSION:-}}",
        ]
    )
    pre_build_commands.extend(
        [
            'SOURCE_SHA=$(printf "%.12s" "$SOURCE_SHA")',
            'test -n "$SOURCE_SHA"',
            f'IMAGE_VERSION="git-${{SOURCE_SHA}}{image_version_suffix(demo_project)}"',
        ]
    )
    prefetch_command = dockerhub_official_base_prefetch_command(
        demo_project,
        manual_source_checkout=manual_source_checkout,
    )
    if prefetch_command:
        pre_build_commands.append(prefetch_command)
    if any(
        build_strategy(build) == BUILDPACK_BUILD_STRATEGY
        for build in builds(demo_project).values()
    ):
        pre_build_commands.append(pack_install_command())
    pre_build_commands.extend(
        [
            (
                f"aws ecr get-login-password --region {shlex.quote(aws_region)} "
                f"| docker login --username AWS --password-stdin {shlex.quote(registry)}"
            )
            for registry in registries
        ]
    )

    build_commands = []
    source_prefix = "cd /tmp/source && " if manual_source_checkout else ""
    for build_name, build in sorted(builds(demo_project).items()):
        image = image_templates[build_name]
        shell_image = shell_quote_image_template(image)
        strategy = build_strategy(build)
        if strategy == DOCKERFILE_BUILD_STRATEGY:
            build_commands.append(
                source_prefix
                + " ".join(
                    [
                        "docker",
                        "build",
                        "-f",
                        shlex.quote(required_build_string(build, "dockerfile")),
                        "-t",
                        shell_image,
                        shlex.quote(required_build_string(build, "context")),
                    ]
                )
            )
            build_commands.append("docker push " + shell_image)
            continue

        if strategy == BUILDPACK_BUILD_STRATEGY:
            process_buildpack_command = buildpack_process_buildpack_command(
                build_name,
                build,
            )
            process_buildpack_path = buildpack_process_buildpack_path(build_name)
            if process_buildpack_command:
                build_commands.append(source_prefix + process_buildpack_command)
            command = [
                "pack",
                "build",
                shell_image,
                "--builder",
                shlex.quote(required_build_string(build, "builder")),
                "--path",
                shlex.quote(required_build_string(build, "context")),
            ]
            for name, value in sorted(buildpack_env(build).items()):
                command.extend(["--env", shlex.quote(f"{name}={value}")])
            if process_buildpack_command:
                command.extend(
                    [
                        "--post-buildpack",
                        shlex.quote(process_buildpack_path),
                    ]
                )
            build_commands.append(source_prefix + " ".join(command))
            build_commands.append("docker push " + shell_image)
            continue

        raise ValueError(f"unsupported build strategy {strategy!r}")

    return "\n".join(
        [
            "version: 0.2",
            "phases:",
            "  pre_build:",
            "    commands:",
            *[f"      - {json.dumps(command)}" for command in pre_build_commands],
            "  build:",
            "    commands:",
            *[f"      - {json.dumps(command)}" for command in build_commands],
            "",
        ]
    )


def pack_install_command() -> str:
    return (
        'arch="$(uname -m)"; '
        'case "$arch" in '
        "x86_64|amd64) pack_arch=linux ;; "
        "aarch64|arm64) pack_arch=linux-arm64 ;; "
        '*) echo "unsupported architecture for pack: $arch" >&2; exit 1 ;; '
        "esac; "
        "curl -sSL "
        f"https://github.com/buildpacks/pack/releases/download/v{PACK_VERSION}/"
        f"pack-v{PACK_VERSION}-${{pack_arch}}.tgz "
        "| tar -xz -C /usr/local/bin pack"
    )


def build_strategy(build: dict[str, Any]) -> str:
    strategy = build.get("strategy", DOCKERFILE_BUILD_STRATEGY)
    return (
        strategy
        if isinstance(strategy, str) and strategy
        else DOCKERFILE_BUILD_STRATEGY
    )


def required_build_string(build: dict[str, Any], field: str) -> str:
    value = build.get(field)
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"build is missing required {field!r}")


def buildpack_env(build: dict[str, Any]) -> dict[str, str]:
    env = build.get("env")
    if not isinstance(env, dict):
        return {}
    return {
        str(name): str(value)
        for name, value in env.items()
        if isinstance(name, str) and isinstance(value, str)
    }


def build_definition_hash(demo_project: dict[str, Any]) -> str:
    encoded = json.dumps(
        build_definition_input(demo_project),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_definition_input(demo_project: dict[str, Any]) -> dict[str, Any]:
    definition = json.loads(json.dumps(builds(demo_project)))
    if not isinstance(definition, dict):
        return {}
    for build in definition.values():
        if isinstance(build, dict) and build_strategy(build) == BUILDPACK_BUILD_STRATEGY:
            build["_registryBuildImplementation"] = BUILDPACK_IMPLEMENTATION_VERSION
    return definition


def buildpack_processes(build: dict[str, Any]) -> dict[str, str]:
    processes = build.get("processes")
    if not isinstance(processes, dict):
        return {}
    return {
        str(name): str(command)
        for name, command in processes.items()
        if isinstance(name, str)
        and name
        and isinstance(command, str)
        and command
    }


def buildpack_process_buildpack_path(build_name: str) -> str:
    safe_name = "".join(
        character if character.isalnum() or character in "._-" else "-"
        for character in build_name
    )
    return f"/tmp/tmprl-demo-generated-buildpacks/{safe_name or 'build'}"


def buildpack_process_buildpack_command(
    build_name: str,
    build: dict[str, Any],
) -> str | None:
    processes = buildpack_processes(build)
    if not processes:
        return None
    launch_toml = "".join(
        buildpack_process_toml(name, command)
        for name, command in sorted(processes.items())
    )
    buildpack_path = buildpack_process_buildpack_path(build_name)
    buildpack_toml = (
        'api = "0.10"\n\n'
        "[buildpack]\n"
        f'id = "{GENERATED_PROCESS_BUILDPACK_ID}"\n'
        'name = "tmprl-demo generated process types"\n'
        'version = "0.0.1"\n'
    )
    script = (
        "import os, pathlib; "
        f"root=pathlib.Path({json.dumps(buildpack_path)}); "
        "(root/'bin').mkdir(parents=True, exist_ok=True); "
        f"(root/'buildpack.toml').write_text({json.dumps(buildpack_toml)}, encoding='utf-8'); "
        "(root/'bin'/'detect').write_text('#!/bin/sh\\nexit 0\\n', encoding='utf-8'); "
        "(root/'bin'/'build').write_text("
        + json.dumps(
            "#!/bin/sh\n"
            "set -eu\n"
            "layers_dir=\"$1\"\n"
            "cat > \"$layers_dir/launch.toml\" <<'TMPRL_DEMO_LAUNCH_TOML'\n"
            f"{launch_toml}"
            "TMPRL_DEMO_LAUNCH_TOML\n"
        )
        + ", encoding='utf-8'); "
        "os.chmod(root/'bin'/'detect', 0o755); "
        "os.chmod(root/'bin'/'build', 0o755)"
    )
    return "python3 -c " + shlex.quote(script)


def buildpack_process_toml(name: str, command: str) -> str:
    return (
        "[[processes]]\n"
        f"type = {json.dumps(name)}\n"
        f"command = [{json.dumps('/bin/sh')}, {json.dumps('-c')}, {json.dumps(command)}]\n"
        "default = false\n\n"
    )


def dockerhub_official_base_prefetch_command(
    demo_project: dict[str, Any],
    *,
    manual_source_checkout: bool,
) -> str | None:
    dockerfiles = sorted(
        {
            str(build.get("dockerfile"))
            for build in builds(demo_project).values()
            if isinstance(build, dict)
            and isinstance(build.get("dockerfile"), str)
            and build.get("dockerfile")
        }
    )
    if not dockerfiles:
        return None

    script = (
        "import pathlib, subprocess, sys; "
        "seen=set(); "
        "\nfor dockerfile in sys.argv[1:]:"
        "\n    for line in pathlib.Path(dockerfile).read_text().splitlines():"
        "\n        parts=line.strip().split()"
        "\n        if not parts or parts[0].upper() != 'FROM':"
        "\n            continue"
        "\n        index=1"
        "\n        while index < len(parts) and parts[index].startswith('--'):"
        "\n            index += 1"
        "\n        if index >= len(parts):"
        "\n            continue"
        "\n        image=parts[index].split('@', 1)[0]"
        "\n        if '/' in image:"
        "\n            continue"
        "\n        if image in seen:"
        "\n            continue"
        "\n        seen.add(image)"
        "\n        mirror='public.ecr.aws/docker/library/' + image"
        "\n        subprocess.run(['docker', 'pull', mirror], check=True)"
        "\n        subprocess.run(['docker', 'tag', mirror, image], check=True)"
    )
    source_prefix = "cd /tmp/source && " if manual_source_checkout else ""
    return (
        source_prefix
        + "python3 -c "
        + shlex.quote(script)
        + " "
        + " ".join(shlex.quote(dockerfile) for dockerfile in dockerfiles)
    )


def manual_source_checkout_commands() -> list[str]:
    return [
        'mkdir -p "$HOME/.ssh"',
        (
            "python3 -c "
            + shlex.quote(
                "import json, os, pathlib; "
                'data=json.loads(os.environ["DEMO_SOURCE_AUTH_JSON"]); '
                'ssh=pathlib.Path.home()/".ssh"; '
                "ssh.mkdir(mode=0o700, exist_ok=True); "
                'identity=data["identity"]; '
                'known_hosts=data.get("known_hosts", ""); '
                'identity += "" if identity.endswith("\\n") else "\\n"; '
                'known_hosts += "" if known_hosts.endswith("\\n") else "\\n"; '
                '(ssh/"id_source").write_text(identity); '
                '(ssh/"known_hosts").write_text(known_hosts); '
                '(ssh/"config").write_text('
                '"Host *\\n  IdentityFile ~/.ssh/id_source\\n  '
                'UserKnownHostsFile ~/.ssh/known_hosts\\n  '
                'StrictHostKeyChecking yes\\n"); '
                'os.chmod(ssh/"id_source", 0o600)'
            )
        ),
        'git clone --no-checkout "$DEMO_SOURCE_REPO" /tmp/source',
        'cd /tmp/source && git checkout "$DEMO_SOURCE_VERSION"',
        "SOURCE_SHA=$(cd /tmp/source && git rev-parse HEAD)",
    ]


def shell_quote_image_template(image_template: str) -> str:
    for suffix in ("${IMAGE_VERSION}", "${SOURCE_SHA}"):
        if image_template.endswith(suffix):
            return shlex.quote(image_template[: -len(suffix)]) + suffix
    return shlex.quote(image_template)


def image_version(demo_project: dict[str, Any], source_commit: str) -> str:
    return f"git-{source_commit[:12]}{image_version_suffix(demo_project)}"


def image_version_suffix(demo_project: dict[str, Any]) -> str:
    if any(
        build_strategy(build) == BUILDPACK_BUILD_STRATEGY
        for build in builds(demo_project).values()
    ):
        return f"-b{build_definition_hash(demo_project)[:12]}"
    return ""


def render_image_templates(
    demo_project: dict[str, Any],
    image_templates: dict[str, str],
    source_commit_sha: str,
) -> dict[str, str]:
    version = image_version(demo_project, source_commit_sha)
    source_prefix = source_commit_sha[:12]
    build_hash = build_definition_hash(demo_project)
    return {
        build_name: (
            image_template.replace("${IMAGE_VERSION}", version)
            .replace("${SOURCE_SHA}", source_prefix)
            .replace("${BUILD_DEFINITION_HASH}", build_hash)
        )
        for build_name, image_template in image_templates.items()
    }


def codebuild_idempotency_token(
    demo_project: dict[str, Any],
    *,
    source_repo: str,
    source_version: str,
    source_auth_secret_path: str | None = None,
    idempotency_token_salt: str | None = None,
    image_templates: dict[str, str],
) -> str:
    encoded = json.dumps(
        {
            "project": project_name(demo_project),
            "sourceRepo": source_repo,
            "sourceVersion": source_version,
            "sourceAuthSecretPath": source_auth_secret_path,
            "idempotencyTokenSalt": idempotency_token_salt,
            "images": image_templates,
            "builds": builds(demo_project),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


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


def codebuild_build_entries(
    demo_project: dict[str, Any],
    image_templates: dict[str, str],
    *,
    source_commit_sha: str | None,
    external_resources: dict[str, Any] | None,
    phase: str,
) -> list[dict[str, Any]]:
    images = {}
    if source_commit_sha and looks_like_commit_sha(source_commit_sha):
        images = render_image_templates(demo_project, image_templates, source_commit_sha)

    return [
        build_entry(
            demo_project,
            build_name,
            image=images.get(build_name),
            phase=phase,
            source="aws-codebuild",
            external_resources=external_resources,
        )
        for build_name in sorted(builds(demo_project))
    ]


def looks_like_commit_sha(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    return 7 <= len(value) <= 40 and all(
        character in "0123456789abcdefABCDEF" for character in value
    )


def build_entry(
    demo_project: dict[str, Any],
    build_name: str,
    *,
    image: str | None,
    phase: str,
    source: str,
    external_resources: dict[str, Any] | None,
) -> dict[str, Any]:
    build = builds(demo_project)[build_name]
    entry: dict[str, Any] = {
        "name": build_name,
        "phase": phase,
        "source": source,
        "strategy": build_strategy(build),
        "context": build["context"],
        "repository": repository_name_for_build(
            demo_project,
            build_name,
            external_resources,
        ),
        "components": components_for_build(demo_project, build_name),
    }
    dockerfile = build.get("dockerfile")
    if isinstance(dockerfile, str) and dockerfile:
        entry["dockerfile"] = dockerfile
    builder = build.get("builder")
    if isinstance(builder, str) and builder:
        entry["builder"] = builder
    if image:
        entry["image"] = image
    return entry


def repository_name_for_build(
    demo_project: dict[str, Any],
    build_name: str,
    external_resources: dict[str, Any] | None,
) -> str:
    repository = repository_status_for_build(build_name, external_resources)
    if isinstance(repository, dict) and isinstance(repository.get("name"), str):
        return repository["name"]
    return f"{derive.image_repository_prefix()}/{project_name(demo_project)}/{build_name}"


def repository_uri_for_build(
    build_name: str,
    external_resources: dict[str, Any] | None,
) -> str | None:
    repository = repository_status_for_build(build_name, external_resources)
    if isinstance(repository, dict) and isinstance(repository.get("repositoryUri"), str):
        return repository["repositoryUri"]
    return None


def repository_status_for_build(
    build_name: str,
    external_resources: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(external_resources, dict):
        return None
    ecr = external_resources.get("ecr")
    if not isinstance(ecr, dict):
        return None
    repositories = ecr.get("repositories")
    if not isinstance(repositories, list):
        return None
    for repository in repositories:
        if isinstance(repository, dict) and repository.get("build") == build_name:
            return repository
    return None


def source_config(demo_project: dict[str, Any]) -> dict[str, str]:
    source = spec_of(demo_project).get("source")
    if not isinstance(source, dict):
        raise ValueError("DemoProject spec.source is required for builds")
    repo = source.get("repo")
    if not isinstance(repo, str):
        raise ValueError("DemoProject spec.source.repo is required")

    config = {"repo": repo, "branch": source_branch(demo_project)}
    auth_secret_name = source_auth_secret_name(demo_project)
    if auth_secret_name:
        git_repository = source.get("gitRepository")
        git_repository_url = (
            git_repository.get("url") if isinstance(git_repository, dict) else None
        )
        if not isinstance(git_repository_url, str) or not git_repository_url.strip():
            raise ValueError(
                "DemoProject spec.source.gitRepository.url is required when "
                "gitRepository.authSecret is used for aws-codebuild source auth"
            )
        config["codebuildRepo"] = source_git_repository_url(demo_project) or repo
        config["sourceAuthSecretPath"] = user_secret_remote_path(
            demo_project,
            auth_secret_name,
        )

    return config


def components_for_build(demo_project: dict[str, Any], build_name: str) -> list[str]:
    return [
        component_name
        for component_name, component in sorted(components(demo_project).items())
        if component.get("build") == build_name
    ]


def demo_project_from_request(request: dict[str, Any]) -> dict[str, Any]:
    demo_project = request.get("demoProject")
    if isinstance(demo_project, dict):
        return demo_project
    if isinstance(request.get("metadata"), dict) and isinstance(request.get("spec"), dict):
        return request
    raise non_retryable_error(
        "DemoProject request must include a demoProject object",
        error_type=VALIDATION_ERROR,
    )


def external_resources_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    external_resources = request.get("externalResources")
    if isinstance(external_resources, dict):
        return external_resources
    return None
