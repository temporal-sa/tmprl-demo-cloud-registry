from __future__ import annotations

import os
import hashlib
import json
import shutil
import shlex
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse, urlunparse

import yaml
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
from demo_registry_operator.activities.builds.codebuild.execution import (
    CODEBUILD_API_TIMEOUT_SECONDS,
    codebuild_build_id_from_heartbeat,
    codebuild_error_message,
    codebuild_status_summary,
)
from demo_registry_operator.activities.builds.definitions import looks_like_commit_sha
from demo_registry_operator.activities.external_resources import (
    aggregate_phase,
    ensure_ecr_repository,
    error_code,
)
from demo_registry_operator.kube import (
    delete_external_secret,
    delete_git_repository,
    patch_instruqt_course_status,
    read_secret_data,
    remove_instruqt_course_finalizer,
)
from demo_registry_operator.resources import (
    CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    CONTENT_SOURCE_COMMIT_ANNOTATION,
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
    content_source_artifact_digest,
    content_source_commit_sha,
    content_source_config,
    content_source_repo,
    content_source_revision,
    instruqt_course_reconcile_key,
    instruqt_course_testing_content_branch,
    instruqt_course_testing_source_branch,
    project_name,
    source_artifact_digest,
    source_auth_kubernetes_secret_name,
    source_branch,
    source_commit_sha,
    source_git_repository_name,
    source_git_repository_secret_ref,
    source_git_repository_url,
    source_repo,
    source_revision,
    source_config_block,
    source_testing_branch,
    spec_of,
    testing_branch_message,
    testing_branch_reason,
    testing_branch_state,
    testing_content_source_artifact_digest,
    testing_content_source_commit_sha,
    testing_content_source_revision,
    testing_source_artifact_digest,
    testing_source_commit_sha,
    testing_source_revision,
    dns_label,
    source_auth_secret_path,
)


INSTRUQT_PRODUCTION_CHANNEL = "production"
INSTRUQT_TESTING_CHANNEL = "testing"
INSTRUQT_CHANNELS = {INSTRUQT_PRODUCTION_CHANNEL, INSTRUQT_TESTING_CHANNEL}
INSTRUQT_TEST_TIMEOUT_SECONDS = 7200


@activity.defn
def reconcile_instruqt_course(instruqt_course: dict[str, Any]) -> dict[str, Any]:
    course = instruqt_course_from_request(instruqt_course)
    errors = validate_instruqt_course(course)
    now = datetime.now(UTC).isoformat()
    generation = resource_generation(course)
    resolved_source = bool(source_commit_sha(course) and source_artifact_digest(course))
    has_content = bool(content_source_config(course))
    resolved_content = not has_content or bool(
        content_source_commit_sha(course) and content_source_artifact_digest(course)
    )
    testing = testing_status(course)
    cli = instruqt_cli_status()
    phase = "Error" if errors else "Validated"
    message = (
        "; ".join(errors)
        if errors
        else "Course source contract validated; Instruqt push/test automation is not enabled yet."
    )

    status: dict[str, Any] = {
        "observedGeneration": generation,
        "reconcileKey": instruqt_course_reconcile_key(course),
        "phase": phase,
        "message": message,
        "track": track_status(course),
        "source": source_status(course),
        **({"content": content_status(course)} if has_content else {}),
        "testing": testing,
        "cli": cli,
        "images": image_bindings_status(course),
        "conditions": [
            condition(
                "Reconciled",
                "False" if errors else "True",
                generation,
                phase,
                message,
                now,
            ),
            condition(
                "SourceResolved",
                "True" if resolved_source else "False",
                generation,
                "SourceResolved" if resolved_source else "SourceUnavailable",
                (
                    "Flux source artifact revision and digest are available."
                    if resolved_source
                    else "Flux source artifact revision and digest are not available."
                ),
                now,
            ),
            condition(
                "ContentResolved",
                "True" if resolved_content else "False",
                generation,
                "ContentResolved" if resolved_content else "ContentUnavailable",
                (
                    "No separate content source is configured."
                    if not has_content
                    else "Flux content source artifact revision and digest are available."
                    if resolved_content
                    else "Flux content source artifact revision and digest are not available."
                ),
                now,
            ),
            condition(
                "CourseValidated",
                "False" if errors else "True",
                generation,
                "ValidationFailed" if errors else "Validated",
                message,
                now,
            ),
            condition(
                "TestingResolved",
                "True" if testing.get("resolved") is True else "False",
                generation,
                str(testing.get("reason") or testing.get("phase") or "Unknown"),
                str(testing.get("message") or "Testing channel status is unknown."),
                now,
            ),
            condition(
                "InstruqtCLIReady",
                "True" if cli.get("ready") is True else "False",
                generation,
                str(cli.get("reason") or cli.get("phase") or "Unknown"),
                str(cli.get("message") or "Instruqt CLI status is unknown."),
                now,
            ),
        ],
    }
    if errors:
        status["lastError"] = {
            "reason": "ValidationFailed",
            "message": message,
            "lastTransitionTime": now,
        }
    return status


@activity.defn
def render_instruqt_course_config(request: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = request.get("instruqtCourse")
    config = request.get("config")
    resolved_images = request.get("images")
    if not isinstance(instruqt_course, dict) or not isinstance(config, str):
        raise non_retryable_error(
            "render_instruqt_course_config requires instruqtCourse and config",
            error_type=VALIDATION_ERROR,
        )
    if resolved_images is None:
        resolved_images = {}
    if not isinstance(resolved_images, dict):
        raise non_retryable_error(
            "render_instruqt_course_config images must be an object",
            error_type=VALIDATION_ERROR,
        )

    try:
        return render_managed_instruqt_config(
            instruqt_course,
            config,
            {
                str(name): str(image)
                for name, image in resolved_images.items()
                if isinstance(name, str) and isinstance(image, str) and image
            },
        )
    except ValueError as error:
        raise non_retryable_error(str(error), error_type=VALIDATION_ERROR) from error


@activity.defn
def prepare_instruqt_course_artifact(request: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = request.get("instruqtCourse")
    resolved_images = request.get("images")
    channel = string_value(request.get("channel"), INSTRUQT_PRODUCTION_CHANNEL)
    if not isinstance(instruqt_course, dict):
        raise non_retryable_error(
            "prepare_instruqt_course_artifact requires an instruqtCourse object",
            error_type=VALIDATION_ERROR,
        )
    if resolved_images is None:
        resolved_images = {}
    if not isinstance(resolved_images, dict):
        raise non_retryable_error(
            "prepare_instruqt_course_artifact images must be an object",
            error_type=VALIDATION_ERROR,
        )

    images = {
        str(name): str(image)
        for name, image in resolved_images.items()
        if isinstance(name, str) and isinstance(image, str) and image
    }
    if image_bindings(instruqt_course) and not images:
        return {
            "phase": "WaitingForImages",
            "message": "Managed image bindings are configured but no resolved images were provided.",
            "missingImages": sorted(image_bindings(instruqt_course)),
        }

    with tempfile.TemporaryDirectory(prefix="instruqt-course-") as workspace:
        return prepare_managed_instruqt_artifact(
            instruqt_course,
            Path(workspace),
            images,
            validate=True,
            channel=channel,
        )


@activity.defn
def reconcile_instruqt_course_ecr_repositories(
    instruqt_course: dict[str, Any],
) -> dict[str, Any]:
    config = WorkerConfig.from_env()
    checked_at = datetime.now(UTC).isoformat()
    bindings = image_bindings(instruqt_course)
    if not bindings:
        return {
            "phase": "NotRequired",
            "region": config.aws_region,
            "accountId": config.aws_account_id,
            "checkedAt": checked_at,
            "repositories": [],
            "message": "No managed Instruqt images are configured.",
        }

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for Instruqt image repositories: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    repositories = []
    for image_name in sorted(bindings):
        repository = instruqt_image_repository_plan_entry(instruqt_course, image_name)
        try:
            repositories.append(ensure_ecr_repository(ecr, repository))
        except (BotoCoreError, ClientError) as error:
            raise aws_application_error(
                error,
                operation=f"Reconcile ECR repository {repository['name']}",
            ) from error

    return {
        "phase": aggregate_phase(repositories),
        "region": config.aws_region,
        "accountId": config.aws_account_id,
        "checkedAt": checked_at,
        "repositories": repositories,
    }


@activity.defn
def delete_instruqt_course_ecr_repositories(plan: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = plan.get("instruqtCourse")
    if not isinstance(instruqt_course, dict):
        raise non_retryable_error(
            "delete_instruqt_course_ecr_repositories requires an instruqtCourse object",
            error_type=VALIDATION_ERROR,
        )
    config = WorkerConfig.from_env()
    checked_at = string_value(plan.get("checkedAt"), datetime.now(UTC).isoformat())
    bindings = image_bindings(instruqt_course)
    if not bindings:
        return {
            "phase": "NotRequired",
            "checkedAt": checked_at,
            "repositories": [],
            "message": "No managed Instruqt image repositories to delete.",
        }

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return {"phase": "Error", "message": f"boto3 is required: {error}"}

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    repositories: list[dict[str, Any]] = []
    for image_name in sorted(bindings):
        name = instruqt_image_repository_name(instruqt_course, image_name)
        try:
            ecr.delete_repository(repositoryName=name, force=True)
            repositories.append({"name": name, "image": image_name, "action": "deleted"})
        except (BotoCoreError, ClientError) as error:
            if error_code(error) == "RepositoryNotFoundException":
                repositories.append(
                    {"name": name, "image": image_name, "action": "not-found"}
                )
            else:
                repositories.append(
                    {
                        "name": name,
                        "image": image_name,
                        "action": "error",
                        "message": str(error),
                    }
                )
    phase = (
        "Error"
        if any(repository.get("action") == "error" for repository in repositories)
        else "Deleted"
    )
    return {"phase": phase, "checkedAt": checked_at, "repositories": repositories}


@activity.defn
def plan_instruqt_course_images(request: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = request.get("instruqtCourse")
    if not isinstance(instruqt_course, dict):
        raise non_retryable_error(
            "plan_instruqt_course_images requires an instruqtCourse object",
            error_type=VALIDATION_ERROR,
        )
    external_resources = request.get("externalResources")
    external_resources = external_resources if isinstance(external_resources, dict) else {}
    previous_build_status = request.get("previousBuildStatus")
    previous_build_status = (
        previous_build_status if isinstance(previous_build_status, dict) else None
    )
    channel = string_value(request.get("channel"), INSTRUQT_PRODUCTION_CHANNEL)
    if channel not in INSTRUQT_CHANNELS:
        raise non_retryable_error(
            f"unsupported Instruqt image channel {channel!r}",
            error_type=VALIDATION_ERROR,
        )

    config = WorkerConfig.from_env()
    started_at = datetime.now(UTC).isoformat()
    bindings = image_bindings(instruqt_course)
    if not bindings:
        return completed_instruqt_image_plan(
            instruqt_image_build_status(
                instruqt_course,
                channel=channel,
                phase="NotRequired",
                mode=config.build_mode,
                started_at=started_at,
                images={},
                message="No managed Instruqt images are configured.",
                build_entries=[],
            )
        )

    reusable = reusable_instruqt_image_build_status(
        instruqt_course,
        previous_build_status,
        external_resources,
        channel=channel,
        started_at=started_at,
    )
    if reusable is not None:
        return completed_instruqt_image_plan(reusable)

    if not config.codebuild_project:
        return completed_instruqt_image_plan(
            instruqt_image_build_status(
                instruqt_course,
                channel=channel,
                phase="Error",
                mode=config.build_mode,
                started_at=started_at,
                images={},
                message="AWS CodeBuild project is not configured.",
                build_entries=[],
                error=(
                    "Set DEMO_REGISTRY_CODEBUILD_PROJECT to build managed "
                    "Instruqt images."
                ),
            )
        )

    try:
        source_plan = instruqt_codebuild_source_plan(instruqt_course, channel)
        image_templates = instruqt_codebuild_image_templates(
            instruqt_course,
            external_resources,
            config,
        )
    except ValueError as error:
        return completed_instruqt_image_plan(
            instruqt_image_build_status(
                instruqt_course,
                channel=channel,
                phase="Error",
                mode=config.build_mode,
                started_at=started_at,
                images={},
                message="AWS CodeBuild image build cannot start.",
                build_entries=[],
                error=str(error),
            )
        )

    return {
        "action": "codebuild",
        "instruqtCourse": instruqt_course,
        "externalResources": external_resources,
        "channel": channel,
        "startedAt": started_at,
        "mode": config.build_mode,
        "codebuildProject": config.codebuild_project,
        "imageTemplates": image_templates,
        "sources": source_plan,
    }


@activity.defn
def start_instruqt_course_image_build(plan: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = instruqt_course_from_request(plan)
    config = WorkerConfig.from_env()
    channel = string_value(plan.get("channel"), INSTRUQT_PRODUCTION_CHANNEL)
    started_at = string_field(plan, "startedAt")
    image_templates = instruqt_image_templates_from_plan(plan)
    sources = instruqt_sources_from_plan(plan)
    codebuild_build: dict[str, Any] | None = None

    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return completed_instruqt_image_plan(
            instruqt_image_build_status(
                instruqt_course,
                channel=channel,
                phase="Error",
                mode=config.build_mode,
                started_at=started_at,
                images={},
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
                "course": project_name(instruqt_course),
                "channel": channel,
                "codebuildProject": config.codebuild_project,
                "codebuildBuildId": build_id,
                "phase": "Reattaching",
            }
        )
        return {**plan, "codebuildBuildId": build_id}

    start_args = instruqt_codebuild_start_build_args(
        instruqt_course,
        config,
        channel=channel,
        started_at=started_at,
        image_templates=image_templates,
        sources=sources,
    )
    activity.heartbeat(
        {
            "course": project_name(instruqt_course),
            "channel": channel,
            "codebuildProject": config.codebuild_project,
            "phase": "Starting",
        }
    )
    try:
        response = codebuild.start_build(**start_args)
    except (BotoCoreError, ClientError) as error:
        if aws_error_code(error) in NON_RETRYABLE_AWS_ERROR_CODES:
            return completed_instruqt_image_plan(
                instruqt_image_build_status(
                    instruqt_course,
                    channel=channel,
                    phase="Error",
                    mode=config.build_mode,
                    started_at=started_at,
                    images={},
                    message="AWS CodeBuild image build failed.",
                    build_entries=[],
                    error=str(error),
                    codebuild_status=codebuild_status_summary(
                        codebuild_build,
                        project_name=config.codebuild_project,
                    ),
                )
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
            "course": project_name(instruqt_course),
            "channel": channel,
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
def wait_for_instruqt_course_image_build(started: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = instruqt_course_from_request(started)
    external_resources = started.get("externalResources")
    external_resources = external_resources if isinstance(external_resources, dict) else {}
    config = WorkerConfig.from_env()
    channel = string_value(started.get("channel"), INSTRUQT_PRODUCTION_CHANNEL)
    started_at = string_field(started, "startedAt")
    image_templates = instruqt_image_templates_from_plan(started)
    build_id = string_field(started, "codebuildBuildId")

    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return instruqt_image_build_status(
            instruqt_course,
            channel=channel,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images={},
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
        codebuild_build = wait_for_instruqt_codebuild_build(
            codebuild,
            build_id,
            config=config,
            instruqt_course=instruqt_course,
            channel=channel,
        )
    except TimeoutError as error:
        raise non_retryable_error(str(error), error_type=IMAGE_BUILD_TIMEOUT) from error
    except (BotoCoreError, ClientError) as error:
        if aws_error_code(error) in NON_RETRYABLE_AWS_ERROR_CODES:
            return instruqt_image_build_status(
                instruqt_course,
                channel=channel,
                phase="Error",
                mode=config.build_mode,
                started_at=started_at,
                images={},
                message="AWS CodeBuild image build failed.",
                build_entries=[],
                error=str(error),
            )
        raise aws_application_error(error, operation="Poll CodeBuild build") from error

    codebuild_status = codebuild_status_summary(
        codebuild_build,
        project_name=config.codebuild_project,
    )
    if codebuild_build.get("buildStatus") != "SUCCEEDED":
        return instruqt_image_build_status(
            instruqt_course,
            channel=channel,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images={},
            message="AWS CodeBuild image build failed.",
            build_entries=instruqt_image_build_entries(
                instruqt_course,
                image_templates,
                images={},
                external_resources=external_resources,
                phase="Failed",
                source="aws-codebuild",
            ),
            error=codebuild_error_message(codebuild_build),
            codebuild_status=codebuild_status,
        )

    source_commit = instruqt_channel_source_commit_sha(instruqt_course, channel)
    if not source_commit or not looks_like_commit_sha(source_commit):
        return instruqt_image_build_status(
            instruqt_course,
            channel=channel,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images={},
            message="AWS CodeBuild image build did not have an immutable source commit.",
            build_entries=[],
            error=(
                "CodeBuild completed but the Instruqt source annotation did not "
                "contain a git commit SHA."
            ),
            codebuild_status=codebuild_status,
        )

    images = render_instruqt_image_templates(
        instruqt_course,
        image_templates,
        source_commit,
    )
    return instruqt_image_build_status(
        instruqt_course,
        channel=channel,
        phase="Pushed",
        mode=config.build_mode,
        started_at=started_at,
        images=images,
        message="AWS CodeBuild built and pushed immutable images for every managed Instruqt image.",
        build_entries=instruqt_image_build_entries(
            instruqt_course,
            image_templates,
            images=images,
            external_resources=external_resources,
            phase="Pushed",
            source="aws-codebuild",
        ),
        codebuild_status=codebuild_status,
    )


@activity.defn
def deploy_instruqt_course_track(request: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = request.get("instruqtCourse")
    resolved_images = request.get("images")
    channel = string_value(request.get("channel"), INSTRUQT_PRODUCTION_CHANNEL)
    if not isinstance(instruqt_course, dict):
        raise non_retryable_error(
            "deploy_instruqt_course_track requires an instruqtCourse object",
            error_type=VALIDATION_ERROR,
        )
    if resolved_images is None:
        resolved_images = {}
    if not isinstance(resolved_images, dict):
        raise non_retryable_error(
            "deploy_instruqt_course_track images must be an object",
            error_type=VALIDATION_ERROR,
        )
    images = {
        str(name): str(image)
        for name, image in resolved_images.items()
        if isinstance(name, str) and isinstance(image, str) and image
    }
    if image_bindings(instruqt_course) and not images:
        return {
            "phase": "WaitingForImages",
            "channel": channel,
            "message": "Managed image bindings are configured but no resolved images were provided.",
            "missingImages": sorted(image_bindings(instruqt_course)),
        }

    with tempfile.TemporaryDirectory(prefix="instruqt-deploy-") as workspace:
        workspace_path = Path(workspace)
        artifact_status = prepare_managed_instruqt_artifact(
            instruqt_course,
            workspace_path,
            images,
            validate=True,
            channel=channel,
        )
        if artifact_status.get("phase") != "Validated":
            return {
                **artifact_status,
                "channel": channel,
            }

        track_dir = workspace_path / "rendered"
        target = instruqt_track_target(instruqt_course, channel)
        push = run_instruqt_command(
            track_dir,
            ["track", "push", "--force"],
            timeout_seconds=300,
            home_dir=workspace_path / "home",
        )
        if push.get("phase") != "Succeeded":
            return instruqt_command_failure_status(
                "PushFailed",
                channel=channel,
                target=target,
                message="instruqt track push failed.",
                artifact_status=artifact_status,
                command_status=push,
            )

        test = run_instruqt_command(
            track_dir,
            ["track", "test"],
            timeout_seconds=INSTRUQT_TEST_TIMEOUT_SECONDS,
            home_dir=workspace_path / "home",
        )
        if test.get("phase") != "Succeeded":
            return instruqt_command_failure_status(
                "TestFailed",
                channel=channel,
                target=target,
                message="instruqt track test failed.",
                artifact_status=artifact_status,
                command_status=test,
                push_status=push,
            )

        deploy = run_instruqt_command(
            track_dir,
            ["track", "deploy", target],
            timeout_seconds=300,
            home_dir=workspace_path / "home",
        )
        if deploy.get("phase") != "Succeeded":
            return instruqt_command_failure_status(
                "DeployFailed",
                channel=channel,
                target=target,
                message="instruqt track deploy failed.",
                artifact_status=artifact_status,
                command_status=deploy,
                push_status=push,
                test_status=test,
            )

        return {
            "phase": "Deployed",
            "channel": channel,
            "target": target,
            "message": "Instruqt track was pushed, tested, and deployed.",
            "artifact": artifact_status,
            "push": push,
            "test": test,
            "deploy": deploy,
        }


@activity.defn
def delete_instruqt_course_track(request: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = request.get("instruqtCourse")
    channel = string_value(request.get("channel"), INSTRUQT_PRODUCTION_CHANNEL)
    if not isinstance(instruqt_course, dict):
        raise non_retryable_error(
            "delete_instruqt_course_track requires an instruqtCourse object",
            error_type=VALIDATION_ERROR,
        )
    target = instruqt_track_target(instruqt_course, channel)
    with tempfile.TemporaryDirectory(prefix="instruqt-delete-") as workspace:
        command = run_instruqt_command(
            Path(workspace),
            ["track", "delete", target],
            timeout_seconds=120,
            stdin="y\n",
            home_dir=Path(workspace) / "home",
        )
    if command.get("phase") == "Succeeded":
        return {
            "phase": "Deleted",
            "channel": channel,
            "target": target,
            "message": "Instruqt track was deleted.",
            "delete": command,
        }
    output = str(command.get("output") or "").lower()
    if any(marker in output for marker in ("not found", "could not find", "404")):
        return {
            "phase": "NotFound",
            "channel": channel,
            "target": target,
            "message": "Instruqt track was already absent.",
            "delete": command,
        }
    return {
        "phase": "DeleteFailed",
        "channel": channel,
        "target": target,
        "message": "instruqt track delete failed.",
        "delete": command,
    }


@activity.defn
def record_instruqt_course_status(request: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = request.get("instruqtCourse")
    status = request.get("status")
    if not isinstance(instruqt_course, dict) or not isinstance(status, dict):
        raise non_retryable_error(
            "record_instruqt_course_status requires instruqtCourse and status objects",
            error_type=VALIDATION_ERROR,
        )

    name = project_name(instruqt_course)
    status = status_with_reconcile_key(instruqt_course, status)
    try:
        patch_instruqt_course_status(name, status, dry_run=False)
    except ValueError as error:
        raise non_retryable_error(str(error), error_type=VALIDATION_ERROR) from error
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error
    return status


@activity.defn
def plan_instruqt_course_finalization(request: dict[str, Any]) -> dict[str, Any]:
    instruqt_course = request.get("instruqtCourse")
    if not isinstance(instruqt_course, dict):
        raise non_retryable_error(
            "plan_instruqt_course_finalization requires an instruqtCourse object",
            error_type=VALIDATION_ERROR,
        )
    config = WorkerConfig.from_env()
    name = project_name(instruqt_course)
    return {
        "action": "finalize",
        "course": name,
        "instruqtCourse": instruqt_course,
        "sourceWatchNamespace": config.source_watch_namespace,
        "gitRepository": source_git_repository_name(instruqt_course),
        "sourceAuthSecret": source_auth_kubernetes_secret_name(instruqt_course),
        **(
            {
                "contentGitRepository": source_git_repository_name(
                    instruqt_course,
                    "content",
                ),
                "contentSourceAuthSecret": source_auth_kubernetes_secret_name(
                    instruqt_course,
                    "content",
                ),
            }
            if content_source_config(instruqt_course)
            else {}
        ),
        "checkedAt": datetime.now(UTC).isoformat(),
    }


@activity.defn
def delete_instruqt_course_source_repository(plan: dict[str, Any]) -> str:
    try:
        return delete_git_repository(
            string_field(plan, "sourceWatchNamespace"),
            string_field(plan, "gitRepository"),
            dry_run=False,
        )
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error


@activity.defn
def delete_instruqt_course_source_auth_secret(plan: dict[str, Any]) -> str:
    name = plan.get("sourceAuthSecret")
    if not isinstance(name, str) or not name:
        return "not-required"
    try:
        return delete_external_secret(
            string_field(plan, "sourceWatchNamespace"),
            name,
            dry_run=False,
        )
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error


@activity.defn
def delete_instruqt_course_content_repository(plan: dict[str, Any]) -> str:
    name = plan.get("contentGitRepository")
    if not isinstance(name, str) or not name:
        return "not-required"
    try:
        return delete_git_repository(
            string_field(plan, "sourceWatchNamespace"),
            name,
            dry_run=False,
        )
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error


@activity.defn
def delete_instruqt_course_content_auth_secret(plan: dict[str, Any]) -> str:
    name = plan.get("contentSourceAuthSecret")
    if not isinstance(name, str) or not name:
        return "not-required"
    try:
        return delete_external_secret(
            string_field(plan, "sourceWatchNamespace"),
            name,
            dry_run=False,
        )
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error


@activity.defn
def remove_instruqt_course_finalizer_resource(plan: dict[str, Any]) -> str:
    try:
        remove_instruqt_course_finalizer(
            string_field(plan, "course"),
            dry_run=False,
        )
    except RuntimeError as error:
        raise non_retryable_error(str(error), error_type=CONFIGURATION_ERROR) from error
    return "removed"


def instruqt_course_finalization_status(
    plan: dict[str, Any],
    *,
    source_action: str,
    source_auth_action: str = "not-required",
    content_source_action: str = "not-required",
    content_source_auth_action: str = "not-required",
    production_track_action: dict[str, Any] | None = None,
    testing_track_action: dict[str, Any] | None = None,
    ecr_action: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = {
        "gitRepository": string_field(plan, "gitRepository"),
        "namespace": string_field(plan, "sourceWatchNamespace"),
        "action": source_action,
    }
    if source_auth_action != "not-required":
        source["authSecretAction"] = source_auth_action
    status = {
        "phase": "Finalized",
        "course": string_field(plan, "course"),
        "checkedAt": string_field(plan, "checkedAt"),
        "message": "Operator-owned source-watch resources were finalized.",
        "source": source,
        "tracks": {
            "production": production_track_action or {"phase": "NotRequested"},
            "testing": testing_track_action or {"phase": "NotRequested"},
        },
        "ecr": ecr_action or {"phase": "NotRequested"},
    }
    content_git_repository = plan.get("contentGitRepository")
    if isinstance(content_git_repository, str) and content_git_repository:
        content = {
            "gitRepository": content_git_repository,
            "namespace": string_field(plan, "sourceWatchNamespace"),
            "action": content_source_action,
        }
        if content_source_auth_action != "not-required":
            content["authSecretAction"] = content_source_auth_action
        status["content"] = content
    return status


def status_with_reconcile_key(
    instruqt_course: dict[str, Any],
    status: dict[str, Any],
) -> dict[str, Any]:
    return {
        **status,
        "reconcileKey": instruqt_course_reconcile_key(instruqt_course),
    }


def validate_instruqt_course(instruqt_course: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not source_repo(instruqt_course):
        errors.append("spec.source.repo is required")

    errors.extend(validate_relative_source_path(instruqt_course, "source"))
    if content_source_config(instruqt_course):
        if not content_source_repo(instruqt_course):
            errors.append("spec.content.repo is required when spec.content is set")
        errors.extend(validate_relative_source_path(instruqt_course, "content"))

    track = spec_of(instruqt_course).get("track")
    if not isinstance(track, dict):
        errors.append("spec.track is required")
    else:
        slug = track.get("slug")
        if not isinstance(slug, str) or not slug.strip():
            errors.append("spec.track.slug is required")
        organization = track.get("organization")
        if not isinstance(organization, str) or not organization.strip():
            errors.append("spec.track.organization is required")

    testing_track = spec_of(instruqt_course).get("testingTrack")
    if testing_track is not None:
        if not isinstance(testing_track, dict):
            errors.append("spec.testingTrack must be an object")
        else:
            slug = testing_track.get("slug")
            if slug is not None and (not isinstance(slug, str) or not slug.strip()):
                errors.append("spec.testingTrack.slug must be a non-empty string")
            title_prefix = testing_track.get("titlePrefix")
            if title_prefix is not None and (
                not isinstance(title_prefix, str) or not title_prefix
            ):
                errors.append(
                    "spec.testingTrack.titlePrefix must be a non-empty string"
                )

    if source_testing_branch(instruqt_course, "content") and not (
        instruqt_course_testing_source_branch(instruqt_course)
    ):
        errors.append("spec.content.testingBranch requires spec.source.testingBranch")

    if not source_commit_sha(instruqt_course):
        errors.append(f"missing {SOURCE_COMMIT_ANNOTATION} annotation")
    if not source_artifact_digest(instruqt_course):
        errors.append(f"missing {SOURCE_ARTIFACT_DIGEST_ANNOTATION} annotation")
    if content_source_config(instruqt_course):
        if not content_source_commit_sha(instruqt_course):
            errors.append(f"missing {CONTENT_SOURCE_COMMIT_ANNOTATION} annotation")
        if not content_source_artifact_digest(instruqt_course):
            errors.append(
                f"missing {CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION} annotation"
            )
    errors.extend(validate_image_bindings(instruqt_course))
    return errors


def validate_relative_source_path(
    instruqt_course: dict[str, Any],
    source_key: str,
) -> list[str]:
    source = source_config_block(instruqt_course, source_key)
    path = source.get("path", ".")
    if not isinstance(path, str) or not path.strip():
        return [f"spec.{source_key}.path must be a non-empty relative path"]
    if path.startswith("/") or ".." in path.split("/"):
        return [f"spec.{source_key}.path must stay within the source repository"]
    return []


def validate_image_bindings(instruqt_course: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    content_configured = bool(content_source_config(instruqt_course))
    for image_name, image in image_bindings(instruqt_course).items():
        container = image.get("container")
        if not isinstance(container, str) or not container.strip():
            errors.append(f"spec.images.{image_name}.container is required")
        for key in ("context", "dockerfile"):
            location = image.get(key)
            if not isinstance(location, dict):
                errors.append(f"spec.images.{image_name}.{key} is required")
                continue
            source_key = location.get("source", "source")
            if source_key not in {"source", "content"}:
                errors.append(
                    f"spec.images.{image_name}.{key}.source must be source or content"
                )
            if source_key == "content" and not content_configured:
                errors.append(
                    f"spec.images.{image_name}.{key}.source references content but spec.content is not set"
                )
            path = location.get("path")
            if not isinstance(path, str) or not path.strip():
                errors.append(f"spec.images.{image_name}.{key}.path is required")
            elif path.startswith("/") or ".." in path.split("/"):
                errors.append(
                    f"spec.images.{image_name}.{key}.path must stay within its source repository"
                )
        platform = image.get("platform")
        if platform is not None and (
            not isinstance(platform, str) or not platform.strip()
        ):
            errors.append(f"spec.images.{image_name}.platform must be a non-empty string")
        target = image.get("target")
        if target is not None and (
            not isinstance(target, str) or not target.strip()
        ):
            errors.append(f"spec.images.{image_name}.target must be a non-empty string")
        build_args = image.get("buildArgs")
        if build_args is not None:
            if not isinstance(build_args, dict):
                errors.append(f"spec.images.{image_name}.buildArgs must be an object")
            else:
                for arg_name, arg_value in build_args.items():
                    if not isinstance(arg_name, str) or not arg_name:
                        errors.append(
                            f"spec.images.{image_name}.buildArgs keys must be non-empty strings"
                        )
                    if not isinstance(arg_value, str):
                        errors.append(
                            f"spec.images.{image_name}.buildArgs.{arg_name} must be a string"
                        )
    return errors


def render_managed_instruqt_config(
    instruqt_course: dict[str, Any],
    config: str,
    resolved_images: dict[str, str],
) -> dict[str, Any]:
    try:
        decoded = yaml.safe_load(config)
    except yaml.YAMLError as error:
        raise ValueError(f"Instruqt config YAML could not be parsed: {error}") from error
    if decoded is None:
        decoded = {}
    if not isinstance(decoded, dict):
        raise ValueError("Instruqt config YAML must be an object")

    bindings = image_bindings(instruqt_course)
    containers = decoded.get("containers")
    if containers is None:
        if bindings:
            raise ValueError(
                "Instruqt config has no containers; spec.images cannot be applied"
            )
        rendered = yaml.safe_dump(decoded, sort_keys=False)
        return {
            "phase": "Skipped",
            "message": "No containers or managed image bindings are configured.",
            "config": rendered,
            "managedImages": {},
        }
    if not isinstance(containers, list):
        raise ValueError("Instruqt config containers must be a list")

    bindings_by_container: dict[str, tuple[str, dict[str, Any]]] = {}
    errors: list[str] = []
    for image_name, binding in bindings.items():
        container_name = binding.get("container")
        if not isinstance(container_name, str) or not container_name:
            continue
        if container_name in bindings_by_container:
            errors.append(
                f"multiple spec.images entries target container {container_name!r}"
            )
            continue
        bindings_by_container[container_name] = (image_name, binding)

    managed: dict[str, dict[str, str]] = {}
    seen_containers: set[str] = set()
    for index, container in enumerate(containers):
        if not isinstance(container, dict):
            errors.append(f"containers[{index}] must be an object")
            continue
        name = container.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"containers[{index}].name is required")
            continue

        binding_entry = bindings_by_container.get(name)
        existing_image = container.get("image")
        existing_image = existing_image if isinstance(existing_image, str) else None
        if binding_entry is None:
            if existing_image:
                errors.append(
                    f"containers[{index}] {name!r} has unmanaged image {existing_image!r}"
                )
            continue

        image_name, _binding = binding_entry
        seen_containers.add(name)
        resolved_image = resolved_images.get(image_name) or resolved_images.get(name)
        if not isinstance(resolved_image, str) or not resolved_image:
            errors.append(
                f"managed image {image_name!r} for container {name!r} has no resolved image"
            )
            continue

        action = (
            "injected"
            if not existing_image
            else "kept"
            if existing_image == resolved_image
            else "overridden"
        )
        container["image"] = resolved_image
        managed[image_name] = {
            "container": name,
            "image": resolved_image,
            "action": action,
        }
        if existing_image:
            managed[image_name]["previousImage"] = existing_image

    for container_name, (image_name, _binding) in sorted(bindings_by_container.items()):
        if container_name not in seen_containers:
            errors.append(
                f"spec.images.{image_name}.container {container_name!r} was not found in config containers"
            )

    if errors:
        raise ValueError("; ".join(errors))

    rendered = yaml.safe_dump(decoded, sort_keys=False)
    if not managed:
        return {
            "phase": "Skipped",
            "message": "No managed image bindings were applied.",
            "config": rendered,
            "managedImages": {},
        }
    return {
        "phase": "Rendered",
        "message": "Managed images were injected into Instruqt config.",
        "config": rendered,
        "managedImages": managed,
    }


def source_status(instruqt_course: dict[str, Any]) -> dict[str, Any]:
    source = source_config_block(instruqt_course, "source")
    status: dict[str, Any] = {
        "repo": source_repo(instruqt_course),
        "branch": source_branch(instruqt_course),
        "path": source.get("path", "."),
    }
    revision = source_revision(instruqt_course)
    if revision:
        status["revision"] = revision
    commit_sha = source_commit_sha(instruqt_course)
    if commit_sha:
        status["commitSha"] = commit_sha
    artifact_digest = source_artifact_digest(instruqt_course)
    if artifact_digest:
        status["artifactDigest"] = artifact_digest
    return status


def content_status(instruqt_course: dict[str, Any]) -> dict[str, Any]:
    content = content_source_config(instruqt_course)
    status: dict[str, Any] = {
        "repo": content_source_repo(instruqt_course),
        "branch": source_branch(instruqt_course, "content"),
        "path": content.get("path", "."),
    }
    revision = content_source_revision(instruqt_course)
    if revision:
        status["revision"] = revision
    commit_sha = content_source_commit_sha(instruqt_course)
    if commit_sha:
        status["commitSha"] = commit_sha
    artifact_digest = content_source_artifact_digest(instruqt_course)
    if artifact_digest:
        status["artifactDigest"] = artifact_digest
    return status


def testing_status(instruqt_course: dict[str, Any]) -> dict[str, Any]:
    branch = instruqt_course_testing_source_branch(instruqt_course)
    if not branch:
        return {
            "phase": "Disabled",
            "reason": "TestingDisabled",
            "message": "No testing branch is configured.",
            "resolved": True,
        }

    state = testing_branch_state(instruqt_course)
    reason = testing_branch_reason(instruqt_course)
    message = testing_branch_message(instruqt_course)
    source = testing_source_status(instruqt_course, branch)
    content = testing_content_status(instruqt_course)
    source_resolved = bool(source.get("commitSha") and source.get("artifactDigest"))
    content_resolved = content is None or bool(
        content.get("commitSha") and content.get("artifactDigest")
    )

    status: dict[str, Any] = {
        "branch": branch,
        "track": testing_track_status(instruqt_course),
        "source": source,
    }
    if state:
        status["state"] = state
    if content is not None:
        status["content"] = content

    if state == "missing":
        return {
            **status,
            "phase": "BranchMissing",
            "reason": reason or "BranchMissing",
            "message": message
            or "Testing source branch is absent; testing track teardown is pending.",
            "resolved": False,
        }
    if state == "unknown":
        return {
            **status,
            "phase": "BranchUnknown",
            "reason": reason or "BranchUnknown",
            "message": message
            or "Testing source branch existence could not be confirmed.",
            "resolved": False,
        }
    if state == "exists" and source_resolved and content_resolved:
        return {
            **status,
            "phase": "Validated",
            "reason": "TestingSourceResolved",
            "message": "Testing branch source artifacts are resolved.",
            "resolved": True,
        }
    if state == "exists":
        return {
            **status,
            "phase": "WaitingForSource",
            "reason": "TestingSourceUnavailable",
            "message": "Testing branch exists but source artifacts are not resolved yet.",
            "resolved": False,
        }
    return {
        **status,
        "phase": "WaitingForBranchProbe",
        "reason": "TestingBranchProbePending",
        "message": "Testing branch existence has not been checked yet.",
        "resolved": False,
    }


def testing_source_status(
    instruqt_course: dict[str, Any],
    branch: str,
) -> dict[str, Any]:
    source = source_config_block(instruqt_course, "source")
    status: dict[str, Any] = {
        "repo": source_repo(instruqt_course),
        "branch": branch,
        "path": source.get("path", "."),
    }
    revision = testing_source_revision(instruqt_course)
    if revision:
        status["revision"] = revision
    commit_sha = testing_source_commit_sha(instruqt_course)
    if commit_sha:
        status["commitSha"] = commit_sha
    artifact_digest = testing_source_artifact_digest(instruqt_course)
    if artifact_digest:
        status["artifactDigest"] = artifact_digest
    return status


def testing_content_status(instruqt_course: dict[str, Any]) -> dict[str, Any] | None:
    if not content_source_config(instruqt_course):
        return None
    content = content_source_config(instruqt_course)
    status: dict[str, Any] = {
        "repo": content_source_repo(instruqt_course),
        "branch": instruqt_course_testing_content_branch(instruqt_course),
        "path": content.get("path", "."),
    }
    revision = testing_content_source_revision(instruqt_course)
    if revision:
        status["revision"] = revision
    commit_sha = testing_content_source_commit_sha(instruqt_course)
    if commit_sha:
        status["commitSha"] = commit_sha
    artifact_digest = testing_content_source_artifact_digest(instruqt_course)
    if artifact_digest:
        status["artifactDigest"] = artifact_digest
    return status


def testing_track_status(instruqt_course: dict[str, Any]) -> dict[str, Any]:
    spec = spec_of(instruqt_course)
    track = spec.get("track")
    track = track if isinstance(track, dict) else {}
    testing_track = spec.get("testingTrack")
    testing_track = testing_track if isinstance(testing_track, dict) else {}

    status: dict[str, Any] = {
        "titlePrefix": testing_track.get("titlePrefix", "[testing] "),
    }
    organization = track.get("organization")
    if isinstance(organization, str) and organization.strip():
        status["organization"] = organization.strip()

    slug = testing_track.get("slug")
    if isinstance(slug, str) and slug.strip():
        status["slug"] = slug.strip()
        return status

    production_slug = track.get("slug")
    if isinstance(production_slug, str) and production_slug.strip():
        status["slug"] = dns_label(f"testing-{production_slug.strip()}")
    return status


def instruqt_cli_status() -> dict[str, Any]:
    path = shutil.which("instruqt")
    token = os.getenv("INSTRUQT_TOKEN")
    if not path:
        return {
            "phase": "Unavailable",
            "reason": "InstruqtCLINotFound",
            "message": "The instruqt executable is not available in the worker image.",
            "ready": False,
        }
    if not isinstance(token, str) or not token.strip():
        return {
            "phase": "Unavailable",
            "reason": "InstruqtTokenMissing",
            "message": "INSTRUQT_TOKEN is not set in the worker environment.",
            "executable": path,
            "ready": False,
        }

    try:
        result = subprocess.run(
            [path, "version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {
            "phase": "Unavailable",
            "reason": "InstruqtVersionFailed",
            "message": f"instruqt version failed: {error}",
            "executable": path,
            "ready": False,
        }

    output = bounded_text(result.stdout or result.stderr)
    if result.returncode != 0:
        return {
            "phase": "Unavailable",
            "reason": "InstruqtVersionFailed",
            "message": f"instruqt version exited with {result.returncode}.",
            "executable": path,
            **({"versionOutput": output} if output else {}),
            "ready": False,
        }
    return {
        "phase": "Ready",
        "reason": "InstruqtCLIReady",
        "message": "Instruqt CLI and token are available.",
        "executable": path,
        **({"versionOutput": output} if output else {}),
        "ready": True,
    }


def bounded_text(value: str, limit: int = 500) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: limit - 3]}..."


def prepare_managed_instruqt_artifact(
    instruqt_course: dict[str, Any],
    workspace: Path,
    resolved_images: dict[str, str],
    *,
    validate: bool,
    channel: str = INSTRUQT_PRODUCTION_CHANNEL,
) -> dict[str, Any]:
    source_checkout = workspace / "source"
    rendered_track = workspace / "rendered"
    try:
        clone_course_source(
            instruqt_course,
            "source",
            source_checkout,
            commit_sha=instruqt_channel_source_commit_sha(instruqt_course, channel),
            branch=instruqt_channel_source_branch(instruqt_course, channel),
        )
        render_status = render_instruqt_track_directory(
            instruqt_course,
            source_checkout,
            rendered_track,
            resolved_images,
            channel=channel,
        )
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        return {
            "phase": "Error",
            "reason": "ArtifactPreparationFailed",
            "message": safe_error_message(error),
        }

    status = {
        "phase": "Rendered",
        "message": "Instruqt track artifact was rendered locally.",
        "channel": channel,
        "target": instruqt_track_target(instruqt_course, channel),
        "source": {
            "commitSha": instruqt_channel_source_commit_sha(instruqt_course, channel),
            "artifactDigest": instruqt_channel_source_artifact_digest(
                instruqt_course,
                channel,
            ),
        },
        "render": render_status,
    }
    if not validate:
        return status

    validate_status = run_instruqt_track_validate(rendered_track)
    status["validation"] = validate_status
    if validate_status.get("phase") == "Validated":
        status["phase"] = "Validated"
        status["message"] = "Instruqt track artifact rendered and validated."
    elif validate_status.get("phase") == "ValidationFailed":
        status["phase"] = "ValidationFailed"
        status["message"] = "Instruqt track validation failed."
    else:
        status["phase"] = "ValidationBlocked"
        status["message"] = "Instruqt track validation could not run."
    return status


def render_instruqt_track_directory(
    instruqt_course: dict[str, Any],
    source_checkout: Path,
    rendered_track: Path,
    resolved_images: dict[str, str],
    *,
    channel: str = INSTRUQT_PRODUCTION_CHANNEL,
) -> dict[str, Any]:
    source = source_config_block(instruqt_course, "source")
    track_source = safe_relative_path(source_checkout, str(source.get("path", ".")))
    if not track_source.is_dir():
        raise ValueError(f"Instruqt source path {source.get('path', '.')} was not found")

    shutil.copytree(track_source, rendered_track)
    config_path = rendered_track / "config.yml"
    if not config_path.exists():
        if image_bindings(instruqt_course):
            raise ValueError(
                "Instruqt config.yml is required when spec.images is configured"
            )
        return {
            "phase": "Skipped",
            "message": "No config.yml was found to render.",
        }

    rendered = render_managed_instruqt_config(
        instruqt_course,
        config_path.read_text(encoding="utf-8"),
        resolved_images,
    )
    config_path.write_text(rendered["config"], encoding="utf-8")
    track_metadata = render_instruqt_track_metadata(
        instruqt_course,
        rendered_track,
        channel=channel,
    )
    return {
        **{key: value for key, value in rendered.items() if key != "config"},
        "track": track_metadata,
    }


def clone_course_source(
    instruqt_course: dict[str, Any],
    source_key: str,
    destination: Path,
    *,
    commit_sha: str | None,
    branch: str | None = None,
) -> None:
    url = source_git_repository_url(instruqt_course, source_key)
    if not url:
        raise ValueError(f"spec.{source_key}.repo is required")
    clone_url = authenticated_git_url(instruqt_course, source_key, url)
    branch = branch or source_branch(instruqt_course, source_key)
    env = {
        **os.environ,
        "GIT_TERMINAL_PROMPT": "0",
    }
    subprocess.run(
        ["git", "clone", "--no-checkout", "--branch", branch, clone_url, str(destination)],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    checkout_ref = commit_sha or branch
    subprocess.run(
        ["git", "-C", str(destination), "checkout", checkout_ref],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )


def authenticated_git_url(
    instruqt_course: dict[str, Any],
    source_key: str,
    url: str,
) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        return url
    secret_ref = source_git_repository_secret_ref(instruqt_course, source_key)
    if not isinstance(secret_ref, dict):
        return url
    secret_name = secret_ref.get("name")
    if not isinstance(secret_name, str) or not secret_name:
        return url
    try:
        data = read_secret_data(
            WorkerConfig.from_env().source_watch_namespace,
            secret_name,
        )
    except Exception:
        return url
    if not isinstance(data, dict):
        return url
    username = data.get("username")
    password = data.get("password") or data.get("token") or data.get("githubToken")
    if not isinstance(username, str) or not username:
        username = "x-access-token"
    if not isinstance(password, str) or not password:
        return url
    netloc = f"{quote(username, safe='')}:{quote(password, safe='')}@{parsed.netloc}"
    return urlunparse(parsed._replace(netloc=netloc))


def safe_relative_path(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    root = root.resolve()
    if candidate != root and root not in candidate.parents:
        raise ValueError(f"path {relative!r} escapes source repository")
    return candidate


def run_instruqt_track_validate(track_dir: Path) -> dict[str, Any]:
    cli = instruqt_cli_status()
    if cli.get("ready") is not True:
        return {
            "phase": "Blocked",
            "reason": cli.get("reason") or "InstruqtCLIUnavailable",
            "message": cli.get("message") or "Instruqt CLI is not ready.",
        }
    executable = str(cli.get("executable") or "instruqt")
    try:
        result = subprocess.run(
            [executable, "track", "validate"],
            cwd=track_dir,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {
            "phase": "Blocked",
            "reason": "InstruqtValidateFailed",
            "message": f"instruqt track validate could not run: {error}",
        }

    output = bounded_text(result.stdout or result.stderr, limit=1000)
    if result.returncode == 0:
        return {
            "phase": "Validated",
            "reason": "InstruqtTrackValidated",
            "message": "instruqt track validate succeeded.",
            **({"output": output} if output else {}),
        }
    return {
        "phase": "ValidationFailed",
        "reason": "InstruqtTrackValidationFailed",
        "message": f"instruqt track validate exited with {result.returncode}.",
        "exitCode": result.returncode,
        **({"output": output} if output else {}),
    }


def run_instruqt_command(
    track_dir: Path,
    args: list[str],
    *,
    timeout_seconds: int,
    stdin: str | None = None,
    home_dir: Path | None = None,
) -> dict[str, Any]:
    cli = instruqt_cli_status()
    if cli.get("ready") is not True:
        return {
            "phase": "Blocked",
            "reason": cli.get("reason") or "InstruqtCLIUnavailable",
            "message": cli.get("message") or "Instruqt CLI is not ready.",
            "command": ["instruqt", *args],
        }
    executable = str(cli.get("executable") or "instruqt")
    home = home_dir or (track_dir.parent / ".instruqt-home")
    home.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "HOME": str(home),
        "GIT_TERMINAL_PROMPT": "0",
    }
    try:
        result = subprocess.run(
            [executable, *args],
            cwd=track_dir,
            input=stdin,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {
            "phase": "Blocked",
            "reason": "InstruqtCommandFailed",
            "message": f"instruqt {' '.join(args)} could not run: {error}",
            "command": ["instruqt", *args],
        }

    output = bounded_text("\n".join([result.stdout or "", result.stderr or ""]), limit=2000)
    if result.returncode == 0:
        return {
            "phase": "Succeeded",
            "reason": "InstruqtCommandSucceeded",
            "message": f"instruqt {' '.join(args)} succeeded.",
            "command": ["instruqt", *args],
            **({"output": output} if output else {}),
        }
    return {
        "phase": "Failed",
        "reason": "InstruqtCommandFailed",
        "message": f"instruqt {' '.join(args)} exited with {result.returncode}.",
        "command": ["instruqt", *args],
        "exitCode": result.returncode,
        **({"output": output} if output else {}),
    }


def instruqt_command_failure_status(
    phase: str,
    *,
    channel: str,
    target: str,
    message: str,
    artifact_status: dict[str, Any],
    command_status: dict[str, Any],
    push_status: dict[str, Any] | None = None,
    test_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "phase": phase,
        "channel": channel,
        "target": target,
        "message": message,
        "artifact": artifact_status,
    }
    if push_status:
        status["push"] = push_status
    if test_status:
        status["test"] = test_status
    if phase == "PushFailed":
        status["push"] = command_status
    elif phase == "TestFailed":
        status["test"] = command_status
    elif phase == "DeployFailed":
        status["deploy"] = command_status
    return status


def render_instruqt_track_metadata(
    instruqt_course: dict[str, Any],
    rendered_track: Path,
    *,
    channel: str,
) -> dict[str, Any]:
    track_path = rendered_track / "track.yml"
    if not track_path.exists():
        raise ValueError("Instruqt track.yml is required")
    try:
        decoded = yaml.safe_load(track_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        raise ValueError(f"Instruqt track.yml could not be parsed: {error}") from error
    if not isinstance(decoded, dict):
        raise ValueError("Instruqt track.yml must be an object")

    target = instruqt_track_status(instruqt_course, channel)
    organization = target.get("organization")
    slug = target.get("slug")
    if not isinstance(organization, str) or not organization:
        raise ValueError("spec.track.organization is required for Instruqt deployment")
    if not isinstance(slug, str) or not slug:
        raise ValueError("spec.track.slug is required for Instruqt deployment")

    decoded["owner"] = organization
    decoded["slug"] = slug
    decoded.pop("id", None)
    title_prefix = target.get("titlePrefix")
    title = decoded.get("title")
    if (
        channel == INSTRUQT_TESTING_CHANNEL
        and isinstance(title_prefix, str)
        and title_prefix
        and isinstance(title, str)
        and not title.startswith(title_prefix)
    ):
        decoded["title"] = f"{title_prefix}{title}"

    track_path.write_text(yaml.safe_dump(decoded, sort_keys=False), encoding="utf-8")
    rendered = {
        "organization": organization,
        "slug": slug,
        "target": f"{organization}/{slug}",
    }
    if isinstance(decoded.get("title"), str):
        rendered["title"] = decoded["title"]
    if channel == INSTRUQT_TESTING_CHANNEL and isinstance(title_prefix, str):
        rendered["titlePrefix"] = title_prefix
    return rendered


def instruqt_track_target(instruqt_course: dict[str, Any], channel: str) -> str:
    status = instruqt_track_status(instruqt_course, channel)
    organization = status.get("organization")
    slug = status.get("slug")
    if not isinstance(organization, str) or not organization:
        raise non_retryable_error(
            "spec.track.organization is required for Instruqt deployment",
            error_type=VALIDATION_ERROR,
        )
    if not isinstance(slug, str) or not slug:
        raise non_retryable_error(
            "spec.track.slug is required for Instruqt deployment",
            error_type=VALIDATION_ERROR,
        )
    return f"{organization}/{slug}"


def instruqt_track_status(instruqt_course: dict[str, Any], channel: str) -> dict[str, Any]:
    if channel == INSTRUQT_TESTING_CHANNEL:
        return testing_track_status(instruqt_course)
    return track_status(instruqt_course)


def completed_instruqt_image_plan(status: dict[str, Any]) -> dict[str, Any]:
    return {"action": "complete", "status": status}


def completed_instruqt_image_status(value: dict[str, Any]) -> dict[str, Any] | None:
    status = value.get("status")
    return (
        status
        if value.get("action") == "complete" and isinstance(status, dict)
        else None
    )


def reusable_instruqt_image_build_status(
    instruqt_course: dict[str, Any],
    previous_build_status: dict[str, Any] | None,
    external_resources: dict[str, Any],
    *,
    channel: str,
    started_at: str,
) -> dict[str, Any] | None:
    if not isinstance(previous_build_status, dict):
        return None
    if previous_build_status.get("phase") not in {"Pushed", "Resolved"}:
        return None
    if previous_build_status.get("lastError"):
        return None
    if previous_build_status.get("imageDefinitionHash") != instruqt_image_definition_hash(
        instruqt_course
    ):
        return None
    if previous_build_status.get("channel") != channel:
        return None

    for key, requested in instruqt_channel_source_identity(
        instruqt_course,
        channel,
    ).items():
        previous = previous_build_status.get(key)
        if isinstance(requested, str) and requested:
            if previous != requested:
                return None

    previous_images = previous_build_status.get("images")
    if not isinstance(previous_images, dict):
        return None
    images: dict[str, str] = {}
    for image_name in sorted(image_bindings(instruqt_course)):
        image = previous_images.get(image_name)
        if not isinstance(image, str) or not image:
            return None
        images[image_name] = image

    return instruqt_image_build_status(
        instruqt_course,
        channel=channel,
        phase="Resolved",
        mode="workflow-cache",
        started_at=started_at,
        images=images,
        message="Source artifacts were already built by this entity workflow; reusing immutable images.",
        build_entries=instruqt_image_build_entries(
            instruqt_course,
            instruqt_codebuild_image_templates(
                instruqt_course,
                external_resources,
                WorkerConfig.from_env(),
            ),
            images=images,
            external_resources=external_resources,
            phase="Resolved",
            source="workflow-cache",
        ),
    )


def instruqt_image_build_status(
    instruqt_course: dict[str, Any],
    *,
    channel: str,
    phase: str,
    mode: str,
    started_at: str,
    images: dict[str, str],
    message: str,
    build_entries: list[dict[str, Any]],
    error: str | None = None,
    codebuild_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    completed_at = datetime.now(UTC).isoformat()
    status: dict[str, Any] = {
        "phase": phase,
        "channel": channel,
        "mode": mode,
        "startedAt": started_at,
        "completedAt": completed_at,
        "message": message,
        "images": images,
        "missingImages": [
            image_name
            for image_name in sorted(image_bindings(instruqt_course))
            if image_name not in images
        ],
        "builds": build_entries,
        "imageDefinitionHash": instruqt_image_definition_hash(instruqt_course),
        **instruqt_channel_source_identity(instruqt_course, channel),
    }
    source_commit = instruqt_channel_source_commit_sha(instruqt_course, channel)
    if source_commit:
        status["imageVersion"] = instruqt_image_version(instruqt_course, source_commit)
    if error:
        status["lastError"] = {
            "reason": "BuildFailed",
            "message": error,
            "lastTransitionTime": completed_at,
        }
    if codebuild_status:
        status["codebuild"] = codebuild_status
    return status


def instruqt_image_build_entries(
    instruqt_course: dict[str, Any],
    image_templates: dict[str, str],
    *,
    images: dict[str, str],
    external_resources: dict[str, Any],
    phase: str,
    source: str,
) -> list[dict[str, Any]]:
    entries = []
    for image_name, binding in sorted(image_bindings(instruqt_course).items()):
        entry: dict[str, Any] = {
            "name": image_name,
            "phase": phase,
            "source": source,
            "strategy": "dockerfile",
            "container": binding.get("container"),
            "context": binding.get("context"),
            "dockerfile": binding.get("dockerfile"),
            "repository": instruqt_image_repository_name(instruqt_course, image_name),
        }
        repository = instruqt_repository_status_for_image(image_name, external_resources)
        if isinstance(repository, dict) and isinstance(repository.get("name"), str):
            entry["repository"] = repository["name"]
        if image_name in image_templates:
            entry["imageTemplate"] = image_templates[image_name]
        if image_name in images:
            entry["image"] = images[image_name]
        entries.append(entry)
    return entries


def instruqt_codebuild_source_plan(
    instruqt_course: dict[str, Any],
    channel: str,
) -> dict[str, dict[str, str]]:
    sources: dict[str, dict[str, str]] = {
        "source": {
            "repo": required_source_repo(instruqt_course, "source"),
            "version": required_channel_commit(instruqt_course, channel, "source"),
            "branch": instruqt_channel_source_branch(instruqt_course, channel),
        }
    }
    auth_path = source_auth_secret_path(instruqt_course, "source")
    if auth_path:
        sources["source"]["authSecretPath"] = auth_path

    if content_source_config(instruqt_course):
        sources["content"] = {
            "repo": required_source_repo(instruqt_course, "content"),
            "version": required_channel_commit(instruqt_course, channel, "content"),
            "branch": instruqt_channel_content_branch(instruqt_course, channel),
        }
        auth_path = source_auth_secret_path(instruqt_course, "content")
        if auth_path:
            sources["content"]["authSecretPath"] = auth_path
    return sources


def instruqt_codebuild_image_templates(
    instruqt_course: dict[str, Any],
    external_resources: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, str]:
    image_templates: dict[str, str] = {}
    for image_name in sorted(image_bindings(instruqt_course)):
        repository_uri = instruqt_repository_uri_for_image(image_name, external_resources)
        if not repository_uri:
            if not config.aws_account_id:
                raise ValueError(
                    "DEMO_REGISTRY_AWS_ACCOUNT_ID is required when ECR repositoryUri "
                    "is not available from external resource status."
                )
            repository_uri = (
                f"{config.aws_account_id}.dkr.ecr.{config.aws_region}.amazonaws.com/"
                f"{instruqt_image_repository_name(instruqt_course, image_name)}"
            )
        image_templates[image_name] = f"{repository_uri}:${{IMAGE_VERSION}}"
    return image_templates


def instruqt_codebuild_start_build_args(
    instruqt_course: dict[str, Any],
    config: WorkerConfig,
    *,
    channel: str,
    started_at: str,
    image_templates: dict[str, str],
    sources: dict[str, dict[str, str]],
) -> dict[str, Any]:
    if not config.codebuild_project:
        raise ValueError("DEMO_REGISTRY_CODEBUILD_PROJECT is required")
    environment_variables = [
        {
            "name": "INSTRUQT_COURSE_NAME",
            "value": project_name(instruqt_course),
            "type": "PLAINTEXT",
        },
        {
            "name": "INSTRUQT_COURSE_CHANNEL",
            "value": channel,
            "type": "PLAINTEXT",
        },
        {
            "name": "DEMO_REGISTRY_BUILD_MODE",
            "value": "aws-codebuild",
            "type": "PLAINTEXT",
        },
    ]
    for source_key, source in sorted(sources.items()):
        prefix = f"INSTRUQT_{source_key.upper()}_"
        environment_variables.extend(
            [
                {
                    "name": f"{prefix}REPO",
                    "value": source["repo"],
                    "type": "PLAINTEXT",
                },
                {
                    "name": f"{prefix}VERSION",
                    "value": source["version"],
                    "type": "PLAINTEXT",
                },
            ]
        )
        auth_path = source.get("authSecretPath")
        if auth_path:
            environment_variables.append(
                {
                    "name": f"{prefix}AUTH_JSON",
                    "value": auth_path,
                    "type": "SECRETS_MANAGER",
                }
            )

    return {
        "projectName": config.codebuild_project,
        "sourceTypeOverride": "NO_SOURCE",
        "buildspecOverride": instruqt_codebuild_buildspec(
            instruqt_course,
            image_templates,
            sources=sources,
            aws_region=config.aws_region,
        ),
        "environmentVariablesOverride": environment_variables,
        "privilegedModeOverride": True,
        "timeoutInMinutesOverride": config.codebuild_timeout_minutes,
        "idempotencyToken": instruqt_codebuild_idempotency_token(
            instruqt_course,
            channel=channel,
            started_at=started_at,
            image_templates=image_templates,
            sources=sources,
        ),
    }


def instruqt_codebuild_buildspec(
    instruqt_course: dict[str, Any],
    image_templates: dict[str, str],
    *,
    sources: dict[str, dict[str, str]],
    aws_region: str,
) -> str:
    registries = sorted(
        {
            image_template.split("/", 1)[0]
            for image_template in image_templates.values()
            if "/" in image_template
        }
    )
    pre_build_commands = [
        *instruqt_source_checkout_commands(sources),
        f'IMAGE_VERSION="{instruqt_image_version(instruqt_course, "${INSTRUQT_SOURCE_SHA}")}"',
        *[
            (
                f"aws ecr get-login-password --region {shlex.quote(aws_region)} "
                f"| docker login --username AWS --password-stdin {shlex.quote(registry)}"
            )
            for registry in registries
        ],
    ]

    build_commands = []
    for image_name, binding in sorted(image_bindings(instruqt_course).items()):
        image = image_templates[image_name]
        command = ["docker", "build"]
        platform = binding.get("platform")
        if isinstance(platform, str) and platform.strip():
            command.extend(["--platform", platform.strip()])
        target = binding.get("target")
        if isinstance(target, str) and target.strip():
            command.extend(["--target", target.strip()])
        build_args = binding.get("buildArgs")
        if isinstance(build_args, dict):
            for name, value in sorted(build_args.items()):
                if isinstance(name, str):
                    command.extend(["--build-arg", f"{name}={value}"])
        command.extend(
            [
                "-f",
                codebuild_source_path(binding["dockerfile"]),
            ]
        )
        build_command = " ".join(shlex.quote(part) for part in command)
        build_command += " -t " + shell_quote_image(image)
        build_command += " " + shlex.quote(codebuild_source_path(binding["context"]))
        build_commands.append(build_command)
        build_commands.append("docker push " + shell_quote_image(image))

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


def instruqt_source_checkout_commands(sources: dict[str, dict[str, str]]) -> list[str]:
    auth_setup_script = "\n".join(
        [
            "import json, os, pathlib, urllib.parse",
            "home = pathlib.Path.home()",
            'ssh = home / ".ssh"',
            "ssh.mkdir(mode=0o700, exist_ok=True)",
            "credential_lines = []",
            "for env_name in ('INSTRUQT_SOURCE_AUTH_JSON', 'INSTRUQT_CONTENT_AUTH_JSON'):",
            "    raw = os.environ.get(env_name)",
            "    if not raw:",
            "        continue",
            "    data = json.loads(raw)",
            '    identity = data.get("identity")',
            "    if identity:",
            '        known_hosts = data.get("known_hosts", "")',
            '        identity += "" if identity.endswith("\\n") else "\\n"',
            '        known_hosts += "" if known_hosts.endswith("\\n") else "\\n"',
            '        (ssh / "id_source").write_text(identity)',
            '        (ssh / "known_hosts").write_text(known_hosts)',
            "        (ssh / \"config\").write_text(",
            '            "Host *\\n  IdentityFile ~/.ssh/id_source\\n"',
            '            "  UserKnownHostsFile ~/.ssh/known_hosts\\n"',
            '            "  StrictHostKeyChecking yes\\n"',
            "        )",
            '        os.chmod(ssh / "id_source", 0o600)',
            '    username = data.get("username") or "x-access-token"',
            '    password = data.get("password") or data.get("token") or data.get("githubToken")',
            "    if password:",
            '        quoted_user = urllib.parse.quote(username, safe="")',
            '        quoted_password = urllib.parse.quote(password, safe="")',
            '        credential_lines.append(f"https://{quoted_user}:{quoted_password}@github.com")',
            "if credential_lines:",
            '    creds = home / ".git-credentials"',
            '    creds.write_text("\\n".join(credential_lines) + "\\n")',
            "    os.chmod(creds, 0o600)",
        ]
    )
    commands = [
        'mkdir -p "$HOME/.ssh"',
        "python3 -c " + shlex.quote(auth_setup_script),
        'git config --global credential.helper "store --file=$HOME/.git-credentials"',
        "export GIT_TERMINAL_PROMPT=0",
    ]
    for source_key in sorted(sources):
        env_prefix = f"INSTRUQT_{source_key.upper()}"
        destination = f"/tmp/{source_key}"
        commands.extend(
            [
                f'git clone --no-checkout "${env_prefix}_REPO" {destination}',
                f'cd {destination} && git checkout "${env_prefix}_VERSION"',
            ]
        )
    commands.append("INSTRUQT_SOURCE_SHA=$(cd /tmp/source && git rev-parse HEAD)")
    return commands


def codebuild_source_path(location: dict[str, Any]) -> str:
    source_key = location.get("source")
    path = location.get("path")
    if source_key not in {"source", "content"}:
        raise ValueError("Instruqt image source must be source or content")
    if not isinstance(path, str) or not path:
        raise ValueError("Instruqt image path is required")
    return f"/tmp/{source_key}/{path.strip('/')}" if path != "." else f"/tmp/{source_key}"


def shell_quote_image(image_template: str) -> str:
    for suffix in ("${IMAGE_VERSION}", "${INSTRUQT_SOURCE_SHA}"):
        if image_template.endswith(suffix):
            return shlex.quote(image_template[: -len(suffix)]) + suffix
    return shlex.quote(image_template)


def instruqt_codebuild_idempotency_token(
    instruqt_course: dict[str, Any],
    *,
    channel: str,
    started_at: str,
    image_templates: dict[str, str],
    sources: dict[str, dict[str, str]],
) -> str:
    encoded = json.dumps(
        {
            "course": project_name(instruqt_course),
            "channel": channel,
            "startedAt": started_at,
            "imageTemplates": image_templates,
            "sources": sources,
            "images": image_bindings(instruqt_course),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def instruqt_image_templates_from_plan(plan: dict[str, Any]) -> dict[str, str]:
    image_templates = plan.get("imageTemplates")
    if not isinstance(image_templates, dict):
        raise non_retryable_error(
            "Instruqt CodeBuild image plan is missing imageTemplates",
            error_type=VALIDATION_ERROR,
        )
    rendered = {
        str(name): str(template)
        for name, template in image_templates.items()
        if isinstance(name, str) and isinstance(template, str) and template
    }
    if not rendered:
        raise non_retryable_error(
            "Instruqt CodeBuild image plan has no image templates",
            error_type=VALIDATION_ERROR,
        )
    return rendered


def instruqt_sources_from_plan(plan: dict[str, Any]) -> dict[str, dict[str, str]]:
    sources = plan.get("sources")
    if not isinstance(sources, dict):
        raise non_retryable_error(
            "Instruqt CodeBuild image plan is missing sources",
            error_type=VALIDATION_ERROR,
        )
    rendered: dict[str, dict[str, str]] = {}
    for source_key, source in sources.items():
        if not isinstance(source_key, str) or not isinstance(source, dict):
            continue
        rendered[source_key] = {
            str(key): str(value)
            for key, value in source.items()
            if isinstance(key, str) and isinstance(value, str) and value
        }
    if "source" not in rendered:
        raise non_retryable_error(
            "Instruqt CodeBuild image plan is missing source checkout",
            error_type=VALIDATION_ERROR,
        )
    return rendered


def wait_for_instruqt_codebuild_build(
    codebuild: Any,
    build_id: str,
    *,
    config: WorkerConfig,
    instruqt_course: dict[str, Any],
    channel: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + (config.codebuild_timeout_minutes * 60)
    while True:
        response = codebuild.batch_get_builds(ids=[build_id])
        builds_response = response.get("builds", [])
        build = builds_response[0] if builds_response else {}
        status = build.get("buildStatus")
        activity.heartbeat(
            {
                "course": project_name(instruqt_course),
                "channel": channel,
                "codebuildBuildId": build_id,
                "status": status,
            }
        )
        if status in {"SUCCEEDED", "FAILED", "FAULT", "STOPPED", "TIMED_OUT"}:
            return build
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for CodeBuild build {build_id}")
        time.sleep(config.codebuild_poll_seconds)


def render_instruqt_image_templates(
    instruqt_course: dict[str, Any],
    image_templates: dict[str, str],
    source_commit: str,
) -> dict[str, str]:
    version = instruqt_image_version(instruqt_course, source_commit)
    return {
        image_name: image_template.replace("${IMAGE_VERSION}", version).replace(
            "${INSTRUQT_SOURCE_SHA}",
            source_commit[:12],
        )
        for image_name, image_template in image_templates.items()
    }


def instruqt_image_version(instruqt_course: dict[str, Any], source_commit: str) -> str:
    source = source_commit if source_commit != "${INSTRUQT_SOURCE_SHA}" else source_commit
    suffix = f"-b{instruqt_image_definition_hash(instruqt_course)[:12]}"
    return f"git-{source[:12]}{suffix}" if source != "${INSTRUQT_SOURCE_SHA}" else f"git-{source}{suffix}"


def instruqt_image_definition_hash(instruqt_course: dict[str, Any]) -> str:
    encoded = json.dumps(
        image_bindings(instruqt_course),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def instruqt_channel_source_identity(
    instruqt_course: dict[str, Any],
    channel: str,
) -> dict[str, str]:
    identity: dict[str, str] = {}
    source_commit = instruqt_channel_source_commit_sha(instruqt_course, channel)
    if source_commit:
        identity["sourceCommitSha"] = source_commit
    source_digest = instruqt_channel_source_artifact_digest(instruqt_course, channel)
    if source_digest:
        identity["sourceArtifactDigest"] = source_digest
    content_commit = instruqt_channel_content_commit_sha(instruqt_course, channel)
    if content_commit:
        identity["contentSourceCommitSha"] = content_commit
    content_digest = instruqt_channel_content_artifact_digest(instruqt_course, channel)
    if content_digest:
        identity["contentSourceArtifactDigest"] = content_digest
    return identity


def instruqt_channel_source_commit_sha(
    instruqt_course: dict[str, Any],
    channel: str,
) -> str | None:
    if channel == INSTRUQT_TESTING_CHANNEL:
        return testing_source_commit_sha(instruqt_course)
    return source_commit_sha(instruqt_course)


def instruqt_channel_source_artifact_digest(
    instruqt_course: dict[str, Any],
    channel: str,
) -> str | None:
    if channel == INSTRUQT_TESTING_CHANNEL:
        return testing_source_artifact_digest(instruqt_course)
    return source_artifact_digest(instruqt_course)


def instruqt_channel_content_commit_sha(
    instruqt_course: dict[str, Any],
    channel: str,
) -> str | None:
    if not content_source_config(instruqt_course):
        return None
    if channel == INSTRUQT_TESTING_CHANNEL:
        return testing_content_source_commit_sha(instruqt_course)
    return content_source_commit_sha(instruqt_course)


def instruqt_channel_content_artifact_digest(
    instruqt_course: dict[str, Any],
    channel: str,
) -> str | None:
    if not content_source_config(instruqt_course):
        return None
    if channel == INSTRUQT_TESTING_CHANNEL:
        return testing_content_source_artifact_digest(instruqt_course)
    return content_source_artifact_digest(instruqt_course)


def instruqt_channel_source_branch(instruqt_course: dict[str, Any], channel: str) -> str:
    if channel == INSTRUQT_TESTING_CHANNEL:
        branch = instruqt_course_testing_source_branch(instruqt_course)
        if branch:
            return branch
    return source_branch(instruqt_course)


def instruqt_channel_content_branch(instruqt_course: dict[str, Any], channel: str) -> str:
    if channel == INSTRUQT_TESTING_CHANNEL:
        branch = instruqt_course_testing_content_branch(instruqt_course)
        if branch:
            return branch
    return source_branch(instruqt_course, "content")


def required_channel_commit(
    instruqt_course: dict[str, Any],
    channel: str,
    source_key: str,
) -> str:
    commit = (
        instruqt_channel_source_commit_sha(instruqt_course, channel)
        if source_key == "source"
        else instruqt_channel_content_commit_sha(instruqt_course, channel)
    )
    if not commit:
        raise ValueError(f"{channel} {source_key} commit SHA is not resolved")
    return commit


def required_source_repo(instruqt_course: dict[str, Any], source_key: str) -> str:
    repo = source_git_repository_url(instruqt_course, source_key)
    if not isinstance(repo, str) or not repo:
        raise ValueError(f"spec.{source_key}.repo is required")
    return repo


def instruqt_image_repository_plan_entry(
    instruqt_course: dict[str, Any],
    image_name: str,
) -> dict[str, Any]:
    return {
        "name": instruqt_image_repository_name(instruqt_course, image_name),
        "project": project_name(instruqt_course),
        "build": image_name,
        "components": [str(image_bindings(instruqt_course).get(image_name, {}).get("container") or image_name)],
    }


def instruqt_image_repository_name(
    instruqt_course: dict[str, Any],
    image_name: str,
) -> str:
    return f"{derive.image_repository_prefix()}/{project_name(instruqt_course)}/{image_name}"


def instruqt_repository_uri_for_image(
    image_name: str,
    external_resources: dict[str, Any],
) -> str | None:
    repository = instruqt_repository_status_for_image(image_name, external_resources)
    if isinstance(repository, dict) and isinstance(repository.get("repositoryUri"), str):
        return repository["repositoryUri"]
    return None


def instruqt_repository_status_for_image(
    image_name: str,
    external_resources: dict[str, Any],
) -> dict[str, Any] | None:
    ecr = external_resources.get("ecr")
    if not isinstance(ecr, dict):
        ecr = external_resources
    repositories = ecr.get("repositories") if isinstance(ecr, dict) else None
    if not isinstance(repositories, list):
        return None
    for repository in repositories:
        if isinstance(repository, dict) and repository.get("build") == image_name:
            return repository
    return None


def string_value(value: Any, default: str) -> str:
    return value if isinstance(value, str) and value else default


def safe_error_message(error: BaseException) -> str:
    if isinstance(error, subprocess.CalledProcessError):
        output = bounded_text(error.stderr or error.stdout, limit=1000)
        return output or f"command exited with {error.returncode}"
    return str(error)


def image_bindings_status(instruqt_course: dict[str, Any]) -> dict[str, Any]:
    return {
        image_name: {
            "container": image.get("container"),
            "context": image.get("context"),
            "dockerfile": image.get("dockerfile"),
        }
        for image_name, image in image_bindings(instruqt_course).items()
    }


def image_bindings(instruqt_course: dict[str, Any]) -> dict[str, dict[str, Any]]:
    images = spec_of(instruqt_course).get("images")
    if not isinstance(images, dict):
        return {}
    return {key: image for key, image in images.items() if isinstance(image, dict)}


def track_status(instruqt_course: dict[str, Any]) -> dict[str, Any]:
    track = spec_of(instruqt_course).get("track")
    track = track if isinstance(track, dict) else {}
    status: dict[str, Any] = {}
    organization = track.get("organization")
    slug = track.get("slug")
    if isinstance(organization, str) and organization.strip():
        status["organization"] = organization.strip()
    if isinstance(slug, str) and slug.strip():
        status["slug"] = slug.strip()
    return status


def condition(
    condition_type: str,
    status: str,
    observed_generation: int,
    reason: str,
    message: str,
    transition_time: str,
) -> dict[str, Any]:
    return {
        "type": condition_type,
        "status": status,
        "observedGeneration": observed_generation,
        "reason": reason,
        "message": message,
        "lastTransitionTime": transition_time,
    }


def resource_generation(resource: dict[str, Any]) -> int:
    metadata = resource.get("metadata", {})
    generation = metadata.get("generation") if isinstance(metadata, dict) else None
    return generation if isinstance(generation, int) else 0


def instruqt_course_from_request(request: dict[str, Any]) -> dict[str, Any]:
    if isinstance(request.get("instruqtCourse"), dict):
        return request["instruqtCourse"]
    if isinstance(request.get("metadata"), dict) and isinstance(request.get("spec"), dict):
        return request
    raise non_retryable_error(
        "InstruqtCourse request must include an instruqtCourse object",
        error_type=VALIDATION_ERROR,
    )


def string_field(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if isinstance(value, str) and value:
        return value
    raise non_retryable_error(
        f"required field {key!r} is missing",
        error_type=VALIDATION_ERROR,
    )
