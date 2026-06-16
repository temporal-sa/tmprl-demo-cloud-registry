from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from demo_registry_operator import derive
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    VALIDATION_ERROR,
    non_retryable_error,
)
from demo_registry_operator.resources import (
    builds,
    project_name,
    source_artifact_digest,
    source_commit_sha,
)

from demo_registry_operator.activities.builds.definitions import (
    build_definition_hash,
    build_strategy,
    image_version,
    looks_like_commit_sha,
    render_image_templates,
    same_commit_sha,
)
from demo_registry_operator.activities.builds.source import components_for_build


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
