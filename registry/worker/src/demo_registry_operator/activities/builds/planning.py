from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import VALIDATION_ERROR, non_retryable_error
from demo_registry_operator.resources import (
    builds,
    missing_image_overrides,
    parse_image_overrides,
    source_commit_sha,
)

from demo_registry_operator.activities.builds.source import (
    demo_project_from_request,
    external_resources_from_request,
    source_config,
)
from demo_registry_operator.activities.builds.status import (
    build_entry,
    build_status,
    codebuild_image_templates,
    completed_build_plan,
    previous_build_status_from_request,
    reusable_previous_build_status,
)


@dataclass(frozen=True)
class ResolveProjectImageBuildRequest:
    demo_project: dict[str, Any]
    external_resources: dict[str, Any] | None = None
    previous_build_status: dict[str, Any] | None = None


@dataclass(frozen=True)
class ProjectImageBuildPlan:
    plan: dict[str, Any]


@activity.defn
def resolve_project_image_build_plan(
    request: ResolveProjectImageBuildRequest,
) -> ProjectImageBuildPlan:
    """Resolve cached/override images or prepare an AWS CodeBuild request."""

    demo_project = request.demo_project
    if not isinstance(demo_project, dict):
        raise non_retryable_error(
            "resolve_project_image_build_plan requires demo_project",
            error_type=VALIDATION_ERROR,
        )
    external_resources = request.external_resources or {}
    config = WorkerConfig.from_env()
    started_at = datetime.now(UTC).isoformat()

    image_overrides = parse_image_overrides(demo_project)
    missing_images = missing_image_overrides(demo_project, image_overrides)
    previous_build_status = request.previous_build_status

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
        return ProjectImageBuildPlan(completed_build_plan(status))

    reusable_build_status = reusable_previous_build_status(
        demo_project,
        previous_build_status,
        external_resources,
        started_at=started_at,
    )
    if reusable_build_status is not None:
        return ProjectImageBuildPlan(completed_build_plan(reusable_build_status))

    if not config.codebuild_project:
        return ProjectImageBuildPlan(completed_build_plan(
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
        ))

    try:
        source = source_config(demo_project)
        requested_version = source_commit_sha(demo_project) or source["branch"]
        image_templates = codebuild_image_templates(
            demo_project,
            external_resources,
            config,
        )
    except ValueError as error:
        return ProjectImageBuildPlan(completed_build_plan(
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
        ))

    return ProjectImageBuildPlan({
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
    })
