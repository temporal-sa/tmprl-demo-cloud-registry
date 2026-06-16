from __future__ import annotations

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
    completed_build_status,
    previous_build_status_from_request,
    reusable_previous_build_status,
)
from demo_registry_operator.activities.builds.codebuild.execution import (
    start_project_image_build,
    wait_for_project_image_build,
)


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


def build_project_images(request: dict[str, Any]) -> dict[str, Any]:
    """Compatibility wrapper for direct callers; workflows use split activities."""

    plan = plan_project_images(request)
    if completed_status := completed_build_status(plan):
        return completed_status
    started = start_project_image_build(plan)
    if completed_status := completed_build_status(started):
        return completed_status
    return wait_for_project_image_build(started)
