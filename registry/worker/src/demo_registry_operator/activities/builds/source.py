from __future__ import annotations

from typing import Any

from demo_registry_operator.errors import (
    VALIDATION_ERROR,
    non_retryable_error,
)
from demo_registry_operator.resources import (
    components,
    source_auth_secret_name,
    source_branch,
    source_git_repository_url,
    spec_of,
    user_secret_remote_path,
)


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
