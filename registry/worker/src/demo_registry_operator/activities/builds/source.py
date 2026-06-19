from __future__ import annotations

from typing import Any

from demo_registry_operator import errors, resources


def source_config(demo_project: dict[str, Any]) -> dict[str, str]:
    source = resources.spec_of(demo_project).get("source")
    if not isinstance(source, dict):
        raise ValueError("DemoProject spec.source is required for builds")
    repo = source.get("repo")
    if not isinstance(repo, str):
        raise ValueError("DemoProject spec.source.repo is required")

    config = {"repo": repo, "branch": resources.source_branch(demo_project)}
    auth_secret_path = resources.source_auth_secret_path(demo_project)
    if auth_secret_path:
        git_repository = source.get("gitRepository")
        git_repository_url = (
            git_repository.get("url") if isinstance(git_repository, dict) else None
        )
        codebuild_repo = resources.source_git_repository_url(demo_project) or repo
        if not isinstance(codebuild_repo, str) or not codebuild_repo.strip():
            raise ValueError(
                "DemoProject source repository URL is required when source auth "
                "is used for aws-codebuild checkout"
            )
        if git_repository and not isinstance(git_repository_url, str):
            raise ValueError(
                "DemoProject spec.source.gitRepository.url must be a string when set"
            )
        config["codebuildRepo"] = codebuild_repo
        config["sourceAuthSecretPath"] = auth_secret_path

    return config


def components_for_build(demo_project: dict[str, Any], build_name: str) -> list[str]:
    return [
        component_name
        for component_name, component in sorted(resources.components(demo_project).items())
        if component.get("build") == build_name
    ]


def demo_project_from_request(request: dict[str, Any]) -> dict[str, Any]:
    demo_project = request.get("demoProject")
    if isinstance(demo_project, dict):
        return demo_project
    if isinstance(request.get("metadata"), dict) and isinstance(request.get("spec"), dict):
        return request
    raise errors.non_retryable_error(
        "DemoProject request must include a demoProject object",
        error_type=errors.VALIDATION_ERROR,
    )


def external_resources_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    external_resources = request.get("externalResources")
    if isinstance(external_resources, dict):
        return external_resources
    return None
