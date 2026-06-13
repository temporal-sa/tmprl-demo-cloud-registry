from __future__ import annotations

import base64
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.resources import (
    builds,
    components,
    missing_image_overrides,
    parse_image_overrides,
    project_name,
    source_commit_sha,
    spec_of,
)


@dataclass(frozen=True)
class CommandResult:
    args: list[str]
    returncode: int
    stdout: str
    stderr: str


@activity.defn
def build_project_images(request: dict[str, Any]) -> dict[str, Any]:
    """Resolve immutable images for one DemoProject.

    Default `plan` mode is non-mutating. `local-docker` mode clones the source
    repository, builds each configured Dockerfile, pushes immutable ECR tags,
    and returns the resolved image map for candidate rollout.
    """

    demo_project = demo_project_from_request(request)
    external_resources = external_resources_from_request(request)
    config = WorkerConfig.from_env()
    started_at = datetime.now(UTC).isoformat()
    image_overrides = parse_image_overrides(demo_project)
    missing_images = missing_image_overrides(demo_project, image_overrides)

    if not missing_images:
        return build_status(
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

    if config.build_mode == "local-docker":
        return build_with_local_docker(
            demo_project,
            external_resources,
            config,
            started_at=started_at,
        )

    return build_status(
        demo_project,
        phase="Planned",
        mode=config.build_mode,
        started_at=started_at,
        images=image_overrides,
        source_commit_sha=source_commit_sha(demo_project),
        message=(
            "Build plan recorded without building images. Set "
            "DEMO_REGISTRY_BUILD_MODE=local-docker to build and push images."
        ),
        missing_images=missing_images,
        build_entries=[
            build_entry(
                demo_project,
                build_name,
                image=image_overrides.get(build_name),
                phase="Resolved" if build_name in image_overrides else "Planned",
                source="image-overrides" if build_name in image_overrides else "build-plan",
                external_resources=external_resources,
            )
            for build_name in sorted(builds(demo_project))
        ],
    )


def build_with_local_docker(
    demo_project: dict[str, Any],
    external_resources: dict[str, Any] | None,
    config: WorkerConfig,
    *,
    started_at: str,
) -> dict[str, Any]:
    workspace_root = Path(config.build_workspace)
    workspace_root.mkdir(parents=True, exist_ok=True)
    workdir = Path(
        tempfile.mkdtemp(
            prefix=f"{project_name(demo_project)}-",
            dir=str(workspace_root),
        )
    )
    source_dir = workdir / "source"
    source = source_config(demo_project)
    images: dict[str, str] = {}
    build_entries: list[dict[str, Any]] = []
    resolved_commit: str | None = None

    try:
        run_command(["git", "clone", "--depth", "1", "--branch", source["branch"], source["repo"], str(source_dir)])
        requested_commit = source_commit_sha(demo_project)
        if requested_commit:
            run_command(["git", "-C", str(source_dir), "fetch", "--depth", "1", "origin", requested_commit])
            run_command(["git", "-C", str(source_dir), "checkout", requested_commit])
        resolved_commit = run_command(
            ["git", "-C", str(source_dir), "rev-parse", "HEAD"]
        ).stdout.strip()

        login_to_ecr(config)
        for build_name, build in sorted(builds(demo_project).items()):
            image = image_uri_for_build(
                demo_project,
                build_name,
                resolved_commit,
                config,
                external_resources,
            )
            dockerfile = safe_child_path(source_dir, build["dockerfile"])
            context = safe_child_path(source_dir, build["context"])
            run_command(
                [
                    "docker",
                    "build",
                    "-f",
                    str(dockerfile),
                    "-t",
                    image,
                    str(context),
                ]
            )
            run_command(["docker", "push", image])
            images[build_name] = image
            build_entries.append(
                build_entry(
                    demo_project,
                    build_name,
                    image=image,
                    phase="Pushed",
                    source="local-docker",
                    external_resources=external_resources,
                )
            )
    except Exception as error:
        return build_status(
            demo_project,
            phase="Error",
            mode=config.build_mode,
            started_at=started_at,
            images=images,
            source_commit_sha=resolved_commit or source_commit_sha(demo_project),
            message="Image build failed.",
            build_entries=build_entries,
            error=str(error),
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    return build_status(
        demo_project,
        phase="Pushed",
        mode=config.build_mode,
        started_at=started_at,
        images=images,
        source_commit_sha=resolved_commit,
        message="Built and pushed immutable images for every build.",
        build_entries=build_entries,
    )


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
    }
    if source_commit_sha:
        status["sourceCommitSha"] = source_commit_sha
        status["imageVersion"] = f"git-{source_commit_sha[:12]}"
    if error:
        status["lastError"] = {
            "reason": "BuildFailed",
            "message": error,
            "lastTransitionTime": completed_at,
        }
    return status


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
        "context": build["context"],
        "dockerfile": build["dockerfile"],
        "repository": repository_name_for_build(
            demo_project,
            build_name,
            external_resources,
        ),
        "components": components_for_build(demo_project, build_name),
    }
    if image:
        entry["image"] = image
    return entry


def image_uri_for_build(
    demo_project: dict[str, Any],
    build_name: str,
    commit_sha: str,
    config: WorkerConfig,
    external_resources: dict[str, Any] | None,
) -> str:
    repository_uri = repository_uri_for_build(build_name, external_resources)
    if not repository_uri:
        if not config.aws_account_id:
            raise ValueError(
                "DEMO_REGISTRY_AWS_ACCOUNT_ID is required when ECR repositoryUri "
                "is not available from external resource status."
            )
        repository_uri = (
            f"{config.aws_account_id}.dkr.ecr.{config.aws_region}.amazonaws.com/"
            f"{repository_name_for_build(demo_project, build_name, external_resources)}"
        )
    return f"{repository_uri}:git-{commit_sha[:12]}"


def repository_name_for_build(
    demo_project: dict[str, Any],
    build_name: str,
    external_resources: dict[str, Any] | None,
) -> str:
    repository = repository_status_for_build(build_name, external_resources)
    if isinstance(repository, dict) and isinstance(repository.get("name"), str):
        return repository["name"]
    derived = spec_of(demo_project).get("derived", {})
    prefix = "tmprl-cloud-demo"
    if isinstance(derived, dict) and isinstance(derived.get("imageRepositoryPrefix"), str):
        prefix = derived["imageRepositoryPrefix"]
    return f"{prefix}/{project_name(demo_project)}/{build_name}"


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


def login_to_ecr(config: WorkerConfig) -> None:
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required for ECR login") from error

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    response = ecr.get_authorization_token()
    authorization_data = response.get("authorizationData", [])
    if not authorization_data:
        raise RuntimeError("ECR authorization token response was empty")

    token = authorization_data[0].get("authorizationToken")
    proxy_endpoint = authorization_data[0].get("proxyEndpoint")
    if not isinstance(token, str) or not isinstance(proxy_endpoint, str):
        raise RuntimeError("ECR authorization token response was malformed")

    username, password = base64.b64decode(token).decode("utf-8").split(":", 1)
    run_command(
        ["docker", "login", "--username", username, "--password-stdin", proxy_endpoint],
        stdin=password,
    )


def run_command(args: list[str], *, stdin: str | None = None) -> CommandResult:
    activity.heartbeat({"command": args[:3]})
    completed = subprocess.run(
        args,
        input=stdin,
        text=True,
        capture_output=True,
        check=False,
    )
    result = CommandResult(
        args=args,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(args)}\n"
            f"stdout:\n{tail(result.stdout)}\n"
            f"stderr:\n{tail(result.stderr)}"
        )
    return result


def safe_child_path(root: Path, raw_path: str) -> Path:
    path = (root / raw_path).resolve()
    root_resolved = root.resolve()
    if path == root_resolved or root_resolved in path.parents:
        return path
    raise ValueError(f"build path {raw_path!r} escapes source checkout")


def source_config(demo_project: dict[str, Any]) -> dict[str, str]:
    source = spec_of(demo_project).get("source")
    if not isinstance(source, dict):
        raise ValueError("DemoProject spec.source is required for builds")
    repo = source.get("repo")
    branch = source.get("branch")
    if not isinstance(repo, str) or not isinstance(branch, str):
        raise ValueError("DemoProject spec.source.repo and branch are required")
    return {"repo": repo, "branch": branch}


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
    return request


def external_resources_from_request(request: dict[str, Any]) -> dict[str, Any] | None:
    external_resources = request.get("externalResources")
    if isinstance(external_resources, dict):
        return external_resources
    return None


def tail(value: str, *, limit: int = 4000) -> str:
    return value if len(value) <= limit else value[-limit:]
