from __future__ import annotations

import hashlib
import json
import shlex
from typing import Any

from demo_registry_operator import resources


DOCKERFILE_BUILD_STRATEGY = "dockerfile"
BUILDPACK_BUILD_STRATEGY = "buildpack"
PACK_VERSION = "0.40.6"
BUILDPACK_IMPLEMENTATION_VERSION = "buildpack-post-processes-v1"
GENERATED_PROCESS_BUILDPACK_ID = "tmprl-demo/generated-processes"
SERVERLESS_LAMBDA_WRAPPER_IMPLEMENTATION_VERSION = "lambda-secret-wrapper-v1"


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
    definition = json.loads(json.dumps(resources.builds(demo_project)))
    if not isinstance(definition, dict):
        return {}
    lambda_builds = serverless_lambda_build_names(demo_project)
    for build_name, build in definition.items():
        if isinstance(build, dict) and build_strategy(build) == BUILDPACK_BUILD_STRATEGY:
            build["_registryBuildImplementation"] = BUILDPACK_IMPLEMENTATION_VERSION
        if isinstance(build, dict) and build_name in lambda_builds:
            build["_registryServerlessLambdaWrapper"] = (
                SERVERLESS_LAMBDA_WRAPPER_IMPLEMENTATION_VERSION
            )
    return definition


def serverless_lambda_build_names(demo_project: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for component in resources.serverless_lambda_components(demo_project).values():
        build_name = component.get("build")
        if isinstance(build_name, str) and build_name:
            names.add(build_name)
    return names


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


def image_version(demo_project: dict[str, Any], source_commit: str) -> str:
    return f"git-{source_commit[:12]}{image_version_suffix(demo_project)}"


def image_version_suffix(demo_project: dict[str, Any]) -> str:
    if any(
        build_strategy(build) == BUILDPACK_BUILD_STRATEGY
        for build in resources.builds(demo_project).values()
    ) or serverless_lambda_build_names(demo_project):
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


def looks_like_commit_sha(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    return 7 <= len(value) <= 40 and all(
        character in "0123456789abcdefABCDEF" for character in value
    )


def same_commit_sha(left: Any, right: Any) -> bool:
    if not isinstance(left, str) or not isinstance(right, str):
        return False
    if not looks_like_commit_sha(left) or not looks_like_commit_sha(right):
        return False
    left = left.lower()
    right = right.lower()
    return left == right or left.startswith(right) or right.startswith(left)
