from __future__ import annotations

import hashlib
import json
import shlex
from typing import Any

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.resources import builds, project_name

from demo_registry_operator.activities.builds.definitions import (
    BUILDPACK_BUILD_STRATEGY,
    DOCKERFILE_BUILD_STRATEGY,
    PACK_VERSION,
    build_strategy,
    buildpack_env,
    buildpack_process_buildpack_command,
    buildpack_process_buildpack_path,
    image_version_suffix,
    required_build_string,
)


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
    auth_setup_script = "\n".join(
        [
            "import json, os, pathlib, urllib.parse",
            'data = json.loads(os.environ["DEMO_SOURCE_AUTH_JSON"])',
            "home = pathlib.Path.home()",
            'ssh = home / ".ssh"',
            "ssh.mkdir(mode=0o700, exist_ok=True)",
            'identity = data.get("identity")',
            "if identity:",
            '    known_hosts = data.get("known_hosts", "")',
            '    identity += "" if identity.endswith("\\n") else "\\n"',
            '    known_hosts += "" if known_hosts.endswith("\\n") else "\\n"',
            '    (ssh / "id_source").write_text(identity)',
            '    (ssh / "known_hosts").write_text(known_hosts)',
            "    (ssh / \"config\").write_text(",
            '        "Host *\\n  IdentityFile ~/.ssh/id_source\\n"',
            '        "  UserKnownHostsFile ~/.ssh/known_hosts\\n"',
            '        "  StrictHostKeyChecking yes\\n"',
            "    )",
            '    os.chmod(ssh / "id_source", 0o600)',
            'username = data.get("username")',
            'password = data.get("password")',
            "if username and password:",
            '    quoted_user = urllib.parse.quote(username, safe="")',
            '    quoted_password = urllib.parse.quote(password, safe="")',
            '    creds = home / ".git-credentials"',
            '    creds.write_text(f"https://{quoted_user}:{quoted_password}@github.com\\n")',
            "    os.chmod(creds, 0o600)",
        ]
    )
    return [
        'mkdir -p "$HOME/.ssh"',
        "python3 -c " + shlex.quote(auth_setup_script),
        'git config --global credential.helper "store --file=$HOME/.git-credentials"',
        "export GIT_TERMINAL_PROMPT=0",
        'git clone --no-checkout "$DEMO_SOURCE_REPO" /tmp/source',
        'cd /tmp/source && git checkout "$DEMO_SOURCE_VERSION"',
        "SOURCE_SHA=$(cd /tmp/source && git rev-parse HEAD)",
    ]


def shell_quote_image_template(image_template: str) -> str:
    for suffix in ("${IMAGE_VERSION}", "${SOURCE_SHA}"):
        if image_template.endswith(suffix):
            return shlex.quote(image_template[: -len(suffix)]) + suffix
    return shlex.quote(image_template)


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
