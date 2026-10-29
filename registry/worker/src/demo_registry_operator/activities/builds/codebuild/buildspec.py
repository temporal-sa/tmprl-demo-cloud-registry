from __future__ import annotations

import hashlib
import json
import shlex
from typing import Any

from demo_registry_operator import resources
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.activities.builds import definitions as build_definitions


LAMBDA_SECRET_WRAPPER_GO_SOURCE = r'''
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type manifestRef struct {
	Name      string `json:"name"`
	SecretARN string `json:"secretArn"`
	SecretID  string `json:"secretId"`
	Property  string `json:"property"`
	Path      string `json:"path"`
}

type secretValueResponse struct {
	SecretString *string `json:"SecretString"`
	SecretBinary *string `json:"SecretBinary"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tmprl-demo-cloud secret wrapper:", err)
		os.Exit(1)
	}
}

func run() error {
	cache := map[string]map[string]any{}
	envRefs, err := parseManifest("TMPRL_DEMO_CLOUD_SECRET_ENV")
	if err != nil {
		return err
	}
	for _, ref := range envRefs {
		if ref.Name == "" {
			return errors.New("secret env manifest entry is missing name")
		}
		value, err := materializeSecretProperty(ref, cache)
		if err != nil {
			return err
		}
		if err := os.Setenv(ref.Name, value); err != nil {
			return err
		}
	}

	fileRefs, err := parseManifest("TMPRL_DEMO_CLOUD_SECRET_FILES")
	if err != nil {
		return err
	}
	for _, ref := range fileRefs {
		if ref.Path == "" {
			return errors.New("secret file manifest entry is missing path")
		}
		value, err := materializeSecretProperty(ref, cache)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(ref.Path), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(ref.Path, []byte(value), 0o600); err != nil {
			return err
		}
	}

	command, err := originalCommand(os.Args[1:])
	if err != nil {
		return err
	}
	return syscall.Exec(command[0], command, os.Environ())
}

func parseManifest(name string) ([]manifestRef, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil, nil
	}
	var refs []manifestRef
	if err := json.Unmarshal([]byte(raw), &refs); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	return refs, nil
}

func materializeSecretProperty(ref manifestRef, cache map[string]map[string]any) (string, error) {
	secretID := ref.SecretARN
	if secretID == "" {
		secretID = ref.SecretID
	}
	if secretID == "" {
		return "", errors.New("secret manifest entry is missing secretArn/secretId")
	}
	values, ok := cache[secretID]
	if !ok {
		secretString, err := getSecretValue(secretID)
		if err != nil {
			return "", err
		}
		values, err = decodeSecret(secretID, secretString)
		if err != nil {
			return "", err
		}
		cache[secretID] = values
	}
	value, ok := values[ref.Property]
	if !ok {
		return "", fmt.Errorf("secret %s is missing property %q", secretID, ref.Property)
	}
	return stringifySecretProperty(value)
}

func decodeSecret(secretID, secretString string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(secretString))
	decoder.UseNumber()
	var values map[string]any
	if err := decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("secret %s must be a JSON object: %w", secretID, err)
	}
	return values, nil
}

func stringifySecretProperty(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case bool:
		if typed {
			return "true", nil
		}
		return "false", nil
	case json.Number:
		return typed.String(), nil
	case nil:
		return "", nil
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}

func getSecretValue(secretID string) (string, error) {
	region := firstEnv("AWS_REGION", "AWS_DEFAULT_REGION")
	if region == "" {
		return "", errors.New("AWS_REGION or AWS_DEFAULT_REGION is required")
	}
	body, err := json.Marshal(map[string]string{"SecretId": secretID})
	if err != nil {
		return "", err
	}
	endpoint := "https://secretsmanager." + region + ".amazonaws.com/"
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", "secretsmanager.GetSecretValue")
	if err := signV4(request, body, region, "secretsmanager"); err != nil {
		return "", err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("GetSecretValue %s failed: %s: %s", secretID, response.Status, strings.TrimSpace(string(responseBody)))
	}
	var decoded secretValueResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return "", err
	}
	if decoded.SecretString != nil {
		return *decoded.SecretString, nil
	}
	if decoded.SecretBinary != nil {
		bytes, err := base64.StdEncoding.DecodeString(*decoded.SecretBinary)
		if err != nil {
			return "", err
		}
		return string(bytes), nil
	}
	return "", fmt.Errorf("GetSecretValue %s returned no SecretString or SecretBinary", secretID)
}

func signV4(request *http.Request, payload []byte, region, service string) error {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")
	if accessKey == "" || secretKey == "" {
		return errors.New("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")
	}

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	request.Header.Set("X-Amz-Date", amzDate)
	if sessionToken != "" {
		request.Header.Set("X-Amz-Security-Token", sessionToken)
	}

	payloadHash := sha256Hex(payload)
	canonicalHeaders, signedHeaders := canonicalHeaders(request)
	canonicalRequest := strings.Join([]string{
		request.Method,
		"/",
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signingKey := v4SigningKey(secretKey, date, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	request.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential="+accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature,
	)
	return nil
}

func canonicalHeaders(request *http.Request) (string, string) {
	headers := map[string]string{
		"host": request.URL.Host,
	}
	for key, values := range request.Header {
		lower := strings.ToLower(key)
		normalized := make([]string, 0, len(values))
		for _, value := range values {
			normalized = append(normalized, strings.Join(strings.Fields(value), " "))
		}
		headers[lower] = strings.Join(normalized, ",")
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		lines = append(lines, name+":"+headers[name])
	}
	return strings.Join(lines, "\n") + "\n", strings.Join(names, ";")
}

func v4SigningKey(secretKey, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func originalCommand(args []string) ([]string, error) {
	entrypoint, err := decodeStringArrayEnv("TMPRL_ORIGINAL_ENTRYPOINT_B64")
	if err != nil {
		return nil, err
	}
	cmd, err := decodeStringArrayEnv("TMPRL_ORIGINAL_CMD_B64")
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		args = cmd
	}
	if len(entrypoint) > 0 {
		return append(entrypoint, args...), nil
	}
	if len(args) > 0 {
		return args, nil
	}
	return nil, errors.New("original image has no entrypoint or command")
}

func decodeStringArrayEnv(name string) ([]string, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var values []string
	if err := json.Unmarshal(decoded, &values); err != nil {
		return nil, err
	}
	return values, nil
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}
'''


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
            "value": resources.project_name(demo_project),
            "type": "PLAINTEXT",
        },
        {
            "name": "DEMO_REGISTRY_BUILD_MODE",
            "value": "aws-codebuild",
            "type": "PLAINTEXT",
        },
    ]
    if config.dockerhub_secret_path:
        environment_variables.extend(
            [
                {
                    "name": "DOCKERHUB_USERNAME",
                    "value": f"{config.dockerhub_secret_path}:username",
                    "type": "SECRETS_MANAGER",
                },
                {
                    "name": "DOCKERHUB_TOKEN",
                    "value": f"{config.dockerhub_secret_path}:token",
                    "type": "SECRETS_MANAGER",
                },
            ]
        )
    start_args: dict[str, Any] = {
        "projectName": config.codebuild_project,
        "buildspecOverride": codebuild_buildspec(
            demo_project,
            image_templates,
            aws_region=config.aws_region,
            manual_source_checkout=bool(source_auth_secret_path),
            dockerhub_login=bool(config.dockerhub_secret_path),
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
    dockerhub_login: bool = False,
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
            f'IMAGE_VERSION="git-${{SOURCE_SHA}}{build_definitions.image_version_suffix(demo_project)}"',
        ]
    )
    if dockerhub_login:
        pre_build_commands.append(
            'printf "%s" "$DOCKERHUB_TOKEN" '
            '| docker login --username "$DOCKERHUB_USERNAME" --password-stdin'
        )
    if any(
        build_definitions.build_strategy(build) == build_definitions.BUILDPACK_BUILD_STRATEGY
        for build in resources.builds(demo_project).values()
    ):
        pre_build_commands.append(pack_install_command())
    lambda_build_architectures = serverless_lambda_build_architectures(demo_project)
    if lambda_build_architectures:
        pre_build_commands.extend(
            lambda_secret_wrapper_build_commands(
                sorted(set(lambda_build_architectures.values()))
            )
        )
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
    for build_name, build in sorted(resources.builds(demo_project).items()):
        image = image_templates[build_name]
        shell_image = shell_quote_image_template(image)
        lambda_goarch = lambda_build_architectures.get(build_name)
        strategy = build_definitions.build_strategy(build)
        if strategy == build_definitions.DOCKERFILE_BUILD_STRATEGY:
            target_image = (
                shlex.quote(local_unwrapped_image_tag(build_name))
                if lambda_goarch
                else shell_image
            )
            build_commands.append(
                source_prefix
                + " ".join(
                    [
                        "docker",
                        "build",
                        "-f",
                        shlex.quote(build_definitions.required_build_string(build, "dockerfile")),
                        "-t",
                        target_image,
                        shlex.quote(build_definitions.required_build_string(build, "context")),
                    ]
                )
            )
            if lambda_goarch:
                build_commands.extend(
                    wrap_lambda_image_commands(
                        build_name=build_name,
                        base_image=local_unwrapped_image_tag(build_name),
                        target_image=image,
                        goarch=lambda_goarch,
                    )
                )
            build_commands.append("docker push " + shell_image)
            continue

        if strategy == build_definitions.BUILDPACK_BUILD_STRATEGY:
            process_buildpack_command = build_definitions.buildpack_process_buildpack_command(
                build_name,
                build,
            )
            process_buildpack_path = build_definitions.buildpack_process_buildpack_path(build_name)
            if process_buildpack_command:
                build_commands.append(source_prefix + process_buildpack_command)
            command = [
                "pack",
                "build",
                shell_image,
                "--builder",
                shlex.quote(build_definitions.required_build_string(build, "builder")),
                "--path",
                shlex.quote(build_definitions.required_build_string(build, "context")),
            ]
            for name, value in sorted(build_definitions.buildpack_env(build).items()):
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
        f"https://github.com/buildpacks/pack/releases/download/v{build_definitions.PACK_VERSION}/"
        f"pack-v{build_definitions.PACK_VERSION}-${{pack_arch}}.tgz "
        "| tar -xz -C /usr/local/bin pack"
    )


def lambda_secret_wrapper_build_commands(goarches: list[str]) -> list[str]:
    commands = [write_lambda_secret_wrapper_source_command()]
    commands.extend(compile_lambda_secret_wrapper_command(goarch) for goarch in goarches)
    return commands


def write_lambda_secret_wrapper_source_command() -> str:
    script = (
        "import pathlib; "
        "root=pathlib.Path('/tmp/tmprl-lambda-secret-wrapper'); "
        "root.mkdir(parents=True, exist_ok=True); "
        "(root/'main.go').write_text("
        + json.dumps(LAMBDA_SECRET_WRAPPER_GO_SOURCE)
        + ", encoding='utf-8')"
    )
    return "python3 -c " + shlex.quote(script)


def compile_lambda_secret_wrapper_command(goarch: str) -> str:
    return " ".join(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            "/tmp/tmprl-lambda-secret-wrapper:/src",
            "-w",
            "/src",
            "-e",
            "CGO_ENABLED=0",
            "-e",
            "GOOS=linux",
            "-e",
            f"GOARCH={shlex.quote(goarch)}",
            "public.ecr.aws/docker/library/golang:1.22",
            "go",
            "build",
            "-trimpath",
            "-ldflags",
            shlex.quote("-s -w"),
            "-o",
            shlex.quote(f"/src/bin/{goarch}/tmprl-secret-wrapper"),
            "/src/main.go",
        ]
    )


def wrap_lambda_image_commands(
    *,
    build_name: str,
    base_image: str,
    target_image: str,
    goarch: str,
) -> list[str]:
    context_dir = lambda_wrapper_context_dir(build_name)
    return [
        write_lambda_wrapper_dockerfile_command(
            build_name=build_name,
            base_image=base_image,
            goarch=goarch,
        ),
        " ".join(
            [
                "docker",
                "build",
                "-f",
                shlex.quote(f"{context_dir}/Dockerfile"),
                "-t",
                shell_quote_image_template(target_image),
                shlex.quote(context_dir),
            ]
        ),
    ]


def write_lambda_wrapper_dockerfile_command(
    *,
    build_name: str,
    base_image: str,
    goarch: str,
) -> str:
    script = (
        "import base64, json, pathlib, shutil, subprocess, sys; "
        "base_image, goarch, build_name = sys.argv[1:4]; "
        "safe=''.join(c if c.isalnum() or c in '._-' else '-' for c in build_name) or 'build'; "
        "root=pathlib.Path('/tmp/tmprl-lambda-wrapper-images')/safe; "
        "root.mkdir(parents=True, exist_ok=True); "
        "wrapper=pathlib.Path('/tmp/tmprl-lambda-secret-wrapper/bin')/goarch/'tmprl-secret-wrapper'; "
        "shutil.copy2(wrapper, root/'tmprl-secret-wrapper'); "
        "config=json.loads(subprocess.check_output(['docker','image','inspect',base_image]))[0].get('Config') or {}; "
        "entrypoint=config.get('Entrypoint') or []; "
        "cmd=config.get('Cmd') or []; "
        "enc=lambda value: base64.b64encode(json.dumps(value, separators=(',', ':')).encode()).decode(); "
        "dockerfile='\\n'.join(["
        "'FROM '+base_image,"
        "'COPY tmprl-secret-wrapper /opt/tmprl-demo-cloud/bin/secret-wrapper',"
        "'ENV TMPRL_ORIGINAL_ENTRYPOINT_B64='+enc(entrypoint),"
        "'ENV TMPRL_ORIGINAL_CMD_B64='+enc(cmd),"
        "'ENTRYPOINT [\"/opt/tmprl-demo-cloud/bin/secret-wrapper\"]',"
        "('CMD '+json.dumps(cmd, separators=(',', ':')) if cmd else ''),"
        "])+'\\n'; "
        "(root/'Dockerfile').write_text(dockerfile, encoding='utf-8')"
    )
    return (
        "python3 -c "
        + shlex.quote(script)
        + " "
        + shlex.quote(base_image)
        + " "
        + shlex.quote(goarch)
        + " "
        + shlex.quote(build_name)
    )


def serverless_lambda_build_architectures(
    demo_project: dict[str, Any],
) -> dict[str, str]:
    architectures: dict[str, str] = {}
    for component_name, component in sorted(
        resources.serverless_lambda_components(demo_project).items()
    ):
        build_name = component.get("build")
        if not isinstance(build_name, str) or not build_name:
            continue
        architecture = resources.serverless_config(component).get(
            "architecture",
            "x86_64",
        )
        goarch = lambda_wrapper_goarch(str(architecture))
        previous = architectures.get(build_name)
        if previous and previous != goarch:
            raise ValueError(
                "serverless aws-lambda components sharing build "
                f"{build_name!r} must use the same architecture; "
                f"{component_name!r} uses {architecture!r}"
            )
        architectures[build_name] = goarch
    return architectures


def lambda_wrapper_goarch(architecture: str) -> str:
    if architecture == "arm64":
        return "arm64"
    return "amd64"


def local_unwrapped_image_tag(build_name: str) -> str:
    safe = "".join(
        character if character.isalnum() or character in "._-" else "-"
        for character in build_name.lower()
    )
    return f"tmprl-demo-local/{safe or 'build'}:unwrapped"


def lambda_wrapper_context_dir(build_name: str) -> str:
    safe = "".join(
        character if character.isalnum() or character in "._-" else "-"
        for character in build_name
    )
    return f"/tmp/tmprl-lambda-wrapper-images/{safe or 'build'}"


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
            "project": resources.project_name(demo_project),
            "sourceRepo": source_repo,
            "sourceVersion": source_version,
            "sourceAuthSecretPath": source_auth_secret_path,
            "idempotencyTokenSalt": idempotency_token_salt,
            "images": image_templates,
            "builds": resources.builds(demo_project),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
