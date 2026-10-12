"""Activities behind catalog onboarding (see workflows/catalog_onboarding.py).

- ``inspect_onboarding_repo`` checks that a demo repo is public and in an allowed
  org, and pins its default-branch commit.
- ``validate_onboarding_draft`` runs scripts/validate_projects.py against the
  draft plus every project already in the cluster, checks that every secret it
  declares is stored with its required properties, and renders the YAML and the
  GitHub new-file link the user opens the PR from.
- ``verify_onboarding_pull_request`` checks that the PR the user opened adds
  exactly the validated file.
"""

from __future__ import annotations

import base64
import json
import re
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import yaml
from temporalio import activity
from temporalio.common import RetryPolicy

from demo_registry_operator import errors, kube, project_validation, resources
from demo_registry_operator.activities.external_resources.models import (
    SecretRequirement,
)
from demo_registry_operator.activities.external_resources.secrets import secret_status
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.workflows.catalog_onboarding_messages import (
    DraftError,
    DraftValidation,
    InspectRepoRequest,
    OnboardingRepo,
    PullRequestCheck,
    ValidateDraftRequest,
    VerifyPullRequestRequest,
)

ALLOWED_REPO_OWNERS = ("temporal-sa", "temporalio", "temporal-community")
CATALOG_REPO = "temporal-sa/tmprl-demo-cloud-registry"
CATALOG_BRANCH = "main"
DEMO_PROJECT_KIND = "DemoProject"
GITHUB_API = "https://api.github.com"
GITHUB_TIMEOUT_SECONDS = 10
REPO_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
DNS_LABEL_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
PULL_REQUEST_PATH = re.compile(r"^/([^/]+)/([^/]+)/pull/(\d+)(?:/.*)?$")
# Spec sections in the order the files in projects/demo use.
SPEC_KEY_ORDER = (
    "registry",
    "source",
    "builds",
    "secrets",
    "temporal",
    "components",
    "ingress",
    "rollout",
    "smokeCheck",
)

# Someone is waiting on the onboarding page for each of these, so fail fast.
ONBOARDING_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


@activity.defn
def inspect_onboarding_repo(request: InspectRepoRequest) -> OnboardingRepo:
    owner, name = parse_repo_url(request.repo_url)
    token = platform_github_token()
    repo = github_get(f"/repos/{quote(owner)}/{quote(name)}", token)
    if repo is None:
        raise errors.non_retryable_error(
            f"github.com/{owner}/{name} was not found. Onboarding needs a public repo."
        )
    if repo.get("private") is not False or repo.get("visibility") != "public":
        raise errors.non_retryable_error(
            f"github.com/{owner}/{name} is not public. Onboarding supports public "
            "repos only for now."
        )
    owner = str((repo.get("owner") or {}).get("login") or owner)
    name = str(repo.get("name") or name)
    if owner.lower() not in ALLOWED_REPO_OWNERS:
        raise errors.non_retryable_error(
            f"github.com/{owner}/{name} moved outside the allowed orgs."
        )
    branch = repo.get("default_branch")
    if not isinstance(branch, str) or not branch:
        raise RuntimeError(f"GitHub returned no default branch for {owner}/{name}.")
    branch_info = github_get(
        f"/repos/{quote(owner)}/{quote(name)}/branches/{quote(branch, safe='')}",
        token,
    )
    commit = (branch_info or {}).get("commit") or {}
    sha = commit.get("sha") if isinstance(commit, dict) else None
    if not isinstance(sha, str) or not sha:
        raise RuntimeError(f"GitHub returned no commit for {owner}/{name}@{branch}.")
    return OnboardingRepo(
        url=f"https://github.com/{owner}/{name}",
        owner=owner,
        name=name,
        default_branch=branch,
        commit_sha=sha,
        description=str(repo.get("description") or ""),
    )


@activity.defn
def validate_onboarding_draft(request: ValidateDraftRequest) -> DraftValidation:
    validator = project_validation.validator_module()
    validators = project_validation.default_validators()

    if request.yaml is not None:
        try:
            document = yaml.safe_load(request.yaml)
        except yaml.YAMLError as error:
            return DraftValidation(
                document=None,
                yaml=request.yaml,
                errors=[DraftError(f"YAML parse error: {error}")],
            )
        if not isinstance(document, dict):
            return DraftValidation(
                document=None,
                yaml=request.yaml,
                errors=[DraftError("The YAML must be a single mapping.")],
            )
    else:
        document = request.document or {}

    rendered = render_yaml(document)
    metadata = document.get("metadata")
    name = metadata.get("name") if isinstance(metadata, dict) else None
    project_path = (
        f"projects/demo/{name}.yaml"
        if isinstance(name, str) and DNS_LABEL_PATTERN.match(name)
        else None
    )

    draft_errors: list[DraftError] = []
    if document.get("kind") != DEMO_PROJECT_KIND:
        draft_errors.append(
            DraftError(f"kind must be {DEMO_PROJECT_KIND}.", pointer="/kind")
        )
    else:
        spec_fields = validator.spec_fields_for_kind(validators, DEMO_PROJECT_KIND)
        for error in validator.validate_candidate(
            document,
            existing_project_documents(validator),
            path=Path(project_path or "projects/demo/draft.yaml"),
            validators=validators,
        ):
            draft_errors.append(
                DraftError(
                    error.message,
                    pointer=validator.error_pointer(error, spec_fields),
                )
            )
        if project_path and not draft_errors:
            draft_errors.extend(secret_errors(document))

    return DraftValidation(
        document=document,
        yaml=rendered,
        errors=draft_errors,
        project_path=project_path,
        new_file_url=(
            new_file_url(project_path, rendered)
            if project_path and not draft_errors
            else None
        ),
    )


@activity.defn
def verify_onboarding_pull_request(
    request: VerifyPullRequestRequest,
) -> PullRequestCheck:
    number = parse_pull_request_url(request.pull_request_url)
    token = platform_github_token()
    if not token:
        raise errors.non_retryable_error(
            "The operator has no GitHub token to read pull requests with.",
            error_type=errors.CONFIGURATION_ERROR,
        )
    url = f"https://github.com/{CATALOG_REPO}/pull/{number}"
    pull = github_get(f"/repos/{CATALOG_REPO}/pulls/{number}", token)
    if pull is None:
        return PullRequestCheck(url=url, problems=[f"Pull request #{number} was not found."])

    problems: list[str] = []
    if pull.get("state") != "open":
        problems.append(f"Pull request #{number} is not open.")
    base_ref = (pull.get("base") or {}).get("ref")
    if base_ref != CATALOG_BRANCH:
        problems.append(f"Pull request #{number} targets {base_ref!r}, not {CATALOG_BRANCH!r}.")

    files = github_get(
        f"/repos/{CATALOG_REPO}/pulls/{number}/files?per_page=100", token
    )
    files = files if isinstance(files, list) else []
    added = [
        item
        for item in files
        if isinstance(item, dict) and item.get("filename") == request.project_path
    ]
    others = [
        str(item.get("filename"))
        for item in files
        if isinstance(item, dict) and item.get("filename") != request.project_path
    ]
    if not added:
        problems.append(f"The pull request does not add {request.project_path}.")
    elif added[0].get("status") != "added":
        problems.append(
            f"{request.project_path} already exists on {CATALOG_BRANCH}; "
            "onboarding only adds new projects."
        )
    else:
        content = pull_request_file_document(added[0], token)
        if content != request.document:
            problems.append(
                f"{request.project_path} in the pull request differs from the "
                "validated draft. Open the pull request from the onboarding page "
                "again without editing the file."
            )
    if others:
        problems.append(
            "The pull request also changes other files: " + ", ".join(sorted(others))
        )

    author = (pull.get("user") or {}).get("login")
    return PullRequestCheck(
        url=url,
        problems=problems,
        number=number,
        author=author if isinstance(author, str) else None,
    )


def secret_errors(document: dict[str, Any]) -> list[DraftError]:
    """A declared secret that isn't stored would block the first deploy, so
    report it now. Uses the same check the operator runs before deploying."""

    names = list(resources.project_secret_definitions(document))
    if not names:
        return []
    import boto3

    secretsmanager = boto3.session.Session(
        region_name=WorkerConfig.from_env().aws_region
    ).client("secretsmanager")
    problems = []
    for name in names:
        try:
            path = resources.user_secret_remote_path(document, name)
        except ValueError:
            continue  # the validator already reported the bad shared path
        status = secret_status(
            secretsmanager,
            SecretRequirement(
                path=path,
                required=True,
                managed_by=resources.user_secret_managed_by(document, name),
                required_properties=tuple(
                    resources.user_secret_required_properties(document, name)
                ),
            ),
        )
        if status.get("phase") == "MissingValue":
            message = (
                f"Secret {name} isn't stored yet. Add its value in the Secrets section."
                if status.get("managedBy") != "platform"
                else f"Shared secret {path} doesn't exist."
            )
        elif status.get("phase") == "MissingProperties":
            message = f"Secret {name} is missing {', '.join(status.get('missingProperties') or [])}."
        else:
            continue
        problems.append(DraftError(message, pointer=f"/spec/secrets/{name}"))
    return problems


def parse_repo_url(repo_url: str) -> tuple[str, str]:
    parsed = urlparse(repo_url.strip())
    parts = [part for part in parsed.path.split("/") if part]
    if (
        parsed.scheme != "https"
        or parsed.netloc.lower() not in {"github.com", "www.github.com"}
        or len(parts) != 2
        or parsed.query
        or parsed.fragment
    ):
        raise errors.non_retryable_error(
            "Enter a GitHub repo URL like https://github.com/temporal-sa/my-demo."
        )
    owner, name = parts
    name = name.removesuffix(".git")
    if owner.lower() not in ALLOWED_REPO_OWNERS or not REPO_NAME_PATTERN.match(name):
        raise errors.non_retryable_error(
            "The repo must be in temporal-sa, temporalio, or temporal-community."
        )
    return owner, name


def parse_pull_request_url(pull_request_url: str) -> int:
    parsed = urlparse(pull_request_url.strip())
    match = PULL_REQUEST_PATH.match(parsed.path)
    if (
        parsed.scheme != "https"
        or parsed.netloc.lower() != "github.com"
        or not match
        or f"{match.group(1)}/{match.group(2)}".lower() != CATALOG_REPO
    ):
        raise errors.non_retryable_error(
            f"Paste the link to your pull request on github.com/{CATALOG_REPO}."
        )
    return int(match.group(3))


def render_yaml(document: dict[str, Any]) -> str:
    """YAML in the key order people write project files in.

    Temporal's JSON converter sorts dict keys, so the order a draft arrives in
    means nothing; order by the schema's properties instead.
    """

    validator = project_validation.validator_module()
    schema = validator.load_schema(validator.DEFAULT_DEMO_PROJECT_SCHEMA)
    ordered = ordered_like_schema(document, schema, schema)
    spec = ordered.get("spec")
    if isinstance(spec, dict):
        ordered["spec"] = {
            key: spec[key]
            for key in sorted(
                spec,
                key=lambda key: (
                    SPEC_KEY_ORDER.index(key) if key in SPEC_KEY_ORDER else len(SPEC_KEY_ORDER)
                ),
            )
        }
    return yaml.dump(
        ordered,
        Dumper=IndentedListDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=100,
    )


def ordered_like_schema(value: Any, schema: Any, root: dict[str, Any]) -> Any:
    while isinstance(schema, dict) and isinstance(schema.get("$ref"), str):
        schema = root.get("$defs", {}).get(schema["$ref"].removeprefix("#/$defs/"), {})
    schema = schema if isinstance(schema, dict) else {}
    if isinstance(value, list):
        return [ordered_like_schema(item, schema.get("items"), root) for item in value]
    if not isinstance(value, dict):
        return value
    properties = schema.get("properties") or {}
    keys = [key for key in properties if key in value] + sorted(
        key for key in value if key not in properties
    )
    return {
        key: ordered_like_schema(
            value[key],
            properties.get(key, schema.get("additionalProperties")),
            root,
        )
        for key in keys
    }


class IndentedListDumper(yaml.SafeDumper):
    """Indents list items under their key, like the files in projects/."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        return super().increase_indent(flow, False)


def new_file_url(project_path: str, content: str) -> str:
    return (
        f"https://github.com/{CATALOG_REPO}/new/{CATALOG_BRANCH}"
        f"?filename={quote(project_path)}&value={quote(content, safe='')}"
    )


def existing_project_documents(validator: Any) -> list[Any]:
    """Every project in the cluster, as the validator's ProjectDocument.

    This is what Flux applied from main, which is what the draft would be merged
    next to. Operator canaries are skipped: they come and go and never merge.
    """

    documents = []
    for kind, items in (
        ("DemoProject", kube.list_demo_projects()),
        ("InstruqtCourse", kube.list_instruqt_courses()),
    ):
        for item in items:
            metadata = item.get("metadata") or {}
            labels = metadata.get("labels") or {}
            name = metadata.get("name")
            if not isinstance(name, str) or labels.get(resources.CANARY_LABEL) == "true":
                continue
            documents.append(
                validator.ProjectDocument(
                    path=Path(f"cluster/{kind}/{name}.yaml"),
                    data={
                        "apiVersion": item.get("apiVersion"),
                        "kind": kind,
                        "metadata": {"name": name},
                        "spec": item.get("spec") or {},
                    },
                )
            )
    return documents


def pull_request_file_document(file_entry: dict[str, Any], token: str) -> Any:
    # PR head blobs, fork or not, are readable from the base repo.
    sha = file_entry.get("sha")
    blob = github_get(f"/repos/{CATALOG_REPO}/git/blobs/{sha}", token) if sha else None
    if not isinstance(blob, dict) or blob.get("encoding") != "base64":
        return None
    try:
        return yaml.safe_load(base64.b64decode(blob.get("content") or ""))
    except (ValueError, yaml.YAMLError):
        return None


def platform_github_token() -> str | None:
    """The shared read-only GitHub token, read the same way the canary does."""

    config = WorkerConfig.from_env()
    try:
        data = kube.read_secret_data(
            config.source_watch_namespace, resources.PLATFORM_SOURCE_AUTH_SECRET_NAME
        )
    except Exception:  # noqa: BLE001 - fall back to anonymous GitHub reads
        return None
    for key in ("password", "token"):
        value = (data or {}).get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def github_get(path: str, token: str | None) -> Any:
    """GET from the GitHub API. Returns None for 404."""

    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "tmprl-demo-cloud-registry-operator",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        with urlopen(
            Request(GITHUB_API + path, headers=headers), timeout=GITHUB_TIMEOUT_SECONDS
        ) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        if error.code == 404:
            return None
        if error.code in {401, 403} and error.headers.get("X-RateLimit-Remaining") != "0":
            raise errors.non_retryable_error(
                f"GitHub refused {path} with HTTP {error.code}; check the "
                "operator's GitHub token.",
                error_type=errors.CONFIGURATION_ERROR,
            ) from error
        raise RuntimeError(f"GitHub {path} failed with HTTP {error.code}.") from error
    except (OSError, URLError) as error:
        raise RuntimeError(f"GitHub {path} failed: {error}") from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise RuntimeError(f"GitHub {path} returned invalid JSON.") from error
