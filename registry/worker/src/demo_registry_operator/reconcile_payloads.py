from __future__ import annotations

import copy
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.kube import (
    apply_resources,
    delete_external_secret,
    delete_git_repository,
    read_git_repository,
    read_secret_data,
)
from demo_registry_operator.resources import (
    CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    CONTENT_SOURCE_COMMIT_ANNOTATION,
    CONTENT_SOURCE_REVISION_ANNOTATION,
    INSTRUQT_TESTING_CONTENT_REPOSITORY_KEY,
    INSTRUQT_TESTING_SOURCE_REPOSITORY_KEY,
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
    SOURCE_REVISION_ANNOTATION,
    TESTING_BRANCH_MESSAGE_ANNOTATION,
    TESTING_BRANCH_REASON_ANNOTATION,
    TESTING_BRANCH_STATE_ANNOTATION,
    TESTING_CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    TESTING_CONTENT_SOURCE_COMMIT_ANNOTATION,
    TESTING_CONTENT_SOURCE_REVISION_ANNOTATION,
    TESTING_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    TESTING_SOURCE_COMMIT_ANNOTATION,
    TESTING_SOURCE_REVISION_ANNOTATION,
    content_source_config,
    instruqt_course_content_watch_required,
    instruqt_course_source_watch_required,
    instruqt_course_testing_content_branch,
    instruqt_course_testing_content_watch_required,
    instruqt_course_testing_enabled,
    instruqt_course_testing_source_branch,
    metadata_annotations,
    project_name,
    source_auth_kubernetes_secret_name,
    source_git_repository_name,
    source_git_repository_secret_ref,
    source_git_repository_url,
    source_watch_required,
    source_watch_resources,
    spec_of,
)


def demo_project_reconcile_payload_state(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    source_watch_fn: Any = None,
) -> dict[str, Any]:
    source_watch = source_watch_fn or reconcile_source_watch
    source_state = None
    if source_watch_required(demo_project):
        source_state = source_watch(demo_project, config)
        if not source_state.get("sourceCommitSha") or not source_state.get(
            "sourceArtifactDigest"
        ):
            return {
                "ready": False,
                "reason": source_state.get("reason", "SourceArtifactUnavailable"),
                "message": source_state.get(
                    "message",
                    "Source artifact is not resolved.",
                ),
                "sourceState": source_state,
            }

    return {
        "ready": True,
        "payload": workflow_signal_payload(
            demo_project,
            source_commit_sha=(
                source_state.get("sourceCommitSha")
                if isinstance(source_state, dict)
                else None
            ),
            source_artifact_digest=(
                source_state.get("sourceArtifactDigest")
                if isinstance(source_state, dict)
                else None
            ),
            source_revision=(
                source_state.get("revision") if isinstance(source_state, dict) else None
            ),
        ),
        "sourceState": source_state,
    }


def instruqt_course_reconcile_payload_state(
    instruqt_course: dict[str, Any],
    config: WorkerConfig,
    *,
    source_watch_fn: Any = None,
    probe_github_branch_fn: Any = None,
    cleanup_testing_source_watches_fn: Any = None,
) -> dict[str, Any]:
    source_watch = source_watch_fn or reconcile_source_watch
    probe_branch = probe_github_branch_fn or probe_github_branch
    cleanup_testing_watches = (
        cleanup_testing_source_watches_fn or cleanup_instruqt_testing_source_watches
    )
    source_state = None
    if instruqt_course_source_watch_required(instruqt_course):
        source_state = source_watch(instruqt_course, config)
        if not source_state.get("sourceCommitSha") or not source_state.get(
            "sourceArtifactDigest"
        ):
            return {
                "ready": False,
                "reason": source_state.get("reason", "SourceArtifactUnavailable"),
                "message": source_state.get(
                    "message",
                    "Source artifact is not resolved.",
                ),
                "sourceState": source_state,
            }

    content_state = None
    if instruqt_course_content_watch_required(instruqt_course):
        content_state = source_watch(
            instruqt_course,
            config,
            source_key="content",
        )
        if not content_state.get("sourceCommitSha") or not content_state.get(
            "sourceArtifactDigest"
        ):
            return {
                "ready": False,
                "reason": content_state.get("reason", "ContentSourceUnavailable"),
                "message": content_state.get(
                    "message",
                    "Content source artifact is not resolved.",
                ),
                "contentState": content_state,
            }

    extra_annotations: dict[str, str] = {}
    testing_branch = instruqt_course_testing_source_branch(instruqt_course)
    if testing_branch:
        testing_branch_state = probe_branch(
            instruqt_course,
            config,
            source_key="source",
            branch=testing_branch,
        )
        extra_annotations.update(testing_branch_state_annotations(testing_branch_state))
        if testing_branch_state.get("state") == "exists":
            testing_source_state = source_watch(
                instruqt_course,
                config,
                source_key="source",
                branch=testing_branch,
                repository_key=INSTRUQT_TESTING_SOURCE_REPOSITORY_KEY,
            )
            extra_annotations.update(
                testing_source_state_annotations(testing_source_state)
            )

            if instruqt_course_testing_content_watch_required(instruqt_course):
                testing_content_branch = instruqt_course_testing_content_branch(
                    instruqt_course
                )
                testing_content_state = source_watch(
                    instruqt_course,
                    config,
                    source_key="content",
                    branch=testing_content_branch,
                    repository_key=INSTRUQT_TESTING_CONTENT_REPOSITORY_KEY,
                )
                extra_annotations.update(
                    testing_content_state_annotations(testing_content_state)
                )
        elif testing_branch_state.get("state") == "missing":
            cleanup_testing_watches(instruqt_course, config)
    elif testing_cleanup_required(instruqt_course):
        cleanup_testing_watches(instruqt_course, config)

    return {
        "ready": True,
        "payload": workflow_signal_payload(
            instruqt_course,
            source_commit_sha=(
                source_state.get("sourceCommitSha")
                if isinstance(source_state, dict)
                else None
            ),
            source_artifact_digest=(
                source_state.get("sourceArtifactDigest")
                if isinstance(source_state, dict)
                else None
            ),
            source_revision=(
                source_state.get("revision") if isinstance(source_state, dict) else None
            ),
            content_source_commit_sha=(
                content_state.get("sourceCommitSha")
                if isinstance(content_state, dict)
                else None
            ),
            content_source_artifact_digest=(
                content_state.get("sourceArtifactDigest")
                if isinstance(content_state, dict)
                else None
            ),
            content_source_revision=(
                content_state.get("revision")
                if isinstance(content_state, dict)
                else None
            ),
            extra_annotations=extra_annotations,
        ),
        "sourceState": source_state,
        "contentState": content_state,
    }


def reconcile_source_watch(
    demo_project: dict[str, Any],
    config: WorkerConfig,
    *,
    source_key: str = "source",
    branch: str | None = None,
    repository_key: str | None = None,
) -> dict[str, Any]:
    resources = source_watch_resources(
        demo_project,
        namespace=config.source_watch_namespace,
        interval=config.source_watch_interval,
        source_key=source_key,
        branch=branch,
        repository_key=repository_key,
    )
    apply_resources(resources, dry_run=False)

    git_repository_resource = resources[-1]
    metadata = git_repository_resource["metadata"]
    name = metadata["name"]
    namespace = metadata["namespace"]
    git_repository = read_git_repository(namespace, name)
    state = git_repository_source_state(git_repository)
    return {
        "gitRepository": name,
        "namespace": namespace,
        **state,
    }


def probe_github_branch(
    instruqt_course: dict[str, Any],
    config: WorkerConfig,
    *,
    source_key: str,
    branch: str,
    github_token_fn: Any = None,
    urlopen_fn: Any = None,
    repo_access_fn: Any = None,
) -> dict[str, str]:
    repo = github_repo_from_url(source_git_repository_url(instruqt_course, source_key))
    if repo is None:
        return {
            "state": "unknown",
            "reason": "UnsupportedRepository",
            "message": f"spec.{source_key}.repo is not a GitHub HTTPS repository.",
        }

    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "tmprl-demo-cloud-registry-operator",
    }
    secret_ref = source_git_repository_secret_ref(instruqt_course, source_key)
    token = (github_token_fn or github_token_from_secret_ref)(config, secret_ref)
    if secret_ref and not token:
        return {
            "state": "unknown",
            "reason": "GitHubAuthUnavailable",
            "message": (
                f"GitHub auth secret {secret_ref['name']!r} is not available; "
                "testing branch existence was not changed."
            ),
        }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    owner, name = repo
    url = (
        f"https://api.github.com/repos/{quote(owner, safe='')}/"
        f"{quote(name, safe='')}/branches/{quote(branch, safe='')}"
    )
    opener = urlopen_fn or urlopen
    try:
        with opener(Request(url, headers=headers), timeout=10) as response:
            status = getattr(response, "status", None)
            if isinstance(status, int) and 200 <= status < 300:
                return {
                    "state": "exists",
                    "reason": "BranchExists",
                    "message": f"Testing branch {branch!r} exists.",
                }
    except HTTPError as error:
        if error.code == 404:
            repo_access = (repo_access_fn or probe_github_repo_access)(
                owner,
                name,
                headers,
            )
            if repo_access != "exists":
                return {
                    "state": "unknown",
                    "reason": "GitHubRepoUnavailable",
                    "message": (
                        f"GitHub repository {owner}/{name} could not be confirmed; "
                        "testing branch existence was not changed."
                    ),
                }
            return {
                "state": "missing",
                "reason": "BranchMissing",
                "message": f"Testing branch {branch!r} does not exist.",
            }
        return {
            "state": "unknown",
            "reason": f"GitHubHTTP{error.code}",
            "message": f"GitHub branch probe failed with HTTP {error.code}.",
        }
    except (OSError, URLError) as error:
        return {
            "state": "unknown",
            "reason": "GitHubProbeFailed",
            "message": f"GitHub branch probe failed: {error}",
        }

    return {
        "state": "unknown",
        "reason": "GitHubProbeFailed",
        "message": "GitHub branch probe did not return a successful response.",
    }


def probe_github_repo_access(
    owner: str,
    name: str,
    headers: dict[str, str],
    *,
    urlopen_fn: Any = None,
) -> str:
    url = (
        f"https://api.github.com/repos/{quote(owner, safe='')}/"
        f"{quote(name, safe='')}"
    )
    opener = urlopen_fn or urlopen
    try:
        with opener(Request(url, headers=headers), timeout=10) as response:
            status = getattr(response, "status", None)
            if isinstance(status, int) and 200 <= status < 300:
                return "exists"
    except (HTTPError, OSError, URLError):
        return "unknown"
    return "unknown"


def github_repo_from_url(url: str | None) -> tuple[str, str] | None:
    if not isinstance(url, str) or not url.strip():
        return None
    parsed = urlparse(url.strip())
    if parsed.scheme != "https" or parsed.netloc.lower() != "github.com":
        return None
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) < 2:
        return None
    repo = parts[1]
    if repo.endswith(".git"):
        repo = repo[:-4]
    return parts[0], repo


def github_token_from_secret_ref(
    config: WorkerConfig,
    secret_ref: dict[str, str] | None,
) -> str | None:
    if not isinstance(secret_ref, dict):
        return None
    name = secret_ref.get("name")
    if not isinstance(name, str) or not name:
        return None
    try:
        data = read_secret_data(config.source_watch_namespace, name)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    for key in ("password", "token", "githubToken", "GITHUB_TOKEN"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def testing_branch_state_annotations(state: dict[str, str]) -> dict[str, str]:
    annotations = {}
    value = state.get("state")
    reason = state.get("reason")
    message = state.get("message")
    if isinstance(value, str) and value:
        annotations[TESTING_BRANCH_STATE_ANNOTATION] = value
    if isinstance(reason, str) and reason:
        annotations[TESTING_BRANCH_REASON_ANNOTATION] = reason
    if isinstance(message, str) and message:
        annotations[TESTING_BRANCH_MESSAGE_ANNOTATION] = message
    return annotations


def testing_source_state_annotations(state: dict[str, Any]) -> dict[str, str]:
    return {
        key: value
        for key, value in {
            TESTING_SOURCE_COMMIT_ANNOTATION: state.get("sourceCommitSha"),
            TESTING_SOURCE_ARTIFACT_DIGEST_ANNOTATION: state.get(
                "sourceArtifactDigest"
            ),
            TESTING_SOURCE_REVISION_ANNOTATION: state.get("revision"),
        }.items()
        if isinstance(value, str) and value
    }


def testing_content_state_annotations(state: dict[str, Any]) -> dict[str, str]:
    return {
        key: value
        for key, value in {
            TESTING_CONTENT_SOURCE_COMMIT_ANNOTATION: state.get("sourceCommitSha"),
            TESTING_CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION: state.get(
                "sourceArtifactDigest"
            ),
            TESTING_CONTENT_SOURCE_REVISION_ANNOTATION: state.get("revision"),
        }.items()
        if isinstance(value, str) and value
    }


def cleanup_instruqt_testing_source_watches(
    instruqt_course: dict[str, Any],
    config: WorkerConfig,
) -> dict[str, str]:
    namespace = config.source_watch_namespace
    actions = {
        "testingSourceRepository": delete_git_repository(
            namespace,
            source_git_repository_name(
                instruqt_course,
                "source",
                repository_key=INSTRUQT_TESTING_SOURCE_REPOSITORY_KEY,
            ),
            dry_run=False,
        )
    }
    testing_source_auth_secret = source_auth_kubernetes_secret_name(
        instruqt_course,
        "source",
        repository_key=INSTRUQT_TESTING_SOURCE_REPOSITORY_KEY,
    )
    if testing_source_auth_secret:
        actions["testingSourceAuthSecret"] = delete_external_secret(
            namespace,
            testing_source_auth_secret,
            dry_run=False,
        )

    if content_source_config(instruqt_course):
        actions["testingContentRepository"] = delete_git_repository(
            namespace,
            source_git_repository_name(
                instruqt_course,
                "content",
                repository_key=INSTRUQT_TESTING_CONTENT_REPOSITORY_KEY,
            ),
            dry_run=False,
        )
        testing_content_auth_secret = source_auth_kubernetes_secret_name(
            instruqt_course,
            "content",
            repository_key=INSTRUQT_TESTING_CONTENT_REPOSITORY_KEY,
        )
        if testing_content_auth_secret:
            actions["testingContentAuthSecret"] = delete_external_secret(
                namespace,
                testing_content_auth_secret,
                dry_run=False,
            )
    return actions


def testing_cleanup_required(instruqt_course: dict[str, Any]) -> bool:
    if instruqt_course_testing_enabled(instruqt_course):
        return False
    status = instruqt_course.get("status")
    status = status if isinstance(status, dict) else {}
    testing = status.get("testing")
    testing = testing if isinstance(testing, dict) else {}
    phase = testing.get("phase")
    return isinstance(phase, str) and phase not in {"Disabled", ""}


def git_repository_source_state(
    git_repository: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(git_repository, dict):
        return {
            "phase": "Waiting",
            "reason": "GitRepositoryNotFound",
            "message": "Flux GitRepository has not been created yet.",
        }

    metadata = git_repository.get("metadata", {})
    status = git_repository.get("status", {})
    generation = metadata.get("generation") if isinstance(metadata, dict) else None
    observed_generation = (
        status.get("observedGeneration") if isinstance(status, dict) else None
    )
    if generation and observed_generation != generation:
        return {
            "phase": "Waiting",
            "reason": "GitRepositoryNotObserved",
            "message": "Flux has not observed the current GitRepository spec.",
        }

    ready = ready_condition(status)
    if ready and ready.get("status") != "True":
        return {
            "phase": "Waiting",
            "reason": ready.get("reason") or "GitRepositoryNotReady",
            "message": ready.get("message") or "Flux GitRepository is not Ready.",
        }

    artifact = status.get("artifact") if isinstance(status, dict) else None
    revision = artifact.get("revision") if isinstance(artifact, dict) else None
    commit_sha = source_commit_from_revision(revision)
    if not commit_sha:
        return {
            "phase": "Waiting",
            "reason": "RevisionUnavailable",
            "message": "Flux has not reported a git commit revision yet.",
            **({"revision": revision} if isinstance(revision, str) else {}),
        }
    digest = artifact.get("digest") if isinstance(artifact, dict) else None
    if not isinstance(digest, str) or not digest.strip():
        return {
            "phase": "Waiting",
            "reason": "ArtifactDigestUnavailable",
            "message": "Flux has not reported a source artifact digest yet.",
            "revision": revision,
        }

    return {
        "phase": "Resolved",
        "reason": "RevisionResolved",
        "revision": revision,
        "sourceCommitSha": commit_sha,
        "sourceArtifactDigest": digest.strip(),
    }


def ready_condition(status: Any) -> dict[str, Any] | None:
    conditions = status.get("conditions") if isinstance(status, dict) else None
    if not isinstance(conditions, list):
        return None
    for condition in conditions:
        if isinstance(condition, dict) and condition.get("type") == "Ready":
            return condition
    return None


def source_commit_from_revision(revision: Any) -> str | None:
    if not isinstance(revision, str):
        return None
    marker = "@sha1:"
    if marker not in revision:
        return None
    commit_sha = revision.rsplit(marker, 1)[1].strip()
    if 7 <= len(commit_sha) <= 40 and all(
        character in "0123456789abcdefABCDEF" for character in commit_sha
    ):
        return commit_sha
    return None


ACTIVE_VERSION_STATUS_KEYS = (
    "activeImageVersion",
    "activeImages",
    "activeRuntimeNamespace",
    "activeRevision",
    "activeRouteServices",
)


def active_version_status(demo_project: dict[str, Any]) -> dict[str, Any]:
    status = demo_project.get("status") if isinstance(demo_project, dict) else None
    if not isinstance(status, dict):
        return {}
    return {
        key: copy.deepcopy(status[key])
        for key in ACTIVE_VERSION_STATUS_KEYS
        if status.get(key) is not None
    }


def workflow_signal_payload(
    demo_project: dict[str, Any],
    *,
    source_commit_sha: str | None = None,
    source_artifact_digest: str | None = None,
    source_revision: str | None = None,
    content_source_commit_sha: str | None = None,
    content_source_artifact_digest: str | None = None,
    content_source_revision: str | None = None,
    extra_annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    metadata = demo_project.get("metadata", {})
    payload_metadata: dict[str, Any] = {"name": project_name(demo_project)}
    if isinstance(metadata, dict):
        generation = metadata.get("generation")
        if isinstance(generation, int):
            payload_metadata["generation"] = generation
        deletion_timestamp = metadata.get("deletionTimestamp")
        if isinstance(deletion_timestamp, str) and deletion_timestamp:
            payload_metadata["deletionTimestamp"] = deletion_timestamp

    annotations = metadata_annotations(demo_project)
    if source_commit_sha:
        annotations[SOURCE_COMMIT_ANNOTATION] = source_commit_sha
    if source_artifact_digest:
        annotations[SOURCE_ARTIFACT_DIGEST_ANNOTATION] = source_artifact_digest
    if source_revision:
        annotations[SOURCE_REVISION_ANNOTATION] = source_revision
    if content_source_commit_sha:
        annotations[CONTENT_SOURCE_COMMIT_ANNOTATION] = content_source_commit_sha
    if content_source_artifact_digest:
        annotations[CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION] = (
            content_source_artifact_digest
        )
    if content_source_revision:
        annotations[CONTENT_SOURCE_REVISION_ANNOTATION] = content_source_revision
    if extra_annotations:
        annotations.update(
            {
                key: value
                for key, value in extra_annotations.items()
                if isinstance(key, str)
                and isinstance(value, str)
                and key
                and value
            }
        )
    if annotations:
        payload_metadata["annotations"] = annotations

    payload: dict[str, Any] = {
        "apiVersion": demo_project.get(
            "apiVersion",
            "registry.tmprl-demo.cloud/v1alpha1",
        ),
        "kind": demo_project.get("kind", "DemoProject"),
        "metadata": payload_metadata,
        "spec": copy.deepcopy(spec_of(demo_project)),
    }
    active_status = active_version_status(demo_project)
    if active_status:
        payload["status"] = active_status
    return payload
