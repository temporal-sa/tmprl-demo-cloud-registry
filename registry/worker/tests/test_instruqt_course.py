from __future__ import annotations

import asyncio
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.error import HTTPError

import pytest
import yaml

import demo_registry_operator.activities.instruqt as instruqt_activities
import demo_registry_operator.watch as watch
import demo_registry_operator.workflows.instruqt_course as workflow_module
from demo_registry_operator.activities.instruqt import (
    InstruqtCourseArtifactRequest,
    InstruqtCourseImageBuildPlanRequest,
    instruqt_codebuild_buildspec,
    instruqt_cli_status,
    prepare_instruqt_course_artifact,
    reconcile_instruqt_course,
    render_managed_instruqt_config,
    render_instruqt_track_directory,
    resolve_instruqt_course_image_build_plan,
)
from demo_registry_operator.resources import (
    CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    CONTENT_SOURCE_COMMIT_ANNOTATION,
    CONTENT_SOURCE_REVISION_ANNOTATION,
    FORCE_RECONCILE_ANNOTATION,
    INSTRUQT_TESTING_SOURCE_REPOSITORY_KEY,
    PLATFORM_SOURCE_AUTH_SECRET_NAME,
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
    SOURCE_REVISION_ANNOTATION,
    TESTING_BRANCH_MESSAGE_ANNOTATION,
    TESTING_BRANCH_REASON_ANNOTATION,
    TESTING_BRANCH_STATE_ANNOTATION,
    TESTING_SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    TESTING_SOURCE_COMMIT_ANNOTATION,
    TESTING_SOURCE_REVISION_ANNOTATION,
    instruqt_course_reconcile_key,
)
from demo_registry_operator.workflows.instruqt_course import (
    InstruqtCourseWorkflow,
    remember_completed_reconcile_identity,
    reconcile_identity,
    status_with_deployment_status,
)
from demo_registry_operator.workflows.messages import (
    ForceReconcileSignal,
    InstruqtCourseReconcileSignal,
    InstruqtCourseRunRequest,
)


def course(
    *,
    generation: int = 2,
    annotations: dict[str, str] | None = None,
    content: bool = False,
    images: dict[str, Any] | None = None,
    testing: bool = False,
) -> dict[str, Any]:
    if annotations is None:
        annotations = {
            SOURCE_COMMIT_ANNOTATION: (
                "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
            ),
            SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:aaaabbbb",
            SOURCE_REVISION_ANNOTATION: (
                "main@sha1:e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
            ),
        }
        if content:
            annotations.update(
                {
                    CONTENT_SOURCE_COMMIT_ANNOTATION: (
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    CONTENT_SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:ccccdddd",
                    CONTENT_SOURCE_REVISION_ANNOTATION: (
                        "main@sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                }
            )
    spec: dict[str, Any] = {
        "source": {
            "repo": "https://github.com/temporal-sa/workflow-fundamentals",
            "branch": "main",
            "path": "tracks/workflow-fundamentals",
        },
        "track": {
            "organization": "temporal",
            "slug": "workflow-fundamentals",
        },
    }
    if testing:
        spec["source"]["testingBranch"] = "course-update"
    if content:
        spec["content"] = {
            "repo": "https://github.com/temporal-sa/workshop-code",
            "branch": "main",
            "path": ".",
        }
    if images is not None:
        spec["images"] = images
    return {
        "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
        "kind": "InstruqtCourse",
        "metadata": {
            "name": "workflow-fundamentals",
            "generation": generation,
            "annotations": annotations,
        },
        "spec": spec,
    }


def workflow_instance(
    name: str = "workflow-fundamentals",
) -> InstruqtCourseWorkflow:
    return InstruqtCourseWorkflow(InstruqtCourseRunRequest(course_name=name))


def reconcile_signal(instruqt_course: dict[str, Any]) -> InstruqtCourseReconcileSignal:
    return InstruqtCourseReconcileSignal(instruqtCourse=instruqt_course)


def test_reconcile_instruqt_course_records_validated_source_status():
    status = reconcile_instruqt_course(course())

    assert status["phase"] == "Validated"
    assert status["observedGeneration"] == 2
    assert status["reconcileKey"] == instruqt_course_reconcile_key(course())
    assert status["track"] == {
        "organization": "temporal",
        "slug": "workflow-fundamentals",
    }
    assert status["source"] == {
        "repo": "https://github.com/temporal-sa/workflow-fundamentals",
        "branch": "main",
        "path": "tracks/workflow-fundamentals",
        "revision": "main@sha1:e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        "commitSha": "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        "artifactDigest": "sha256:aaaabbbb",
    }
    assert any(
        condition["type"] == "Reconciled" and condition["status"] == "True"
        for condition in status["conditions"]
    )


def test_reconcile_instruqt_course_records_content_and_image_bindings():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "content", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }

    status = reconcile_instruqt_course(course(content=True, images=images))

    assert status["phase"] == "Validated"
    assert status["content"] == {
        "repo": "https://github.com/temporal-sa/workshop-code",
        "branch": "main",
        "path": ".",
        "revision": "main@sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "commitSha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "artifactDigest": "sha256:ccccdddd",
    }
    assert status["images"] == images
    assert any(
        condition["type"] == "ContentResolved" and condition["status"] == "True"
        for condition in status["conditions"]
    )


def test_reconcile_instruqt_course_records_testing_branch_status():
    annotations = {
        SOURCE_COMMIT_ANNOTATION: "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:aaaabbbb",
        SOURCE_REVISION_ANNOTATION: "main@sha1:e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        TESTING_BRANCH_STATE_ANNOTATION: "exists",
        TESTING_BRANCH_REASON_ANNOTATION: "BranchExists",
        TESTING_BRANCH_MESSAGE_ANNOTATION: "Testing branch 'course-update' exists.",
        TESTING_SOURCE_COMMIT_ANNOTATION: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        TESTING_SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:testing",
        TESTING_SOURCE_REVISION_ANNOTATION: (
            "course-update@sha1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ),
    }

    status = reconcile_instruqt_course(
        course(annotations=annotations, testing=True)
    )

    assert status["phase"] == "Validated"
    assert status["testing"] == {
        "branch": "course-update",
        "track": {
            "titlePrefix": "[testing] ",
            "organization": "temporal",
            "slug": "testing-workflow-fundamentals",
        },
        "source": {
            "repo": "https://github.com/temporal-sa/workflow-fundamentals",
            "branch": "course-update",
            "path": "tracks/workflow-fundamentals",
            "revision": (
                "course-update@sha1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            "commitSha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "artifactDigest": "sha256:testing",
        },
        "state": "exists",
        "phase": "Validated",
        "reason": "TestingSourceResolved",
        "message": "Testing branch source artifacts are resolved.",
        "resolved": True,
    }
    assert any(
        condition["type"] == "TestingResolved" and condition["status"] == "True"
        for condition in status["conditions"]
    )


def test_reconcile_instruqt_course_records_missing_testing_branch():
    annotations = {
        SOURCE_COMMIT_ANNOTATION: "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        SOURCE_ARTIFACT_DIGEST_ANNOTATION: "sha256:aaaabbbb",
        SOURCE_REVISION_ANNOTATION: "main@sha1:e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
        TESTING_BRANCH_STATE_ANNOTATION: "missing",
        TESTING_BRANCH_REASON_ANNOTATION: "BranchMissing",
        TESTING_BRANCH_MESSAGE_ANNOTATION: "Testing branch 'course-update' is absent.",
    }

    status = reconcile_instruqt_course(
        course(annotations=annotations, testing=True)
    )

    assert status["phase"] == "Validated"
    assert status["testing"]["phase"] == "BranchMissing"
    assert status["testing"]["reason"] == "BranchMissing"
    assert status["testing"]["resolved"] is False
    assert any(
        condition["type"] == "TestingResolved" and condition["status"] == "False"
        for condition in status["conditions"]
    )


def test_instruqt_cli_status_reports_missing_token(monkeypatch):
    monkeypatch.setattr(instruqt_activities.shutil, "which", lambda name: "/usr/bin/instruqt")
    monkeypatch.delenv("INSTRUQT_TOKEN", raising=False)

    status = instruqt_cli_status()

    assert status["phase"] == "Unavailable"
    assert status["reason"] == "InstruqtTokenMissing"
    assert status["ready"] is False


def test_instruqt_cli_status_reports_ready_without_home(monkeypatch, tmp_path):
    monkeypatch.setattr(instruqt_activities.shutil, "which", lambda name: "/usr/bin/instruqt")
    monkeypatch.setenv("INSTRUQT_TOKEN", "token")
    monkeypatch.setenv("INSTRUQT_CLI_CACHE_DIR", str(tmp_path))
    monkeypatch.delenv("HOME", raising=False)
    observed = {}

    def run(*args, **kwargs):
        home = Path(kwargs["env"]["HOME"])
        assert home.is_dir()
        observed["home"] = home
        return SimpleNamespace(returncode=0, stdout="instruqt version test\n", stderr="")

    monkeypatch.setattr(instruqt_activities.subprocess, "run", run)

    status = instruqt_cli_status()

    assert status["phase"] == "Ready"
    assert status["reason"] == "InstruqtCLIReady"
    assert status["versionOutput"] == "instruqt version test"
    assert status["ready"] is True
    assert not observed["home"].exists()


UPDATE_REQUIRED_STDERR = (
    "An update of the CLI is required. To update, run the CLI in an interactive "
    "terminal, or run `instruqt update`.\n"
)


def fake_instruqt_cli(monkeypatch, tmp_path, *, update_returncode=0):
    image = tmp_path / "image" / "instruqt"
    image.parent.mkdir()
    image.write_text("pinned")
    cache = tmp_path / "cache"
    monkeypatch.setattr(instruqt_activities.shutil, "which", lambda name: str(image))
    monkeypatch.setenv("INSTRUQT_TOKEN", "token")
    monkeypatch.setenv("INSTRUQT_CLI_CACHE_DIR", str(cache))
    calls = []

    def run(command, **kwargs):
        executable, subcommand = Path(command[0]), command[1]
        calls.append((executable, subcommand))
        if subcommand == "update":
            if update_returncode == 0:
                executable.write_text("updated")
            return SimpleNamespace(
                returncode=update_returncode, stdout="", stderr="download failed\n"
            )
        if executable.read_text() == "pinned":
            return SimpleNamespace(
                returncode=1, stdout="Version: 2380-865ec91\n", stderr=UPDATE_REQUIRED_STDERR
            )
        return SimpleNamespace(returncode=0, stdout="Version: 2427-422d0f4\n", stderr="")

    monkeypatch.setattr(instruqt_activities.subprocess, "run", run)
    return image, cache / "instruqt", calls


def test_instruqt_cli_status_self_updates_when_update_required(monkeypatch, tmp_path):
    image, cached, calls = fake_instruqt_cli(monkeypatch, tmp_path)

    status = instruqt_cli_status()

    assert status["phase"] == "Ready"
    assert status["executable"] == str(cached)
    assert status["versionOutput"] == "Version: 2427-422d0f4"
    assert cached.read_text() == "updated"
    assert image.read_text() == "pinned"
    assert [subcommand for _, subcommand in calls] == ["version", "update", "version"]
    assert list(cached.parent.iterdir()) == [cached]

    calls.clear()
    assert instruqt_cli_status()["executable"] == str(cached)
    assert calls == [(cached, "version")]


def test_instruqt_cli_status_reports_failed_self_update_as_retryable(monkeypatch, tmp_path):
    image, cached, _ = fake_instruqt_cli(monkeypatch, tmp_path, update_returncode=1)

    status = instruqt_cli_status()

    assert status["ready"] is False
    assert status["reason"] == "InstruqtCLIUpdateFailed"
    assert status["message"] == "instruqt update exited with 1."
    assert status["versionOutput"] == "download failed"
    assert status["executable"] == str(image)
    assert status["retryable"] is True
    assert not cached.exists()


def test_instruqt_cli_status_reports_stdout_and_stderr(monkeypatch, tmp_path):
    monkeypatch.setattr(instruqt_activities.shutil, "which", lambda name: "/usr/bin/instruqt")
    monkeypatch.setenv("INSTRUQT_TOKEN", "token")
    monkeypatch.setenv("INSTRUQT_CLI_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(
        instruqt_activities.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="Version: 2380-865ec91\n", stderr="token is invalid\n"
        ),
    )

    status = instruqt_cli_status()

    assert status["reason"] == "InstruqtVersionFailed"
    assert status["versionOutput"] == "Version: 2380-865ec91 token is invalid"
    assert status["retryable"] is False


def test_instruqt_track_validate_supplies_writable_home(monkeypatch, tmp_path):
    monkeypatch.setattr(
        instruqt_activities,
        "instruqt_cli_status",
        lambda: {"ready": True, "executable": "/usr/bin/instruqt"},
    )
    monkeypatch.delenv("HOME", raising=False)
    observed = {}

    def run(*args, **kwargs):
        home = Path(kwargs["env"]["HOME"])
        assert home.is_dir()
        observed["home"] = home
        return SimpleNamespace(returncode=0, stdout="track is valid\n", stderr="")

    monkeypatch.setattr(instruqt_activities.subprocess, "run", run)

    status = instruqt_activities.run_instruqt_track_validate(tmp_path)

    assert status["phase"] == "Validated"
    assert status["output"] == "track is valid"
    assert not observed["home"].exists()


def test_reconcile_instruqt_course_rejects_unresolved_source():
    status = reconcile_instruqt_course(course(annotations={}))

    assert status["phase"] == "Error"
    assert "missing registry.tmprl-demo.cloud/source-commit-sha" in status["message"]
    assert status["lastError"]["reason"] == "ValidationFailed"


def test_reconcile_instruqt_course_requires_content_source_when_images_reference_it():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "content", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }

    status = reconcile_instruqt_course(course(images=images))

    assert status["phase"] == "Error"
    assert "references content but spec.content is not set" in status["message"]


def test_render_managed_instruqt_config_injects_missing_image_key():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "source", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }
    config = """
version: "3"
containers:
- name: workstation
  shell: /bin/bash
  ports:
  - 8233
"""

    rendered = render_managed_instruqt_config(
        course(images=images),
        config,
        {"workstation": "123456789012.dkr.ecr.us-west-1.amazonaws.com/course/workstation:git-abc123"},
    )

    decoded = yaml.safe_load(rendered["config"])
    assert decoded["containers"][0]["image"] == (
        "123456789012.dkr.ecr.us-west-1.amazonaws.com/course/workstation:git-abc123"
    )
    assert rendered["managedImages"]["workstation"]["action"] == "injected"


def test_render_managed_instruqt_config_overrides_existing_image_key():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "source", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }
    config = """
version: "3"
containers:
- name: workstation
  image: mjones2219/temporal-python-workshop:v13
  shell: /bin/bash
"""

    rendered = render_managed_instruqt_config(
        course(images=images),
        config,
        {"workstation": "123456789012.dkr.ecr.us-west-1.amazonaws.com/course/workstation:git-abc123"},
    )

    decoded = yaml.safe_load(rendered["config"])
    assert decoded["containers"][0]["image"] == (
        "123456789012.dkr.ecr.us-west-1.amazonaws.com/course/workstation:git-abc123"
    )
    assert rendered["managedImages"]["workstation"] == {
        "container": "workstation",
        "image": "123456789012.dkr.ecr.us-west-1.amazonaws.com/course/workstation:git-abc123",
        "action": "overridden",
        "previousImage": "mjones2219/temporal-python-workshop:v13",
    }


def test_render_managed_instruqt_config_skips_vm_lifecycle_config():
    config = """
version: "3"
virtualmachines:
- name: host
  image: ubuntu-os-cloud/ubuntu-2204-lts
"""

    rendered = render_managed_instruqt_config(course(), config, {})

    assert rendered["phase"] == "Skipped"
    assert rendered["managedImages"] == {}
    assert yaml.safe_load(rendered["config"])["virtualmachines"][0]["name"] == "host"


def test_render_managed_instruqt_config_rejects_unmanaged_container_image():
    config = """
version: "3"
containers:
- name: workstation
  image: mjones2219/temporal-python-workshop:v13
"""

    with pytest.raises(ValueError, match="unmanaged image"):
        render_managed_instruqt_config(course(), config, {})


def test_render_managed_instruqt_config_requires_resolved_image():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "source", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }
    config = """
version: "3"
containers:
- name: workstation
"""

    with pytest.raises(ValueError, match="has no resolved image"):
        render_managed_instruqt_config(course(images=images), config, {})


def test_prepare_instruqt_course_artifact_waits_for_managed_images():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "source", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }

    status = prepare_instruqt_course_artifact(
        InstruqtCourseArtifactRequest(
            instruqt_course=course(images=images),
            images={},
        )
    ).status

    assert status == {
        "phase": "WaitingForImages",
        "message": "Managed image bindings are configured but no resolved images were provided.",
        "missingImages": ["workstation"],
    }


def test_render_instruqt_track_directory_overrides_existing_image(tmp_path):
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "source", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }
    source_root = tmp_path / "source"
    track_dir = source_root / "tracks" / "workflow-fundamentals"
    rendered_track = tmp_path / "rendered"
    track_dir.mkdir(parents=True)
    (track_dir / "track.yml").write_text(
        """
slug: old-slug
id: existing-track-id
title: Workflow Fundamentals
owner: old-owner
""",
        encoding="utf-8",
    )
    (track_dir / "config.yml").write_text(
        """
version: "3"
containers:
- name: workstation
  image: mjones2219/temporal-python-workshop:v13
  shell: /bin/bash
""",
        encoding="utf-8",
    )

    status = render_instruqt_track_directory(
        course(images=images),
        source_root,
        rendered_track,
        {
            "workstation": (
                "123456789012.dkr.ecr.us-west-1.amazonaws.com/"
                "course/workstation:git-abc123"
            )
        },
    )

    decoded = yaml.safe_load((rendered_track / "config.yml").read_text())
    assert decoded["containers"][0]["image"] == (
        "123456789012.dkr.ecr.us-west-1.amazonaws.com/course/workstation:git-abc123"
    )
    rendered_track_yaml = yaml.safe_load((rendered_track / "track.yml").read_text())
    assert rendered_track_yaml["owner"] == "temporal"
    assert rendered_track_yaml["slug"] == "workflow-fundamentals"
    assert "id" not in rendered_track_yaml
    assert status["phase"] == "Rendered"
    assert status["managedImages"]["workstation"]["action"] == "overridden"


def test_render_instruqt_track_directory_rewrites_testing_track_metadata(tmp_path):
    source_root = tmp_path / "source"
    track_dir = source_root / "tracks" / "workflow-fundamentals"
    rendered_track = tmp_path / "rendered"
    track_dir.mkdir(parents=True)
    (track_dir / "track.yml").write_text(
        """
slug: workflow-fundamentals
id: production-track-id
title: Workflow Fundamentals
owner: temporal
""",
        encoding="utf-8",
    )
    (track_dir / "config.yml").write_text('version: "3"\n', encoding="utf-8")

    status = render_instruqt_track_directory(
        course(testing=True),
        source_root,
        rendered_track,
        {},
        channel="testing",
    )

    rendered_track_yaml = yaml.safe_load((rendered_track / "track.yml").read_text())
    assert rendered_track_yaml["owner"] == "temporal"
    assert rendered_track_yaml["slug"] == "testing-workflow-fundamentals"
    assert rendered_track_yaml["title"] == "[testing] Workflow Fundamentals"
    assert "id" not in rendered_track_yaml
    assert status["track"]["target"] == "temporal/testing-workflow-fundamentals"


def test_instruqt_image_plan_uses_codebuild_with_source_and_content(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_CODEBUILD_PROJECT", "tmprl-dem-cld-build")
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "content", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
            "platform": "linux/amd64",
            "buildArgs": {"PYTHON_VERSION": "3.12"},
        }
    }

    plan = resolve_instruqt_course_image_build_plan(
        InstruqtCourseImageBuildPlanRequest(
            instruqt_course=course(content=True, images=images),
            external_resources={
                "ecr": {
                    "repositories": [
                        {
                            "build": "workstation",
                            "name": (
                                "tmprl-dem-cld/workflow-fundamentals/workstation"
                            ),
                            "repositoryUri": (
                                "public.ecr.aws/example/tmprl-dem-cld/"
                                "workflow-fundamentals/workstation"
                            ),
                        }
                    ]
                }
            },
            channel="production",
        )
    ).plan

    assert plan["action"] == "codebuild"
    assert set(plan["sources"]) == {"source", "content"}
    assert plan["sources"]["source"]["version"] == (
        "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
    )
    assert plan["sources"]["content"]["version"] == (
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    assert plan["imageTemplates"]["workstation"].startswith(
        "public.ecr.aws/example/"
    )


def test_instruqt_codebuild_buildspec_builds_from_source_and_content():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "content", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
            "platform": "linux/amd64",
            "buildArgs": {"PYTHON_VERSION": "3.12"},
        }
    }

    buildspec = instruqt_codebuild_buildspec(
        course(content=True, images=images),
        {
            "workstation": (
                "public.ecr.aws/example/"
                "tmprl-dem-cld/workflow-fundamentals/workstation:${IMAGE_VERSION}"
            )
        },
        sources={
            "source": {
                "repo": "https://github.com/temporal-sa/workflow-fundamentals",
                "version": "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
            },
            "content": {
                "repo": "https://github.com/temporal-sa/workshop-code",
                "version": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
        },
        aws_region="us-west-1",
    )

    assert "git clone --no-checkout" in buildspec
    assert "INSTRUQT_SOURCE_REPO" in buildspec
    assert "/tmp/source" in buildspec
    assert "INSTRUQT_CONTENT_REPO" in buildspec
    assert "/tmp/content" in buildspec
    assert "aws ecr-public get-login-password --region us-east-1" in buildspec
    assert "docker login --username AWS --password-stdin public.ecr.aws" in buildspec
    assert "docker build --platform linux/amd64" in buildspec
    assert "--build-arg PYTHON_VERSION=3.12" in buildspec
    assert "-f /tmp/source/Dockerfile" in buildspec
    assert "/tmp/content" in buildspec
    assert "docker push public.ecr.aws/example/" in buildspec


def test_instruqt_rendered_image_version_uses_full_source_commit():
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "source", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }
    instruqt_course = course(images=images)
    source_commit = "7ff770274160730eda857b88838b5111f7c69719"

    rendered = instruqt_activities.render_instruqt_image_templates(
        instruqt_course,
        {
            "workstation": (
                "public.ecr.aws/example/tmprl-dem-cld/"
                "workflow-fundamentals/workstation:${IMAGE_VERSION}"
            )
        },
        source_commit,
    )

    expected_suffix = (
        f"-b{instruqt_activities.instruqt_image_definition_hash(instruqt_course)[:12]}"
    )
    assert rendered["workstation"].endswith(
        f":git-{source_commit}{expected_suffix}"
    )


def test_instruqt_image_plan_does_not_reuse_mismatched_image_template(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_CODEBUILD_PROJECT", "tmprl-dem-cld-build")
    images = {
        "workstation": {
            "container": "workstation",
            "context": {"source": "source", "path": "."},
            "dockerfile": {"source": "source", "path": "Dockerfile"},
        }
    }
    instruqt_course = course(images=images)
    source_commit = instruqt_course["metadata"]["annotations"][SOURCE_COMMIT_ANNOTATION]
    build_hash = instruqt_activities.instruqt_image_definition_hash(instruqt_course)

    plan = resolve_instruqt_course_image_build_plan(
        InstruqtCourseImageBuildPlanRequest(
            instruqt_course=instruqt_course,
            external_resources={
                "ecr": {
                    "repositories": [
                        {
                            "build": "workstation",
                            "repositoryUri": (
                                "public.ecr.aws/example/tmprl-dem-cld/"
                                "workflow-fundamentals/workstation"
                            ),
                        }
                    ]
                }
            },
            previous_build_status={
                "phase": "Pushed",
                "channel": "production",
                "imageDefinitionHash": build_hash,
                "sourceCommitSha": source_commit,
                "sourceArtifactDigest": instruqt_course["metadata"]["annotations"][
                    SOURCE_ARTIFACT_DIGEST_ANNOTATION
                ],
                "images": {
                    "workstation": (
                        "public.ecr.aws/example/tmprl-dem-cld/"
                        "workflow-fundamentals/workstation:"
                        f"git-{source_commit[:12]}-b{build_hash[:12]}"
                    )
                },
            },
            channel="production",
        )
    ).plan

    assert plan["action"] == "codebuild"
    assert plan["imageTemplates"]["workstation"] == (
        "public.ecr.aws/example/tmprl-dem-cld/workflow-fundamentals/"
        "workstation:${IMAGE_VERSION}"
    )


def test_instruqt_track_publish_only_force_pushes(monkeypatch):
    calls = []

    async def fake_execute_activity(activity, request, **kwargs):
        calls.append((activity, request, kwargs))
        if activity is instruqt_activities.push_instruqt_course_track:
            return SimpleNamespace(
                status={
                    "phase": "Pushed",
                    "push": {"phase": "Succeeded"},
                }
            )
        raise AssertionError(f"Unexpected activity: {activity}")

    monkeypatch.setattr(
        workflow_module.workflow,
        "execute_activity",
        fake_execute_activity,
    )

    result = asyncio.run(
        workflow_instance()._push_track_channel(
            course(),
            images={},
            channel="production",
        )
    )

    assert result == {
        "phase": "Pushed",
        "push": {"phase": "Succeeded"},
    }
    assert [activity for activity, _, _ in calls] == [
        instruqt_activities.push_instruqt_course_track,
    ]


def test_instruqt_track_push_skips_explicit_validation(monkeypatch, tmp_path):
    observed = {}

    class TemporaryDirectory:
        def __init__(self, *, prefix):
            observed["prefix"] = prefix

        def __enter__(self):
            return str(tmp_path)

        def __exit__(self, *_args):
            return False

    def prepare(_course, _workspace, _images, *, validate, channel):
        observed["validate"] = validate
        return {
            "phase": "Rendered",
            "channel": channel,
            "target": "temporal/workflow-fundamentals",
        }

    def run(_track_dir, args, **_kwargs):
        observed["args"] = args
        return {"phase": "Succeeded"}

    monkeypatch.setattr(
        instruqt_activities.tempfile,
        "TemporaryDirectory",
        TemporaryDirectory,
    )
    monkeypatch.setattr(
        instruqt_activities,
        "prepare_managed_instruqt_artifact",
        prepare,
    )
    monkeypatch.setattr(instruqt_activities, "run_instruqt_command", run)

    result = instruqt_activities.push_instruqt_course_track(
        InstruqtCourseArtifactRequest(
            instruqt_course=course(),
            images={},
            channel="production",
        )
    ).status

    assert observed == {
        "prefix": "instruqt-push-",
        "validate": False,
        "args": ["track", "push", "--force"],
    }
    assert result["phase"] == "Pushed"
    assert result["artifact"]["phase"] == "Rendered"


def test_instruqt_disabled_testing_channel_schedules_no_activities(monkeypatch):
    workflow = workflow_instance()
    workflow._status = {
        "testing": {
            "phase": "Disabled",
            "reason": "TestingDisabled",
            "resolved": True,
        }
    }

    async def unexpected_execute_activity(*_args, **_kwargs):
        raise AssertionError("disabled testing channel scheduled an activity")

    monkeypatch.setattr(
        workflow_module.workflow,
        "execute_activity",
        unexpected_execute_activity,
    )

    asyncio.run(
        workflow._reconcile_testing_channel(
            course(),
            {"phase": "Ready"},
        )
    )

    assert workflow._status["testing"]["phase"] == "Disabled"


def test_finalized_instruqt_status_accepts_force_push_with_testing_disabled():
    status = reconcile_instruqt_course(course())
    status["deployment"] = {"phase": "Pushed"}

    finalized = workflow_module.finalized_instruqt_status(status)

    assert finalized["phase"] == "Ready"
    assert finalized["message"] == "Instruqt production track is force-pushed."
    assert any(
        condition["type"] == "Reconciled" and condition["status"] == "True"
        for condition in finalized["conditions"]
    )


def test_instruqt_reconcile_key_changes_with_force_reconcile_token():
    base = course()
    forced = course(
        annotations={
            **base["metadata"]["annotations"],
            FORCE_RECONCILE_ANNOTATION: "manual-1",
        }
    )
    forced_again = course(
        annotations={
            **base["metadata"]["annotations"],
            FORCE_RECONCILE_ANNOTATION: "manual-2",
        }
    )

    assert instruqt_course_reconcile_key(base) != instruqt_course_reconcile_key(forced)
    assert instruqt_course_reconcile_key(forced) != instruqt_course_reconcile_key(
        forced_again
    )


def test_instruqt_workflow_reconcile_signal_dedupes_completed_identity():
    instruqt_course = course()
    workflow = workflow_instance()
    workflow._completed_reconcile_identity = reconcile_identity(instruqt_course)

    workflow.reconcile(reconcile_signal(deepcopy(instruqt_course)))

    assert workflow._pending_spec is None
    assert workflow._pending_reconcile_identity is None


def test_instruqt_workflow_force_reconcile_signal_sets_pending_request_and_dedupes():
    workflow = workflow_instance()

    workflow.force_reconcile(
        ForceReconcileSignal(token=" token-1 ", reason=" manual retry ")
    )
    workflow.force_reconcile(
        ForceReconcileSignal(token="token-1", reason="ignored duplicate")
    )

    assert workflow._pending_force_reconcile == {
        "token": "token-1",
        "reason": "manual retry",
    }

    workflow._pending_force_reconcile = None
    workflow._completed_force_reconcile_token = "token-1"
    workflow.force_reconcile(
        ForceReconcileSignal(token="token-1", reason="already completed")
    )

    assert workflow._pending_force_reconcile is None


def test_instruqt_workflow_failed_identity_is_not_remembered():
    instruqt_course = course()
    identity = reconcile_identity(instruqt_course)
    workflow = workflow_instance()
    workflow._current_reconcile_identity = identity
    workflow._current_force_reconcile_token = "manual-1"
    workflow._status = {"phase": "Error"}

    workflow._complete_current_reconcile()
    workflow.reconcile(reconcile_signal(deepcopy(instruqt_course)))

    assert workflow._completed_reconcile_identity is None
    assert workflow._completed_force_reconcile_token is None
    assert workflow._pending_spec is not None
    assert workflow._pending_reconcile_identity == identity


def test_instruqt_successful_force_reconcile_token_is_remembered_as_completed():
    instruqt_course = course()
    workflow = workflow_instance()
    workflow._current_reconcile_identity = reconcile_identity(instruqt_course)
    workflow._current_force_reconcile_token = "manual-1"
    workflow._status = {"phase": "Ready"}

    workflow._complete_current_reconcile()

    assert workflow._completed_reconcile_identity == reconcile_identity(instruqt_course)
    assert workflow._completed_force_reconcile_token == "manual-1"


def test_instruqt_status_with_deployment_status_promotes_waiting_for_images():
    status = status_with_deployment_status(
        {"phase": "Validated", "message": "base ok"},
        {"phase": "WaitingForImages", "message": "images are not ready"},
        testing=False,
    )

    assert status["phase"] == "WaitingForImages"
    assert status["message"] == "images are not ready"
    assert remember_completed_reconcile_identity(status) is False


def test_instruqt_check_worker_version_signal_requests_continue_as_new(monkeypatch):
    workflow = workflow_instance()
    monkeypatch.setattr(
        workflow_module.workflow,
        "info",
        lambda: SimpleNamespace(
            is_target_worker_deployment_version_changed=lambda: True
        ),
    )

    workflow.check_worker_version()

    assert workflow._continue_as_new_for_worker_version is True


def test_instruqt_worker_update_continue_as_new_uses_initial_auto_upgrade(
    monkeypatch,
):
    calls = []
    workflow = workflow_instance()
    workflow._continue_as_new_for_worker_version = True

    monkeypatch.setattr(
        workflow_module.workflow,
        "continue_as_new",
        lambda **kwargs: calls.append(kwargs),
    )

    workflow._continue_as_new_if_needed("workflow-fundamentals")

    assert len(calls) == 1
    assert calls[0]["initial_versioning_behavior"] == (
        workflow_module.workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
    )


def test_instruqt_course_watch_signals_resolved_source(monkeypatch):
    calls = []

    monkeypatch.setattr(
        watch,
        "ensure_instruqt_course_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "reconcile_source_watch",
        lambda resource, config: {
            "sourceCommitSha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "sourceArtifactDigest": "sha256:ccccdddd",
            "revision": "main@sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
    )

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args[2]["metadata"]["name"], args[3]))

    monkeypatch.setattr(watch, "signal_instruqt_course_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_instruqt_course_resource(
            None,
            SimpleNamespace(source_watch_namespace="source-system"),
            course(annotations={}),
            seen,
        )
    )

    assert calls == [
        ("finalizer", "workflow-fundamentals"),
        ("signal", "workflow-fundamentals", "reconcile"),
    ]
    key = next(iter(seen.values()))
    assert key


def test_instruqt_course_watch_resolves_source_and_content(monkeypatch):
    calls = []
    resolved = {
        "source": {
            "sourceCommitSha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "sourceArtifactDigest": "sha256:bbbb",
            "revision": "main@sha1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        },
        "content": {
            "sourceCommitSha": "cccccccccccccccccccccccccccccccccccccccc",
            "sourceArtifactDigest": "sha256:cccc",
            "revision": "main@sha1:cccccccccccccccccccccccccccccccccccccccc",
        },
    }

    monkeypatch.setattr(
        watch,
        "ensure_instruqt_course_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )

    def reconcile_source_watch(resource, config, *, source_key="source"):
        calls.append(("source-watch", source_key))
        return resolved[source_key]

    monkeypatch.setattr(watch, "reconcile_source_watch", reconcile_source_watch)

    async def signal_workflow(*args, **kwargs):
        payload = args[2]
        calls.append(
            (
                "signal",
                payload["metadata"]["annotations"][SOURCE_COMMIT_ANNOTATION],
                payload["metadata"]["annotations"][CONTENT_SOURCE_COMMIT_ANNOTATION],
            )
        )

    monkeypatch.setattr(watch, "signal_instruqt_course_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_instruqt_course_resource(
            None,
            SimpleNamespace(source_watch_namespace="source-system"),
            course(annotations={}, content=True),
            seen,
        )
    )

    assert calls == [
        ("finalizer", "workflow-fundamentals"),
        ("source-watch", "source"),
        ("source-watch", "content"),
        (
            "signal",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "cccccccccccccccccccccccccccccccccccccccc",
        ),
    ]


def test_instruqt_course_watch_resolves_testing_branch(monkeypatch):
    calls = []

    monkeypatch.setattr(
        watch,
        "ensure_instruqt_course_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "probe_github_branch",
        lambda resource, config, *, source_key, branch: {
            "state": "exists",
            "reason": "BranchExists",
            "message": f"{branch} exists",
        },
    )

    def reconcile_source_watch(
        resource,
        config,
        *,
        source_key="source",
        branch=None,
        repository_key=None,
    ):
        calls.append(("source-watch", source_key, branch, repository_key))
        revision_branch = branch or "main"
        return {
            "sourceCommitSha": (
                "b" * 40 if repository_key else "a" * 40
            ),
            "sourceArtifactDigest": (
                "sha256:testing" if repository_key else "sha256:prod"
            ),
            "revision": f"{revision_branch}@sha1:{'b' * 40 if repository_key else 'a' * 40}",
        }

    monkeypatch.setattr(watch, "reconcile_source_watch", reconcile_source_watch)

    async def signal_workflow(*args, **kwargs):
        payload = args[2]
        annotations = payload["metadata"]["annotations"]
        calls.append(
            (
                "signal",
                annotations[TESTING_BRANCH_STATE_ANNOTATION],
                annotations[TESTING_SOURCE_COMMIT_ANNOTATION],
            )
        )

    monkeypatch.setattr(watch, "signal_instruqt_course_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_instruqt_course_resource(
            None,
            SimpleNamespace(source_watch_namespace="source-system"),
            course(annotations={}, testing=True),
            seen,
        )
    )

    assert calls == [
        ("finalizer", "workflow-fundamentals"),
        ("source-watch", "source", None, None),
        (
            "source-watch",
            "source",
            "course-update",
            INSTRUQT_TESTING_SOURCE_REPOSITORY_KEY,
        ),
        ("signal", "exists", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
    ]


def test_instruqt_course_watch_cleans_testing_resources_when_branch_missing(
    monkeypatch,
):
    calls = []

    monkeypatch.setattr(
        watch,
        "ensure_instruqt_course_finalizer",
        lambda name: calls.append(("finalizer", name)),
    )
    monkeypatch.setattr(
        watch,
        "probe_github_branch",
        lambda resource, config, *, source_key, branch: {
            "state": "missing",
            "reason": "BranchMissing",
            "message": f"{branch} is absent",
        },
    )
    monkeypatch.setattr(
        watch,
        "cleanup_instruqt_testing_source_watches",
        lambda resource, config: calls.append(("cleanup", resource["metadata"]["name"])),
    )

    def reconcile_source_watch(resource, config, *, source_key="source"):
        calls.append(("source-watch", source_key))
        return {
            "sourceCommitSha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "sourceArtifactDigest": "sha256:prod",
            "revision": "main@sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }

    monkeypatch.setattr(watch, "reconcile_source_watch", reconcile_source_watch)

    async def signal_workflow(*args, **kwargs):
        payload = args[2]
        calls.append(
            (
                "signal",
                payload["metadata"]["annotations"][TESTING_BRANCH_STATE_ANNOTATION],
            )
        )

    monkeypatch.setattr(watch, "signal_instruqt_course_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_instruqt_course_resource(
            None,
            SimpleNamespace(source_watch_namespace="source-system"),
            course(annotations={}, testing=True),
            seen,
        )
    )

    assert calls == [
        ("finalizer", "workflow-fundamentals"),
        ("source-watch", "source"),
        ("cleanup", "workflow-fundamentals"),
        ("signal", "missing"),
    ]


def test_probe_github_branch_confirms_repo_before_reporting_missing(monkeypatch):
    calls = []

    monkeypatch.setattr(
        watch,
        "github_token_from_secret_ref",
        lambda config, secret_ref: "github_pat_test",
    )

    def fake_urlopen(request, timeout):
        calls.append(request.full_url)
        if "/branches/" in request.full_url:
            raise HTTPError(request.full_url, 404, "Not Found", None, None)
        return FakeHTTPResponse(200)

    monkeypatch.setattr(watch, "urlopen", fake_urlopen)

    result = watch.probe_github_branch(
        course(testing=True),
        SimpleNamespace(source_watch_namespace="source-system"),
        source_key="source",
        branch="deleted-branch",
    )

    assert result["state"] == "missing"
    assert calls == [
        "https://api.github.com/repos/temporal-sa/workflow-fundamentals/branches/deleted-branch",
        "https://api.github.com/repos/temporal-sa/workflow-fundamentals",
    ]


def test_probe_github_branch_keeps_unknown_when_repo_access_is_unknown(monkeypatch):
    monkeypatch.setattr(
        watch,
        "github_token_from_secret_ref",
        lambda config, secret_ref: "github_pat_test",
    )

    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", None, None)

    monkeypatch.setattr(watch, "urlopen", fake_urlopen)

    result = watch.probe_github_branch(
        course(testing=True),
        SimpleNamespace(source_watch_namespace="source-system"),
        source_key="source",
        branch="deleted-branch",
    )

    assert result["state"] == "unknown"
    assert result["reason"] == "GitHubRepoUnavailable"


class FakeHTTPResponse:
    def __init__(self, status: int):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_instruqt_course_watch_signals_finalize_on_delete(monkeypatch):
    calls = []
    deleting = course()
    deleting["metadata"]["deletionTimestamp"] = "2026-06-16T12:00:00Z"

    async def signal_workflow(*args, **kwargs):
        calls.append(("signal", args[2]["metadata"]["name"], args[3]))

    monkeypatch.setattr(watch, "signal_instruqt_course_workflow", signal_workflow)

    seen = {}
    asyncio.run(
        watch.reconcile_instruqt_course_resource(
            None,
            SimpleNamespace(source_watch_namespace="source-system"),
            deleting,
            seen,
        )
    )

    assert calls == [("signal", "workflow-fundamentals", "finalize")]
    assert seen == {
        "instruqt-course/workflow-fundamentals": "finalize:2026-06-16T12:00:00Z"
    }


def test_signal_instruqt_course_workflow_starts_without_versioning_override():
    calls = []

    class TemporalClient:
        async def start_workflow(self, *args, **kwargs):
            calls.append((args, kwargs))

    asyncio.run(
        watch.signal_instruqt_course_workflow(
            TemporalClient(),
            SimpleNamespace(task_queue="registry-management", cluster=None),
            course(),
            "reconcile",
        )
    )

    assert len(calls) == 1
    _, kwargs = calls[0]
    assert kwargs["id"] == "instruqt-course/workflow-fundamentals"
    assert kwargs["task_queue"] == "registry-management"
    assert kwargs["start_signal"] == "reconcile"
    assert kwargs["args"] == [
        InstruqtCourseRunRequest(course_name="workflow-fundamentals")
    ]
    signal_arg = kwargs["start_signal_args"][0]
    assert isinstance(signal_arg, InstruqtCourseReconcileSignal)
    assert signal_arg.instruqtCourse["spec"]["source"]["repo"].endswith(
        "/workflow-fundamentals"
    )
    assert "versioning_override" not in kwargs


def test_instruqt_source_watch_uses_shared_platform_auth():
    resources = watch.source_watch_resources(
        course(annotations={}),
        namespace="source-system",
        interval="1m",
    )

    assert resources[-1]["spec"]["secretRef"] == {
        "name": PLATFORM_SOURCE_AUTH_SECRET_NAME
    }


def test_instruqt_content_source_watch_uses_content_repository_name():
    resources = watch.source_watch_resources(
        course(annotations={}, content=True),
        namespace="source-system",
        interval="1m",
        source_key="content",
    )

    assert resources[-1]["metadata"]["name"] == "workflow-fundamentals-content"
    assert resources[-1]["spec"]["url"] == "https://github.com/temporal-sa/workshop-code"
    assert resources[-1]["spec"]["secretRef"] == {
        "name": PLATFORM_SOURCE_AUTH_SECRET_NAME
    }


def test_instruqt_testing_source_watch_uses_testing_name_and_branch():
    resources = watch.source_watch_resources(
        course(annotations={}, testing=True),
        namespace="source-system",
        interval="1m",
        source_key="source",
        branch="course-update",
        repository_key=INSTRUQT_TESTING_SOURCE_REPOSITORY_KEY,
    )

    assert resources[-1]["metadata"]["name"] == (
        "workflow-fundamentals-testing-source"
    )
    assert resources[-1]["spec"]["ref"] == {"branch": "course-update"}
    assert resources[-1]["spec"]["secretRef"] == {
        "name": PLATFORM_SOURCE_AUTH_SECRET_NAME
    }
