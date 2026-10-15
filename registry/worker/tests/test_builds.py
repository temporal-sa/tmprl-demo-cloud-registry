from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from demo_registry_operator.activities import builds
from demo_registry_operator.resources import (
    PLATFORM_SOURCE_AUTH_SECRET_PATH,
    SOURCE_ARTIFACT_DIGEST_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
)
from temporalio.exceptions import ApplicationError

SOURCE_SHA = "388c26cb1e5ed0842f44b7949416aeddbd908cba"
SOURCE_DIGEST = "sha256:aaaabbbb"


def demo_project(*, source_sha=SOURCE_SHA, source_digest: str | None = None):
    annotations = {SOURCE_COMMIT_ANNOTATION: source_sha}
    if source_digest:
        annotations[SOURCE_ARTIFACT_DIGEST_ANNOTATION] = source_digest
    return {
        "metadata": {
            "name": "banking-money-transfer",
            "annotations": annotations,
        },
        "spec": {
            "source": {
                "repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
            },
            "builds": {
                "frontend": {
                    "context": ".",
                    "dockerfile": "docker/frontend.Dockerfile",
                },
                "backend": {
                    "context": ".",
                    "dockerfile": "docker/backend.Dockerfile",
                },
            },
            "components": {
                "frontend": {"build": "frontend"},
                "backend": {"build": "backend"},
            },
        },
    }


def private_source_demo_project(*, source_sha=SOURCE_SHA):
    project = demo_project(source_sha=source_sha)
    project["metadata"]["name"] = "registry-operator"
    project["spec"]["source"]["gitRepository"] = {
        "url": "ssh://git@github.com/temporal-sa/tmprl-demo-cloud-demo-registry.git",
        "authSecret": "source-auth",
    }
    return project


def buildpack_demo_project(*, source_sha=SOURCE_SHA):
    project = demo_project(source_sha=source_sha)
    project["spec"]["builds"] = {
        "app": {
            "context": ".",
            "strategy": "buildpack",
            "builder": "paketobuildpacks/builder-jammy-base",
            "env": {
                "BP_PYTHON_VERSION": "3.12.*",
            },
            "processes": {
                "web": "python -m app.web",
                "worker": "python -m app.worker",
            },
        }
    }
    project["spec"]["components"] = {
        "web": {"build": "app"},
    }
    return project


def lambda_demo_project(*, source_sha=SOURCE_SHA):
    project = demo_project(source_sha=source_sha)
    project["spec"]["builds"]["worker"] = {
        "context": ".",
        "dockerfile": "docker/worker-lambda.Dockerfile",
    }
    project["spec"]["components"]["worker"] = {
        "build": "worker",
        "serverless": {
            "provider": "aws-lambda",
            "packageType": "image",
            "architecture": "x86_64",
        },
        "worker": {
            "versioning": {
                "enabled": True,
                "deploymentName": "money-transfer",
            }
        },
    }
    return project


def previous_build_status(
    *,
    source_sha=SOURCE_SHA,
    source_digest: str | None = None,
    build_definition_hash: str | None = None,
):
    status = {
        "phase": "Pushed",
        "mode": "aws-codebuild",
        "sourceCommitSha": source_sha,
        "buildDefinitionHash": build_definition_hash
        or builds.build_definition_hash(demo_project()),
        "images": {
            "frontend": (
                "example.com/tmprl-dem-cld/banking-money-transfer/"
                "frontend:git-388c26cb1e5e"
            ),
            "backend": (
                "example.com/tmprl-dem-cld/banking-money-transfer/"
                "backend:git-388c26cb1e5e"
            ),
        },
    }
    if source_digest:
        status["sourceArtifactDigest"] = source_digest
    return status


def build_plan_status(request):
    status = builds.completed_build_status(
        builds.resolve_project_image_build_plan(
            builds.ResolveProjectImageBuildRequest(
                demo_project=request["demoProject"],
                external_resources=request.get("externalResources"),
                previous_build_status=request.get("previousBuildStatus"),
            )
        ).plan
    )
    assert status is not None
    return status


def test_build_reuses_workflow_bookkept_images_for_same_source_commit(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CODEBUILD_PROJECT", raising=False)

    status = build_plan_status(
        {
            "demoProject": demo_project(),
            "previousBuildStatus": previous_build_status(),
        }
    )

    assert status["phase"] == "Resolved"
    assert status["mode"] == "workflow-cache"
    assert status["sourceCommitSha"] == SOURCE_SHA
    assert status["buildDefinitionHash"] == builds.build_definition_hash(
        demo_project()
    )
    assert status["imageVersion"] == "git-388c26cb1e5e"
    assert status["images"] == previous_build_status()["images"]
    assert "codebuild" not in status
    assert {entry["source"] for entry in status["builds"]} == {"workflow-cache"}


def test_build_reuses_workflow_cache_for_same_source_artifact_digest(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CODEBUILD_PROJECT", raising=False)

    status = build_plan_status(
        {
            "demoProject": demo_project(
                source_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                source_digest=SOURCE_DIGEST,
            ),
            "previousBuildStatus": previous_build_status(
                source_sha=SOURCE_SHA,
                source_digest=SOURCE_DIGEST,
            ),
        }
    )

    assert status["phase"] == "Resolved"
    assert status["mode"] == "workflow-cache"
    assert status["sourceCommitSha"] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert status["sourceArtifactDigest"] == SOURCE_DIGEST
    assert status["images"] == previous_build_status()["images"]


def test_image_version_adds_build_hash_for_buildpack_projects():
    project = buildpack_demo_project()

    assert builds.image_version(demo_project(), SOURCE_SHA) == "git-388c26cb1e5e"
    assert builds.image_version(project, SOURCE_SHA) == (
        f"git-388c26cb1e5e-b{builds.build_definition_hash(project)[:12]}"
    )


def test_build_does_not_reuse_workflow_cache_for_new_source_commit(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CODEBUILD_PROJECT", raising=False)

    with pytest.raises(ApplicationError) as raised:
        build_plan_status(
            {
                "demoProject": demo_project(
                    source_sha="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                "previousBuildStatus": previous_build_status(),
            }
        )
    status = raised.value.details[0]["status"]

    assert status["phase"] == "Error"
    assert status["message"] == "AWS CodeBuild project is not configured."


def test_build_does_not_reuse_workflow_cache_for_changed_build_definition(
    monkeypatch,
):
    monkeypatch.delenv("DEMO_REGISTRY_CODEBUILD_PROJECT", raising=False)
    project = demo_project()
    project["spec"]["builds"]["frontend"]["dockerfile"] = "Dockerfile"

    with pytest.raises(ApplicationError) as raised:
        build_plan_status(
            {
                "demoProject": project,
                "previousBuildStatus": previous_build_status(),
            }
        )
    status = raised.value.details[0]["status"]

    assert status["phase"] == "Error"
    assert status["message"] == "AWS CodeBuild project is not configured."


def test_codebuild_build_id_is_recovered_from_heartbeat_details(monkeypatch):
    monkeypatch.setattr(
        builds.activity,
        "info",
        lambda: SimpleNamespace(
            heartbeat_details=[
                {
                    "project": "banking-money-transfer",
                    "codebuildBuildId": "tmprl-dem-cld-build:abc123",
                    "status": "IN_PROGRESS",
                }
            ]
        ),
    )

    assert builds.codebuild_build_id_from_heartbeat() == "tmprl-dem-cld-build:abc123"


def test_codebuild_build_id_is_absent_without_heartbeat_details(monkeypatch):
    monkeypatch.setattr(
        builds.activity,
        "info",
        lambda: SimpleNamespace(heartbeat_details=[]),
    )

    assert builds.codebuild_build_id_from_heartbeat() is None


def test_codebuild_source_resolution_prefers_commit_fallback_over_branch_marker():
    assert (
        builds.resolved_codebuild_source_version(
            {"resolvedSourceVersion": "main"},
            SOURCE_SHA,
        )
        == SOURCE_SHA
    )


def test_source_config_derives_codebuild_private_source_auth():
    source = builds.source_config(private_source_demo_project())

    assert source == {
        "repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        "branch": "main",
        "codebuildRepo": (
            "ssh://git@github.com/temporal-sa/"
            "tmprl-demo-cloud-demo-registry.git"
        ),
        "sourceAuthSecretPath": "tmprl-dem-cld/registry-operator/source-auth",
    }


def test_source_config_uses_shared_pat_for_temporal_sa_https_source():
    source = builds.source_config(demo_project())

    assert source == {
        "repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        "branch": "main",
        "codebuildRepo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        "sourceAuthSecretPath": PLATFORM_SOURCE_AUTH_SECRET_PATH,
    }


def test_monty_project_resolves_authenticated_codebuild_plan(monkeypatch):
    project_path = Path(__file__).resolve().parents[3] / "projects/demo/monty.yaml"
    project = yaml.safe_load(project_path.read_text())
    monkeypatch.setenv("DEMO_REGISTRY_CODEBUILD_PROJECT", "tmprl-dem-cld-build")
    monkeypatch.setenv("DEMO_REGISTRY_AWS_ACCOUNT_ID", "429214323166")
    monkeypatch.setenv("DEMO_REGISTRY_AWS_REGION", "us-west-1")

    plan = builds.resolve_project_image_build_plan(
        builds.ResolveProjectImageBuildRequest(demo_project=project)
    ).plan

    assert plan["action"] == "codebuild"
    # The catalog repo is the upstream harness; CodeBuild clones the adapter.
    assert plan["sourceRepo"] == project["spec"]["source"]["gitRepository"]["url"]
    assert plan["sourceVersion"] == "main"
    assert plan["sourceAuthSecretPath"] == PLATFORM_SOURCE_AUTH_SECRET_PATH
    assert set(plan["imageTemplates"]) == {"app"}


def test_codebuild_private_source_uses_no_source_checkout():
    args = builds.codebuild_start_build_args(
        private_source_demo_project(),
        SimpleNamespace(
            codebuild_project="tmprl-dem-cld-build",
            aws_region="us-west-1",
            codebuild_timeout_minutes=30,
        ),
        image_templates={
            "frontend": (
                "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
                "tmprl-dem-cld/registry-operator/frontend:git-${SOURCE_SHA}"
            ),
            "backend": (
                "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
                "tmprl-dem-cld/registry-operator/backend:git-${SOURCE_SHA}"
            ),
        },
        source_repo=(
            "ssh://git@github.com/temporal-sa/"
            "tmprl-demo-cloud-demo-registry.git"
        ),
        source_version=SOURCE_SHA,
        source_auth_secret_path="tmprl-dem-cld/registry-operator/source-auth",
    )

    assert args["sourceTypeOverride"] == "NO_SOURCE"
    assert "sourceLocationOverride" not in args
    assert "sourceVersion" not in args
    env = {
        variable["name"]: variable
        for variable in args["environmentVariablesOverride"]
    }
    assert env["DEMO_SOURCE_REPO"]["type"] == "PLAINTEXT"
    assert env["DEMO_SOURCE_VERSION"]["value"] == SOURCE_SHA
    assert env["DEMO_SOURCE_AUTH_JSON"] == {
        "name": "DEMO_SOURCE_AUTH_JSON",
        "value": "tmprl-dem-cld/registry-operator/source-auth",
        "type": "SECRETS_MANAGER",
    }
    buildspec = args["buildspecOverride"]
    assert "git clone --no-checkout" in buildspec
    assert "DEMO_SOURCE_REPO" in buildspec
    assert "cd /tmp/source && git checkout" in buildspec
    assert "cd /tmp/source && python3 -c" in buildspec
    assert "public.ecr.aws/docker/library" in buildspec
    assert "DEMO_SOURCE_VERSION" in buildspec
    assert "cd /tmp/source && docker build" in buildspec


def test_codebuild_manual_checkout_supports_https_pat_json():
    buildspec = builds.codebuild_buildspec(
        demo_project(),
        {
            "frontend": (
                "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
                "tmprl-dem-cld/banking-money-transfer/frontend:git-${SOURCE_SHA}"
            ),
            "backend": (
                "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
                "tmprl-dem-cld/banking-money-transfer/backend:git-${SOURCE_SHA}"
            ),
        },
        aws_region="us-west-1",
        manual_source_checkout=True,
    )

    assert "DEMO_SOURCE_AUTH_JSON" in buildspec
    assert "username = data.get" in buildspec
    assert ".git-credentials" in buildspec
    assert "credential.helper" in buildspec


def test_codebuild_public_source_keeps_github_source_override():
    args = builds.codebuild_start_build_args(
        demo_project(),
        SimpleNamespace(
            codebuild_project="tmprl-dem-cld-build",
            aws_region="us-west-1",
            codebuild_timeout_minutes=30,
        ),
        image_templates={
            "frontend": (
                "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
                "tmprl-dem-cld/banking-money-transfer/frontend:git-${SOURCE_SHA}"
            ),
            "backend": (
                "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
                "tmprl-dem-cld/banking-money-transfer/backend:git-${SOURCE_SHA}"
            ),
        },
        source_repo="https://github.com/temporal-sa/demo-banking-app-money-transfer",
        source_version=SOURCE_SHA,
    )

    assert args["sourceTypeOverride"] == "GITHUB"
    assert args["sourceLocationOverride"] == (
        "https://github.com/temporal-sa/demo-banking-app-money-transfer"
    )
    assert args["sourceVersion"] == SOURCE_SHA
    assert "DEMO_SOURCE_AUTH_JSON" not in {
        variable["name"] for variable in args["environmentVariablesOverride"]
    }
    assert "git clone --no-checkout" not in args["buildspecOverride"]
    assert "public.ecr.aws/docker/library" in args["buildspecOverride"]


def test_codebuild_buildspec_defaults_to_dockerfile_strategy():
    buildspec = builds.codebuild_buildspec(
        demo_project(),
        {
            "frontend": "example.com/frontend:git-${SOURCE_SHA}",
            "backend": "example.com/backend:git-${SOURCE_SHA}",
        },
        aws_region="us-west-1",
    )

    assert "docker build -f docker/frontend.Dockerfile" in buildspec
    assert "docker push example.com/frontend:git-${SOURCE_SHA}" in buildspec
    assert "pack build" not in buildspec


def test_codebuild_buildspec_wraps_serverless_lambda_images():
    buildspec = builds.codebuild_buildspec(
        lambda_demo_project(),
        {
            "frontend": "example.com/frontend:git-${SOURCE_SHA}",
            "backend": "example.com/backend:git-${SOURCE_SHA}",
            "worker": "example.com/worker:${IMAGE_VERSION}",
        },
        aws_region="us-west-1",
    )

    assert "tmprl-lambda-secret-wrapper" in buildspec
    assert "public.ecr.aws/docker/library/golang:1.22" in buildspec
    assert "GOARCH=amd64" in buildspec
    assert "docker build -f docker/worker-lambda.Dockerfile -t tmprl-demo-local/worker:unwrapped" in buildspec
    assert "TMPRL_ORIGINAL_ENTRYPOINT_B64" in buildspec
    assert "ENTRYPOINT [\\\"/opt/tmprl-demo-cloud/bin/secret-wrapper\\\"]" in buildspec
    assert "docker build -f /tmp/tmprl-lambda-wrapper-images/worker/Dockerfile -t example.com/worker:${IMAGE_VERSION}" in buildspec
    assert "docker push example.com/worker:${IMAGE_VERSION}" in buildspec


def test_codebuild_buildspec_supports_buildpack_strategy():
    buildspec = builds.codebuild_buildspec(
        buildpack_demo_project(),
        {
            "app": (
                "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
                "tmprl-dem-cld/banking-money-transfer/app:${IMAGE_VERSION}"
            )
        },
        aws_region="us-west-1",
    )

    assert "pack-v0.40.6-${pack_arch}.tgz" in buildspec
    assert "x86_64|amd64) pack_arch=linux" in buildspec
    assert "aarch64|arm64) pack_arch=linux-arm64" in buildspec
    assert "pack build" in buildspec
    assert 'IMAGE_VERSION=\\"git-${SOURCE_SHA}-b' in buildspec
    assert "tmprl-demo/generated-processes" in buildspec
    assert "launch.toml" in buildspec
    assert "--post-buildpack /tmp/tmprl-demo-generated-buildpacks/app" in buildspec
    assert "web" in buildspec
    assert 'python -m app.web' in buildspec
    assert "worker" in buildspec
    assert 'python -m app.worker' in buildspec
    assert buildspec.index("launch.toml") < buildspec.index("pack build")
    assert "--builder paketobuildpacks/builder-jammy-base" in buildspec
    assert "--env 'BP_PYTHON_VERSION=3.12.*'" in buildspec
    assert "--publish" not in buildspec
    assert (
        "docker push 429214323166.dkr.ecr.us-west-1.amazonaws.com/"
        "tmprl-dem-cld/banking-money-transfer/app:${IMAGE_VERSION}"
    ) in buildspec
    assert "docker build" not in buildspec


def test_build_entry_records_buildpack_strategy_without_dockerfile():
    entry = builds.build_entry(
        buildpack_demo_project(),
        "app",
        image="example.com/app:git-388c26cb1e5e",
        phase="Pushed",
        source="aws-codebuild",
        external_resources=None,
    )

    assert entry["strategy"] == "buildpack"
    assert entry["builder"] == "paketobuildpacks/builder-jammy-base"
    assert "dockerfile" not in entry


def test_codebuild_idempotency_token_changes_between_build_plans():
    common = {
        "source_repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        "source_version": SOURCE_SHA,
        "image_templates": {
            "frontend": "example.com/frontend:git-${SOURCE_SHA}",
            "backend": "example.com/backend:git-${SOURCE_SHA}",
        },
    }

    first = builds.codebuild_idempotency_token(
        demo_project(),
        idempotency_token_salt="2026-06-15T01:00:00Z",
        **common,
    )
    same_plan = builds.codebuild_idempotency_token(
        demo_project(),
        idempotency_token_salt="2026-06-15T01:00:00Z",
        **common,
    )
    next_plan = builds.codebuild_idempotency_token(
        demo_project(),
        idempotency_token_salt="2026-06-15T01:05:00Z",
        **common,
    )

    assert first == same_plan
    assert first != next_plan
