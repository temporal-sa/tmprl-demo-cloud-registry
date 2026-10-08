from __future__ import annotations

import pytest

from demo_registry_operator import resources
from demo_registry_operator.activities.external_resources.iam import (
    lambda_project_secrets_policy,
)
from demo_registry_operator.activities.external_resources.planning import (
    secrets_manager_request,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.workflows.demo_project_serverless_lambda import (
    secret_env,
    secret_paths_for_lambda,
)


def shared_project(shared: str = "openai") -> dict:
    return {
        "metadata": {"name": "shared-demo"},
        "spec": {
            "secrets": {
                "model": {"shared": shared, "requiredProperties": ["OPENAI_API_KEY"]},
                "private": {},
            },
            "components": {
                "app": {
                    "build": "app",
                    "secrets": ["private"],
                    "env": [
                        {
                            "name": "MODEL_API_KEY",
                            "valueFrom": {"inputRef": "secrets.model.OPENAI_API_KEY"},
                        }
                    ],
                }
            },
        },
    }


@pytest.mark.parametrize("shared", ["openai", "team/service", "service.name+version=1"])
def test_shared_secrets_render_with_platform_ownership_and_keep_private_paths(shared):
    project = shared_project(shared)
    rendered = resources.runtime_secret_resources(project)
    by_name = {item["metadata"]["name"]: item for item in rendered}
    shared_path = f"tmprl-dem-cld/platform/shared/{shared}"

    assert by_name["user-secret-model"]["spec"]["dataFrom"] == [
        {"extract": {"key": shared_path}}
    ]
    assert by_name["user-secret-private"]["spec"]["dataFrom"] == [
        {"extract": {"key": "tmprl-dem-cld/shared-demo/private"}}
    ]
    assert resources.env_vars(project, "app", project["spec"]["components"]["app"], {})[0] == {
        "name": "MODEL_API_KEY",
        "valueFrom": {
            "secretKeyRef": {"name": "user-secret-model", "key": "OPENAI_API_KEY"}
        },
    }
    request = secrets_manager_request(project, checked_at="2026-10-08T00:00:00+00:00")
    requirements = {item.path: item for item in request.user_managed}
    assert requirements[shared_path].managed_by == "platform"
    assert requirements[shared_path].required_properties == ("OPENAI_API_KEY",)
    assert requirements["tmprl-dem-cld/shared-demo/private"].managed_by == "project-owner"
    status = resources.external_resource_status(project)["secretsManager"]["userManaged"]
    assert next(item for item in status if item["path"] == shared_path)["managedBy"] == "platform"


@pytest.mark.parametrize("shared", ["", "/openai", "../openai", "team/../../openai", "team/./key", "team//key", "openai/*", "openai?", True])
def test_shared_secret_paths_cannot_escape_the_shared_prefix(shared):
    with pytest.raises(ValueError, match="relative path under platform/shared"):
        resources.user_secret_remote_path(shared_project(shared), "model")


def test_lambda_shared_credentials_use_the_requested_path_and_scoped_iam(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_AWS_ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("AWS_REGION", "us-west-1")
    project = shared_project()
    component = project["spec"]["components"]["app"]
    component.pop("secrets")
    refs = secret_env(component, project)
    assert len(refs) == 1
    assert refs[0].name == "MODEL_API_KEY"
    assert refs[0].secret_path == "tmprl-dem-cld/platform/shared/openai"
    assert refs[0].property_name == "OPENAI_API_KEY"

    policy = lambda_project_secrets_policy(
        WorkerConfig.from_env(),
        project="shared-demo",
        secret_paths=secret_paths_for_lambda(project),
    )
    arns = policy["Statement"][0]["Resource"]
    shared_arns = [arn for arn in arns if "/platform/shared/" in arn]
    assert shared_arns == [
        "arn:aws:secretsmanager:us-west-1:123456789012:secret:tmprl-dem-cld/platform/shared/openai-*"
    ]
