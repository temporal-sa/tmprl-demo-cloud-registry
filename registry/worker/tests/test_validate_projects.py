from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]


def load_validator_module():
    spec = importlib.util.spec_from_file_location(
        "validate_projects",
        ROOT / "scripts/validate_projects.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


validate_projects = load_validator_module()
NEXUS_CANARY_TEMPLATE = (
    ROOT
    / "registry/worker/src/demo_registry_operator/canary_templates/nexus.yaml"
)


def project_document(spec: dict):
    return validate_projects.ProjectDocument(
        path=ROOT / "projects/test.yaml",
        data={
            "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
            "kind": "DemoProject",
            "metadata": {"name": "env-inputs"},
            "spec": spec,
        },
    )


def base_spec() -> dict:
    return {
        "source": {
            "repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        },
        "builds": {
            "app": {"context": ".", "dockerfile": "Dockerfile"},
        },
        "components": {
            "app": {
                "build": "app",
            },
        },
        "ingress": {"subdomain": "env-inputs"},
    }


def test_schema_accepts_component_iam_role_and_rejects_user_arn():
    spec = base_spec()
    spec["components"]["app"]["awsIamRoleArn"] = (
        "arn:aws:iam::429214323166:role/monty-payloads"
    )
    validator = validate_projects.load_default_validators()["DemoProject"]
    assert validate_projects.validate_schema(validator, project_document(spec)) == []

    spec["components"]["app"]["awsIamRoleArn"] = (
        "arn:aws:iam::429214323166:user/monty-payloads"
    )
    errors = validate_projects.validate_schema(validator, project_document(spec))
    assert len(errors) == 1
    assert "awsIamRoleArn" in errors[0].message


def test_lambda_component_rejects_kubernetes_iam_role():
    spec = base_spec()
    spec["components"]["app"]["serverless"] = {"provider": "aws-lambda"}
    spec["components"]["app"]["awsIamRoleArn"] = (
        "arn:aws:iam::429214323166:role/monty-payloads"
    )
    errors = validate_projects.validate_serverless_components(project_document(spec))
    assert [error.message for error in errors] == [
        "components.app.awsIamRoleArn is not supported for serverless aws-lambda components"
    ]


@pytest.mark.parametrize("name", ["api-key", "mtls", "lambda", "nexus", "crashable", "isolated-lambda"])
def test_canary_demo_project_template_passes_schema_and_semantics(name):
    template_path = NEXUS_CANARY_TEMPLATE.with_name(f"{name}.yaml")
    wrapper, load_errors = validate_projects.load_yaml_document(
        template_path
    )
    assert load_errors == []
    assert wrapper is not None
    document = validate_projects.ProjectDocument(
        path=template_path,
        data=wrapper["demoProjectTemplate"],
    )

    assert validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    ) == []
    assert validate_projects.validate_semantics([document]) == []


@pytest.mark.parametrize(
    ("app_path", "ok"),
    [("/control", True), ("/", True), ("control", False), ("/a b", False)],
)
def test_schema_accepts_only_absolute_registry_app_paths(app_path, ok):
    spec = base_spec()
    spec["registry"] = {"title": "App", "summary": "Does things", "appPath": app_path}
    validator = validate_projects.load_default_validators()["DemoProject"]
    errors = validate_projects.validate_schema(validator, project_document(spec))
    assert (errors == []) is ok


def test_control_panel_sample_project_passes_schema_and_semantics():
    sample_path = ROOT / "registry/web/demo/sample-project.json"
    data = json.loads(sample_path.read_text())["demoProjectTemplate"]
    document = validate_projects.ProjectDocument(path=sample_path, data=data)

    assert validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    ) == []
    assert validate_projects.validate_semantics([document]) == []


def instruqt_course_document(
    repo: str,
    *,
    git_repository_url: str | None = None,
):
    source = {"repo": repo, "path": "instruqt"}
    if git_repository_url is not None:
        source["gitRepository"] = {"url": git_repository_url}
    return validate_projects.ProjectDocument(
        path=ROOT / "projects/instruqt/test.yaml",
        data={
            "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
            "kind": "InstruqtCourse",
            "metadata": {"name": "test-course"},
            "spec": {
                "source": source,
                "track": {
                    "organization": "temporal",
                    "slug": "test-course",
                },
            },
        },
    )


def test_instruqt_schema_accepts_allowed_github_organizations():
    validators = validate_projects.load_default_validators()

    for organization in ("temporal-sa", "temporal-community", "temporalio"):
        document = instruqt_course_document(
            f"https://github.com/{organization}/workshop",
            git_repository_url=f"ssh://git@github.com/{organization}/workshop.git",
        )

        assert validate_projects.validate_document_schema(validators, document) == []


def test_instruqt_schema_rejects_unapproved_canonical_repository():
    document = instruqt_course_document("https://github.com/example/workshop")

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert len(errors) == 1
    assert "schema error at spec.source.repo" in errors[0].message


def test_instruqt_catalog_parent_requires_distinct_matching_variants():
    def course(name, **catalog):
        document = instruqt_course_document("https://github.com/temporal-sa/workshop")
        document.data["metadata"]["name"] = name
        document.data["spec"]["catalog"] = {
            "title": "Durable Ad Campaign Workshop",
            "summary": "Build a durable advertising campaign application.",
            "parent": "durable-ad-campaign",
            **catalog,
        }
        return document

    missing_variant = course("java-workshop")
    assert any(
        "catalog.variant is required" in error.message
        for error in validate_projects.validate_semantics([missing_variant])
    )

    duplicated = [
        course("java-workshop", variant="Java"),
        course("other-java-workshop", variant="Java"),
    ]
    assert any(
        "duplicated" in error.message
        for error in validate_projects.validate_semantics(duplicated)
    )

    mismatched = [
        course("java-workshop", variant="Java", title="Java Workshop"),
        course("python-workshop", variant="Python"),
    ]
    assert any(
        "same title and summary" in error.message
        for error in validate_projects.validate_semantics(mismatched)
    )

    valid = [
        course("java-workshop", variant="Java"),
        course("python-workshop", variant="Python"),
    ]
    assert validate_projects.validate_semantics(valid) == []


def test_instruqt_schema_rejects_unapproved_git_repository_override():
    document = instruqt_course_document(
        "https://github.com/temporal-community/workshop",
        git_repository_url="ssh://git@github.com/example/workshop.git",
    )

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert len(errors) == 1
    assert "schema error at spec.source.gitRepository.url" in errors[0].message


def test_env_input_ref_user_secret_counts_as_secret_usage():
    spec = base_spec()
    spec["secrets"] = {
        "anthropic-credentials": {
            "requiredProperties": ["ANTHROPIC_API_KEY"],
        }
    }
    spec["components"]["app"]["env"] = [
        {
            "name": "MODEL_API_KEY",
            "valueFrom": {
                "inputRef": "secrets.anthropic-credentials.ANTHROPIC_API_KEY",
            },
        }
    ]
    document = project_document(spec)

    assert validate_projects.validate_env_input_refs(document) == []
    assert validate_projects.validate_project_secrets(document) == []


@pytest.mark.parametrize("shared", ["openai", "team/service", "service.name+version=1"])
def test_schema_accepts_platform_shared_secret_references(shared):
    spec = base_spec()
    spec["secrets"] = {"model": {"shared": shared}}
    spec["components"]["app"]["secrets"] = ["model"]
    document = project_document(spec)

    assert validate_projects.validate_document_schema(
        validate_projects.load_default_validators(), document
    ) == []
    assert validate_projects.validate_project_secrets(document) == []


@pytest.mark.parametrize("shared", ["", "/openai", "../openai", "team/../../openai", "team/./key", "team//key", "openai/*", "openai?", True])
def test_schema_rejects_shared_paths_outside_the_shared_prefix(shared):
    spec = base_spec()
    spec["secrets"] = {"model": {"shared": shared}}
    spec["components"]["app"]["secrets"] = ["model"]

    assert validate_projects.validate_document_schema(
        validate_projects.load_default_validators(), project_document(spec)
    )


def test_rollout_version_input_ref_is_allowed():
    spec = base_spec()
    spec["components"]["app"]["env"] = [
        {
            "name": "TEMPORAL_WORKER_BUILD_ID",
            "valueFrom": {"inputRef": "platform.rolloutVersion"},
        }
    ]
    document = project_document(spec)

    assert validate_projects.validate_env_input_refs(document) == []


def test_env_input_ref_requires_declared_user_secret():
    spec = base_spec()
    spec["components"]["app"]["env"] = [
        {
            "name": "MODEL_API_KEY",
            "valueFrom": {
                "inputRef": "secrets.anthropic-credentials.ANTHROPIC_API_KEY",
            },
        }
    ]
    document = project_document(spec)

    errors = validate_projects.validate_project_secrets(document)

    assert [error.message for error in errors] == [
        (
            "components.app.env[0].valueFrom.inputRef secret "
            "'anthropic-credentials' must be declared in top-level secrets"
        )
    ]


def test_temporal_input_ref_cannot_expose_direct_credentials_with_proxy():
    spec = base_spec()
    spec["components"]["app"].update(
        {
            "temporalAccess": True,
            "temporalProxy": {"enabled": True},
            "env": [
                {
                    "name": "TEMPORAL_TOKEN",
                    "valueFrom": {"inputRef": "temporal.apiKey"},
                }
            ],
        }
    )
    document = project_document(spec)

    errors = validate_projects.validate_env_input_refs(document)

    assert [error.message for error in errors] == [
        (
            "components.app.env[0].valueFrom.inputRef 'temporal.apiKey' "
            "cannot be used with temporalProxy"
        )
    ]


def test_mtls_cert_input_refs_require_mtls_auth_mode():
    spec = base_spec()
    spec["components"]["app"]["env"] = [
        {
            "name": "TEMPORAL_CA_PEM",
            "valueFrom": {"inputRef": "temporal.caCert"},
        }
    ]
    document = project_document(spec)

    errors = validate_projects.validate_env_input_refs(document)

    assert [error.message for error in errors] == [
        (
            "components.app.env[0].valueFrom.inputRef 'temporal.caCert' "
            "requires temporal auth mode mtls"
        )
    ]


def test_mtls_cert_input_refs_are_allowed_with_mtls_auth_mode():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["app"]["env"] = [
        {
            "name": "TEMPORAL_CA_PEM",
            "valueFrom": {"inputRef": "temporal.caCert"},
        }
    ]
    document = project_document(spec)

    assert validate_projects.validate_env_input_refs(document) == []


def test_mtls_cert_files_require_mtls_auth_mode():
    spec = base_spec()
    spec["components"]["app"]["temporalMtls"] = {
        "certFiles": {
            "pathEnv": {
                "clientCert": "TEMPORAL_TLS_CERT",
            }
        }
    }
    document = project_document(spec)

    errors = validate_projects.validate_temporal_mtls_cert_files(document)

    assert [error.message for error in errors] == [
        "components.app.temporalMtls.certFiles requires temporal auth mode mtls"
    ]


def test_schema_accepts_mtls_cert_file_path_env_config():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["app"]["temporalMtls"] = {
        "certFiles": {
            "mountPath": "/etc/temporal/certs",
            "paths": {
                "caCert": "/etc/temporal/certs/root-ca.pem",
                "clientCert": "/etc/temporal/certs/client.pem",
                "clientKey": "/etc/temporal/certs/client-key.pem",
            },
            "pathEnv": {
                "certDir": "TEMPORAL_CERT_DIR",
                "caCert": "TEMPORAL_CA_CERT_FILE",
                "clientCert": "TEMPORAL_TLS_CERT",
                "clientKey": "TEMPORAL_TLS_KEY",
            },
        }
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []
    assert validate_projects.validate_temporal_mtls_cert_files(document) == []


def test_mtls_cert_file_paths_must_share_parent_directory():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["app"]["temporalMtls"] = {
        "certFiles": {
            "paths": {
                "caCert": "/etc/temporal/ca.pem",
                "clientCert": "/var/run/temporal/client.pem",
                "clientKey": "/opt/temporal/client-key.pem",
            }
        }
    }
    document = project_document(spec)

    errors = validate_projects.validate_temporal_mtls_cert_files(document)

    assert [error.message for error in errors] == [
        (
            "components.app.temporalMtls.certFiles.paths must share a non-root "
            "parent directory"
        )
    ]


def test_mtls_auth_rejects_temporal_proxy_and_worker_versioning():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["app"]["temporalProxy"] = {"enabled": True}
    spec["components"]["worker"] = {
        "build": "app",
        "worker": {
            "versioning": {
                "enabled": True,
                "deploymentName": "worker",
            }
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_temporal_auth_compatibility(document)

    assert [error.message for error in errors] == [
        (
            "components.app.temporalProxy.enabled cannot be true with temporal "
            "auth mode mtls"
        ),
        (
            "components.worker.worker.versioning.enabled cannot be true with "
            "temporal auth mode mtls"
        ),
    ]


def test_schema_accepts_serverless_lambda_component():
    spec = base_spec()
    spec["components"]["app"] = {
        "build": "app",
        "serverless": {
            "provider": "aws-lambda",
            "packageType": "image",
            "architecture": "x86_64",
            "memoryMb": 512,
            "timeoutSeconds": 600,
            "workerDeployment": {
                "deploymentName": "worker",
                "buildId": "stable-worker-build",
                "env": {
                    "deploymentName": ["WORKER_DEPLOYMENT_NAME"],
                    "buildId": ["WORKER_BUILD_ID"],
                },
            }
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []


def test_serverless_lambda_accepts_secret_env_and_mtls_path_remaps():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["app"] = {
        "build": "app",
        "serverless": {
            "provider": "aws-lambda",
            "packageType": "image",
            "workerDeployment": {
                "deploymentName": "worker",
            },
        },
        "temporalMtls": {
            "certFiles": {
                "pathEnv": {
                    "clientCert": "TEMPORAL_CERT_PATH",
                    "clientKey": "TEMPORAL_KEY_PATH",
                }
            }
        },
        "env": [
            {
                "name": "TEMPORAL_CERT_PEM",
                "valueFrom": {"inputRef": "temporal.clientCert"},
            },
            {
                "name": "BANKING_TOKEN",
                "valueFrom": {"inputRef": "secrets.banking-api.TOKEN"},
            },
        ],
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []
    assert validate_projects.validate_env_input_refs(document) == []
    assert validate_projects.validate_temporal_mtls_cert_files(document) == []
    assert validate_projects.validate_serverless_components(document) == []


def test_schema_rejects_worker_versioning_task_queue_field():
    spec = base_spec()
    spec["components"]["app"] = {
        "build": "app",
        "worker": {
            "versioning": {
                "enabled": True,
                "taskQueue": "app",
            }
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert len(errors) == 1
    assert "schema error at spec.components.app.worker" in errors[0].message
    assert "taskQueue" in errors[0].message


def test_mtls_auth_allows_serverless_lambda_worker_deployment():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["app"] = {
        "build": "app",
        "serverless": {
            "provider": "aws-lambda",
            "workerDeployment": {
                "deploymentName": "worker",
            }
        },
    }
    document = project_document(spec)

    assert validate_projects.validate_temporal_auth_compatibility(document) == []
    assert validate_projects.validate_serverless_components(document) == []


def test_serverless_lambda_rejects_worker_controller_versioning():
    spec = base_spec()
    spec["components"]["app"] = {
        "build": "app",
        "serverless": {"provider": "aws-lambda"},
        "worker": {
            "versioning": {
                "enabled": True,
                "deploymentName": "worker",
            }
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_serverless_components(document)

    assert [error.message for error in errors] == [
        (
            "components.app.worker is not supported for serverless aws-lambda "
            "components; configure the Temporal Worker Deployment under "
            "serverless.workerDeployment"
        )
    ]


def test_serverless_lambda_rejects_worker_deployment_env_name_reuse():
    spec = base_spec()
    spec["components"]["app"] = {
        "build": "app",
        "serverless": {
            "provider": "aws-lambda",
            "workerDeployment": {
                "env": {
                    "deploymentName": ["TEMPORAL_WORKER_VERSION"],
                    "buildId": ["TEMPORAL_WORKER_VERSION"],
                }
            },
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_serverless_components(document)

    assert [error.message for error in errors] == [
        (
            "components.app.serverless.workerDeployment.env cannot map "
            "deploymentName and buildId to the same environment variable(s): "
            "TEMPORAL_WORKER_VERSION"
        )
    ]


def test_schema_allows_temporal_search_attributes():
    spec = base_spec()
    spec["temporal"] = {
        "searchAttributes": {
            "CustomerId": {},
            "CartTotal": {"type": "double"},
            "FulfillmentTags": {"type": "keywordList"},
        }
    }
    document = project_document(spec)
    validator = validate_projects.load_default_validators()["DemoProject"]

    assert validate_projects.validate_schema(validator, document) == []


def test_schema_rejects_unsupported_temporal_search_attribute_type():
    spec = base_spec()
    spec["temporal"] = {
        "searchAttributes": {
            "CustomerId": {"type": "unsupported"},
        }
    }
    document = project_document(spec)
    validator = validate_projects.load_default_validators()["DemoProject"]

    errors = validate_projects.validate_schema(validator, document)

    assert len(errors) == 1
    assert "unsupported" in errors[0].message


def test_serverless_lambda_component_rejects_kubernetes_and_buildpack_shape():
    spec = base_spec()
    spec["builds"]["app"] = {
        "context": ".",
        "strategy": "buildpack",
        "builder": "paketobuildpacks/builder-jammy-base",
    }
    spec["components"]["app"] = {
        "build": "app",
        "serverless": {"provider": "aws-lambda"},
        "ports": [{"containerPort": 8000}],
    }
    document = project_document(spec)

    errors = validate_projects.validate_serverless_components(document)

    assert [error.message for error in errors] == [
        (
            "components.app.build must reference a dockerfile build for "
            "serverless aws-lambda"
        ),
        "components.app.ports is not supported for serverless aws-lambda components",
    ]


def test_serverless_lambda_rejects_custom_mtls_file_paths():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["app"] = {
        "build": "app",
        "serverless": {"provider": "aws-lambda"},
        "temporalMtls": {
            "certFiles": {
                "mountPath": "/etc/temporal",
                "paths": {
                    "clientCert": "/etc/temporal/client.pem",
                    "clientKey": "/etc/temporal/client-key.pem",
                },
                "pathEnv": {
                    "clientCert": "TEMPORAL_CERT_PATH",
                    "clientKey": "TEMPORAL_KEY_PATH",
                },
            }
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_serverless_components(document)

    assert [error.message for error in errors] == [
        (
            "components.app.temporalMtls.certFiles.mountPath is not supported "
            "for serverless aws-lambda components; Lambda cert files are "
            "materialized under /tmp"
        ),
        (
            "components.app.temporalMtls.certFiles.paths is not supported for "
            "serverless aws-lambda components; Lambda cert files are "
            "materialized under /tmp"
        ),
    ]


def test_schema_accepts_temporal_region():
    for region in ("aws-ca-central-1", "gcp-us-central1", "azure-centralus"):
        spec = base_spec()
        spec["temporal"] = {"region": region}
        document = project_document(spec)

        errors = validate_projects.validate_document_schema(
            validate_projects.load_default_validators(),
            document,
        )

        assert errors == [], f"region {region!r} should be accepted: {errors}"


def test_schema_rejects_invalid_temporal_region():
    spec = base_spec()
    spec["temporal"] = {"region": "us-central1"}
    document = project_document(spec)
    validator = validate_projects.load_default_validators()["DemoProject"]

    errors = validate_projects.validate_schema(validator, document)

    assert len(errors) == 1
    assert "us-central1" in errors[0].message


def test_schema_allows_temporal_region_omitted():
    document = project_document(base_spec())

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []


def test_schema_accepts_registry_canonical_flag():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "canonical": True,
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []


def test_schema_accepts_registry_ai_sdks():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "temporalSdks": ["Python"],
        "aiSdks": ["Google ADK", "Anthropic SDK"],
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []


def test_registry_ai_sdks_must_be_unique():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "aiSdks": ["Google ADK", "google adk"],
    }
    document = project_document(spec)

    errors = validate_projects.validate_registry(document)

    assert any("registry.aiSdks" in error.message for error in errors)


def test_schema_rejects_non_boolean_registry_canonical():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "canonical": "yes",
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert any(
        "canonical" in error.message for error in errors
    ), [error.message for error in errors]


def test_schema_accepts_named_registry_link_slots():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "links": {
            "speakerNotes": {"url": "https://example.com/notes"},
            "slideDeck": {"url": "https://example.com/slides"},
            "runbook": {"url": "https://example.com/runbook"},
            "goldenPath": {"url": "https://example.com/recording"},
            "externalRecording": {"url": "https://example.com/external"},
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []


def test_schema_accepts_registry_additional_links():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "links": {
            "additional": [
                {
                    "label": "Architecture diagram",
                    "url": "https://example.com/arch.png",
                    "audience": "technical",
                },
                {
                    "label": "FAQ",
                    "url": "https://example.com/faq",
                    "audience": "nonTechnical",
                },
            ],
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []


def test_schema_rejects_unknown_registry_link_slot():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "links": {"randomLink": {"url": "https://example.com"}},
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert any(
        "randomLink" in error.message for error in errors
    ), [error.message for error in errors]


def test_schema_rejects_invalid_additional_link_audience():
    spec = base_spec()
    spec["registry"] = {
        "title": "Demo",
        "summary": "Summary.",
        "links": {
            "additional": [
                {
                    "label": "Item",
                    "url": "https://example.com",
                    "audience": "internal",
                }
            ],
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert any(
        "audience" in error.message or "internal" in error.message
        for error in errors
    ), [error.message for error in errors]


# --- multi-namespace and Nexus endpoints ------------------------------------


def namespace_spec(**overrides) -> dict:
    spec = base_spec()
    spec["temporal"] = {"namespaces": {"alpha": {}}}
    spec["components"] = {
        "app": {"build": "app", "worker": True, "temporalNamespace": "alpha"},
    }
    spec.update(overrides)
    return spec


def messages(errors) -> list[str]:
    return [error.message for error in errors]


def test_declared_namespace_and_binding_validate():
    document = project_document(namespace_spec())
    assert validate_projects.validate_temporal_namespace_labels(document) == []
    assert validate_projects.validate_component_temporal_namespace_refs(document) == []


def test_reserved_namespace_labels_are_rejected():
    for label in ("candidate", "ca-candidate"):
        spec = base_spec()
        spec["temporal"] = {"namespaces": {label: {}}}
        errors = validate_projects.validate_temporal_namespace_labels(
            project_document(spec)
        )
        assert any("reserved name" in message for message in messages(errors)), label


def test_component_must_reference_a_declared_namespace():
    spec = namespace_spec()
    spec["components"]["app"]["temporalNamespace"] = "beta"
    errors = validate_projects.validate_component_temporal_namespace_refs(
        project_document(spec)
    )
    assert (
        "components.app.temporalNamespace 'beta' must match a declared "
        "temporal.namespaces entry"
    ) in messages(errors)


def test_declared_namespace_must_be_referenced():
    spec = base_spec()
    spec["temporal"] = {"namespaces": {"alpha": {}}}
    errors = validate_projects.validate_component_temporal_namespace_refs(
        project_document(spec)
    )
    assert (
        "temporal.namespaces.alpha must be referenced by at least one component "
        "or Nexus endpoint"
    ) in messages(errors)


def test_namespace_referenced_only_by_an_endpoint_is_not_orphaned():
    spec = base_spec()
    spec["temporal"] = {
        "namespaces": {"alpha": {}},
        "nexusEndpoints": {
            "orders": {
                "targetNamespace": "alpha",
                "targetTaskQueue": "orders-tq",
                "allowedCallerNamespaces": ["primary"],
            }
        },
    }
    assert (
        validate_projects.validate_component_temporal_namespace_refs(
            project_document(spec)
        )
        == []
    )


def test_binding_is_rejected_on_lambda_and_proxy_components():
    spec = namespace_spec()
    spec["components"]["lambda"] = {
        "build": "app",
        "temporalNamespace": "alpha",
        "serverless": {"provider": "aws-lambda"},
    }
    spec["components"]["proxied"] = {
        "build": "app",
        "temporalNamespace": "alpha",
        "temporalProxy": {"enabled": True},
    }
    rendered = messages(
        validate_projects.validate_component_temporal_namespace_refs(
            project_document(spec)
        )
    )
    assert (
        "components.lambda.temporalNamespace is not supported for serverless "
        "aws-lambda components"
    ) in rendered
    assert (
        "components.proxied.temporalNamespace cannot be used with temporalProxy"
    ) in rendered


def endpoint_spec(**endpoint) -> dict:
    spec = namespace_spec()
    base = {
        "targetNamespace": "alpha",
        "targetTaskQueue": "orders-tq",
        "allowedCallerNamespaces": ["primary"],
    }
    base.update(endpoint)
    spec["temporal"]["nexusEndpoints"] = {"orders": base}
    return spec


def test_valid_nexus_endpoint_passes():
    assert validate_projects.validate_nexus_endpoints(
        project_document(endpoint_spec())
    ) == []


def test_nexus_endpoint_target_must_be_primary_or_declared():
    errors = validate_projects.validate_nexus_endpoints(
        project_document(endpoint_spec(targetNamespace="beta"))
    )
    assert any("targetNamespace 'beta'" in message for message in messages(errors))


def test_nexus_endpoint_target_may_be_primary():
    assert validate_projects.validate_nexus_endpoints(
        project_document(endpoint_spec(targetNamespace="primary"))
    ) == []


def test_nexus_endpoint_requires_a_task_queue():
    errors = validate_projects.validate_nexus_endpoints(
        project_document(endpoint_spec(targetTaskQueue="  "))
    )
    assert any("targetTaskQueue must be" in message for message in messages(errors))


def test_nexus_endpoint_requires_a_non_empty_caller_allowlist():
    # Temporal admits no callers by default, so an empty list is unreachable.
    errors = validate_projects.validate_nexus_endpoints(
        project_document(endpoint_spec(allowedCallerNamespaces=[]))
    )
    assert any(
        "must allow at least one caller namespace or caller project" in message
        for message in messages(errors)
    )


def test_nexus_endpoint_rejects_unknown_and_duplicate_callers():
    errors = messages(
        validate_projects.validate_nexus_endpoints(
            project_document(
                endpoint_spec(allowedCallerNamespaces=["primary", "primary", "beta"])
            )
        )
    )
    assert any("duplicates allowedCallerNamespaces[0]" in message for message in errors)
    assert any("[2] 'beta' must be" in message for message in errors)


def test_nexus_endpoint_name_must_start_with_a_letter():
    spec = namespace_spec()
    spec["temporal"]["nexusEndpoints"] = {
        "9orders": {
            "targetNamespace": "alpha",
            "targetTaskQueue": "orders-tq",
            "allowedCallerNamespaces": ["primary"],
        }
    }
    errors = validate_projects.validate_nexus_endpoints(project_document(spec))
    assert any("must start with a letter" in message for message in messages(errors))


def test_nexus_endpoint_accepts_another_demo_by_project_name():
    spec = endpoint_spec()
    del spec["temporal"]["nexusEndpoints"]["orders"]["allowedCallerNamespaces"]
    spec["temporal"]["nexusEndpoints"]["orders"]["allowedCallerProjects"] = [
        "temporal-podpocalypse"
    ]
    caller = validate_projects.ProjectDocument(
        path=ROOT / "projects/caller.yaml",
        data={
            "kind": "DemoProject",
            "metadata": {"name": "temporal-podpocalypse"},
            "spec": {"components": {}},
        },
    )
    document = project_document(spec)
    assert validate_projects.validate_nexus_endpoints(document) == []
    assert validate_projects.validate_nexus_caller_projects([document, caller]) == []


def test_nexus_endpoint_rejects_a_resolved_namespace_and_itself():
    rendered = messages(
        validate_projects.validate_nexus_endpoints(
            project_document(
                endpoint_spec(
                    allowedCallerProjects=[
                        "tmprl-dem-cld-other",
                        "env-inputs",
                    ]
                )
            )
        )
    )
    assert any("not a resolved namespace" in message for message in rendered)
    assert any("is this project" in message for message in rendered)


def test_nexus_endpoint_rejects_an_unknown_caller_project():
    document = project_document(
        endpoint_spec(allowedCallerProjects=["missing-demo"])
    )
    errors = validate_projects.validate_nexus_caller_projects([document])
    assert any(
        "must name another DemoProject in the registry" in message
        for message in messages(errors)
    )


def test_nexus_endpoint_names_are_unique_across_the_registry():
    first = project_document(endpoint_spec())
    second = validate_projects.ProjectDocument(
        path=ROOT / "projects/other.yaml", data=first.data
    )
    errors = validate_projects.validate_unique_nexus_endpoint_names([first, second])
    assert any(
        "duplicate Nexus endpoint name 'orders'" in message
        for message in messages(errors)
    )
    assert any("unique account-wide" in message for message in messages(errors))


def test_declared_namespaces_join_the_temporal_uniqueness_map():
    # The label-derived name is checked too, not just the project's own -- the
    # Temporal name truncates at 39 chars where the Kubernetes one truncates at 63.
    spec = base_spec()
    spec["temporal"] = {"namespaces": {"alpha": {}}}
    first = validate_projects.ProjectDocument(
        path=ROOT / "projects/first.yaml",
        data={"kind": "DemoProject", "metadata": {"name": "demo"}, "spec": spec},
    )
    second = validate_projects.ProjectDocument(
        path=ROOT / "projects/second.yaml", data=first.data
    )
    errors = messages(
        validate_projects.validate_unique_derived_namespaces([first, second])
    )
    assert any(
        "duplicate derived Temporal namespace 'tmprl-dem-cld-demo-alpha'" in message
        and "temporal.namespaces.alpha" in message
        for message in errors
    )
    assert any(
        "duplicate derived Temporal namespace 'tmprl-dem-cld-demo'" in message
        for message in errors
    )


def test_temporal_namespace_cannot_be_used_with_worker_versioning():
    # The Worker Controller's Connection and WorkerDeployment resolve the project's
    # primary namespace and credentials, and neither is label-aware.
    spec = base_spec()
    spec["temporal"] = {"namespaces": {"alpha": {}}}
    spec["components"]["app"].update(
        {
            "worker": {"versioning": {"enabled": True}},
            "temporalNamespace": "alpha",
        }
    )
    document = project_document(spec)

    errors = validate_projects.validate_component_temporal_namespace_refs(document)

    assert [error.message for error in errors] == [
        "components.app.temporalNamespace cannot be used with worker.versioning"
    ]


def test_temporal_namespace_is_allowed_on_an_unversioned_worker():
    spec = base_spec()
    spec["temporal"] = {"namespaces": {"alpha": {}}}
    spec["components"]["app"].update({"worker": True, "temporalNamespace": "alpha"})
    document = project_document(spec)

    assert validate_projects.validate_component_temporal_namespace_refs(document) == []


def _isolated_spec() -> dict:
    spec = base_spec()
    spec["builds"]["worker"] = {"context": ".", "dockerfile": "Dockerfile"}
    spec["components"]["orders"] = {
        "build": "worker",
        "serverless": {
            "provider": "aws-lambda",
            "workerDeployment": {"deploymentName": "orders", "buildId": "v1"},
        },
    }
    spec["registry"] = {
        "isolatedLambda": {
            "enabled": True,
            "taskQueueEnv": "TEMPORAL_TASK_QUEUE",
            "baseTaskQueue": "orders",
        }
    }
    return spec


def test_isolated_lambda_rejects_being_enabled_with_crashable():
    spec = _isolated_spec()
    spec["registry"]["crashable"] = {
        "enabled": True,
        "taskQueueEnv": "TEMPORAL_TASK_QUEUE",
    }
    errors = validate_projects.validate_isolated_lambda(project_document(spec))
    assert any("cannot both be enabled" in error.message for error in errors)


def test_isolated_lambda_requires_task_queue_env_and_a_serverless_component():
    spec = _isolated_spec()
    del spec["registry"]["isolatedLambda"]["taskQueueEnv"]
    errors = validate_projects.validate_isolated_lambda(project_document(spec))
    assert any("taskQueueEnv is required" in error.message for error in errors)

    spec = base_spec()
    spec["registry"] = {"isolatedLambda": {"enabled": True, "taskQueueEnv": "TQ"}}
    errors = validate_projects.validate_isolated_lambda(project_document(spec))
    assert any("serverless aws-lambda" in error.message for error in errors)


def test_initial_current_is_unique_per_worker_deployment():
    spec = _isolated_spec()
    spec["components"]["orders"]["serverless"]["workerDeployment"]["initialCurrent"] = True
    spec["components"]["orders-b"] = {
        "build": "worker",
        "serverless": {
            "provider": "aws-lambda",
            "workerDeployment": {
                "deploymentName": "orders",
                "buildId": "v2",
                "initialCurrent": True,
            },
        },
    }
    errors = validate_projects.validate_isolated_lambda(project_document(spec))
    assert any("initialCurrent" in error.message for error in errors)

    spec["components"]["orders-b"]["serverless"]["workerDeployment"]["initialCurrent"] = False
    assert validate_projects.validate_isolated_lambda(project_document(spec)) == []


def test_demo_project_accepts_allowed_github_organizations():
    validators = validate_projects.load_default_validators()

    for organization in ("temporal-sa", "temporal-community", "temporalio"):
        spec = base_spec()
        spec["source"]["repo"] = f"https://github.com/{organization}/demo"
        document = project_document(spec)

        assert validate_projects.validate_document_schema(validators, document) == []
        assert validate_projects.validate_source_repo(document) == []


def test_demo_project_rejects_other_github_organizations():
    spec = base_spec()
    spec["source"]["repo"] = "https://github.com/example/demo"
    document = project_document(spec)

    schema_errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(), document
    )

    assert [error.pointer for error in schema_errors] == ["/spec/source/repo"]
    assert len(validate_projects.validate_source_repo(document)) == 1


def candidate_errors(candidate: dict, existing=()):
    validators = validate_projects.load_default_validators()
    errors = validate_projects.validate_candidate(
        candidate,
        list(existing),
        path=ROOT / "projects/demo/candidate.yaml",
        validators=validators,
    )
    fields = validate_projects.spec_fields_for_kind(validators, candidate.get("kind"))
    return [
        (validate_projects.error_pointer(error, fields), error.message)
        for error in errors
    ]


def test_validate_candidate_maps_semantic_errors_to_pointers():
    candidate = project_document(base_spec()).data
    candidate["spec"]["components"]["app"]["build"] = "missing"

    errors = candidate_errors(candidate)

    assert [pointer for pointer, _ in errors] == ["/spec/components/app/build"]


def test_validate_candidate_reports_collisions_with_existing_projects():
    existing = project_document(base_spec())
    candidate = project_document(base_spec()).data

    pointers = {pointer for pointer, _ in candidate_errors(candidate, [existing])}

    assert pointers == {"/metadata/name", "/spec/ingress/subdomain"}


def test_validate_candidate_ignores_errors_in_existing_projects():
    broken = project_document({"source": {"repo": "not a url"}})
    candidate = project_document(base_spec()).data
    candidate["metadata"]["name"] = "fresh"
    candidate["spec"]["ingress"]["subdomain"] = "fresh"

    assert candidate_errors(candidate, [broken]) == []


def test_error_pointer_leaves_unscoped_messages_without_a_pointer():
    error = validate_projects.ProjectError(
        ROOT / "projects/demo/candidate.yaml",
        "service name 'app' is used by multiple components: a, b",
    )

    assert validate_projects.error_pointer(error, frozenset({"components"})) is None


def test_instruqt_schema_accepts_presenter_links():
    document = instruqt_course_document("https://github.com/temporal-sa/workshop")
    document.data["spec"]["catalog"] = {
        "title": "Workshop",
        "summary": "A workshop.",
        "links": {
            "speakerNotes": {"url": "https://docs.google.com/document/d/notes"},
            "slideDeck": {"url": "https://docs.google.com/presentation/d/deck"},
            "runbook": {"label": "Facilitator runbook", "url": "https://example.com/run"},
            "goldenPath": {"url": "https://drive.google.com/file/d/golden"},
            "externalRecording": {"url": "https://www.youtube.com/watch?v=abc"},
            "additional": [
                {
                    "label": "Customer one-pager",
                    "url": "https://example.com/one-pager",
                    "audience": "nonTechnical",
                }
            ],
        },
    }
    validators = validate_projects.load_default_validators()

    assert validate_projects.validate_document_schema(validators, document) == []

    links = document.data["spec"]["catalog"]["links"]
    links["slideDeck"] = {"url": "http://example.com/deck"}
    links["recording"] = {"url": "https://example.com/recording"}
    messages = [
        error.message
        for error in validate_projects.validate_document_schema(validators, document)
    ]
    assert any("spec.catalog.links.slideDeck.url" in message for message in messages)
    assert any("'recording' was unexpected" in message for message in messages)


def test_instruqt_schema_accepts_ai_sdks_and_duration():
    document = instruqt_course_document("https://github.com/temporal-sa/workshop")
    document.data["spec"]["catalog"] = {
        "title": "Workshop",
        "summary": "A workshop.",
        "aiSdks": ["OpenAI Agents SDK", "LangGraph"],
        "duration": {"minHours": 1.5, "maxHours": 4},
    }
    validators = validate_projects.load_default_validators()

    assert validate_projects.validate_document_schema(validators, document) == []
    assert validate_projects.validate_semantics([document]) == []

    catalog = document.data["spec"]["catalog"]
    catalog["duration"] = {"minHours": 0, "maxHours": 2, "hours": 2}
    messages = [
        error.message
        for error in validate_projects.validate_document_schema(validators, document)
    ]
    assert any("spec.catalog.duration.minHours" in message for message in messages)
    assert any("'hours' was unexpected" in message for message in messages)

    catalog["duration"] = {"minHours": 2}
    messages = [
        error.message
        for error in validate_projects.validate_document_schema(validators, document)
    ]
    assert any("'maxHours' is a required property" in message for message in messages)


def test_instruqt_catalog_duration_min_must_not_exceed_max():
    document = instruqt_course_document("https://github.com/temporal-sa/workshop")
    document.data["spec"]["catalog"] = {
        "title": "Workshop",
        "summary": "A workshop.",
        "duration": {"minHours": 4, "maxHours": 2},
    }
    assert any(
        "minHours must not be greater than" in error.message
        for error in validate_projects.validate_semantics([document])
    )

    document.data["spec"]["catalog"]["duration"] = {"minHours": 2, "maxHours": 2}
    assert validate_projects.validate_semantics([document]) == []


def test_instruqt_course_crd_keeps_every_catalog_field():
    # The InstruqtCourse CRD is structural, so the API server drops any catalog
    # field it doesn't declare even though the validator accepts it.
    import json

    import yaml

    schema = json.loads((ROOT / "schemas/instruqt-course.schema.json").read_text())
    catalog = schema["properties"]["spec"]["properties"]["catalog"]
    schema_catalog = schema["$defs"][catalog["$ref"].rsplit("/", 1)[-1]]["properties"]
    schema_links = schema["$defs"]["catalogLinks"]["properties"]
    schema_duration = schema["$defs"]["catalogDuration"]["properties"]

    for color in ("blue", "green"):
        crd = yaml.safe_load(
            (
                ROOT
                / f"clusters/{color}/bootstrap/crds/instruqtcourses.registry.tmprl-demo.cloud.yaml"
            ).read_text()
        )
        spec = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        crd_catalog = spec["properties"]["catalog"]["properties"]
        assert set(crd_catalog) == set(schema_catalog), color
        assert set(crd_catalog["links"]["properties"]) == set(schema_links), color
        assert set(crd_catalog["duration"]["properties"]) == set(schema_duration), color


def autoscaling_spec(**autoscaling) -> dict:
    spec = base_spec()
    spec["components"]["worker"] = {
        "build": "app",
        "worker": True,
        "autoscaling": {"taskQueue": "orders", **autoscaling},
    }
    return spec


def test_schema_accepts_autoscaling_and_resources():
    spec = autoscaling_spec(minReplicas=0, maxReplicas=5, queueTypes=["activity"])
    spec["components"]["worker"]["resources"] = {
        "requests": {"cpu": "250m", "memory": "512Mi"},
        "limits": {"memory": "1Gi"},
    }

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(), project_document(spec)
    )

    assert errors == []
    assert validate_projects.validate_autoscaling(project_document(spec)) == []


def test_schema_rejects_autoscaling_above_cap():
    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        project_document(autoscaling_spec(maxReplicas=50)),
    )

    assert errors


def test_autoscaling_semantic_rules():
    spec = autoscaling_spec(minReplicas=4, maxReplicas=2)
    spec["components"]["worker"]["replicas"] = 1
    spec["components"]["app"]["autoscaling"] = {"taskQueue": "orders"}
    spec["components"]["noqueue"] = {
        "build": "app",
        "worker": True,
        "autoscaling": {"maxReplicas": 3},
    }

    messages = [
        error.message
        for error in validate_projects.validate_autoscaling(project_document(spec))
    ]

    assert "components.app.autoscaling requires worker" in messages
    assert any(
        message.startswith("components.worker.autoscaling cannot be combined")
        for message in messages
    )
    assert (
        "components.worker.autoscaling minReplicas (4) cannot exceed maxReplicas (2)"
        in messages
    )
    assert any(
        message.startswith("components.noqueue.autoscaling requires taskQueue")
        for message in messages
    )


def test_autoscaling_requires_api_key_auth_and_no_proxy():
    spec = autoscaling_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
    spec["components"]["worker"]["temporalProxy"] = {"enabled": True}

    messages = [
        error.message
        for error in validate_projects.validate_autoscaling(project_document(spec))
    ]

    assert messages == [
        "components.worker.autoscaling cannot be used with temporalProxy",
        "components.worker.autoscaling requires temporal auth mode apiKey",
    ]


def test_autoscaling_task_queue_falls_back_to_crashable_base_queue():
    spec = autoscaling_spec()
    del spec["components"]["worker"]["autoscaling"]["taskQueue"]
    spec["registry"] = {
        "crashable": {
            "enabled": True,
            "taskQueueEnv": "ORDERS_QUEUE",
            "baseTaskQueue": "orders",
        }
    }

    assert validate_projects.validate_autoscaling(project_document(spec)) == []


def test_disabled_autoscaling_needs_no_task_queue():
    spec = autoscaling_spec(enabled=False)
    del spec["components"]["worker"]["autoscaling"]["taskQueue"]

    assert validate_projects.validate_autoscaling(project_document(spec)) == []


def test_reviewed_projects_cannot_claim_test_deploy_names_or_metadata():
    document = project_document(base_spec())
    document.data["metadata"] = {
        "name": "pv-demo",
        "labels": {"registry.tmprl-demo.cloud/preview": "true"},
        "annotations": {
            "registry.tmprl-demo.cloud/secrets-from": "other-demo",
            "registry.tmprl-demo.cloud/force-reconcile": "ok",
        },
    }
    document.data["spec"]["ingress"] = {"subdomain": "pv-demo"}

    errors = validate_projects.validate_reserved_metadata(document)
    pointers = {
        validate_projects.error_pointer(error, frozenset({"ingress"}))
        for error in errors
    }

    assert len(errors) == 4
    assert pointers == {
        "/metadata/name",
        "/metadata/labels",
        "/metadata/annotations",
        "/spec/ingress/subdomain",
    }


def test_ordinary_project_metadata_is_not_reserved():
    document = project_document(base_spec())
    document.data["metadata"]["annotations"] = {
        "registry.tmprl-demo.cloud/force-reconcile": "ok"
    }

    assert validate_projects.validate_reserved_metadata(document) == []


def public_route_spec(*routes: dict) -> dict:
    spec = base_spec()
    spec["components"]["app"]["ports"] = [{"containerPort": 8080, "servicePort": 80}]
    spec["ingress"] = {
        "subdomain": "env-inputs",
        "temporalAuthRequired": True,
        "routes": list(routes),
    }
    return spec


def test_public_route_with_path_prefix_validates():
    document = project_document(
        public_route_spec(
            {"pathPrefix": "/webhooks/github", "component": "app", "port": "http", "public": True},
            {"pathPrefix": "/", "component": "app", "port": "http"},
        )
    )

    validator = validate_projects.load_default_validators()["DemoProject"]
    assert validate_projects.validate_schema(validator, document) == []
    assert validate_projects.validate_ingress_routes(document) == []


@pytest.mark.parametrize("route", [{}, {"pathPrefix": "/"}], ids=["catch-all", "root"])
def test_public_route_must_not_cover_the_whole_host(route):
    document = project_document(
        public_route_spec({**route, "component": "app", "port": "http", "public": True})
    )

    assert messages(validate_projects.validate_ingress_routes(document)) == [
        "ingress.routes[0] is public and needs a pathPrefix other than '/'"
    ]


def test_public_is_only_allowed_on_listed_routes():
    spec = base_spec()
    spec["components"]["app"]["ports"] = [{"containerPort": 8080, "servicePort": 80}]
    spec["ingress"] = {
        "subdomain": "env-inputs",
        "component": "app",
        "port": "http",
        "public": True,
    }
    validator = validate_projects.load_default_validators()["DemoProject"]

    errors = validate_projects.validate_schema(validator, project_document(spec))

    assert any("public" in error.message for error in errors)


def test_duplicate_yaml_keys_are_rejected(tmp_path):
    path = tmp_path / "duplicate.yaml"
    path.write_text(
        "apiVersion: registry.tmprl-demo.cloud/v1alpha1\n"
        "kind: DemoProject\n"
        "metadata:\n"
        "  name: duplicate\n"
        "spec:\n"
        "  components:\n"
        "    app: {build: app}\n"
        "  components:\n"
        "    web: {build: app}\n"
    )

    document, errors = validate_projects.load_yaml_document(path)

    assert document is None
    assert len(errors) == 1
    assert "found duplicate key 'components'" in errors[0].message


def test_yaml_merge_keys_are_not_duplicates(tmp_path):
    path = tmp_path / "merge.yaml"
    path.write_text(
        "base: &base {a: 1}\n"
        "merged:\n"
        "  <<: *base\n"
        "  b: 2\n"
    )

    document, errors = validate_projects.load_yaml_document(path)

    assert errors == []
    assert document == {"base": {"a": 1}, "merged": {"a": 1, "b": 2}}
