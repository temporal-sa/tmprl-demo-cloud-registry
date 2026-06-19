from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


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
        },
        "worker": {
            "versioning": {
                "enabled": True,
                "deploymentName": "worker",
            }
        },
    }
    document = project_document(spec)

    errors = validate_projects.validate_document_schema(
        validate_projects.load_default_validators(),
        document,
    )

    assert errors == []


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


def test_mtls_auth_allows_serverless_lambda_worker_versioning():
    spec = base_spec()
    spec["temporal"] = {"auth": {"mode": "mtls"}}
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

    assert validate_projects.validate_temporal_auth_compatibility(document) == []
    assert validate_projects.validate_serverless_components(document) == []


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
        "env": [
            {
                "name": "TEMPORAL_TOKEN",
                "valueFrom": {"inputRef": "temporal.apiKey"},
            }
        ],
    }
    document = project_document(spec)

    errors = validate_projects.validate_serverless_components(document)

    assert [error.message for error in errors] == [
        (
            "components.app.build must reference a dockerfile build for "
            "serverless aws-lambda"
        ),
        (
            "components.app.serverless aws-lambda requires "
            "worker.versioning.enabled true"
        ),
        "components.app.ports is not supported for serverless aws-lambda components",
        (
            "components.app.env[0].valueFrom is not supported for serverless "
            "aws-lambda components"
        ),
    ]
