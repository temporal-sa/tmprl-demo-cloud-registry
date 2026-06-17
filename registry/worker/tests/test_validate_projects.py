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


def test_mtls_cert_input_refs_are_reserved_until_mtls_secrets_exist():
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
            "is reserved until Temporal mTLS runtime secrets are implemented"
        )
    ]
