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
        "allowedCallerNamespaces must list at least one namespace" in message
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
