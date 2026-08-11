#!/usr/bin/env python3
"""Validate platform project configs.

This script validates every project YAML document against the JSON Schema and
then runs cross-file and cross-field checks that JSON Schema cannot express
cleanly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEMO_PROJECT_SCHEMA = ROOT / "schemas" / "project.schema.json"
DEFAULT_INSTRUQT_COURSE_SCHEMA = ROOT / "schemas" / "instruqt-course.schema.json"
DEFAULT_SCHEMAS = {
    "DemoProject": DEFAULT_DEMO_PROJECT_SCHEMA,
    "InstruqtCourse": DEFAULT_INSTRUQT_COURSE_SCHEMA,
}
DEFAULT_PROJECT_GLOB = "projects/**/*.yaml"
NAMESPACE_PREFIX = "tmprl-dem-cld-"
TEMPORAL_NAMESPACE_PREFIX = "tmprl-dem-cld-"
# Temporal Cloud namespace names (before the account suffix) cap at 39 chars.
TEMPORAL_NAMESPACE_MAX_LENGTH = 39
TEMPORAL_SA_REPO_PATH = re.compile(r"^/temporal-sa/[A-Za-z0-9_.-]+/?$")
DNS_LABEL_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
ENV_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")
TEMPORAL_AUTH_MODE_API_KEY = "apiKey"
TEMPORAL_AUTH_MODE_MTLS = "mtls"
SERVERLESS_PROVIDER_AWS_LAMBDA = "aws-lambda"
# Both collide with the credential-rotation staging paths
# tmprl-dem-cld/<project>/temporal-<suffix>, so no namespace may claim them.
RESERVED_TEMPORAL_NAMESPACE_LABELS = frozenset({"candidate", "ca-candidate"})
NEXUS_PRIMARY_NAMESPACE_REF = "primary"
NEXUS_ENDPOINT_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9]$")
SERVERLESS_PACKAGE_TYPE_IMAGE = "image"
TEMPORAL_API_KEY_INPUT_REFS = {
    "temporal.address",
    "temporal.namespace",
    "temporal.apiKey",
    "temporal.tls",
}
TEMPORAL_MTLS_INPUT_REFS = {
    "temporal.caCert",
    "temporal.clientCert",
    "temporal.clientKey",
}
TEMPORAL_INPUT_REFS = TEMPORAL_API_KEY_INPUT_REFS | TEMPORAL_MTLS_INPUT_REFS
TEMPORAL_MTLS_CERT_FILES_MOUNT_PATH = "/var/run/secrets/tmprl-demo.cloud/temporal"
TEMPORAL_MTLS_CERT_FILES = {
    "caCert": "ca.crt",
    "clientCert": "tls.crt",
    "clientKey": "tls.key",
}


@dataclass(frozen=True)
class ProjectDocument:
    path: Path
    data: dict[str, Any]


@dataclass(frozen=True)
class ProjectError:
    path: Path
    message: str

    def render(self) -> str:
        return f"{relative_path(self.path)}: {self.message}"


def relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def load_yaml_document(path: Path) -> tuple[dict[str, Any] | None, list[ProjectError]]:
    try:
        with path.open("r", encoding="utf-8") as file:
            documents = list(yaml.safe_load_all(file))
    except yaml.YAMLError as error:
        return None, [ProjectError(path, f"YAML parse error: {error}")]
    except OSError as error:
        return None, [ProjectError(path, f"cannot read file: {error}")]

    if len(documents) != 1:
        return None, [ProjectError(path, "must contain exactly one YAML document")]

    document = documents[0]
    if not isinstance(document, dict):
        return None, [ProjectError(path, "top-level YAML document must be an object")]

    return document, []


def load_schema(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        schema = json.load(file)

    Draft202012Validator.check_schema(schema)
    return schema


def load_default_validators() -> dict[str, Draft202012Validator]:
    return {
        kind: Draft202012Validator(load_schema(path.resolve()))
        for kind, path in DEFAULT_SCHEMAS.items()
    }


def format_error_path(error: ValidationError) -> str:
    path = ".".join(str(part) for part in error.absolute_path)
    return path if path else "<root>"


def validate_schema(
    validator: Draft202012Validator, document: ProjectDocument
) -> list[ProjectError]:
    schema_errors = sorted(
        validator.iter_errors(document.data),
        key=lambda error: [str(part) for part in error.absolute_path],
    )
    return [
        ProjectError(
            document.path,
            f"schema error at {format_error_path(error)}: {error.message}",
        )
        for error in schema_errors
    ]


def validate_document_schema(
    validators: dict[str, Draft202012Validator],
    document: ProjectDocument,
    *,
    fallback_validator: Draft202012Validator | None = None,
) -> list[ProjectError]:
    if fallback_validator is not None:
        return validate_schema(fallback_validator, document)

    kind = document.data.get("kind")
    if not isinstance(kind, str):
        return [ProjectError(document.path, "kind is required")]
    validator = validators.get(kind)
    if validator is None:
        supported = ", ".join(sorted(validators))
        return [
            ProjectError(
                document.path,
                f"unsupported kind {kind!r}; supported kinds: {supported}",
            )
        ]
    return validate_schema(validator, document)


def validate_semantics(documents: list[ProjectDocument]) -> list[ProjectError]:
    errors: list[ProjectError] = []
    demo_projects = [
        document for document in documents if document.data.get("kind") == "DemoProject"
    ]
    instruqt_courses = [
        document
        for document in documents
        if document.data.get("kind") == "InstruqtCourse"
    ]

    errors.extend(
        validate_unique_field(documents, ("metadata", "name"), "resource name")
    )
    errors.extend(validate_unique_derived_namespaces(demo_projects))
    errors.extend(validate_unique_nexus_endpoint_names(demo_projects))
    errors.extend(
        validate_unique_field(
            demo_projects,
            ("spec", "ingress", "subdomain"),
            "ingress subdomain",
        )
    )

    for document in demo_projects:
        errors.extend(validate_source_repo(document))
        errors.extend(validate_build_references(document))
        errors.extend(validate_component_ports(document))
        errors.extend(validate_service_names(document))
        errors.extend(validate_ingress_routes(document))
        errors.extend(validate_env_input_refs(document))
        errors.extend(validate_temporal_auth_compatibility(document))
        errors.extend(validate_temporal_mtls_cert_files(document))
        errors.extend(validate_project_secrets(document))
        errors.extend(validate_temporal_proxy(document))
        errors.extend(validate_temporal_namespace_labels(document))
        errors.extend(validate_component_temporal_namespace_refs(document))
        errors.extend(validate_nexus_endpoints(document))
        errors.extend(validate_serverless_components(document))
        errors.extend(validate_registry(document))

    for document in instruqt_courses:
        errors.extend(validate_instruqt_course_source_path(document, "source"))
        errors.extend(validate_instruqt_course_source_path(document, "content"))
        errors.extend(validate_instruqt_course_testing(document))
        errors.extend(validate_instruqt_course_images(document))

    return errors


def validate_unique_field(
    documents: list[ProjectDocument], field_path: tuple[str, ...], label: str
) -> list[ProjectError]:
    by_value: dict[str, list[Path]] = defaultdict(list)

    for document in documents:
        value = get_nested(document.data, field_path)
        if isinstance(value, str):
            by_value[value].append(document.path)

    errors: list[ProjectError] = []
    for value, paths in sorted(by_value.items()):
        if len(paths) < 2:
            continue

        locations = ", ".join(relative_path(path) for path in paths)
        for path in paths:
            errors.append(
                ProjectError(
                    path,
                    f"duplicate {label} {value!r}; declared in: {locations}",
                )
            )

    return errors


def validate_unique_derived_namespaces(
    documents: list[ProjectDocument],
) -> list[ProjectError]:
    """Guard both derived names against collision.

    The Kubernetes namespace truncates at 63 characters and the Temporal Cloud one
    at 39, so two projects can collide on the Temporal name while their Kubernetes
    names stay distinct. Declared namespaces make that materially likelier: the
    label lengthens the name being truncated, and several projects already sit at
    the 39-character limit.
    """
    by_kubernetes_namespace: dict[str, list[Path]] = defaultdict(list)
    # Keyed by derived Temporal namespace -> the paths (and labels) claiming it.
    by_temporal_namespace: dict[str, list[tuple[Path, str]]] = defaultdict(list)

    for document in documents:
        project_name = get_nested(document.data, ("metadata", "name"))
        if not isinstance(project_name, str):
            continue
        by_kubernetes_namespace[derive_namespace(project_name)].append(document.path)
        by_temporal_namespace[derive_temporal_namespace(project_name)].append(
            (document.path, "the project namespace")
        )
        for label in temporal_namespace_labels(document):
            by_temporal_namespace[
                derive_temporal_namespace(f"{project_name}-{label}")
            ].append((document.path, f"temporal.namespaces.{label}"))

    errors: list[ProjectError] = []
    for namespace, paths in sorted(by_kubernetes_namespace.items()):
        if len(paths) < 2:
            continue
        locations = ", ".join(relative_path(path) for path in paths)
        for path in paths:
            errors.append(
                ProjectError(
                    path,
                    f"duplicate derived namespace {namespace!r}; declared in: {locations}",
                )
            )

    for namespace, claims in sorted(by_temporal_namespace.items()):
        if len(claims) < 2:
            continue
        locations = ", ".join(
            f"{relative_path(path)} ({source})" for path, source in claims
        )
        for path, _source in claims:
            errors.append(
                ProjectError(
                    path,
                    f"duplicate derived Temporal namespace {namespace!r}; "
                    f"claimed by: {locations}",
                )
            )

    return errors


def validate_source_repo(document: ProjectDocument) -> list[ProjectError]:
    repo = get_nested(document.data, ("spec", "source", "repo"))
    if not isinstance(repo, str):
        return []

    parsed = urlparse(repo)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or not TEMPORAL_SA_REPO_PATH.match(parsed.path)
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        return [
            ProjectError(
                document.path,
                "source.repo must be an https://github.com/temporal-sa/<repo> URL",
            )
        ]

    return []


def validate_instruqt_course_source_path(
    document: ProjectDocument,
    source_key: str,
) -> list[ProjectError]:
    path = get_nested(document.data, ("spec", source_key, "path"))
    if path is None:
        return []
    if not isinstance(path, str):
        return []
    parts = [part for part in path.split("/") if part]
    if path.startswith("/") or ".." in parts:
        return [
            ProjectError(
                document.path,
                f"{source_key}.path must be a relative path within the source repository",
            )
        ]
    return []


def validate_instruqt_course_images(document: ProjectDocument) -> list[ProjectError]:
    images = get_nested(document.data, ("spec", "images"))
    if not isinstance(images, dict):
        return []

    errors: list[ProjectError] = []
    content = get_nested(document.data, ("spec", "content"))
    content_configured = isinstance(content, dict)
    for image_name, image in images.items():
        if not isinstance(image_name, str) or not isinstance(image, dict):
            continue
        for field in ("context", "dockerfile"):
            ref = image.get(field)
            if not isinstance(ref, dict):
                continue
            source_key = ref.get("source")
            path = ref.get("path")
            if source_key == "content" and not content_configured:
                errors.append(
                    ProjectError(
                        document.path,
                        f"images.{image_name}.{field}.source references content but spec.content is not set",
                    )
                )
            if isinstance(path, str):
                parts = [part for part in path.split("/") if part]
                if path.startswith("/") or ".." in parts:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"images.{image_name}.{field}.path must be a relative path within its source repository",
                        )
                    )
        build_args = image.get("buildArgs")
        if isinstance(build_args, dict):
            for arg_name, arg_value in build_args.items():
                if not isinstance(arg_name, str) or not arg_name:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"images.{image_name}.buildArgs keys must be non-empty strings",
                        )
                    )
                if not isinstance(arg_value, (str, int)):
                    errors.append(
                        ProjectError(
                            document.path,
                            f"images.{image_name}.buildArgs.{arg_name} must be a string or integer",
                        )
                    )
    return errors


def validate_instruqt_course_testing(document: ProjectDocument) -> list[ProjectError]:
    content_testing_branch = get_nested(
        document.data,
        ("spec", "content", "testingBranch"),
    )
    source_testing_branch = get_nested(
        document.data,
        ("spec", "source", "testingBranch"),
    )
    if isinstance(content_testing_branch, str) and not isinstance(
        source_testing_branch,
        str,
    ):
        return [
            ProjectError(
                document.path,
                "content.testingBranch requires source.testingBranch",
            )
        ]
    return []


def validate_build_references(document: ProjectDocument) -> list[ProjectError]:
    build_names = set(builds(document))
    errors: list[ProjectError] = []

    for component_name, component in components(document).items():
        build_name = component.get("build")
        if isinstance(build_name, str) and build_name not in build_names:
            errors.append(
                ProjectError(
                    document.path,
                    f"components.{component_name}.build {build_name!r} must match a declared build",
                )
            )

    return errors


def validate_component_ports(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []

    for component_name, component in components(document).items():
        seen: dict[str, int] = {}
        for index, port in enumerate(component_ports(component)):
            port_name = port.get("name")
            if not isinstance(port_name, str):
                continue
            if port_name in seen:
                errors.append(
                    ProjectError(
                        document.path,
                        f"components.{component_name}.ports[{index}].name {port_name!r} duplicates ports[{seen[port_name]}]",
                    )
                )
            else:
                seen[port_name] = index

    return errors


def validate_service_names(document: ProjectDocument) -> list[ProjectError]:
    by_service_name: dict[str, list[str]] = defaultdict(list)

    for component_name, component in components(document).items():
        if component_ports(component):
            by_service_name[effective_service_name(component_name, component)].append(
                component_name
            )

    errors: list[ProjectError] = []
    for service_name, component_names in sorted(by_service_name.items()):
        if len(component_names) < 2:
            continue

        joined_components = ", ".join(component_names)
        errors.append(
            ProjectError(
                document.path,
                f"service name {service_name!r} is used by multiple components: {joined_components}",
            )
        )

    return errors


def validate_ingress_routes(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    ingress = get_nested(document.data, ("spec", "ingress"))
    if not isinstance(ingress, dict):
        return errors

    routes = ingress_routes(ingress)
    catchall_indexes = [
        index for index, route in enumerate(routes) if "pathPrefix" not in route
    ]
    if len(catchall_indexes) > 1:
        errors.append(
            ProjectError(
                document.path,
                f"ingress has multiple catch-all routes at indexes {catchall_indexes}",
            )
        )

    seen_path_prefixes: dict[str, int] = {}
    for index, route in enumerate(routes):
        path_prefix = route.get("pathPrefix")
        if not isinstance(path_prefix, str):
            continue
        if path_prefix in seen_path_prefixes:
            errors.append(
                ProjectError(
                    document.path,
                    f"ingress.routes[{index}].pathPrefix {path_prefix!r} duplicates routes[{seen_path_prefixes[path_prefix]}]",
                )
            )
        else:
            seen_path_prefixes[path_prefix] = index

    for index, route in enumerate(routes):
        component_name = route.get("component")
        port_name = route.get("port")
        errors.extend(
            validate_component_port_ref(
                document,
                component_name,
                port_name,
                f"ingress.routes[{index}]",
            )
        )

    return errors


def validate_env_input_refs(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    auth_mode = temporal_auth_mode(document)

    for component_name, component in components(document).items():
        proxy_enabled = temporal_proxy_enabled(component)
        for index, input_ref in component_input_refs(component):
            path = f"components.{component_name}.env[{index}].valueFrom.inputRef"
            if input_ref in TEMPORAL_API_KEY_INPUT_REFS:
                if proxy_enabled:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"{path} {input_ref!r} cannot be used with temporalProxy",
                        )
                    )
                if auth_mode == TEMPORAL_AUTH_MODE_MTLS:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"{path} {input_ref!r} requires temporal auth mode apiKey",
                        )
                    )
                continue

            if input_ref in TEMPORAL_MTLS_INPUT_REFS:
                if proxy_enabled:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"{path} {input_ref!r} cannot be used with temporalProxy",
                        )
                    )
                if auth_mode != TEMPORAL_AUTH_MODE_MTLS:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"{path} {input_ref!r} requires temporal auth mode mtls",
                        )
                    )
                continue

            if input_ref.startswith("temporal."):
                supported = ", ".join(sorted(TEMPORAL_INPUT_REFS))
                errors.append(
                    ProjectError(
                        document.path,
                        f"{path} {input_ref!r} must be one of: {supported}",
                    )
                )
                continue

            user_secret_ref = parse_user_secret_input_ref(input_ref)
            if user_secret_ref is None:
                errors.append(
                    ProjectError(
                        document.path,
                        (
                            f"{path} {input_ref!r} must start with temporal. "
                            "or secrets.<name>.<property>"
                        ),
                    )
                )
                continue

            secret_name, property_name = user_secret_ref
            if DNS_LABEL_PATTERN.fullmatch(secret_name) is None:
                errors.append(
                    ProjectError(
                        document.path,
                        f"{path} secret name {secret_name!r} must be a DNS label",
                    )
                )
            if ENV_NAME_PATTERN.fullmatch(property_name) is None:
                errors.append(
                    ProjectError(
                        document.path,
                        f"{path} property {property_name!r} must be an env var name",
                    )
                )

    return errors


def validate_temporal_auth_compatibility(
    document: ProjectDocument,
) -> list[ProjectError]:
    if temporal_auth_mode(document) != TEMPORAL_AUTH_MODE_MTLS:
        return []

    errors: list[ProjectError] = []
    for component_name, component in components(document).items():
        if temporal_proxy_enabled(component):
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.temporalProxy.enabled cannot "
                        "be true with temporal auth mode mtls"
                    ),
                )
            )
        if worker_versioning_enabled(component):
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.worker.versioning.enabled "
                        "cannot be true with temporal auth mode mtls"
                    ),
                )
            )
    return errors


def validate_temporal_mtls_cert_files(
    document: ProjectDocument,
) -> list[ProjectError]:
    errors: list[ProjectError] = []
    auth_mode = temporal_auth_mode(document)

    for component_name, component in components(document).items():
        if not temporal_mtls_cert_files_configured(component):
            continue

        if auth_mode != TEMPORAL_AUTH_MODE_MTLS:
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.temporalMtls.certFiles "
                        "requires temporal auth mode mtls"
                    ),
                )
            )

        if temporal_mtls_cert_files_mount_root(component) == "/":
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.temporalMtls.certFiles.paths "
                        "must share a non-root parent directory"
                    ),
                )
            )

    return errors


def validate_serverless_components(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    project_builds = builds(document)

    for component_name, component in components(document).items():
        if not serverless_lambda_enabled(component):
            continue

        serverless = serverless_config(component)
        package_type = serverless.get("packageType", SERVERLESS_PACKAGE_TYPE_IMAGE)
        if package_type != SERVERLESS_PACKAGE_TYPE_IMAGE:
            errors.append(
                ProjectError(
                    document.path,
                    f"components.{component_name}.serverless.packageType must be image",
                )
            )

        build_name = component.get("build")
        build = project_builds.get(build_name) if isinstance(build_name, str) else None
        strategy = (
            build.get("strategy", "dockerfile") if isinstance(build, dict) else None
        )
        if strategy != "dockerfile":
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.build must reference a "
                        "dockerfile build for serverless aws-lambda"
                    ),
                )
            )

        if "worker" in component:
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.worker is not supported for "
                        "serverless aws-lambda components; configure the Temporal "
                        "Worker Deployment under serverless.workerDeployment"
                    ),
                )
            )

        worker_deployment_env = serverless_worker_deployment_env_names(component)
        duplicate_env_names = sorted(
            set(worker_deployment_env["deploymentName"])
            & set(worker_deployment_env["buildId"])
        )
        if duplicate_env_names:
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.serverless.workerDeployment."
                        "env cannot map deploymentName and buildId to the same "
                        f"environment variable(s): {', '.join(duplicate_env_names)}"
                    ),
                )
            )

        unsupported_fields = [
            field
            for field in (
                "ports",
                "serviceName",
                "replicas",
                "automountServiceAccountToken",
                "command",
                "args",
                "temporalProxy",
                "temporalAccess",
                "secrets",
            )
            if field in component
        ]
        for field in unsupported_fields:
            errors.append(
                ProjectError(
                    document.path,
                    (
                        f"components.{component_name}.{field} is not supported "
                        "for serverless aws-lambda components"
                    ),
                )
            )

        cert_files = temporal_mtls_cert_files_config(component)
        for field in ("mountPath", "paths"):
            if field in cert_files:
                errors.append(
                    ProjectError(
                        document.path,
                        (
                            f"components.{component_name}.temporalMtls.certFiles."
                            f"{field} is not supported for serverless aws-lambda "
                            "components; Lambda cert files are materialized under /tmp"
                        ),
                    )
                )

    return errors


def validate_project_secrets(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    declared = project_secret_definitions(document)
    referenced: set[str] = set()

    for component_name, component in components(document).items():
        references = component.get("secrets")
        if isinstance(references, list):
            seen: dict[str, int] = {}
            for index, secret_name in enumerate(references):
                if not isinstance(secret_name, str):
                    continue

                if secret_name in seen:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"components.{component_name}.secrets[{index}] {secret_name!r} duplicates secrets[{seen[secret_name]}]",
                        )
                    )
                else:
                    seen[secret_name] = index
                    referenced.add(secret_name)

                if secret_name not in declared:
                    errors.append(
                        ProjectError(
                            document.path,
                            f"components.{component_name}.secrets[{index}] {secret_name!r} must be declared in top-level secrets",
                        )
                    )

        for index, secret_name, _property_name in component_user_secret_input_refs(
            component
        ):
            referenced.add(secret_name)
            if secret_name not in declared:
                errors.append(
                    ProjectError(
                        document.path,
                        (
                            f"components.{component_name}.env[{index}].valueFrom.inputRef "
                            f"secret {secret_name!r} must be declared in top-level secrets"
                        ),
                    )
                )

    for secret_name in sorted(set(declared) - referenced):
        errors.append(
            ProjectError(
                document.path,
                f"secrets.{secret_name} must be referenced by at least one component",
            )
        )

    return errors


def temporal_namespace_labels(document: ProjectDocument) -> dict[str, dict[str, Any]]:
    value = get_nested(document.data, ("spec", "temporal", "namespaces"))
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def nexus_endpoints(document: ProjectDocument) -> dict[str, dict[str, Any]]:
    value = get_nested(document.data, ("spec", "temporal", "nexusEndpoints"))
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def namespace_references(document: ProjectDocument) -> set[str]:
    """Every reference a Nexus endpoint may name: "primary" plus declared labels."""
    return {NEXUS_PRIMARY_NAMESPACE_REF, *temporal_namespace_labels(document)}


def validate_temporal_namespace_labels(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []

    for label, declared in temporal_namespace_labels(document).items():
        if "searchAttributes" in declared:
            # The schema rejects this too; the point of repeating it here is the
            # message, which says why and where to put them instead.
            errors.append(
                ProjectError(
                    document.path,
                    f"temporal.namespaces.{label}.searchAttributes is not supported; "
                    "search attributes are managed on the project's own namespace "
                    "only, via spec.temporal.searchAttributes",
                )
            )
        if not DNS_LABEL_PATTERN.match(label):
            errors.append(
                ProjectError(
                    document.path,
                    f"temporal.namespaces.{label} must be a DNS label",
                )
            )
        if label in RESERVED_TEMPORAL_NAMESPACE_LABELS:
            errors.append(
                ProjectError(
                    document.path,
                    f"temporal.namespaces.{label} uses a reserved name; "
                    f"{sorted(RESERVED_TEMPORAL_NAMESPACE_LABELS)} collide with the "
                    "Temporal credential rotation staging paths",
                )
            )
    return errors


def validate_component_temporal_namespace_refs(
    document: ProjectDocument,
) -> list[ProjectError]:
    declared = set(temporal_namespace_labels(document))
    errors: list[ProjectError] = []
    referenced: set[str] = set()

    for component_name, component in components(document).items():
        label = component.get("temporalNamespace")
        if not isinstance(label, str) or not label:
            continue
        referenced.add(label)

        if label not in declared:
            errors.append(
                ProjectError(
                    document.path,
                    f"components.{component_name}.temporalNamespace {label!r} must "
                    "match a declared temporal.namespaces entry",
                )
            )
        if serverless_lambda_enabled(component):
            errors.append(
                ProjectError(
                    document.path,
                    f"components.{component_name}.temporalNamespace is not supported "
                    "for serverless aws-lambda components",
                )
            )
        if temporal_proxy_enabled(component):
            errors.append(
                ProjectError(
                    document.path,
                    f"components.{component_name}.temporalNamespace cannot be used "
                    "with temporalProxy",
                )
            )
        if worker_versioning_enabled(component):
            # The Worker Controller's Connection and WorkerDeployment both resolve
            # the project's primary namespace and credentials, so a bound versioned
            # worker would poll one namespace while its pod authenticates to another.
            errors.append(
                ProjectError(
                    document.path,
                    f"components.{component_name}.temporalNamespace cannot be used "
                    "with worker.versioning",
                )
            )

    endpoint_targets: set[str] = set()
    for endpoint in nexus_endpoints(document).values():
        target = endpoint.get("targetNamespace", NEXUS_PRIMARY_NAMESPACE_REF)
        if isinstance(target, str):
            endpoint_targets.add(target)
        callers = endpoint.get("allowedCallerNamespaces")
        if isinstance(callers, list):
            endpoint_targets.update(item for item in callers if isinstance(item, str))

    for label in sorted(declared - referenced - endpoint_targets):
        errors.append(
            ProjectError(
                document.path,
                f"temporal.namespaces.{label} must be referenced by at least one "
                "component or Nexus endpoint",
            )
        )

    return errors


def validate_nexus_endpoints(document: ProjectDocument) -> list[ProjectError]:
    allowed = namespace_references(document)
    errors: list[ProjectError] = []

    for endpoint_name, endpoint in nexus_endpoints(document).items():
        if not NEXUS_ENDPOINT_NAME_PATTERN.match(endpoint_name):
            errors.append(
                ProjectError(
                    document.path,
                    f"temporal.nexusEndpoints.{endpoint_name} must start with a "
                    "letter, end alphanumeric, and use only letters, digits, and "
                    "hyphens",
                )
            )

        target = endpoint.get("targetNamespace", NEXUS_PRIMARY_NAMESPACE_REF)
        if not isinstance(target, str) or target not in allowed:
            errors.append(
                ProjectError(
                    document.path,
                    f"temporal.nexusEndpoints.{endpoint_name}.targetNamespace "
                    f"{target!r} must be {NEXUS_PRIMARY_NAMESPACE_REF!r} or a "
                    "declared temporal.namespaces entry",
                )
            )

        task_queue = endpoint.get("targetTaskQueue")
        if not isinstance(task_queue, str) or not task_queue.strip():
            errors.append(
                ProjectError(
                    document.path,
                    f"temporal.nexusEndpoints.{endpoint_name}.targetTaskQueue must "
                    "be a non-empty string",
                )
            )

        callers = endpoint.get("allowedCallerNamespaces")
        if not isinstance(callers, list) or not callers:
            # Temporal admits no callers by default, even from the target's own
            # namespace, so an empty allowlist makes the endpoint unreachable.
            errors.append(
                ProjectError(
                    document.path,
                    f"temporal.nexusEndpoints.{endpoint_name}.allowedCallerNamespaces "
                    "must list at least one namespace",
                )
            )
            continue

        seen: dict[str, int] = {}
        for index, caller in enumerate(callers):
            if not isinstance(caller, str) or caller not in allowed:
                errors.append(
                    ProjectError(
                        document.path,
                        f"temporal.nexusEndpoints.{endpoint_name}"
                        f".allowedCallerNamespaces[{index}] {caller!r} must be "
                        f"{NEXUS_PRIMARY_NAMESPACE_REF!r} or a declared "
                        "temporal.namespaces entry",
                    )
                )
                continue
            if caller in seen:
                errors.append(
                    ProjectError(
                        document.path,
                        f"temporal.nexusEndpoints.{endpoint_name}"
                        f".allowedCallerNamespaces[{index}] {caller!r} duplicates "
                        f"allowedCallerNamespaces[{seen[caller]}]",
                    )
                )
            else:
                seen[caller] = index

    return errors


def validate_unique_nexus_endpoint_names(
    documents: list[ProjectDocument],
) -> list[ProjectError]:
    """Endpoint names are unique per Temporal Cloud account, not per project."""
    by_name: dict[str, list[Path]] = defaultdict(list)

    for document in documents:
        for endpoint_name in nexus_endpoints(document):
            by_name[endpoint_name].append(document.path)

    errors: list[ProjectError] = []
    for endpoint_name, paths in sorted(by_name.items()):
        if len(paths) < 2:
            continue
        locations = ", ".join(relative_path(path) for path in paths)
        for path in paths:
            errors.append(
                ProjectError(
                    path,
                    f"duplicate Nexus endpoint name {endpoint_name!r}; declared in: "
                    f"{locations}. Endpoint names are unique account-wide.",
                )
            )

    return errors


def validate_temporal_proxy(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    for component_name, component in components(document).items():
        temporal_proxy = component.get("temporalProxy")
        if (
            not isinstance(temporal_proxy, dict)
            or temporal_proxy.get("enabled") is not True
        ):
            continue
        if not component_needs_temporal(component):
            errors.append(
                ProjectError(
                    document.path,
                    f"components.{component_name}.temporalProxy requires worker or temporalAccess",
                )
            )
    return errors


def validate_registry(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    registry = get_nested(document.data, ("spec", "registry"))
    if not isinstance(registry, dict):
        return errors

    errors.extend(
        validate_unique_string_list(
            document,
            registry.get("tags"),
            "registry.tags",
            case_insensitive=True,
        )
    )
    errors.extend(
        validate_unique_string_list(
            document,
            registry.get("temporalSdks"),
            "registry.temporalSdks",
            case_insensitive=True,
        )
    )

    readiness = registry.get("readiness")
    if isinstance(readiness, dict):
        score = readiness.get("score")
        max_score = readiness.get("maxScore")
        if isinstance(score, int) and isinstance(max_score, int) and score > max_score:
            errors.append(
                ProjectError(
                    document.path,
                    "registry.readiness.score must be less than or equal to maxScore",
                )
            )

    assets = registry.get("assets")
    if isinstance(assets, dict):
        for asset_group in ("technical", "nonTechnical"):
            errors.extend(
                validate_registry_asset_group(
                    document,
                    assets.get(asset_group),
                    f"registry.assets.{asset_group}",
                )
            )

    return errors


def validate_registry_asset_group(
    document: ProjectDocument,
    assets: Any,
    path: str,
) -> list[ProjectError]:
    if not isinstance(assets, list):
        return []

    labels: dict[str, int] = {}
    errors: list[ProjectError] = []
    for index, asset in enumerate(assets):
        if not isinstance(asset, dict):
            continue
        label = asset.get("label")
        if not isinstance(label, str):
            continue

        key = label.lower()
        if key in labels:
            errors.append(
                ProjectError(
                    document.path,
                    f"{path}[{index}].label {label!r} duplicates {path}[{labels[key]}]",
                )
            )
        else:
            labels[key] = index

    return errors


def validate_unique_string_list(
    document: ProjectDocument,
    values: Any,
    path: str,
    *,
    case_insensitive: bool = False,
) -> list[ProjectError]:
    if not isinstance(values, list):
        return []

    seen: dict[str, int] = {}
    errors: list[ProjectError] = []
    for index, value in enumerate(values):
        if not isinstance(value, str):
            continue

        key = value.lower() if case_insensitive else value
        if key in seen:
            errors.append(
                ProjectError(
                    document.path,
                    f"{path}[{index}] {value!r} duplicates {path}[{seen[key]}]",
                )
            )
        else:
            seen[key] = index

    return errors


def validate_component_port_ref(
    document: ProjectDocument,
    component_name: Any,
    port_name: Any,
    path: str,
    *,
    allow_default_port: bool = False,
) -> list[ProjectError]:
    if not isinstance(component_name, str):
        return []

    component = components(document).get(component_name)
    if component is None:
        return [
            ProjectError(
                document.path,
                f"{path}.component {component_name!r} must match a declared component",
            )
        ]

    ports = component_port_names(component)
    if not ports:
        return [
            ProjectError(
                document.path,
                f"{path}.component {component_name!r} must reference a component with ports",
            )
        ]

    if port_name is None and allow_default_port:
        if len(ports) == 1:
            return []
        return [
            ProjectError(
                document.path,
                f"{path}.port is required because component {component_name!r} has multiple ports",
            )
        ]

    if not isinstance(port_name, str) or port_name not in ports:
        return [
            ProjectError(
                document.path,
                f"{path}.port {port_name!r} must match a port on component {component_name!r}",
            )
        ]

    return []


def ingress_routes(ingress: dict[str, Any]) -> list[dict[str, Any]]:
    routes = ingress.get("routes")
    if isinstance(routes, list):
        return [route for route in routes if isinstance(route, dict)]

    component = ingress.get("component")
    port = ingress.get("port")
    if isinstance(component, str) and isinstance(port, str):
        return [{"component": component, "port": port}]

    return []


def builds(document: ProjectDocument) -> dict[str, dict[str, Any]]:
    value = get_nested(document.data, ("spec", "builds"))
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def components(document: ProjectDocument) -> dict[str, dict[str, Any]]:
    value = get_nested(document.data, ("spec", "components"))
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def project_secret_definitions(document: ProjectDocument) -> dict[str, dict[str, Any]]:
    value = get_nested(document.data, ("spec", "secrets"))
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def component_ports(component: dict[str, Any]) -> list[dict[str, Any]]:
    ports = component.get("ports")
    if not isinstance(ports, list):
        return []
    raw = [port for port in ports if isinstance(port, dict)]
    # Mirror the operator defaults: a lone port is named "http", and servicePort
    # falls back to containerPort.
    single = len(raw) == 1
    normalized: list[dict[str, Any]] = []
    for port in raw:
        port = dict(port)
        if "name" not in port and single:
            port["name"] = "http"
        if "servicePort" not in port and "containerPort" in port:
            port["servicePort"] = port["containerPort"]
        normalized.append(port)
    return normalized


def component_port_names(component: dict[str, Any]) -> set[str]:
    return {
        port["name"]
        for port in component_ports(component)
        if isinstance(port.get("name"), str)
    }


def effective_service_name(component_name: str, component: dict[str, Any]) -> str:
    service_name = component.get("serviceName")
    if isinstance(service_name, str):
        return service_name
    return component_name


def component_needs_temporal(component: dict[str, Any]) -> bool:
    worker = component.get("worker")
    return (
        worker is True
        or isinstance(worker, dict)
        or component.get("temporalAccess") is True
        or bool(component_temporal_input_refs(component))
        or temporal_mtls_cert_files_requested(component)
    )


def temporal_proxy_enabled(component: dict[str, Any]) -> bool:
    temporal_proxy = component.get("temporalProxy")
    return isinstance(temporal_proxy, dict) and temporal_proxy.get("enabled") is True


def worker_versioning_enabled(component: dict[str, Any]) -> bool:
    worker = component.get("worker")
    if not isinstance(worker, dict):
        return False
    versioning = worker.get("versioning")
    return isinstance(versioning, dict) and versioning.get("enabled") is True


def serverless_config(component: dict[str, Any]) -> dict[str, Any]:
    serverless = component.get("serverless")
    return serverless if isinstance(serverless, dict) else {}


def serverless_worker_deployment_config(component: dict[str, Any]) -> dict[str, Any]:
    worker_deployment = serverless_config(component).get("workerDeployment")
    return worker_deployment if isinstance(worker_deployment, dict) else {}


def serverless_worker_deployment_env_names(
    component: dict[str, Any],
) -> dict[str, tuple[str, ...]]:
    configured = serverless_worker_deployment_config(component).get("env")
    configured = configured if isinstance(configured, dict) else {}
    return {
        "deploymentName": tuple(
            env_name
            for env_name in configured.get("deploymentName", ())
            if isinstance(env_name, str) and env_name
        ),
        "buildId": tuple(
            env_name
            for env_name in configured.get("buildId", ())
            if isinstance(env_name, str) and env_name
        ),
    }


def serverless_lambda_enabled(component: dict[str, Any]) -> bool:
    return (
        serverless_config(component).get("provider")
        == SERVERLESS_PROVIDER_AWS_LAMBDA
    )


def temporal_mtls_config(component: dict[str, Any]) -> dict[str, Any]:
    config = component.get("temporalMtls")
    return config if isinstance(config, dict) else {}


def temporal_mtls_cert_files_configured(component: dict[str, Any]) -> bool:
    return "certFiles" in temporal_mtls_config(component)


def temporal_mtls_cert_files_config(component: dict[str, Any]) -> dict[str, Any]:
    cert_files = temporal_mtls_config(component).get("certFiles")
    return cert_files if isinstance(cert_files, dict) else {}


def temporal_mtls_cert_files_enabled(component: dict[str, Any]) -> bool:
    return temporal_mtls_cert_files_config(component).get("enabled") is not False


def temporal_mtls_cert_files_requested(component: dict[str, Any]) -> bool:
    return (
        temporal_mtls_cert_files_configured(component)
        and temporal_mtls_cert_files_enabled(component)
    )


def temporal_mtls_cert_files_mount_path(component: dict[str, Any]) -> str:
    configured = temporal_mtls_cert_files_config(component).get("mountPath")
    if isinstance(configured, str) and configured.startswith("/"):
        return configured.rstrip("/") or "/"
    return TEMPORAL_MTLS_CERT_FILES_MOUNT_PATH


def temporal_mtls_cert_file_paths(component: dict[str, Any]) -> dict[str, str]:
    mount_path = temporal_mtls_cert_files_mount_path(component)
    paths = {
        name: f"{mount_path}/{filename}"
        for name, filename in TEMPORAL_MTLS_CERT_FILES.items()
    }
    configured = temporal_mtls_cert_files_config(component).get("paths")
    if isinstance(configured, dict):
        paths.update(
            {
                name: value
                for name, value in configured.items()
                if name in TEMPORAL_MTLS_CERT_FILES
                and isinstance(value, str)
                and value.startswith("/")
            }
        )
    return paths


def temporal_mtls_cert_files_mount_root(component: dict[str, Any]) -> str:
    paths = list(temporal_mtls_cert_file_paths(component).values())
    split_paths = [path.strip("/").split("/") for path in paths]
    common: list[str] = []
    for parts in zip(*split_paths):
        if len(set(parts)) != 1:
            break
        common.append(parts[0])
    if not common:
        return "/"
    return "/" + "/".join(common)


def temporal_auth_mode(document: ProjectDocument) -> str:
    mode = get_nested(document.data, ("spec", "temporal", "auth", "mode"))
    if mode in {TEMPORAL_AUTH_MODE_API_KEY, TEMPORAL_AUTH_MODE_MTLS}:
        return str(mode)
    return TEMPORAL_AUTH_MODE_API_KEY


def component_input_refs(component: dict[str, Any]) -> list[tuple[int, str]]:
    env = component.get("env")
    if not isinstance(env, list):
        return []

    refs: list[tuple[int, str]] = []
    for index, item in enumerate(env):
        if not isinstance(item, dict):
            continue
        value_from = item.get("valueFrom")
        if not isinstance(value_from, dict):
            continue
        input_ref = value_from.get("inputRef")
        if isinstance(input_ref, str):
            input_ref = input_ref.strip()
            if input_ref:
                refs.append((index, input_ref))
    return refs


def component_temporal_input_refs(component: dict[str, Any]) -> list[str]:
    return [
        input_ref
        for _index, input_ref in component_input_refs(component)
        if input_ref in TEMPORAL_INPUT_REFS
    ]


def component_user_secret_input_refs(
    component: dict[str, Any],
) -> list[tuple[int, str, str]]:
    refs: list[tuple[int, str, str]] = []
    for index, input_ref in component_input_refs(component):
        parsed = parse_user_secret_input_ref(input_ref)
        if parsed:
            secret_name, property_name = parsed
            refs.append((index, secret_name, property_name))
    return refs


def parse_user_secret_input_ref(input_ref: str) -> tuple[str, str] | None:
    parts = input_ref.split(".", 2)
    if len(parts) != 3 or parts[0] != "secrets":
        return None
    secret_name = parts[1].strip()
    property_name = parts[2].strip()
    if not secret_name or not property_name:
        return None
    return secret_name, property_name


def derive_namespace(project_name: str) -> str:
    return _prefixed_truncated(NAMESPACE_PREFIX, project_name, 63)


def derive_temporal_namespace(project_name: str) -> str:
    return _prefixed_truncated(
        TEMPORAL_NAMESPACE_PREFIX, project_name, TEMPORAL_NAMESPACE_MAX_LENGTH
    )


def _prefixed_truncated(prefix: str, project_name: str, limit: int) -> str:
    raw = f"{prefix}{project_name}"
    if len(raw) <= limit:
        return raw

    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
    stem = raw[: limit - len(digest) - 1].rstrip("-")
    return f"{stem}-{digest}"


def get_nested(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def discover_project_paths(paths: list[str]) -> list[Path]:
    if paths:
        return sorted(Path(path).resolve() for path in paths)

    return sorted(ROOT.glob(DEFAULT_PROJECT_GLOB))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate project configs against schema and platform semantic rules.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Project YAML files to validate. Defaults to projects/**/*.yaml.",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Override JSON Schema path for every config file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    fallback_validator = None
    try:
        if args.schema:
            schema = load_schema(Path(args.schema).resolve())
            fallback_validator = Draft202012Validator(schema)
            validators = {}
        else:
            validators = load_default_validators()
    except (OSError, json.JSONDecodeError, SchemaError) as error:
        print(f"Cannot load config schema: {error}", file=sys.stderr)
        return 2

    project_paths = discover_project_paths(args.paths)
    if not project_paths:
        print("No project config files found.", file=sys.stderr)
        return 1

    load_errors: list[ProjectError] = []
    documents: list[ProjectDocument] = []
    for path in project_paths:
        data, errors = load_yaml_document(path)
        load_errors.extend(errors)
        if data is not None:
            documents.append(ProjectDocument(path=path, data=data))

    schema_errors_by_path: dict[Path, list[ProjectError]] = {}
    for document in documents:
        schema_errors = validate_document_schema(
            validators,
            document,
            fallback_validator=fallback_validator,
        )
        if schema_errors:
            schema_errors_by_path[document.path] = schema_errors

    errors = [
        *load_errors,
        *[
            error
            for errors_for_path in schema_errors_by_path.values()
            for error in errors_for_path
        ],
        *validate_semantics(documents),
    ]

    if errors:
        print("Project validation failed:")
        for error in sorted(
            errors,
            key=lambda item: (relative_path(item.path), item.message),
        ):
            print(f"- {error.render()}")
        return 1

    print(f"Validated {len(documents)} project config(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
