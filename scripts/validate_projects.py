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
from typing import Any, Iterable
from urllib.parse import urlparse

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "schemas" / "project.schema.json"
DEFAULT_PROJECT_GLOB = "projects/*.yaml"
DOMAIN = "tmprl-demo.cloud"
NAMESPACE_PREFIX = "tmprl-dem-cld-"
TEMPORAL_NAMESPACE_PREFIX = "tmprl-dem-cld-"
# Temporal Cloud namespace names (before the account suffix) cap at 39 chars.
TEMPORAL_NAMESPACE_MAX_LENGTH = 39
TEMPORAL_SA_REPO_PATH = re.compile(r"^/temporal-sa/[A-Za-z0-9_.-]+/?$")


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


def validate_semantics(documents: list[ProjectDocument]) -> list[ProjectError]:
    errors: list[ProjectError] = []

    errors.extend(validate_unique_field(documents, ("metadata", "name"), "project name"))
    errors.extend(validate_unique_derived_namespaces(documents))
    errors.extend(validate_unique_field(documents, ("spec", "ingress", "subdomain"), "ingress subdomain"))

    for document in documents:
        errors.extend(validate_source_repo(document))
        errors.extend(validate_no_full_domain(document))
        errors.extend(validate_build_references(document))
        errors.extend(validate_component_ports(document))
        errors.extend(validate_service_names(document))
        errors.extend(validate_ingress_routes(document))
        errors.extend(validate_project_secrets(document))
        errors.extend(validate_registry(document))

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
    by_namespace: dict[str, list[Path]] = defaultdict(list)

    for document in documents:
        project_name = get_nested(document.data, ("metadata", "name"))
        if isinstance(project_name, str):
            by_namespace[derive_namespace(project_name)].append(document.path)

    errors: list[ProjectError] = []
    for namespace, paths in sorted(by_namespace.items()):
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


def validate_no_full_domain(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    needle = DOMAIN.lower()

    for path, value in iter_string_values(document.data):
        # The CR envelope's apiVersion legitimately carries the platform group
        # (registry.tmprl-demo.cloud/v1alpha1); the rule targets authored hosts.
        if path == "apiVersion":
            continue
        if needle in value.lower():
            errors.append(
                ProjectError(
                    document.path,
                    f"{path} must not include {DOMAIN}; configure only the platform subdomain",
                )
            )

    return errors


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


def validate_project_secrets(document: ProjectDocument) -> list[ProjectError]:
    errors: list[ProjectError] = []
    declared = project_secret_definitions(document)
    referenced: set[str] = set()

    for component_name, component in components(document).items():
        references = component.get("secrets")
        if not isinstance(references, list):
            continue

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

    for secret_name in sorted(set(declared) - referenced):
        errors.append(
            ProjectError(
                document.path,
                f"secrets.{secret_name} must be referenced by at least one component",
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


def iter_string_values(data: Any, path: str = "<root>") -> Iterable[tuple[str, str]]:
    if isinstance(data, str):
        yield path, data
    elif isinstance(data, dict):
        for key, value in data.items():
            child_path = str(key) if path == "<root>" else f"{path}.{key}"
            yield from iter_string_values(value, child_path)
    elif isinstance(data, list):
        for index, value in enumerate(data):
            yield from iter_string_values(value, f"{path}[{index}]")


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
        help="Project YAML files to validate. Defaults to projects/*.yaml.",
    )
    parser.add_argument(
        "--schema",
        default=str(DEFAULT_SCHEMA),
        help=f"Project JSON Schema path. Defaults to {relative_path(DEFAULT_SCHEMA)}.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    try:
        schema = load_schema(Path(args.schema).resolve())
    except (OSError, json.JSONDecodeError, SchemaError) as error:
        print(f"Cannot load project schema: {error}", file=sys.stderr)
        return 2

    validator = Draft202012Validator(schema)
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
        schema_errors = validate_schema(validator, document)
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
