from __future__ import annotations

from typing import Any

from demo_registry_operator import derive
from demo_registry_operator.activities.external_resources.common import string_field
from demo_registry_operator.activities.external_resources.models import (
    EcrRepositoryRequest,
    EcrRepositoryResult,
    ExternalResourcesPlan,
    SecretRequirement,
    TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
    TemporalCloudProject,
    VerifySecretsManagerResourcesRequest,
)
from demo_registry_operator.resources import (
    TEMPORAL_AUTH_MODE_MTLS,
    TEMPORAL_MTLS_CA_SECRET_PROPERTIES,
    builds,
    components_for_user_secret,
    project_name,
    project_namespace,
    project_secret_prefix,
    project_temporal_namespace,
    spec_of,
    temporal_auth_mode,
    temporal_components,
    temporal_mtls_ca_secret_remote_path,
    temporal_runtime_secret_properties,
    temporal_secret_remote_path,
    user_secret_names,
    user_secret_remote_path,
    user_secret_required_properties,
)


def plan_external_resources(
    demo_project: dict[str, Any],
    *,
    checked_at: str,
    force_temporal_auth_rotation: bool = False,
) -> ExternalResourcesPlan:
    """Return a deterministic, side-effect-free external-resource plan."""

    return ExternalResourcesPlan(
        demo_project=demo_project,
        checked_at=checked_at,
        mode="aws",
        kubernetes_namespace=project_namespace(demo_project),
        force_temporal_auth_rotation=force_temporal_auth_rotation,
        ecr_repositories=tuple(
            repository_request(demo_project, build_name)
            for build_name in sorted(builds(demo_project))
        ),
        temporal_project=temporal_cloud_project(demo_project),
        secrets_manager=secrets_manager_request(demo_project, checked_at=checked_at),
    )


def temporal_cloud_project(demo_project: dict[str, Any]) -> TemporalCloudProject:
    return TemporalCloudProject(
        name=project_name(demo_project),
        namespace=project_temporal_namespace(demo_project),
        kubernetes_namespace=project_namespace(demo_project),
        auth_mode=temporal_auth_mode(demo_project),
        components=temporal_components(demo_project),
        runtime_secret_path=temporal_secret_remote_path(demo_project),
        mtls_ca_secret_path=temporal_mtls_ca_secret_remote_path(demo_project),
    )


def repository_request(
    demo_project: dict[str, Any],
    build_name: str,
) -> EcrRepositoryRequest:
    return EcrRepositoryRequest(
        name=image_repository_name(demo_project, build_name),
        project=project_name(demo_project),
        build=build_name,
        components=tuple(components_for_build(demo_project, build_name)),
    )


def secrets_manager_request(
    demo_project: dict[str, Any],
    *,
    checked_at: str,
) -> VerifySecretsManagerResourcesRequest:
    temporal_required = bool(temporal_components(demo_project))
    auth_mode = temporal_auth_mode(demo_project)
    return VerifySecretsManagerResourcesRequest(
        checked_at=checked_at,
        secret_prefix=project_secret_prefix(demo_project),
        temporal_bootstrap=SecretRequirement(
            path="",  # Filled from platform config inside the activity.
            required=temporal_required,
            managed_by="platform",
            required_properties=TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
        ),
        temporal_credentials=SecretRequirement(
            path=temporal_secret_remote_path(demo_project),
            required=temporal_required,
            managed_by="platform",
            required_properties=tuple(temporal_runtime_secret_properties(demo_project)),
        ),
        temporal_mtls_ca=SecretRequirement(
            path=temporal_mtls_ca_secret_remote_path(demo_project),
            required=temporal_required and auth_mode == TEMPORAL_AUTH_MODE_MTLS,
            managed_by="platform",
            required_properties=tuple(TEMPORAL_MTLS_CA_SECRET_PROPERTIES),
        ),
        user_managed=tuple(
            SecretRequirement(
                path=user_secret_remote_path(demo_project, secret_name),
                required=True,
                managed_by="project-owner",
                required_properties=tuple(
                    user_secret_required_properties(demo_project, secret_name)
                ),
                components=tuple(components_for_user_secret(demo_project, secret_name)),
            )
            for secret_name in user_secret_names(demo_project)
        ),
    )


def ecr_repositories_status(
    plan: ExternalResourcesPlan,
    repositories: list[EcrRepositoryResult],
) -> dict[str, Any]:
    statuses = [repository.as_status() for repository in repositories]
    first = statuses[0] if statuses else {}
    return {
        "phase": aggregate_phase(statuses) if statuses else "NotRequired",
        "region": first.get("region"),
        "accountId": first.get("accountId"),
        "checkedAt": plan.checked_at,
        "repositories": statuses,
    }


def external_resources_status(
    plan: ExternalResourcesPlan,
    *,
    ecr_status: dict[str, Any],
    temporal_status: dict[str, Any],
    secrets_status: dict[str, Any],
) -> dict[str, Any]:
    phases = [
        str(ecr_status.get("phase")),
        str(secrets_status.get("phase")),
        str(temporal_status.get("phase")),
    ]
    return {
        "phase": aggregate_top_level_phase(phases),
        "mode": plan.mode,
        "checkedAt": plan.checked_at,
        "kubernetesNamespace": plan.kubernetes_namespace,
        "ecr": ecr_status,
        "secretsManager": secrets_status,
        "temporalCloud": temporal_status,
    }


def external_resource_finalization_status(
    finalization_plan: dict[str, Any],
    *,
    temporal_status: dict[str, Any],
    ecr_status: dict[str, Any],
) -> dict[str, Any]:
    phase = "Deleted"
    if temporal_status.get("phase") == "Error" or ecr_status.get("phase") == "Error":
        phase = "Error"
    elif {temporal_status.get("phase"), ecr_status.get("phase")} <= {"Skipped"}:
        phase = "Skipped"
    return {
        "phase": phase,
        "checkedAt": string_field(finalization_plan, "checkedAt"),
        "temporalCloud": temporal_status,
        "ecr": ecr_status,
    }


def temporal_cloud_finalization_status(
    finalization_plan: dict[str, Any],
    *,
    api_key_status: dict[str, Any],
    service_account_status: dict[str, Any],
    namespace_protection_status: dict[str, Any],
    namespace_status: dict[str, Any],
    runtime_secret_status: dict[str, Any],
    mtls_ca_secret_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    statuses = [
        api_key_status,
        service_account_status,
        namespace_protection_status,
        namespace_status,
        runtime_secret_status,
    ]
    if mtls_ca_secret_status is not None:
        statuses.append(mtls_ca_secret_status)
    phases = {status.get("phase") for status in statuses}
    phase = "Deleted"
    if "Error" in phases:
        phase = "Error"
    elif phases <= {"Skipped", "NotRequired"}:
        phase = "Skipped"

    deleted: dict[str, Any] = {}
    namespace: dict[str, Any] = {}
    for status in statuses:
        status_deleted = status.get("deleted")
        if isinstance(status_deleted, dict):
            deleted.update(status_deleted)
        status_namespace = status.get("namespace")
        if isinstance(status_namespace, dict) and status_namespace.get("name"):
            namespace = status_namespace

    rendered = {
        "phase": phase,
        "provider": "temporal-cloud",
        "mode": "cloud-api",
        "checkedAt": string_field(finalization_plan, "checkedAt"),
        "namespace": namespace,
        "deleted": deleted,
        "steps": {
            "apiKey": api_key_status,
            "serviceAccount": service_account_status,
            "namespaceDeleteProtection": namespace_protection_status,
            "namespace": namespace_status,
            "secret": runtime_secret_status,
        },
    }
    if mtls_ca_secret_status is not None:
        rendered["steps"]["mtlsCaSecret"] = mtls_ca_secret_status
    return rendered


def image_repository_name(demo_project: dict[str, Any], build_name: str) -> str:
    return f"{derive.image_repository_prefix()}/{project_name(demo_project)}/{build_name}"


def components_for_build(demo_project: dict[str, Any], build_name: str) -> list[str]:
    return [
        component_name
        for component_name, component in sorted(spec_components(demo_project).items())
        if component.get("build") == build_name
    ]


def spec_components(demo_project: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = spec_of(demo_project).get("components")
    if not isinstance(value, dict):
        return {}
    return {key: item for key, item in value.items() if isinstance(item, dict)}


def aggregate_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if "Error" in phases:
        return "Error"
    if "MissingValue" in phases:
        return "ValuesRequired"
    if phases and phases <= {"Ready", "NotRequired"}:
        return "Ready"
    return "Unknown"


def aggregate_secret_phase(resources: list[dict[str, Any]]) -> str:
    phases = {resource.get("phase") for resource in resources}
    if "Error" in phases:
        return "Error"
    if "MissingValue" in phases or "MissingProperties" in phases:
        return "ValuesRequired"
    if phases and phases <= {"Ready", "NotRequired"}:
        return "Ready"
    return "Unknown"


def aggregate_top_level_phase(phases: list[str]) -> str:
    if "Error" in phases:
        return "Error"
    if "ManualSetupRequired" in phases:
        return "ManualSetupRequired"
    if "ValuesRequired" in phases:
        return "ValuesRequired"
    if phases and set(phases) <= {"Ready", "NotRequired"}:
        return "Ready"
    return "Unknown"
