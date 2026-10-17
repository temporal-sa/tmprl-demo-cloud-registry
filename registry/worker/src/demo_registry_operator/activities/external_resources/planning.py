from __future__ import annotations

from typing import Any

from demo_registry_operator import derive, resources
from demo_registry_operator.activities.external_resources import common, models


def plan_external_resources(
    demo_project: dict[str, Any],
    *,
    checked_at: str,
    force_temporal_auth_rotation: bool = False,
) -> models.ExternalResourcesPlan:
    """Return a deterministic, side-effect-free external-resource plan."""

    return models.ExternalResourcesPlan(
        demo_project=demo_project,
        checked_at=checked_at,
        mode="aws",
        kubernetes_namespace=resources.project_namespace(demo_project),
        force_temporal_auth_rotation=force_temporal_auth_rotation,
        ecr_repositories=tuple(
            repository_request(demo_project, build_name)
            for build_name in sorted(resources.builds(demo_project))
        ),
        temporal_project=temporal_cloud_project(demo_project),
        secrets_manager=secrets_manager_request(demo_project, checked_at=checked_at),
    )


def temporal_cloud_project(demo_project: dict[str, Any]) -> models.TemporalCloudProject:
    return models.TemporalCloudProject(
        name=resources.project_name(demo_project),
        namespace=resources.project_temporal_namespace(demo_project),
        kubernetes_namespace=resources.project_namespace(demo_project),
        auth_mode=resources.temporal_auth_mode(demo_project),
        # Only the unbound components, which is every component until a project
        # declares spec.temporal.namespaces.
        components=resources.components_bound_to_namespace_label(
            resources.temporal_components(demo_project), None
        ),
        runtime_secret_path=resources.temporal_secret_remote_path(demo_project),
        mtls_ca_secret_path=resources.temporal_mtls_ca_secret_remote_path(demo_project),
        region=resources.temporal_region(demo_project),
        serverless=resources.has_serverless_lambda_components(demo_project),
        task_queue_fairness=resources.temporal_task_queue_fairness(demo_project),
        search_attributes=tuple(
            models.TemporalCloudSearchAttribute(name=name, type=attribute_type)
            for name, attribute_type in resources.temporal_search_attributes(
                demo_project
            ).items()
        ),
        additional_namespaces=tuple(
            temporal_namespace_spec(demo_project, label)
            for label in sorted(resources.temporal_namespace_labels(demo_project))
        ),
        nexus_endpoints=tuple(
            nexus_endpoint_spec(name, endpoint)
            for name, endpoint in sorted(
                resources.temporal_nexus_endpoints(demo_project).items()
            )
        ),
    )


def temporal_namespace_spec(
    demo_project: dict[str, Any],
    label: str,
) -> models.TemporalNamespaceSpec:
    declared = resources.temporal_namespace_labels(demo_project).get(label, {})
    retention_days = declared.get("retentionDays")
    region = declared.get("region")
    return models.TemporalNamespaceSpec(
        label=label,
        namespace=resources.project_temporal_namespace_for_label(demo_project, label),
        # Inherited from the project: auth is not declared per namespace.
        auth_mode=resources.temporal_auth_mode(demo_project),
        runtime_secret_path=resources.temporal_secret_remote_path_for_label(
            demo_project, label
        ),
        mtls_ca_secret_path=(
            f"{resources.temporal_secret_remote_path_for_label(demo_project, label)}/ca"
        ),
        components=resources.components_bound_to_namespace_label(
            resources.temporal_components(demo_project), label
        ),
        # search_attributes is left empty on purpose. The reconcile step that
        # manages them computes its delete set against a single project-scoped
        # status blob, so it runs for the project's own namespace only, and the
        # schema rejects the key here rather than accepting it and ignoring it.
        region=region if isinstance(region, str) and region else None,
        retention_days=retention_days if isinstance(retention_days, int) else None,
        task_queue_fairness=declared.get("fairness") is True,
    )


def nexus_endpoint_spec(
    name: str,
    endpoint: dict[str, Any],
) -> models.NexusEndpointSpec:
    callers = endpoint.get("allowedCallerNamespaces")
    projects = endpoint.get("allowedCallerProjects")
    return models.NexusEndpointSpec(
        name=name,
        target_namespace=str(endpoint.get("targetNamespace") or "primary"),
        target_task_queue=str(endpoint.get("targetTaskQueue") or ""),
        allowed_caller_namespaces=tuple(
            caller
            for caller in (callers if isinstance(callers, list) else ())
            if isinstance(caller, str) and caller
        ),
        allowed_caller_projects=tuple(
            project
            for project in (projects if isinstance(projects, list) else ())
            if isinstance(project, str) and project
        ),
    )


def repository_request(
    demo_project: dict[str, Any],
    build_name: str,
) -> models.EcrRepositoryRequest:
    return models.EcrRepositoryRequest(
        name=image_repository_name(demo_project, build_name),
        project=resources.project_name(demo_project),
        build=build_name,
        components=tuple(components_for_build(demo_project, build_name)),
    )


def secrets_manager_request(
    demo_project: dict[str, Any],
    *,
    checked_at: str,
) -> models.VerifySecretsManagerResourcesRequest:
    temporal_required = bool(resources.temporal_components(demo_project))
    auth_mode = resources.temporal_auth_mode(demo_project)
    return models.VerifySecretsManagerResourcesRequest(
        checked_at=checked_at,
        secret_prefix=resources.project_secret_prefix(demo_project),
        temporal_bootstrap=models.SecretRequirement(
            path="",  # Filled from platform config inside the activity.
            required=temporal_required,
            managed_by="platform",
            required_properties=models.TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES,
        ),
        temporal_credentials=models.SecretRequirement(
            path=resources.temporal_secret_remote_path(demo_project),
            required=temporal_required,
            managed_by="platform",
            required_properties=tuple(resources.temporal_runtime_secret_properties(demo_project)),
        ),
        temporal_mtls_ca=models.SecretRequirement(
            path=resources.temporal_mtls_ca_secret_remote_path(demo_project),
            required=temporal_required and auth_mode == resources.TEMPORAL_AUTH_MODE_MTLS,
            managed_by="platform",
            required_properties=tuple(resources.TEMPORAL_MTLS_CA_SECRET_PROPERTIES),
        ),
        user_managed=tuple(
            models.SecretRequirement(
                path=resources.user_secret_remote_path(demo_project, secret_name),
                required=True,
                managed_by=resources.user_secret_managed_by(demo_project, secret_name),
                required_properties=tuple(
                    resources.user_secret_required_properties(demo_project, secret_name)
                ),
                components=tuple(resources.components_for_user_secret(demo_project, secret_name)),
            )
            for secret_name in resources.user_secret_names(demo_project)
        ),
    )


def ecr_repositories_status(
    plan: models.ExternalResourcesPlan,
    repositories: list[models.EcrRepositoryResult],
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
    plan: models.ExternalResourcesPlan,
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
    serverless_lambda_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    phase = "Deleted"
    if (
        temporal_status.get("phase") == "Error"
        or ecr_status.get("phase") == "Error"
        or (
            isinstance(serverless_lambda_status, dict)
            and serverless_lambda_status.get("phase") == "Error"
        )
    ):
        phase = "Error"
    elif {temporal_status.get("phase"), ecr_status.get("phase")} <= {"Skipped"}:
        phase = "Skipped"
    status = {
        "phase": phase,
        "checkedAt": common.string_field(finalization_plan, "checkedAt"),
        "temporalCloud": temporal_status,
        "ecr": ecr_status,
    }
    if serverless_lambda_status is not None:
        status["serverlessLambda"] = serverless_lambda_status
    return status


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
        "checkedAt": common.string_field(finalization_plan, "checkedAt"),
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
    return f"{derive.image_repository_prefix()}/{resources.project_name(demo_project)}/{build_name}"


def components_for_build(demo_project: dict[str, Any], build_name: str) -> list[str]:
    return [
        component_name
        for component_name, component in sorted(spec_components(demo_project).items())
        if component.get("build") == build_name
    ]


def spec_components(demo_project: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = resources.spec_of(demo_project).get("components")
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
