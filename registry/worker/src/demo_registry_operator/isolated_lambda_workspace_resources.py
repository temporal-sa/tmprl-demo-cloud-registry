"""Pure plan for one user's isolated Lambda workspace.

The Kubernetes half reuses ``build_workspace_manifests`` without crashable-worker
labels. Serverless components stay off that plan: each becomes a new Lambda
function and a version on a workspace-suffixed Worker Deployment.
"""

from __future__ import annotations

from typing import Any

from demo_registry_operator import resources
from demo_registry_operator.activities.external_resources import serverless_names
from demo_registry_operator.crashable_workspace_resources import (
    build_workspace_manifests,
)
from demo_registry_operator.workflows.demo_project_serverless_lambda import (
    literal_env,
    mtls_file_path_env,
    secret_env,
    worker_deployment_env,
)
from demo_registry_operator.workflows.isolated_lambda_workspace_messages import (
    IsolatedLambdaComponentPlan,
    IsolatedWorkspacePlan,
)


def isolated_lambda_settings(demo_project: dict[str, Any]) -> dict[str, Any] | None:
    """Return the isolation block when both eligibility fields are set."""

    registry = demo_project.get("spec")
    registry = registry.get("registry") if isinstance(registry, dict) else None
    isolated = registry.get("isolatedLambda") if isinstance(registry, dict) else None
    if not isinstance(isolated, dict):
        return None
    task_queue_env = isolated.get("taskQueueEnv")
    if isolated.get("enabled") is True and isinstance(task_queue_env, str) and task_queue_env:
        return isolated
    return None


def serverless_status_images(demo_project: dict[str, Any]) -> dict[str, str]:
    """Read Lambda images, falling back to promoted images for pruned status."""

    status = demo_project.get("status")
    if not isinstance(status, dict):
        return {}
    active_images = status.get("activeImages")
    active_images = active_images if isinstance(active_images, dict) else {}
    serverless = status.get("serverlessWorkerStatus")
    component_status = (
        serverless.get("components") if isinstance(serverless, dict) else None
    )
    component_status = component_status if isinstance(component_status, dict) else {}
    images: dict[str, str] = {}
    for name, component in resources.serverless_lambda_components(demo_project).items():
        recorded = component_status.get(name)
        image = recorded.get("image") if isinstance(recorded, dict) else None
        build_name = component.get("build")
        if not isinstance(build_name, str) or not build_name:
            continue
        if not isinstance(image, str) or not image:
            image = active_images.get(build_name)
        if isinstance(image, str) and image:
            images[build_name] = image
    return images


def build_isolated_workspace_plan(
    demo_project: dict[str, Any],
    images: dict[str, str],
    workspace_id: str,
) -> IsolatedWorkspacePlan:
    settings = isolated_lambda_settings(demo_project)
    if settings is None:
        raise ValueError(
            "demo is not an isolated Lambda workspace "
            "(spec.registry.isolatedLambda.enabled and taskQueueEnv are required)"
        )

    manifests, task_queue, host, namespace, deployment_names = build_workspace_manifests(
        demo_project,
        images,
        workspace_id,
        label_workers=False,
        isolation=settings,
    )
    task_queue_env = str(settings["taskQueueEnv"])
    project = resources.project_name(demo_project)
    temporal_secret_path = (
        resources.temporal_runtime_secret_path_from_status(demo_project.get("status"))
        or resources.temporal_secret_remote_path(demo_project)
    )
    temporal_auth_mode = resources.temporal_auth_mode(demo_project)
    serverless_components = list(resources.serverless_lambda_components(demo_project).items())
    current_choices = _initial_current_choices(serverless_components)
    default_build_id = _default_build_id(demo_project, images, serverless_components)

    components: list[IsolatedLambdaComponentPlan] = []
    for component_name, component in serverless_components:
        source_deployment = resources.serverless_worker_deployment_name(
            component_name,
            component,
        )
        deployment_name = resources.suffixed_dns_name(source_deployment, workspace_id)
        build_id = resources.serverless_worker_build_id(component, default_build_id)
        if not build_id:
            raise ValueError(
                f"serverless component {component_name!r} has no build ID"
            )
        build_name = component.get("build")
        image_uri = images.get(build_name) if isinstance(build_name, str) else None
        if not isinstance(image_uri, str) or not image_uri:
            raise ValueError(
                f"serverless component {component_name!r} build {build_name!r} "
                "has no resolved image"
            )
        serverless = resources.serverless_config(component)
        env = literal_env(component)
        env[task_queue_env] = task_queue
        current_component, _defaulted = current_choices[source_deployment]
        components.append(
            IsolatedLambdaComponentPlan(
                project=project,
                component=component_name,
                function_name=serverless_names.workspace_lambda_function_name(
                    project,
                    component_name,
                    workspace_id,
                ),
                deployment_name=deployment_name,
                build_id=build_id,
                image_uri=image_uri,
                architecture=str(serverless.get("architecture", "x86_64")),
                memory_mb=int(serverless.get("memoryMb", 512)),
                timeout_seconds=int(serverless.get("timeoutSeconds", 600)),
                execution_role_name=serverless_names.lambda_execution_role_name(
                    project,
                    component_name,
                ),
                invoke_role_name=serverless_names.temporal_lambda_invoke_role_name(
                    project,
                    component_name,
                ),
                external_id=serverless_names.temporal_invoke_external_id(
                    project,
                    component_name,
                ),
                env=env,
                temporal_secret_path=temporal_secret_path,
                temporal_auth_mode=temporal_auth_mode,
                secret_env=secret_env(component, demo_project),
                mtls_file_path_env=mtls_file_path_env(component),
                worker_deployment_env=worker_deployment_env(
                    component,
                    deployment_name,
                    build_id,
                ),
                set_current=current_component == component_name,
            )
        )

    initial_current = {
        resources.suffixed_dns_name(deployment, workspace_id): component_name
        for deployment, (component_name, _defaulted) in current_choices.items()
    }
    defaulted = tuple(
        sorted(
            resources.suffixed_dns_name(deployment, workspace_id)
            for deployment, (_component_name, was_defaulted) in current_choices.items()
            if was_defaulted
        )
    )
    return IsolatedWorkspacePlan(
        resources=manifests,
        task_queue=task_queue,
        host=host,
        namespace=namespace,
        deployment_names=deployment_names,
        components=tuple(components),
        initial_current=initial_current,
        initial_current_defaulted=defaulted,
    )


def _default_build_id(
    demo_project: dict[str, Any],
    images: dict[str, str],
    serverless_components: list[tuple[str, dict[str, Any]]],
) -> str:
    needs_default = any(
        not _configured_build_id(component) for _name, component in serverless_components
    )
    if not needs_default:
        return ""
    return resources.candidate_version(demo_project, images) or ""


def _configured_build_id(component: dict[str, Any]) -> str:
    build_id = resources.serverless_worker_deployment_config(component).get("buildId")
    return build_id if isinstance(build_id, str) else ""


def _initial_current_choices(
    serverless_components: list[tuple[str, dict[str, Any]]],
) -> dict[str, tuple[str, bool]]:
    """Source deployment name -> (component, whether the choice was lexicographic)."""

    groups: dict[str, list[str]] = {}
    marked: dict[str, str] = {}
    for component_name, component in serverless_components:
        deployment = resources.serverless_worker_deployment_name(
            component_name,
            component,
        )
        groups.setdefault(deployment, []).append(component_name)
        if (
            resources.serverless_worker_deployment_config(component).get(
                "initialCurrent"
            )
            is True
        ):
            marked[deployment] = component_name
    choices: dict[str, tuple[str, bool]] = {}
    for deployment, names in groups.items():
        if deployment in marked:
            choices[deployment] = (marked[deployment], False)
        else:
            choices[deployment] = (sorted(names)[0], True)
    return choices
