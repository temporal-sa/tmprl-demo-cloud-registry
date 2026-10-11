"""Activities for an isolated Lambda workspace.

Kubernetes apply, readiness, and namespace delete stay on the crashable
activities. These activities cover the Lambda-only steps: the plan, the shared
role lookup, the invoke-policy union, and function delete. Worker Deployment
RPCs stay in the serverless activities so their payloads are not duplicated.
"""

from __future__ import annotations

import json
from typing import Any

from temporalio import activity

from demo_registry_operator import errors, kube
from demo_registry_operator.activities.external_resources.aws_lambda import (
    delete_lambda_function,
    workspace_invoke_arns,
)
from demo_registry_operator.activities.external_resources.common import error_code
from demo_registry_operator.activities.external_resources.iam import (
    TEMPORAL_LAMBDA_INVOKE_POLICY_NAME,
    iam_client,
    invoke_policy_resource_arns,
    normalize_policy,
    temporal_lambda_invoke_policy_resources,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.isolated_lambda_workspace_resources import (
    build_isolated_workspace_plan,
    isolated_lambda_settings,
    serverless_status_images,
)
from demo_registry_operator.workflows.isolated_lambda_workspace_messages import (
    BuildIsolatedWorkspacePlanRequest,
    DeleteWorkspaceLambdaFunctionsRequest,
    DeleteWorkspaceLambdaFunctionsResult,
    IsolatedWorkspacePlan,
    ResolveSharedLambdaRolesRequest,
    ResolveSharedLambdaRolesResult,
    UpdateWorkspaceLambdaInvokePolicyRequest,
    UpdateWorkspaceLambdaInvokePolicyResult,
)


@activity.defn
def build_isolated_workspace_plan_activity(
    request: BuildIsolatedWorkspacePlanRequest,
) -> IsolatedWorkspacePlan:
    demo_project = kube.read_demo_project(request.demo_name)
    if demo_project is None:
        raise errors.non_retryable_error(
            f"DemoProject {request.demo_name!r} not found",
            error_type=errors.CONFIGURATION_ERROR,
        )
    if isolated_lambda_settings(demo_project) is None:
        raise errors.non_retryable_error(
            f"demo {request.demo_name!r} is not an isolated Lambda demo "
            "(spec.registry.isolatedLambda.enabled and taskQueueEnv are required)",
            error_type=errors.CONFIGURATION_ERROR,
        )
    images = {**serverless_status_images(demo_project), **request.images}
    try:
        return build_isolated_workspace_plan(
            demo_project,
            images,
            request.workspace_id,
        )
    except (ValueError, KeyError) as error:
        raise errors.non_retryable_error(
            f"failed to render isolated workspace plan: {error}",
            error_type=errors.VALIDATION_ERROR,
        ) from error


@activity.defn
def resolve_shared_lambda_roles(
    request: ResolveSharedLambdaRolesRequest,
) -> ResolveSharedLambdaRolesResult:
    """Read the always-on role ARNs. This does not create a workspace role."""

    client = iam_client(WorkerConfig.from_env())
    return ResolveSharedLambdaRolesResult(
        execution_role_arn=_existing_role_arn(client, request.execution_role_name),
        invoke_role_arn=_existing_role_arn(client, request.invoke_role_name),
    )


def _existing_role_arn(client: Any, role_name: str) -> str:
    try:
        role = client.get_role(RoleName=role_name)["Role"]
    except Exception as error:
        if error_code(error) == "NoSuchEntity":
            raise errors.non_retryable_error(
                f"shared IAM role {role_name!r} does not exist",
                error_type=errors.CONFIGURATION_ERROR,
            ) from error
        raise errors.aws_application_error(
            error,
            operation=f"Read shared IAM role {role_name}",
        ) from error
    arn = role.get("Arn")
    if not isinstance(arn, str) or not arn:
        raise errors.non_retryable_error(
            f"shared IAM role {role_name!r} is missing an ARN",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return arn


@activity.defn
def update_workspace_lambda_invoke_policy(
    request: UpdateWorkspaceLambdaInvokePolicyRequest,
) -> UpdateWorkspaceLambdaInvokePolicyResult:
    """Union or remove workspace function ARNs on the shared invoke role.

    Replacing the policy with only the workspace ARN would stop the always-on
    demo from being invoked. A missing policy while adding ARNs is a
    configuration error. A missing policy while only removing ARNs is success.
    """

    client = iam_client(WorkerConfig.from_env())
    try:
        existing = client.get_role_policy(
            RoleName=request.role_name,
            PolicyName=TEMPORAL_LAMBDA_INVOKE_POLICY_NAME,
        )
    except Exception as error:
        if error_code(error) != "NoSuchEntity":
            raise errors.aws_application_error(
                error,
                operation=(
                    "Read Temporal Lambda invoke policy "
                    f"{request.role_name}/{TEMPORAL_LAMBDA_INVOKE_POLICY_NAME}"
                ),
            ) from error
        if not request.remove:
            raise errors.non_retryable_error(
                (
                    f"shared invoke policy on {request.role_name!r} does not exist; "
                    "refusing to create one that omits the always-on function"
                ),
                error_type=errors.CONFIGURATION_ERROR,
            ) from error
        return UpdateWorkspaceLambdaInvokePolicyResult(action="not-found")

    document = existing.get("PolicyDocument")
    document = document if isinstance(document, dict) else {}
    changed_arns = workspace_invoke_arns(
        request.function_arn,
        request.function_version_arn,
    )
    current = invoke_policy_resource_arns(document)
    updated_arns = (
        current - changed_arns if request.remove else current | changed_arns
    )
    if not updated_arns:
        raise errors.non_retryable_error(
            "Refusing to remove every ARN from the shared Lambda invoke policy",
            error_type=errors.CONFIGURATION_ERROR,
        )
    updated = temporal_lambda_invoke_policy_resources(updated_arns)
    if normalize_policy(document) == normalize_policy(updated):
        return UpdateWorkspaceLambdaInvokePolicyResult(action="verified")
    try:
        client.put_role_policy(
            RoleName=request.role_name,
            PolicyName=TEMPORAL_LAMBDA_INVOKE_POLICY_NAME,
            PolicyDocument=json.dumps(updated, sort_keys=True),
        )
    except Exception as error:
        raise errors.aws_application_error(
            error,
            operation=(
                "Update Temporal Lambda invoke policy "
                f"{request.role_name}/{TEMPORAL_LAMBDA_INVOKE_POLICY_NAME}"
            ),
        ) from error
    return UpdateWorkspaceLambdaInvokePolicyResult(action="updated")


@activity.defn
def delete_workspace_lambda_functions(
    request: DeleteWorkspaceLambdaFunctionsRequest,
) -> DeleteWorkspaceLambdaFunctionsResult:
    """Delete workspace functions only. Shared functions and both IAM roles stay."""

    config = WorkerConfig.from_env()
    try:
        import boto3
    except ImportError as error:
        raise errors.non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error

    lambda_client = boto3.session.Session(region_name=config.aws_region).client(
        "lambda"
    )
    functions: list[dict[str, str]] = []
    for function_name in request.function_names:
        result = delete_lambda_function(lambda_client, function_name)
        if result.get("action") == "error":
            raise errors.retryable_provider_error(
                f"Delete workspace Lambda function {function_name} failed: "
                f"{result.get('message', '')}"
            )
        functions.append(
            {
                "name": str(result.get("name", function_name)),
                "action": str(result.get("action", "")),
            }
        )
    return DeleteWorkspaceLambdaFunctionsResult(functions=tuple(functions))
