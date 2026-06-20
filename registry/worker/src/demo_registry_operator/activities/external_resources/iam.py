from __future__ import annotations

import json
import os
from typing import Any

from temporalio import activity

from demo_registry_operator import derive, errors
from demo_registry_operator.activities.external_resources import common, models
from demo_registry_operator.config import WorkerConfig


LAMBDA_EXECUTION_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}
LAMBDA_LOGS_POLICY_NAME = "LambdaBasicExecution"
LAMBDA_PROJECT_SECRETS_POLICY_NAME = "LambdaProjectSecrets"
TEMPORAL_LAMBDA_INVOKE_POLICY_NAME = "TemporalLambdaInvoke"
TEMPORAL_CLOUD_LAMBDA_INVOKE_PRINCIPALS_ENV = (
    "DEMO_REGISTRY_TEMPORAL_CLOUD_LAMBDA_INVOKE_PRINCIPALS"
)
DEFAULT_TEMPORAL_CLOUD_LAMBDA_INVOKE_PRINCIPALS = (
    "arn:aws:iam::902542641901:role/wci-lambda-invoke",
    "arn:aws:iam::160190466495:role/wci-lambda-invoke",
    "arn:aws:iam::819232936619:role/wci-lambda-invoke",
    "arn:aws:iam::829909441867:role/wci-lambda-invoke",
    "arn:aws:iam::354116250941:role/wci-lambda-invoke",
)
SERVERLESS_PERMISSIONS_BOUNDARY_ENV = (
    "DEMO_REGISTRY_SERVERLESS_PERMISSIONS_BOUNDARY_ARN"
)
DEFAULT_SERVERLESS_PERMISSIONS_BOUNDARY_NAME = (
    "tmprl-dem-cld-serverless-worker-boundary"
)


@activity.defn
def ensure_lambda_execution_role(
    request: models.EnsureLambdaExecutionRoleRequest,
) -> models.EnsureLambdaExecutionRoleResult:
    config = WorkerConfig.from_env()
    iam = iam_client(config)
    try:
        result = ensure_role(
            iam,
            role_name=request.role_name,
            trust_policy=LAMBDA_EXECUTION_TRUST_POLICY,
            description=(
                "Execution role for a tmprl-demo.cloud Temporal Serverless "
                f"Worker Lambda ({request.project}/{request.component})"
            ),
            tags=aws_tags(request.project, request.component),
            permissions_boundary=serverless_permissions_boundary_arn(config),
        )
        policy_action = ensure_inline_policy(
            iam,
            role_name=request.role_name,
            policy_name=LAMBDA_LOGS_POLICY_NAME,
            policy_document=lambda_logs_policy(config),
        )
        secrets_policy_action = ensure_inline_policy(
            iam,
            role_name=request.role_name,
            policy_name=LAMBDA_PROJECT_SECRETS_POLICY_NAME,
            policy_document=lambda_project_secrets_policy(
                config,
                project=request.project,
                secret_paths=request.secret_paths,
            ),
        )
    except Exception as error:
        raise errors.aws_application_error(
            error,
            operation=f"Create or verify Lambda execution role {request.role_name}",
        ) from error

    status = {
        "phase": "Ready",
        "action": aggregate_action(
            result["action"],
            policy_action,
            secrets_policy_action,
        ),
        "roleName": request.role_name,
        "roleArn": result["arn"],
        "policy": LAMBDA_LOGS_POLICY_NAME,
        "policies": [
            LAMBDA_LOGS_POLICY_NAME,
            LAMBDA_PROJECT_SECRETS_POLICY_NAME,
        ],
    }
    return models.EnsureLambdaExecutionRoleResult(
        status=status,
        role_name=request.role_name,
        role_arn=str(result["arn"]),
    )


@activity.defn
def ensure_temporal_lambda_invoke_role(
    request: models.EnsureTemporalLambdaInvokeRoleRequest,
) -> models.EnsureTemporalLambdaInvokeRoleResult:
    config = WorkerConfig.from_env()
    iam = iam_client(config)
    principals = temporal_cloud_lambda_invoke_principals()
    trust_policy = temporal_lambda_invoke_trust_policy(
        principals=principals,
        external_id=request.external_id,
    )
    policy_document = temporal_lambda_invoke_policy(
        request.function_arn,
        request.function_version_arn,
    )
    try:
        result = ensure_role(
            iam,
            role_name=request.role_name,
            trust_policy=trust_policy,
            description=(
                "Role Temporal Cloud assumes to invoke a tmprl-demo.cloud "
                f"Serverless Worker Lambda ({request.project}/{request.component})"
            ),
            tags=aws_tags(request.project, request.component),
            permissions_boundary=serverless_permissions_boundary_arn(config),
        )
        policy_action = ensure_inline_policy(
            iam,
            role_name=request.role_name,
            policy_name=TEMPORAL_LAMBDA_INVOKE_POLICY_NAME,
            policy_document=policy_document,
        )
    except Exception as error:
        raise errors.aws_application_error(
            error,
            operation=f"Create or verify Temporal Lambda invoke role {request.role_name}",
        ) from error

    status = {
        "phase": "Ready",
        "action": aggregate_action(result["action"], policy_action),
        "roleName": request.role_name,
        "roleArn": result["arn"],
        "externalId": request.external_id,
        "trustedPrincipals": principals,
        "policy": TEMPORAL_LAMBDA_INVOKE_POLICY_NAME,
        "functionArn": request.function_arn,
        "functionVersionArn": request.function_version_arn,
    }
    return models.EnsureTemporalLambdaInvokeRoleResult(
        status=status,
        role_name=request.role_name,
        role_arn=str(result["arn"]),
        external_id=request.external_id,
    )


def iam_client(config: WorkerConfig) -> Any:
    try:
        import boto3
    except ImportError as error:
        raise errors.non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error

    return boto3.session.Session(region_name=config.aws_region).client("iam")


def ensure_role(
    iam: Any,
    *,
    role_name: str,
    trust_policy: dict[str, Any],
    description: str,
    tags: list[dict[str, str]],
    permissions_boundary: str,
) -> dict[str, str]:
    try:
        response = iam.get_role(RoleName=role_name)
        role = response["Role"]
        action = "verified"
    except Exception as error:
        if common.error_code(error) != "NoSuchEntity":
            raise
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy, sort_keys=True),
            Description=description,
            Tags=tags,
            PermissionsBoundary=permissions_boundary,
        )
        waiter = iam.get_waiter("role_exists")
        waiter.wait(RoleName=role_name)
        role = response["Role"]
        action = "created"

    current_boundary = role.get("PermissionsBoundary")
    current_boundary_arn = (
        current_boundary.get("PermissionsBoundaryArn")
        if isinstance(current_boundary, dict)
        else None
    )
    if current_boundary_arn != permissions_boundary:
        raise errors.non_retryable_error(
            (
                f"IAM role {role_name} is missing the required permissions "
                f"boundary {permissions_boundary}"
            ),
            error_type=errors.CONFIGURATION_ERROR,
        )

    if normalize_policy(role.get("AssumeRolePolicyDocument")) != normalize_policy(
        trust_policy
    ):
        iam.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(trust_policy, sort_keys=True),
        )
        action = "updated"

    return {"arn": str(role["Arn"]), "action": action}


def ensure_inline_policy(
    iam: Any,
    *,
    role_name: str,
    policy_name: str,
    policy_document: dict[str, Any],
) -> str:
    action = "verified"
    try:
        existing = iam.get_role_policy(RoleName=role_name, PolicyName=policy_name)
        if normalize_policy(existing.get("PolicyDocument")) == normalize_policy(
            policy_document
        ):
            return action
        action = "updated"
    except Exception as error:
        if common.error_code(error) != "NoSuchEntity":
            raise
        action = "created"

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_document, sort_keys=True),
    )
    return action


def lambda_logs_policy(config: WorkerConfig) -> dict[str, Any]:
    account = config.aws_account_id or "*"
    region = config.aws_region
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "logs:CreateLogGroup",
                "Resource": (
                    f"arn:aws:logs:{region}:{account}:"
                    "log-group:/aws/lambda/tmprl-dem-cld-*"
                ),
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": (
                    f"arn:aws:logs:{region}:{account}:"
                    "log-group:/aws/lambda/tmprl-dem-cld-*:*"
                ),
            },
        ],
    }


def lambda_project_secrets_policy(
    config: WorkerConfig,
    *,
    project: str,
    secret_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    account = required_aws_account_id(config)
    region = config.aws_region
    resources = sorted(
        {
            secret_resource_arn(config, pattern)
            for pattern in lambda_secret_resource_patterns(project, secret_paths)
        }
    )
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:DescribeSecret",
                    "secretsmanager:GetSecretValue",
                ],
                "Resource": resources
                or (
                    f"arn:aws:secretsmanager:{region}:{account}:"
                    f"secret:{derive.secret_prefix(project)}/*"
                ),
            }
        ],
    }


def lambda_secret_resource_patterns(
    project: str,
    secret_paths: tuple[str, ...],
) -> list[str]:
    project_prefix = derive.secret_prefix(project)
    patterns = {f"{project_prefix}/*"}
    for path in secret_paths:
        cleaned = normalized_secret_path(path)
        if not cleaned:
            continue
        if cleaned == project_prefix:
            patterns.add(f"{cleaned}/*")
        else:
            patterns.add(f"{cleaned}-*")
    return sorted(patterns)


def normalized_secret_path(path: str) -> str:
    cleaned = path.strip().strip("/")
    if "*" in cleaned or "?" in cleaned:
        raise errors.non_retryable_error(
            f"Secrets Manager path {path!r} must not contain wildcard characters",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return cleaned


def secret_resource_arn(config: WorkerConfig, secret_name_pattern: str) -> str:
    account = required_aws_account_id(config)
    return (
        f"arn:aws:secretsmanager:{config.aws_region}:{account}:"
        f"secret:{secret_name_pattern}"
    )


def required_aws_account_id(config: WorkerConfig) -> str:
    if not config.aws_account_id:
        raise errors.non_retryable_error(
            "DEMO_REGISTRY_AWS_ACCOUNT_ID is required to scope Lambda secret access",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return config.aws_account_id


def temporal_lambda_invoke_trust_policy(
    *,
    principals: list[str],
    external_id: str,
) -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": principals},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": external_id,
                    }
                },
            }
        ],
    }


def temporal_lambda_invoke_policy(
    function_arn: str,
    function_version_arn: str,
) -> dict[str, Any]:
    resources = sorted(
        {
            function_arn,
            f"{function_arn}:*",
            function_version_arn,
        }
    )
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "lambda:GetFunction",
                    "lambda:InvokeFunction",
                ],
                "Resource": resources,
            }
        ],
    }


def temporal_cloud_lambda_invoke_principals() -> list[str]:
    configured = os.getenv(TEMPORAL_CLOUD_LAMBDA_INVOKE_PRINCIPALS_ENV, "")
    values = [
        item.strip()
        for item in configured.split(",")
        if item.strip()
    ]
    return values or list(DEFAULT_TEMPORAL_CLOUD_LAMBDA_INVOKE_PRINCIPALS)


def serverless_permissions_boundary_arn(config: WorkerConfig) -> str:
    configured = os.getenv(SERVERLESS_PERMISSIONS_BOUNDARY_ENV, "").strip()
    if configured:
        return configured
    if not config.aws_account_id:
        raise errors.non_retryable_error(
            (
                "DEMO_REGISTRY_AWS_ACCOUNT_ID is required to derive the "
                "serverless worker permissions boundary ARN"
            ),
            error_type=errors.CONFIGURATION_ERROR,
        )
    return (
        f"arn:aws:iam::{config.aws_account_id}:policy/"
        f"{DEFAULT_SERVERLESS_PERMISSIONS_BOUNDARY_NAME}"
    )


def aws_tags(project: str, component: str) -> list[dict[str, str]]:
    return [
        {"Key": "managed-by", "Value": "demo-registry"},
        {"Key": "project", "Value": project},
        {"Key": "component", "Value": component},
    ]


def normalize_policy(value: Any) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"))


def aggregate_action(*actions: str) -> str:
    if "created" in actions:
        return "created"
    if "updated" in actions:
        return "updated"
    return "verified"
