from __future__ import annotations

from typing import Any

from temporalio import activity

from demo_registry_operator.activities.external_resources.common import (
    error_code,
    string_field,
    string_optional,
)
from demo_registry_operator.activities.external_resources.models import (
    DeleteEcrRepositoriesRequest,
    DeleteEcrRepositoriesResult,
    EcrRepositoryRequest,
    EcrRepositoryResult,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import (
    CONFIGURATION_ERROR,
    aws_application_error,
    non_retryable_error,
)


@activity.defn
def create_or_verify_ecr_repository(
    request: EcrRepositoryRequest,
) -> EcrRepositoryResult:
    config = WorkerConfig.from_env()

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=CONFIGURATION_ERROR,
        ) from error

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    try:
        status = ensure_ecr_repository(ecr, request)
    except (BotoCoreError, ClientError) as error:
        raise aws_application_error(
            error,
            operation=f"Create or verify ECR repository {request.name}",
        ) from error

    return EcrRepositoryResult(
        name=request.name,
        project=request.project,
        build=request.build,
        components=request.components,
        phase=string_field(status, "phase"),
        action=string_field(status, "action"),
        repository_uri=string_optional(status.get("repositoryUri")),
        region=config.aws_region,
        account_id=config.aws_account_id,
    )


@activity.defn
def delete_ecr_repositories(
    request: DeleteEcrRepositoriesRequest,
) -> DeleteEcrRepositoriesResult:
    config = WorkerConfig.from_env()
    if config.external_resource_mode != "aws":
        return DeleteEcrRepositoriesResult(
            {"phase": "Skipped", "mode": config.external_resource_mode}
        )

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return DeleteEcrRepositoriesResult(
            {"phase": "Error", "message": f"boto3 is required: {error}"}
        )

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    repositories: list[dict[str, Any]] = []
    for repository in request.repositories:
        try:
            ecr.delete_repository(repositoryName=repository.name, force=True)
            repositories.append({"name": repository.name, "action": "deleted"})
        except (BotoCoreError, ClientError) as error:
            if error_code(error) == "RepositoryNotFoundException":
                repositories.append({"name": repository.name, "action": "not-found"})
            else:
                repositories.append(
                    {
                        "name": repository.name,
                        "action": "error",
                        "message": str(error),
                    }
                )
    return DeleteEcrRepositoriesResult(
        {
            "phase": "Deleted",
            "checkedAt": request.checked_at,
            "repositories": repositories,
        }
    )


def ensure_ecr_repository(
    ecr: Any,
    repository: EcrRepositoryRequest,
) -> dict[str, Any]:
    try:
        response = ecr.describe_repositories(repositoryNames=[repository.name])
        repositories = response.get("repositories", [])
        existing = repositories[0] if repositories else {}
        return {
            "phase": "Ready",
            "action": "verified",
            "repositoryUri": existing.get("repositoryUri"),
        }
    except Exception as error:
        if error_code(error) != "RepositoryNotFoundException":
            raise

    response = ecr.create_repository(
        repositoryName=repository.name,
        imageScanningConfiguration={"scanOnPush": True},
        tags=[
            {"Key": "managed-by", "Value": "demo-registry"},
            {"Key": "project", "Value": repository.project},
            {"Key": "build", "Value": repository.build},
        ],
    )
    created = response.get("repository", {})
    return {
        "phase": "Ready",
        "action": "created",
        "repositoryUri": created.get("repositoryUri"),
    }
