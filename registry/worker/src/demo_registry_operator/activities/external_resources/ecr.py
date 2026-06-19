from typing import Any

from temporalio import activity

from demo_registry_operator import errors
from demo_registry_operator.activities.external_resources import common, models
from demo_registry_operator.config import WorkerConfig


@activity.defn
def create_or_verify_ecr_repository(
    request: models.EcrRepositoryRequest,
) -> models.EcrRepositoryResult:
    config = WorkerConfig.from_env()

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise errors.non_retryable_error(
            f"boto3 is required for aws external resource mode: {error}",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    try:
        status = ensure_ecr_repository(ecr, request)
    except (BotoCoreError, ClientError) as error:
        raise errors.aws_application_error(
            error,
            operation=f"Create or verify ECR repository {request.name}",
        ) from error

    return models.EcrRepositoryResult(
        name=request.name,
        project=request.project,
        build=request.build,
        components=request.components,
        phase=common.string_field(status, "phase"),
        action=common.string_field(status, "action"),
        repository_uri=common.string_optional(status.get("repositoryUri")),
        region=config.aws_region,
        account_id=config.aws_account_id,
    )


@activity.defn
def delete_ecr_repositories(
    request: models.DeleteEcrRepositoriesRequest,
) -> models.DeleteEcrRepositoriesResult:
    config = WorkerConfig.from_env()
    if config.external_resource_mode != "aws":
        return models.DeleteEcrRepositoriesResult(
            {"phase": "Skipped", "mode": config.external_resource_mode}
        )

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        return models.DeleteEcrRepositoriesResult(
            {"phase": "Error", "message": f"boto3 is required: {error}"}
        )

    ecr = boto3.session.Session(region_name=config.aws_region).client("ecr")
    repositories: list[dict[str, Any]] = []
    for repository in request.repositories:
        try:
            ecr.delete_repository(repositoryName=repository.name, force=True)
            repositories.append({"name": repository.name, "action": "deleted"})
        except (BotoCoreError, ClientError) as error:
            if common.error_code(error) == "RepositoryNotFoundException":
                repositories.append({"name": repository.name, "action": "not-found"})
            else:
                repositories.append(
                    {
                        "name": repository.name,
                        "action": "error",
                        "message": str(error),
                    }
                )
    return models.DeleteEcrRepositoriesResult(
        {
            "phase": "Deleted",
            "checkedAt": request.checked_at,
            "repositories": repositories,
        }
    )


def ensure_ecr_repository(
    ecr: Any,
    repository: models.EcrRepositoryRequest,
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
        if common.error_code(error) != "RepositoryNotFoundException":
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
