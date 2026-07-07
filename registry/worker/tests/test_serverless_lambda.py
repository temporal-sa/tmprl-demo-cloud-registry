from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest
from temporalio.api.compute.v1 import config_pb2
from temporalio.api.deployment.v1 import message_pb2 as deployment_pb2
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from demo_registry_operator import errors
from demo_registry_operator import resources
from demo_registry_operator.activities.external_resources.aws_lambda import (
    SECRET_ENV_MANIFEST_ENV,
    SECRET_FILE_MANIFEST_ENV,
    TEMPORAL_RUNTIME_SECRET_ARN_ENV,
    TEMPORAL_RUNTIME_SECRET_ID_ENV,
    ensure_image_lambda,
    lambda_configuration_needs_update,
    lambda_description,
    lambda_environment,
)
from demo_registry_operator.activities.external_resources.iam import (
    lambda_project_secrets_policy,
    lambda_logs_policy,
    serverless_permissions_boundary_arn,
    temporal_lambda_invoke_policy,
    temporal_lambda_invoke_trust_policy,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.activities.external_resources.models import (
    EnsureLambdaFunctionRequest,
    LambdaEnvironmentVariable,
    UpsertServerlessWorkerDeploymentVersionRequest,
    ValidateServerlessWorkerComputeConfigRequest,
)
from demo_registry_operator.activities.external_resources.serverless_names import (
    lambda_execution_role_name,
    lambda_function_name,
    temporal_invoke_external_id,
    temporal_lambda_invoke_role_name,
)
from demo_registry_operator.activities.external_resources import serverless_worker
from demo_registry_operator.activities.external_resources.serverless_worker import (
    SERVERLESS_WORKER_DEPLOYMENT_RPC_TIMEOUT,
    aws_lambda_provider_details_payload,
    compute_provider_unreachable,
    create_or_update_worker_deployment_version,
    temporal_service_application_error,
    upsert_serverless_worker_deployment_version,
    validate_serverless_worker_compute_config,
)
from demo_registry_operator.workflows import demo_project_serverless_lambda
from demo_registry_operator.workflows.demo_project_serverless_lambda import (
    mtls_file_path_env,
    register_serverless_lambda_compensation,
    run_serverless_lambda_compensations,
    serverless_worker_deployment_status,
    should_compensate_serverless_lambda,
    worker_deployment_env,
)


def lambda_request() -> EnsureLambdaFunctionRequest:
    return EnsureLambdaFunctionRequest(
        project="banking-money-transfer",
        component="worker",
        function_name="tmprl-dem-cld-banking-money-transfer-worker",
        execution_role_arn="arn:aws:iam::123456789012:role/lambda",
        image_uri="123456789012.dkr.ecr.us-west-1.amazonaws.com/worker:git-abc",
        architecture="x86_64",
        memory_mb=512,
        timeout_seconds=600,
        deployment_name="money-transfer",
        build_id="git-e12b0ed9d450",
        temporal_secret_path="tmprl-dem-cld/banking-money-transfer/temporal",
        env={"APP_MODE": "demo", "TEMPORAL_API_KEY": "user-value"},
    )


def test_lambda_environment_injects_api_key_runtime_config():
    temporal_secret = {
        "arn": (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/temporal-AbCdEf"
        ),
        "name": "tmprl-dem-cld/banking-money-transfer/temporal",
    }
    environment = lambda_environment(
        lambda_request(),
        temporal_secret,
    )

    assert environment["APP_MODE"] == "demo"
    assert environment["TEMPORAL_DEPLOYMENT_NAME"] == "money-transfer"
    assert environment["TEMPORAL_WORKER_BUILD_ID"] == "git-e12b0ed9d450"
    assert environment[TEMPORAL_RUNTIME_SECRET_ARN_ENV] == temporal_secret["arn"]
    assert environment[TEMPORAL_RUNTIME_SECRET_ID_ENV] == temporal_secret["name"]
    assert "TEMPORAL_ADDRESS" not in environment
    assert "TEMPORAL_NAMESPACE" not in environment
    assert "TEMPORAL_TLS" not in environment
    assert "TEMPORAL_API_KEY" not in environment
    assert "TEMPORAL_TLS_CLIENT_CERT_DATA" not in environment
    assert "TEMPORAL_TLS_CLIENT_KEY_DATA" not in environment
    assert json.loads(environment[SECRET_ENV_MANIFEST_ENV]) == [
        {
            "name": "TEMPORAL_ADDRESS",
            "property": "address",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
        },
        {
            "name": "TEMPORAL_NAMESPACE",
            "property": "namespace",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
        },
        {
            "name": "TEMPORAL_TLS",
            "property": "tls",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
        },
        {
            "name": "TEMPORAL_API_KEY",
            "property": "apiKey",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
        },
    ]
    assert SECRET_FILE_MANIFEST_ENV not in environment


def test_lambda_environment_injects_worker_deployment_env_remaps():
    temporal_secret = {
        "arn": (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/temporal-AbCdEf"
        ),
        "name": "tmprl-dem-cld/banking-money-transfer/temporal",
    }
    request = EnsureLambdaFunctionRequest(
        **{
            **lambda_request().__dict__,
            "worker_deployment_env": (
                LambdaEnvironmentVariable(
                    name="BANKING_WORKER_DEPLOYMENT",
                    value="money-transfer",
                ),
                LambdaEnvironmentVariable(
                    name="BANKING_WORKER_BUILD_ID",
                    value="stable-money-transfer-worker",
                ),
            ),
        }
    )

    environment = lambda_environment(request, temporal_secret)

    assert environment["TEMPORAL_DEPLOYMENT_NAME"] == "money-transfer"
    assert environment["TEMPORAL_WORKER_DEPLOYMENT_NAME"] == "money-transfer"
    assert environment["TEMPORAL_WORKER_BUILD_ID"] == "git-e12b0ed9d450"
    assert environment["BANKING_WORKER_DEPLOYMENT"] == "money-transfer"
    assert environment["BANKING_WORKER_BUILD_ID"] == "stable-money-transfer-worker"


def test_lambda_environment_injects_mtls_runtime_config():
    temporal_secret = {
        "arn": (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/temporal-AbCdEf"
        ),
        "name": "tmprl-dem-cld/banking-money-transfer/temporal",
    }
    request = EnsureLambdaFunctionRequest(
        **{
            **lambda_request().__dict__,
            "temporal_auth_mode": "mtls",
            "mtls_file_path_env": (
                LambdaEnvironmentVariable(
                    name="TEMPORAL_MTLS_CERT_DIR",
                    value="/tmp/tmprl-demo-cloud/temporal",
                ),
                LambdaEnvironmentVariable(
                    name="TEMPORAL_MTLS_CA_CERT_PATH",
                    value="/tmp/tmprl-demo-cloud/temporal/ca.crt",
                ),
                LambdaEnvironmentVariable(
                    name="TEMPORAL_MTLS_CLIENT_CERT_PATH",
                    value="/tmp/tmprl-demo-cloud/temporal/tls.crt",
                ),
                LambdaEnvironmentVariable(
                    name="TEMPORAL_MTLS_CLIENT_KEY_PATH",
                    value="/tmp/tmprl-demo-cloud/temporal/tls.key",
                ),
                LambdaEnvironmentVariable(
                    name="TEMPORAL_CERT_PATH",
                    value="/tmp/tmprl-demo-cloud/temporal/tls.crt",
                ),
                LambdaEnvironmentVariable(
                    name="TEMPORAL_KEY_PATH",
                    value="/tmp/tmprl-demo-cloud/temporal/tls.key",
                ),
            ),
        }
    )
    environment = lambda_environment(
        request,
        temporal_secret,
    )

    assert "TEMPORAL_API_KEY" not in environment
    assert "TEMPORAL_TLS_CLIENT_CERT_DATA" not in environment
    assert "TEMPORAL_TLS_CLIENT_KEY_DATA" not in environment
    assert environment["TEMPORAL_CERT_PATH"] == "/tmp/tmprl-demo-cloud/temporal/tls.crt"
    assert environment["TEMPORAL_KEY_PATH"] == "/tmp/tmprl-demo-cloud/temporal/tls.key"
    assert environment["TEMPORAL_MTLS_CLIENT_CERT_PATH"] == (
        "/tmp/tmprl-demo-cloud/temporal/tls.crt"
    )
    assert json.loads(environment[SECRET_FILE_MANIFEST_ENV]) == [
        {
            "path": "/tmp/tmprl-demo-cloud/temporal/ca.crt",
            "property": "caCert",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
        },
        {
            "path": "/tmp/tmprl-demo-cloud/temporal/tls.crt",
            "property": "clientCert",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
        },
        {
            "path": "/tmp/tmprl-demo-cloud/temporal/tls.key",
            "property": "clientKey",
            "secretArn": temporal_secret["arn"],
            "secretId": temporal_secret["name"],
        },
    ]


def test_lambda_environment_includes_user_secret_env_manifest():
    temporal_secret = {
        "arn": (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/temporal-AbCdEf"
        ),
        "name": "tmprl-dem-cld/banking-money-transfer/temporal",
    }
    user_secret = {
        "name": "API_TOKEN",
        "secretArn": (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/app-token-AbCdEf"
        ),
        "secretId": "tmprl-dem-cld/banking-money-transfer/app-token",
        "property": "TOKEN",
    }

    environment = lambda_environment(
        lambda_request(),
        temporal_secret,
        (user_secret,),
    )

    manifest = json.loads(environment[SECRET_ENV_MANIFEST_ENV])
    assert user_secret in manifest


def test_lambda_mtls_file_path_env_uses_tmp_paths_and_custom_names():
    component = {
        "temporalMtls": {
            "certFiles": {
                "mountPath": "/etc/ignored",
                "pathEnv": {
                    "clientCert": "TEMPORAL_CERT_PATH",
                    "clientKey": "TEMPORAL_KEY_PATH",
                },
            }
        }
    }

    env = {item.name: item.value for item in mtls_file_path_env(component)}

    assert env["TEMPORAL_MTLS_CERT_DIR"] == "/tmp/tmprl-demo-cloud/temporal"
    assert env["TEMPORAL_MTLS_CA_CERT_PATH"] == (
        "/tmp/tmprl-demo-cloud/temporal/ca.crt"
    )
    assert env["TEMPORAL_MTLS_CLIENT_CERT_PATH"] == (
        "/tmp/tmprl-demo-cloud/temporal/tls.crt"
    )
    assert env["TEMPORAL_MTLS_CLIENT_KEY_PATH"] == (
        "/tmp/tmprl-demo-cloud/temporal/tls.key"
    )
    assert env["TEMPORAL_CERT_PATH"] == "/tmp/tmprl-demo-cloud/temporal/tls.crt"
    assert env["TEMPORAL_KEY_PATH"] == "/tmp/tmprl-demo-cloud/temporal/tls.key"


def test_serverless_worker_deployment_config_uses_serverless_block():
    component = {
        "build": "worker",
        "serverless": {
            "provider": "aws-lambda",
            "workerDeployment": {
                "deploymentName": "money-transfer",
                "buildId": "stable-money-transfer-worker",
                "env": {
                    "deploymentName": ["BANKING_WORKER_DEPLOYMENT"],
                    "buildId": ["BANKING_WORKER_BUILD_ID"],
                },
            },
        },
    }

    assert resources.worker_versioning_enabled(component) is False
    assert (
        resources.serverless_worker_deployment_name("worker", component)
        == "money-transfer"
    )
    assert (
        resources.serverless_worker_build_id(component, "git-e12b0ed9d450")
        == "stable-money-transfer-worker"
    )
    env = {
        item.name: item.value
        for item in worker_deployment_env(
            component,
            "money-transfer",
            "stable-money-transfer-worker",
        )
    }
    assert env == {
        "BANKING_WORKER_DEPLOYMENT": "money-transfer",
        "BANKING_WORKER_BUILD_ID": "stable-money-transfer-worker",
    }


def test_serverless_names_are_stable_and_within_aws_limits():
    project = "very-long-project-name-that-would-overflow-the-aws-role-name-limit"
    component = "very-long-worker-component-name"

    assert len(lambda_function_name(project, component)) <= 64
    assert len(lambda_execution_role_name(project, component)) <= 64
    assert lambda_execution_role_name(project, component).endswith("-lambda")
    assert len(temporal_lambda_invoke_role_name(project, component)) <= 64
    assert temporal_lambda_invoke_role_name(project, component).endswith("-temporal")
    assert temporal_invoke_external_id(project, component).startswith("tdc-")
    assert len(temporal_invoke_external_id(project, component)) <= 45


def test_temporal_lambda_invoke_policy_covers_versioned_function_arn():
    policy = temporal_lambda_invoke_policy(
        "arn:aws:lambda:us-west-1:123456789012:function:worker",
        "arn:aws:lambda:us-west-1:123456789012:function:worker:12",
    )

    statement = policy["Statement"][0]
    assert statement["Action"] == ["lambda:GetFunction", "lambda:InvokeFunction"]
    assert statement["Resource"] == [
        "arn:aws:lambda:us-west-1:123456789012:function:worker",
        "arn:aws:lambda:us-west-1:123456789012:function:worker:*",
        "arn:aws:lambda:us-west-1:123456789012:function:worker:12",
    ]


def test_temporal_lambda_trust_policy_sets_external_id():
    policy = temporal_lambda_invoke_trust_policy(
        principals=["arn:aws:iam::902542641901:role/wci-lambda-invoke"],
        external_id="tdc-external",
    )

    statement = policy["Statement"][0]
    assert statement["Principal"]["AWS"] == [
        "arn:aws:iam::902542641901:role/wci-lambda-invoke"
    ]
    assert statement["Condition"]["StringEquals"]["sts:ExternalId"] == "tdc-external"


def test_serverless_permissions_boundary_arn_defaults_to_project_policy():
    config = WorkerConfig.from_env()
    config = WorkerConfig(
        **{
            **config.__dict__,
            "aws_account_id": "123456789012",
        }
    )

    assert serverless_permissions_boundary_arn(config) == (
        "arn:aws:iam::123456789012:policy/"
        "tmprl-dem-cld-serverless-worker-boundary"
    )


def test_lambda_logs_policy_is_scoped_to_managed_function_prefix():
    config = WorkerConfig.from_env()
    config = WorkerConfig(
        **{
            **config.__dict__,
            "aws_account_id": "123456789012",
            "aws_region": "us-west-1",
        }
    )

    policy = lambda_logs_policy(config)

    assert policy["Statement"][0]["Resource"] == (
        "arn:aws:logs:us-west-1:123456789012:"
        "log-group:/aws/lambda/tmprl-dem-cld-*"
    )
    assert policy["Statement"][1]["Resource"] == (
        "arn:aws:logs:us-west-1:123456789012:"
        "log-group:/aws/lambda/tmprl-dem-cld-*:*"
    )


def test_lambda_project_secrets_policy_is_scoped_to_project_prefix_and_paths():
    config = WorkerConfig.from_env()
    config = WorkerConfig(
        **{
            **config.__dict__,
            "aws_account_id": "123456789012",
            "aws_region": "us-west-1",
        }
    )

    policy = lambda_project_secrets_policy(
        config,
        project="banking-money-transfer",
        secret_paths=(
            "tmprl-dem-cld/banking-money-transfer",
            "tmprl-dem-cld/banking-money-transfer/temporal",
            "tmprl-dem-cld/banking-money-transfer/app-token",
        ),
    )

    statement = policy["Statement"][0]
    assert statement["Action"] == [
        "secretsmanager:DescribeSecret",
        "secretsmanager:GetSecretValue",
    ]
    assert statement["Resource"] == [
        (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/*"
        ),
        (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/app-token-*"
        ),
        (
            "arn:aws:secretsmanager:us-west-1:123456789012:"
            "secret:tmprl-dem-cld/banking-money-transfer/temporal-*"
        ),
    ]


def test_temporal_compute_provider_payload_matches_cli_shape():
    request = UpsertServerlessWorkerDeploymentVersionRequest(
        project="banking-money-transfer",
        component="worker",
        temporal_secret_path="tmprl-dem-cld/banking-money-transfer/temporal",
        deployment_name="money-transfer",
        build_id="git-e12b0ed9d450",
        function_version_arn=(
            "arn:aws:lambda:us-west-1:123456789012:function:worker:12"
        ),
        invoke_role_arn="arn:aws:iam::123456789012:role/temporal-invoke",
        invoke_role_external_id="tdc-external",
    )

    payload = aws_lambda_provider_details_payload(request)

    assert payload.metadata["encoding"] == b"json/plain"
    assert json.loads(payload.data) == {
        "arn": "arn:aws:lambda:us-west-1:123456789012:function:worker:12",
        "role": "arn:aws:iam::123456789012:role/temporal-invoke",
        "role_external_id": "tdc-external",
    }


def test_temporal_service_cancelled_timeout_is_retryable():
    error = RPCError("Timeout expired", RPCStatusCode.CANCELLED, b"")

    application_error = temporal_service_application_error(
        error,
        operation="Upsert Temporal Serverless Worker Deployment Version worker.build",
    )

    assert application_error.type == errors.PROVIDER_TRANSIENT_ERROR
    assert application_error.non_retryable is False


def test_worker_deployment_version_create_uses_activity_owned_retry():
    class WorkflowService:
        def __init__(self):
            self.calls = []

        async def create_worker_deployment_version(self, request, *, retry, timeout):
            self.calls.append((request, retry, timeout))

    class Client:
        def __init__(self):
            self.workflow_service = WorkflowService()

    client = Client()

    action = asyncio.run(
        create_or_update_worker_deployment_version(
            client,
            namespace="tmprl-demo",
            deployment_version=deployment_pb2.WorkerDeploymentVersion(
                deployment_name="money-transfer",
                build_id="git-e12b0ed9d450",
            ),
            scaling_group=config_pb2.ComputeConfigScalingGroup(),
            identity="tmprl-demo-cloud-registry",
        )
    )

    assert action == "created"
    [(request, retry, timeout)] = client.workflow_service.calls
    assert request.namespace == "tmprl-demo"
    assert request.deployment_version.deployment_name == "money-transfer"
    assert request.deployment_version.build_id == "git-e12b0ed9d450"
    assert retry is False
    assert timeout == SERVERLESS_WORKER_DEPLOYMENT_RPC_TIMEOUT


def test_worker_deployment_version_create_requires_existing_deployment():
    class WorkflowService:
        def __init__(self):
            self.calls = []

        async def create_worker_deployment_version(self, request, *, retry, timeout):
            self.calls.append(("create-version", request, retry, timeout))
            raise RPCError("deployment not found", RPCStatusCode.NOT_FOUND, b"")

    class Client:
        def __init__(self):
            self.workflow_service = WorkflowService()

    client = Client()

    try:
        asyncio.run(
            create_or_update_worker_deployment_version(
                client,
                namespace="tmprl-demo",
                deployment_version=deployment_pb2.WorkerDeploymentVersion(
                    deployment_name="money-transfer",
                    build_id="git-e12b0ed9d450",
                ),
                scaling_group=config_pb2.ComputeConfigScalingGroup(),
                identity="tmprl-demo-cloud-registry",
            )
        )
    except RPCError as error:
        assert error.status == RPCStatusCode.NOT_FOUND
    else:
        raise AssertionError("expected missing deployment to propagate")

    assert [call[0] for call in client.workflow_service.calls] == ["create-version"]
    assert all(call[2] is False for call in client.workflow_service.calls)
    assert all(
        call[3] == SERVERLESS_WORKER_DEPLOYMENT_RPC_TIMEOUT
        for call in client.workflow_service.calls
    )


def test_serverless_worker_deployment_status_preserves_version_step_boundaries():
    status = serverless_worker_deployment_status(
        {
            "phase": "Ready",
            "namespace": "tmprl-dem-cld-banking.a2dd6",
            "deploymentName": "money-transfer",
            "buildId": "git-e12b0ed9d450",
            "functionVersionArn": (
                "arn:aws:lambda:us-west-1:123456789012:function:worker:12"
            ),
            "invokeRoleArn": "arn:aws:iam::123456789012:role/temporal-invoke",
            "externalId": "tdc-external",
            "computeProvider": "aws-lambda",
            "scaler": "no-sync",
        },
        {
            "phase": "Ready",
            "message": "Temporal validated the Lambda compute configuration.",
        },
        {
            "phase": "Ready",
            "action": "set-current",
        },
        {
            "phase": "Ready",
            "taskQueues": [{"name": "money-transfer", "type": "workflow"}],
        },
    )

    assert status["phase"] == "Ready"
    assert status["namespace"] == "tmprl-dem-cld-banking.a2dd6"
    assert status["buildId"] == "git-e12b0ed9d450"
    assert status["computeProvider"] == "aws-lambda"
    assert status["validation"]["message"] == (
        "Temporal validated the Lambda compute configuration."
    )
    assert set(status["steps"]) == {
        "upsertVersion",
        "validateComputeConfig",
        "setCurrent",
        "describeVersion",
    }


def test_serverless_worker_deployment_status_preserves_legacy_ensure_step():
    status = serverless_worker_deployment_status(
        {
            "phase": "Ready",
            "buildId": "git-e12b0ed9d450",
        },
        {
            "phase": "Ready",
        },
        {
            "phase": "Ready",
        },
        {
            "phase": "Ready",
        },
        ensure_status={
            "phase": "Ready",
            "namespace": "tmprl-dem-cld-banking.a2dd6",
            "deploymentName": "money-transfer",
            "action": "verified",
        },
    )

    assert status["namespace"] == "tmprl-dem-cld-banking.a2dd6"
    assert list(status["steps"]) == [
        "ensureDeployment",
        "upsertVersion",
        "validateComputeConfig",
        "setCurrent",
        "describeVersion",
    ]


def test_serverless_lambda_compensation_skips_established_projects():
    assert should_compensate_serverless_lambda({}) is True
    assert (
        should_compensate_serverless_lambda(
            {"status": {"activeImageVersion": "git-active"}}
        )
        is False
    )
    assert (
        should_compensate_serverless_lambda(
            {"status": {"serverlessWorkerStatus": {"phase": "Ready"}}}
        )
        is False
    )


def test_serverless_lambda_compensation_deletes_first_time_resources(monkeypatch):
    demo_project = {
        "metadata": {"name": "operator-canary-lambda-ffff370e"},
        "spec": {
            "components": {
                "worker": {
                    "build": "worker",
                    "serverless": {"provider": "aws-lambda"},
                }
            }
        },
    }
    calls = []

    async def fake_execute_activity(activity, request, **kwargs):
        calls.append((activity, request, kwargs))

    monkeypatch.setattr(
        demo_project_serverless_lambda.workflow,
        "now",
        lambda: datetime(2026, 6, 21, tzinfo=UTC),
    )
    monkeypatch.setattr(
        demo_project_serverless_lambda.workflow,
        "execute_activity",
        fake_execute_activity,
    )

    compensations = []
    register_serverless_lambda_compensation(compensations, demo_project)
    asyncio.run(run_serverless_lambda_compensations(compensations))

    [(activity, request, kwargs)] = calls
    assert (
        activity
        is demo_project_serverless_lambda.external_resources.delete_serverless_lambda_resources
    )
    assert request.checked_at == "2026-06-21T00:00:00+00:00"
    assert request.demo_project is demo_project
    assert kwargs["retry_policy"] == demo_project_serverless_lambda.COMMON_RETRY_POLICY


# --- compute-provider AssumeRole propagation retryability --------------------


COMPUTE_PROVIDER_DENIED_MESSAGE = (
    "default: cannot connect to the compute provider: failed to assume role "
    "arn:aws:iam::429214323166:role/x-worker-temporal: operation error STS: "
    "AssumeRole, https response error StatusCode: 403, api error AccessDenied: "
    "User: arn:aws:sts::819232936619:assumed-role/wci-lambda-invoke/wci-intermediary "
    "is not authorized to perform: sts:AssumeRole"
)


def _propagation_upsert_request():
    return UpsertServerlessWorkerDeploymentVersionRequest(
        project="operator-canary-lambda",
        component="worker",
        temporal_secret_path="tmprl-dem-cld/operator-canary-lambda/temporal",
        deployment_name="money-transfer",
        build_id="git-4f38b74a1f16-key-6849b6ac",
        function_version_arn="arn:aws:lambda:ca-central-1:429214323166:function:worker:3",
        invoke_role_arn="arn:aws:iam::429214323166:role/x-worker-temporal",
        invoke_role_external_id="tdc-external",
    )


def _propagation_validate_request():
    return ValidateServerlessWorkerComputeConfigRequest(
        project="operator-canary-lambda",
        component="worker",
        temporal_secret_path="tmprl-dem-cld/operator-canary-lambda/temporal",
        deployment_name="money-transfer",
        build_id="git-4f38b74a1f16-key-6849b6ac",
        function_version_arn="arn:aws:lambda:ca-central-1:429214323166:function:worker:3",
        invoke_role_arn="arn:aws:iam::429214323166:role/x-worker-temporal",
        invoke_role_external_id="tdc-external",
    )


async def _fake_connect(_path):
    return object(), "money-transfer.acct1"


def test_compute_provider_unreachable_matches_assume_role_failure():
    denied = RPCError(
        COMPUTE_PROVIDER_DENIED_MESSAGE, RPCStatusCode.FAILED_PRECONDITION, b""
    )
    unrelated = RPCError("compute config invalid", RPCStatusCode.INVALID_ARGUMENT, b"")

    assert compute_provider_unreachable(denied) is True
    assert compute_provider_unreachable(unrelated) is False


def test_upsert_retries_when_compute_provider_unreachable(monkeypatch):
    # A freshly-created invoke role whose trust policy has not yet propagated
    # surfaces as AssumeRole AccessDenied. It must be retryable (transient), not
    # a permanent ProviderConfigurationError, even though the RPC status would
    # otherwise map to non-retryable.
    async def fake_create_or_update(client, **kwargs):
        raise RPCError(
            COMPUTE_PROVIDER_DENIED_MESSAGE, RPCStatusCode.FAILED_PRECONDITION, b""
        )

    monkeypatch.setattr(
        serverless_worker,
        "connect_to_project_namespace_from_secret_path",
        _fake_connect,
    )
    monkeypatch.setattr(
        serverless_worker,
        "create_or_update_worker_deployment_version",
        fake_create_or_update,
    )

    with pytest.raises(ApplicationError) as excinfo:
        asyncio.run(
            upsert_serverless_worker_deployment_version(_propagation_upsert_request())
        )

    assert excinfo.value.type == errors.PROVIDER_TRANSIENT_ERROR
    assert excinfo.value.non_retryable is False


def test_upsert_stays_nonretryable_on_real_config_error(monkeypatch):
    async def fake_create_or_update(client, **kwargs):
        raise RPCError("compute config invalid", RPCStatusCode.INVALID_ARGUMENT, b"")

    monkeypatch.setattr(
        serverless_worker,
        "connect_to_project_namespace_from_secret_path",
        _fake_connect,
    )
    monkeypatch.setattr(
        serverless_worker,
        "create_or_update_worker_deployment_version",
        fake_create_or_update,
    )

    with pytest.raises(ApplicationError) as excinfo:
        asyncio.run(
            upsert_serverless_worker_deployment_version(_propagation_upsert_request())
        )

    assert excinfo.value.non_retryable is True
    assert excinfo.value.type == errors.PROVIDER_CONFIGURATION_ERROR


def test_validate_retries_when_compute_provider_unreachable(monkeypatch):
    async def fake_validate(client, **kwargs):
        raise RPCError(
            COMPUTE_PROVIDER_DENIED_MESSAGE, RPCStatusCode.FAILED_PRECONDITION, b""
        )

    monkeypatch.setattr(
        serverless_worker,
        "connect_to_project_namespace_from_secret_path",
        _fake_connect,
    )
    monkeypatch.setattr(
        serverless_worker,
        "validate_worker_deployment_compute_config",
        fake_validate,
    )

    with pytest.raises(ApplicationError) as excinfo:
        asyncio.run(
            validate_serverless_worker_compute_config(_propagation_validate_request())
        )

    assert excinfo.value.type == errors.PROVIDER_TRANSIENT_ERROR
    assert excinfo.value.non_retryable is False


# --- Lambda function update path (no Architectures on update) -----------------


class _FakeWaiter:
    def wait(self, **kwargs):
        return None


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return self._pages


class _FakeLambdaClient:
    def __init__(self, existing, latest, versions):
        self._existing = existing
        self._latest = latest
        self._versions = versions
        self.update_config_calls = []
        self.update_code_calls = []
        self.publish_calls = []

    def get_function(self, FunctionName):
        return self._existing

    def update_function_configuration(self, **kwargs):
        self.update_config_calls.append(kwargs)

    def update_function_code(self, **kwargs):
        self.update_code_calls.append(kwargs)

    def get_waiter(self, name):
        return _FakeWaiter()

    def get_function_configuration(self, FunctionName):
        return self._latest

    def get_paginator(self, name):
        return _FakePaginator([{"Versions": self._versions}])

    def publish_version(self, **kwargs):
        self.publish_calls.append(kwargs)
        return {"FunctionArn": "arn:aws:lambda:us-west-1:123456789012:function:x:99", "Version": "99"}


def test_lambda_configuration_needs_update_ignores_architecture():
    request = lambda_request()
    base = {
        "Role": request.execution_role_arn,
        "Timeout": request.timeout_seconds,
        "MemorySize": request.memory_mb,
        "Description": lambda_description(request),
        "Environment": {"Variables": {"APP_MODE": "demo"}},
    }
    env = {"APP_MODE": "demo"}

    # Architecture differing must NOT trigger a configuration update.
    drift_arch = {**base, "Architectures": ["arm64"]}
    assert lambda_configuration_needs_update(drift_arch, request, env) is False

    # A real, updatable attribute still triggers.
    drift_mem = {**base, "Architectures": ["x86_64"], "MemorySize": request.memory_mb + 128}
    assert lambda_configuration_needs_update(drift_mem, request, env) is True


def test_ensure_image_lambda_update_omits_architectures():
    request = lambda_request()
    env = {"APP_MODE": "demo"}
    description = lambda_description(request)
    # Existing function: same image, but Timeout drifts so the config-update path
    # fires (this is the path that previously crashed on the Architectures param).
    existing = {
        "Configuration": {
            "Role": request.execution_role_arn,
            "Timeout": request.timeout_seconds - 60,
            "MemorySize": request.memory_mb,
            "Architectures": ["x86_64"],
            "Description": description,
            "Environment": {"Variables": env},
        },
        "Code": {"ImageUri": request.image_uri},
    }
    latest = {
        "FunctionArn": "arn:aws:lambda:us-west-1:123456789012:function:worker",
        "CodeSha256": "sha",
        "Role": request.execution_role_arn,
        "Timeout": request.timeout_seconds,
        "MemorySize": request.memory_mb,
        "Description": description,
        "Architectures": ["x86_64"],
        "Environment": {"Variables": env},
    }
    published = {**latest, "Version": "1", "FunctionArn": f"{latest['FunctionArn']}:1"}
    client = _FakeLambdaClient(existing, latest, [published])

    result = ensure_image_lambda(client, request, env)

    assert len(client.update_config_calls) == 1
    assert "Architectures" not in client.update_config_calls[0]
    assert client.update_code_calls == []  # same image -> no code update
    assert client.publish_calls == []  # matching published version reused
    assert result["action"] == "updated"
    assert result["functionVersionArn"] == f"{latest['FunctionArn']}:1"
