from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

from temporalio.api.compute.v1 import config_pb2
from temporalio.api.deployment.v1 import message_pb2 as deployment_pb2
from temporalio.service import RPCError, RPCStatusCode

from demo_registry_operator import errors
from demo_registry_operator import resources
from demo_registry_operator.activities.external_resources.aws_lambda import (
    SECRET_ENV_MANIFEST_ENV,
    SECRET_FILE_MANIFEST_ENV,
    TEMPORAL_RUNTIME_SECRET_ARN_ENV,
    TEMPORAL_RUNTIME_SECRET_ID_ENV,
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
)
from demo_registry_operator.activities.external_resources.serverless_names import (
    lambda_execution_role_name,
    lambda_function_name,
    temporal_invoke_external_id,
    temporal_lambda_invoke_role_name,
)
from demo_registry_operator.activities.external_resources.serverless_worker import (
    SERVERLESS_WORKER_DEPLOYMENT_RPC_TIMEOUT,
    aws_lambda_provider_details_payload,
    create_or_update_worker_deployment_version,
    temporal_service_application_error,
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
