from __future__ import annotations

import json

from demo_registry_operator.activities.external_resources.aws_lambda import (
    lambda_environment,
)
from demo_registry_operator.activities.external_resources.iam import (
    lambda_logs_policy,
    serverless_permissions_boundary_arn,
    temporal_lambda_invoke_policy,
    temporal_lambda_invoke_trust_policy,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.activities.external_resources.models import (
    EnsureLambdaFunctionRequest,
    ReconcileServerlessWorkerDeploymentRequest,
)
from demo_registry_operator.activities.external_resources.serverless_names import (
    lambda_execution_role_name,
    lambda_function_name,
    temporal_invoke_external_id,
    temporal_lambda_invoke_role_name,
)
from demo_registry_operator.activities.external_resources.serverless_worker import (
    aws_lambda_provider_details_payload,
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
        task_queue="money-transfer",
        temporal_secret_path="tmprl-dem-cld/banking-money-transfer/temporal",
        env={"APP_MODE": "demo", "TEMPORAL_API_KEY": "user-value"},
    )


def test_lambda_environment_injects_api_key_runtime_config():
    environment = lambda_environment(
        lambda_request(),
        {
            "address": "namespace.account.tmprl.cloud:7233",
            "namespace": "namespace.account",
            "tls": True,
            "apiKey": "platform-key",
        },
    )

    assert environment["APP_MODE"] == "demo"
    assert environment["TEMPORAL_ADDRESS"] == "namespace.account.tmprl.cloud:7233"
    assert environment["TEMPORAL_NAMESPACE"] == "namespace.account"
    assert environment["TEMPORAL_TASK_QUEUE"] == "money-transfer"
    assert environment["TEMPORAL_DEPLOYMENT_NAME"] == "money-transfer"
    assert environment["TEMPORAL_WORKER_BUILD_ID"] == "git-e12b0ed9d450"
    assert environment["TEMPORAL_TLS"] == "true"
    assert environment["TEMPORAL_API_KEY"] == "platform-key"
    assert "TEMPORAL_TLS_CLIENT_CERT_DATA" not in environment
    assert "TEMPORAL_TLS_CLIENT_KEY_DATA" not in environment


def test_lambda_environment_injects_mtls_runtime_config():
    environment = lambda_environment(
        lambda_request(),
        {
            "address": "namespace.account.tmprl.cloud:7233",
            "namespace": "namespace.account",
            "tls": "true",
            "clientCert": "cert-pem",
            "clientKey": "key-pem",
        },
    )

    assert "TEMPORAL_API_KEY" not in environment
    assert environment["TEMPORAL_TLS_CLIENT_CERT_DATA"] == "cert-pem"
    assert environment["TEMPORAL_TLS_CLIENT_KEY_DATA"] == "key-pem"


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


def test_temporal_compute_provider_payload_matches_cli_shape():
    request = ReconcileServerlessWorkerDeploymentRequest(
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
