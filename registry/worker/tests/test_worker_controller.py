from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from temporalio.exceptions import ApplicationError

from demo_registry_operator.activities import canary
from demo_registry_operator.workflows.messages import (
    DemoProjectFinalizeSignal,
    ForceReconcileSignal,
)
from demo_registry_operator.activities import reconcile
from demo_registry_operator.resources import (
    FINALIZATION_POLICY_ANNOTATION,
    IMAGE_OVERRIDES_ANNOTATION,
    REGISTRY_CATALOG_ANNOTATION,
    SOURCE_COMMIT_ANNOTATION,
    TEMPORAL_API_KEY_ID_ANNOTATION,
    base_resources,
    candidate_version,
    candidate_service_status,
    deployment_resources,
    rbac_resources,
    runtime_secret_resources,
    runtime_namespace,
    runtime_workload_resources,
    source_git_repository,
    worker_controller_auth_secret_name,
    worker_deployment_status,
    worker_deployment_rollout_wait_seconds,
)


ROOT = Path(__file__).resolve().parents[3]


def plan_registry_operator_canary(request):
    return canary.prepare_registry_operator_canary_plan(
        canary.RegistryOperatorCanaryPlanRequest(
            demo_project_template=(
                request.get("demoProjectTemplate")
                or request.get("demoProject")
                or {}
            ),
            workflow_id=request.get("workflowID", "operator-canary"),
            run_id=request.get("runID", ""),
            project_name_prefix=request.get("projectNamePrefix", "operator-canary"),
            project_name=request.get("projectName"),
            expected_source_commit_sha=request.get("expectedSourceCommitSha"),
            expected_images=request.get("expectedImages"),
            auth_mode=request.get("authMode"),
            expected_operator_image=request.get("expectedOperatorImage"),
            expected_operator_source_sha=request.get("expectedOperatorSourceSha"),
            timeout_seconds=int(request.get("timeoutSeconds", 900)),
            poll_seconds=int(request.get("pollSeconds", 10)),
            cleanup_timeout_seconds=int(request.get("cleanupTimeoutSeconds", 600)),
            cleanup_policy=request.get("cleanupPolicy", "Always"),
        )
    ).plan


def project(*, worker: object, smoke_check: dict | None = None) -> dict:
    spec = {
        "source": {
            "repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        },
        "builds": {
            "api": {"context": ".", "dockerfile": "docker/api.Dockerfile"},
            "worker": {"context": ".", "dockerfile": "docker/worker.Dockerfile"},
        },
        "components": {
            "api": {
                "build": "api",
                "ports": [{"containerPort": 8000, "servicePort": 80}],
                "temporalAccess": True,
            },
            "worker": {
                "build": "worker",
                "worker": worker,
                "env": [{"name": "APP_MODE", "value": "demo"}],
            },
        },
        "ingress": {"subdomain": "banking-app"},
    }
    if smoke_check:
        spec["smokeCheck"] = smoke_check
    return {
        "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
        "kind": "DemoProject",
        "metadata": {
            "name": "banking-money-transfer",
            "annotations": {
                SOURCE_COMMIT_ANNOTATION: (
                    "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
                )
            },
        },
        "spec": spec,
    }


def images() -> dict[str, str]:
    return {
        "api": "example.com/api:git-e12b0ed9d450",
        "worker": "example.com/worker:git-e12b0ed9d450",
    }


def external_resources() -> dict:
    return {
        "temporalCloud": {
            "namespace": {
                "name": "tmprl-dem-cld-banking-money-transfer.acct1",
                "grpcAddress": (
                    "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233"
                ),
            }
        }
    }


def versioned_worker() -> dict:
    return {
        "versioning": {
            "enabled": True,
            "deploymentName": "money-transfer",
            "rollout": {
                "strategy": "Progressive",
                "steps": [
                    {"rampPercentage": 10, "pauseDuration": "5m"},
                    {"rampPercentage": 50, "pauseDuration": "10m"},
                ],
                "gate": {
                    "workflowType": "WorkerCanary",
                    "input": {"project": "banking-money-transfer"},
                },
            },
            "sunset": {
                "scaledownDelay": "2h",
                "deleteDelay": "48h",
            },
        }
    }


def test_plain_worker_still_renders_deployment():
    demo_project = project(worker=True)

    deployments = deployment_resources(demo_project, images())

    assert {item["metadata"]["name"] for item in deployments} == {
        "api-git-e12b0ed9d450",
        "worker-git-e12b0ed9d450",
    }


def test_component_can_receive_generated_rollout_version():
    demo_project = project(worker=True)
    worker = demo_project["spec"]["components"]["worker"]
    worker["env"].append(
        {
            "name": "TEMPORAL_WORKER_BUILD_ID",
            "valueFrom": {"inputRef": "platform.rolloutVersion"},
        }
    )

    deployments = deployment_resources(demo_project, images())
    deployment = next(
        item for item in deployments if item["metadata"]["name"].startswith("worker-")
    )
    container = deployment["spec"]["template"]["spec"]["containers"][0]

    assert {
        "name": "TEMPORAL_WORKER_BUILD_ID",
        "value": "git-e12b0ed9d450",
    } in container["env"]


def test_temporal_api_key_id_participates_in_rollout_version():
    demo_project = project(worker=True)
    demo_project["metadata"]["annotations"][TEMPORAL_API_KEY_ID_ANNOTATION] = "key-old"
    old_version = candidate_version(demo_project, images())

    updated = project(worker=True)
    updated["metadata"]["annotations"][TEMPORAL_API_KEY_ID_ANNOTATION] = "key-new"
    new_version = candidate_version(updated, images())

    assert old_version is not None
    assert new_version is not None
    assert old_version != new_version
    assert old_version.startswith("git-e12b0ed9d450-key-")
    assert new_version.startswith("git-e12b0ed9d450-key-")


def test_temporal_proxy_worker_renders_sidecar_and_local_app_env(monkeypatch):
    monkeypatch.setenv(
        "DEMO_REGISTRY_TEMPORAL_PROXY_IMAGE",
        "example.com/temporal-cloud-proxy:git-e12b0ed9d450",
    )
    demo_project = project(worker=True)
    worker = demo_project["spec"]["components"]["worker"]
    worker["temporalProxy"] = {
        "enabled": True,
        "addressEnv": ["TEMPORAL_HOST", "TEMPORAL_GRPC_ENDPOINT"],
    }
    worker["env"].append({"name": "TEMPORAL_HOST", "value": "localhost:7233"})

    secrets = runtime_secret_resources(demo_project)
    deployments = deployment_resources(demo_project, images())
    worker_deployment = next(
        item for item in deployments if item["metadata"]["name"].startswith("worker-")
    )
    containers = worker_deployment["spec"]["template"]["spec"]["containers"]

    assert {item["metadata"]["name"] for item in secrets} == {"temporal-credentials"}
    assert [item["name"] for item in containers] == [
        "worker",
        "temporal-cloud-proxy",
    ]

    app = containers[0]
    assert "envFrom" not in app
    assert {"name": "APP_MODE", "value": "demo"} in app["env"]
    assert {"name": "TEMPORAL_HOST", "value": "127.0.0.1:7233"} in app["env"]
    assert {
        "name": "TEMPORAL_GRPC_ENDPOINT",
        "value": "127.0.0.1:7233",
    } in app["env"]
    assert {"name": "TEMPORAL_NAMESPACE", "value": "default"} in app["env"]
    assert {"name": "TEMPORAL_TLS", "value": "false"} in app["env"]

    proxy = containers[1]
    assert proxy["image"] == "example.com/temporal-cloud-proxy:git-e12b0ed9d450"
    assert proxy["envFrom"] == [{"secretRef": {"name": "temporal-credentials"}}]
    assert {
        "name": "TEMPORAL_PROXY_LISTEN_ADDRESS",
        "value": "127.0.0.1:7233",
    } in proxy["env"]


def test_component_env_can_map_temporal_and_user_secret_inputs():
    demo_project = project(worker=False)
    demo_project["spec"]["secrets"] = {
        "anthropic-credentials": {
            "requiredProperties": ["ANTHROPIC_API_KEY"],
        }
    }
    api = demo_project["spec"]["components"]["api"]
    api.pop("temporalAccess")
    api["env"] = [
        {
            "name": "TEMPORAL_HOST",
            "valueFrom": {"inputRef": "temporal.address"},
        },
        {
            "name": "MODEL_API_KEY",
            "valueFrom": {
                "inputRef": "secrets.anthropic-credentials.ANTHROPIC_API_KEY",
            },
        },
    ]

    secrets = runtime_secret_resources(demo_project)
    deployments = deployment_resources(demo_project, images())
    api_deployment = next(
        item for item in deployments if item["metadata"]["name"].startswith("api-")
    )
    container = api_deployment["spec"]["template"]["spec"]["containers"][0]

    assert {item["metadata"]["name"] for item in secrets} == {
        "temporal-credentials",
        "user-secret-anthropic-credentials",
    }
    assert "envFrom" not in container
    assert {
        "name": "TEMPORAL_HOST",
        "valueFrom": {
            "secretKeyRef": {
                "name": "temporal-credentials",
                "key": "TEMPORAL_ADDRESS",
            }
        },
    } in container["env"]
    assert {
        "name": "MODEL_API_KEY",
        "valueFrom": {
            "secretKeyRef": {
                "name": "user-secret-anthropic-credentials",
                "key": "ANTHROPIC_API_KEY",
            }
        },
    } in container["env"]


def test_mtls_temporal_components_mount_cert_files_and_path_envs():
    demo_project = project(worker=True)
    demo_project["spec"]["temporal"] = {"auth": {"mode": "mtls"}}

    deployments = deployment_resources(demo_project, images())
    api_deployment = next(
        item for item in deployments if item["metadata"]["name"].startswith("api-")
    )
    pod_spec = api_deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert pod_spec["volumes"] == [
        {
            "name": "temporal-mtls-certs",
            "secret": {
                "secretName": "temporal-credentials",
                "items": [
                    {"key": "TEMPORAL_CA_CERT", "path": "ca.crt"},
                    {"key": "TEMPORAL_CLIENT_CERT", "path": "tls.crt"},
                    {"key": "TEMPORAL_CLIENT_KEY", "path": "tls.key"},
                ],
            },
        }
    ]
    assert {
        "name": "temporal-mtls-certs",
        "mountPath": "/var/run/secrets/tmprl-demo.cloud/temporal",
        "readOnly": True,
    } in container["volumeMounts"]
    env = {item["name"]: item["value"] for item in container["env"] if "value" in item}
    assert env["TEMPORAL_MTLS_CERT_DIR"] == (
        "/var/run/secrets/tmprl-demo.cloud/temporal"
    )
    assert env["TEMPORAL_MTLS_CA_CERT_PATH"] == (
        "/var/run/secrets/tmprl-demo.cloud/temporal/ca.crt"
    )
    assert env["TEMPORAL_MTLS_CLIENT_CERT_PATH"] == (
        "/var/run/secrets/tmprl-demo.cloud/temporal/tls.crt"
    )
    assert env["TEMPORAL_MTLS_CLIENT_KEY_PATH"] == (
        "/var/run/secrets/tmprl-demo.cloud/temporal/tls.key"
    )


def test_mtls_cert_files_can_set_legacy_path_env_names():
    demo_project = project(worker=True)
    demo_project["spec"]["temporal"] = {"auth": {"mode": "mtls"}}
    api = demo_project["spec"]["components"]["api"]
    api["temporalMtls"] = {
        "certFiles": {
            "mountPath": "/etc/temporal/certs",
            "paths": {
                "caCert": "/etc/temporal/certs/root-ca.pem",
                "clientCert": "/etc/temporal/certs/client.pem",
                "clientKey": "/etc/temporal/certs/client-key.pem",
            },
            "pathEnv": {
                "clientCert": "TEMPORAL_TLS_CERT",
                "clientKey": "TEMPORAL_TLS_KEY",
            },
        }
    }
    api["env"] = [
        {"name": "TEMPORAL_TLS_CERT", "value": "/wrong/path"},
    ]

    deployments = deployment_resources(demo_project, images())
    api_deployment = next(
        item for item in deployments if item["metadata"]["name"].startswith("api-")
    )
    pod_spec = api_deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert {
        "name": "temporal-mtls-certs",
        "mountPath": "/etc/temporal/certs",
        "readOnly": True,
    } in container["volumeMounts"]
    assert pod_spec["volumes"] == [
        {
            "name": "temporal-mtls-certs",
            "secret": {
                "secretName": "temporal-credentials",
                "items": [
                    {"key": "TEMPORAL_CA_CERT", "path": "root-ca.pem"},
                    {"key": "TEMPORAL_CLIENT_CERT", "path": "client.pem"},
                    {"key": "TEMPORAL_CLIENT_KEY", "path": "client-key.pem"},
                ],
            },
        }
    ]
    env = {item["name"]: item["value"] for item in container["env"] if "value" in item}
    assert env["TEMPORAL_TLS_CERT"] == "/etc/temporal/certs/client.pem"
    assert env["TEMPORAL_TLS_KEY"] == "/etc/temporal/certs/client-key.pem"
    assert env["TEMPORAL_MTLS_CERT_DIR"] == "/etc/temporal/certs"
    assert env["TEMPORAL_MTLS_CA_CERT_PATH"] == "/etc/temporal/certs/root-ca.pem"
    assert env["TEMPORAL_MTLS_CLIENT_CERT_PATH"] == "/etc/temporal/certs/client.pem"
    assert env["TEMPORAL_MTLS_CLIENT_KEY_PATH"] == (
        "/etc/temporal/certs/client-key.pem"
    )


def test_mtls_cert_file_mount_can_be_disabled():
    demo_project = project(worker=True)
    demo_project["spec"]["temporal"] = {"auth": {"mode": "mtls"}}
    demo_project["spec"]["components"]["api"]["temporalMtls"] = {
        "certFiles": {"enabled": False}
    }

    deployments = deployment_resources(demo_project, images())
    api_deployment = next(
        item for item in deployments if item["metadata"]["name"].startswith("api-")
    )
    pod_spec = api_deployment["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert "volumes" not in pod_spec
    assert "volumeMounts" not in container
    assert all(
        item.get("name") != "TEMPORAL_MTLS_CLIENT_CERT_PATH"
        for item in container.get("env", [])
    )


def test_versioned_worker_renders_connection_and_workerdeployment_not_deployment():
    demo_project = project(worker=versioned_worker())
    version = candidate_version(demo_project, images())
    namespace = runtime_namespace(demo_project, version)
    controller_secret_name = worker_controller_auth_secret_name(
        demo_project,
        namespace=namespace,
    )

    deployments = deployment_resources(demo_project, images())
    resources = runtime_workload_resources(
        demo_project,
        images(),
        include_routes=False,
        external_resources=external_resources(),
    )

    assert {item["metadata"]["name"] for item in deployments} == {
        "api-git-e12b0ed9d450"
    }

    connection = next(item for item in resources if item["kind"] == "Connection")
    assert connection["apiVersion"] == "temporal.io/v1alpha1"
    assert connection["metadata"]["namespace"] == namespace
    assert connection["spec"] == {
        "hostPort": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
        "apiKeySecretRef": {
            "name": controller_secret_name,
            "key": "TEMPORAL_API_KEY",
        },
    }

    worker_deployment = next(
        item for item in resources if item["kind"] == "WorkerDeployment"
    )
    assert worker_deployment["metadata"]["name"] == "money-transfer"
    assert worker_deployment["metadata"]["namespace"] == namespace
    assert worker_deployment["spec"]["workerOptions"] == {
        "connectionRef": {"name": "temporal"},
        "temporalNamespace": "tmprl-dem-cld-banking-money-transfer.acct1",
    }
    assert worker_deployment["spec"]["rollout"] == {
        "strategy": "Progressive",
        "steps": [
            {"rampPercentage": 10, "pauseDuration": "5m"},
            {"rampPercentage": 50, "pauseDuration": "10m"},
        ],
        "gate": {
            "workflowType": "WorkerCanary",
            "input": {"project": "banking-money-transfer"},
        },
    }
    assert worker_deployment["spec"]["sunset"] == {
        "scaledownDelay": "2h",
        "deleteDelay": "48h",
    }
    assert (
        worker_deployment["spec"]["template"]["spec"]["containers"][0]["image"]
        == "example.com/worker:git-e12b0ed9d450"
    )
    container = worker_deployment["spec"]["template"]["spec"]["containers"][0]
    assert container["envFrom"] == [
        {"secretRef": {"name": "temporal-worker-runtime"}}
    ]
    rendered = json.dumps(worker_deployment)
    assert "PLACEHOLDER_BUILD_ID" not in rendered
    assert "TEMPORAL_WORKER_BUILD_ID" not in rendered
    assert "TEMPORAL_ADDRESS" not in rendered
    assert "TEMPORAL_NAMESPACE" not in rendered


def test_serverless_lambda_worker_skips_kubernetes_worker_resources():
    demo_project = project(worker=versioned_worker())
    worker = demo_project["spec"]["components"]["worker"]
    worker["serverless"] = {
        "provider": "aws-lambda",
        "packageType": "image",
        "architecture": "x86_64",
        "memoryMb": 512,
        "timeoutSeconds": 600,
    }

    secrets = runtime_secret_resources(demo_project)
    deployments = deployment_resources(demo_project, images())
    resources = runtime_workload_resources(
        demo_project,
        images(),
        include_routes=False,
        external_resources=external_resources(),
    )

    assert {item["metadata"]["name"] for item in secrets} == {
        "temporal-credentials",
        "temporal-worker-runtime",
    }
    assert {item["metadata"]["name"] for item in deployments} == {
        "api-git-e12b0ed9d450"
    }
    assert "worker" not in candidate_service_status(demo_project, images())
    assert worker_deployment_status(demo_project, images()) == {}
    assert {item["kind"] for item in resources}.isdisjoint(
        {"Connection", "WorkerDeployment"}
    )
    assert (
        worker_deployment_rollout_wait_seconds(
            demo_project,
            base_timeout_seconds=300,
        )
        == 300
    )


def test_versioned_worker_maps_auth_input_refs_to_worker_runtime_secret():
    demo_project = project(worker=versioned_worker())
    worker = demo_project["spec"]["components"]["worker"]
    worker["env"].append(
        {
            "name": "TEMPORAL_TOKEN",
            "valueFrom": {"inputRef": "temporal.apiKey"},
        }
    )

    resources = runtime_workload_resources(
        demo_project,
        images(),
        include_routes=False,
        external_resources=external_resources(),
    )
    worker_deployment = next(
        item for item in resources if item["kind"] == "WorkerDeployment"
    )
    container = worker_deployment["spec"]["template"]["spec"]["containers"][0]

    assert {
        "name": "TEMPORAL_TOKEN",
        "valueFrom": {
            "secretKeyRef": {
                "name": "temporal-worker-runtime",
                "key": "TEMPORAL_API_KEY",
            }
        },
    } in container["env"]


@pytest.mark.parametrize("worker", [True, versioned_worker()])
def test_component_iam_role_is_scoped_to_its_revision_service_account(worker):
    demo_project = project(worker=worker)
    role_arn = "arn:aws:iam::429214323166:role/monty-payloads"
    demo_project["spec"]["components"]["worker"]["awsIamRoleArn"] = role_arn
    namespace = "tmprl-dem-cld-banking-money-transfer-abcde"

    service_accounts = {
        item["metadata"]["name"]: item
        for item in rbac_resources(demo_project, namespace=namespace)
        if item["kind"] == "ServiceAccount"
    }

    assert service_accounts["worker"]["metadata"]["namespace"] == namespace
    assert service_accounts["worker"]["metadata"]["annotations"] == {
        "platform.temporal.io/component": "worker",
        "eks.amazonaws.com/role-arn": role_arn,
    }
    assert service_accounts["worker"]["automountServiceAccountToken"] is False
    assert service_accounts["api"]["metadata"]["annotations"] == {
        "platform.temporal.io/component": "api",
    }


def test_service_account_token_automount_is_component_opt_in():
    demo_project = project(worker=versioned_worker())
    demo_project["spec"]["components"]["worker"][
        "automountServiceAccountToken"
    ] = True

    base = base_resources(demo_project)
    service_accounts = {
        item["metadata"]["name"]: item
        for item in base
        if item["kind"] == "ServiceAccount"
    }
    assert service_accounts["api"]["automountServiceAccountToken"] is False
    assert service_accounts["worker"]["automountServiceAccountToken"] is True

    resources = runtime_workload_resources(
        demo_project,
        images(),
        include_routes=False,
        external_resources=external_resources(),
    )
    api_deployment = next(
        item
        for item in resources
        if item["kind"] == "Deployment" and item["metadata"]["name"].startswith("api-")
    )
    worker_deployment = next(
        item for item in resources if item["kind"] == "WorkerDeployment"
    )

    assert (
        api_deployment["spec"]["template"]["spec"]["automountServiceAccountToken"]
        is False
    )
    assert (
        worker_deployment["spec"]["template"]["spec"]["automountServiceAccountToken"]
        is True
    )


def test_versioned_worker_runtime_secret_contains_only_auth_env():
    demo_project = project(worker=versioned_worker())

    secrets = runtime_secret_resources(demo_project)

    external_secret_names = {
        item["metadata"]["name"]
        for item in secrets
        if item["kind"] == "ExternalSecret"
    }
    assert external_secret_names == {
        "temporal-credentials",
        "temporal-worker-runtime",
    }
    worker_runtime = next(
        item
        for item in secrets
        if item["kind"] == "ExternalSecret"
        and item["metadata"]["name"] == "temporal-worker-runtime"
    )
    assert [item["secretKey"] for item in worker_runtime["spec"]["data"]] == [
        "TEMPORAL_API_KEY",
        "TEMPORAL_TLS",
    ]


def test_candidate_runtime_isolates_worker_controller_credential_secret():
    demo_project = project(worker=versioned_worker())
    version = candidate_version(demo_project, images())
    namespace = runtime_namespace(demo_project, version)
    controller_secret_name = worker_controller_auth_secret_name(
        demo_project,
        namespace=namespace,
    )

    secrets = runtime_secret_resources(demo_project, namespace=namespace)
    secret_names = {
        item["metadata"]["name"]
        for item in secrets
        if item["kind"] == "ExternalSecret"
    }
    assert controller_secret_name != "temporal-worker-runtime"
    assert secret_names == {
        "temporal-credentials",
        "temporal-worker-runtime",
        controller_secret_name,
    }

    controller_secret = next(
        item
        for item in secrets
        if item["metadata"]["name"] == controller_secret_name
    )
    assert controller_secret["spec"]["target"]["name"] == controller_secret_name

    workloads = runtime_workload_resources(
        demo_project,
        images(),
        include_routes=False,
        external_resources=external_resources(),
    )
    connection = next(item for item in workloads if item["kind"] == "Connection")
    assert connection["spec"]["apiKeySecretRef"]["name"] == controller_secret_name

    worker_deployment = next(
        item for item in workloads if item["kind"] == "WorkerDeployment"
    )
    assert worker_deployment["spec"]["template"]["spec"]["containers"][0][
        "envFrom"
    ] == [{"secretRef": {"name": "temporal-worker-runtime"}}]


def test_versioned_worker_with_ports_uses_stable_service_name_and_rollout_selector():
    demo_project = project(worker=versioned_worker())
    demo_project["spec"]["components"]["worker"]["ports"] = [
        {"containerPort": 9000, "servicePort": 9000}
    ]

    resources = runtime_workload_resources(
        demo_project,
        images(),
        include_routes=False,
        external_resources=external_resources(),
    )

    service = next(
        item
        for item in resources
        if item["kind"] == "Service" and item["metadata"]["name"] == "worker"
    )
    assert service["spec"]["selector"] == {
        "app.kubernetes.io/name": "banking-money-transfer",
        "app.kubernetes.io/component": "worker",
        "platform.temporal.io/rollout-version": "git-e12b0ed9d450",
    }
    assert candidate_service_status(demo_project, images())["worker"] == "worker"


def test_worker_deployment_status_maps_versioned_components():
    assert worker_deployment_status(project(worker=versioned_worker()), images()) == {
        "worker": "money-transfer"
    }


def test_worker_deployment_name_defaults_to_component_name():
    worker = versioned_worker()
    worker["versioning"].pop("deploymentName")

    assert worker_deployment_status(project(worker=worker), images()) == {
        "worker": "worker"
    }


def test_worker_deployment_wait_seconds_includes_gate_and_progressive_durations():
    worker = versioned_worker()
    gate_input = worker["versioning"]["rollout"]["gate"]["input"]
    gate_input["timeoutSeconds"] = 1800
    gate_input["cleanupTimeoutSeconds"] = 900

    assert (
        worker_deployment_rollout_wait_seconds(
            project(worker=worker),
            base_timeout_seconds=300,
        )
        == 3900
    )


def test_failed_gate_workflow_marks_worker_deployment_rollout_failed(monkeypatch):
    def fake_status(namespace: str, name: str) -> dict:
        return {
            "name": name,
            "namespace": namespace,
            "conditions": [{"type": "Ready", "status": "False", "reason": "Waiting"}],
            "targetVersion": {
                "buildID": "git-e12b0ed9d450",
                "status": "Inactive",
                "testWorkflows": [
                    {
                        "workflowID": "gate",
                        "runID": "run",
                        "status": "Failed",
                        "taskQueue": "money-transfer",
                    }
                ],
            },
        }

    monkeypatch.setattr(reconcile.kube, "read_worker_deployment_status", fake_status)

    status = reconcile.worker_deployment_sync_status(
        [
            {
                "apiVersion": "temporal.io/v1alpha1",
                "kind": "WorkerDeployment",
                "namespace": "tmprl-dem-cld-banking-money-transfer",
                "name": "money-transfer",
                "action": "patched",
            }
        ]
    )

    assert status["phase"] == "Failed"
    assert status["ok"] is False
    assert status["resources"][0]["targetVersion"]["testWorkflows"][0]["status"] == (
        "Failed"
    )


def test_schema_accepts_controller_versioning_and_rejects_gate_input_conflict():
    schema = json.loads((ROOT / "schemas/project.schema.json").read_text())
    validator = Draft202012Validator(schema)
    valid = project(worker=versioned_worker())
    valid["spec"]["temporal"] = {"auth": {"mode": "mtls"}}
    valid["spec"]["components"]["worker"]["automountServiceAccountToken"] = True
    valid["spec"]["components"]["worker"]["temporalProxy"] = {
        "enabled": True,
        "addressEnv": ["TEMPORAL_HOST"],
    }
    valid["spec"]["components"]["worker"]["env"].append(
        {
            "name": "MODEL_API_KEY",
            "valueFrom": {
                "inputRef": "secrets.anthropic-credentials.ANTHROPIC_API_KEY",
            },
        }
    )
    valid["spec"]["secrets"] = {
        "anthropic-credentials": {
            "requiredProperties": ["ANTHROPIC_API_KEY"],
        }
    }

    assert list(validator.iter_errors(valid)) == []

    invalid = project(worker=versioned_worker())
    invalid["spec"]["components"]["worker"]["worker"]["versioning"]["rollout"][
        "gate"
    ]["inputFrom"] = {"configMapKeyRef": {"name": "gate-input", "key": "payload.json"}}

    errors = list(validator.iter_errors(invalid))

    assert errors


def test_schema_rejects_unknown_temporal_auth_mode():
    schema = json.loads((ROOT / "schemas/project.schema.json").read_text())
    validator = Draft202012Validator(schema)
    invalid = project(worker=True)
    invalid["spec"]["temporal"] = {"auth": {"mode": "password"}}

    errors = list(validator.iter_errors(invalid))

    assert errors


def test_schema_rejects_env_value_and_value_from_conflict():
    schema = json.loads((ROOT / "schemas/project.schema.json").read_text())
    validator = Draft202012Validator(schema)
    invalid = project(worker=True)
    invalid["spec"]["components"]["worker"]["env"].append(
        {
            "name": "TEMPORAL_HOST",
            "value": "localhost:7233",
            "valueFrom": {"inputRef": "temporal.address"},
        }
    )

    errors = list(validator.iter_errors(invalid))

    assert errors


def test_registry_yaml_renders_operator_as_versioned_worker_without_pinned_gate(
    monkeypatch,
):
    schema = json.loads((ROOT / "schemas/project.schema.json").read_text())
    validator = Draft202012Validator(schema)
    demo_project = yaml.safe_load(
        (ROOT / "projects/operator/registry.yaml").read_text()
    )
    diagnostics_bucket = "tmprl-dem-cld-rollout-diagnostics-429214323166-us-west-1"

    assert list(validator.iter_errors(demo_project)) == []
    assert demo_project["metadata"]["name"] == "registry-operator"
    assert demo_project["metadata"]["annotations"] == {
        REGISTRY_CATALOG_ANNOTATION: "true",
        FINALIZATION_POLICY_ANNOTATION: "retain",
    }
    assert demo_project["spec"]["source"]["gitRepository"]["url"] == (
        "ssh://git@github.com/temporal-sa/tmprl-demo-cloud-registry.git"
    )
    assert demo_project["spec"]["source"]["gitRepository"]["authSecret"] == (
        "source-auth"
    )
    assert demo_project["spec"]["source"]["gitRepository"]["ignore"].splitlines() == [
        "/*",
        "!/registry/",
        "/registry/*",
        "!/registry/web/",
        "!/registry/web/**",
        "!/registry/worker/",
        "!/registry/worker/**",
        "!/registry/temporal-cloud-proxy/",
        "!/registry/temporal-cloud-proxy/**",
        "!/schemas/",
        "!/schemas/**",
        "!/scripts/",
        "/scripts/*",
        "!/scripts/validate_projects.py",
    ]
    assert source_git_repository(
        demo_project,
        namespace="tmprl-dem-cld-registry-operator",
        interval="1m",
    )["spec"] == {
        "interval": "1m",
        "url": "ssh://git@github.com/temporal-sa/tmprl-demo-cloud-registry.git",
        "ref": {"branch": "main"},
        "secretRef": {"name": "registry-operator-source-auth"},
        "ignore": "\n".join(
            [
                "/*",
                "!/registry/",
                "/registry/*",
                "!/registry/web/",
                "!/registry/web/**",
                "!/registry/worker/",
                "!/registry/worker/**",
                "!/registry/temporal-cloud-proxy/",
                "!/registry/temporal-cloud-proxy/**",
                "!/schemas/",
                "!/schemas/**",
                "!/scripts/",
                "/scripts/*",
                "!/scripts/validate_projects.py",
            ]
        ),
    }
    assert demo_project["spec"]["source"]["gitRepository"] == {
        "url": "ssh://git@github.com/temporal-sa/tmprl-demo-cloud-registry.git",
        "authSecret": "source-auth",
        "ignore": (
            "/*\n"
            "!/registry/\n"
            "/registry/*\n"
            "!/registry/web/\n"
            "!/registry/web/**\n"
            "!/registry/worker/\n"
            "!/registry/worker/**\n"
            "!/registry/temporal-cloud-proxy/\n"
            "!/registry/temporal-cloud-proxy/**\n"
            "!/schemas/\n"
            "!/schemas/**\n"
            "!/scripts/\n"
            "/scripts/*\n"
            "!/scripts/validate_projects.py\n"
        ),
    }
    assert "registry.tmprl-demo.cloud/image-overrides" not in demo_project["metadata"][
        "annotations"
    ]
    assert SOURCE_COMMIT_ANNOTATION not in demo_project["metadata"]["annotations"]

    operator_component = demo_project["spec"]["components"]["registry-operator"]
    assert not any(
        item["name"] == "DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET"
        for item in operator_component["env"]
    )
    # The running operator supplies this platform-owned value. Simulate a stale
    # declaration too so a future collision cannot produce an invalid
    # WorkerDeployment with duplicate env names.
    operator_component["env"].append(
        {
            "name": "DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET",
            "value": "stale-bucket",
        }
    )
    monkeypatch.setenv(
        "DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET",
        diagnostics_bucket,
    )

    image_overrides = {
        "temporal-cloud-proxy": (
            "example.com/registry/temporal-cloud-proxy:git-e12b0ed9d450"
        ),
        "web": "example.com/registry/web:git-e12b0ed9d450",
        "worker": "example.com/registry/worker:git-e12b0ed9d450",
    }
    registry_external_resources = {
        "temporalCloud": {
            "namespace": {
                "name": "tmprl-dem-cld-registry-operator.a2dd6",
                "grpcAddress": (
                    "tmprl-dem-cld-registry-operator.a2dd6.tmprl.cloud:7233"
                ),
            }
        }
    }

    base = base_resources(demo_project)
    registry_service_account = next(
        item
        for item in base
        if item["kind"] == "ServiceAccount"
        and item["metadata"]["name"] == "registry-operator"
    )
    assert registry_service_account["automountServiceAccountToken"] is True

    secrets = runtime_secret_resources(demo_project)
    assert {item["metadata"]["name"] for item in secrets} == {
        "instruqt-cli",
        "shared-auth",
        "temporal-worker-runtime",
        "user-secret-temporal-cloud-bootstrap",
    }
    instruqt_secret = next(
        item for item in secrets if item["metadata"]["name"] == "instruqt-cli"
    )
    assert instruqt_secret["metadata"]["annotations"] == {
        "platform.temporal.io/secret-source": "tmprl-dem-cld/platform/instruqt-cli"
    }

    resources = runtime_workload_resources(
        demo_project,
        image_overrides,
        include_routes=False,
        external_resources=registry_external_resources,
    )
    worker_deployment = next(
        item for item in resources if item["kind"] == "WorkerDeployment"
    )
    assert worker_deployment["metadata"]["name"] == "registry-operator"
    assert worker_deployment["metadata"]["namespace"] == (
        "tmprl-dem-cld-registry-operator"
    )
    assert worker_deployment["spec"]["workerOptions"] == {
        "connectionRef": {"name": "temporal"},
        "temporalNamespace": "tmprl-dem-cld-registry-operator.a2dd6",
    }

    pod_spec = worker_deployment["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "registry-operator"
    assert pod_spec["automountServiceAccountToken"] is True
    container = pod_spec["containers"][0]
    assert container["image"] == "example.com/registry/worker:git-e12b0ed9d450"
    assert container["envFrom"] == [
        {"secretRef": {"name": "temporal-worker-runtime"}},
        {"secretRef": {"name": "instruqt-cli"}},
        {"secretRef": {"name": "user-secret-temporal-cloud-bootstrap"}},
    ]
    assert {
        "name": "DEMO_REGISTRY_OPERATOR_IMAGE",
        "value": "example.com/registry/worker:git-e12b0ed9d450",
    } in container["env"]
    assert {
        "name": "DEMO_REGISTRY_TEMPORAL_PROXY_IMAGE",
        "value": "example.com/registry/temporal-cloud-proxy:git-e12b0ed9d450",
    } in container["env"]
    # The operator no longer bakes an explicit DEMO_REGISTRY_TASK_QUEUE: it derives
    # registry-management-<color> from the injected DEMO_REGISTRY_CLUSTER (config.
    # from_env) so a colored operator polls its own queue. An explicit value would
    # win over that derivation and pin the uncolored queue.
    assert not any(e["name"] == "DEMO_REGISTRY_TASK_QUEUE" for e in container["env"])
    assert {
        "name": "DEMO_REGISTRY_TEMPORAL_CLOUD_BOOTSTRAP_SECRET_PATH",
        "value": "tmprl-dem-cld/registry-operator/temporal-cloud-bootstrap",
    } in container["env"]
    assert {
        "name": "DEMO_REGISTRY_WATCH_ENABLED",
        "value": "auto",
    } in container["env"]
    assert [
        item
        for item in container["env"]
        if item["name"] == "DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET"
    ] == [
        {
            "name": "DEMO_REGISTRY_ROLLOUT_DIAGNOSTICS_BUCKET",
            "value": diagnostics_bucket,
        }
    ]
    env_names = [item["name"] for item in container["env"]]
    assert len(env_names) == len(set(env_names))
    assert worker_deployment["spec"]["sunset"] == {
        "scaledownDelay": "1m",
        "deleteDelay": "5m",
    }

    gate_input = worker_deployment["spec"]["rollout"]["gate"]["input"]
    assert gate_input["projectNamePrefix"] == "operator-canary"
    assert "expectedOperatorImage" not in gate_input
    assert "expectedOperatorSourceSha" not in gate_input

    web_deployment = next(
        item
        for item in resources
        if item["kind"] == "Deployment"
        and item["metadata"]["name"].startswith("web-")
    )
    web_pod_spec = web_deployment["spec"]["template"]["spec"]
    assert web_pod_spec["volumes"] == [
        {
            "name": "catalog-status",
            "configMap": {"name": "registry-catalog-status", "optional": True},
        },
        {
            "name": "catalog",
            "configMap": {"name": "registry-catalog", "optional": True},
        },
    ]
    assert {"secretRef": {"name": "shared-auth"}} in web_pod_spec["containers"][0][
        "envFrom"
    ]


def test_operator_canary_plan_uses_unique_name_and_expected_annotations(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_IMAGE", "example.com/operator:git-test")
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_SOURCE_SHA", "e12b0ed9d450")
    monkeypatch.setenv("TEMPORAL_DEPLOYMENT_NAME", "registry-operator")
    monkeypatch.setenv("TEMPORAL_WORKER_BUILD_ID", "git-test-1234")
    expected_images = {"api": "example.com/api:git-e12b0ed9d450"}

    plan = plan_registry_operator_canary(
        {
            "workflowID": "worker-controller-gate",
            "runID": "run-1",
            "projectNamePrefix": "operator-canary",
            "expectedOperatorImage": "example.com/operator:git-test",
            "expectedOperatorSourceSha": "e12b0ed9d450",
            "expectedSourceCommitSha": "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c",
            "expectedImages": expected_images,
            "demoProjectTemplate": project(worker=True),
        }
    )

    demo_project = plan["demoProject"]
    annotations = demo_project["metadata"]["annotations"]
    labels = demo_project["metadata"]["labels"]
    assert demo_project["metadata"]["name"].startswith("operator-canary-")
    assert labels == {
        "registry.tmprl-demo.cloud/canary": "true",
        "registry.tmprl-demo.cloud/canary-kind": "operator-rollout",
    }
    assert annotations[SOURCE_COMMIT_ANNOTATION] == (
        "e12b0ed9d4505be2f6b0bc9d2664b1eb64a8d69c"
    )
    assert json.loads(annotations[IMAGE_OVERRIDES_ANNOTATION]) == expected_images
    assert demo_project["spec"]["ingress"]["subdomain"] == demo_project["metadata"]["name"]
    assert plan["expectedOperatorSourceSha"] == "e12b0ed9d450"
    assert plan["actualOperatorSourceSha"] == "e12b0ed9d450"
    assert plan["targetDeploymentName"] == "registry-operator"
    assert plan["targetBuildID"] == "git-test-1234"
    assert plan["namespace"].startswith("tmprl-dem-cld-operator-canary-")


def test_operator_canary_plan_resolves_unannotated_source_branch(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_IMAGE", "example.com/operator:git-test")
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_SOURCE_SHA", "e12b0ed9d450")
    monkeypatch.setenv("TEMPORAL_DEPLOYMENT_NAME", "registry-operator")
    monkeypatch.setenv("TEMPORAL_WORKER_BUILD_ID", "git-test-1234")
    source_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    template = project(worker=True)
    template["metadata"]["annotations"].pop(SOURCE_COMMIT_ANNOTATION)
    calls = []

    class FakeHTTPResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps({"commit": {"sha": source_sha}}).encode("utf-8")

    def fake_urlopen(request, timeout):
        calls.append(request.full_url)
        return FakeHTTPResponse()

    monkeypatch.setattr(canary.kube, "read_secret_data",
        lambda namespace, name: {"password": "github_pat_test"},
    )
    monkeypatch.setattr(canary, "urlopen", fake_urlopen)

    plan = plan_registry_operator_canary(
        {
            "workflowID": "worker-controller-gate",
            "runID": "run-1",
            "projectNamePrefix": "operator-canary",
            "expectedOperatorImage": "example.com/operator:git-test",
            "expectedOperatorSourceSha": "e12b0ed9d450",
            "demoProjectTemplate": template,
        }
    )

    annotations = plan["demoProject"]["metadata"]["annotations"]
    assert annotations[SOURCE_COMMIT_ANNOTATION] == source_sha
    assert plan["expectedSourceCommitSha"] == source_sha
    assert plan["sourceResolution"] == {
        "phase": "Resolved",
        "source": "github",
        "repo": "https://github.com/temporal-sa/demo-banking-app-money-transfer",
        "branch": "main",
        "sourceCommitSha": source_sha,
    }
    assert calls == [
        "https://api.github.com/repos/temporal-sa/demo-banking-app-money-transfer/branches/main"
    ]


def test_operator_canary_plan_resolves_public_source_when_secret_unreadable(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_IMAGE", "example.com/operator:git-test")
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_SOURCE_SHA", "e12b0ed9d450")
    monkeypatch.setenv("TEMPORAL_DEPLOYMENT_NAME", "registry-operator")
    monkeypatch.setenv("TEMPORAL_WORKER_BUILD_ID", "git-test-1234")
    source_sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    template = project(worker=True)
    template["metadata"]["annotations"].pop(SOURCE_COMMIT_ANNOTATION)
    auth_headers = []

    class FakeHTTPResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps({"commit": {"sha": source_sha}}).encode("utf-8")

    def fake_urlopen(request, timeout):
        auth_headers.append(request.get_header("Authorization"))
        return FakeHTTPResponse()

    def forbidden_secret(namespace, name):
        raise RuntimeError("forbidden")

    monkeypatch.setattr(canary.kube, "read_secret_data", forbidden_secret)
    monkeypatch.setattr(canary, "urlopen", fake_urlopen)

    plan = plan_registry_operator_canary(
        {
            "workflowID": "worker-controller-gate",
            "runID": "run-1",
            "projectNamePrefix": "operator-canary",
            "expectedOperatorImage": "example.com/operator:git-test",
            "expectedOperatorSourceSha": "e12b0ed9d450",
            "demoProjectTemplate": template,
        }
    )

    assert plan["expectedSourceCommitSha"] == source_sha
    assert plan["demoProject"]["metadata"]["annotations"][SOURCE_COMMIT_ANNOTATION] == (
        source_sha
    )
    assert auth_headers == [None]


def test_operator_canary_plan_rejects_unexpected_source_sha(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_IMAGE", "example.com/operator:git-test")
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_SOURCE_SHA", "actual")
    monkeypatch.setenv("TEMPORAL_DEPLOYMENT_NAME", "registry-operator")
    monkeypatch.setenv("TEMPORAL_WORKER_BUILD_ID", "git-test-1234")

    with pytest.raises(ApplicationError):
        plan_registry_operator_canary(
            {
                "workflowID": "worker-controller-gate",
                "runID": "run-1",
                "expectedOperatorImage": "example.com/operator:git-test",
                "expectedOperatorSourceSha": "expected",
                "demoProjectTemplate": project(worker=True),
            }
        )


def test_operator_canary_plan_requires_target_version(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_IMAGE", "example.com/operator:git-test")
    monkeypatch.setenv("DEMO_REGISTRY_OPERATOR_SOURCE_SHA", "e12b0ed9d450")
    monkeypatch.delenv("TEMPORAL_DEPLOYMENT_NAME", raising=False)
    monkeypatch.delenv("TEMPORAL_WORKER_BUILD_ID", raising=False)

    with pytest.raises(ApplicationError):
        plan_registry_operator_canary(
            {
                "workflowID": "worker-controller-gate",
                "runID": "run-1",
                "expectedOperatorImage": "example.com/operator:git-test",
                "expectedOperatorSourceSha": "e12b0ed9d450",
                "demoProjectTemplate": project(worker=True),
            }
        )


def test_apply_operator_canary_project_applies_demo_project(monkeypatch):
    demo_project = project(worker=True)
    plan = {
        "project": "operator-canary-abc123",
        "namespace": "tmprl-dem-cld-operator-canary-abc123",
        "demoProject": demo_project,
        "targetDeploymentName": "registry-operator",
        "targetBuildID": "git-test-1234",
    }

    monkeypatch.setattr(canary.kube, "apply_demo_project", lambda resource, dry_run: "created")
    monkeypatch.setattr(canary.kube, "ensure_demo_project_finalizer", lambda name: True)

    result = canary.apply_registry_operator_canary_project(
        canary.ApplyRegistryOperatorCanaryProjectRequest(
            project=plan["project"],
            namespace=plan["namespace"],
            demo_project=demo_project,
        )
    ).status

    assert result["action"] == "created"
    assert result["finalizerAdded"] is True


def test_operator_canary_force_reconcile_signal_passes_args(monkeypatch):
    calls = []
    unset = object()

    class FakeHandle:
        async def signal(self, signal, arg=unset, *, args=None, **_kwargs):
            calls.append((signal, arg, args))

    class FakeClient:
        def get_workflow_handle(self, workflow_id):
            calls.append(("handle", workflow_id))
            return FakeHandle()

    async def fake_connect(_config):
        return FakeClient()

    monkeypatch.setattr(canary, "connect", fake_connect)

    result = asyncio.run(
        canary.signal_registry_operator_canary_force_reconcile_async(
            {"project": "operator-canary-abc123"},
            token="rotation-token",
            reason="rotate-temporal-auth",
        )
    )

    assert result["workflowID"] == "demo-project/operator-canary-abc123"
    assert calls[0] == ("handle", "demo-project/operator-canary-abc123")
    assert calls[1][0] == "force_reconcile"
    assert calls[1][1] == ForceReconcileSignal(
        token="rotation-token",
        reason="rotate-temporal-auth",
    )
    assert calls[1][2] is None


def test_operator_canary_finalize_signal_passes_args(monkeypatch):
    calls = []
    demo_project = project(worker=True)

    class FakeHandle:
        async def signal(self, signal, arg=None, *, args=None, **_kwargs):
            calls.append((signal, arg, args))

    class FakeClient:
        def get_workflow_handle(self, workflow_id):
            calls.append(("handle", workflow_id))
            return FakeHandle()

    async def fake_connect(_config):
        return FakeClient()

    monkeypatch.setattr(canary, "connect", fake_connect)

    result = asyncio.run(
        canary.signal_registry_operator_canary_finalize_async(
            canary.SignalRegistryOperatorCanaryFinalizeRequest(
                project="operator-canary-abc123",
                demo_project=demo_project,
            )
        )
    )

    assert result["phase"] == "Signaled"
    assert result["workflowID"] == "demo-project/operator-canary-abc123"
    assert calls[0] == ("handle", "demo-project/operator-canary-abc123")
    assert calls[1] == (
        "finalize",
        DemoProjectFinalizeSignal(demoProject=demo_project),
        None,
    )


def test_finalized_demo_project_status_ignores_missing_cr(monkeypatch):
    class MissingDemoProjectError(Exception):
        status = 404

    calls = []

    def missing_status_patch(name, status, *, dry_run):
        calls.append((name, status, dry_run))
        raise MissingDemoProjectError("missing")

    demo_project = {"metadata": {"name": "operator-canary-abc123"}}
    monkeypatch.setattr(reconcile.kube, "patch_demo_project_status", missing_status_patch)

    result = reconcile.record_demo_project_status(
        reconcile.RecordDemoProjectStatusRequest(
            demo_project=demo_project,
            status={"phase": "Finalized"},
        )
    ).status

    assert result["phase"] == "Finalized"
    assert calls[0][0] == "operator-canary-abc123"


def test_non_final_demo_project_status_does_not_ignore_missing_cr(monkeypatch):
    class MissingDemoProjectError(Exception):
        status = 404

    def missing_status_patch(name, status, *, dry_run):
        raise MissingDemoProjectError("missing")

    demo_project = {"metadata": {"name": "operator-canary-abc123"}}
    monkeypatch.setattr(reconcile.kube, "patch_demo_project_status", missing_status_patch)

    with pytest.raises(MissingDemoProjectError):
        reconcile.record_demo_project_status(
            reconcile.RecordDemoProjectStatusRequest(
                demo_project=demo_project,
                status={"phase": "Ready"},
            )
        )


def test_worker_deployment_config_uses_controller_env(monkeypatch):
    from demo_registry_operator.worker import worker_deployment_config_from_env

    monkeypatch.delenv("TEMPORAL_DEPLOYMENT_NAME", raising=False)
    monkeypatch.delenv("TEMPORAL_WORKER_BUILD_ID", raising=False)
    assert worker_deployment_config_from_env() is None

    monkeypatch.setenv("TEMPORAL_DEPLOYMENT_NAME", "registry-operator")
    monkeypatch.setenv("TEMPORAL_WORKER_BUILD_ID", "git-abc123")
    config = worker_deployment_config_from_env()

    assert config is not None
    assert config.use_worker_versioning is True
    assert config.version.deployment_name == "registry-operator"
    assert config.version.build_id == "git-abc123"


def test_registry_operator_rbac_can_manage_canary_and_prune_resources():
    manifests = list(
        yaml.safe_load_all(
            (
                ROOT / "clusters/blue/bootstrap/registry-operator/rbac.yaml"
            ).read_text()
        )
    )
    cluster_role = next(
        item
        for item in manifests
        if item
        and item["kind"] == "ClusterRole"
        and item["metadata"]["name"] == "registry-operator"
    )
    registry_resource_rule = next(
        rule
        for rule in cluster_role["rules"]
        if rule["apiGroups"] == ["registry.tmprl-demo.cloud"]
        and set(rule["resources"])
        == {"demoprojects", "demoprojectrevisions", "instruqtcourses"}
    )

    assert set(registry_resource_rule["verbs"]) >= {
        "create",
        "delete",
        "get",
        "list",
        "patch",
        "update",
        "watch",
    }

    apps_deployment_rule = next(
        rule
        for rule in cluster_role["rules"]
        if rule["apiGroups"] == ["apps"]
        and rule["resources"] == ["deployments"]
    )
    assert set(apps_deployment_rule["verbs"]) >= {
        "create",
        "delete",
        "get",
        "list",
        "patch",
    }

    service_rule = next(
        rule
        for rule in cluster_role["rules"]
        if rule["apiGroups"] == [""]
        and "services" in rule["resources"]
    )
    assert set(service_rule["verbs"]) >= {
        "create",
        "delete",
        "get",
        "list",
        "patch",
    }

    namespace_rule = next(
        rule
        for rule in cluster_role["rules"]
        if rule["apiGroups"] == [""]
        and rule["resources"] == ["namespaces"]
    )
    assert set(namespace_rule["verbs"]) >= {
        "create",
        "delete",
        "get",
        "list",
        "patch",
    }


def cleanup_plan() -> dict:
    return {
        "project": "operator-canary-abc123",
        "namespace": "tmprl-dem-cld-operator-canary-abc123",
        "gitRepository": "operator-canary-abc123-source",
        "sourceWatchNamespace": "tmprl-dem-cld-registry-operator",
        "catalogStatusNamespace": "tmprl-dem-cld-registry-operator",
        "catalogStatusMirrorNamespace": "tmprl-dem-cld-registry-operator",
        "catalogStatusConfigMap": "registry-catalog-status",
        "cleanupTimeoutSeconds": 0,
        "pollSeconds": 0,
    }


def delete_canary_request(plan: dict):
    return canary.DeleteRegistryOperatorCanaryProjectRequest(
        project=plan["project"],
        cleanup_policy=plan.get("cleanupPolicy", "Always"),
    )


def promotion_wait_request(plan: dict):
    return canary.RegistryOperatorCanaryPromotionWaitRequest(
        project=plan["project"],
        namespace=plan["namespace"],
        expected_source_commit_sha=plan.get("expectedSourceCommitSha"),
        expected_images=plan.get("expectedImages", {}),
        timeout_seconds=int(plan.get("timeoutSeconds", 900)),
        poll_seconds=int(plan.get("pollSeconds", 10)),
    )


def cleanup_wait_request(plan: dict):
    return canary.WaitRegistryOperatorCanaryCleanupRequest(
        project=plan["project"],
        namespace=plan["namespace"],
        git_repository=plan["gitRepository"],
        source_watch_namespace=plan["sourceWatchNamespace"],
        catalog_status_namespace=plan["catalogStatusNamespace"],
        catalog_status_mirror_namespace=plan["catalogStatusMirrorNamespace"],
        catalog_status_config_map=plan["catalogStatusConfigMap"],
        cleanup_policy=plan.get("cleanupPolicy", "Always"),
        cleanup_timeout_seconds=int(plan.get("cleanupTimeoutSeconds", 600)),
        poll_seconds=int(plan.get("pollSeconds", 10)),
        delete_action=plan.get("deleteAction"),
    )


def test_canary_delete_requests_demo_project_deletion(monkeypatch):
    calls = []
    monkeypatch.setattr(canary.kube, "delete_demo_project",
        lambda name: calls.append(name) or "deleting",
    )

    result = canary.delete_registry_operator_canary_project(
        delete_canary_request(cleanup_plan())
    ).status

    assert result["phase"] == "DeleteRequested"
    assert result["ok"] is True
    assert result["deleteAction"] == "deleting"
    assert calls == ["operator-canary-abc123"]


def test_canary_promotion_wait_fails_fast_when_project_errors(monkeypatch):
    monkeypatch.setattr(canary.kube, "read_demo_project",
        lambda name: {
            "status": {
                "phase": "Error",
                "message": "Image build failed; workload resources were not reconciled.",
                "buildStatus": {"phase": "Error"},
            }
        },
    )

    with pytest.raises(ApplicationError) as raised:
        canary.wait_for_registry_operator_canary_promotion(
            promotion_wait_request(cleanup_plan())
        )
    assert raised.value.non_retryable
    result = raised.value.details[0]["status"]

    assert result["phase"] == "Failed"
    assert result["ok"] is False
    assert result["message"] == (
        "Canary project failed before promotion: "
        "Image build failed; workload resources were not reconciled."
    )


def test_canary_promotion_wait_fails_fast_when_workflow_errors(monkeypatch):
    monkeypatch.setattr(canary.kube, "read_demo_project", lambda name: {"status": {}})
    monkeypatch.setattr(
        canary,
        "canary_workflow_terminal_failure",
        lambda plan: {
            "phase": "Failed",
            "workflowID": "demo-project/operator-canary-abc123",
            "runID": "run-1",
            "executionStatus": "FAILED",
            "message": (
                "Canary DemoProject workflow reached terminal failed state "
                "FAILED before the gate completed."
            ),
        },
    )

    with pytest.raises(ApplicationError) as raised:
        canary.wait_for_registry_operator_canary_promotion(
            promotion_wait_request(cleanup_plan())
        )
    result = raised.value.details[0]["status"]

    assert result["phase"] == "Failed"
    assert result["ok"] is False
    assert result["workflow"]["executionStatus"] == "FAILED"


def test_canary_promotion_wait_uses_active_runtime_namespace(monkeypatch):
    runtime_namespace = "tmprl-dem-cld-operator-canary-abc123-a1b2c"
    read_namespaces = []
    status = {
        "phase": "Promoted",
        "activeRuntimeNamespace": runtime_namespace,
        "activeRouteServices": {"frontend": "frontend"},
        "candidateRolloutStatus": {
            "deployments": {"frontend": "frontend-git-abc"},
            "runtimeNamespace": runtime_namespace,
            "sourceCommitSha": "abc123",
        },
    }

    monkeypatch.setattr(
        canary.kube,
        "read_demo_project",
        lambda name: {"status": status},
    )

    def read_namespace(name):
        read_namespaces.append(name)
        return {"metadata": {"name": name}} if name == runtime_namespace else None

    monkeypatch.setattr(canary.kube, "read_namespace", read_namespace)
    monkeypatch.setattr(
        canary.kube,
        "read_deployment_status",
        lambda namespace, name: {"namespace": namespace, "name": name},
    )
    monkeypatch.setattr(
        canary.kube,
        "read_service",
        lambda namespace, name: {"metadata": {"namespace": namespace, "name": name}},
    )

    plan = {
        **cleanup_plan(),
        "expectedSourceCommitSha": "abc123",
    }
    result = canary.wait_for_registry_operator_canary_promotion(
        promotion_wait_request(plan)
    ).status

    assert result["phase"] == "Promoted"
    assert result["ok"] is True
    assert read_namespaces == [runtime_namespace]


def test_canary_cleanup_requires_finalized_catalog_status(monkeypatch):
    monkeypatch.setattr(canary.kube, "read_namespace", lambda name: None)
    monkeypatch.setattr(canary.kube, "list_namespaces", lambda *, label_selector: [])
    monkeypatch.setattr(canary.kube, "read_git_repository", lambda namespace, name: None)
    monkeypatch.setattr(canary.kube, "read_demo_project", lambda name: None)
    monkeypatch.setattr(canary.kube, "read_config_map",
        lambda namespace, name: {
            "data": {
                "operator-canary-abc123.json": json.dumps(
                    {"phase": "Promoted"}
                )
            }
        },
    )

    result = canary.canary_cleanup_snapshot(cleanup_plan())

    assert result["phase"] == "CleanupPending"
    assert result["ok"] is False
    assert result["waitingOn"] == ["catalog"]
    assert result["catalog"]["ok"] is False


def test_canary_cleanup_accepts_finalized_catalog_status(monkeypatch):
    monkeypatch.setattr(canary.kube, "read_namespace", lambda name: None)
    monkeypatch.setattr(canary.kube, "list_namespaces", lambda *, label_selector: [])
    monkeypatch.setattr(canary.kube, "read_git_repository", lambda namespace, name: None)
    monkeypatch.setattr(canary.kube, "read_demo_project", lambda name: None)
    monkeypatch.setattr(canary.kube, "read_config_map",
        lambda namespace, name: {
            "data": {
                "operator-canary-abc123.json": json.dumps(
                    {
                        "phase": "Finalized",
                        "finalizeStatus": {
                            "externalResources": {"phase": "Deleted"}
                        },
                    }
                )
            }
        },
    )

    result = canary.canary_cleanup_snapshot(cleanup_plan())

    assert result["phase"] == "Deleted"
    assert result["ok"] is True


def test_canary_cleanup_waits_for_runtime_namespaces(monkeypatch):
    runtime_namespaces = [
        {"name": "tmprl-dem-cld-operator-canary-abc123-a1b2c"},
    ]

    def list_runtime_namespaces(*, label_selector):
        assert "app.kubernetes.io/name=operator-canary-abc123" in label_selector
        assert "platform.temporal.io/runtime-namespace=true" in label_selector
        return runtime_namespaces

    monkeypatch.setattr(canary.kube, "read_namespace", lambda name: None)
    monkeypatch.setattr(canary.kube, "list_namespaces", list_runtime_namespaces)
    monkeypatch.setattr(canary.kube, "read_git_repository", lambda namespace, name: None)
    monkeypatch.setattr(canary.kube, "read_demo_project", lambda name: None)
    monkeypatch.setattr(canary.kube, "read_config_map",
        lambda namespace, name: {
            "data": {
                "operator-canary-abc123.json": json.dumps(
                    {
                        "phase": "Finalized",
                        "finalizeStatus": {
                            "externalResources": {"phase": "Deleted"}
                        },
                    }
                )
            }
        },
    )

    result = canary.canary_cleanup_snapshot(cleanup_plan())

    assert result["phase"] == "CleanupPending"
    assert result["ok"] is False
    assert result["waitingOn"] == ["runtimeNamespaces"]
    assert result["runtimeNamespaces"] == runtime_namespaces


def test_canary_cleanup_wait_returns_when_cleanup_is_complete(monkeypatch):
    monkeypatch.setattr(canary.kube, "read_namespace", lambda name: None)
    monkeypatch.setattr(canary.kube, "list_namespaces", lambda *, label_selector: [])
    monkeypatch.setattr(canary.kube, "read_git_repository", lambda namespace, name: None)
    monkeypatch.setattr(canary.kube, "read_demo_project", lambda name: None)
    monkeypatch.setattr(canary.kube, "read_config_map",
        lambda namespace, name: {
            "data": {
                "operator-canary-abc123.json": json.dumps(
                    {
                        "phase": "Finalized",
                        "finalizeStatus": {
                            "externalResources": {"phase": "Deleted"}
                        },
                    }
                )
            }
        },
    )

    result = canary.wait_registry_operator_canary_cleanup(
        cleanup_wait_request({**cleanup_plan(), "deleteAction": "deleting"})
    ).status

    assert result["phase"] == "Deleted"
    assert result["ok"] is True
    assert result["deleteAction"] == "deleting"


def test_canary_cleanup_wait_times_out_on_remaining_resources(monkeypatch):
    monkeypatch.setattr(canary.kube, "read_namespace", lambda name: {"phase": "Active"})
    monkeypatch.setattr(canary.kube, "list_namespaces", lambda *, label_selector: [])
    monkeypatch.setattr(canary.kube, "read_git_repository", lambda namespace, name: None)
    monkeypatch.setattr(canary.kube, "read_demo_project", lambda name: None)
    monkeypatch.setattr(canary.kube, "read_config_map",
        lambda namespace, name: {
            "data": {
                "operator-canary-abc123.json": json.dumps(
                    {
                        "phase": "Finalized",
                        "finalizeStatus": {
                            "externalResources": {"phase": "Deleted"}
                        },
                    }
                )
            }
        },
    )

    with pytest.raises(ApplicationError) as raised:
        canary.wait_registry_operator_canary_cleanup(
            cleanup_wait_request(cleanup_plan())
        )
    result = raised.value.details[0]["status"]

    assert result["phase"] == "CleanupFailed"
    assert result["ok"] is False
    assert result["waitingOn"] == ["namespace"]
