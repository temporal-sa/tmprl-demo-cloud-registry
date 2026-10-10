import pytest

from demo_registry_operator import resources
from demo_registry_operator.crashable_workspace_resources import build_workspace_manifests


def test_advanced_agent_executor_is_isolated_in_runtime_and_crashable_clone():
    project = {"metadata": {"name": "advanced-ai-agent"}, "spec": {
        "source": {"repo": "https://github.com/temporal-sa/test"},
        "builds": {"app": {"context": "."}, "redis": {"context": "."}},
        "registry": {"crashable": {"enabled": True, "taskQueueEnv": "SIMPLE_CHAT_TASK_QUEUE", "baseTaskQueue": "advanced-ai-agent"}},
        "components": {
            "advanced-ai-agent-worker": {
                "build": "app", "terminationGracePeriodSeconds": 1260,
                "worker": {"versioning": {"enabled": True, "deploymentName": "advanced-ai-agent"}},
            },
            "executor": {"build": "app", "isolatedExecutor": True, "ports": [{"containerPort": 8082, "servicePort": 80}], "env": [
                {"name": "WORKSPACE_EXECUTOR_TOKEN", "value": "test-only-token"},
                {"name": "WORKSPACE_EXECUTOR_ROOT", "value": "/workspaces"},
                {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
            ]},
        },
        "ingress": {"subdomain": "advanced-ai-agent", "component": "executor", "port": "http"},
    }}
    images = {"app": "example/app:abc", "redis": "example/redis:abc"}
    manifests = resources.runtime_resources(project, images)
    cloned, task_queue, _, _, _ = build_workspace_manifests(project, images, "0123456789")
    for manifest_set in (manifests, cloned):
        executor = next(m for m in manifest_set if m["kind"] == "Deployment" and m["metadata"]["annotations"]["platform.temporal.io/component"] == "executor")
        pod = executor["spec"]["template"]["spec"]
        assert not pod["automountServiceAccountToken"]
        assert pod["securityContext"]["runAsNonRoot"]
        assert pod["containers"][0]["securityContext"]["capabilities"] == {"drop": ["ALL"]}
        assert "envFrom" not in pod["containers"][0]
        assert [e["name"] for e in pod["containers"][0]["env"]] == ["WORKSPACE_EXECUTOR_TOKEN", "WORKSPACE_EXECUTOR_ROOT", "PYTHONDONTWRITEBYTECODE"]
        assert "169.254.0.0/16" in pod["initContainers"][0]["command"][2]
        assert pod["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/healthz"
        worker = next(m for m in manifest_set if m["kind"] == "WorkerDeployment")
        assert worker["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == 1260
    worker = next(m for m in cloned if m["kind"] == "WorkerDeployment")
    worker_env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    assert next(e["value"] for e in worker_env if e["name"] == "SIMPLE_CHAT_TASK_QUEUE") == task_queue
    assert worker["metadata"]["name"] != "advanced-ai-agent"


@pytest.mark.parametrize("field,value", [("worker", True), ("temporalAccess", True), ("awsIamRoleArn", "arn:aws:iam::123456789012:role/x"), ("secrets", ["anthropic"])])
def test_executor_profile_rejects_credentials(field, value):
    project = {"metadata": {"name": "test"}, "spec": {"components": {"executor": {"build": "app", "isolatedExecutor": True, field: value}}}}
    with pytest.raises(ValueError, match="cannot receive runtime credentials"):
        resources.workload_pod_spec(project, "executor", project["spec"]["components"]["executor"], "image")
