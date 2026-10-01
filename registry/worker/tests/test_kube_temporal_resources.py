from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from demo_registry_operator import kube, resources


@pytest.mark.parametrize("role_arn", [None, "arn:aws:iam::429214323166:role/monty-payloads"])
def test_service_account_patch_reconciles_iam_role_annotation(monkeypatch, role_arn):
    annotations = {"platform.temporal.io/component": "web"}
    if role_arn is not None:
        annotations["eks.amazonaws.com/role-arn"] = role_arn
    resource = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "web", "namespace": "monty", "annotations": annotations},
        "automountServiceAccountToken": False,
    }
    calls = []
    api = SimpleNamespace(
        read_namespaced_service_account=lambda *args: {},
        patch_namespaced_service_account=lambda *args: calls.append(args),
    )
    monkeypatch.setattr(kube, "_client_module", lambda: SimpleNamespace(CoreV1Api=lambda: api))
    monkeypatch.setattr(kube, "load_kubernetes_config", lambda: None)

    assert kube.apply_resource(resource) == "patched"
    assert calls[0][:2] == ("web", "monty")
    assert calls[0][2]["metadata"]["annotations"] == annotations
    assert ("eks.amazonaws.com/role-arn" in annotations) == (role_arn is not None)


@pytest.mark.parametrize("cluster", ["blue", "green"])
def test_registry_operator_reconcile_preserves_bootstrap_iam_role(monkeypatch, cluster):
    root = Path(__file__).resolve().parents[3]
    bootstrap_path = root / f"clusters/{cluster}/bootstrap/registry-operator/rbac.yaml"
    bootstrap_service_account = next(
        resource
        for resource in yaml.safe_load_all(bootstrap_path.read_text())
        if resource["kind"] == "ServiceAccount"
    )
    project = yaml.safe_load((root / "projects/operator/registry.yaml").read_text())
    project["spec"]["components"]["registry-operator"].pop("awsIamRoleArn", None)
    service_account = next(
        resource
        for resource in resources.rbac_resources(project)
        if resource["kind"] == "ServiceAccount"
        and resource["metadata"]["name"] == "registry-operator"
    )
    existing_annotations = dict(bootstrap_service_account["metadata"]["annotations"])
    expected_role = existing_annotations["eks.amazonaws.com/role-arn"]

    def patch_service_account(name, namespace, body):
        assert name == bootstrap_service_account["metadata"]["name"]
        assert namespace == bootstrap_service_account["metadata"]["namespace"]
        for annotation_name, value in body["metadata"]["annotations"].items():
            if value is None:
                existing_annotations.pop(annotation_name, None)
            else:
                existing_annotations[annotation_name] = value

    api = SimpleNamespace(
        read_namespaced_service_account=lambda *args: SimpleNamespace(
            metadata=SimpleNamespace(annotations=existing_annotations)
        ),
        patch_namespaced_service_account=patch_service_account,
    )
    monkeypatch.setattr(kube, "_client_module", lambda: SimpleNamespace(CoreV1Api=lambda: api))
    monkeypatch.setattr(kube, "load_kubernetes_config", lambda: None)

    assert kube.apply_resource(service_account) == "patched"
    assert existing_annotations["eks.amazonaws.com/role-arn"] == expected_role
    assert existing_annotations["platform.temporal.io/component"] == "registry-operator"


@pytest.mark.parametrize("cluster", ["blue", "green"])
def test_registry_operator_self_upgrade_role_matches_bootstrap(cluster):
    root = Path(__file__).resolve().parents[3]
    bootstrap_path = root / f"clusters/{cluster}/bootstrap/registry-operator/rbac.yaml"
    bootstrap_service_account = next(
        resource
        for resource in yaml.safe_load_all(bootstrap_path.read_text())
        if resource["kind"] == "ServiceAccount"
    )
    project = yaml.safe_load((root / "projects/operator/registry.yaml").read_text())
    role_arn = project["spec"]["components"]["registry-operator"]["awsIamRoleArn"]
    service_account = next(
        resource
        for resource in resources.rbac_resources(project)
        if resource["kind"] == "ServiceAccount"
        and resource["metadata"]["name"] == "registry-operator"
    )

    assert role_arn == bootstrap_service_account["metadata"]["annotations"][
        "eks.amazonaws.com/role-arn"
    ]
    assert service_account["metadata"]["annotations"]["eks.amazonaws.com/role-arn"] == role_arn
    legacy_patch_annotations = {
        "eks.amazonaws.com/role-arn": None,
        **service_account["metadata"]["annotations"],
    }
    assert legacy_patch_annotations["eks.amazonaws.com/role-arn"] == role_arn


class ApiError(Exception):
    def __init__(self, status: int | str):
        self.status = status


@dataclass
class FakeCustomObjectsApi:
    list_response: Any = field(default_factory=lambda: {"items": []})
    get_response: Any = field(default_factory=dict)
    list_error: Exception | None = None
    get_error: Exception | None = None
    delete_error: Exception | None = None
    patch_error: Exception | None = None
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = field(
        default_factory=list
    )

    def list_namespaced_custom_object(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("list", args, kwargs))
        if self.list_error is not None:
            raise self.list_error
        return self.list_response

    def get_namespaced_custom_object(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("get", args, kwargs))
        if self.get_error is not None:
            raise self.get_error
        return self.get_response

    def delete_namespaced_custom_object(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("delete", args, kwargs))
        if self.delete_error is not None:
            raise self.delete_error
        return {}

    def patch_namespaced_custom_object(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("patch", args, kwargs))
        if self.patch_error is not None:
            raise self.patch_error
        return {}


@pytest.fixture
def install_fake_client(monkeypatch):
    def install(api: FakeCustomObjectsApi) -> None:
        client = SimpleNamespace(CustomObjectsApi=lambda: api)
        monkeypatch.setattr(kube, "_client_module", lambda: client)
        monkeypatch.setattr(kube, "load_kubernetes_config", lambda: None)

    return install


@pytest.mark.parametrize(
    ("list_resource", "plural"),
    [
        (kube.list_worker_deployments, kube.TEMPORAL_WORKER_DEPLOYMENT_PLURAL),
        (kube.list_connections, kube.TEMPORAL_CONNECTION_PLURAL),
    ],
)
def test_list_temporal_resources_uses_expected_api(
    install_fake_client,
    list_resource,
    plural,
):
    resource = {"metadata": {"name": "one"}}
    api = FakeCustomObjectsApi(list_response={"items": [resource, "invalid"]})
    install_fake_client(api)

    assert list_resource("runtime-ns", label_selector="app=demo") == [resource]
    assert api.calls == [
        (
            "list",
            (
                kube.TEMPORAL_WORKER_CONTROLLER_GROUP,
                kube.TEMPORAL_WORKER_CONTROLLER_VERSION,
                "runtime-ns",
                plural,
            ),
            {"label_selector": "app=demo"},
        )
    ]


def test_list_worker_deployment_images_reads_build_and_primary_template_image(
    monkeypatch,
):
    monkeypatch.setattr(
        kube,
        "list_worker_deployments",
        lambda namespace: [
            {
                "metadata": {
                    "annotations": {"platform.temporal.io/build": "worker"}
                },
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {"image": "example.com/worker:git-123"},
                                {"image": "example.com/sidecar:git-123"},
                            ]
                        }
                    }
                },
            },
            {"metadata": {"annotations": {}}},
        ],
    )

    assert kube.list_worker_deployment_images("runtime-ns") == {
        "worker": "example.com/worker:git-123"
    }


def test_scale_worker_deployment_patches_replica_count(install_fake_client):
    api = FakeCustomObjectsApi()
    install_fake_client(api)

    kube.scale_worker_deployment("runtime-ns", "worker", 0)

    assert api.calls == [
        (
            "patch",
            (
                kube.TEMPORAL_WORKER_CONTROLLER_GROUP,
                kube.TEMPORAL_WORKER_CONTROLLER_VERSION,
                "runtime-ns",
                kube.TEMPORAL_WORKER_DEPLOYMENT_PLURAL,
                "worker",
                {"spec": {"replicas": 0}},
            ),
            {},
        )
    ]


def test_scale_deployment_patches_replica_count(monkeypatch):
    calls = []
    apps_api = SimpleNamespace(
        patch_namespaced_deployment=lambda *args: calls.append(args)
    )
    client = SimpleNamespace(AppsV1Api=lambda: apps_api)
    monkeypatch.setattr(kube, "_client_module", lambda: client)
    monkeypatch.setattr(kube, "load_kubernetes_config", lambda: None)

    kube.scale_deployment("runtime-ns", "web", 1)

    assert calls == [("web", "runtime-ns", {"spec": {"replicas": 1}})]


def test_list_external_secrets_uses_expected_api(install_fake_client):
    resource = {"metadata": {"name": "temporal-credentials"}}
    api = FakeCustomObjectsApi(list_response={"items": [resource, "invalid"]})
    install_fake_client(api)

    assert kube.list_external_secrets("runtime-ns") == [resource]
    assert api.calls == [
        (
            "list",
            (
                kube.EXTERNAL_SECRETS_GROUP,
                kube.EXTERNAL_SECRETS_VERSION,
                "runtime-ns",
                kube.EXTERNAL_SECRET_PLURAL,
            ),
            {},
        )
    ]


@pytest.mark.parametrize(
    ("read_resource", "plural"),
    [
        (kube.read_worker_deployment, kube.TEMPORAL_WORKER_DEPLOYMENT_PLURAL),
        (kube.read_connection, kube.TEMPORAL_CONNECTION_PLURAL),
    ],
)
def test_read_temporal_resource_returns_resource_and_maps_404_to_none(
    install_fake_client,
    read_resource,
    plural,
):
    resource = {"metadata": {"name": "one"}}
    api = FakeCustomObjectsApi(get_response=resource)
    install_fake_client(api)

    assert read_resource("runtime-ns", "one") == resource
    assert api.calls == [
        (
            "get",
            (
                kube.TEMPORAL_WORKER_CONTROLLER_GROUP,
                kube.TEMPORAL_WORKER_CONTROLLER_VERSION,
                "runtime-ns",
                plural,
                "one",
            ),
            {},
        )
    ]

    api.get_error = ApiError("404")
    assert read_resource("runtime-ns", "gone") is None


@pytest.mark.parametrize(
    ("delete_resource", "plural"),
    [
        (kube.delete_worker_deployment, kube.TEMPORAL_WORKER_DEPLOYMENT_PLURAL),
        (kube.delete_connection, kube.TEMPORAL_CONNECTION_PLURAL),
    ],
)
def test_delete_temporal_resource_is_idempotent(
    install_fake_client,
    delete_resource,
    plural,
):
    api = FakeCustomObjectsApi()
    install_fake_client(api)

    assert delete_resource("runtime-ns", "one") == "deleting"
    assert api.calls == [
        (
            "delete",
            (
                kube.TEMPORAL_WORKER_CONTROLLER_GROUP,
                kube.TEMPORAL_WORKER_CONTROLLER_VERSION,
                "runtime-ns",
                plural,
                "one",
            ),
            {},
        )
    ]

    api.delete_error = ApiError(404)
    assert delete_resource("runtime-ns", "gone") == "not-found"
    assert delete_resource("runtime-ns", "dry", dry_run=True) == "planned"


def test_temporal_resource_helpers_propagate_non_404_errors(install_fake_client):
    api = FakeCustomObjectsApi(get_error=ApiError(403), delete_error=ApiError(409))
    install_fake_client(api)

    with pytest.raises(ApiError):
        kube.read_worker_deployment("runtime-ns", "one")
    with pytest.raises(ApiError):
        kube.delete_connection("runtime-ns", "one")
