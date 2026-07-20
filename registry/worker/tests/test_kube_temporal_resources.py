from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from demo_registry_operator import kube


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
