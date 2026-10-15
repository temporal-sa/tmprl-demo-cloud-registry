import socket
from types import SimpleNamespace

import pytest
from temporalio.exceptions import ApplicationError

from demo_registry_operator.activities.external_resources import models, temporal


HOST = "tmprl-dem-cld-demo.acct1.tmprl.cloud"


def request(grpc_address=f"{HOST}:7233", namespace_required=True):
    return models.WaitForTemporalCloudNamespaceDnsRequest(
        checked_at="2026-10-15T00:00:00+00:00",
        project=SimpleNamespace(namespace_required=namespace_required),
        namespace=models.TemporalCloudNamespaceResult(
            status={"phase": "Ready"},
            namespace="tmprl-dem-cld-demo.acct1",
            grpc_address=grpc_address,
            auth_mode="apiKey",
            created=True,
        ),
    )


def test_resolved_host_returns_sorted_unique_addresses(monkeypatch):
    seen = []

    def fake_getaddrinfo(host, port, **_kwargs):
        seen.append((host, port))
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("35.182.0.87", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("3.98.48.139", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("35.182.0.87", port)),
        ]

    monkeypatch.setattr(temporal.socket, "getaddrinfo", fake_getaddrinfo)

    result = temporal.wait_for_temporal_cloud_namespace_dns(request())

    assert seen == [(HOST, 7233)]
    assert result == models.TemporalCloudNamespaceDnsResult(
        host=HOST,
        addresses=("3.98.48.139", "35.182.0.87"),
    )


def test_unresolved_host_raises_a_retryable_error(monkeypatch):
    def fake_getaddrinfo(*_args, **_kwargs):
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(temporal.socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(ApplicationError) as raised:
        temporal.wait_for_temporal_cloud_namespace_dns(request())

    assert raised.value.type == temporal.TEMPORAL_NAMESPACE_DNS_PENDING
    assert not raised.value.non_retryable


@pytest.mark.parametrize(
    "kwargs",
    [{"namespace_required": False}, {"grpc_address": None}],
)
def test_nothing_to_resolve_skips_the_lookup(monkeypatch, kwargs):
    def fail_getaddrinfo(*_args, **_kwargs):
        raise AssertionError("must not resolve")

    monkeypatch.setattr(temporal.socket, "getaddrinfo", fail_getaddrinfo)

    result = temporal.wait_for_temporal_cloud_namespace_dns(request(**kwargs))

    assert result == models.TemporalCloudNamespaceDnsResult(host=None)
