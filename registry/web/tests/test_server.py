from __future__ import annotations

import json
import asyncio
import sys
import threading
import time
from urllib.parse import parse_qs, urlparse
from http.server import ThreadingHTTPServer
from urllib.request import HTTPRedirectHandler, build_opener
from pathlib import Path
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
import server


@pytest.mark.parametrize("query_phase", ["Failed", "Deleting"])
def test_failed_workspace_returns_retained_failure_and_compensation(query_phase):
    status = {
        "phase": query_phase,
        "last_error": "pods did not become ready",
        "failure": {"activityType": "wait_workspace_ready", "scheduledEventId": 20},
        "compensation_status": {
            "phase": "Error",
            "failures": [{"message": "delete denied"}],
        },
    }
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(status=server.WorkflowExecutionStatus.FAILED)
        ),
        query=AsyncMock(return_value=status),
    )
    gateway = SimpleNamespace(
        _get_client=AsyncMock(
            return_value=SimpleNamespace(get_workflow_handle=lambda _: handle)
        )
    )
    result = asyncio.run(
        server.TemporalGateway._workspace_status(gateway, "user@temporal.io", "demo")
    )
    assert result == {**status, "phase": "Failed"}
    handle.query.assert_awaited_once_with("status", rpc_timeout=timedelta(seconds=5))


def test_failed_workspace_has_fallback_if_closed_execution_cannot_be_queried():
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(status=server.WorkflowExecutionStatus.FAILED)
        ),
        query=AsyncMock(
            side_effect=server.RPCError(
                "no worker", server.RPCStatusCode.DEADLINE_EXCEEDED, b""
            )
        ),
    )
    gateway = SimpleNamespace(
        _get_client=AsyncMock(
            return_value=SimpleNamespace(get_workflow_handle=lambda _: handle)
        )
    )
    result = asyncio.run(
        server.TemporalGateway._workspace_status(gateway, "user@temporal.io", "demo")
    )
    assert result["phase"] == "Failed"
    assert result["last_error"] == "Environment provisioning failed. Try again."


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def urlopen_no_redirect(request, timeout=2):
    return build_opener(NoRedirect).open(request, timeout=timeout)


def run_test_server(tmp_path: Path, temporal_gateway=None):
    handler = server.make_handler(
        tmp_path / "registry.json",
        tmp_path / "status",
        temporal_gateway,
    )
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


@pytest.mark.parametrize(
    ("path", "update"),
    [
        ("/api/crashable-workspace/scale-down", "scale_down"),
        ("/api/crashable-workspace/scale-up", "scale_up"),
    ],
)
def test_scale_endpoint_dispatches_workspace_update(
    tmp_path, monkeypatch, path, update
):
    calls = []

    class Gateway:
        def update_workspace(self, owner_email, demo_name, update_name):
            calls.append((owner_email, demo_name, update_name))
            return {"phase": "Scaling", "demo": demo_name}

    monkeypatch.setenv("AUTH_MODE", "bootstrap-allow")
    httpd = run_test_server(tmp_path, Gateway())
    try:
        body = json.dumps({"demo": "canonical-ai-demo"}).encode("utf-8")
        response = urlopen(
            Request(
                f"http://127.0.0.1:{httpd.server_port}{path}",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            ),
            timeout=2,
        )
        assert response.status == 200
        assert json.loads(response.read().decode("utf-8"))["demo"] == (
            "canonical-ai-demo"
        )
        assert calls == [
            ("bootstrap@temporal.io", "canonical-ai-demo", update)
        ]
    finally:
        httpd.shutdown()


def test_auth_verify_requires_explicit_bootstrap_mode(tmp_path, monkeypatch):
    monkeypatch.delenv("AUTH_MODE", raising=False)
    httpd = run_test_server(tmp_path)
    try:
        request = Request(f"http://127.0.0.1:{httpd.server_port}/_auth/verify")
        try:
            urlopen(request, timeout=2)
        except HTTPError as error:
            assert error.code == 503
            payload = json.loads(error.read().decode("utf-8"))
            assert payload["error"] == "auth_not_configured"
        else:
            raise AssertionError("expected auth verification to fail")
    finally:
        httpd.shutdown()


def test_auth_verify_denies_in_deny_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "deny")
    httpd = run_test_server(tmp_path)
    try:
        request = Request(f"http://127.0.0.1:{httpd.server_port}/_auth/verify")
        try:
            urlopen(request, timeout=2)
        except HTTPError as error:
            assert error.code == 401
            payload = json.loads(error.read().decode("utf-8"))
            assert payload["error"] == "unauthorized"
        else:
            raise AssertionError("expected auth verification to fail")
    finally:
        httpd.shutdown()


def test_auth_verify_allows_in_bootstrap_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "bootstrap-allow")
    httpd = run_test_server(tmp_path)
    try:
        response = urlopen(
            f"http://127.0.0.1:{httpd.server_port}/_auth/verify",
            timeout=2,
        )
        assert response.status == 204
        assert response.headers["X-Temporal-Auth-Email"] == "bootstrap@temporal.io"
        assert response.read() == b""
    finally:
        httpd.shutdown()


def configure_google_auth(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "google")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "google-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "google-client-secret")
    monkeypatch.setenv("AUTH_COOKIE_SIGNING_KEY", "test-signing-key")
    monkeypatch.setenv("AUTH_COOKIE_SECURE", "false")
    monkeypatch.setenv("AUTH_COOKIE_DOMAIN", ".tmprl-demo.cloud")
    monkeypatch.setenv("AUTH_BASE_URL", "https://catalog.tmprl-demo.cloud")
    monkeypatch.setenv("AUTH_ISSUER", "https://catalog.tmprl-demo.cloud")
    monkeypatch.setenv("AUTH_ALLOWED_DOMAIN", "temporal.io")


def test_auth_verify_redirects_to_central_login_without_cookie(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    httpd = run_test_server(tmp_path)
    try:
        request = Request(
            f"http://127.0.0.1:{httpd.server_port}/_auth/verify",
            headers={
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": "banking-app.tmprl-demo.cloud",
                "X-Forwarded-Uri": "/transfer?id=123",
            },
        )
        try:
            urlopen_no_redirect(request)
        except HTTPError as error:
            assert error.code == 302
            location = error.headers["Location"]
            parsed = urlparse(location)
            assert parsed.scheme == "https"
            assert parsed.netloc == "catalog.tmprl-demo.cloud"
            assert parsed.path == "/auth/login"
            assert parse_qs(parsed.query)["return_to"] == [
                "https://banking-app.tmprl-demo.cloud/transfer?id=123"
            ]
        else:
            raise AssertionError("expected auth verification to redirect")
    finally:
        httpd.shutdown()


def test_auth_verify_allows_signed_session_cookie(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    token = server.create_session_token(
        server.GoogleIdentity(
            subject="google-subject",
            email="mjones@temporal.io",
            name="M Jones",
            hosted_domain="temporal.io",
        )
    )
    httpd = run_test_server(tmp_path)
    try:
        request = Request(
            f"http://127.0.0.1:{httpd.server_port}/_auth/verify",
            headers={
                "Cookie": f"{server.AUTH_SESSION_COOKIE}={token}",
                "X-Forwarded-Host": "banking-app.tmprl-demo.cloud",
            },
        )
        response = urlopen(request, timeout=2)
        assert response.status == 204
        assert response.headers["X-Temporal-Auth-Email"] == "mjones@temporal.io"
        assert response.headers["X-Temporal-Auth-Subject"] == "google:google-subject"
        assert response.headers["X-Temporal-Auth-Name"] == "M Jones"
        claims = server.decode_jwt(response.headers["X-Temporal-Auth-JWT"])
        assert claims["aud"] == "banking-app.tmprl-demo.cloud"
        assert claims["project_host"] == "banking-app.tmprl-demo.cloud"
    finally:
        httpd.shutdown()


def test_google_callback_sets_shared_domain_cookie(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)

    token_payload = {
        "iss": "https://accounts.google.com",
        "aud": "google-client-id",
        "sub": "google-subject",
        "email": "mjones@temporal.io",
        "email_verified": True,
        "hd": "temporal.io",
        "name": "M Jones",
        "exp": int(time.time()) + 300,
    }
    id_token = ".".join(
        [
            server.base64url_json({"alg": "none", "typ": "JWT"}),
            server.base64url_json(token_payload),
            "",
        ]
    )
    monkeypatch.setattr(
        server,
        "exchange_google_code",
        lambda code, *, redirect_uri: {"id_token": id_token},
    )

    nonce = "nonce-value"
    state = server.create_state_token(
        {
            "return_to": "https://banking-app.tmprl-demo.cloud/transfer",
            "nonce": nonce,
        }
    )
    httpd = run_test_server(tmp_path)
    try:
        request = Request(
            f"http://127.0.0.1:{httpd.server_port}/oauth/google/callback?"
            f"code=google-code&state={state}",
            headers={"Cookie": f"{server.AUTH_STATE_COOKIE}={nonce}"},
        )
        try:
            urlopen_no_redirect(request)
        except HTTPError as error:
            assert error.code == 302
            assert error.headers["Location"] == (
                "https://banking-app.tmprl-demo.cloud/transfer"
            )
            set_cookies = error.headers.get_all("Set-Cookie")
            session_cookie = next(
                value
                for value in set_cookies
                if value.startswith(f"{server.AUTH_SESSION_COOKIE}=")
            )
            assert "Domain=.tmprl-demo.cloud" in session_cookie
            assert "HttpOnly" in session_cookie
            assert "SameSite=Lax" in session_cookie
        else:
            raise AssertionError("expected Google callback to redirect")
    finally:
        httpd.shutdown()


class RecordingAccessGateway:
    def __init__(self, result=None, error=None):
        self.emails = []
        self.result = result or {
            "status": "added",
            "email": "mjones@temporal.io",
            "group": "tmprl-demo-cld-read",
        }
        self.error = error

    def grant_namespace_access(self, email):
        self.emails.append(email)
        if self.error:
            raise self.error
        return self.result


def post_namespace_access(httpd, *, cookie=None, body=b"{}"):
    headers = {"Content-Type": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    return urlopen(
        Request(
            f"http://127.0.0.1:{httpd.server_port}/api/namespace-access",
            data=body,
            headers=headers,
            method="POST",
        ),
        timeout=2,
    )


def signed_session_cookie(email="mjones@temporal.io"):
    token = server.create_session_token(
        server.GoogleIdentity(
            subject="google-subject",
            email=email,
            name="M Jones",
            hosted_domain="temporal.io",
        )
    )
    return f"{server.AUTH_SESSION_COOKIE}={token}"


def test_namespace_access_requires_a_signed_in_user(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = RecordingAccessGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            post_namespace_access(httpd)
        assert raised.value.code == 401
        assert gateway.emails == []
    finally:
        httpd.shutdown()


def test_namespace_access_grants_the_session_user_not_the_request_body(
    tmp_path, monkeypatch
):
    configure_google_auth(monkeypatch)
    gateway = RecordingAccessGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        response = post_namespace_access(
            httpd,
            cookie=signed_session_cookie(),
            body=json.dumps({"email": "someone-else@temporal.io"}).encode("utf-8"),
        )
        assert response.status == 200
        assert json.loads(response.read().decode("utf-8"))["status"] == "added"
        assert gateway.emails == ["mjones@temporal.io"]
    finally:
        httpd.shutdown()


@pytest.mark.parametrize(
    "gateway",
    [None, RecordingAccessGateway(error=server.TemporalUnavailable("down"))],
    ids=["no-temporal", "temporal-error"],
)
def test_namespace_access_is_unavailable_without_temporal(tmp_path, monkeypatch, gateway):
    configure_google_auth(monkeypatch)
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            post_namespace_access(httpd, cookie=signed_session_cookie())
        assert raised.value.code == 503
        payload = json.loads(raised.value.read().decode("utf-8"))
        assert payload["error"] == "temporal-unavailable"
    finally:
        httpd.shutdown()


def test_namespace_access_workflow_id_matches_the_operator():
    # demo_registry_operator.derive.namespace_read_access_workflow_id
    assert (
        server.namespace_access_workflow_id(" MJones@temporal.io ")
        == "namespace-readers/grant/mjones@temporal.io"
    )
