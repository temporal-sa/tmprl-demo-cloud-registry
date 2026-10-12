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


def test_isolated_demo_rejects_crash_and_scale(tmp_path, monkeypatch):
    (tmp_path / "registry.json").write_text(
        json.dumps(
            {
                "projects": [
                    {
                        "name": "rainbow",
                        "isolatedLambda": {"enabled": True},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    calls = []

    class Gateway:
        def update_workspace(self, owner_email, demo_name, update_name):
            calls.append((owner_email, demo_name, update_name))
            return {"phase": "Active", "demo": demo_name}

        def ensure_workspace(self, owner_email, demo_name, workflow_type):
            calls.append((owner_email, demo_name, workflow_type))
            return {"phase": "Provisioning", "demo": demo_name}

    monkeypatch.setenv("AUTH_MODE", "bootstrap-allow")
    httpd = run_test_server(tmp_path, Gateway())
    try:
        for path in (
            "/api/crashable-workspace/crash",
            "/api/crashable-workspace/scale-down",
            "/api/crashable-workspace/scale-up",
        ):
            body = json.dumps({"demo": "rainbow"}).encode("utf-8")
            request = Request(
                f"http://127.0.0.1:{httpd.server_port}{path}",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                urlopen(request, timeout=2)
            except HTTPError as error:
                payload = json.loads(error.read().decode("utf-8"))
                assert error.code == 409
                assert payload["error"] == "worker-disruption-unsupported"
            else:
                raise AssertionError(f"{path} should reject an isolated demo")
        create = Request(
            f"http://127.0.0.1:{httpd.server_port}/api/crashable-workspace",
            data=json.dumps({"demo": "rainbow"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        response = urlopen(create, timeout=2)
        assert response.status == 200
        assert calls == [
            (
                "bootstrap@temporal.io",
                "rainbow",
                "IsolatedLambdaWorkspaceWorkflow",
            )
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
        claims = server.decode_jwt(
            response.headers["X-Temporal-Auth-JWT"],
            typ=server.TOKEN_TYPE_DOWNSTREAM,
        )
        assert claims["aud"] == "banking-app.tmprl-demo.cloud"
        assert claims["project_host"] == "banking-app.tmprl-demo.cloud"
    finally:
        httpd.shutdown()


def assert_verify_rejects_cookie(httpd, token):
    request = Request(
        f"http://127.0.0.1:{httpd.server_port}/_auth/verify",
        headers={
            "Cookie": f"{server.AUTH_SESSION_COOKIE}={token}",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "banking-app.tmprl-demo.cloud",
            "X-Forwarded-Uri": "/",
        },
    )
    with pytest.raises(HTTPError) as raised:
        urlopen_no_redirect(request)
    assert raised.value.code == 302
    assert urlparse(raised.value.headers["Location"]).path == "/auth/login"


def test_auth_verify_rejects_downstream_jwt_replayed_as_session(
    tmp_path, monkeypatch
):
    configure_google_auth(monkeypatch)
    httpd = run_test_server(tmp_path)
    try:
        response = urlopen(
            Request(
                f"http://127.0.0.1:{httpd.server_port}/_auth/verify",
                headers={
                    "Cookie": signed_session_cookie(),
                    "X-Forwarded-Host": "banking-app.tmprl-demo.cloud",
                },
            ),
            timeout=2,
        )
        downstream = response.headers["X-Temporal-Auth-JWT"]
        assert_verify_rejects_cookie(httpd, downstream)
    finally:
        httpd.shutdown()


def test_auth_verify_rejects_oauth_state_token_as_session(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    state = server.create_state_token({"return_to": "", "nonce": "n"})
    httpd = run_test_server(tmp_path)
    try:
        assert_verify_rejects_cookie(httpd, state)
    finally:
        httpd.shutdown()


def test_auth_verify_rejects_untyped_legacy_session(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    now = int(time.time())
    header = server.base64url_json({"alg": "HS256", "typ": "JWT"})
    body = server.base64url_json(
        {
            "iss": "https://catalog.tmprl-demo.cloud",
            "aud": "tmprl-demo.cloud",
            "sub": "google:google-subject",
            "email": "mjones@temporal.io",
            "iat": now,
            "exp": now + 300,
        }
    )
    signature = server.base64url_encode(
        server.hmac.new(
            b"test-signing-key",
            f"{header}.{body}".encode("ascii"),
            server.hashlib.sha256,
        ).digest()
    )
    httpd = run_test_server(tmp_path)
    try:
        assert_verify_rejects_cookie(httpd, f"{header}.{body}.{signature}")
    finally:
        httpd.shutdown()


def test_session_token_is_not_accepted_as_oauth_state(monkeypatch):
    configure_google_auth(monkeypatch)
    token = server.create_session_token(
        server.GoogleIdentity(
            subject="google-subject",
            email="mjones@temporal.io",
            name="M Jones",
            hosted_domain="temporal.io",
        )
    )
    with pytest.raises(server.AuthError):
        server.decode_state_token(token)


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


# --- workspace share links -----------------------------------------------

SHARE_DEMO = "canonical-ai-demo"
SHARE_HOST = "crash-0123abcd89.tmprl-demo.cloud"


class ShareGateway:
    def __init__(self, **status):
        self.status = {
            "phase": "Active",
            "demo": SHARE_DEMO,
            "workspace_id": "0123abcd89",
            "host": SHARE_HOST,
            "app_url": f"https://{SHARE_HOST}",
            "created_at": "2026-10-12T12:00:00+00:00",
            "share_epoch": 0,
            **status,
        }
        self.status_calls = []
        self.updates = []

    def workspace_status(self, owner, demo):
        self.status_calls.append((owner, demo))
        return dict(self.status)

    def update_workspace(self, owner, demo, update):
        self.updates.append((owner, demo, update))
        if update == "revoke_share_links":
            self.status["share_epoch"] += 1
        return dict(self.status)


@pytest.fixture
def share_env(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    monkeypatch.setattr(server, "SHARE_STATUS_CACHE", server.ShareStatusCache(30))
    (tmp_path / "registry.json").write_text(
        json.dumps(
            {
                "projects": [
                    {
                        "name": SHARE_DEMO,
                        "crashable": {"enabled": True},
                        "authRequired": True,
                    },
                    {
                        "name": "public-demo",
                        "crashable": {"enabled": True},
                        "authRequired": False,
                    },
                ]
            }
        )
    )
    gateway = ShareGateway()
    httpd = run_test_server(tmp_path, gateway)
    yield httpd, gateway
    httpd.shutdown()


def mint_share_link(httpd, *, demo=SHARE_DEMO, cookie=None, headers=None, body=None):
    return urlopen(
        Request(
            f"http://127.0.0.1:{httpd.server_port}/api/crashable-workspace/share",
            data=json.dumps(body or {"demo": demo}).encode(),
            headers={
                "Content-Type": "application/json",
                "Cookie": cookie or signed_session_cookie(),
                **(headers or {}),
            },
            method="POST",
        ),
        timeout=2,
    )


def share_token_from(url):
    return parse_qs(urlparse(url).query)[server.WORKSPACE_SHARE_PARAM][0]


def forward_auth(httpd, path, *, host=SHARE_HOST, uri="/", cookie=None):
    headers = {
        "X-Forwarded-Proto": "https",
        "X-Forwarded-Host": host,
        "X-Forwarded-Uri": uri,
    }
    if cookie:
        headers["Cookie"] = cookie
    request = Request(f"http://127.0.0.1:{httpd.server_port}{path}", headers=headers)
    try:
        return urlopen_no_redirect(request)
    except HTTPError as error:
        return error


def exchange_for_cookie(httpd, token, *, uri_suffix=""):
    response = forward_auth(
        httpd,
        "/_auth/verify-workspace",
        uri=f"/chat?{server.WORKSPACE_SHARE_PARAM}={token}{uri_suffix}",
    )
    assert response.code == 302
    set_cookie = response.headers["Set-Cookie"]
    value = set_cookie.split(";", 1)[0]
    assert value.startswith(f"{server.WORKSPACE_SHARE_COOKIE}=")
    return response, set_cookie, value


def test_share_link_mints_signed_link_for_running_workspace(share_env):
    httpd, _ = share_env
    payload = json.loads(mint_share_link(httpd).read())

    assert urlparse(payload["url"]).netloc == SHARE_HOST
    token = share_token_from(payload["url"])
    claims = server.decode_jwt(
        token, typ=server.TOKEN_TYPE_SHARE_LINK, key=server.share_signing_key()
    )
    assert claims["aud"] == SHARE_HOST
    assert claims["owner"] == "mjones@temporal.io"
    assert claims["epoch"] == 0
    assert claims["exp"] - claims["iat"] == server.DEFAULT_SHARE_SECONDS
    # Never valid under the main key or as any other token type.
    with pytest.raises(server.AuthError):
        server.decode_jwt(token, typ=server.TOKEN_TYPE_SHARE_LINK)
    with pytest.raises(server.AuthError):
        server.decode_jwt(
            token, typ=server.TOKEN_TYPE_SESSION, key=server.share_signing_key()
        )


def test_share_link_ttl_is_capped(share_env):
    httpd, _ = share_env
    payload = json.loads(
        mint_share_link(httpd, body={"demo": SHARE_DEMO, "ttl_seconds": 10**9}).read()
    )
    claims = server.decode_jwt(
        share_token_from(payload["url"]),
        typ=server.TOKEN_TYPE_SHARE_LINK,
        key=server.share_signing_key(),
    )
    assert claims["exp"] - claims["iat"] == server.MAX_SHARE_SECONDS


def test_share_link_requires_session_auth_gate_and_running_workspace(share_env):
    httpd, gateway = share_env
    with pytest.raises(HTTPError) as raised:
        mint_share_link(httpd, cookie="other=1")
    assert raised.value.code == 401

    with pytest.raises(HTTPError) as raised:
        mint_share_link(httpd, demo="public-demo")
    assert raised.value.code == 403

    gateway.status["phase"] = "Provisioning"
    with pytest.raises(HTTPError) as raised:
        mint_share_link(httpd)
    assert raised.value.code == 409

    gateway.status["phase"] = "Active"
    del gateway.status["share_epoch"]  # workspace from before share links
    with pytest.raises(HTTPError) as raised:
        mint_share_link(httpd)
    assert raised.value.code == 409


def test_share_link_refuses_cross_origin_requests(share_env):
    httpd, _ = share_env
    with pytest.raises(HTTPError) as raised:
        mint_share_link(
            httpd, headers={"Origin": "https://canonical-ai-demo.tmprl-demo.cloud"}
        )
    assert raised.value.code == 403
    with pytest.raises(HTTPError) as raised:
        mint_share_link(httpd, headers={"Sec-Fetch-Site": "same-site"})
    assert raised.value.code == 403


def test_share_link_exchanges_for_host_only_cookie_and_strips_token(share_env):
    httpd, _ = share_env
    token = share_token_from(json.loads(mint_share_link(httpd).read())["url"])

    response, set_cookie, _ = exchange_for_cookie(httpd, token, uri_suffix="&x=1")

    assert response.headers["Location"] == f"https://{SHARE_HOST}/chat?x=1"
    assert "Domain" not in set_cookie
    assert "HttpOnly" in set_cookie
    assert "SameSite=Lax" in set_cookie


def test_share_cookie_grants_guest_identity_on_its_workspace(share_env):
    httpd, _ = share_env
    token = share_token_from(json.loads(mint_share_link(httpd).read())["url"])
    _, _, cookie = exchange_for_cookie(httpd, token)

    response = forward_auth(httpd, "/_auth/verify-workspace", cookie=cookie)

    assert response.status == 204
    subject = response.headers["X-Temporal-Auth-Subject"]
    email = response.headers["X-Temporal-Auth-Email"]
    assert subject.startswith("share:")
    assert email.startswith("guest-")
    assert email.endswith(f"@{server.GUEST_EMAIL_DOMAIN}")
    assert "mjones" not in email
    downstream = server.decode_jwt(
        response.headers["X-Temporal-Auth-JWT"], typ=server.TOKEN_TYPE_DOWNSTREAM
    )
    assert downstream["guest"] is True
    assert downstream["aud"] == SHARE_HOST
    assert downstream["email"] == email

    # Same browser keeps its identity; another browser on the same link differs.
    again = forward_auth(httpd, "/_auth/verify-workspace", cookie=cookie)
    assert again.headers["X-Temporal-Auth-Email"] == email
    _, _, other_cookie = exchange_for_cookie(httpd, token)
    other = forward_auth(httpd, "/_auth/verify-workspace", cookie=other_cookie)
    assert other.headers["X-Temporal-Auth-Email"] != email
    assert other.headers["X-Temporal-Auth-Subject"] != subject


def test_main_verifier_ignores_share_links_and_cookies(share_env):
    httpd, _ = share_env
    token = share_token_from(json.loads(mint_share_link(httpd).read())["url"])
    _, _, cookie = exchange_for_cookie(httpd, token)
    session_value = cookie.split("=", 1)[1]

    for host in (SHARE_HOST, "canonical-ai-demo.tmprl-demo.cloud"):
        for request_cookie, uri in (
            (cookie, "/"),
            (None, f"/?{server.WORKSPACE_SHARE_PARAM}={token}"),
            (f"{server.AUTH_SESSION_COOKIE}={token}", "/"),
            (f"{server.AUTH_SESSION_COOKIE}={session_value}", "/"),
        ):
            response = forward_auth(
                httpd, "/_auth/verify", host=host, uri=uri, cookie=request_cookie
            )
            assert response.code == 302
            assert urlparse(response.headers["Location"]).path == "/auth/login"


def test_share_credentials_only_work_on_their_own_workspace_host(share_env):
    httpd, _ = share_env
    token = share_token_from(json.loads(mint_share_link(httpd).read())["url"])
    _, _, cookie = exchange_for_cookie(httpd, token)

    for host in ("crash-ffffffffff.tmprl-demo.cloud", "canonical-ai-demo.tmprl-demo.cloud"):
        # Browsers never send the host-only cookie elsewhere; if one is
        # replayed by hand it is still refused.
        response = forward_auth(
            httpd, "/_auth/verify-workspace", host=host, cookie=cookie
        )
        assert response.code == 403
        response = forward_auth(
            httpd,
            "/_auth/verify-workspace",
            host=host,
            uri=f"/?{server.WORKSPACE_SHARE_PARAM}={token}",
        )
        assert response.code == 403


@pytest.mark.parametrize(
    "change",
    [
        {"share_epoch": 1},
        {"created_at": "2026-10-12T15:00:00+00:00"},
        {"phase": "Deleting"},
    ],
    ids=["revoked", "recreated", "deleting"],
)
def test_share_access_ends_when_workspace_changes(share_env, monkeypatch, change):
    httpd, gateway = share_env
    token = share_token_from(json.loads(mint_share_link(httpd).read())["url"])
    _, _, cookie = exchange_for_cookie(httpd, token)

    gateway.status.update(change)
    monkeypatch.setattr(server, "SHARE_STATUS_CACHE", server.ShareStatusCache(30))

    response = forward_auth(httpd, "/_auth/verify-workspace", cookie=cookie)
    assert response.code == 403
    response = forward_auth(
        httpd,
        "/_auth/verify-workspace",
        uri=f"/?{server.WORKSPACE_SHARE_PARAM}={token}",
    )
    assert response.code == 403


def test_revoke_share_links_bumps_epoch_and_drops_cache(share_env):
    httpd, gateway = share_env
    token = share_token_from(json.loads(mint_share_link(httpd).read())["url"])
    _, _, cookie = exchange_for_cookie(httpd, token)
    assert forward_auth(httpd, "/_auth/verify-workspace", cookie=cookie).status == 204

    urlopen(
        Request(
            f"http://127.0.0.1:{httpd.server_port}"
            "/api/crashable-workspace/revoke-share-links",
            data=json.dumps({"demo": SHARE_DEMO}).encode(),
            headers={
                "Content-Type": "application/json",
                "Cookie": signed_session_cookie(),
            },
            method="POST",
        ),
        timeout=2,
    )

    assert gateway.updates == [
        ("mjones@temporal.io", SHARE_DEMO, "revoke_share_links")
    ]
    assert forward_auth(httpd, "/_auth/verify-workspace", cookie=cookie).code == 403


def test_employee_session_still_passes_workspace_verifier(share_env):
    httpd, _ = share_env
    response = forward_auth(
        httpd, "/_auth/verify-workspace", cookie=signed_session_cookie()
    )
    assert response.status == 204
    assert response.headers["X-Temporal-Auth-Email"] == "mjones@temporal.io"


def test_workspace_verifier_redirects_anonymous_visitors_to_login(share_env):
    httpd, _ = share_env
    response = forward_auth(httpd, "/_auth/verify-workspace")
    assert response.code == 302
    assert urlparse(response.headers["Location"]).path == "/auth/login"
