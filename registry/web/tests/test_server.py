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


@pytest.mark.parametrize(
    ("host", "uri", "code"),
    [
        # The catalog's own API answers fetch() with a 401 it can act on.
        ("catalog.tmprl-demo.cloud", "/api/catalog", 401),
        ("catalog.tmprl-demo.cloud", "/api/helper?x=1", 401),
        # Catalog pages and every demo's paths, /api/* included, still redirect.
        ("catalog.tmprl-demo.cloud", "/demos/canonical-ai-demo", 302),
        ("catalog.tmprl-demo.cloud", "/apis", 302),
        ("banking-app.tmprl-demo.cloud", "/api/transfer", 302),
    ],
)
def test_auth_verify_rejects_catalog_api_without_redirect(
    tmp_path, monkeypatch, host, uri, code
):
    configure_google_auth(monkeypatch)
    httpd = run_test_server(tmp_path)
    try:
        request = Request(
            f"http://127.0.0.1:{httpd.server_port}/_auth/verify",
            headers={
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": host,
                "X-Forwarded-Uri": uri,
            },
        )
        with pytest.raises(HTTPError) as raised:
            urlopen_no_redirect(request)
    finally:
        httpd.shutdown()

    assert raised.value.code == code
    if code == 401:
        assert json.loads(raised.value.read().decode("utf-8"))["error"] == "unauthorized"


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


# --- catalog onboarding --------------------------------------------------


class OnboardingGateway:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def _record(self, *call):
        self.calls.append(call)
        if self.error:
            raise self.error
        return {"phase": "Drafting", "revision": 3}

    def onboarding_status(self, email):
        return self._record("status", email)

    def onboarding_start(self, email):
        return self._record("start", email)

    def onboarding_update(self, email, update, arg):
        return self._record(update, email, arg)

    def preview_status(self, email):
        return self._record("preview_status", email)

    def preview_end(self, email):
        return self._record("preview_end", email)

    def demo_control_status(self, email):
        return self._record("demo_control_status", email)

    def demo_control_deploy(self, email, request):
        return self._record("demo_control_deploy", email, request)

    def demo_control_end(self, email):
        return self._record("demo_control_end", email)


def onboarding_request(httpd, path, *, body=None, method="POST", headers=None):
    return urlopen(
        Request(
            f"http://127.0.0.1:{httpd.server_port}{path}",
            data=None if body is None else json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Cookie": signed_session_cookie(),
                **(headers or {}),
            },
            method=method,
        ),
        timeout=2,
    )


@pytest.mark.parametrize(
    ("path", "body", "expected"),
    [
        (
            "/api/onboarding/repo",
            {"repo_url": " https://github.com/temporalio/demo ", "owner": "x"},
            ("set_repo", "mjones@temporal.io", {"repo_url": "https://github.com/temporalio/demo"}),
        ),
        (
            "/api/onboarding/draft",
            {"base_revision": 3, "yaml": "kind: DemoProject\n"},
            ("update_draft", "mjones@temporal.io", {"base_revision": 3, "yaml": "kind: DemoProject\n"}),
        ),
        (
            "/api/onboarding/draft",
            {"base_revision": 3, "document": {"kind": "DemoProject"}},
            ("update_draft", "mjones@temporal.io", {"base_revision": 3, "document": {"kind": "DemoProject"}}),
        ),
        (
            "/api/onboarding/submit",
            {"pull_request_url": "https://github.com/temporal-sa/tmprl-demo-cloud-registry/pull/9"},
            ("submit_pull_request", "mjones@temporal.io", {"pull_request_url": "https://github.com/temporal-sa/tmprl-demo-cloud-registry/pull/9"}),
        ),
        ("/api/onboarding/abandon", {}, ("abandon", "mjones@temporal.io", None)),
        (
            "/api/onboarding/preview",
            {"document": {"kind": "DemoProject"}},
            ("deploy_preview", "mjones@temporal.io", None),
        ),
        ("/api/onboarding/preview/end", {}, ("preview_end", "mjones@temporal.io")),
        ("/api/onboarding", {}, ("start", "mjones@temporal.io")),
    ],
)
def test_onboarding_endpoints_forward_only_known_fields_for_the_session_user(
    tmp_path, monkeypatch, path, body, expected
):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        response = onboarding_request(httpd, path, body=body)
        assert response.status == 200
        assert gateway.calls == [expected]
    finally:
        httpd.shutdown()


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/api/onboarding/repo", {}),
        ("/api/onboarding/draft", {"yaml": "x"}),
        ("/api/onboarding/draft", {"base_revision": 1, "yaml": "x", "document": {}}),
        ("/api/onboarding/draft", {"base_revision": True, "yaml": "x"}),
        ("/api/onboarding/submit", {"pull_request_url": " "}),
    ],
)
def test_onboarding_rejects_malformed_edits(tmp_path, monkeypatch, path, body):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(httpd, path, body=body)
        assert raised.value.code == 400
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_onboarding_refuses_cross_origin_writes(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(
                httpd,
                "/api/onboarding/abandon",
                body={},
                headers={"Origin": "https://evil.tmprl-demo.cloud"},
            )
        assert raised.value.code == 403
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_onboarding_status_reads_the_session_users_workflow(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        response = onboarding_request(httpd, "/api/onboarding", method="GET")
        assert json.loads(response.read().decode("utf-8"))["phase"] == "Drafting"
        assert gateway.calls == [("status", "mjones@temporal.io")]
    finally:
        httpd.shutdown()


def test_onboarding_rejections_are_conflicts_with_the_workflow_message(
    tmp_path, monkeypatch
):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway(error=server.OnboardingRejected("Pick a repo first."))
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(
                httpd, "/api/onboarding/draft", body={"base_revision": 0, "yaml": "x"}
            )
        assert raised.value.code == 409
        payload = json.loads(raised.value.read().decode("utf-8"))
        assert payload == {"error": "rejected", "message": "Pick a repo first."}
    finally:
        httpd.shutdown()


def test_onboarding_rejects_oversized_drafts(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(
                httpd,
                "/api/onboarding/draft",
                body={"base_revision": 0, "yaml": "x" * server.ONBOARDING_MAX_BODY_BYTES},
            )
        assert raised.value.code == 413
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_onboarding_page_is_served_by_the_spa(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    httpd = run_test_server(tmp_path)
    try:
        response = urlopen(f"http://127.0.0.1:{httpd.server_port}/onboard", timeout=2)
        assert response.status == 200
        assert b"data-view=\"onboard\"" in response.read()
    finally:
        httpd.shutdown()


def test_preview_status_reads_the_session_users_preview(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        response = onboarding_request(httpd, "/api/onboarding/preview", method="GET")
        assert response.status == 200
        assert gateway.calls == [("preview_status", "mjones@temporal.io")]
    finally:
        httpd.shutdown()


def test_ending_a_preview_refuses_cross_origin_requests(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(
                httpd,
                "/api/onboarding/preview/end",
                body={},
                headers={"Origin": "https://evil.tmprl-demo.cloud"},
            )
        assert raised.value.code == 403
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_preview_workflow_id_matches_the_operator():
    # demo_registry_operator.derive.onboarding_preview_workflow_id
    assert server.onboarding_preview_workflow_id(" Ada@Temporal.io ", "blue") == (
        "onboarding-preview/blue/ada@temporal.io"
    )
    assert server.onboarding_preview_workflow_id("ada@temporal.io", None) == (
        "onboarding-preview/ada@temporal.io"
    )


def test_preview_status_is_none_without_a_preview_and_final_once_ended():
    async def status(describe):
        handle = SimpleNamespace(
            describe=describe, result=AsyncMock(return_value={"phase": "Ended"})
        )
        gateway = SimpleNamespace(
            cluster="blue",
            _get_client=AsyncMock(
                return_value=SimpleNamespace(get_workflow_handle=lambda _id: handle)
            ),
        )
        return await server.TemporalGateway._read_preview(
            gateway, "onboarding-preview/blue/ada@temporal.io"
        )

    missing = AsyncMock(
        side_effect=server.RPCError("missing", server.RPCStatusCode.NOT_FOUND, b"")
    )
    ended = AsyncMock(
        return_value=SimpleNamespace(status=server.WorkflowExecutionStatus.COMPLETED)
    )

    assert asyncio.run(status(missing)) == {"phase": "None"}
    assert asyncio.run(status(ended)) == {"phase": "Ended"}


SAMPLE = {
    "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
    "kind": "DemoProject",
    "metadata": {"name": "infra-demo"},
    "spec": {"source": {"repo": "https://github.com/temporal-sa/x", "branch": "a" * 40}},
}


def test_control_page_is_served_by_the_spa(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    httpd = run_test_server(tmp_path)
    try:
        response = urlopen(f"http://127.0.0.1:{httpd.server_port}/control", timeout=2)
        assert response.status == 200
        assert b"data-view=\"control\"" in response.read()
    finally:
        httpd.shutdown()


def test_control_deploys_the_session_users_copy_of_the_pinned_sample(
    tmp_path, monkeypatch
):
    configure_google_auth(monkeypatch)
    monkeypatch.setattr(server, "demo_control_sample", lambda: SAMPLE)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        response = onboarding_request(httpd, "/api/control/provision", body={})
        assert response.status == 200
        [(call, email, request)] = gateway.calls
        project = server.demo_control_project("mjones@temporal.io")
        assert (call, email) == ("demo_control_deploy", "mjones@temporal.io")
        assert request["project"] == project
        assert request["document"]["metadata"] == {"name": project}
        assert request["commit_sha"] == "a" * 40
        assert SAMPLE["metadata"] == {"name": "infra-demo"}  # not mutated
    finally:
        httpd.shutdown()


def test_control_refuses_to_deploy_an_unpinned_sample(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    unpinned = {**SAMPLE, "spec": {"source": {"branch": "0" * 40}}}
    monkeypatch.setattr(server, "demo_control_sample", lambda: unpinned)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(httpd, "/api/control/provision", body={})
        assert raised.value.code == 409
        assert gateway.calls == []
    finally:
        httpd.shutdown()


@pytest.mark.parametrize(
    ("path", "method", "call"),
    [
        ("/api/control/provision", "GET", "demo_control_status"),
        ("/api/control/provision/end", "POST", "demo_control_end"),
    ],
)
def test_control_status_and_end_act_on_the_session_user(
    tmp_path, monkeypatch, path, method, call
):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        body = {} if method == "POST" else None
        response = onboarding_request(httpd, path, body=body, method=method)
        assert response.status == 200
        assert gateway.calls == [(call, "mjones@temporal.io")]
    finally:
        httpd.shutdown()


@pytest.mark.parametrize("path", ["/api/control/provision", "/api/control/provision/end"])
def test_control_writes_refuse_cross_origin_requests(tmp_path, monkeypatch, path):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(
                httpd, path, body={}, headers={"Origin": "https://evil.tmprl-demo.cloud"}
            )
        assert raised.value.code == 403
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_control_ids_and_names():
    assert server.demo_control_workflow_id(" Ada@Temporal.io ", "blue") == (
        "demo-control/blue/ada@temporal.io"
    )
    assert server.demo_control_workflow_id("ada@temporal.io", None) == (
        "demo-control/ada@temporal.io"
    )
    project = server.demo_control_project(" Ada@Temporal.io ")
    assert project == server.demo_control_project("ada@temporal.io")
    assert project.startswith("infra-demo-") and len(project) == len("infra-demo-") + 6


def test_control_links_point_at_the_entity_workflow_and_sample_namespace():
    links = server.demo_control_links(
        {"name": "pv-infra-demo-abc123"},
        namespace="tmprl-dem-cld-registry-operator.a2dd6",
        cluster="blue",
    )
    assert links["entityWorkflow"] == (
        "https://cloud.temporal.io/namespaces/tmprl-dem-cld-registry-operator.a2dd6"
        "/workflows?query=WorkflowId%3D%22demo-project%2Fblue%2Fpv-infra-demo-abc123%22"
    )
    assert links["sampleNamespace"] == (
        "https://cloud.temporal.io/namespaces/tmprl-dem-cld-pv-infra-demo-abc123.a2dd6/workflows"
    )
    assert server.demo_control_links({"phase": "None"}, namespace="x.y", cluster=None) == {}


def test_control_sample_ships_pinned_to_a_commit_or_not_at_all():
    sample = server.demo_control_sample()
    branch = sample["spec"]["source"]["branch"]
    assert server.COMMIT_SHA_PATTERN.match(branch)


def test_onboarding_workflow_id_matches_the_operator():
    # demo_registry_operator.derive.catalog_onboarding_workflow_id
    assert server.onboarding_workflow_id(" Ada@Temporal.io ", "blue") == (
        "catalog-onboarding/blue/ada@temporal.io"
    )
    assert server.onboarding_workflow_id("ada@temporal.io", None) == (
        "catalog-onboarding/ada@temporal.io"
    )


def test_onboarding_status_returns_the_final_status_of_a_completed_run():
    final = {"phase": "HandedOff", "pull_request_url": "https://github.com/x/pull/1"}
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(
                status=server.WorkflowExecutionStatus.COMPLETED
            )
        ),
        result=AsyncMock(return_value=final),
    )
    ids = []
    gateway = SimpleNamespace(
        cluster="blue",
        _get_client=AsyncMock(
            return_value=SimpleNamespace(
                get_workflow_handle=lambda workflow_id: ids.append(workflow_id)
                or handle
            )
        ),
    )

    result = asyncio.run(
        server.TemporalGateway._onboarding_status(gateway, "ada@temporal.io")
    )

    assert result == final
    assert ids == ["catalog-onboarding/blue/ada@temporal.io"]


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ({"mode": "draft"}, {"mode": "draft"}),
        (
            {"mode": "section", "pointer": "/spec/components", "question": " Workers? ", "owner": "x"},
            {"mode": "section", "pointer": "/spec/components", "question": "Workers?"},
        ),
        (
            {"mode": "chat", "question": "No secrets needed.", "pointer": " "},
            {"mode": "chat", "question": "No secrets needed."},
        ),
    ],
)
def test_onboarding_agent_requests_forward_only_known_fields(
    tmp_path, monkeypatch, body, expected
):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        onboarding_request(httpd, "/api/onboarding/agent", body=body)
        onboarding_request(httpd, "/api/onboarding/agent/cancel", body={})
        onboarding_request(httpd, "/api/onboarding/agent/new", body={})
        assert gateway.calls == [
            ("request_agent", "mjones@temporal.io", expected),
            ("cancel_agent", "mjones@temporal.io", None),
            ("new_agent_conversation", "mjones@temporal.io", None),
        ]
    finally:
        httpd.shutdown()


def test_onboarding_agent_rejects_unknown_modes(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = OnboardingGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(httpd, "/api/onboarding/agent", body={"mode": "deploy"})
        assert raised.value.code == 400
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_onboarding_status_adds_the_running_agents_latest_step():
    from temporalio.api.common.v1 import Payloads
    from temporalio.api.workflow.v1 import PendingActivityInfo
    from temporalio.api.workflowservice.v1 import DescribeWorkflowExecutionResponse
    from temporalio.converter import DataConverter

    async def scenario():
        payloads = await DataConverter.default.encode([{"progress": "Reading main.go"}])
        raw = DescribeWorkflowExecutionResponse(
            pending_activities=[
                PendingActivityInfo(
                    activity_type={"name": "validate_onboarding_draft"},
                ),
                PendingActivityInfo(
                    activity_type={"name": "run_onboarding_agent"},
                    heartbeat_details=Payloads(payloads=payloads),
                ),
            ]
        )
        handle = SimpleNamespace(
            describe=AsyncMock(
                return_value=SimpleNamespace(
                    status=server.WorkflowExecutionStatus.RUNNING,
                    raw_description=raw,
                )
            ),
            query=AsyncMock(return_value={"phase": "Drafting", "agent": {"phase": "Running"}}),
        )
        client = SimpleNamespace(
            get_workflow_handle=lambda _: handle,
            data_converter=DataConverter.default,
        )
        gateway = SimpleNamespace(cluster="blue", _get_client=AsyncMock(return_value=client))
        return await server.TemporalGateway._onboarding_status(gateway, "ada@temporal.io")

    status = asyncio.run(scenario())

    assert status["agent"]["progress"] == "Reading main.go"


# --- onboarding secrets ---------------------------------------------------


class NotFound(Exception):
    response = {"Error": {"Code": "ResourceNotFoundException"}}


class FakeSecretsManager:
    def __init__(self, secrets=None):
        # name -> {"SecretString", "Description", "Tags"}
        self.secrets = secrets or {}
        self.calls = []

    def list_secrets(self, Filters, NextToken=None):
        prefix = Filters[0]["Values"][0]
        return {
            "SecretList": [
                {"Name": name, "Description": item.get("Description"), "Tags": item.get("Tags", [])}
                for name, item in self.secrets.items()
                if name.startswith(prefix)
            ]
        }

    def describe_secret(self, SecretId):
        if SecretId not in self.secrets:
            raise NotFound()
        return {"Name": SecretId, "Tags": self.secrets[SecretId].get("Tags", [])}

    def create_secret(self, Name, SecretString, Description, Tags):
        self.calls.append(("create", Name))
        self.secrets[Name] = {"SecretString": SecretString, "Description": Description, "Tags": Tags}

    def put_secret_value(self, SecretId, SecretString):
        self.calls.append(("put", SecretId))
        self.secrets[SecretId]["SecretString"] = SecretString

    def update_secret(self, SecretId, Description):
        self.calls.append(("describe-update", SecretId))
        self.secrets[SecretId]["Description"] = Description


def onboarding_tags(owner):
    return [
        {"Key": server.SECRET_MANAGED_BY_TAG, "Value": server.SECRET_MANAGED_BY},
        {"Key": server.SECRET_OWNER_TAG, "Value": owner},
    ]


def test_store_creates_a_tagged_secret_with_property_names_in_the_description():
    fake = FakeSecretsManager()
    store = server.OnboardingSecrets(lambda: fake)

    result = store.store(
        "money", "api", {"B_KEY": "b", "A_KEY": "a"},
        owner="ada@temporal.io", onboarding_id="catalog-onboarding/blue/ada@temporal.io",
    )

    secret = fake.secrets["tmprl-dem-cld/money/api"]
    assert json.loads(secret["SecretString"]) == {"B_KEY": "b", "A_KEY": "a"}
    assert secret["Description"] == server.SECRET_DESCRIPTION_PREFIX + "A_KEY B_KEY"
    assert {tag["Key"]: tag["Value"] for tag in secret["Tags"]} == {
        server.SECRET_MANAGED_BY_TAG: "catalog-onboarding",
        server.SECRET_OWNER_TAG: "ada@temporal.io",
        server.SECRET_PROJECT_TAG: "money",
        server.SECRET_ONBOARDING_TAG: "catalog-onboarding/blue/ada@temporal.io",
    }
    assert result == {"name": "api", "properties": ["A_KEY", "B_KEY"], "created": True}


def test_store_updates_your_own_onboarding_secret_without_retagging():
    fake = FakeSecretsManager(
        {"tmprl-dem-cld/money/api": {"SecretString": "{}", "Tags": onboarding_tags("ada@temporal.io")}}
    )
    store = server.OnboardingSecrets(lambda: fake)

    store.store("money", "api", {"KEY": "new"}, owner="ada@temporal.io", onboarding_id="x")

    assert fake.calls == [("put", "tmprl-dem-cld/money/api"), ("describe-update", "tmprl-dem-cld/money/api")]
    assert json.loads(fake.secrets["tmprl-dem-cld/money/api"]["SecretString"]) == {"KEY": "new"}


@pytest.mark.parametrize(
    ("tags", "message"),
    [
        ([], "wasn't created by onboarding"),
        (onboarding_tags("bob@temporal.io"), "belongs to bob@temporal.io"),
    ],
)
def test_store_never_overwrites_someone_elses_secret(tags, message):
    fake = FakeSecretsManager({"tmprl-dem-cld/money/api": {"SecretString": "{}", "Tags": tags}})
    store = server.OnboardingSecrets(lambda: fake)

    with pytest.raises(server.OnboardingRejected, match=message):
        store.store("money", "api", {"KEY": "v"}, owner="ada@temporal.io", onboarding_id="x")

    assert fake.calls == []


def test_list_shows_metadata_only():
    fake = FakeSecretsManager(
        {
            "tmprl-dem-cld/money/api": {
                "SecretString": '{"KEY": "hidden"}',
                "Description": server.SECRET_DESCRIPTION_PREFIX + "KEY",
                "Tags": onboarding_tags("ada@temporal.io"),
            },
            "tmprl-dem-cld/platform/shared/gemini": {
                "SecretString": "x",
                "Description": "Shared gemini",
                "Tags": [{"Key": "properties", "Value": "GEMINI_API_KEY GOOGLE_API_KEY"}],
            },
        }
    )
    store = server.OnboardingSecrets(lambda: fake)

    assert store.list_project("money") == [
        {"name": "api", "properties": ["KEY"], "owner": "ada@temporal.io", "onboarding": True, "updated_at": None}
    ]
    assert store.list_shared() == [
        {"name": "gemini", "properties": ["GEMINI_API_KEY", "GOOGLE_API_KEY"], "description": "Shared gemini"}
    ]
    assert "hidden" not in json.dumps(store.list_project("money"))


class SecretsGateway(OnboardingGateway):
    cluster = "blue"

    def __init__(self, status):
        super().__init__()
        self.status = status

    def onboarding_status(self, email):
        self.calls.append(("status", email))
        return self.status


def drafting(name="money"):
    return {"phase": "Drafting", "document": {"metadata": {"name": name}}}


def secrets_server(tmp_path, monkeypatch, status, fake=None, catalog=()):
    configure_google_auth(monkeypatch)
    (tmp_path / "registry.json").write_text(
        json.dumps({"projects": [{"name": name} for name in catalog]})
    )
    handler = server.make_handler(
        tmp_path / "registry.json",
        tmp_path / "status",
        SecretsGateway(status),
        server.OnboardingSecrets(lambda: fake or FakeSecretsManager()),
    )
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def test_storing_a_secret_uses_the_session_users_draft_and_echoes_no_values(tmp_path, monkeypatch):
    fake = FakeSecretsManager()
    httpd = secrets_server(tmp_path, monkeypatch, drafting(), fake)
    try:
        response = onboarding_request(
            httpd,
            "/api/onboarding/secrets",
            body={"name": "api", "values": {"API_KEY": "s3cret"}, "project": "someone-else"},
        )
        payload = response.read().decode("utf-8")
        assert json.loads(payload) == {"name": "api", "properties": ["API_KEY"], "created": True, "project": "money"}
        assert "s3cret" not in payload
        tags = {t["Key"]: t["Value"] for t in fake.secrets["tmprl-dem-cld/money/api"]["Tags"]}
        assert tags[server.SECRET_OWNER_TAG] == "mjones@temporal.io"
        assert tags[server.SECRET_ONBOARDING_TAG] == "catalog-onboarding/blue/mjones@temporal.io"
    finally:
        httpd.shutdown()


@pytest.mark.parametrize(
    ("status", "catalog", "code"),
    [
        ({"phase": "None"}, (), 409),
        ({"phase": "HandedOff", "document": {"metadata": {"name": "money"}}}, (), 409),
        (drafting("Not A Label"), (), 409),
        (drafting(), ("money",), 409),
    ],
    ids=["no-onboarding", "handed-off", "invalid-name", "already-in-catalog"],
)
def test_secrets_are_only_stored_for_an_open_draft_not_in_the_catalog(
    tmp_path, monkeypatch, status, catalog, code
):
    fake = FakeSecretsManager()
    httpd = secrets_server(tmp_path, monkeypatch, status, fake, catalog)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(httpd, "/api/onboarding/secrets", body={"name": "api", "values": {"K": "v"}})
        assert raised.value.code == code
        assert fake.secrets == {}
    finally:
        httpd.shutdown()


@pytest.mark.parametrize(
    "body",
    [
        {"name": "Bad_Name", "values": {"K": "v"}},
        {"name": "api", "values": {}},
        {"name": "api", "values": {"lower": "v"}},
        {"name": "api", "values": {"K": ""}},
        {"name": "api", "values": {"K": 5}},
        {"name": "api", "values": {"K": "x" * (server.MAX_SECRET_VALUE_CHARS + 1)}},
    ],
)
def test_malformed_secrets_are_rejected_without_echoing_values(tmp_path, monkeypatch, body):
    fake = FakeSecretsManager()
    httpd = secrets_server(tmp_path, monkeypatch, drafting(), fake)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(httpd, "/api/onboarding/secrets", body=body)
        assert raised.value.code == 400
        assert "xxxxx" not in raised.value.read().decode("utf-8")
        assert fake.secrets == {}
    finally:
        httpd.shutdown()


def test_listing_secrets_for_the_draft(tmp_path, monkeypatch):
    fake = FakeSecretsManager(
        {"tmprl-dem-cld/money/api": {"Description": server.SECRET_DESCRIPTION_PREFIX + "K", "Tags": onboarding_tags("x")}}
    )
    httpd = secrets_server(tmp_path, monkeypatch, drafting(), fake)
    try:
        payload = json.loads(
            onboarding_request(httpd, "/api/onboarding/secrets", method="GET").read()
        )
        assert payload["project"] == "money"
        assert [item["name"] for item in payload["secrets"]] == ["api"]
        assert payload["shared"] == []
    finally:
        httpd.shutdown()


# --- catalog helper (Ziggy) ---------------------------------------------------


class HelperGateway:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def _record(self, *call):
        self.calls.append(call)
        if self.error:
            raise self.error
        return {"phase": "Open", "messages": []}

    def helper_status(self, email):
        return self._record("status", email)

    def helper_send(self, email, text):
        return self._record("send", email, text)

    def helper_update(self, email, update):
        return self._record(update, email)


@pytest.mark.parametrize(
    ("path", "body", "method", "expected"),
    [
        ("/api/helper", None, "GET", ("status", "mjones@temporal.io")),
        (
            "/api/helper/message",
            {"text": "  AI demos in Go  ", "owner": "x"},
            "POST",
            ("send", "mjones@temporal.io", "AI demos in Go"),
        ),
        ("/api/helper/clear-view", {}, "POST", ("clear_view", "mjones@temporal.io")),
        ("/api/helper/new", {}, "POST", ("end", "mjones@temporal.io")),
    ],
)
def test_helper_endpoints_drive_the_session_users_chat(
    tmp_path, monkeypatch, path, body, method, expected
):
    configure_google_auth(monkeypatch)
    gateway = HelperGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        response = onboarding_request(httpd, path, body=body, method=method)
        assert response.status == 200
        assert gateway.calls == [expected]
    finally:
        httpd.shutdown()


@pytest.mark.parametrize(
    ("path", "body", "code"),
    [
        ("/api/helper/message", {"text": "  "}, 400),
        ("/api/helper/message", {"text": 7}, 400),
        ("/api/helper/message", {"text": "x" * server.HELPER_MAX_BODY_BYTES}, 413),
        ("/api/helper/delete-everything", {}, 404),
    ],
)
def test_helper_rejects_bad_requests(tmp_path, monkeypatch, path, body, code):
    configure_google_auth(monkeypatch)
    gateway = HelperGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(httpd, path, body=body)
        assert raised.value.code == code
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_helper_needs_a_session_and_same_origin_writes(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = HelperGateway()
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            urlopen(f"http://127.0.0.1:{httpd.server_port}/api/helper", timeout=2)
        assert raised.value.code == 401
        with pytest.raises(HTTPError) as raised:
            onboarding_request(
                httpd,
                "/api/helper/message",
                body={"text": "hi"},
                headers={"Origin": "https://evil.tmprl-demo.cloud"},
            )
        assert raised.value.code == 403
        assert gateway.calls == []
    finally:
        httpd.shutdown()


def test_helper_rejections_are_conflicts_with_the_workflow_message(tmp_path, monkeypatch):
    configure_google_auth(monkeypatch)
    gateway = HelperGateway(error=server.HelperRejected("Ziggy is still answering your last message."))
    httpd = run_test_server(tmp_path, gateway)
    try:
        with pytest.raises(HTTPError) as raised:
            onboarding_request(httpd, "/api/helper/message", body={"text": "hi"})
        assert raised.value.code == 409
        assert json.loads(raised.value.read().decode("utf-8"))["message"] == (
            "Ziggy is still answering your last message."
        )
    finally:
        httpd.shutdown()


def test_helper_workflow_id_matches_the_operator():
    # demo_registry_operator.derive.catalog_helper_workflow_id
    assert server.helper_workflow_id(" Ada@Temporal.io ", "blue") == (
        "catalog-helper/blue/ada@temporal.io"
    )
    assert server.helper_workflow_id("ada@temporal.io", None) == (
        "catalog-helper/ada@temporal.io"
    )


@pytest.mark.parametrize(
    "status", ["COMPLETED", "TERMINATED", "TIMED_OUT"]
)
def test_helper_status_forgets_chats_that_ended(status):
    handle = SimpleNamespace(
        describe=AsyncMock(
            return_value=SimpleNamespace(status=getattr(server.WorkflowExecutionStatus, status))
        ),
        query=AsyncMock(),
    )
    gateway = SimpleNamespace(
        cluster="blue",
        _get_client=AsyncMock(
            return_value=SimpleNamespace(get_workflow_handle=lambda _: handle)
        ),
    )

    result = asyncio.run(server.TemporalGateway._helper_status(gateway, "ada@temporal.io"))

    assert result == {"phase": "None"}
    handle.query.assert_not_awaited()


def test_helper_send_starts_a_chat_when_none_is_open():
    client = SimpleNamespace(execute_update_with_start_workflow=AsyncMock(return_value={"phase": "Open"}))
    gateway = SimpleNamespace(
        cluster="blue",
        task_queue="registry-management-blue",
        _get_client=AsyncMock(return_value=client),
    )

    result = asyncio.run(server.TemporalGateway._helper_send(gateway, "ada@temporal.io", "hi"))

    assert result == {"phase": "Open"}
    args, kwargs = client.execute_update_with_start_workflow.await_args
    assert args == ("send", {"text": "hi"})
    operation = kwargs["start_workflow_operation"]
    assert operation._start_workflow_input.id == "catalog-helper/blue/ada@temporal.io"
    assert operation._start_workflow_input.id_conflict_policy == server.WorkflowIDConflictPolicy.USE_EXISTING


def fetch_page(httpd, path):
    response = urlopen(f"http://127.0.0.1:{httpd.server_port}{path}", timeout=2)
    assert response.status == 200
    assert response.headers["Content-Type"] == "text/html; charset=utf-8"
    return response.read().decode("utf-8")


def write_catalog(tmp_path):
    (tmp_path / "registry.json").write_text(
        json.dumps(
            {
                "baseDomain": "tmprl-demo.cloud",
                "projects": [
                    {
                        "name": "canonical-ai-demo",
                        "title": "Canonical AI <Demo>",
                        "summary": "An agent that survives crashes & restarts.",
                        "temporalSdks": ["Python"],
                        "aiSdks": ["OpenAI Agents SDK"],
                        "tags": ["ai", "agents"],
                    },
                    {"name": "plain-demo", "summary": "No screenshot here."},
                ],
                "courses": [
                    {
                        "name": "intro-python",
                        "parent": "intro",
                        "title": "Intro (Python)",
                        "temporalSdks": ["Python"],
                        "duration": {"minHours": 2, "maxHours": 3},
                    },
                    {
                        "name": "intro",
                        "parent": "intro",
                        "title": "Temporal 101",
                        "summary": "Learn Temporal by building.",
                        "temporalSdks": ["Go"],
                        "duration": {"minHours": 2, "maxHours": 2},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def test_demo_page_has_link_preview_meta(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTH_BASE_URL", "https://catalog.example.test")
    write_catalog(tmp_path)
    httpd = run_test_server(tmp_path)
    try:
        html = fetch_page(httpd, "/demos/canonical-ai-demo")
    finally:
        httpd.shutdown()

    assert "<title>Canonical AI &lt;Demo&gt; · Temporal Field Kit</title>" in html
    assert '<meta property="og:title" content="Canonical AI &lt;Demo&gt;" />' in html
    assert (
        '<meta property="og:description" '
        'content="An agent that survives crashes &amp; restarts." />'
    ) in html
    assert (
        '<meta property="og:url" '
        'content="https://catalog.example.test/demos/canonical-ai-demo" />'
    ) in html
    # Canonical demos with a screenshot preview with it.
    assert (
        '<meta property="og:image" content="https://catalog.example.test'
        '/static/screenshots/canonical-ai-demo.jpg" />'
    ) in html
    assert '<meta name="twitter:data1" content="Python, OpenAI Agents SDK" />' in html
    assert '<meta name="twitter:data2" content="ai, agents" />' in html
    assert server.SOCIAL_META_MARKER not in html


def test_demo_without_screenshot_uses_default_image(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTH_BASE_URL", "https://catalog.example.test")
    write_catalog(tmp_path)
    httpd = run_test_server(tmp_path)
    try:
        html = fetch_page(httpd, "/demos/plain-demo")
    finally:
        httpd.shutdown()

    assert '<meta property="og:title" content="plain-demo" />' in html
    assert "https://catalog.example.test/static/og-image.png" in html
    assert "twitter:label1" not in html


def test_workshop_page_merges_its_tracks(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTH_BASE_URL", "https://catalog.example.test")
    write_catalog(tmp_path)
    httpd = run_test_server(tmp_path)
    try:
        html = fetch_page(httpd, "/courses/intro")
    finally:
        httpd.shutdown()

    assert '<meta property="og:title" content="Temporal 101" />' in html
    assert '<meta property="og:description" content="Learn Temporal by building." />' in html
    assert '<meta name="twitter:data1" content="Python, Go" />' in html
    assert '<meta name="twitter:data2" content="2–3 hours, 2 hours" />' in html


@pytest.mark.parametrize(
    ("path", "title", "url"),
    [
        ("/", "Temporal Field Kit", "https://catalog.example.test/"),
        ("/courses", "Workshops · Temporal Field Kit", "https://catalog.example.test/courses"),
        ("/demos/missing", "Temporal Field Kit", "https://catalog.example.test/"),
        ("/courses/missing", "Workshops · Temporal Field Kit", "https://catalog.example.test/courses"),
    ],
)
def test_catalog_pages_have_site_preview_meta(tmp_path, monkeypatch, path, title, url):
    monkeypatch.setenv("AUTH_BASE_URL", "https://catalog.example.test")
    write_catalog(tmp_path)
    httpd = run_test_server(tmp_path)
    try:
        html = fetch_page(httpd, path)
    finally:
        httpd.shutdown()

    assert f"<title>{title}</title>" in html
    assert f'<meta property="og:url" content="{url}" />' in html
    assert "https://catalog.example.test/static/og-image.png" in html


def test_unknown_page_serves_not_found_page(tmp_path):
    httpd = run_test_server(tmp_path)
    try:
        with pytest.raises(HTTPError) as raised:
            urlopen(f"http://127.0.0.1:{httpd.server_port}/no-such-page", timeout=2)
    finally:
        httpd.shutdown()

    assert raised.value.code == 404
    assert raised.value.headers["Content-Type"] == "text/html; charset=utf-8"
    assert "We couldn't find that page." in raised.value.read().decode("utf-8")


@pytest.mark.parametrize("path", ["/api/no-such-endpoint", "/_auth/no-such-check"])
def test_unknown_api_path_stays_json(tmp_path, path):
    httpd = run_test_server(tmp_path)
    try:
        with pytest.raises(HTTPError) as raised:
            urlopen(f"http://127.0.0.1:{httpd.server_port}{path}", timeout=2)
    finally:
        httpd.shutdown()

    assert raised.value.code == 404
    assert json.loads(raised.value.read().decode("utf-8")) == {
        "error": "not_found",
        "path": path,
    }


def test_missing_static_file_stays_json(tmp_path):
    httpd = run_test_server(tmp_path)
    try:
        with pytest.raises(HTTPError) as raised:
            urlopen(f"http://127.0.0.1:{httpd.server_port}/static/missing.png", timeout=2)
    finally:
        httpd.shutdown()

    assert raised.value.code == 404
    assert raised.value.headers["Content-Type"] == "application/json; charset=utf-8"


def test_page_still_renders_when_catalog_is_unreadable(tmp_path, monkeypatch):
    (tmp_path / "registry.json").write_text("not json", encoding="utf-8")
    httpd = run_test_server(tmp_path)
    try:
        html = fetch_page(httpd, "/demos/canonical-ai-demo")
    finally:
        httpd.shutdown()

    assert "<title>Temporal Field Kit</title>" in html
