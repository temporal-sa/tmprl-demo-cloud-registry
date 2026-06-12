from __future__ import annotations

import json
import sys
import threading
import time
from urllib.parse import parse_qs, urlparse
from http.server import ThreadingHTTPServer
from urllib.request import HTTPRedirectHandler, build_opener
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
import server


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def urlopen_no_redirect(request, timeout=2):
    return build_opener(NoRedirect).open(request, timeout=timeout)


def run_test_server(tmp_path: Path):
    handler = server.make_handler(
        tmp_path / "registry.json",
        tmp_path / "status",
    )
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


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
