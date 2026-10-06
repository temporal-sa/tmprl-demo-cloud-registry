from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import mimetypes
import os
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import timedelta
from html import escape
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, unquote, urlparse
from urllib.request import Request, urlopen

# Temporal is only needed for the crashable-workspace endpoints. Import it
# defensively so the catalog server still boots in environments where the
# temporalio wheel is absent (the endpoints then report temporal-unavailable).
try:
    from temporalio.client import (
        Client,
        WithStartWorkflowOperation,
        WorkflowExecutionStatus,
    )
    from temporalio.common import WorkflowIDConflictPolicy
    from temporalio.service import RPCError, RPCStatusCode

    TEMPORALIO_AVAILABLE = True
except Exception:  # noqa: BLE001 - optional dependency
    TEMPORALIO_AVAILABLE = False


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DEFAULT_REGISTRY_PATH = ROOT / "data" / "registry.json"
DEFAULT_STATUS_DIR = ROOT / "status"
AUTH_SESSION_COOKIE = "temporal_demo_auth"
AUTH_STATE_COOKIE = "temporal_demo_auth_state"
DEFAULT_AUTH_BASE_URL = "https://catalog.tmprl-demo.cloud"
DEFAULT_COOKIE_DOMAIN = ".tmprl-demo.cloud"
DEFAULT_AUTH_ISSUER = "https://catalog.tmprl-demo.cloud"
DEFAULT_ALLOWED_DOMAIN = "temporal.io"
DEFAULT_SESSION_SECONDS = 60 * 60 * 24 * 7
DEFAULT_DOWNSTREAM_JWT_SECONDS = 5 * 60
GOOGLE_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_ISSUERS = {"https://accounts.google.com", "accounts.google.com"}

CRASHABLE_WORKFLOW_TYPE = "CrashableWorkspaceWorkflow"
DEFAULT_CRASHABLE_TASK_QUEUE = "registry-management"
# Bound how long a sync request thread will block on a Temporal RPC.
CRASHABLE_RPC_TIMEOUT_SECONDS = 30

# Self-service "grant yourself access": the operator adds the signed-in user to
# the Temporal Cloud user group that can read every demo namespace.
NAMESPACE_ACCESS_WORKFLOW_TYPE = "GrantNamespaceReadAccessWorkflow"
# Usually a few seconds; the very first grant may also create the group and
# backfill its namespace grants.
NAMESPACE_ACCESS_RPC_TIMEOUT_SECONDS = 90


class AuthError(Exception):
    pass


class GoogleOAuthError(Exception):
    pass


@dataclass(frozen=True)
class GoogleIdentity:
    subject: str
    email: str
    name: str | None
    hosted_domain: str | None


def configured_path(env_name: str, default: Path) -> Path:
    # Do NOT resolve() symlinks here. REGISTRY_DATA_PATH / CATALOG_STATUS_DIR are
    # Kubernetes ConfigMap volume mounts, which update via an atomic symlink swap
    # (registry.json -> ..data -> ..<timestamp>/). resolve() would bake in the
    # timestamped realpath at startup; when the operator republishes the catalog,
    # Kubernetes swaps ..data to a new dir and deletes the old one, leaving the
    # resolved path pointing at a deleted directory — so the server would serve
    # the empty fallback forever. abspath() normalizes without following symlinks,
    # so each open() re-follows the stable path to the current target.
    return Path(os.path.abspath(os.environ.get(env_name, str(default))))


def load_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def load_registry(path: Path) -> dict[str, Any]:
    # The catalog is published at runtime by the operator. Tolerate it being
    # absent or not-yet-written (e.g. before the first publish) by serving an
    # empty catalog rather than erroring.
    if not path.exists():
        return {"baseDomain": "tmprl-demo.cloud", "projects": []}
    registry = load_json_file(path)
    projects = registry.get("projects")
    if not isinstance(projects, list):
        raise ValueError(f"{path} must contain a projects list")
    return registry


def status_key(project: dict[str, Any]) -> str | None:
    catalog_status = project.get("catalogStatus")
    if not isinstance(catalog_status, dict):
        return None
    config_map = catalog_status.get("configMap")
    if not isinstance(config_map, dict):
        return None
    key = config_map.get("key")
    if not isinstance(key, str) or not key:
        return None
    if Path(key).name != key:
        return None
    return key


def load_project_status(project: dict[str, Any], status_dir: Path) -> dict[str, Any] | None:
    key = status_key(project)
    if key is None:
        return None
    path = (status_dir / key).resolve()
    if status_dir not in path.parents:
        return None
    if not path.exists():
        return None
    try:
        return load_json_file(path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {
            "phase": "StatusUnavailable",
            "message": f"Could not read {key}: {error}",
        }


def catalog_payload(registry_path: Path, status_dir: Path) -> dict[str, Any]:
    registry = load_registry(registry_path)
    projects: list[dict[str, Any]] = []
    live_status_count = 0

    for project in registry.get("projects", []):
        if not isinstance(project, dict):
            continue
        enriched = dict(project)
        project_status = load_project_status(project, status_dir)
        if project_status is not None:
            enriched["status"] = project_status
            live_status_count += 1
        projects.append(enriched)

    return {
        **registry,
        "projects": projects,
        "catalog": {
            "projectCount": len(projects),
            "liveStatusCount": live_status_count,
        },
    }


def crashable_workflow_id(owner_email: str, demo_name: str) -> str:
    return f"crashable-workspace/{owner_email}/{demo_name}"


def namespace_access_workflow_id(email: str) -> str:
    # Must match derive.namespace_read_access_workflow_id in the operator.
    return f"namespace-readers/grant/{email.strip().lower()}"


class TemporalUnavailable(Exception):
    """The Temporal client could not be reached for a crashable-workspace call."""


class WorkspaceNotFound(Exception):
    """No crashable-workspace workflow exists for this (user, demo)."""


class TemporalGateway:
    """Sync facade over the async Temporal client for the request handlers.

    ``ThreadingHTTPServer`` gives every request its own thread and there is no
    asyncio loop, so we run one background loop thread and marshal coroutines
    onto it. The client is connected lazily on that loop the first time an
    endpoint is used, so the catalog server still starts if Temporal is down.
    """

    def __init__(
        self,
        *,
        address: str,
        namespace: str,
        api_key: str | None,
        tls: bool,
        task_queue: str,
    ) -> None:
        self._address = address
        self._namespace = namespace
        self._api_key = api_key
        self._tls = tls
        self.task_queue = task_queue
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Any | None = None
        self._connect_lock: asyncio.Lock | None = None
        self._start_lock = threading.Lock()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            with self._start_lock:
                if self._loop is None:
                    loop = asyncio.new_event_loop()
                    thread = threading.Thread(
                        target=loop.run_forever,
                        name="temporal-gateway-loop",
                        daemon=True,
                    )
                    thread.start()
                    self._loop = loop
        return self._loop

    def _run(self, coro: Any, *, timeout: float = CRASHABLE_RPC_TIMEOUT_SECONDS) -> Any:
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        try:
            return future.result(timeout=timeout)
        except WorkspaceNotFound:
            # A clean "no workspace" signal, mapped to 404/None by the handlers.
            raise
        except Exception as error:  # noqa: BLE001 - surface as a clean 503
            # Everything else (RPC failures, timeouts, connection issues) becomes
            # a temporal-unavailable 503 rather than an unhandled 500. Timeouts
            # carry no message of their own, so name them.
            message = str(error) or f"{type(error).__name__} waiting on Temporal"
            raise TemporalUnavailable(message) from error

    async def _get_client(self) -> Any:
        # Runs on the loop thread; the None-checks are safe because there is no
        # await between check and assignment.
        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()
        async with self._connect_lock:
            if self._client is None:
                rpc_metadata = (
                    {"authorization": f"Bearer {self._api_key}"}
                    if self._api_key
                    else None
                )
                self._client = await Client.connect(
                    self._address,
                    namespace=self._namespace,
                    tls=self._tls,
                    rpc_metadata=rpc_metadata,
                )
        return self._client

    def ensure_workspace(self, owner_email: str, demo_name: str) -> dict[str, Any]:
        return self._run(self._ensure_workspace(owner_email, demo_name))

    async def _ensure_workspace(
        self, owner_email: str, demo_name: str
    ) -> dict[str, Any]:
        client = await self._get_client()
        operation = WithStartWorkflowOperation(
            CRASHABLE_WORKFLOW_TYPE,
            {
                "demo_name": demo_name,
                "owner_email": owner_email,
                "ttl_seconds": 3600,
            },
            id=crashable_workflow_id(owner_email, demo_name),
            task_queue=self.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return await client.execute_update_with_start_workflow(
            "ensure",
            start_workflow_operation=operation,
        )

    def update_workspace(
        self, owner_email: str, demo_name: str, update: str
    ) -> dict[str, Any]:
        return self._run(self._update_workspace(owner_email, demo_name, update))

    async def _update_workspace(
        self, owner_email: str, demo_name: str, update: str
    ) -> dict[str, Any]:
        client = await self._get_client()
        handle = client.get_workflow_handle(
            crashable_workflow_id(owner_email, demo_name)
        )
        try:
            return await handle.execute_update(update)
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise WorkspaceNotFound(demo_name) from error
            raise

    def workspace_status(self, owner_email: str, demo_name: str) -> dict[str, Any]:
        return self._run(self._workspace_status(owner_email, demo_name))

    def grant_namespace_access(self, email: str) -> dict[str, Any]:
        return self._run(
            self._grant_namespace_access(email),
            timeout=NAMESPACE_ACCESS_RPC_TIMEOUT_SECONDS,
        )

    async def _grant_namespace_access(self, email: str) -> dict[str, Any]:
        client = await self._get_client()
        # One short workflow per click. USE_EXISTING makes a double-click wait on
        # the run already in flight instead of starting a second one.
        result = await client.execute_workflow(
            NAMESPACE_ACCESS_WORKFLOW_TYPE,
            {"email": email},
            id=namespace_access_workflow_id(email),
            task_queue=self.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            execution_timeout=timedelta(minutes=3),
        )
        return result if isinstance(result, dict) else {"status": "unknown"}

    async def _workspace_status(
        self, owner_email: str, demo_name: str
    ) -> dict[str, Any]:
        client = await self._get_client()
        handle = client.get_workflow_handle(
            crashable_workflow_id(owner_email, demo_name)
        )
        try:
            description = await handle.describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return {"phase": "None", "demo": demo_name}
            raise
        if description.status == WorkflowExecutionStatus.RUNNING:
            return await handle.query("status")
        if description.status == WorkflowExecutionStatus.FAILED:
            return {
                "phase": "Failed",
                "demo": demo_name,
                "step": "Provisioning failed",
                "last_error": "Environment provisioning failed. Try again.",
            }
        # Completed (torn down), cancelled, terminated, or timed out -> gone.
        return {"phase": "None", "demo": demo_name}


def env_truthy(name: str, *, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def build_temporal_gateway() -> TemporalGateway | None:
    if not TEMPORALIO_AVAILABLE:
        return None
    address = os.environ.get("TEMPORAL_ADDRESS", "").strip()
    if not address:
        return None
    return TemporalGateway(
        address=address,
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default").strip() or "default",
        api_key=os.environ.get("TEMPORAL_API_KEY", "").strip() or None,
        tls=env_truthy("TEMPORAL_TLS"),
        task_queue=(
            os.environ.get("DEMO_REGISTRY_TASK_QUEUE", "").strip()
            or DEFAULT_CRASHABLE_TASK_QUEUE
        ),
    )


class RegistryRequestHandler(BaseHTTPRequestHandler):
    registry_path: Path
    status_dir: Path
    temporal_gateway: TemporalGateway | None = None
    server_version = "DemoRegistryWeb/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/health":
                self.send_json({"ok": True})
            elif path in {"/registry.json", "/api/registry"}:
                self.send_json(load_registry(self.registry_path))
            elif path == "/api/catalog":
                self.send_json(catalog_payload(self.registry_path, self.status_dir))
            elif path == "/api/crashable-workspace":
                self.crashable_status(parse_query(parsed.query))
            elif path in {"/_auth/verify", "/traefik/verify"}:
                self.send_auth_verification()
            elif path == "/auth/login":
                self.send_auth_login(parsed.query)
            elif path == "/oauth/google/start":
                self.start_google_oauth(parsed.query)
            elif path == "/oauth/google/callback":
                self.complete_google_oauth(parsed.query)
            elif path == "/auth/logout":
                self.logout(parsed.query)
            elif path in {"/api/auth/config", "/api/auth/google/configured"}:
                self.send_auth_config()
            elif path == "/catalog":
                self.send_redirect("/")
            elif path in {"/", "/index.html", "/courses"}:
                self.send_static("index.html")
            elif path.startswith("/static/"):
                self.send_static(path.removeprefix("/static/"))
            else:
                self.send_json(
                    {"error": "not_found", "path": path},
                    status=HTTPStatus.NOT_FOUND,
                )
        except (OSError, ValueError, json.JSONDecodeError) as error:
            self.send_json(
                {"error": "registry_unavailable", "message": str(error)},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/api/crashable-workspace":
                self.crashable_create()
            elif path == "/api/crashable-workspace/extend":
                self.crashable_update("extend")
            elif path == "/api/crashable-workspace/crash":
                self.crashable_update("crash")
            elif path == "/api/crashable-workspace/scale-down":
                self.crashable_update("scale_down")
            elif path == "/api/crashable-workspace/scale-up":
                self.crashable_update("scale_up")
            elif path == "/api/crashable-workspace/delete":
                self.crashable_delete()
            elif path == "/api/namespace-access":
                self.namespace_access_grant()
            else:
                self.send_json(
                    {"error": "not_found", "path": path},
                    status=HTTPStatus.NOT_FOUND,
                )
        except (OSError, ValueError, json.JSONDecodeError) as error:
            self.send_json(
                {"error": "bad_request", "message": str(error)},
                status=HTTPStatus.BAD_REQUEST,
            )

    def read_json_body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}

    def authenticated_email(self) -> str:
        # Mirror send_auth_verification: bootstrap-allow short-circuits to a
        # fixed identity; otherwise require a valid signed session cookie.
        if os.environ.get("AUTH_MODE", "disabled") == "bootstrap-allow":
            return "bootstrap@temporal.io"
        claims = session_from_request(self)
        email = claims.get("email")
        if not isinstance(email, str) or not email:
            raise AuthError("Session is missing an email claim.")
        return email

    def demo_is_crashable(self, demo: str) -> bool:
        registry = load_registry(self.registry_path)
        for project in registry.get("projects", []):
            if isinstance(project, dict) and project.get("name") == demo:
                crashable = project.get("crashable")
                return bool(isinstance(crashable, dict) and crashable.get("enabled"))
        return False

    def crashable_request(self) -> tuple[str, str] | None:
        # Shared preamble for the mutating endpoints: auth + gateway + demo name.
        # Returns (email, demo) or None after having already sent an error.
        try:
            email = self.authenticated_email()
        except AuthError as error:
            self.send_json(
                {"error": "unauthorized", "message": str(error)},
                status=HTTPStatus.UNAUTHORIZED,
            )
            return None
        if self.temporal_gateway is None:
            self.send_json(
                {"error": "temporal-unavailable"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return None
        demo = self.read_json_body().get("demo")
        if not isinstance(demo, str) or not demo:
            self.send_json(
                {"error": "bad_request", "message": "demo is required"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return None
        return email, demo

    def crashable_create(self) -> None:
        request = self.crashable_request()
        if request is None:
            return
        email, demo = request
        if not self.demo_is_crashable(demo):
            self.send_json(
                {
                    "error": "not-crashable",
                    "message": f"{demo} is not a crashable demo.",
                },
                status=HTTPStatus.FORBIDDEN,
            )
            return
        try:
            status = self.temporal_gateway.ensure_workspace(email, demo)
        except TemporalUnavailable as error:
            self.send_json(
                {"error": "temporal-unavailable", "message": str(error)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.send_json(status)

    def crashable_update(self, update: str) -> None:
        request = self.crashable_request()
        if request is None:
            return
        email, demo = request
        try:
            status = self.temporal_gateway.update_workspace(email, demo, update)
        except WorkspaceNotFound:
            self.send_json(
                {"error": "not-found", "message": "No active workspace."},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        except TemporalUnavailable as error:
            self.send_json(
                {"error": "temporal-unavailable", "message": str(error)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.send_json(status)

    def crashable_delete(self) -> None:
        request = self.crashable_request()
        if request is None:
            return
        email, demo = request
        try:
            status = self.temporal_gateway.update_workspace(email, demo, "delete")
        except WorkspaceNotFound:
            # Already gone is a successful delete.
            self.send_json({"phase": "None", "demo": demo})
            return
        except TemporalUnavailable as error:
            self.send_json(
                {"error": "temporal-unavailable", "message": str(error)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.send_json(status)

    def namespace_access_grant(self) -> None:
        # The email always comes from the signed session, never the request body,
        # so a user can only ever add themselves.
        try:
            email = self.authenticated_email()
        except AuthError as error:
            self.send_json(
                {"error": "unauthorized", "message": str(error)},
                status=HTTPStatus.UNAUTHORIZED,
            )
            return
        if self.temporal_gateway is None:
            self.send_json(
                {"error": "temporal-unavailable"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        try:
            result = self.temporal_gateway.grant_namespace_access(email)
        except TemporalUnavailable as error:
            self.send_json(
                {"error": "temporal-unavailable", "message": str(error)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.send_json(result)

    def crashable_status(self, params: dict[str, str]) -> None:
        try:
            email = self.authenticated_email()
        except AuthError as error:
            self.send_json(
                {"error": "unauthorized", "message": str(error)},
                status=HTTPStatus.UNAUTHORIZED,
            )
            return
        demo = params.get("demo", "")
        if not demo:
            self.send_json(
                {"error": "bad_request", "message": "demo is required"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if self.temporal_gateway is None:
            self.send_json(
                {"error": "temporal-unavailable"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        try:
            status = self.temporal_gateway.workspace_status(email, demo)
        except TemporalUnavailable as error:
            self.send_json(
                {"error": "temporal-unavailable", "message": str(error)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.send_json(status)

    def send_json(
        self,
        payload: dict[str, Any],
        *,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def send_auth_verification(self) -> None:
        auth_mode = os.environ.get("AUTH_MODE", "disabled")
        if auth_mode == "bootstrap-allow":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Temporal-Auth-JWT", "bootstrap-catalog-auth")
            self.send_header("X-Temporal-Auth-Subject", "bootstrap-catalog-auth")
            self.send_header("X-Temporal-Auth-Email", "bootstrap@temporal.io")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if auth_mode == "deny":
            self.send_json(
                {
                    "error": "unauthorized",
                    "message": "Temporal employee auth is required.",
                },
                status=HTTPStatus.UNAUTHORIZED,
            )
            return

        if auth_mode in {"google", "enabled"}:
            if not google_oauth_configured():
                self.send_json(
                    {
                        "error": "auth_not_configured",
                        "message": "Google OAuth credentials are not configured.",
                    },
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return

            try:
                session_claims = session_from_request(self)
            except AuthError:
                self.redirect_to_login()
                return

            now = int(time.time())
            host = forwarded_host(self)
            jwt = create_jwt(
                {
                    "iss": auth_issuer(),
                    "aud": host or "tmprl-demo.cloud",
                    "sub": session_claims["sub"],
                    "email": session_claims["email"],
                    "name": session_claims.get("name"),
                    "hd": session_claims.get("hd"),
                    "project_host": host,
                    "iat": now,
                    "exp": now + downstream_jwt_seconds(),
                }
            )
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Temporal-Auth-JWT", jwt)
            self.send_header("X-Temporal-Auth-Subject", str(session_claims["sub"]))
            self.send_header("X-Temporal-Auth-Email", str(session_claims["email"]))
            if session_claims.get("name"):
                self.send_header("X-Temporal-Auth-Name", str(session_claims["name"]))
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        self.send_json(
            {
                "error": "auth_not_configured",
                "message": "Set AUTH_MODE=google and configure the shared auth provider.",
            },
            status=HTTPStatus.SERVICE_UNAVAILABLE,
        )

    def redirect_to_login(self) -> None:
        return_to = sanitize_return_to(original_request_url(self))
        login_url = (
            f"{auth_base_url()}/auth/login?"
            f"{urlencode({'return_to': return_to})}"
        )
        self.send_redirect(login_url)

    def send_auth_login(self, query: str) -> None:
        params = parse_query(query)
        return_to = sanitize_return_to(params.get("return_to", ""))
        error = params.get("error", "")
        configured = google_oauth_configured()
        start_url = (
            "/oauth/google/start?"
            f"{urlencode({'return_to': return_to})}"
            if configured
            else "#"
        )
        aria_disabled = "" if configured else ' aria-disabled="true"'
        error_markup = (
            f'<p class="login-error">{escape(error)}</p>'
            if error
            else '<p class="login-error"></p>'
        )
        disabled_error = (
            '<p class="login-error">Authentication is not configured.</p>'
            if not configured
            else error_markup
        )
        body = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Log In</title>
    <link rel="stylesheet" href="/static/auth.css">
  </head>
  <body>
    <section class="login-screen">
      <div class="login-card">
        <h1 aria-label="Simple Chat Agent"></h1>
        <p class="login-subtitle"></p>
        <div class="login-form">
          <a class="login-button login-google" href="{escape(start_url)}"{aria_disabled}>Log In</a>
          {disabled_error}
        </div>
      </div>
    </section>
  </body>
</html>
"""
        self.send_html(body)

    def start_google_oauth(self, query: str) -> None:
        if not google_oauth_configured():
            self.send_json(
                {"error": "auth_not_configured"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        return_to = sanitize_return_to(parse_query(query).get("return_to", ""))
        nonce = secrets.token_urlsafe(24)
        state = create_state_token({"return_to": return_to, "nonce": nonce})
        params = {
            "client_id": google_client_id(),
            "redirect_uri": google_redirect_uri(),
            "response_type": "code",
            "scope": "openid email profile",
            "state": state,
            "access_type": "online",
            "prompt": "select_account",
        }
        allowed_domain = auth_allowed_domain()
        if allowed_domain:
            params["hd"] = allowed_domain

        self.send_response(HTTPStatus.FOUND)
        self.send_header(
            "Location",
            f"{GOOGLE_AUTHORIZE_URL}?{urlencode(params)}",
        )
        self.send_cookie(
            AUTH_STATE_COOKIE,
            nonce,
            max_age=10 * 60,
            domain=None,
            httponly=True,
            secure=auth_cookie_secure(),
            same_site="Lax",
        )
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def complete_google_oauth(self, query: str) -> None:
        params = parse_query(query)
        if params.get("error"):
            self.redirect_login_error(params.get("error_description") or params["error"])
            return

        code = params.get("code")
        state = params.get("state")
        if not code or not state:
            self.redirect_login_error("Missing Google OAuth callback.")
            return

        try:
            state_claims = decode_state_token(state)
            expected_nonce = str(state_claims["nonce"])
            actual_nonce = cookie_value(self, AUTH_STATE_COOKIE)
            if not hmac.compare_digest(actual_nonce or "", expected_nonce):
                raise AuthError("Invalid OAuth state.")
            token_payload = exchange_google_code(code, redirect_uri=google_redirect_uri())
            id_token = token_payload.get("id_token")
            if not isinstance(id_token, str):
                raise GoogleOAuthError("Google did not return an ID token.")
            identity = identity_from_id_token(id_token)
        except (AuthError, GoogleOAuthError) as error:
            self.redirect_login_error(str(error))
            return

        return_to = sanitize_return_to(str(state_claims.get("return_to") or ""))
        session_token = create_session_token(identity)
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", return_to)
        self.send_cookie(
            AUTH_SESSION_COOKIE,
            session_token,
            max_age=session_seconds(),
            domain=auth_cookie_domain(),
            httponly=True,
            secure=auth_cookie_secure(),
            same_site="Lax",
        )
        self.send_cookie(
            AUTH_STATE_COOKIE,
            "",
            max_age=0,
            domain=None,
            httponly=True,
            secure=auth_cookie_secure(),
            same_site="Lax",
        )
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def redirect_login_error(self, message: str) -> None:
        self.send_redirect(
            f"{auth_base_url()}/auth/login?"
            f"{urlencode({'error': message, 'return_to': auth_base_url()})}"
        )

    def logout(self, query: str) -> None:
        return_to = sanitize_return_to(parse_query(query).get("return_to", ""))
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", return_to)
        self.send_cookie(
            AUTH_SESSION_COOKIE,
            "",
            max_age=0,
            domain=auth_cookie_domain(),
            httponly=True,
            secure=auth_cookie_secure(),
            same_site="Lax",
        )
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def send_auth_config(self) -> None:
        configured = google_oauth_configured()
        self.send_json(
            {
                "mode": "google" if configured else "none",
                "configured": configured,
                "google_configured": configured,
                "allowed_domain": auth_allowed_domain(),
            }
        )

    def send_redirect(self, location: str) -> None:
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", location)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def send_html(self, body: str, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def send_cookie(
        self,
        name: str,
        value: str,
        *,
        max_age: int,
        domain: str | None,
        httponly: bool,
        secure: bool,
        same_site: str,
    ) -> None:
        cookie = SimpleCookie()
        cookie[name] = value
        cookie[name]["path"] = "/"
        cookie[name]["max-age"] = str(max_age)
        cookie[name]["samesite"] = same_site
        if domain:
            cookie[name]["domain"] = domain
        if httponly:
            cookie[name]["httponly"] = True
        if secure:
            cookie[name]["secure"] = True
        self.send_header("Set-Cookie", cookie.output(header="").strip())

    def send_static(self, relative_name: str) -> None:
        path = safe_static_path(relative_name)
        if path is None or not path.is_file():
            self.send_json(
                {"error": "not_found", "path": relative_name},
                status=HTTPStatus.NOT_FOUND,
            )
            return

        encoded = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "public, max-age=60")
        self.end_headers()
        self.wfile.write(encoded)


def safe_static_path(relative_name: str) -> Path | None:
    decoded = unquote(relative_name).lstrip("/")
    path = (STATIC_DIR / decoded).resolve()
    if path == STATIC_DIR or STATIC_DIR not in path.parents:
        return None
    return path


def parse_query(query: str) -> dict[str, str]:
    values = parse_qs(query, keep_blank_values=True)
    return {key: item[-1] for key, item in values.items() if item}


def env_first(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def google_client_id() -> str:
    return env_first("GOOGLE_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_ID")


def google_client_secret() -> str:
    return env_first("GOOGLE_CLIENT_SECRET", "GOOGLE_OAUTH_CLIENT_SECRET")


def google_oauth_configured() -> bool:
    return bool(google_client_id() and google_client_secret() and auth_signing_key())


def auth_signing_key() -> str:
    return env_first(
        "AUTH_COOKIE_SIGNING_KEY",
        "AUTH_JWT_SIGNING_KEY",
        "SIMPLE_CHAT_JWT_SECRET",
    )


def auth_base_url() -> str:
    return os.environ.get("AUTH_BASE_URL", DEFAULT_AUTH_BASE_URL).strip().rstrip("/")


def auth_issuer() -> str:
    return os.environ.get("AUTH_ISSUER", DEFAULT_AUTH_ISSUER).strip().rstrip("/")


def auth_allowed_domain() -> str | None:
    value = env_first("AUTH_ALLOWED_DOMAIN", "GOOGLE_OAUTH_ALLOWED_DOMAIN")
    if not value:
        value = DEFAULT_ALLOWED_DOMAIN
    return value.lower() or None


def auth_cookie_domain() -> str | None:
    value = os.environ.get("AUTH_COOKIE_DOMAIN", DEFAULT_COOKIE_DOMAIN).strip()
    return value or None


def auth_cookie_secure() -> bool:
    return os.environ.get("AUTH_COOKIE_SECURE", "true").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def session_seconds() -> int:
    return positive_int_env("AUTH_SESSION_SECONDS", DEFAULT_SESSION_SECONDS)


def downstream_jwt_seconds() -> int:
    return positive_int_env("AUTH_DOWNSTREAM_JWT_SECONDS", DEFAULT_DOWNSTREAM_JWT_SECONDS)


def positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, ""))
    except ValueError:
        return default
    return value if value > 0 else default


def google_redirect_uri() -> str:
    explicit = env_first("GOOGLE_REDIRECT_URI", "GOOGLE_OAUTH_REDIRECT_URI")
    if explicit:
        return explicit
    return f"{auth_base_url()}/oauth/google/callback"


def original_request_url(handler: BaseHTTPRequestHandler) -> str:
    proto = handler.headers.get("X-Forwarded-Proto", "https").split(",", 1)[0].strip()
    host = forwarded_host(handler)
    uri = handler.headers.get("X-Forwarded-Uri") or handler.headers.get(
        "X-Forwarded-Path"
    )
    if not uri:
        uri = "/"
    if not uri.startswith("/"):
        uri = f"/{uri}"
    if host:
        return f"{proto}://{host}{uri}"
    return auth_base_url()


def forwarded_host(handler: BaseHTTPRequestHandler) -> str:
    return (
        handler.headers.get("X-Forwarded-Host", "").split(",", 1)[0].strip()
        or handler.headers.get("X-Original-Host", "").strip()
    )


def sanitize_return_to(value: str) -> str:
    parsed = urlparse(value.strip() or auth_base_url())
    if parsed.scheme not in {"https", "http"}:
        return auth_base_url()
    if parsed.username or parsed.password:
        return auth_base_url()
    host = (parsed.hostname or "").lower()
    if parsed.scheme == "http" and host not in {"localhost", "127.0.0.1"}:
        return auth_base_url()
    if parsed.scheme == "https" and not (
        host == "tmprl-demo.cloud" or host.endswith(".tmprl-demo.cloud")
    ):
        return auth_base_url()
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{parsed.scheme}://{parsed.netloc}{path}{query}"


def cookie_value(handler: BaseHTTPRequestHandler, name: str) -> str | None:
    raw = handler.headers.get("Cookie")
    if not raw:
        return None
    cookie = SimpleCookie()
    try:
        cookie.load(raw)
    except Exception:
        return None
    morsel = cookie.get(name)
    return morsel.value if morsel else None


def session_from_request(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    token = cookie_value(handler, AUTH_SESSION_COOKIE)
    if not token:
        raise AuthError("Missing auth session.")
    return decode_jwt(token)


def create_session_token(identity: GoogleIdentity) -> str:
    now = int(time.time())
    subject = f"google:{identity.subject}"
    return create_jwt(
        {
            "iss": auth_issuer(),
            "aud": "tmprl-demo.cloud",
            "sub": subject,
            "email": identity.email,
            "name": identity.name,
            "hd": identity.hosted_domain,
            "iat": now,
            "exp": now + session_seconds(),
            "auth_time": now,
        }
    )


def create_state_token(payload: dict[str, Any]) -> str:
    now = int(time.time())
    return create_jwt(
        {
            **payload,
            "iss": auth_issuer(),
            "aud": "google-oauth-state",
            "sub": "oauth-state",
            "iat": now,
            "exp": now + 10 * 60,
        }
    )


def decode_state_token(token: str) -> dict[str, Any]:
    claims = decode_jwt(token)
    if claims.get("aud") != "google-oauth-state":
        raise AuthError("Invalid OAuth state.")
    if not isinstance(claims.get("nonce"), str):
        raise AuthError("Invalid OAuth state.")
    return claims


def create_jwt(payload: dict[str, Any]) -> str:
    if not auth_signing_key():
        raise AuthError("Auth signing key is not configured.")
    header = {"alg": "HS256", "typ": "JWT"}
    body = {key: value for key, value in payload.items() if value is not None}
    signing_input = ".".join([base64url_json(header), base64url_json(body)])
    signature = hmac.new(
        auth_signing_key().encode("utf-8"),
        signing_input.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{signing_input}.{base64url_encode(signature)}"


def decode_jwt(token: str) -> dict[str, Any]:
    if not auth_signing_key():
        raise AuthError("Auth signing key is not configured.")
    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("Invalid session token.")
    signing_input = f"{parts[0]}.{parts[1]}"
    expected = hmac.new(
        auth_signing_key().encode("utf-8"),
        signing_input.encode("ascii"),
        hashlib.sha256,
    ).digest()
    try:
        actual = base64url_decode(parts[2])
    except ValueError as error:
        raise AuthError("Invalid session signature.") from error
    if not hmac.compare_digest(actual, expected):
        raise AuthError("Invalid session signature.")
    try:
        header = json.loads(base64url_decode(parts[0]))
        payload = json.loads(base64url_decode(parts[1]))
    except (ValueError, json.JSONDecodeError) as error:
        raise AuthError("Invalid session token.") from error
    if header.get("alg") != "HS256":
        raise AuthError("Invalid session algorithm.")
    exp = payload.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        raise AuthError("Session token expired.")
    if payload.get("iss") != auth_issuer():
        raise AuthError("Invalid session issuer.")
    if not isinstance(payload.get("sub"), str):
        raise AuthError("Session token is missing subject.")
    return payload


def base64url_json(value: dict[str, Any]) -> str:
    return base64url_encode(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )


def base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def base64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def exchange_google_code(code: str, *, redirect_uri: str) -> dict[str, Any]:
    request = Request(
        GOOGLE_TOKEN_URL,
        data=urlencode(
            {
                "client_id": google_client_id(),
                "client_secret": google_client_secret(),
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
            }
        ).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise GoogleOAuthError(f"Google HTTP {error.code}: {details}") from error
    except URLError as error:
        raise GoogleOAuthError(f"Google request failed: {error.reason}") from error
    return json.loads(raw or "{}")


def identity_from_id_token(id_token: str) -> GoogleIdentity:
    claims = decode_id_token_claims(id_token)
    if claims.get("iss") not in GOOGLE_ISSUERS:
        raise GoogleOAuthError("ID token has unexpected issuer.")
    if claims.get("aud") != google_client_id():
        raise GoogleOAuthError("ID token audience does not match this client.")
    exp = claims.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        raise GoogleOAuthError("ID token is expired.")
    subject = claims.get("sub")
    email = claims.get("email")
    if not isinstance(subject, str) or not isinstance(email, str):
        raise GoogleOAuthError("ID token is missing identity claims.")
    if claims.get("email_verified") is not True:
        raise GoogleOAuthError("Google account email is not verified.")
    allowed_domain = auth_allowed_domain()
    hosted_domain = claims.get("hd") if isinstance(claims.get("hd"), str) else None
    if allowed_domain:
        email_domain = email.rsplit("@", 1)[-1].lower()
        if hosted_domain != allowed_domain and email_domain != allowed_domain:
            raise GoogleOAuthError(
                f"Only {allowed_domain} accounts are allowed to sign in."
            )
    name = claims.get("name") if isinstance(claims.get("name"), str) else None
    return GoogleIdentity(
        subject=subject,
        email=email,
        name=name,
        hosted_domain=hosted_domain,
    )


def decode_id_token_claims(id_token: str) -> dict[str, Any]:
    parts = id_token.split(".")
    if len(parts) != 3:
        raise GoogleOAuthError("ID token is malformed.")
    try:
        return json.loads(base64url_decode(parts[1]))
    except (ValueError, json.JSONDecodeError) as error:
        raise GoogleOAuthError("ID token payload could not be decoded.") from error


def make_handler(
    registry_path: Path,
    status_dir: Path,
    temporal_gateway: TemporalGateway | None = None,
) -> type[RegistryRequestHandler]:
    class ConfiguredRegistryRequestHandler(RegistryRequestHandler):
        pass

    ConfiguredRegistryRequestHandler.registry_path = registry_path
    ConfiguredRegistryRequestHandler.status_dir = status_dir
    ConfiguredRegistryRequestHandler.temporal_gateway = temporal_gateway
    return ConfiguredRegistryRequestHandler


def main() -> None:
    registry_path = configured_path("REGISTRY_DATA_PATH", DEFAULT_REGISTRY_PATH)
    status_dir = configured_path("CATALOG_STATUS_DIR", DEFAULT_STATUS_DIR)
    port = int(os.environ.get("PORT", "8080"))
    temporal_gateway = build_temporal_gateway()
    handler = make_handler(registry_path, status_dir, temporal_gateway)
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    crashable = "enabled" if temporal_gateway else "disabled (no TEMPORAL_ADDRESS)"
    print(f"Serving registry catalog on :{port} (crashable workspaces: {crashable})")
    server.serve_forever()


if __name__ == "__main__":
    main()
