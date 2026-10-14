from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import mimetypes
import os
import re
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
from urllib.parse import (
    parse_qs,
    parse_qsl,
    quote,
    unquote,
    urlencode,
    urlparse,
    urlsplit,
    urlunsplit,
)
from urllib.request import Request, urlopen

# Temporal is only needed for the crashable-workspace endpoints. Import it
# defensively so the catalog server still boots in environments where the
# temporalio wheel is absent (the endpoints then report temporal-unavailable).
try:
    from temporalio.client import (
        Client,
        WithStartWorkflowOperation,
        WorkflowExecutionStatus,
        WorkflowUpdateFailedError,
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
# Every token this server signs carries a "typ" claim, and every decoder requires
# its own typ. All typs share one signing key, so without this a downstream JWT
# (handed to demo backends in X-Temporal-Auth-JWT) would replay as a session.
TOKEN_TYPE_SESSION = "session"
TOKEN_TYPE_DOWNSTREAM = "downstream"
TOKEN_TYPE_OAUTH_STATE = "oauth-state"
SESSION_AUDIENCE = "tmprl-demo.cloud"
# Workspace share links are signed with a key derived from (never equal to) the
# session key, and carry their own typs, so neither key nor typ overlaps with an
# employee session. Only /_auth/verify-workspace (wired onto crashable/isolated
# workspace routes alone) ever reads them.
TOKEN_TYPE_SHARE_LINK = "workspace-share-link"
TOKEN_TYPE_SHARE_SESSION = "workspace-share-session"
SHARE_KEY_CONTEXT = b"workspace-share-v1"
WORKSPACE_SHARE_COOKIE = "temporal_workspace_share"
WORKSPACE_SHARE_PARAM = "tmprl_share"
WORKSPACE_HOST_PREFIX = "crash-"
# Guests get a synthetic, per-browser email on a domain we own (no MX), so demos
# that expect X-Temporal-Auth-Email keep working and never see a real address.
GUEST_EMAIL_DOMAIN = "guest.tmprl-demo.cloud"
WORKSPACE_LIVE_PHASES = frozenset(
    {"Active", "Crashing", "ScalingDown", "ScaledDown", "ScalingUp"}
)
DEFAULT_SHARE_SECONDS = 4 * 60 * 60
MIN_SHARE_SECONDS = 5 * 60
MAX_SHARE_SECONDS = 24 * 60 * 60
SHARE_STATUS_CACHE_SECONDS = 30
GOOGLE_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_ISSUERS = {"https://accounts.google.com", "accounts.google.com"}

CRASHABLE_WORKFLOW_TYPE = "CrashableWorkspaceWorkflow"
ISOLATED_LAMBDA_WORKFLOW_TYPE = "IsolatedLambdaWorkspaceWorkflow"
WORKER_DISRUPTION_UPDATES = frozenset({"crash", "scale_down", "scale_up"})
DEFAULT_CRASHABLE_TASK_QUEUE = "registry-management"
# Bound how long a sync request thread will block on a Temporal RPC.
CRASHABLE_RPC_TIMEOUT_SECONDS = 30

# Self-service "grant yourself access": the operator adds the signed-in user to
# the Temporal Cloud user group that can read every demo namespace.
NAMESPACE_ACCESS_WORKFLOW_TYPE = "GrantNamespaceReadAccessWorkflow"
# Usually a few seconds; the very first grant may also create the group and
# backfill its namespace grants.
NAMESPACE_ACCESS_RPC_TIMEOUT_SECONDS = 90

# Catalog onboarding: one CatalogOnboardingWorkflow per signed-in user takes a
# demo repo to a catalog pull request. Edits run validation and GitHub checks
# as activities, so allow more than a plain RPC.
ONBOARDING_WORKFLOW_TYPE = "CatalogOnboardingWorkflow"
ONBOARDING_RPC_TIMEOUT_SECONDS = 90
# A DemoProject is a few KB; anything near Temporal's payload limit is a mistake.
ONBOARDING_MAX_BODY_BYTES = 256 * 1024
ONBOARDING_UPDATES = {
    "repo": "set_repo",
    "draft": "update_draft",
    "submit": "submit_pull_request",
    "abandon": "abandon",
    "agent": "request_agent",
    "agent/cancel": "cancel_agent",
    "agent/new": "new_agent_conversation",
    "preview": "deploy_preview",
}
ONBOARDING_AGENT_ACTIVITY = "run_onboarding_agent"
# Project secrets a user enters while onboarding go straight to Secrets Manager
# at <prefix>/<project>/<secret>; Temporal only ever sees their names. The web's
# IAM role (infra/irsa.tf, registry_web) only creates secrets with the
# managed-by tag and only updates secrets that carry it.
SECRET_PREFIX = "tmprl-dem-cld"
SHARED_SECRET_PREFIX = f"{SECRET_PREFIX}/platform/shared/"
SECRET_MANAGED_BY_TAG = "registry.tmprl-demo.cloud/managed-by"
SECRET_MANAGED_BY = "catalog-onboarding"
SECRET_OWNER_TAG = "registry.tmprl-demo.cloud/owner"
SECRET_PROJECT_TAG = "registry.tmprl-demo.cloud/project"
SECRET_ONBOARDING_TAG = "registry.tmprl-demo.cloud/onboarding"
SECRET_DESCRIPTION_PREFIX = "Catalog onboarding secret. Properties: "
SECRET_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
SECRET_PROPERTY_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")
MAX_SECRET_PROPERTIES = 20
MAX_SECRET_VALUE_CHARS = 8192
ONBOARDING_AGENT_MODES = frozenset({"draft", "section", "chat"})


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


def onboarding_preview_workflow_id(email: str, cluster: str | None) -> str:
    # Must match derive.onboarding_preview_workflow_id in the operator. Keyed by
    # user, not onboarding: a test deploy outlives the onboarding that started it.
    email = email.strip().lower()
    if cluster:
        return f"onboarding-preview/{cluster}/{email}"
    return f"onboarding-preview/{email}"


def onboarding_workflow_id(email: str, cluster: str | None) -> str:
    # Must match derive.catalog_onboarding_workflow_id in the operator.
    email = email.strip().lower()
    if cluster:
        return f"catalog-onboarding/{cluster}/{email}"
    return f"catalog-onboarding/{email}"


class TemporalUnavailable(Exception):
    """The Temporal client could not be reached for a crashable-workspace call."""


class WorkspaceNotFound(Exception):
    """No crashable-workspace workflow exists for this (user, demo)."""


class OnboardingRejected(Exception):
    """The onboarding workflow refused an edit; the message is for the user."""


class SecretsUnavailable(Exception):
    """Secrets Manager can't be reached or the web has no role for it."""


class OnboardingSecrets:
    """Project secrets for onboarding drafts, in AWS Secrets Manager.

    Reads metadata only (ListSecrets, DescribeSecret); the only values this ever
    handles are the ones a user is writing. Property names are kept in the
    secret's description, so listing never needs the value.
    """

    def __init__(self, client_factory: Any | None = None) -> None:
        self._client_factory = client_factory or default_secrets_client
        self._client: Any | None = None
        self._lock = threading.Lock()

    def _secretsmanager(self) -> Any:
        with self._lock:
            if self._client is None:
                self._client = self._client_factory()
            return self._client

    def _call(self, method: str, **kwargs: Any) -> Any:
        try:
            return getattr(self._secretsmanager(), method)(**kwargs)
        except Exception as error:  # noqa: BLE001 - mapped to a clean 503 / 404
            if aws_error_code(error) == "ResourceNotFoundException":
                raise KeyError(kwargs.get("SecretId")) from error
            raise SecretsUnavailable(
                f"Secrets Manager {method} failed: {aws_error_code(error) or type(error).__name__}"
            ) from error

    def _list(self, prefix: str) -> list[dict[str, Any]]:
        secrets: list[dict[str, Any]] = []
        token = None
        while True:
            kwargs: dict[str, Any] = {"Filters": [{"Key": "name", "Values": [prefix]}]}
            if token:
                kwargs["NextToken"] = token
            response = self._call("list_secrets", **kwargs)
            secrets.extend(
                item
                for item in response.get("SecretList", [])
                if str(item.get("Name", "")).startswith(prefix)
            )
            token = response.get("NextToken")
            if not token:
                return secrets

    def list_project(self, project: str) -> list[dict[str, Any]]:
        prefix = f"{SECRET_PREFIX}/{project}/"
        result = []
        for item in self._list(prefix):
            tags = secret_tags(item)
            changed = item.get("LastChangedDate")
            result.append(
                {
                    "name": item["Name"][len(prefix) :],
                    "properties": description_properties(item.get("Description")),
                    "owner": tags.get(SECRET_OWNER_TAG),
                    "onboarding": tags.get(SECRET_MANAGED_BY_TAG) == SECRET_MANAGED_BY,
                    "updated_at": changed.isoformat() if hasattr(changed, "isoformat") else None,
                }
            )
        return sorted(result, key=lambda item: item["name"])

    def list_shared(self) -> list[dict[str, Any]]:
        result = []
        for item in self._list(SHARED_SECRET_PREFIX):
            properties = secret_tags(item).get("properties", "")
            result.append(
                {
                    "name": item["Name"][len(SHARED_SECRET_PREFIX) :],
                    "properties": properties.split(),
                    "description": item.get("Description") or "",
                }
            )
        return sorted(result, key=lambda item: item["name"])

    def store(
        self,
        project: str,
        name: str,
        values: dict[str, str],
        *,
        owner: str,
        onboarding_id: str,
    ) -> dict[str, Any]:
        path = f"{SECRET_PREFIX}/{project}/{name}"
        description = SECRET_DESCRIPTION_PREFIX + " ".join(sorted(values))
        body = json.dumps(values)
        try:
            existing = self._call("describe_secret", SecretId=path)
        except KeyError:
            self._call(
                "create_secret",
                Name=path,
                SecretString=body,
                Description=description,
                Tags=[
                    {"Key": SECRET_MANAGED_BY_TAG, "Value": SECRET_MANAGED_BY},
                    {"Key": SECRET_OWNER_TAG, "Value": owner},
                    {"Key": SECRET_PROJECT_TAG, "Value": project},
                    {"Key": SECRET_ONBOARDING_TAG, "Value": onboarding_id[:256]},
                ],
            )
            return {"name": name, "properties": sorted(values), "created": True}
        tags = secret_tags(existing)
        if tags.get(SECRET_MANAGED_BY_TAG) != SECRET_MANAGED_BY:
            raise OnboardingRejected(
                f"{path} already exists and wasn't created by onboarding. Pick "
                "another name, or ask a catalog owner to change it."
            )
        if tags.get(SECRET_OWNER_TAG) != owner:
            raise OnboardingRejected(f"{path} belongs to {tags.get(SECRET_OWNER_TAG)}.")
        self._call("put_secret_value", SecretId=path, SecretString=body)
        self._call("update_secret", SecretId=path, Description=description)
        return {"name": name, "properties": sorted(values), "created": False}


def default_secrets_client() -> Any:
    try:
        import boto3
    except ImportError as error:
        raise SecretsUnavailable("boto3 is not installed") from error
    region = os.environ.get("DEMO_REGISTRY_AWS_REGION", "").strip() or "us-west-1"
    return boto3.session.Session(region_name=region).client("secretsmanager")


def aws_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code")
        return code if isinstance(code, str) else None
    return None


def secret_tags(item: dict[str, Any]) -> dict[str, str]:
    return {
        str(tag.get("Key")): str(tag.get("Value"))
        for tag in item.get("Tags") or []
        if isinstance(tag, dict)
    }


def description_properties(description: Any) -> list[str]:
    if isinstance(description, str) and description.startswith(SECRET_DESCRIPTION_PREFIX):
        return description[len(SECRET_DESCRIPTION_PREFIX) :].split()
    return []


def secret_write_problem(name: Any, values: Any) -> str | None:
    """Why a secret write is malformed, or None. Messages never include values."""

    if not isinstance(name, str) or not SECRET_NAME_PATTERN.match(name) or len(name) > 63:
        return "The secret name must be a DNS label, like anthropic-credentials."
    if not isinstance(values, dict) or not values:
        return "Add at least one property."
    if len(values) > MAX_SECRET_PROPERTIES:
        return f"A secret can have at most {MAX_SECRET_PROPERTIES} properties."
    for key, value in values.items():
        if not isinstance(key, str) or not SECRET_PROPERTY_PATTERN.match(key):
            return f"{key!s} isn't an environment variable name (like API_KEY)."
        if not isinstance(value, str) or not value:
            return f"{key} needs a value."
        if len(value) > MAX_SECRET_VALUE_CHARS:
            return f"{key} is longer than {MAX_SECRET_VALUE_CHARS} characters."
    return None


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
        cluster: str | None = None,
    ) -> None:
        self._address = address
        self._namespace = namespace
        self._api_key = api_key
        self._tls = tls
        self.task_queue = task_queue
        self.cluster = cluster
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
        except (WorkspaceNotFound, OnboardingRejected):
            # Clean signals the handlers map to 404s and 4xx responses.
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

    def ensure_workspace(
        self,
        owner_email: str,
        demo_name: str,
        workflow_type: str,
    ) -> dict[str, Any]:
        return self._run(
            self._ensure_workspace(owner_email, demo_name, workflow_type)
        )

    async def _ensure_workspace(
        self, owner_email: str, demo_name: str, workflow_type: str
    ) -> dict[str, Any]:
        client = await self._get_client()
        operation = WithStartWorkflowOperation(
            workflow_type,
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

    def onboarding_status(self, owner_email: str) -> dict[str, Any]:
        return self._run(self._onboarding_status(owner_email))

    async def _onboarding_status(self, owner_email: str) -> dict[str, Any]:
        client = await self._get_client()
        handle = client.get_workflow_handle(
            onboarding_workflow_id(owner_email, self.cluster)
        )
        try:
            description = await handle.describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return {"phase": "None"}
            raise
        if description.status == WorkflowExecutionStatus.RUNNING:
            status = await handle.query("status")
            agent = status.get("agent") if isinstance(status, dict) else None
            if isinstance(agent, dict) and agent.get("phase") == "Running":
                agent["progress"] = await onboarding_agent_progress(
                    client, description
                )
            return status
        if description.status == WorkflowExecutionStatus.COMPLETED:
            # Hand-off, abandon, and expiry all complete with the final status,
            # so the page can show how the last onboarding ended.
            result = await handle.result()
            if isinstance(result, dict):
                return result
        return {"phase": "None"}

    def onboarding_start(self, owner_email: str) -> dict[str, Any]:
        return self._run(
            self._onboarding_start(owner_email),
            timeout=ONBOARDING_RPC_TIMEOUT_SECONDS,
        )

    async def _onboarding_start(self, owner_email: str) -> dict[str, Any]:
        client = await self._get_client()
        # USE_EXISTING returns the open onboarding; after one completes, the same
        # id starts a fresh run.
        operation = WithStartWorkflowOperation(
            ONBOARDING_WORKFLOW_TYPE,
            {"owner_email": owner_email},
            id=onboarding_workflow_id(owner_email, self.cluster),
            task_queue=self.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return await client.execute_update_with_start_workflow(
            "ensure",
            start_workflow_operation=operation,
        )

    def onboarding_update(
        self, owner_email: str, update: str, arg: dict[str, Any] | None
    ) -> dict[str, Any]:
        return self._run(
            self._onboarding_update(owner_email, update, arg),
            timeout=ONBOARDING_RPC_TIMEOUT_SECONDS,
        )

    async def _onboarding_update(
        self, owner_email: str, update: str, arg: dict[str, Any] | None
    ) -> dict[str, Any]:
        client = await self._get_client()
        handle = client.get_workflow_handle(
            onboarding_workflow_id(owner_email, self.cluster)
        )
        try:
            if arg is None:
                return await handle.execute_update(update)
            return await handle.execute_update(update, arg)
        except WorkflowUpdateFailedError as error:
            cause = error.cause
            message = getattr(cause, "message", None) or str(cause or error)
            raise OnboardingRejected(message) from error
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise OnboardingRejected(
                    "No onboarding in progress. Start a new one."
                ) from error
            raise

    def preview_status(self, owner_email: str) -> dict[str, Any]:
        return self._run(self._preview_status(owner_email))

    async def _preview_status(self, owner_email: str) -> dict[str, Any]:
        client = await self._get_client()
        handle = client.get_workflow_handle(
            onboarding_preview_workflow_id(owner_email, self.cluster)
        )
        try:
            description = await handle.describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                return {"phase": "None"}
            raise
        if description.status == WorkflowExecutionStatus.RUNNING:
            return await handle.query("status")
        if description.status == WorkflowExecutionStatus.COMPLETED:
            result = await handle.result()
            if isinstance(result, dict):
                return result
        return {"phase": "None"}

    def preview_end(self, owner_email: str) -> dict[str, Any]:
        return self._run(
            self._preview_end(owner_email),
            timeout=ONBOARDING_RPC_TIMEOUT_SECONDS,
        )

    async def _preview_end(self, owner_email: str) -> dict[str, Any]:
        client = await self._get_client()
        handle = client.get_workflow_handle(
            onboarding_preview_workflow_id(owner_email, self.cluster)
        )
        try:
            return await handle.execute_update("end")
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise OnboardingRejected("You have no test deploy running.") from error
            raise

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
            try:
                status = await handle.query("status", rpc_timeout=timedelta(seconds=5))
                return {**status, "phase": "Failed"}
            except RPCError:
                # Closed executions may no longer have a worker available to query.
                pass
            return {
                "phase": "Failed",
                "demo": demo_name,
                "step": "Provisioning failed",
                "last_error": "Environment provisioning failed. Try again.",
            }
        # Completed (torn down), cancelled, terminated, or timed out -> gone.
        return {"phase": "None", "demo": demo_name}


async def onboarding_agent_progress(client: Any, description: Any) -> str | None:
    """The agent's latest step ("Reading worker/main.go"), from the heartbeat
    details of its running activity."""

    raw = getattr(description, "raw_description", None)
    for pending in getattr(raw, "pending_activities", None) or []:
        if pending.activity_type.name != ONBOARDING_AGENT_ACTIVITY:
            continue
        payloads = list(pending.heartbeat_details.payloads)
        if not payloads:
            return None
        details = await client.data_converter.decode(payloads)
        latest = details[-1] if details else None
        if isinstance(latest, dict) and isinstance(latest.get("progress"), str):
            return latest["progress"]
    return None


def onboarding_update_arg(action: str, body: dict[str, Any]) -> dict[str, Any] | str | None:
    """The update argument for an onboarding action, or a message for a 400.

    Only known fields are forwarded; the owner is never taken from the body.
    """

    if action == "repo":
        repo_url = body.get("repo_url")
        if not isinstance(repo_url, str) or not repo_url.strip():
            return "repo_url is required"
        return {"repo_url": repo_url.strip()}
    if action == "draft":
        revision = body.get("base_revision")
        document = body.get("document")
        text = body.get("yaml")
        if not isinstance(revision, int) or isinstance(revision, bool):
            return "base_revision is required"
        if isinstance(document, dict) and text is None:
            return {"base_revision": revision, "document": document}
        if isinstance(text, str) and document is None:
            return {"base_revision": revision, "yaml": text}
        return "send either document or yaml"
    if action == "submit":
        url = body.get("pull_request_url")
        if not isinstance(url, str) or not url.strip():
            return "pull_request_url is required"
        return {"pull_request_url": url.strip()}
    if action == "agent":
        mode = body.get("mode")
        if mode not in ONBOARDING_AGENT_MODES:
            return "mode must be draft, section, or chat"
        arg: dict[str, Any] = {"mode": mode}
        for key in ("pointer", "question"):
            value = body.get(key)
            if isinstance(value, str) and value.strip():
                arg[key] = value.strip()
        return arg
    return None


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
        cluster=os.environ.get("DEMO_REGISTRY_CLUSTER", "").strip() or None,
    )


class RegistryRequestHandler(BaseHTTPRequestHandler):
    registry_path: Path
    status_dir: Path
    temporal_gateway: TemporalGateway | None = None
    onboarding_secrets: OnboardingSecrets | None = None
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
            elif path == "/api/onboarding":
                self.onboarding_status()
            elif path == "/api/onboarding/secrets":
                self.onboarding_secrets_list()
            elif path == "/api/onboarding/preview":
                self.onboarding_preview_status()
            elif path in {"/_auth/verify", "/traefik/verify"}:
                self.send_auth_verification()
            elif path == "/_auth/verify-workspace":
                self.send_workspace_auth_verification()
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
            elif path in {"/", "/index.html", "/courses", "/onboard"}:
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
            elif path == "/api/crashable-workspace/share":
                self.workspace_share_create()
            elif path == "/api/crashable-workspace/revoke-share-links":
                self.workspace_share_revoke()
            elif path == "/api/namespace-access":
                self.namespace_access_grant()
            elif path == "/api/onboarding":
                self.onboarding_start()
            elif path == "/api/onboarding/secrets":
                self.onboarding_secret_store()
            elif path == "/api/onboarding/preview/end":
                self.onboarding_preview_end()
            elif path.startswith("/api/onboarding/"):
                self.onboarding_update(path.removeprefix("/api/onboarding/"))
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

    def catalog_project(self, demo: str) -> dict[str, Any] | None:
        registry = load_registry(self.registry_path)
        for project in registry.get("projects", []):
            if isinstance(project, dict) and project.get("name") == demo:
                return project
        return None

    def demo_workspace_mode(self, demo: str) -> str | None:
        """Which private-environment workflow this catalog demo starts.

        ``isolatedLambda.enabled`` and ``crashable.enabled`` in the catalog JSON
        are already the two-field gates (enabled and a non-empty taskQueueEnv).
        Isolated wins if a catalog entry ever carries both, so the API does not
        offer crash or scale for a Lambda workspace.
        """

        registry = load_registry(self.registry_path)
        for project in registry.get("projects", []):
            if not isinstance(project, dict) or project.get("name") != demo:
                continue
            isolated = project.get("isolatedLambda")
            if isinstance(isolated, dict) and isolated.get("enabled"):
                return "isolated-lambda"
            crashable = project.get("crashable")
            if isinstance(crashable, dict) and crashable.get("enabled"):
                return "crashable"
        return None

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
        self.request_body = self.read_json_body()
        demo = self.request_body.get("demo")
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
        mode = self.demo_workspace_mode(demo)
        if mode is None:
            self.send_json(
                {
                    "error": "not-crashable",
                    "message": f"{demo} is not a crashable demo.",
                },
                status=HTTPStatus.FORBIDDEN,
            )
            return
        workflow_type = (
            ISOLATED_LAMBDA_WORKFLOW_TYPE
            if mode == "isolated-lambda"
            else CRASHABLE_WORKFLOW_TYPE
        )
        try:
            status = self.temporal_gateway.ensure_workspace(
                email, demo, workflow_type
            )
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
        if (
            update in WORKER_DISRUPTION_UPDATES
            and self.demo_workspace_mode(demo) == "isolated-lambda"
        ):
            self.send_json(
                {
                    "error": "worker-disruption-unsupported",
                    "message": (
                        "Isolated Lambda environments do not support crashing "
                        "or scaling workers."
                    ),
                },
                status=HTTPStatus.CONFLICT,
            )
            return
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

    def workspace_share_create(self) -> None:
        if not same_origin_request(self):
            self.send_json(
                {"error": "forbidden", "message": "Cross-origin request refused."},
                status=HTTPStatus.FORBIDDEN,
            )
            return
        request = self.crashable_request()
        if request is None:
            return
        email, demo = request
        project = self.catalog_project(demo)
        if (
            project is None
            or self.demo_workspace_mode(demo) is None
            or not project.get("authRequired")
        ):
            self.send_json(
                {
                    "error": "not-shareable",
                    "message": f"{demo} does not have a shareable environment.",
                },
                status=HTTPStatus.FORBIDDEN,
            )
            return
        try:
            ttl_seconds = int(self.request_body.get("ttl_seconds", DEFAULT_SHARE_SECONDS))
        except (TypeError, ValueError):
            ttl_seconds = DEFAULT_SHARE_SECONDS
        ttl_seconds = min(max(ttl_seconds, MIN_SHARE_SECONDS), MAX_SHARE_SECONDS)
        try:
            status = self.temporal_gateway.workspace_status(email, demo)
        except TemporalUnavailable as error:
            self.send_json(
                {"error": "temporal-unavailable", "message": str(error)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        host = status.get("host")
        if (
            status.get("phase") not in WORKSPACE_LIVE_PHASES
            or not isinstance(host, str)
            or not host.startswith(WORKSPACE_HOST_PREFIX)
            or not isinstance(status.get("created_at"), str)
        ):
            self.send_json(
                {"error": "not-running", "message": "The environment is not running."},
                status=HTTPStatus.CONFLICT,
            )
            return
        if "share_epoch" not in status:
            # Created before share links existed: its routes still use the
            # employee-only verifier, so a link would never work.
            self.send_json(
                {
                    "error": "not-shareable",
                    "message": (
                        "This environment predates share links. Delete and "
                        "recreate it to share it."
                    ),
                },
                status=HTTPStatus.CONFLICT,
            )
            return
        now = int(time.time())
        jti = secrets.token_urlsafe(12)
        token = create_jwt(
            {
                "iss": auth_issuer(),
                "aud": host,
                "sub": f"share:{jti}",
                "jti": jti,
                "ws": status.get("workspace_id"),
                "demo": demo,
                "owner": email,
                "gen": status["created_at"],
                "epoch": int(status.get("share_epoch") or 0),
                "iat": now,
                "exp": now + ttl_seconds,
            },
            typ=TOKEN_TYPE_SHARE_LINK,
            key=share_signing_key(),
        )
        self.send_json(
            {
                "url": f"https://{host}/?{urlencode({WORKSPACE_SHARE_PARAM: token})}",
                "expires_at": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + ttl_seconds)
                ),
            }
        )

    def workspace_share_revoke(self) -> None:
        if not same_origin_request(self):
            self.send_json(
                {"error": "forbidden", "message": "Cross-origin request refused."},
                status=HTTPStatus.FORBIDDEN,
            )
            return
        request = self.crashable_request()
        if request is None:
            return
        email, demo = request
        try:
            status = self.temporal_gateway.update_workspace(
                email, demo, "revoke_share_links"
            )
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
        SHARE_STATUS_CACHE.invalidate(email, demo)
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

    def onboarding_request(self, *, mutating: bool) -> str | None:
        # Shared preamble: auth, CSRF for writes, and the gateway. Returns the
        # session email, or None after having already sent an error. The email
        # always comes from the session, so a user only ever edits their own.
        if mutating and not same_origin_request(self):
            self.send_json(
                {"error": "forbidden", "message": "Cross-origin request refused."},
                status=HTTPStatus.FORBIDDEN,
            )
            return None
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
        return email

    def onboarding_status(self) -> None:
        email = self.onboarding_request(mutating=False)
        if email is None:
            return
        self.send_onboarding_result(
            lambda: self.temporal_gateway.onboarding_status(email)
        )

    def onboarding_start(self) -> None:
        email = self.onboarding_request(mutating=True)
        if email is None:
            return
        self.send_onboarding_result(
            lambda: self.temporal_gateway.onboarding_start(email)
        )

    def onboarding_preview_status(self) -> None:
        email = self.onboarding_request(mutating=False)
        if email is None:
            return
        self.send_onboarding_result(
            lambda: self.temporal_gateway.preview_status(email)
        )

    def onboarding_preview_end(self) -> None:
        email = self.onboarding_request(mutating=True)
        if email is None:
            return
        self.send_onboarding_result(lambda: self.temporal_gateway.preview_end(email))

    def onboarding_update(self, action: str) -> None:
        update = ONBOARDING_UPDATES.get(action)
        if update is None:
            self.send_json(
                {"error": "not_found", "path": self.path},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        email = self.onboarding_request(mutating=True)
        if email is None:
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length > ONBOARDING_MAX_BODY_BYTES:
            self.send_json(
                {"error": "too_large", "message": "The draft is too large."},
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return
        body = self.read_json_body()
        arg = onboarding_update_arg(action, body)
        if isinstance(arg, str):
            self.send_json(
                {"error": "bad_request", "message": arg},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        self.send_onboarding_result(
            lambda: self.temporal_gateway.onboarding_update(email, update, arg)
        )

    def onboarding_project(self, email: str) -> tuple[str | None, dict[str, Any]]:
        """The project name of the user's open onboarding draft, if it has one."""

        status = self.temporal_gateway.onboarding_status(email)
        if status.get("phase") in {"None", "HandedOff", "Abandoned", "Expired"}:
            return None, status
        metadata = (status.get("document") or {}).get("metadata") or {}
        name = metadata.get("name")
        if isinstance(name, str) and SECRET_NAME_PATTERN.match(name):
            return name, status
        return None, status

    def onboarding_secrets_list(self) -> None:
        email = self.onboarding_request(mutating=False)
        if email is None:
            return
        if self.onboarding_secrets is None:
            self.send_json({"error": "secrets-unavailable"}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        try:
            project, _status = self.onboarding_project(email)
            payload = {
                "project": project,
                "secrets": self.onboarding_secrets.list_project(project) if project else [],
                "shared": self.onboarding_secrets.list_shared(),
            }
        except TemporalUnavailable as error:
            self.send_json({"error": "temporal-unavailable", "message": str(error)}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        except SecretsUnavailable as error:
            self.send_json({"error": "secrets-unavailable", "message": str(error)}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        self.send_json(payload)

    def onboarding_secret_store(self) -> None:
        email = self.onboarding_request(mutating=True)
        if email is None:
            return
        if self.onboarding_secrets is None:
            self.send_json({"error": "secrets-unavailable"}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        body = self.read_json_body()
        name, values = body.get("name"), body.get("values")
        problem = secret_write_problem(name, values)
        if problem:
            self.send_json({"error": "bad_request", "message": problem}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            project, _status = self.onboarding_project(email)
            if project is None:
                raise OnboardingRejected("Pick a repo and a valid project name first.")
            if self.catalog_project(project) is not None:
                raise OnboardingRejected(
                    f"{project} is already in the catalog; its secrets are managed there."
                )
            result = self.onboarding_secrets.store(
                project,
                name,
                values,
                owner=email,
                onboarding_id=onboarding_workflow_id(email, self.temporal_gateway.cluster),
            )
        except OnboardingRejected as error:
            self.send_json({"error": "rejected", "message": str(error)}, status=HTTPStatus.CONFLICT)
            return
        except TemporalUnavailable as error:
            self.send_json({"error": "temporal-unavailable", "message": str(error)}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        except SecretsUnavailable as error:
            self.send_json({"error": "secrets-unavailable", "message": str(error)}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        self.send_json({**result, "project": project})

    def send_onboarding_result(self, call: Any) -> None:
        try:
            result = call()
        except OnboardingRejected as error:
            self.send_json(
                {"error": "rejected", "message": str(error)},
                status=HTTPStatus.CONFLICT,
            )
            return
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
                    "aud": host or SESSION_AUDIENCE,
                    "sub": session_claims["sub"],
                    "email": session_claims["email"],
                    "name": session_claims.get("name"),
                    "hd": session_claims.get("hd"),
                    "project_host": host,
                    "iat": now,
                    "exp": now + downstream_jwt_seconds(),
                },
                typ=TOKEN_TYPE_DOWNSTREAM,
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

    def has_employee_session(self) -> bool:
        try:
            session_from_request(self)
        except AuthError:
            return False
        return True

    def send_workspace_auth_verification(self) -> None:
        # Forward-auth for crashable / isolated workspace routes ONLY. Employees
        # pass exactly as on /_auth/verify; anyone else needs a share link or
        # share cookie minted for this exact workspace host and incarnation.
        if os.environ.get("AUTH_MODE", "disabled") not in {"google", "enabled"}:
            self.send_auth_verification()
            return
        host = forwarded_host(self)
        url = original_request_url(self)
        link = share_param(url)
        if link is not None:
            self.exchange_share_link(host, url, link)
            return
        if self.has_employee_session():
            self.send_auth_verification()
            return
        share_cookie = cookie_value(self, WORKSPACE_SHARE_COOKIE)
        claims = self.verified_share_claims(
            share_cookie,
            host,
            typ=TOKEN_TYPE_SHARE_SESSION,
        )
        if claims is None:
            if share_cookie:
                # A guest whose access lapsed cannot use Google login anyway.
                self.send_html(share_link_invalid_page(), status=HTTPStatus.FORBIDDEN)
            else:
                self.redirect_to_login()
            return
        now = int(time.time())
        guest_id = claims["gid"]
        subject = f"share:{claims['jti']}:{guest_id}"
        email = f"guest-{guest_id}@{GUEST_EMAIL_DOMAIN}"
        jwt = create_jwt(
            {
                "iss": auth_issuer(),
                "aud": host,
                "sub": subject,
                "email": email,
                "name": "Guest",
                "guest": True,
                "project_host": host,
                "iat": now,
                "exp": now + downstream_jwt_seconds(),
            },
            typ=TOKEN_TYPE_DOWNSTREAM,
        )
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Temporal-Auth-JWT", jwt)
        self.send_header("X-Temporal-Auth-Subject", subject)
        self.send_header("X-Temporal-Auth-Email", email)
        self.send_header("X-Temporal-Auth-Name", "Guest")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def exchange_share_link(self, host: str, url: str, token: str) -> None:
        # Trade the link for a host-only cookie, then redirect to the same URL
        # without the token so it stops travelling in URLs, logs and Referer.
        clean_url = without_share_param(url)
        claims = self.verified_share_claims(
            token, host, typ=TOKEN_TYPE_SHARE_LINK, fresh=True
        )
        if claims is None:
            if self.has_employee_session():
                self.send_redirect(clean_url)
                return
            self.send_html(
                share_link_invalid_page(),
                status=HTTPStatus.FORBIDDEN,
            )
            return
        session = create_jwt(
            {
                **{key: value for key, value in claims.items() if key != "typ"},
                # One guest identity per browser that opens the link.
                "gid": secrets.token_hex(6),
            },
            typ=TOKEN_TYPE_SHARE_SESSION,
            key=share_signing_key(),
        )
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", clean_url)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_cookie(
            WORKSPACE_SHARE_COOKIE,
            session,
            max_age=max(claims["exp"] - int(time.time()), 0),
            # Host-only: no Domain attribute, so the browser only ever sends
            # it back to this one workspace host.
            domain=None,
            httponly=True,
            secure=auth_cookie_secure(),
            same_site="Lax",
        )
        self.send_header("Content-Length", "0")
        self.end_headers()

    def verified_share_claims(
        self,
        token: str | None,
        host: str,
        *,
        typ: str,
        fresh: bool = False,
    ) -> dict[str, Any] | None:
        """Claims of a share token valid for this host right now, else None.

        Fails closed: a bad signature, wrong host, torn-down or recreated
        workspace, bumped share epoch, or unreachable Temporal all deny.
        """

        if not token or not host.startswith(WORKSPACE_HOST_PREFIX):
            return None
        try:
            claims = decode_jwt(token, typ=typ, key=share_signing_key())
        except AuthError:
            return None
        if claims.get("aud") != host:
            return None
        owner, demo = claims.get("owner"), claims.get("demo")
        if not isinstance(owner, str) or not isinstance(demo, str):
            return None
        if not isinstance(claims.get("jti"), str):
            return None
        if typ == TOKEN_TYPE_SHARE_SESSION and not isinstance(claims.get("gid"), str):
            return None
        if self.temporal_gateway is None:
            return None
        try:
            status = SHARE_STATUS_CACHE.get(
                self.temporal_gateway, owner, demo, fresh=fresh
            )
        except TemporalUnavailable:
            return None
        if not (
            status.get("phase") in WORKSPACE_LIVE_PHASES
            and status.get("host") == host
            and status.get("workspace_id") == claims.get("ws")
            and status.get("created_at") == claims.get("gen")
            and int(status.get("share_epoch") or 0) == claims.get("epoch")
        ):
            return None
        return claims

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


def share_signing_key() -> bytes:
    return hmac.new(
        auth_signing_key().encode("utf-8"),
        SHARE_KEY_CONTEXT,
        hashlib.sha256,
    ).digest()


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


def share_param(url: str) -> str | None:
    for key, value in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        if key == WORKSPACE_SHARE_PARAM:
            return value
    return None


def without_share_param(url: str) -> str:
    parts = urlsplit(url)
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key != WORKSPACE_SHARE_PARAM
    ]
    return urlunsplit(parts._replace(query=urlencode(query)))


def same_origin_request(handler: BaseHTTPRequestHandler) -> bool:
    # Every demo host is same-site with the catalog, so SameSite=Lax alone does
    # not stop a demo page from POSTing here; require the catalog's own origin.
    fetch_site = handler.headers.get("Sec-Fetch-Site")
    if fetch_site is not None and fetch_site != "same-origin":
        return False
    origin = handler.headers.get("Origin")
    return origin is None or origin == auth_base_url()


def share_link_invalid_page() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Link expired</title>
</head>
<body style="font-family: system-ui, sans-serif; max-width: 32rem; margin: 4rem auto; padding: 0 1rem;">
  <h1>This link no longer works</h1>
  <p>The shared demo link has expired, was revoked, or the environment it
  pointed to has been deleted. Ask the person who shared it for a new link.</p>
</body>
</html>
"""


class ShareStatusCache:
    """Short-lived cache of workspace status for share-cookie checks.

    Share cookies are re-checked against the workspace on every request, so
    this keeps that to one Temporal query per workspace per window. Revocation
    from this replica invalidates immediately; other replicas within the window.
    """

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = ttl_seconds
        self._entries: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def get(
        self,
        gateway: "TemporalGateway",
        owner: str,
        demo: str,
        *,
        fresh: bool = False,
    ) -> dict[str, Any]:
        key = (owner, demo)
        now = time.monotonic()
        if not fresh:
            with self._lock:
                entry = self._entries.get(key)
            if entry and now - entry[0] < self._ttl:
                return entry[1]
        status = gateway.workspace_status(owner, demo)
        with self._lock:
            self._entries[key] = (now, status)
        return status

    def invalidate(self, owner: str, demo: str) -> None:
        with self._lock:
            self._entries.pop((owner, demo), None)


SHARE_STATUS_CACHE = ShareStatusCache(SHARE_STATUS_CACHE_SECONDS)


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
    claims = decode_jwt(token, typ=TOKEN_TYPE_SESSION)
    if claims.get("aud") != SESSION_AUDIENCE:
        raise AuthError("Invalid session audience.")
    return claims


def create_session_token(identity: GoogleIdentity) -> str:
    now = int(time.time())
    subject = f"google:{identity.subject}"
    return create_jwt(
        {
            "iss": auth_issuer(),
            "aud": SESSION_AUDIENCE,
            "sub": subject,
            "email": identity.email,
            "name": identity.name,
            "hd": identity.hosted_domain,
            "iat": now,
            "exp": now + session_seconds(),
            "auth_time": now,
        },
        typ=TOKEN_TYPE_SESSION,
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
        },
        typ=TOKEN_TYPE_OAUTH_STATE,
    )


def decode_state_token(token: str) -> dict[str, Any]:
    claims = decode_jwt(token, typ=TOKEN_TYPE_OAUTH_STATE)
    if claims.get("aud") != "google-oauth-state":
        raise AuthError("Invalid OAuth state.")
    if not isinstance(claims.get("nonce"), str):
        raise AuthError("Invalid OAuth state.")
    return claims


def jwt_key(key: bytes | None) -> bytes:
    if not auth_signing_key():
        raise AuthError("Auth signing key is not configured.")
    return key if key is not None else auth_signing_key().encode("utf-8")


def create_jwt(
    payload: dict[str, Any], *, typ: str, key: bytes | None = None
) -> str:
    signing_key = jwt_key(key)
    header = {"alg": "HS256", "typ": "JWT"}
    body = {key: value for key, value in payload.items() if value is not None}
    body["typ"] = typ
    signing_input = ".".join([base64url_json(header), base64url_json(body)])
    signature = hmac.new(
        signing_key,
        signing_input.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{signing_input}.{base64url_encode(signature)}"


def decode_jwt(token: str, *, typ: str, key: bytes | None = None) -> dict[str, Any]:
    signing_key = jwt_key(key)
    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("Invalid session token.")
    signing_input = f"{parts[0]}.{parts[1]}"
    expected = hmac.new(
        signing_key,
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
    if payload.get("typ") != typ:
        raise AuthError("Invalid token type.")
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
    onboarding_secrets: OnboardingSecrets | None = None,
) -> type[RegistryRequestHandler]:
    class ConfiguredRegistryRequestHandler(RegistryRequestHandler):
        pass

    ConfiguredRegistryRequestHandler.registry_path = registry_path
    ConfiguredRegistryRequestHandler.status_dir = status_dir
    ConfiguredRegistryRequestHandler.temporal_gateway = temporal_gateway
    ConfiguredRegistryRequestHandler.onboarding_secrets = onboarding_secrets
    return ConfiguredRegistryRequestHandler


def main() -> None:
    registry_path = configured_path("REGISTRY_DATA_PATH", DEFAULT_REGISTRY_PATH)
    status_dir = configured_path("CATALOG_STATUS_DIR", DEFAULT_STATUS_DIR)
    port = int(os.environ.get("PORT", "8080"))
    temporal_gateway = build_temporal_gateway()
    # Onboarding secrets need Temporal (to find the user's draft) and AWS; the
    # client is created on first use, so a missing role only fails that call.
    onboarding_secrets = OnboardingSecrets() if temporal_gateway else None
    handler = make_handler(registry_path, status_dir, temporal_gateway, onboarding_secrets)
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    crashable = "enabled" if temporal_gateway else "disabled (no TEMPORAL_ADDRESS)"
    print(f"Serving registry catalog on :{port} (crashable workspaces: {crashable})")
    server.serve_forever()


if __name__ == "__main__":
    main()
