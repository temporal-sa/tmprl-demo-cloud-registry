"""Ziggy, the catalog's demo picker: one Claude call per activity.

``run_catalog_helper_model_call`` makes one model call of a Ziggy turn; the
agent workflow (workflows/catalog_helper.py) runs the tool loop around it. It:

1. reads the demos the catalog lists, built by the same ``catalog_payload``
   the operator publishes to registry.json, so Ziggy only knows demos the
   page shows.
2. calls Claude with the platform's shared Anthropic key
   (tmprl-dem-cld/platform/shared/anthropic, the one demos opt into), read
   from Secrets Manager once per worker process and kept with its client.
3. offers Ziggy's tools on a small model. They only change what the page
   shows (workflows/catalog_helper_tools.py); demo ids are an enum of the
   catalog's names, so Ziggy can't name a demo that isn't there. The workflow
   runs the calls the model makes and sends their results back on the next
   call when it needs one.

The key never leaves the worker. Each call sends the chat so far as plain text
(bounded by the workflow) plus this turn's earlier rounds, so nothing is kept
on the model side between calls.
"""

from __future__ import annotations

import inspect
import json
import os
import threading
from datetime import timedelta
from typing import Any

import anthropic
from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from demo_registry_operator import derive, errors, kube, resources
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.workflows.catalog_helper_messages import (
    HelperModelRequest,
    HelperModelResponse,
    HelperView,
)
from demo_registry_operator.workflows.catalog_helper_tools import HELPER_TOOLS

MODEL_ENV = "DEMO_REGISTRY_CATALOG_HELPER_MODEL"
DEFAULT_MODEL = "claude-haiku-5-5"
ANTHROPIC_SECRET = f"{derive.SECRET_PREFIX_ROOT}/platform/shared/anthropic"
# Thinking counts toward this; a reply is a few sentences.
MAX_OUTPUT_TOKENS = 4096
REQUEST_TIMEOUT_SECONDS = 45
NO_DEMOS_REPLY = "The catalog has no demos listed right now, so I can't recommend one."
PRESENTER_LINKS = {
    "slideDeck": "slide deck",
    "speakerNotes": "speaker notes",
    "externalRecording": "recording",
    "goldenPath": "golden path",
}

# One quick retry for a lost worker or an API blip; a turn the user waits
# on shouldn't retry for long.
HELPER_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_attempts=2,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)

SYSTEM_PROMPT = """\
You are Ziggy, the helper on the Temporal Field Kit demo catalog. Temporal's
field team (solutions architects, sales engineers, developer advocates) use the
catalog to pick a live demo for a customer conversation, a talk, or a workshop.
You help them find the right demo for what they describe: an industry, a use
case, a Temporal feature, an SDK language, an AI framework, or an audience.

Every demo in the catalog is listed below as JSON. That list is everything you
know about the demos; never claim a demo shows something its entry doesn't say.

How to help:
- When you know what they want, call show_demos with the demos that fit, best
  first, and say in a sentence or two why each one fits. Use
  highlight_demos to call out the one or two you'd start with.
- Write your whole reply and make your tool calls in the same response. The
  tools always do what they say, so you don't need to see their results
  before answering, and the user waits on every extra round.
- When the request is broad ("something with AI"), it's fine to show a first
  set and ask one short question that would narrow it (language, audience,
  feature). Don't ask more than one question at a time.
- When they clarify or change direction, call show_demos again with the new
  set; it replaces the old one.
- When no demo covers what they asked for, say so plainly. Don't stretch a
  loosely related demo to fit; if one is close, name it and say what it does
  and doesn't cover. Never filter to an empty catalog.
- When they want to see everything again, call clear_view.
- Canonical demos are curated, presenter-ready stories with decks and
  recordings; prefer them when two demos fit equally well.
- A demo marked underConstruction is still being built and not ready to
  present. Don't highlight one, and when you show one, say it's under
  construction.

Keep replies short: a few sentences. The chat renders light markdown: bold a
demo's title, and use a short bullet list when you name several demos; no
headings or tables. Refer to demos by title. You can only change what the demos page shows; you can't
open, deploy, or crash demos, so point them at the demo's card for that.

The catalog:
{catalog}
"""


@activity.defn
def check_catalog_helper_worker() -> None:
    """The rollout gate's check on Ziggy's queue (workflows/catalog_helper_canary.py):
    it passes once an activity worker of the new version picks it up."""


@activity.defn
def run_catalog_helper_model_call(request: HelperModelRequest) -> HelperModelResponse:
    demos = catalog_demos()
    if not demos:
        return HelperModelResponse(
            content=[{"type": "text", "text": NO_DEMOS_REPLY}], stop_reason="end_turn"
        )
    try:
        return model_call(anthropic_client(), request, demos)
    except ApplicationError as error:
        if getattr(error.__cause__, "status_code", None) in {401, 403}:
            # The shared key may have been rotated; read it again next turn.
            forget_anthropic_client()
        raise


_client_lock = threading.Lock()
_client: anthropic.Anthropic | None = None


def anthropic_client() -> anthropic.Anthropic:
    """One client per worker process, so turns skip the secret read and
    reuse the client's connections."""

    global _client
    with _client_lock:
        if _client is None:
            _client = anthropic.Anthropic(
                api_key=read_anthropic_key(),
                timeout=REQUEST_TIMEOUT_SECONDS,
                max_retries=1,
            )
        return _client


def forget_anthropic_client() -> None:
    global _client
    with _client_lock:
        _client = None


def read_anthropic_key() -> str:
    import boto3

    secretsmanager = boto3.session.Session(
        region_name=WorkerConfig.from_env().aws_region
    ).client("secretsmanager")
    secret = secretsmanager.get_secret_value(SecretId=ANTHROPIC_SECRET)
    try:
        key = json.loads(secret.get("SecretString") or "{}").get("ANTHROPIC_API_KEY")
    except (json.JSONDecodeError, AttributeError):
        key = None
    if not key:
        raise errors.non_retryable_error(
            f"{ANTHROPIC_SECRET} has no ANTHROPIC_API_KEY.",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return key


def model_call(
    client: Any, request: HelperModelRequest, demos: list[dict[str, Any]]
) -> HelperModelResponse:
    """One Messages API call, separate from key handling so tests can drive it."""

    ids = [demo["id"] for demo in demos]
    titles = {demo["id"]: demo["title"] for demo in demos}
    system = [
        {
            "type": "text",
            "text": SYSTEM_PROMPT.format(catalog=json.dumps(demos, sort_keys=True)),
            # The catalog rarely changes, so every call of every chat reads it
            # from the cache.
            "cache_control": {"type": "ephemeral"},
        }
    ]
    messages = conversation(request, titles)
    for earlier in request.rounds:
        messages.append({"role": "assistant", "content": earlier.content})
        messages.append({"role": "user", "content": earlier.tool_results})
    try:
        response = client.messages.create(
            model=os.environ.get(MODEL_ENV, "").strip() or DEFAULT_MODEL,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=system,
            tools=tool_definitions(ids),
            tool_choice={"type": "none"} if request.last_round else {"type": "auto"},
            output_config={"effort": "low"},
            messages=messages,
        )
    except anthropic.APIStatusError as error:
        if error.status_code in {408, 409, 429} or error.status_code >= 500:
            raise
        raise errors.non_retryable_error(
            f"The Claude API refused the request (HTTP {error.status_code}).",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error
    usage = response.usage
    return HelperModelResponse(
        # As the API sent them, so thinking blocks keep their signatures when
        # the next call sends them back.
        content=[block.to_dict(mode="json") for block in response.content],
        stop_reason=response.stop_reason,
        titles=titles,
        input_tokens=(
            (usage.input_tokens or 0)
            + (getattr(usage, "cache_read_input_tokens", 0) or 0)
            + (getattr(usage, "cache_creation_input_tokens", 0) or 0)
        ),
        output_tokens=usage.output_tokens or 0,
    )


def catalog_demos() -> list[dict[str, Any]]:
    """The catalog's demos, trimmed to what helps pick one."""

    projects = resources.catalog_payload(kube.list_demo_projects())["projects"]
    return [demo_brief(project) for project in projects]


def demo_brief(entry: dict[str, Any]) -> dict[str, Any]:
    links = entry.get("links") if isinstance(entry.get("links"), dict) else {}
    brief: dict[str, Any] = {
        "id": entry["name"],
        "title": entry.get("title") or entry["name"],
        "summary": entry.get("summary") or "",
        "tags": list(entry.get("tags") or []),
        "temporalSdks": list(entry.get("temporalSdks") or []),
        "aiSdks": list(entry.get("aiSdks") or []),
        "canonical": bool(entry.get("canonical")),
        "presenterAssets": [
            label for key, label in PRESENTER_LINKS.items() if (links.get(key) or {}).get("url")
        ],
        "signInRequired": bool(entry.get("authRequired")),
    }
    if (entry.get("crashable") or {}).get("enabled") or (entry.get("isolatedLambda") or {}).get("enabled"):
        brief["privateCrashableEnvironment"] = True
    if entry.get("underConstruction"):
        brief["underConstruction"] = True
    return brief


def tool_definitions(ids: list[str]) -> list[dict[str, Any]]:
    ids_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["demo_ids"],
        "properties": {
            "demo_ids": {"type": "array", "items": {"type": "string", "enum": ids}},
        },
    }
    no_arguments = {"type": "object", "additionalProperties": False, "properties": {}}
    schemas = {
        "show_demos": ids_schema,
        "highlight_demos": ids_schema,
        "clear_view": no_arguments,
    }
    return [
        {
            "name": name,
            "description": " ".join((inspect.getdoc(HELPER_TOOLS[name]) or "").split()),
            "input_schema": schema,
            "strict": True,
        }
        for name, schema in schemas.items()
    ]


def conversation(request: HelperModelRequest, titles: dict[str, str]) -> list[dict[str, Any]]:
    """The chat so far as plain text turns, then the new message with what the
    page shows now (the user may have cleared Ziggy's filter since)."""

    messages: list[dict[str, Any]] = []
    for message in request.history:
        if message.role == "ziggy" and message.outcome != "Done":
            continue
        role = "user" if message.role == "user" else "assistant"
        if not messages and role == "assistant":
            continue  # the history must start with the user
        messages.append({"role": role, "content": message.text})
    messages.append(
        {
            "role": "user",
            "content": [
                {"type": "text", "text": view_note(request.view, titles)},
                {"type": "text", "text": request.message},
            ],
        }
    )
    return messages


def view_note(view: HelperView, titles: dict[str, str]) -> str:
    def names(ids: list[str]) -> str:
        return ", ".join(f"{titles[i]} ({i})" for i in ids if i in titles) or "none"

    shown = (
        "every demo"
        if view.demo_ids is None
        else f"only your picks: {names(view.demo_ids)}"
    )
    highlighted = names(view.highlights)
    return f"[The demos page currently shows {shown}. Highlighted: {highlighted}.]"
