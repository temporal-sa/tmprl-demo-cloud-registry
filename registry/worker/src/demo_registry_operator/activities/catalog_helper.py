"""Ziggy, the catalog's demo picker: one chat turn per activity.

``run_catalog_helper_turn`` answers one message from the floating helper on the
demos page. It:

1. reads the demos the catalog lists, built by the same ``catalog_payload``
   the operator publishes to registry.json, so Ziggy only knows demos the
   page shows.
2. calls Claude with the platform's shared Anthropic key
   (tmprl-dem-cld/platform/shared/anthropic, the one demos opt into), read
   from Secrets Manager once per worker process and kept with its client.
3. runs a short Messages API tool loop on a small model. Ziggy's tools only
   change what the page shows: filter it to the demos Ziggy names, put a halo
   on some of them, or clear both. Demo ids are an enum of the catalog's names,
   so Ziggy can't name a demo that isn't there. The activity applies the calls
   to the view it was given and returns the new view; the page renders it.

The key never leaves the worker. Each turn sends the chat so far as plain text (bounded by the workflow), so nothing
is kept on the model side between turns.
"""

from __future__ import annotations

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
    HelperTurnRequest,
    HelperTurnResult,
    HelperView,
)

MODEL_ENV = "DEMO_REGISTRY_CATALOG_HELPER_MODEL"
DEFAULT_MODEL = "claude-haiku-5-5"
ANTHROPIC_SECRET = f"{derive.SECRET_PREFIX_ROOT}/platform/shared/anthropic"
# Thinking counts toward this; a reply is a few sentences.
MAX_OUTPUT_TOKENS = 4096
# Model calls per turn. The last one may not call tools, so the turn ends
# with a reply.
MAX_ROUNDS = 4
REQUEST_TIMEOUT_SECONDS = 45
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

TOOL_DESCRIPTIONS = {
    "show_demos": (
        "Filter the demos page to exactly these demos, best match first. "
        "Replaces any earlier filter. Use it whenever you recommend demos."
    ),
    "highlight_demos": (
        "Put a glowing halo on these demos to call them out, usually the one or "
        "two to start with. Replaces earlier highlights; an empty list removes them."
    ),
    "clear_view": "Remove your filter and highlights so the page shows every demo.",
}


@activity.defn
def run_catalog_helper_turn(request: HelperTurnRequest) -> HelperTurnResult:
    demos = catalog_demos()
    if not demos:
        return HelperTurnResult(
            reply="The catalog has no demos listed right now, so I can't recommend one.",
            view=HelperView(),
        )
    try:
        return run_turn(anthropic_client(), request, demos)
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


def run_turn(
    client: Any, request: HelperTurnRequest, demos: list[dict[str, Any]]
) -> HelperTurnResult:
    """The tool loop, separate from key handling so tests can drive it."""

    ids = [demo["id"] for demo in demos]
    titles = {demo["id"]: demo["title"] for demo in demos}
    system = [
        {
            "type": "text",
            "text": SYSTEM_PROMPT.format(catalog=json.dumps(demos, sort_keys=True)),
            # The catalog rarely changes, so every turn of every chat reads it
            # from the cache.
            "cache_control": {"type": "ephemeral"},
        }
    ]
    tools = tool_definitions(ids)
    messages = conversation(request, titles)
    view = HelperView(
        demo_ids=list(request.view.demo_ids) if request.view.demo_ids is not None else None,
        highlights=list(request.view.highlights),
    )
    texts: list[str] = []
    actions: list[str] = []
    input_tokens = output_tokens = 0
    for round_number in range(MAX_ROUNDS):
        last = round_number == MAX_ROUNDS - 1
        try:
            response = client.messages.create(
                model=os.environ.get(MODEL_ENV, "").strip() or DEFAULT_MODEL,
                max_tokens=MAX_OUTPUT_TOKENS,
                system=system,
                tools=tools,
                tool_choice={"type": "none"} if last else {"type": "auto"},
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
        input_tokens += (
            (usage.input_tokens or 0)
            + (getattr(usage, "cache_read_input_tokens", 0) or 0)
            + (getattr(usage, "cache_creation_input_tokens", 0) or 0)
        )
        output_tokens += usage.output_tokens or 0
        if response.stop_reason == "refusal":
            texts = ["I can't help with that. Tell me what you'd like to demo and I'll find a fit."]
            break
        texts += [block.text.strip() for block in response.content if block.type == "text" and block.text.strip()]
        calls = [block for block in response.content if block.type == "tool_use"]
        if response.stop_reason != "tool_use" or not calls:
            break
        messages.append({"role": "assistant", "content": response.content})
        results = []
        failed = False
        for call in calls:
            outcome, action, is_error = apply_tool(view, call.name, call.input, titles)
            if action:
                actions.append(action)
            failed = failed or is_error
            results.append(
                {"type": "tool_result", "tool_use_id": call.id, "content": outcome, "is_error": is_error}
            )
        # A reply that came with tool calls that all worked is the whole turn:
        # the results only confirm the view changed, and another round would
        # make the user wait for the model to say it again.
        if texts and not failed:
            break
        messages.append({"role": "user", "content": results})

    reply = "\n\n".join(texts) or (
        "I updated the page." if actions else "Sorry, I lost my train of thought. Could you say that again?"
    )
    return HelperTurnResult(
        reply=reply,
        view=view,
        actions=actions,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
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
    return [
        {
            "name": "show_demos",
            "description": TOOL_DESCRIPTIONS["show_demos"],
            "input_schema": ids_schema,
            "strict": True,
        },
        {
            "name": "highlight_demos",
            "description": TOOL_DESCRIPTIONS["highlight_demos"],
            "input_schema": ids_schema,
            "strict": True,
        },
        {
            "name": "clear_view",
            "description": TOOL_DESCRIPTIONS["clear_view"],
            "input_schema": {"type": "object", "additionalProperties": False, "properties": {}},
            "strict": True,
        },
    ]


def conversation(request: HelperTurnRequest, titles: dict[str, str]) -> list[dict[str, Any]]:
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


def apply_tool(
    view: HelperView, name: str, arguments: Any, titles: dict[str, str]
) -> tuple[str, str | None, bool]:
    """Applies one tool call to the view; returns (tool result, action label
    for the page, whether it failed)."""

    requested = arguments.get("demo_ids") if isinstance(arguments, dict) else None
    ids = list(dict.fromkeys(i for i in requested or [] if i in titles))
    if name == "show_demos":
        if not ids:
            return "Pick at least one demo id from the catalog.", None, True
        view.demo_ids = ids
        view.highlights = [h for h in view.highlights if h in ids]
        return (
            f"The page now shows {len(ids)} demo(s): {', '.join(titles[i] for i in ids)}.",
            f"Showing {len(ids)} demo{'s' if len(ids) != 1 else ''}",
            False,
        )
    if name == "highlight_demos":
        view.highlights = ids
        if not ids:
            return "Highlights removed.", "Removed highlights", False
        hidden = [i for i in ids if view.demo_ids is not None and i not in view.demo_ids]
        note = (
            f" Note: {', '.join(titles[i] for i in hidden)} isn't in your current filter, so its halo won't show."
            if hidden
            else ""
        )
        return (
            f"Highlighted {', '.join(titles[i] for i in ids)}.{note}",
            f"Highlighted {len(ids)}",
            False,
        )
    if name == "clear_view":
        view.demo_ids = None
        view.highlights = []
        return "The page shows every demo again.", "Showing every demo", False
    return f"Unknown tool {name}.", None, True
