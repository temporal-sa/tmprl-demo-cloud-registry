"""Boundary payloads for the catalog helper (Ziggy) agent.

Ziggy runs on the Temporal agent harness, whose client and worker use a
Pydantic data converter, so these are pydantic models. The catalog web server
sends ``HelperAsk`` through the harness's ``send_agent_message`` front door and
reads ``HelperStatus`` back as a plain dict from the ``status`` query.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# The chat takes no more messages in these phases.
CLOSED_PHASES = frozenset({"Ended", "Expired"})


class HelperView(BaseModel):
    """What Ziggy has done to the demos page.

    demo_ids is Ziggy's filter: the page shows exactly these demos, or every
    demo when it's None. highlights get a halo wherever they're shown.
    """

    demo_ids: list[str] | None = None
    highlights: list[str] = Field(default_factory=list)


class HelperMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    # "user" | "ziggy"
    role: str
    text: str
    at: str | None = None
    # Ziggy messages: "Done" | "Failed", and what the turn did to the page,
    # e.g. "Showing 3 demos".
    outcome: str | None = None
    actions: list[str] = Field(default_factory=list)


class HelperStatus(BaseModel):
    # "Open" | "Ended" | "Expired"
    phase: str
    owner_email: str
    messages: list[HelperMessage] = Field(default_factory=list)
    view: HelperView = Field(default_factory=HelperView)
    # A turn is running; the page shows Ziggy typing.
    busy: bool = False
    turns: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    updated_at: str | None = None
    expires_at: str | None = None


class StartHelperRequest(BaseModel):
    """Who the chat belongs to."""

    owner_email: str


class HelperAsk(BaseModel):
    """A message from the user to Ziggy."""

    text: str


class HelperReply(BaseModel):
    """Ziggy's answer to one message."""

    text: str
    # "Done" | "Failed"
    outcome: str
    actions: list[str] = Field(default_factory=list)


class HelperRound(BaseModel):
    """One earlier model call of this turn: what the model said, and the
    results of the tools it called."""

    content: list[dict[str, Any]]
    tool_results: list[dict[str, Any]]


class HelperModelRequest(BaseModel):
    message: str
    # Earlier messages of this chat, oldest first.
    history: list[HelperMessage] = Field(default_factory=list)
    # The page when the turn started.
    view: HelperView = Field(default_factory=HelperView)
    rounds: list[HelperRound] = Field(default_factory=list)
    # The turn's last model call may not call tools, so it ends with a reply.
    last_round: bool = False


class HelperModelResponse(BaseModel):
    # The model's content blocks as the Messages API returned them.
    content: list[dict[str, Any]]
    stop_reason: str | None = None
    # The catalog the call saw, demo id to title.
    titles: dict[str, str] = Field(default_factory=dict)
    input_tokens: int = 0
    output_tokens: int = 0
