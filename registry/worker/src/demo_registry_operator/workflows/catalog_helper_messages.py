"""Boundary payloads for the catalog helper (Ziggy) workflow.

The catalog web server drives ``CatalogHelperWorkflow`` with plain dicts that
the SDK decodes into these dataclasses, and reads ``HelperStatus`` back as a
plain dict.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# The chat takes no more messages in these phases.
CLOSED_PHASES = frozenset({"Ended", "Expired"})


@dataclass
class HelperView:
    """What Ziggy has done to the demos page.

    demo_ids is Ziggy's filter: the page shows exactly these demos, or every
    demo when it's None. highlights get a halo wherever they're shown.
    """

    demo_ids: list[str] | None = None
    highlights: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class HelperMessage:
    # "user" | "ziggy"
    role: str
    text: str
    at: str | None = None
    # Ziggy messages: "Done" | "Failed", and what the turn did to the page,
    # e.g. "Showing 3 demos".
    outcome: str | None = None
    actions: list[str] = field(default_factory=list)


@dataclass
class HelperStatus:
    # "Open" | "Ended" | "Expired"
    phase: str
    owner_email: str
    messages: list[HelperMessage] = field(default_factory=list)
    view: HelperView = field(default_factory=HelperView)
    # A turn is running; the page shows Ziggy typing.
    busy: bool = False
    turns: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    updated_at: str | None = None
    expires_at: str | None = None


@dataclass(frozen=True)
class StartHelperRequest:
    owner_email: str


@dataclass(frozen=True)
class SendHelperMessage:
    text: str


@dataclass(frozen=True)
class HelperTurnRequest:
    # Unique per turn; the model key is minted for it.
    turn_id: str
    message: str
    # Earlier messages of this chat, oldest first.
    history: list[HelperMessage] = field(default_factory=list)
    view: HelperView = field(default_factory=HelperView)


@dataclass(frozen=True)
class HelperTurnResult:
    reply: str
    view: HelperView
    actions: list[str] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
