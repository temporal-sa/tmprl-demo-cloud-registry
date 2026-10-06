"""Boundary payloads for the namespace readers workflows.

The catalog web server starts ``GrantNamespaceReadAccessWorkflow`` with a plain
``{"email": ...}`` dict, which the SDK decodes into the request dataclass, and
reads the result back as a plain dict.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class GrantNamespaceReadAccessRequest:
    email: str


@dataclass(frozen=True)
class GrantNamespaceReadAccessResult:
    # "added" | "already-member" | "user-not-found" | "not-configured" | "error"
    status: str
    email: str
    group: str | None = None
    message: str | None = None


@dataclass(frozen=True)
class SyncNamespaceReadersRequest:
    reason: str = "watch-activated"


@dataclass(frozen=True)
class SyncNamespaceReadersResult:
    status: dict[str, Any] = field(default_factory=dict)
