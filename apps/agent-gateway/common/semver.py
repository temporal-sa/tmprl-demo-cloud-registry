"""Version arithmetic for bump-driven release orchestration.

Pure functions with no I/O, no clock, and no randomness, so Workflow code can
call them inline: every replay of the same deployed version and bump type
computes the same next version, which is what makes the orchestrated CASE-2 path
replay safe.

Deliberately strict about input. The version this operates on comes back from a
deployment backend through get_deployed_version, and a version that cannot be
interpreted must surface as a clear failure rather than a guess: promoting
"3.0.0" because "2.x-rc1" was misread is worse than promoting nothing.
"""

from __future__ import annotations

BUMP_TYPES = ("major", "minor", "bugfix")

_DIGITS = frozenset("0123456789")


class InvalidVersionError(ValueError):
    """A version string or bump type could not be interpreted.

    A ValueError subclass so callers that only care that the input was bad can
    catch either. The orchestrator catches this and fails the operation with the
    message attached, rather than inventing a version.
    """


def parse_version(version: str) -> tuple[int, int, int]:
    """Parse MAJOR.MINOR.PATCH into three non-negative integers.

    Accepts exactly three dot-separated runs of ASCII digits, with surrounding
    whitespace tolerated. Everything else raises InvalidVersionError, including
    two-component versions ("2.3"), prerelease or build suffixes ("2.3.0-rc1"),
    a leading "v" ("v2.3.0"), signs, and empty components.
    """
    if not isinstance(version, str):
        raise InvalidVersionError(
            f"version must be a string, got {type(version).__name__}"
        )
    raw = version.strip()
    if not raw:
        raise InvalidVersionError("version is empty")
    parts = raw.split(".")
    if len(parts) != 3:
        raise InvalidVersionError(
            f"version {version!r} is not MAJOR.MINOR.PATCH "
            f"(expected 3 components, found {len(parts)})"
        )
    numbers: list[int] = []
    for part in parts:
        # Not str.isdigit(): that accepts non-ASCII digits such as "٣", which
        # int() would then happily convert into a version nobody typed.
        if not part or not set(part) <= _DIGITS:
            raise InvalidVersionError(
                f"version {version!r} has a non-numeric component {part!r}"
            )
        numbers.append(int(part))
    return numbers[0], numbers[1], numbers[2]


def next_version(current: str, bump: str) -> str:
    """Return the version reached by applying bump to current.

    major: 2.3.0 -> 3.0.0, minor: 2.3.0 -> 2.4.0, bugfix: 2.3.0 -> 2.3.1.
    A bump always resets the components below it, so the result is canonical
    even when the input carried leading zeros.
    """
    major, minor, patch = parse_version(current)
    kind = bump.strip().lower() if isinstance(bump, str) else ""
    if kind == "major":
        return f"{major + 1}.0.0"
    if kind == "minor":
        return f"{major}.{minor + 1}.0"
    if kind == "bugfix":
        return f"{major}.{minor}.{patch + 1}"
    raise InvalidVersionError(
        f"bump must be one of {', '.join(BUMP_TYPES)}, got {bump!r}"
    )


def normalize_bump(bump: str) -> str:
    """Return the canonical bump type, or raise InvalidVersionError.

    Used at the gateway edge so a bad bump type is rejected before a Workflow is
    started, and again inside the Workflow so the Workflow never trusts its
    caller to have done that.
    """
    kind = bump.strip().lower() if isinstance(bump, str) else ""
    if kind not in BUMP_TYPES:
        raise InvalidVersionError(
            f"bump must be one of {', '.join(BUMP_TYPES)}, got {bump!r}"
        )
    return kind
