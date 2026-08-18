from __future__ import annotations

import pytest

from common.semver import (
    BUMP_TYPES,
    InvalidVersionError,
    next_version,
    normalize_bump,
    parse_version,
)


def test_bump_types_are_the_three_the_orchestrator_accepts() -> None:
    assert BUMP_TYPES == ("major", "minor", "bugfix")


@pytest.mark.parametrize(
    ("current", "bump", "expected"),
    [
        ("2.3.0", "major", "3.0.0"),
        ("2.3.0", "minor", "2.4.0"),
        ("2.3.0", "bugfix", "2.3.1"),
        # A bump resets everything below it.
        ("2.3.7", "major", "3.0.0"),
        ("2.3.7", "minor", "2.4.0"),
        ("0.0.0", "bugfix", "0.0.1"),
        # Rollovers are ordinary addition, not decimal digits.
        ("2.9.9", "minor", "2.10.0"),
        ("9.9.9", "major", "10.0.0"),
        # Case and surrounding whitespace are tolerated on both inputs.
        (" 2.3.0 ", "MINOR", "2.4.0"),
    ],
)
def test_next_version_applies_the_bump(
    current: str, bump: str, expected: str
) -> None:
    assert next_version(current, bump) == expected


def test_parse_version_returns_three_integers() -> None:
    assert parse_version("2.3.1") == (2, 3, 1)
    assert parse_version("10.0.42") == (10, 0, 42)


@pytest.mark.parametrize(
    "malformed",
    [
        "",
        "   ",
        "2.3",
        "2.3.0.1",
        "2.3.x",
        "2..0",
        "v2.3.0",
        "2.3.0-rc1",
        "2.3.0+build7",
        "latest",
        "2.-1.0",
        "2.3.0 (rc)",
    ],
)
def test_parse_version_rejects_malformed_input(malformed: str) -> None:
    with pytest.raises(InvalidVersionError):
        parse_version(malformed)


def test_next_version_rejects_a_malformed_current_version() -> None:
    # The failure names the offending value, because this string arrives from a
    # deployment backend and whoever reads the error needs to see what came back.
    with pytest.raises(InvalidVersionError, match="not-a-version"):
        next_version("not-a-version", "minor")


def test_next_version_rejects_a_non_string_version() -> None:
    with pytest.raises(InvalidVersionError):
        next_version(None, "minor")  # type: ignore[arg-type]


def test_next_version_rejects_an_unknown_bump_type() -> None:
    with pytest.raises(InvalidVersionError, match="patch"):
        next_version("2.3.0", "patch")
    with pytest.raises(InvalidVersionError):
        next_version("2.3.0", "")


def test_normalize_bump_canonicalizes_and_rejects() -> None:
    assert normalize_bump(" Minor ") == "minor"
    assert normalize_bump("BUGFIX") == "bugfix"
    for bad in ("", "patch", "PATCH_LEVEL", "1"):
        with pytest.raises(InvalidVersionError):
            normalize_bump(bad)
