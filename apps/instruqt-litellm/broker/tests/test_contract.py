from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from broker.config import TokenPolicy
from broker.litellm_client import LiteLLMClient


def test_budget_and_duration_are_required() -> None:
    with pytest.raises(ValidationError):
        TokenPolicy.model_validate({})
    with pytest.raises(ValidationError):
        TokenPolicy.model_validate({"duration": "2h"})
    with pytest.raises(ValidationError):
        TokenPolicy.model_validate({"max_budget": 5})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_budget", 0),
        ("max_budget", -1),
        ("max_budget", float("inf")),
        ("duration", "0h"),
        ("duration", "2hours"),
        ("duration", ""),
        ("models", []),
        ("models", [""]),
    ],
)
def test_invalid_policy_values_are_rejected(field: str, value: object) -> None:
    values: dict[str, object] = {"max_budget": 5, "duration": "2h"}
    values[field] = value
    with pytest.raises(ValidationError):
        TokenPolicy.model_validate(values)


def test_models_default_to_unrestricted() -> None:
    policy = TokenPolicy(max_budget=5, duration="2h")
    assert policy.models is None
    assert policy.default_model is None


def test_omitted_models_are_not_sent_to_litellm(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"key": "sk-test"}

    class Client:
        async def __aenter__(self) -> "Client":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def post(self, url: str, **kwargs: Any) -> Response:
            captured["url"] = url
            captured.update(kwargs)
            return Response()

    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: Client())
    client = LiteLLMClient(base_url="http://litellm", master_key="sk-master")

    result = asyncio.run(
        client.generate_key(
            credential_key_id="v1",
            track_id="new-track",
            participant_id="learner",
            participant_email=None,
            track_slug=None,
            metadata={},
            policy=TokenPolicy(max_budget=5, duration="2h"),
        )
    )

    assert result["api_key"] == "sk-test"
    assert captured["url"] == "http://litellm/key/generate"
    assert "models" not in captured["json"]
    assert captured["json"]["max_budget"] == 5
    assert captured["json"]["duration"] == "2h"
    assert captured["json"]["metadata"]["broker_credential_key_id"] == "v1"


def test_explicit_models_are_sent_to_litellm(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"key": "sk-test"}

    class Client:
        async def __aenter__(self) -> "Client":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def post(self, _url: str, **kwargs: Any) -> Response:
            captured.update(kwargs)
            return Response()

    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: Client())
    client = LiteLLMClient(base_url="http://litellm", master_key="sk-master")

    asyncio.run(
        client.generate_key(
            credential_key_id="v1",
            track_id="restricted-track",
            participant_id="learner",
            participant_email=None,
            track_slug=None,
            metadata={},
            policy=TokenPolicy(
                max_budget=5,
                duration="2h",
                models=["anthropic/*", "openai/*"],
            ),
        )
    )

    assert captured["json"]["models"] == ["anthropic/*", "openai/*"]
