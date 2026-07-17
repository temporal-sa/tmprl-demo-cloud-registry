"""Temporal Cloud namespace tags (owner + repo).

Every managed demo namespace is stamped with owner=tmprl-demo.cloud and the
source repo slug, so a Cloud namespace is traceable back to the demo that made
it. Tags are applied by a dedicated, idempotent activity (UpdateNamespaceTags is
its own RPC, separate from namespace create).
"""

import asyncio

from demo_registry_operator import resources
from demo_registry_operator.activities.temporal_cloud import (
    ensure_temporal_cloud_namespace_tags,
)

from test_temporal_cloud import PROJECT


def _project_with_repo(repo):
    return {"metadata": {"name": "d"}, "spec": {"source": {"repo": repo}}}


# --- repo slug parsing ------------------------------------------------------

def test_source_repo_slug_parses_https_url():
    assert resources.source_repo_slug(
        _project_with_repo("https://github.com/temporal-sa/canonical-ai-demo")
    ) == "temporal-sa/canonical-ai-demo"


def test_source_repo_slug_parses_scp_and_strips_dot_git():
    assert resources.source_repo_slug(
        _project_with_repo("git@github.com:temporal-sa/canonical-ai-demo.git")
    ) == "temporal-sa/canonical-ai-demo"


def test_source_repo_slug_accepts_bare_slug():
    assert resources.source_repo_slug(
        _project_with_repo("temporal-sa/canonical-ai-demo")
    ) == "temporal-sa/canonical-ai-demo"


def test_source_repo_slug_missing_is_none():
    assert resources.source_repo_slug({"metadata": {"name": "d"}, "spec": {}}) is None


# --- desired tag set --------------------------------------------------------

def test_namespace_tags_owner_and_repo():
    tags = resources.namespace_tags(
        _project_with_repo("https://github.com/temporal-sa/canonical-ai-demo")
    )
    # Temporal Cloud tag values forbid '/', so the owner/repo slug is coerced.
    assert tags == {
        "owner": "tmprl-demo.cloud",
        "repo": "temporal-sa_canonical-ai-demo",
    }


def test_namespace_tags_owner_only_without_repo():
    # owner is always stamped; repo is omitted when the demo declares no source.
    assert resources.namespace_tags({"metadata": {"name": "d"}, "spec": {}}) == {
        "owner": "tmprl-demo.cloud",
    }


def test_tag_value_coerced_to_temporal_cloud_charset():
    # Temporal Cloud allows only [a-z0-9._-@], <=63 chars. Uppercase is lowered and
    # every other disallowed character (/, spaces, etc.) becomes '_'.
    tags = resources.namespace_tags(
        _project_with_repo("https://github.com/Temporal-SA/Canonical_AI/Demo")
    )
    assert tags["repo"] == "temporal-sa_canonical_ai_demo"
    assert all(c.islower() or not c.isalpha() for c in tags["repo"])


# --- ensure activity (idempotent converge) ----------------------------------

class FakeTagReconciler:
    def __init__(self, current=None):
        self.current = dict(current or {})
        self.calls = []

    async def update_namespace_tags(self, qualified_name, tags):
        self.calls.append((qualified_name, dict(tags)))
        to_upsert = {k: v for k, v in tags.items() if self.current.get(k) != v}
        if not to_upsert:
            return {"action": "verified", "tags": dict(tags)}
        self.current.update(to_upsert)
        return {"action": "updated", "tags": to_upsert}


_NAMESPACE_STATUS = {
    "namespace": {
        "name": "tmprl-dem-cld-banking-money-transfer.acct1",
        "grpcAddress": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
        "action": "verified",
    }
}

_DESIRED = (("owner", "tmprl-demo.cloud"), ("repo", "temporal-sa/canonical-ai-demo"))


def test_ensure_tags_upserts_when_absent():
    reconciler = FakeTagReconciler()
    status = asyncio.run(
        ensure_temporal_cloud_namespace_tags(
            PROJECT,
            namespace_status_value=_NAMESPACE_STATUS,
            tags=_DESIRED,
            reconciler=reconciler,
            checked_at="2026-07-16T00:00:00+00:00",
        )
    )
    assert status["phase"] == "Ready"
    assert status["tags"]["action"] == "updated"
    assert status["tags"]["desired"] == dict(_DESIRED)
    # Applied against the resolved qualified namespace name.
    assert reconciler.calls == [
        ("tmprl-dem-cld-banking-money-transfer.acct1", dict(_DESIRED))
    ]


def test_ensure_tags_is_noop_when_already_present():
    reconciler = FakeTagReconciler(current=dict(_DESIRED))
    status = asyncio.run(
        ensure_temporal_cloud_namespace_tags(
            PROJECT,
            namespace_status_value=_NAMESPACE_STATUS,
            tags=_DESIRED,
            reconciler=reconciler,
            checked_at="2026-07-16T00:00:00+00:00",
        )
    )
    assert status["phase"] == "Ready"
    assert status["tags"]["action"] == "verified"
