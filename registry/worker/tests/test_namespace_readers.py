"""Namespace readers user group.

One Temporal Cloud user group (created once by an account admin) holds exactly
Read on every operator-managed demo namespace. Demo reconciles grant it on each
namespace, namespace deletes remove the grant, a singleton sync backfills
existing namespaces, and the catalog's self-service action adds the signed-in
user to the group by email. The operator never creates groups or changes
account roles.
"""

from __future__ import annotations

import asyncio
import dataclasses
from types import SimpleNamespace

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.api.cloud.cloudservice.v1 import (
    GetNamespacesResponse,
    GetUserGroupMembersResponse,
    GetUserGroupsResponse,
    GetUsersResponse,
)
from temporalio.api.cloud.identity.v1 import (
    Access,
    NamespaceAccess,
    User,
    UserGroup,
    UserGroupMember,
    UserGroupMemberId,
    UserGroupSpec,
    UserSpec,
)
from temporalio.api.cloud.namespace.v1 import Namespace
from temporalio.api.cloud.resource.v1 import ResourceState

from demo_registry_operator import config as config_module
from demo_registry_operator import derive, resources, watch
from demo_registry_operator.activities import namespace_readers as readers_activities
from demo_registry_operator.activities.temporal_cloud import (
    CloudApiReconciler,
    UserGroupInfo,
    ensure_temporal_cloud_namespace_readers_access,
    grant_namespace_readers_access,
    grant_user_namespace_read_access,
    revoke_namespace_readers_access,
    sync_namespace_readers_group,
)
from demo_registry_operator.workflows.namespace_readers_messages import (
    GrantNamespaceReadAccessRequest,
)

from test_temporal_cloud import PROJECT


GROUP = "tmprl-demo-cld-read"
OWNER_TAG = ("owner", "tmprl-demo.cloud")
NS_A = "tmprl-dem-cld-alpha.acct1"
NS_B = "tmprl-dem-cld-beta.acct1"
DONE = SimpleNamespace(async_operation=SimpleNamespace(id="", state=0))


class FakeGroupReconciler:
    """In-memory stand-in for the Cloud Ops calls behind the readers group."""

    def __init__(self, *, group=None, users=None, namespaces=None, members=None):
        self.groups: dict[str, UserGroupInfo] = {}
        if group is not None:
            self.groups[group.display_name] = group
        self.users = {email.lower(): user_id for email, user_id in (users or {}).items()}
        self.namespaces = dict(namespaces or {})
        self.members = {key: set(value) for key, value in (members or {}).items()}
        self.set_failures: list[Exception] = []
        self.calls: list[tuple] = []

    async def find_user_group(self, display_name):
        group = self.groups.get(display_name)
        if group is None:
            return None
        # Hand out a snapshot, like a fresh read from the API would.
        return dataclasses.replace(
            group, namespace_permissions=dict(group.namespace_permissions)
        )

    async def set_user_group_namespace_access(self, group, qualified_namespace, permission):
        self.calls.append(("set_access", qualified_namespace, permission))
        if self.set_failures:
            raise self.set_failures.pop(0)
        stored = self.groups[group.display_name]
        if permission is None:
            stored.namespace_permissions.pop(qualified_namespace, None)
        else:
            stored.namespace_permissions[qualified_namespace] = permission
        stored.resource_version = f"v{int(stored.resource_version[1:]) + 1}"

    async def find_user_by_email(self, email):
        return self.users.get(email.strip().lower())

    async def user_group_has_member(self, group_id, user_id):
        return user_id in self.members.get(group_id, set())

    async def add_user_group_member(self, group_id, user_id):
        self.calls.append(("add_member", group_id, user_id))
        self.members.setdefault(group_id, set()).add(user_id)
        return "added"

    async def list_tagged_namespaces(self, key, value):
        return [name for name, tags in self.namespaces.items() if tags.get(key) == value]


def existing_group(**permissions):
    return UserGroupInfo("group-1", GROUP, "v1", dict(permissions))


def set_calls(reconciler):
    return [call for call in reconciler.calls if call[0] == "set_access"]


def grant(reconciler, namespace=NS_A):
    return asyncio.run(
        grant_namespace_readers_access(
            reconciler,
            group_name=GROUP,
            qualified_namespace=namespace,
            retry_delay_seconds=0,
        )
    )


# --- per-namespace grant (demo reconcile) --------------------------------------


def test_grant_is_skipped_until_the_group_exists():
    reconciler = FakeGroupReconciler()

    assert grant(reconciler) == "group-missing"
    # The operator never creates the group; an account admin does, once.
    assert reconciler.calls == []


def test_grant_adds_read_when_missing():
    reconciler = FakeGroupReconciler(group=existing_group())

    assert grant(reconciler) == "granted"
    assert reconciler.groups[GROUP].namespace_permissions == {NS_A: "read"}


def test_grant_is_a_noop_when_read_is_already_there():
    reconciler = FakeGroupReconciler(group=existing_group(**{NS_A: "read"}))

    assert grant(reconciler) == "verified"
    assert set_calls(reconciler) == []


@pytest.mark.parametrize("permission", ["write", "admin"])
def test_grant_restricts_a_higher_group_permission_to_read(permission):
    # Membership must never carry more than Read into a demo namespace.
    reconciler = FakeGroupReconciler(group=existing_group(**{NS_A: permission}))

    assert grant(reconciler) == "restricted"
    assert reconciler.groups[GROUP].namespace_permissions == {NS_A: "read"}


def test_grant_retries_a_stale_group_resource_version():
    reconciler = FakeGroupReconciler(group=existing_group())
    reconciler.set_failures = [RuntimeError("resource version mismatch")]

    assert grant(reconciler) == "granted"
    assert len(set_calls(reconciler)) == 2


def test_grant_surfaces_other_errors():
    reconciler = FakeGroupReconciler(group=existing_group())
    reconciler.set_failures = [RuntimeError("permission denied")]

    with pytest.raises(RuntimeError, match="permission denied"):
        grant(reconciler)


def test_reconcile_step_reports_the_group_action_for_the_resolved_namespace():
    reconciler = FakeGroupReconciler(group=existing_group())
    status = asyncio.run(
        ensure_temporal_cloud_namespace_readers_access(
            PROJECT,
            namespace_status_value={
                "namespace": {"name": NS_A, "grpcAddress": f"{NS_A}.tmprl.cloud:7233"}
            },
            group_name=GROUP,
            reconciler=reconciler,
            checked_at="2026-10-06T00:00:00+00:00",
        )
    )

    assert status["phase"] == "Ready"
    assert status["namespace"] == {"name": NS_A}
    assert status["readersGroup"] == {"name": GROUP, "action": "granted"}


# --- revoke (namespace delete) ---------------------------------------------------


def revoke(reconciler, namespace=NS_A):
    return asyncio.run(
        revoke_namespace_readers_access(
            reconciler,
            group_name=GROUP,
            qualified_namespace=namespace,
            retry_delay_seconds=0,
        )
    )


def test_revoke_removes_the_grant():
    reconciler = FakeGroupReconciler(group=existing_group(**{NS_A: "read", NS_B: "read"}))

    assert revoke(reconciler) == "revoked"
    assert reconciler.groups[GROUP].namespace_permissions == {NS_B: "read"}
    assert set_calls(reconciler) == [("set_access", NS_A, None)]


def test_revoke_is_a_noop_without_a_grant_or_a_group():
    assert revoke(FakeGroupReconciler(group=existing_group())) == "not-granted"
    assert revoke(FakeGroupReconciler()) == "group-missing"


# --- singleton sync ----------------------------------------------------------------


def test_sync_backfills_exactly_read_on_managed_namespaces_only():
    reconciler = FakeGroupReconciler(
        group=existing_group(**{"someone-elses.acct1": "write"}),
        namespaces={
            NS_A: {"owner": "tmprl-demo.cloud"},
            NS_B: {"owner": "tmprl-demo.cloud", "repo": "temporal-sa_beta"},
            "someone-elses.acct1": {"owner": "someone-else"},
            "untagged.acct1": {},
        }
    )

    result = asyncio.run(
        sync_namespace_readers_group(
            reconciler, group_name=GROUP, owner_tag=OWNER_TAG, retry_delay_seconds=0
        )
    )

    assert result["group"]["action"] == "verified"
    assert result["namespaces"] == {NS_A: "granted", NS_B: "granted"}
    # Access on namespaces the operator doesn't manage is left alone.
    assert reconciler.groups[GROUP].namespace_permissions == {
        "someone-elses.acct1": "write",
        NS_A: "read",
        NS_B: "read",
    }


def test_sync_does_nothing_until_an_admin_creates_the_group():
    reconciler = FakeGroupReconciler(namespaces={NS_A: {"owner": "tmprl-demo.cloud"}})

    result = asyncio.run(
        sync_namespace_readers_group(reconciler, group_name=GROUP, owner_tag=OWNER_TAG)
    )

    assert result == {"group": {"name": GROUP, "action": "missing"}, "namespaces": {}}
    assert reconciler.calls == []


def test_sync_is_idempotent():
    reconciler = FakeGroupReconciler(
        group=existing_group(**{NS_A: "read"}),
        namespaces={NS_A: {"owner": "tmprl-demo.cloud"}},
    )

    result = asyncio.run(
        sync_namespace_readers_group(reconciler, group_name=GROUP, owner_tag=OWNER_TAG)
    )

    assert result["group"]["action"] == "verified"
    assert result["namespaces"] == {NS_A: "verified"}
    assert reconciler.calls == []


# --- self-service grant --------------------------------------------------------------


def self_service(reconciler, email="michael.jones@temporal.io"):
    return asyncio.run(
        grant_user_namespace_read_access(
            reconciler, email=email, group_name=GROUP, owner_tag=OWNER_TAG
        )
    )


def test_self_service_reports_a_missing_cloud_user_without_changing_anything():
    reconciler = FakeGroupReconciler(group=existing_group())

    result = self_service(reconciler)

    assert result == {
        "status": "user-not-found",
        "email": "michael.jones@temporal.io",
        "group": GROUP,
    }
    assert reconciler.calls == []


def test_self_service_adds_the_user_to_the_group():
    reconciler = FakeGroupReconciler(
        group=existing_group(), users={"michael.jones@temporal.io": "user-7"}
    )

    assert self_service(reconciler)["status"] == "added"
    assert reconciler.members == {"group-1": {"user-7"}}


def test_self_service_is_a_noop_for_an_existing_member():
    reconciler = FakeGroupReconciler(
        group=existing_group(),
        users={"michael.jones@temporal.io": "user-7"},
        members={"group-1": {"user-7"}},
    )

    assert self_service(reconciler)["status"] == "already-member"
    assert reconciler.calls == []


def test_self_service_reports_a_missing_group_without_changing_anything():
    reconciler = FakeGroupReconciler(users={"michael.jones@temporal.io": "user-7"})

    result = self_service(reconciler)

    assert result["status"] == "group-missing"
    assert GROUP in result["message"]
    assert reconciler.calls == []


def test_self_service_only_changes_membership():
    reconciler = FakeGroupReconciler(
        group=existing_group(**{NS_A: "read"}),
        users={"michael.jones@temporal.io": "user-7"},
    )

    self_service(reconciler)

    assert reconciler.calls == [("add_member", "group-1", "user-7")]


# --- Cloud Ops adapter -----------------------------------------------------------------


def proto_group(group_id, *, created, display_name=GROUP, state="ACTIVE", **permissions):
    accesses = {
        namespace: NamespaceAccess(
            permission=NamespaceAccess.Permission.Value(f"PERMISSION_{value.upper()}")
        )
        for namespace, value in permissions.items()
    }
    return UserGroup(
        id=group_id,
        resource_version=f"rv-{group_id}",
        spec=UserGroupSpec(
            display_name=display_name,
            access=Access(namespace_accesses=accesses),
        ),
        state=ResourceState.Value(f"RESOURCE_STATE_{state}"),
        created_time=Timestamp(seconds=created),
    )


def test_cloud_find_user_group_returns_the_oldest_live_match_with_its_permissions():
    class FakeCloudService:
        def __init__(self):
            self.requests = []

        async def get_user_groups(self, request):
            self.requests.append(request)
            return GetUserGroupsResponse(
                groups=[
                    proto_group("newer", created=200, **{NS_A: "read"}),
                    proto_group("oldest", created=100, **{NS_A: "read", NS_B: "admin"}),
                    proto_group("gone", created=50, state="DELETED"),
                    proto_group("other", created=10, display_name="someone-else"),
                ]
            )

    service = FakeCloudService()
    group = asyncio.run(CloudApiReconciler(service, account_id="acct1").find_user_group(GROUP))

    assert service.requests[0].display_name == GROUP
    assert group == UserGroupInfo("oldest", GROUP, "rv-oldest", {NS_A: "read", NS_B: "admin"})


def test_cloud_set_access_grants_read_and_removes_with_empty_access():
    class FakeCloudService:
        def __init__(self):
            self.requests = []

        async def set_user_group_namespace_access(self, request):
            self.requests.append(request)
            return DONE

    service = FakeCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1")
    group = UserGroupInfo("g-1", GROUP, "rv-9", {})

    asyncio.run(reconciler.set_user_group_namespace_access(group, NS_A, "read"))
    asyncio.run(reconciler.set_user_group_namespace_access(group, NS_A, None))

    granted, removed = service.requests
    assert (granted.group_id, granted.namespace, granted.resource_version) == ("g-1", NS_A, "rv-9")
    assert granted.access.permission == NamespaceAccess.PERMISSION_READ
    assert not removed.HasField("access")


def test_cloud_find_user_by_email_falls_back_to_a_case_insensitive_scan():
    class FakeCloudService:
        def __init__(self):
            self.filters = []

        async def get_users(self, request):
            self.filters.append(request.email)
            if request.email:
                return GetUsersResponse()  # exact-match filter misses on case
            return GetUsersResponse(
                users=[
                    User(
                        id="deleted",
                        spec=UserSpec(email="Michael.Jones@temporal.io"),
                        state=ResourceState.RESOURCE_STATE_DELETED,
                    ),
                    User(
                        id="user-7",
                        spec=UserSpec(email="Michael.Jones@temporal.io"),
                        state=ResourceState.RESOURCE_STATE_ACTIVE,
                    ),
                ]
            )

    service = FakeCloudService()
    user_id = asyncio.run(
        CloudApiReconciler(service, account_id="acct1").find_user_by_email(
            "michael.jones@temporal.io"
        )
    )

    assert user_id == "user-7"
    assert service.filters == ["michael.jones@temporal.io", ""]


def test_cloud_membership_check_and_add_tolerate_an_existing_member():
    class FakeCloudService:
        async def get_user_group_members(self, request):
            return GetUserGroupMembersResponse(
                members=[UserGroupMember(member_id=UserGroupMemberId(user_id="user-7"))]
            )

        async def add_user_group_member(self, request):
            raise RuntimeError("member already exists in group")

    reconciler = CloudApiReconciler(FakeCloudService(), account_id="acct1")

    assert asyncio.run(reconciler.user_group_has_member("g-1", "user-7")) is True
    assert asyncio.run(reconciler.user_group_has_member("g-1", "user-8")) is False
    assert asyncio.run(reconciler.add_user_group_member("g-1", "user-8")) == "already-member"


def test_cloud_list_tagged_namespaces_skips_other_owners_and_deleting_namespaces():
    class FakeCloudService:
        async def get_namespaces(self, request):
            return GetNamespacesResponse(
                namespaces=[
                    Namespace(namespace=NS_A, tags={"owner": "tmprl-demo.cloud"},
                              state=ResourceState.RESOURCE_STATE_ACTIVE),
                    Namespace(namespace=NS_B, tags={"owner": "tmprl-demo.cloud"},
                              state=ResourceState.RESOURCE_STATE_DELETING),
                    Namespace(namespace="other.acct1", tags={"owner": "someone-else"},
                              state=ResourceState.RESOURCE_STATE_ACTIVE),
                ]
            )

    names = asyncio.run(
        CloudApiReconciler(FakeCloudService(), account_id="acct1").list_tagged_namespaces(
            *OWNER_TAG
        )
    )

    assert names == [NS_A]


class DeletingCloudService:
    """Fake for delete_namespace with a readers group that can read the namespace."""

    def __init__(self, *, fail_group_lookup=False):
        self.events = []
        self.fail_group_lookup = fail_group_lookup

    async def get_namespace(self, request):
        return SimpleNamespace(namespace=SimpleNamespace(resource_version="ns-rv"))

    async def get_user_groups(self, request):
        if self.fail_group_lookup:
            raise RuntimeError("permission denied")
        return GetUserGroupsResponse(groups=[proto_group("g-1", created=1, **{NS_A: "read"})])

    async def set_user_group_namespace_access(self, request):
        self.events.append(("revoke", request.namespace, request.HasField("access")))
        return DONE

    async def delete_namespace(self, request):
        self.events.append(("delete", request.namespace))
        return DONE


def test_cloud_delete_namespace_removes_the_readers_grant_first():
    service = DeletingCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1", readers_group=GROUP)

    assert asyncio.run(reconciler.delete_namespace(NS_A)) == "deleted"
    assert service.events == [("revoke", NS_A, False), ("delete", NS_A)]


def test_cloud_delete_namespace_still_deletes_when_the_revoke_fails():
    service = DeletingCloudService(fail_group_lookup=True)
    reconciler = CloudApiReconciler(service, account_id="acct1", readers_group=GROUP)

    assert asyncio.run(reconciler.delete_namespace(NS_A)) == "deleted"
    assert service.events == [("delete", NS_A)]


# --- wiring: config, ids, activity, watch trigger ------------------------------------------------


def test_readers_group_defaults_and_can_be_renamed_or_disabled(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_NAMESPACE_READERS_GROUP", raising=False)
    assert config_module.WorkerConfig.from_env().namespace_readers_group == GROUP

    monkeypatch.setenv("DEMO_REGISTRY_NAMESPACE_READERS_GROUP", "field-readers")
    assert config_module.WorkerConfig.from_env().namespace_readers_group == "field-readers"

    monkeypatch.setenv("DEMO_REGISTRY_NAMESPACE_READERS_GROUP", "none")
    assert config_module.WorkerConfig.from_env().namespace_readers_group == ""


def test_owner_tag_matches_the_tag_stamped_on_every_namespace():
    assert resources.namespace_owner_tag() == OWNER_TAG
    assert resources.namespace_tags(PROJECT)["owner"] == OWNER_TAG[1]


def test_workflow_ids():
    assert derive.namespace_readers_sync_workflow_id() == "namespace-readers/sync"
    assert (
        derive.namespace_read_access_workflow_id(" Michael.Jones@temporal.io ")
        == "namespace-readers/grant/michael.jones@temporal.io"
    )


def test_self_service_activity_explains_when_the_feature_is_off(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_NAMESPACE_READERS_GROUP", "off")

    result = readers_activities.grant_user_namespace_read_access(
        GrantNamespaceReadAccessRequest(email="michael.jones@temporal.io")
    )

    assert result.status == "not-configured"
    assert result.message


class RecordingClient:
    def __init__(self, error=None):
        self.starts = []
        self.error = error

    async def start_workflow(self, workflow, request, **kwargs):
        self.starts.append((workflow, request, kwargs))
        if self.error:
            raise self.error


def watch_config(monkeypatch, **env):
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return config_module.WorkerConfig.from_env()


def test_watch_starts_one_shared_sync_run(monkeypatch):
    from temporalio.common import WorkflowIDConflictPolicy

    client = RecordingClient()
    started = asyncio.run(watch.start_namespace_readers_sync(client, watch_config(monkeypatch)))

    assert started is True
    (_workflow, request, kwargs), = client.starts
    assert request.reason == "watch-activated"
    assert kwargs["id"] == "namespace-readers/sync"
    assert kwargs["task_queue"] == "registry-management-blue"
    assert kwargs["id_conflict_policy"] == WorkflowIDConflictPolicy.USE_EXISTING


def test_watch_retries_the_sync_start_on_the_next_pass_after_a_failure(monkeypatch):
    client = RecordingClient(error=RuntimeError("unavailable"))

    assert asyncio.run(watch.start_namespace_readers_sync(client, watch_config(monkeypatch))) is False


def test_watch_skips_the_sync_when_the_feature_is_off(monkeypatch):
    client = RecordingClient()
    config = watch_config(monkeypatch, DEMO_REGISTRY_NAMESPACE_READERS_GROUP="none")

    assert asyncio.run(watch.start_namespace_readers_sync(client, config)) is True
    assert client.starts == []
