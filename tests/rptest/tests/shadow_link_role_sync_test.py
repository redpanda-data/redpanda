# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from typing import Callable, Iterable

from connectrpc.errors import ConnectError, ConnectErrorCode

import google.protobuf.duration_pb2 as duration_pb2
import google.protobuf.field_mask_pb2 as field_mask_pb2

from ducktape.mark import parametrize
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    security_pb2,
    shadow_link_pb2,
)
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.multi_cluster_services import (
    SecondaryClusterArgs,
    SecondaryClusterSpec,
    ServiceType,
)
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.tests.admin_api_auth_test import create_user_and_wait  # type: ignore[reportUnknownVariableType]
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.tests.rbac_test_v2 import AdminV2RoleWrapper
from rptest.util import expect_timeout


# Matches roles_migrator::task_name in src/v/cluster_link/roles_migrator.h.
ROLES_MIGRATOR_TASK_NAME = "Roles Migrator Task"

ALICE = SaslCredentials("alice", "itsMeH0nest012", "SCRAM-SHA-256")
BOB = SaslCredentials("bob", "itsMeH0nest012", "SCRAM-SHA-256")


def _user(name: str) -> security_pb2.RoleMember:
    """Construct a user-type RoleMember."""
    return security_pb2.RoleMember(user=security_pb2.RoleUser(name=name))


def _group(name: str) -> security_pb2.RoleMember:
    """Construct a group-type RoleMember."""
    return security_pb2.RoleMember(group=security_pb2.RoleGroup(name=name))


def _flatten_member_names(members: Iterable[security_pb2.RoleMember]) -> set[str]:
    """Flatten RoleMember oneofs down to their bare names."""
    result: set[str] = set()
    for m in members:
        match m.WhichOneof("member"):
            case "user":
                result.add(m.user.name)
            case "group":
                result.add(m.group.name)
            case other:
                raise AssertionError(f"unexpected role member type {other!r}")
    return result


class RoleSyncTestBase(ShadowLinkTestBase):
    """Shared helpers for shadow-link role-sync tests. Holds no test methods so
    concrete subclasses with different topologies can reuse it without
    re-running each other's tests."""

    LINK = "role-sync-link"

    def setUp(self):
        super().setUp()
        self._dst = AdminV2RoleWrapper(AdminV2(self.target_cluster_service))

    def _create_link_with_role_sync(
        self,
        filters: list[shadow_link_pb2.NameFilter] | None = None,
        mutate_req: Callable[[shadow_link_pb2.CreateShadowLinkRequest], None]
        | None = None,
    ) -> None:
        req = self.create_default_link_request(self.LINK)
        if filters is None:
            filters = [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="synced-",
                )
            ]
        opts = shadow_link_pb2.RoleSyncOptions(
            interval=duration_pb2.Duration(seconds=1),
            paused=False,
            role_name_filters=filters,
        )
        req.shadow_link.configurations.role_sync_options.CopyFrom(opts)
        if mutate_req is not None:
            mutate_req(req)
        self.create_link_with_request(req)

    def _dst_role_members(self, role: str) -> set[str]:
        try:
            members = self._dst.get_role(role).members
        except ConnectError as e:
            assert e.code == ConnectErrorCode.NOT_FOUND, (
                f"unexpected error fetching role {role!r}: {e}"
            )
            return set()
        return _flatten_member_names(members)

    def _set_role_sync_paused(self, paused: bool) -> None:
        link = self.get_link(self.LINK)
        link.configurations.role_sync_options.paused = paused
        self.update_link(
            shadow_link=link,
            update_mask=field_mask_pb2.FieldMask(
                paths=["configurations.role_sync_options.paused"]
            ),
        )

    def _roles_task(self):
        for task in self.get_link(self.LINK).status.task_statuses:
            if task.name == ROLES_MIGRATOR_TASK_NAME:
                return task
        return None

    def _roles_task_state(self):
        task = self._roles_task()
        return task.state if task is not None else None


class ShadowLinkRoleSyncTest(RoleSyncTestBase):
    """End-to-end test for the shadow link roles migrator (plaintext source)."""

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(),
        )

    def setUp(self):
        super().setUp()
        self._src = AdminV2RoleWrapper(AdminV2(self.source_cluster_service))

    @cluster(num_nodes=6)
    def test_role_sync_full_mirror(self):
        """
        Verify full mirror lifecycle: initial sync of users and groups,
        membership update, out-of-scope exclusion, deletion, and the
        pause/resume cycle (including a member removal made while paused).
        """
        # Roles on the source. "synced-keep" is in scope (prefix "synced-") and
        # carries both a user and a group member; it survives to drive the
        # pause/resume sequence below. "synced-doomed" is in scope and exercises
        # deletion. "excluded-role" is out of scope.
        self._src.create_role(
            role="synced-keep", members=[_user("u1"), _group("synced-group")]
        )
        self._src.create_role(role="synced-doomed", members=[_user("u4")])
        self._src.create_role(role="excluded-role", members=[_user("u2")])

        self._create_link_with_role_sync()

        # Initial mirror: both in-scope roles, with their full membership
        # (users and groups), appear on the destination.
        def initial_mirror_complete() -> bool:
            return self._dst_role_members("synced-keep") == {
                "u1",
                "synced-group",
            } and self._dst_role_members("synced-doomed") == {"u4"}

        wait_until(
            initial_mirror_complete,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="initial role state did not mirror to destination",
        )

        # Out-of-scope role must never appear. The positive checks above prove a
        # full sync cycle completed, so excluded-role's absence here means
        # "excluded by the filter" rather than merely "not synced yet".
        assert "excluded-role" not in self._dst.list_role_names(), (
            "excluded-role (out-of-scope) should not be mirrored to destination"
        )

        # Membership addition propagates.
        self._src.add_role_members(role="synced-keep", members=[_user("u3")])
        wait_until(
            lambda: (
                self._dst_role_members("synced-keep") == {"u1", "u3", "synced-group"}
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="membership addition did not propagate",
        )

        # Role deletion propagates.
        self._src.delete_role("synced-doomed", delete_acls=False)
        wait_until(
            lambda: "synced-doomed" not in self._dst.list_role_names(),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="role deletion did not propagate",
        )

        # Pausing role sync parks the roles migrator task in the paused state.
        self._set_role_sync_paused(True)
        wait_until(
            lambda: self._roles_task_state() == shadow_link_pb2.TASK_STATE_PAUSED,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="roles migrator did not pause after role sync was paused",
        )

        # A removal made while paused must not propagate. The migrator is
        # confirmed parked above; poll for the removal across several sync
        # intervals and require that it never lands. A timeout here is the
        # success case: the forbidden state was never observed.
        self._src.remove_role_members(
            role="synced-keep", members=[_group("synced-group")]
        )
        with expect_timeout():
            wait_until(
                lambda: "synced-group" not in self._dst_role_members("synced-keep"),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Resuming the link should allow the removal to propagate.
        self._set_role_sync_paused(False)
        wait_until(
            lambda: self._roles_task_state() == shadow_link_pb2.TASK_STATE_ACTIVE,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="roles migrator did not resume after role sync was unpaused",
        )
        wait_until(
            lambda: "synced-group" not in self._dst_role_members("synced-keep"),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="member removal made while paused did not propagate on resume",
        )

    @cluster(num_nodes=6)
    def test_reconcile_authority_is_scoped(self):
        """The reconcile is full-replace authoritative, but only over in-scope
        roles. For an in-scope role the destination is driven back to the
        source: a member added directly on the destination is stripped and a
        role deleted on the destination is recreated. A destination role
        outside the filter scope, with no source counterpart, is left
        untouched."""
        self._src.create_role(
            role="synced-role", members=[_user("u1"), _group("synced-group")]
        )
        # "unmanaged-" is outside the default "synced-" include filter, so the
        # migrator never selects it on either side.
        self._dst.create_role(role="unmanaged-role", members=[_user("local-admin")])
        self._create_link_with_role_sync()
        wait_until(
            lambda: self._dst_role_members("synced-role") == {"u1", "synced-group"},
            timeout_sec=30,
            backoff_sec=1,
            err_msg="initial role state did not mirror to destination",
        )

        # A member added directly on the destination is stripped on reconcile.
        self._dst.add_role_members(role="synced-role", members=[_user("drift-member")])
        wait_until(
            lambda: self._dst_role_members("synced-role") == {"u1", "synced-group"},
            timeout_sec=30,
            backoff_sec=1,
            err_msg="foreign member added on destination was not reverted",
        )

        # A role deleted on the destination is recreated from the source.
        self._dst.delete_role("synced-role", delete_acls=False)
        wait_until(
            lambda: self._dst_role_members("synced-role") == {"u1", "synced-group"},
            timeout_sec=30,
            backoff_sec=1,
            err_msg="role deleted on destination was not recreated",
        )

        # The reverts above are observed reconcile cycles that ran while
        # "unmanaged-role" existed, so its survival here is "left alone by the
        # filter" rather than "not yet reconciled".
        assert "unmanaged-role" in self._dst.list_role_names(), (
            "out-of-scope destination role was deleted by the migrator"
        )
        assert self._dst_role_members("unmanaged-role") == {"local-admin"}, (
            "out-of-scope destination role's membership was altered by the migrator"
        )


class ShadowLinkRoleSyncKafkaSourceTest(RoleSyncTestBase):
    """Role sync degrades gracefully when the source is an Apache Kafka cluster
    that does not implement DescribeRedpandaRoles (Kafka API key 15000)."""

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(),
        )

    def get_source_cluster_spec(self) -> SecondaryClusterSpec:
        return SecondaryClusterSpec(
            ServiceType.KAFKA,
            kafka_version="3.8.0",
            kafka_quorum="COMBINED_KRAFT",
        )

    @cluster(num_nodes=6)
    def test_roles_park_but_link_still_syncs_topics(self):
        # No RBAC roles exist on an Apache Kafka source; the roles task cannot
        # negotiate the custom DescribeRedpandaRoles API (Kafka API key 15000)
        # and parks. The rest of the link rides standard Kafka APIs, so it must
        # keep working: one task parking is isolated from its siblings.
        topic = TopicSpec(name="mirror-topic", partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic)

        self._create_link_with_role_sync()

        def parked() -> bool:
            task = self._roles_task()
            return (
                task is not None
                and task.state == shadow_link_pb2.TASK_STATE_LINK_UNAVAILABLE
            )

        wait_until(
            parked,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="roles task did not park LINK_UNAVAILABLE against a Kafka source",
        )
        task = self._roles_task()
        assert task is not None and task.reason, (
            "expected a non-empty reason explaining the unsupported API"
        )
        assert task.state != shadow_link_pb2.TASK_STATE_FAULTED, (
            "roles task must park (LINK_UNAVAILABLE), not fault, on an unsupported source"
        )
        # Nothing was mirrored.
        assert not [
            n for n in self._dst.list_role_names() if n.startswith("synced-")
        ], "no roles should be mirrored from a Kafka source"

        # The parked roles task does not impair the data plane: a topic created
        # on the Kafka source still syncs to the target over standard Kafka APIs.
        wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="topic did not sync to target while the roles task was parked",
        )

        # Topic sync completing did not un-park or fault the roles task.
        assert parked(), "roles task should remain parked after topic sync"


class ShadowLinkRoleSyncAuthTest(RoleSyncTestBase):
    """Role sync with SASL/SCRAM on both clusters: functional authorization and
    the source-permission boundary."""

    SUPERUSER_LINK = "cluster-link-user"
    SUPERUSER_LINK_PW = "cluster-link-password"

    def __init__(self, test_context: TestContext):
        security = SecurityConfig()
        security.enable_sasl = True
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            security=security,
            secondary_cluster_args=SecondaryClusterArgs(security=security),
        )

    def _source_superuser_rpk(self) -> RpkTool:
        su = self.redpanda.SUPERUSER_CREDENTIALS
        return RpkTool(
            self.source_cluster_service,
            username=su.username,
            password=su.password,
            sasl_mechanism=su.mechanism,
        )

    def _add_link_scram_creds(
        self,
        req: shadow_link_pb2.CreateShadowLinkRequest,
        username: str,
        password: str,
    ) -> None:
        req.shadow_link.configurations.client_options.authentication_configuration.scram_configuration.CopyFrom(
            shadow_link_pb2.ScramConfig(
                username=username,
                password=password,
                scram_mechanism=shadow_link_pb2.SCRAM_MECHANISM_SCRAM_SHA_256,
            )
        )

    def setUp(self):
        super().setUp()
        su = self.redpanda.SUPERUSER_CREDENTIALS
        # Under SASL, the base's unauthenticated target clients fail. Rebind the
        # shadow-link service client and the destination role wrapper to a
        # superuser-authenticated AdminV2 (see Global Constraints).
        self.admin_v2 = AdminV2(
            self.target_cluster_service, auth=(su.username, su.password)
        )
        self.service_client = self.admin_v2.shadow_link()
        self._dst = AdminV2RoleWrapper(self.admin_v2)
        # _src talks Admin v2 to the source as the source superuser.
        self._src = AdminV2RoleWrapper(
            AdminV2(self.source_cluster_service, auth=(su.username, su.password))
        )
        # A privileged link principal on the source (added to superusers so it
        # can enumerate roles and ACLs).
        self._source_superuser_rpk().sasl_create_user(
            self.SUPERUSER_LINK, self.SUPERUSER_LINK_PW
        )
        self.source_cluster_service.set_cluster_config(
            {"superusers": [su.username, self.SUPERUSER_LINK]}
        )

    def _target_visible_topics(self, creds: SaslCredentials) -> set[str]:
        rpk = RpkTool(
            self.target_cluster_service,
            username=creds.username,
            password=creds.password,
            sasl_mechanism=creds.algorithm,
        )
        try:
            return set(rpk.list_topics())
        except Exception:
            return set()

    @cluster(num_nodes=6)
    def test_functional_authz_end_to_end(self):
        topic = "authz-topic"
        su_rpk = self._source_superuser_rpk()

        # Pre-provision the member identities on the DESTINATION (credentials do
        # not sync; they model operator-provisioned DR identities).
        target_su = self.redpanda.SUPERUSER_CREDENTIALS
        target_admin = Admin(
            self.target_cluster_service,
            auth=(target_su.username, target_su.password),
        )
        create_user_and_wait(self.target_cluster_service, target_admin, ALICE)
        create_user_and_wait(self.target_cluster_service, target_admin, BOB)

        # SOURCE: topic, in-scope role with ALICE as a member, and an ACL bound
        # to the role principal granting DESCRIBE on the topic.
        su_rpk.create_topic(topic)
        self._src.create_role(role="synced-role", members=[_user(ALICE.username)])
        su_rpk.sasl_allow_principal(
            "RedpandaRole:synced-role", ["describe"], "topic", topic
        )

        # Link with role sync + ACL sync (security_sync is on by default in
        # create_default_link_request) + SCRAM client creds.
        self._create_link_with_role_sync(
            mutate_req=lambda req: self._add_link_scram_creds(
                req, self.SUPERUSER_LINK, self.SUPERUSER_LINK_PW
            )
        )

        # Role membership mirrors to the destination.
        wait_until(
            lambda: self._dst_role_members("synced-role") == {ALICE.username},
            timeout_sec=60,
            backoff_sec=1,
            err_msg="role membership did not mirror to destination",
        )

        # The synced role + role-bound ACL authorize the member, not a non-member.
        # ALICE (member) gains topic visibility once role + ACL have both synced.
        wait_until(
            lambda: topic in self._target_visible_topics(ALICE),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="member ALICE was not authorized via the synced role + ACL",
        )
        # BOB (non-member) is authenticated but must NOT see the ACL-protected
        # topic. Probe directly (not via the exception-swallowing helper) so a
        # transient rpk failure surfaces as an error rather than a vacuous
        # "denied" pass.
        bob_rpk = RpkTool(
            self.target_cluster_service,
            username=BOB.username,
            password=BOB.password,
            sasl_mechanism=BOB.algorithm,
        )
        assert topic not in set(bob_rpk.list_topics()), (
            "non-member BOB must not be authorized by the synced role"
        )

    @cluster(num_nodes=6)
    def test_permission_grant_activates_sync(self):
        limited_user = "limited-link-user"
        limited_pw = "limited-link-password"
        su_rpk = self._source_superuser_rpk()

        # An in-scope role exists on the source, waiting to sync.
        self._src.create_role(role="synced-role", members=[_user("u1")])

        # A non-superuser link principal that lacks cluster DESCRIBE on the source.
        su_rpk.sasl_create_user(limited_user, limited_pw)

        self._create_link_with_role_sync(
            mutate_req=lambda req: self._add_link_scram_creds(
                req, limited_user, limited_pw
            )
        )

        # Without DESCRIBE the migrator cannot enumerate roles -> LINK_UNAVAILABLE.
        wait_until(
            lambda: (
                self._roles_task_state() == shadow_link_pb2.TASK_STATE_LINK_UNAVAILABLE
            ),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="roles task did not park LINK_UNAVAILABLE without permission",
        )
        assert "synced-role" not in self._dst.list_role_names(), (
            "nothing should sync while the link principal lacks permission"
        )
        task = self._roles_task()
        assert task is not None and task.reason, "expected a non-empty reason"

        # Grant cluster DESCRIBE on the source; the task recovers and mirrors.
        su_rpk.acl_create_allow_cluster(username=limited_user, op="describe")
        wait_until(
            lambda: self._roles_task_state() == shadow_link_pb2.TASK_STATE_ACTIVE,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="roles task did not become ACTIVE after the permission grant",
        )
        wait_until(
            lambda: self._dst_role_members("synced-role") == {"u1"},
            timeout_sec=60,
            backoff_sec=1,
            err_msg="role did not mirror after the permission grant",
        )


class ShadowLinkRoleSyncScaleTest(RoleSyncTestBase):
    """Scale characterization for the roles migrator. Each parametrized point
    runs the full create -> update -> delete lifecycle (exercising all three
    migrator apply loops at volume) while pushing one cost axis: role count,
    total membership, or a single role's member set.
    """

    PREFIX = "synced-"

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            secondary_cluster_args=SecondaryClusterArgs(),
        )

    def setUp(self):
        super().setUp()
        self._src = AdminV2RoleWrapper(AdminV2(self.source_cluster_service))

    def _retry_transient(
        self, fn: Callable[..., object], *args: object, **kwargs: object
    ) -> None:
        """Run a source mutation, retrying the transient controller-backpressure
        errors -- raft not_leader/timeout, surfaced as a ConnectError 'internal'
        -- that thousands of rapid role mutations can provoke. Other error codes
        and the final failure propagate."""
        last: ConnectError | None = None
        for attempt in range(6):
            try:
                fn(*args, **kwargs)
                return
            except ConnectError as e:
                if e.code != ConnectErrorCode.INTERNAL:
                    raise
                last = e
                self.logger.debug(f"retrying transient source mutation: {e}")
                time.sleep(0.5 * (attempt + 1))
        assert last is not None
        raise last

    def _seed_source_roles(
        self, num_roles: int, members_per_role: int
    ) -> dict[str, set[str]]:
        """Create num_roles in-scope roles on the source, each with
        members_per_role distinct user members; return the created
        {role name -> member names} map. Members are free-form principal names
        that need no backing user accounts -- the migrator mirrors them as
        data."""
        created: dict[str, set[str]] = {}
        for i in range(num_roles):
            name = f"{self.PREFIX}role-{i}"
            member_names = {f"u-{i}-{m}" for m in range(members_per_role)}
            self._retry_transient(
                self._src.create_role,
                role=name,
                members=[_user(n) for n in member_names],
            )
            created[name] = member_names
        return created

    def _dst_roles_converged(self, phase: str, want: dict[str, set[str]]) -> bool:
        """One poll of the destination: true iff the synced-role map equals
        want."""
        got = {
            r.name: _flatten_member_names(r.members)
            for r in self._dst.list_roles()
            if r.name.startswith(self.PREFIX)
        }
        if got == want:
            return True
        missing_roles = want.keys() - got.keys()
        unexpected_roles = got.keys() - want.keys()
        roles_with_wrong_members = {
            role for role in want.keys() & got.keys() if got[role] != want[role]
        }
        self.logger.debug(
            f"role convergence ({phase}): "
            f"missing_roles={len(missing_roles)}, "
            f"unexpected_roles={len(unexpected_roles)}, "
            f"roles_with_wrong_members={len(roles_with_wrong_members)}"
        )
        return False

    @cluster(num_nodes=6)
    @parametrize(num_roles=5000, members_per_role=1)
    @parametrize(num_roles=50, members_per_role=100)
    @parametrize(num_roles=5, members_per_role=1000)
    def test_role_sync_at_scale(self, num_roles: int, members_per_role: int) -> None:
        # create + update + delete each write ~1 controller record per role, on
        # both the source (seed/modify/delete) and the target (mirror), so each
        # controller log grows by ~3*num_roles over the lifecycle. Raise the
        # guard above its 1000 default to admit that while still catching runaway
        # writes.
        max_controller_records = 1000 + num_roles * 3
        self.redpanda.set_expected_controller_records(max_controller_records)
        self.source_cluster_service.set_expected_controller_records(
            max_controller_records
        )

        mirror_timeout_sec = 240

        # CREATE: seed the source, then mirror it to the destination.
        seed_start = time.time()
        expected = self._seed_source_roles(num_roles, members_per_role)
        self.logger.info(
            f"seeded {num_roles} source roles x {members_per_role} members "
            f"in {time.time() - seed_start:.1f}s"
        )
        create_start = time.time()
        self._create_link_with_role_sync()
        wait_until(
            lambda: self._dst_roles_converged("create", expected),
            timeout_sec=mirror_timeout_sec,
            backoff_sec=2,
            err_msg=f"{num_roles} roles did not fully mirror within "
            f"{mirror_timeout_sec}s",
        )
        self.logger.info(
            f"role sync created {num_roles} roles in {time.time() - create_start:.1f}s"
        )

        # UPDATE: change every role's membership -- drop one member from each --
        # so all num_roles roles flow through the update apply loop.
        update_start = time.time()
        for i in range(num_roles):
            self._retry_transient(
                self._src.remove_role_members,
                role=f"{self.PREFIX}role-{i}",
                members=[_user(f"u-{i}-0")],
            )
        updated = {
            f"{self.PREFIX}role-{i}": {f"u-{i}-{m}" for m in range(1, members_per_role)}
            for i in range(num_roles)
        }
        wait_until(
            lambda: self._dst_roles_converged("update", updated),
            timeout_sec=mirror_timeout_sec,
            backoff_sec=2,
            err_msg=f"membership update did not mirror for {num_roles} roles",
        )
        self.logger.info(
            f"role sync updated {num_roles} roles in {time.time() - update_start:.1f}s"
        )

        # DELETE: drop every source role and confirm the destination is pruned.
        delete_start = time.time()
        for name in expected:
            self._retry_transient(self._src.delete_role, name, delete_acls=False)
        wait_until(
            lambda: self._dst_roles_converged("delete", {}),
            timeout_sec=mirror_timeout_sec,
            backoff_sec=2,
            err_msg=f"{num_roles} roles were not pruned from the destination",
        )
        self.logger.info(
            f"role sync deleted {num_roles} roles in {time.time() - delete_start:.1f}s"
        )

        # The full lifecycle ran fault-free -> the task settles ACTIVE.
        wait_until(
            lambda: self._roles_task_state() == shadow_link_pb2.TASK_STATE_ACTIVE,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="roles task did not settle ACTIVE after the lifecycle",
        )
