# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import random
from collections.abc import Iterable

from connectrpc.errors import ConnectError, ConnectErrorCode

import ducktape.errors
from ducktape.mark import parametrize, ignore
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    security_pb2,
)
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.rpk import RpkException, RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception, wait_until_result

ALICE = SaslCredentials("alice", "itsMeH0nest012", "SCRAM-SHA-256")
BOB = SaslCredentials("bob", "itsMeH0nest012", "SCRAM-SHA-256")


def expect_role_error(connected_error_code: ConnectErrorCode):
    return expect_exception(
        ConnectError,
        lambda e: e.code == connected_error_code,
    )


class AdminV2RoleWrapper:
    """
    Simple convenience wrapper for the generated Admin V2 Security Client.
    """

    def __init__(self, admin: AdminV2):
        self.admin = admin

    def create_role(
        self, role: str, members: Iterable[security_pb2.RoleMember] | None = None
    ) -> security_pb2.Role:
        role = security_pb2.Role(name=role, members=members)
        res = self.admin.security().create_role(
            security_pb2.CreateRoleRequest(role=role)
        )
        return res.role

    def get_role(self, role: str) -> security_pb2.Role:
        res = self.admin.security().get_role(security_pb2.GetRoleRequest(name=role))
        return res.role

    def list_role_members(self, role: str) -> list[security_pb2.RoleMember]:
        res = self.admin.security().get_role(security_pb2.GetRoleRequest(name=role))
        return res.role.members

    def add_role_members(
        self, role: str, members: Iterable[security_pb2.RoleMember]
    ) -> security_pb2.Role:
        res = self.admin.security().add_role_members(
            security_pb2.AddRoleMembersRequest(role_name=role, members=members)
        )
        return res.role

    def remove_role_members(
        self, role: str, members: Iterable[security_pb2.RoleMember]
    ) -> security_pb2.Role:
        res = self.admin.security().remove_role_members(
            security_pb2.RemoveRoleMembersRequest(role_name=role, members=members)
        )
        return res.role

    def delete_role(self, role: str, delete_acls: bool = False):
        self.admin.security().delete_role(
            security_pb2.DeleteRoleRequest(name=role, delete_acls=delete_acls)
        )

    def list_roles(self) -> list[security_pb2.Role]:
        res = self.admin.security().list_roles(security_pb2.ListRolesRequest())
        return res.roles

    def list_role_names(self) -> list[str]:
        res = self.admin.security().list_roles(security_pb2.ListRolesRequest())
        return [role.name for role in res.roles]

    def list_current_user_roles(self) -> list[str]:
        res = self.admin.security().list_current_user_roles(
            security_pb2.ListCurrentUserRolesRequest()
        )
        return res.roles

    def role_exists(self, role_name: str) -> bool:
        roles = self.list_roles()
        return any(role.name == role_name for role in roles)


class RBACTestBase(RedpandaTest):
    PASSWORD = "password012345"
    ALGORITHM = "SCRAM-SHA-256"
    ROLE_NAME0 = "foo"
    ROLE_NAME1 = "bar"
    ROLE_NAME2 = "baz"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.rpk = RpkTool(self.redpanda)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = AdminV2RoleWrapper(
            AdminV2(
                self.redpanda, auth=(self.superuser.username, self.superuser.password)
            )
        )

        self.user_admin = AdminV2RoleWrapper(
            AdminV2(self.redpanda, auth=(ALICE.username, ALICE.password))
        )

    def setUp(self):
        super().setUp()
        # TODO: Replace with v2 Admin client calls once user management is implemented
        v1_admin = Admin(
            self.redpanda, auth=(self.superuser.username, self.superuser.password)
        )
        create_user_and_wait(self.redpanda, v1_admin, ALICE)

        self.redpanda.set_cluster_config({"admin_api_require_auth": True})


class RBACTest(RBACTestBase):
    def _role_exists(self, target_role: str):
        roles = self.superuser_admin.list_roles()
        return any(target_role == role.name for role in roles)

    def _create_and_wait_for_role(self, role: str):
        self.superuser_admin.create_role(role=role)
        wait_until(
            lambda: self._role_exists(role),
            timeout_sec=10,
            backoff_sec=2,
            err_msg="Role was not created",
        )

    def _set_of_role_names(self):
        roles = self.superuser_admin.list_roles()
        return set(role.name for role in roles)

    @cluster(num_nodes=3)
    def test_superuser_access(self):
        # a superuser may access the RBAC API
        roles = self.superuser_admin.list_roles()
        assert len(roles) == 0, "Unexpected roles"

        with expect_role_error(ConnectErrorCode.NOT_FOUND):
            self.superuser_admin.get_role(role=self.ROLE_NAME0)

        # TODO: Update once v2 list_roles supports filters
        roles = self.superuser_admin.list_roles()
        assert len(roles) == 0, "Unexpected roles"

        roles = self.superuser_admin.list_current_user_roles()
        assert len(roles) == 0, "Unexpected user roles"

        self.superuser_admin.delete_role(role=self.ROLE_NAME1)

    @cluster(num_nodes=3)
    def test_regular_user_access(self):
        # a regular user may NOT access the RBAC API

        with expect_role_error(ConnectErrorCode.PERMISSION_DENIED):
            self.user_admin.list_roles()

        with expect_role_error(ConnectErrorCode.PERMISSION_DENIED):
            self.user_admin.create_role(role=self.ROLE_NAME0)

        with expect_role_error(ConnectErrorCode.PERMISSION_DENIED):
            self.user_admin.get_role(role=self.ROLE_NAME0)

        with expect_role_error(ConnectErrorCode.PERMISSION_DENIED):
            self.user_admin.add_role_members(
                role=self.ROLE_NAME1,
                members=[
                    security_pb2.RoleMember(
                        user=security_pb2.RoleUser(name=ALICE.username)
                    )
                ],
            )

        with expect_role_error(ConnectErrorCode.PERMISSION_DENIED):
            self.user_admin.remove_role_members(
                role=self.ROLE_NAME1,
                members=[
                    security_pb2.RoleMember(
                        user=security_pb2.RoleUser(name=ALICE.username)
                    )
                ],
            )

        with expect_role_error(ConnectErrorCode.PERMISSION_DENIED):
            self.user_admin.list_role_members(role=self.ROLE_NAME1)

        with expect_role_error(ConnectErrorCode.PERMISSION_DENIED):
            self.user_admin.delete_role(role=self.ROLE_NAME1)

    @cluster(num_nodes=3)
    def test_create_role(self):
        self.logger.debug("Test that simple create_role succeeds")
        created_role = self.superuser_admin.create_role(role=self.ROLE_NAME0)
        assert created_role.name == self.ROLE_NAME0, (
            f"Incorrect create role response: {created_role}"
        )

        wait_until(
            lambda: self._set_of_role_names() == {self.ROLE_NAME0},
            timeout_sec=10,
            backoff_sec=2,
            err_msg="Role was not created",
        )

        self.logger.debug("Also test idempotency of create_role")
        created_role = self.superuser_admin.create_role(role=self.ROLE_NAME0)
        assert created_role.name == self.ROLE_NAME0, (
            f"Incorrect create role response: {created_role}"
        )

    @cluster(num_nodes=3)
    def test_invalid_create_role(self):
        self.logger.debug("Test that create_role rejects an empty/default role")
        with expect_role_error(ConnectErrorCode.INVALID_ARGUMENT):
            self.superuser_admin.create_role(role="")

        # Two ordinals (corresponding to ',' and '=') are explicitly excluded from role names
        self.logger.debug("Test that create_role rejects invalid role names")
        for ordinal in [0x2C, 0x3D]:
            invalid_rolename = f"john{chr(ordinal)}doe"

            with expect_role_error(ConnectErrorCode.INVALID_ARGUMENT):
                self.superuser_admin.create_role(role=invalid_rolename)

        self.logger.debug("Test that create_role rejects invalid member names")
        invalid_member = security_pb2.RoleMember(
            user=security_pb2.RoleUser(name="alice\nsmith")
        )

        with expect_role_error(ConnectErrorCode.INVALID_ARGUMENT):
            self.superuser_admin.create_role(
                role=self.ROLE_NAME0, members=[invalid_member]
            )

    # TODO: Add test_list_role_filter_v2 once v2 list_roles supports filters
    @ignore
    @cluster(num_nodes=3)
    def test_list_role_filter(self):
        pass

    @cluster(num_nodes=3)
    def test_get_role(self):
        alice = security_pb2.RoleMember(user=security_pb2.RoleUser(name=ALICE.username))
        group0 = security_pb2.RoleMember(group=security_pb2.RoleGroup(name="group0"))

        self.logger.debug("Test that get_role rejects an unknown role")
        with expect_role_error(ConnectErrorCode.NOT_FOUND):
            self.superuser_admin.get_role(role=self.ROLE_NAME0)

        self.logger.debug("Test that get_role succeeds with an existing role")
        self._create_and_wait_for_role(role=self.ROLE_NAME1)

        def get_role_succeeds(role_name: str, expected_members: set[str] = set()):
            try:
                role = self.superuser_admin.get_role(role=role_name)
                actual_members = set()
                for m in role.members:
                    if m.WhichOneof("member") == "user":
                        actual_members.add(m.user.name)
                    elif m.WhichOneof("member") == "group":
                        actual_members.add(m.group.name)

                return role.name == role_name and actual_members == expected_members
            except ConnectError as e:
                assert e.code == ConnectErrorCode.NOT_FOUND, (
                    f"Unexpected error while waiting for get_role to succeed: {e}"
                )
                return False

        wait_until(
            lambda: get_role_succeeds(self.ROLE_NAME1),
            timeout_sec=10,
            backoff_sec=2,
            err_msg="Get role hasn't succeeded in time",
        )

        self.logger.debug(
            "Test that get_role succeeds with an existing role that has members"
        )
        self.superuser_admin.add_role_members(
            role=self.ROLE_NAME1,
            members=[alice, group0],
        )

        wait_until(
            lambda: get_role_succeeds(
                self.ROLE_NAME1, expected_members={alice.user.name, group0.group.name}
            ),
            timeout_sec=10,
            backoff_sec=2,
            err_msg="Get role hasn't succeeded in time",
        )

    @cluster(num_nodes=3)
    def test_delete_role(self):
        self.logger.debug("Test that delete_role succeeds with existing role")
        self._create_and_wait_for_role(role=self.ROLE_NAME0)

        self.superuser_admin.delete_role(role=self.ROLE_NAME0)

        wait_until(
            lambda: not self._role_exists(self.ROLE_NAME0),
            timeout_sec=5,
            backoff_sec=0.5,
        )

        self.logger.debug(
            "Test that delete_role succeeds with non-existing role for idempotency"
        )
        self.superuser_admin.delete_role(role=self.ROLE_NAME0)

    @cluster(num_nodes=3)
    def test_member_operations(self):
        alice = security_pb2.RoleMember(user=security_pb2.RoleUser(name=ALICE.username))
        group0 = security_pb2.RoleMember(group=security_pb2.RoleGroup(name="group0"))
        bob = security_pb2.RoleMember(user=security_pb2.RoleUser(name=BOB.username))
        group1 = security_pb2.RoleMember(group=security_pb2.RoleGroup(name="group1"))

        self.logger.debug("Test that create_role can create the role with members.")
        created_role = self.superuser_admin.create_role(
            role=self.ROLE_NAME0,
            members=[alice, group0],
        )
        assert created_role.name == self.ROLE_NAME0, (
            f"Incorrect role name: {created_role.name}"
        )
        assert len(created_role.members) == 2, (
            f"Incorrect 'number of members': {created_role.members}"
        )
        assert alice in created_role.members, (
            f'Missing member "{alice.user.name}" in {created_role.members}'
        )
        assert group0 in created_role.members, (
            f'Missing member "{group0.group.name}" in {created_role.members}'
        )

        self.logger.debug("And check that we can query the role we created")
        members = wait_until_result(
            lambda: self.superuser_admin.list_role_members(role=self.ROLE_NAME0),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )
        assert members is not None, "Failed to get members for newly created role"

        assert len(members) == 2, f"Unexpected members list: {members}"
        assert alice in members, f'Missing member "{alice.user.name}" in {members}'
        assert group0 in members, f'Missing member "{group0.group.name}" in {members}'

        self.logger.debug("Now add a new member to the role")
        res = self.superuser_admin.add_role_members(
            role=self.ROLE_NAME0, members=[bob, group1]
        )

        member_update = res.members
        assert len(member_update) == 4, (
            f"Updated role members should have 4 members, got: {member_update}"
        )
        assert bob in member_update, (
            f"Updated role members {member_update} should include member {bob}"
        )
        assert group1 in member_update, (
            f"Updated role members {member_update} should include member {group1}"
        )

        def until_members(
            role,
            expected: list[security_pb2.RoleMember] = [],
            excluded: list[security_pb2.RoleMember] = [],
        ):
            members = self.superuser_admin.list_role_members(role=role)
            exp = all(m in members for m in expected)
            excl = not any(m in members for m in excluded)
            return exp and excl, members

        self.logger.debug(
            "And verify that the members list eventually reflects that change"
        )
        members = wait_until_result(
            lambda: until_members(
                self.ROLE_NAME0, expected=[alice, bob, group0, group1]
            ),
            timeout_sec=5,
            backoff_sec=1,
            retry_on_exc=True,
        )

        assert members is not None, "Failed to get members"
        for m in [bob, alice, group0, group1]:
            assert m in members, f"Missing member {m}, got: {members}"

        self.logger.debug("Remove a member from the role")
        res = self.superuser_admin.remove_role_members(
            role=self.ROLE_NAME0,
            members=[alice, group0],
        )
        member_update = res.members

        assert len(member_update) == 2, (
            f"Updated role members should have 2 members, got: {member_update}"
        )
        assert alice not in member_update, (
            f"Expected {alice} to be removed, got {member_update}"
        )
        assert group0 not in member_update, (
            f"Expected {group0} to be removed, got {member_update}"
        )

        self.logger.debug(
            "And verify that the members list eventually reflects the removal"
        )
        members = wait_until_result(
            lambda: until_members(
                self.ROLE_NAME0, expected=[bob, group1], excluded=[alice, group0]
            ),
            timeout_sec=5,
            backoff_sec=1,
            retry_on_exc=True,
        )

        assert members is not None
        assert len(members) == 2, f"Unexpected member: {members}"
        assert alice not in members, f"Unexpected member {alice}, got: {members}"
        assert group0 not in members, f"Unexpected member {group0}, got: {members}"

        self.logger.debug("Test add_role_member idempotency - no-op add should succeed")
        res = self.superuser_admin.add_role_members(
            role=self.ROLE_NAME0,
            members=[bob, group1],
        )
        member_update = res.members
        assert len(member_update) == 2, f"Unexpectedly members: {member_update}"

        self.logger.debug(
            "Test remove_role_member idempotency - no-op remove should succeed"
        )
        res = self.superuser_admin.remove_role_members(
            role=self.ROLE_NAME0,
            members=[alice, group0],
        )
        member_update = res.members
        assert len(member_update) == 2, f"Unexpectedly members: {member_update}"

    @cluster(num_nodes=3)
    def test_member_operations_errors(self):
        alice = security_pb2.RoleMember(user=security_pb2.RoleUser(name=ALICE.username))

        with expect_role_error(ConnectErrorCode.NOT_FOUND):
            self.superuser_admin.list_role_members(role=self.ROLE_NAME0)

        self.logger.debug(
            "NOT_FOUND for add/remove_role_members on a non-existing role"
        )
        with expect_role_error(ConnectErrorCode.NOT_FOUND):
            self.superuser_admin.add_role_members(role=self.ROLE_NAME0, members=[alice])

        with expect_role_error(ConnectErrorCode.NOT_FOUND):
            self.superuser_admin.remove_role_members(
                role=self.ROLE_NAME0, members=[alice]
            )

        self.logger.debug("Check that errored update has no effect")
        with expect_role_error(ConnectErrorCode.NOT_FOUND):
            self.superuser_admin.list_role_members(role=self.ROLE_NAME0)

        self.superuser_admin.create_role(role=self.ROLE_NAME0)

        wait_until(
            lambda: len(self.superuser_admin.list_role_members(role=self.ROLE_NAME0))
            == 0,
            timeout_sec=5,
            backoff_sec=1,
            retry_on_exc=True,
        )

        self.logger.debug("Test that add_role_members rejects invalid member names")
        invalid_member = security_pb2.RoleMember(
            user=security_pb2.RoleUser(name="alice\nsmith")
        )

        with expect_role_error(ConnectErrorCode.INVALID_ARGUMENT):
            self.superuser_admin.add_role_members(
                role=self.ROLE_NAME0, members=[invalid_member]
            )

        self.logger.debug("Test that remove_role_members rejects invalid member names")
        with expect_role_error(ConnectErrorCode.INVALID_ARGUMENT):
            self.superuser_admin.remove_role_members(
                role=self.ROLE_NAME0, members=[invalid_member]
            )

        self.logger.debug("A valid raw request")
        _ = self.superuser_admin.add_role_members(
            role=self.ROLE_NAME0,
            members=[security_pb2.RoleMember(user=security_pb2.RoleUser(name="foo"))],
        )

    @cluster(num_nodes=3)
    def test_list_user_roles(self):
        username = ALICE.username
        alice = security_pb2.RoleMember(user=security_pb2.RoleUser(name=username))

        role_names = self.user_admin.list_current_user_roles()
        assert len(role_names) == 0, "Unexpected roles for user"

        _ = self.superuser_admin.create_role(
            role=self.ROLE_NAME0,
            members=[alice],
        )

        _ = self.superuser_admin.create_role(role=self.ROLE_NAME1, members=[alice])

        def list_roles(n_expected: int):
            ls = self.user_admin.list_current_user_roles()
            return len(ls) == n_expected, ls

        roles_list = wait_until_result(
            lambda: list_roles(2), timeout_sec=5, backoff_sec=1, retry_on_exc=True
        )

        assert roles_list is not None, "Roles list never resolved"

        assert len(roles_list) == 2, f"Unexpected roles list {roles_list}"
        assert all(n in roles_list for n in [self.ROLE_NAME0, self.ROLE_NAME1]), (
            f"Unexpected roles list {roles_list}"
        )
        # TODO: Add testing for filtering once v2 list_current_user_roles supports it
        # self.logger.debug("Test '?filter' parameter")

        bogus_admin = AdminV2RoleWrapper(AdminV2(self.redpanda, auth=("bob", "1234")))
        with expect_role_error(ConnectErrorCode.UNAUTHENTICATED):
            bogus_admin.list_current_user_roles()

    @cluster(num_nodes=3)
    def test_list_user_roles_no_authn(self):
        noauth_admin = AdminV2RoleWrapper(AdminV2(self.redpanda))

        with expect_role_error(ConnectErrorCode.UNAUTHENTICATED):
            noauth_admin.list_current_user_roles()

        self.redpanda.set_cluster_config({"admin_api_require_auth": False})

        roles = noauth_admin.list_current_user_roles()
        assert len(roles) == 0, f"Unexpected roles: {roles}"


class RBACEndToEndTest(RBACTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.security = SecurityConfig()
        self.security.enable_sasl = True
        self.security.kafka_enable_authorization = True
        self.security.endpoint_authn_method = "sasl"
        self.security.require_client_auth = True

        self.su_rpk = RpkTool(
            self.redpanda,
            username=self.superuser.username,
            password=self.superuser.password,
            sasl_mechanism=self.superuser.algorithm,
        )
        self.alice_rpk = RpkTool(
            self.redpanda,
            username=ALICE.username,
            password=ALICE.password,
            sasl_mechanism=ALICE.algorithm,
        )

        self.topic0 = "some-topic"
        self.topic1 = "other-topic"

    def setUp(self):
        self.redpanda.set_security_settings(self.security)
        super().setUp()

    def role_for_user(self, role: str, user: security_pb2.RoleMember):
        res = self.superuser_admin.list_roles()
        return any(r.name == role and user in r.members for r in res)

    def has_topics(self, client: RpkTool):
        tps = client.list_topics()
        return list(tps)

    @cluster(num_nodes=3)
    def test_rbac(self):
        alice = security_pb2.RoleMember(user=security_pb2.RoleUser(name=ALICE.username))

        self.logger.debug(f"Create a couple of roles, one with {alice} and one without")

        _ = self.superuser_admin.create_role(role=self.ROLE_NAME0, members=[alice])

        _ = self.superuser_admin.create_role(role=self.ROLE_NAME1)

        wait_until(
            lambda: self.role_for_user(self.ROLE_NAME0, alice),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        self.su_rpk.create_topic(self.topic0)
        self.su_rpk.create_topic(self.topic1)

        self.logger.debug(
            "Since No permissions have been added to either role, expect authZ failed"
        )
        with expect_exception(
            RpkException, lambda e: "AUTHORIZATION_FAILED" in e.stderr
        ):
            self.alice_rpk.produce(self.topic0, "foo", "bar")

        self.logger.debug("Now add topic access rights for user")
        self.su_rpk.sasl_allow_principal(
            f"RedpandaRole:{self.ROLE_NAME0}", ["all"], "topic", "*"
        )

        self.logger.debug(
            "And a deny ACL to the role which is NOT assigned to the user"
        )
        self.su_rpk.sasl_deny_principal(
            f"RedpandaRole:{self.ROLE_NAME1}", ["read"], "topic", self.topic1
        )

        topics = wait_until_result(
            lambda: self.has_topics(self.alice_rpk),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        assert self.topic0 in topics
        assert self.topic1 in topics

        self.logger.debug("Confirm that the user can produce to both topics")

        self.alice_rpk.produce(self.topic0, "foo", "bar")
        self.alice_rpk.produce(self.topic1, "baz", "qux")

        self.logger.debug("Confirm that the user can consume both topics")

        rec = json.loads(self.alice_rpk.consume(self.topic0, n=1))
        assert rec["topic"] == self.topic0, f"Unexpected topic {rec['topic']}"
        assert rec["key"] == "foo", f"Unexpected key {rec['key']}"
        assert rec["value"] == "bar", f"Unexpected value {rec['value']}"

        rec = json.loads(self.alice_rpk.consume(self.topic1, n=1))
        assert rec["topic"] == self.topic1, f"Unexpected topic {rec['topic']}"
        assert rec["key"] == "baz", f"Unexpected key {rec['key']}"
        assert rec["value"] == "qux", f"Unexpected value {rec['value']}"

        self.logger.debug(
            "Now add user to the role with the deny ACL and confirm change in access"
        )

        _ = self.superuser_admin.add_role_members(role=self.ROLE_NAME1, members=[alice])

        wait_until(
            lambda: self.role_for_user(self.ROLE_NAME1, alice),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        wait_until(
            lambda: "DENY" in self.su_rpk.acl_list(),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        with expect_exception(
            RpkException, lambda e: "AUTHORIZATION_FAILED" in e.stderr
        ):
            self.alice_rpk.consume(self.topic1, n=1)

        self.logger.debug(
            "And finally confirm that the user retains read rights on the other topic"
        )

        rec = json.loads(self.alice_rpk.consume(self.topic0, n=1))
        assert rec["topic"] == self.topic0, f"Unexpected topic {rec['topic']}"
        assert rec["key"] == "foo", f"Unexpected key {rec['key']}"
        assert rec["value"] == "bar", f"Unexpected value {rec['value']}"

    # TODO: Add test_rbac_group once group role membership honored in authZ (CORE-15199)

    @cluster(num_nodes=3)
    @parametrize(delete_acls=True)
    @parametrize(delete_acls=False)
    def test_delete_role_acls(self, delete_acls):
        alice = security_pb2.RoleMember(user=security_pb2.RoleUser(name=ALICE.username))
        r0_principal = f"RedpandaRole:{self.ROLE_NAME0}"
        r1_principal = f"RedpandaRole:{self.ROLE_NAME1}"

        _ = self.superuser_admin.create_role(role=self.ROLE_NAME0, members=[alice])
        _ = self.superuser_admin.create_role(role=self.ROLE_NAME1, members=[alice])

        self.su_rpk.sasl_allow_principal(r0_principal, ["read"], "topic", "*")
        self.su_rpk.sasl_allow_principal(r0_principal, ["all"], "group", "*")
        self.su_rpk.acl_create_allow_cluster(
            username=self.ROLE_NAME0, op="all", principal_type="RedpandaRole"
        )
        self.su_rpk.sasl_allow_principal(r0_principal, ["all"], "transactional-id", "*")
        self.su_rpk.sasl_allow_principal(r1_principal, ["write"], "topic", "*")

        for r in [self.ROLE_NAME0, self.ROLE_NAME1]:
            wait_until(
                lambda: self.role_for_user(r, alice),
                timeout_sec=10,
                backoff_sec=1,
                retry_on_exc=True,
            )

        def acl_list():
            return list(
                filter(
                    lambda l: l != "" and "PRINCIPAL" not in l,
                    self.su_rpk.acl_list().split("\n"),
                )
            )

        wait_until(
            lambda: len(acl_list()) == 5,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        self.superuser_admin.delete_role(role=self.ROLE_NAME0, delete_acls=delete_acls)

        wait_until(
            lambda: not self.role_for_user(self.ROLE_NAME0, alice),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        def expect_acls_deleted():
            return wait_until_result(
                lambda: (len(acl_list()) == 1, acl_list()),
                timeout_sec=10,
                backoff_sec=1,
                retry_on_exc=True,
            )

        if delete_acls:
            acls = expect_acls_deleted()
            assert not any(r0_principal in a for a in acls)
        else:
            with expect_exception(ducktape.errors.TimeoutError, lambda e: True):
                expect_acls_deleted()

        roles = self.superuser_admin.list_roles()

        assert len(roles) == 1, f"Wrong number of roles {str(roles)}"


class RolePersistenceTest(RBACTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin_v1 = Admin(
            self.redpanda, auth=(self.superuser.username, self.superuser.password)
        )

    def _wait_for_everything_snapshotted(self, nodes: list):
        controller_max_offset = max(
            self.admin_v1.get_controller_status(n)["committed_index"] for n in nodes
        )
        self.logger.debug(f"controller max offset is {controller_max_offset}")

        for n in nodes:
            self.redpanda.wait_for_controller_snapshot(
                node=n, prev_start_offset=(controller_max_offset - 1)
            )

        return controller_max_offset

    @cluster(num_nodes=3)
    def test_role_survives_restart(self):
        self.redpanda.set_feature_active("controller_snapshots", True, timeout_sec=10)

        self.redpanda.set_cluster_config({"controller_snapshot_max_age_sec": 1})

        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for the cluster to elect a controller leader after the restart
        self.redpanda.wait_until(self.redpanda.healthy, timeout_sec=30, backoff_sec=1)
        self.admin_v1.await_stable_leader(
            topic="controller",
            partition=0,
            namespace="redpanda",
            timeout_s=30,
            backoff_s=1,
        )

        admin = self.superuser_admin

        names = [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
        ]

        for n in names:
            admin.create_role(role=n)

        rand_role = random.choice(names)

        users = [
            "u1",
            "u2",
            "u3",
            "u4",
            "u5",
            "u6",
        ]

        groups = [
            "g1",
            "g2",
            "g3",
            "g4",
        ]

        self.logger.debug(
            "Submit several updates with both users and groups, each of which is destructive."
        )

        for u in users:
            admin.add_role_members(
                role=rand_role,
                members=[security_pb2.RoleMember(user=security_pb2.RoleUser(name=u))],
            )

        for g in groups:
            admin.add_role_members(
                role=rand_role,
                members=[security_pb2.RoleMember(group=security_pb2.RoleGroup(name=g))],
            )

        user_partition = len(users) // 2
        group_partition = len(groups) // 2

        to_remove = [
            security_pb2.RoleMember(user=security_pb2.RoleUser(name=u))
            for u in users[user_partition:]
        ] + [
            security_pb2.RoleMember(group=security_pb2.RoleGroup(name=g))
            for g in groups[group_partition:]
        ]

        admin.remove_role_members(role=rand_role, members=to_remove)

        self._wait_for_everything_snapshotted(self.redpanda.nodes)

        r = wait_until_result(
            lambda: admin.get_role(role=rand_role),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )

        assert r.name == rand_role

        self.redpanda.restart_nodes(self.redpanda.nodes)

        for n in names:
            r = wait_until_result(
                lambda: admin.get_role(role=n),
                timeout_sec=10,
                backoff_sec=1,
                retry_on_exc=True,
            )
            assert r.name == n

        expected_users = set(users[:user_partition])
        expected_groups = set(groups[:group_partition])

        def check_members():
            members = admin.list_role_members(role=rand_role)
            actual_users = set(
                m.user.name for m in members if m.WhichOneof("member") == "user"
            )
            actual_groups = set(
                m.group.name for m in members if m.WhichOneof("member") == "group"
            )
            return actual_users == expected_users and actual_groups == expected_groups

        wait_until(
            check_members,
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True,
        )
