# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
from requests.exceptions import HTTPError
import random
import time
from typing import Optional

from ducktape.utils.util import wait_until

import ducktape.errors
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.admin import (Admin, RedpandaNode, RoleMemberList,
                                   RoleUpdate, RoleErrorCode, RoleError,
                                   RolesList, RoleDescription,
                                   RoleMemberUpdateResponse, RoleMember, Role)
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.tests.metrics_reporter_test import MetricsReporterServer
from rptest.util import expect_exception, expect_http_error, wait_until_result
from rptest.utils.mode_checks import skip_fips_mode

ALICE = SaslCredentials("alice", "itsMeH0nest", "SCRAM-SHA-256")


def expect_role_error(status_code: RoleErrorCode):
    return expect_exception(
        HTTPError, lambda e: RoleError.from_http_error(e).code == status_code)


class RBACTestBase(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"
    role_name0 = 'foo'
    role_name1 = 'bar'
    role_name2 = 'baz'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.rpk = RpkTool(self.redpanda)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = Admin(self.redpanda,
                                     auth=(self.superuser.username,
                                           self.superuser.password))
        self.user_admin = Admin(self.redpanda,
                                auth=(ALICE.username, ALICE.password))

    def setUp(self):
        super().setUp()
        create_user_and_wait(self.redpanda, self.superuser_admin, ALICE)

        self.redpanda.set_cluster_config({'admin_api_require_auth': True})


class RBACTest(RBACTestBase):
    def _role_exists(self, target_role: str):
        res = self.superuser_admin.list_roles()
        roles = RolesList.from_response(res).roles
        return any(target_role == role.name for role in roles)

    def _create_and_wait_for_role(self, role: str):
        self.superuser_admin.create_role(role)
        wait_until(lambda: self._role_exists(role),
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg="Role was not created")

    def _set_of_user_roles(self):
        res = self.superuser_admin.list_roles()
        roles = RolesList.from_response(res).roles
        return set(role.name for role in roles)

    @cluster(num_nodes=3)
    def test_superuser_access(self):
        # a superuser may access the RBAC API
        res = self.superuser_admin.list_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.get_role(role=self.role_name0)

        res = self.superuser_admin.list_roles(filter='ba',
                                              principal=ALICE.username)
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        res = self.superuser_admin.list_user_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected user roles"

        self.superuser_admin.delete_role(role=self.role_name1)

    @cluster(num_nodes=3)
    def test_regular_user_access(self):
        # a regular user may NOT access the RBAC API

        with expect_http_error(403):
            self.user_admin.list_roles()

        with expect_http_error(403):
            self.user_admin.create_role(role=self.role_name0)

        with expect_http_error(403):
            self.user_admin.get_role(role=self.role_name0)

        with expect_http_error(403):
            self.user_admin.update_role_members(
                role=self.role_name1, add=[RoleMember.User(ALICE.username)])

        with expect_http_error(403):
            self.user_admin.list_role_members(role=self.role_name1)

        with expect_http_error(403):
            self.user_admin.delete_role(role=self.role_name1)

    @cluster(num_nodes=3)
    def test_create_role(self):
        self.logger.debug("Test that simple create_role succeeds")
        res = self.superuser_admin.create_role(role=self.role_name0)
        created_role = res.json()['role']
        assert res.status_code == 201, f"Unexpected HTTP status code: {res.status_code}"
        assert created_role == self.role_name0, f"Incorrect create role response: {res.json()}"

        wait_until(lambda: self._set_of_user_roles() == {self.role_name0},
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg="Role was not created")

        self.logger.debug("Also test idempotency of create_role")
        res = self.superuser_admin.create_role(role=self.role_name0)
        created_role = res.json()['role']
        assert created_role == self.role_name0, f"Incorrect create role response: {res.json()}"

    @cluster(num_nodes=3)
    def test_invalid_create_role(self):
        self.logger.debug("Test that create_role rejects an empty HTTP body")
        with expect_http_error(400):
            self.superuser_admin._request("post", "security/roles")

        self.logger.debug("Test that create_role rejects a JSON list body")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request("post",
                                          "security/roles",
                                          data='["json list not object"]')

        self.logger.debug(
            "Test that create_role rejects an empty JSON object body")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request("post",
                                          "security/roles",
                                          json=dict())

        # Two ordinals (corresponding to ',' and '=') are explicitly excluded from role names
        self.logger.debug("Test that create_role rejects invalid role names")
        for ordinal in [0x2c, 0x3d]:
            invalid_rolename = f"john{chr(ordinal)}doe"

            with expect_http_error(400):
                self.superuser_admin.create_role(role=invalid_rolename)

    @cluster(num_nodes=3)
    def test_list_role_filter(self):
        role_one = "aaaa-1"
        role_two = "aaaa-2"
        role_three = "bbbb-3"
        alice = RoleMember(RoleMember.PrincipalType.USER, 'alice')
        bob = RoleMember(RoleMember.PrincipalType.USER, 'bob')

        self.superuser_admin.create_role(role=role_one)
        self.superuser_admin.create_role(role=role_two)
        self.superuser_admin.create_role(role=role_three)

        def matching_role_set(filter: Optional[str] = None,
                              principal: Optional[str] = None,
                              principal_type: Optional[str] = None):
            res = self.superuser_admin.list_roles(
                filter=filter,
                principal=principal,
                principal_type=principal_type)
            roles = RolesList.from_response(res).roles
            return set(role.name for role in roles)

        def test_filter(expected_roles: set[str],
                        filter: Optional[str] = None,
                        principal: Optional[str] = None,
                        principal_type: Optional[str] = None):
            wait_until(lambda: set(expected_roles) == \
                                matching_role_set(filter=filter, principal=principal, principal_type=principal_type),
                       timeout_sec=10,
                       err_msg=f"Mismatch on filter result")

        self.logger.debug(
            "Test that list_roles works with various combinations of filter")
        test_filter(filter="aaaa", expected_roles=[role_one, role_two])
        test_filter(filter="bb", expected_roles=[role_three])
        test_filter(filter="aaaa-2", expected_roles=[role_two])
        test_filter(filter="ccc", expected_roles=[])

        self.logger.debug(
            "Test that list_roles works with an unknown principal")
        test_filter(principal=alice.name, expected_roles=[])

        self.logger.debug(
            "Test that list_roles works with an existing principals")
        self.superuser_admin.update_role_members(role=role_one, add=[alice])
        self.superuser_admin.update_role_members(role=role_two,
                                                 add=[alice, bob])
        self.superuser_admin.update_role_members(role=role_three, add=[bob])

        test_filter(principal=alice.name, expected_roles=[role_one, role_two])
        test_filter(principal=bob.name, expected_roles=[role_two, role_three])
        test_filter(filter="aaaa",
                    principal=bob.name,
                    expected_roles=[role_two])

        self.logger.debug(
            "Test that list_roles rejects a non-User principal type")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin.list_roles(filter="aaa",
                                            principal=alice.name,
                                            principal_type="Role")
        self.logger.debug("Test that list_roles accepts a User principal type")
        test_filter(filter="aaaa",
                    principal=bob.name,
                    principal_type="User",
                    expected_roles=[role_two])

    @cluster(num_nodes=3)
    def test_get_role(self):
        alice = RoleMember(RoleMember.PrincipalType.USER, 'alice')

        self.logger.debug("Test that get_role rejects an unknown role")
        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.get_role(role=self.role_name0)

        self.logger.debug("Test that get_role succeeds with an existing role")
        self._create_and_wait_for_role(role=self.role_name1)

        def get_role_succeeds(role_name: str,
                              expected_members: list[str] = []):
            try:
                res = self.superuser_admin.get_role(role=role_name)
                role = Role.from_response(res)

                return role.name == role_name and \
                    len(role.members) == len(expected_members) and \
                    all(member in role.members for member in expected_members)
            except HTTPError as e:
                assert RoleError.from_http_error(e).code == RoleErrorCode.ROLE_NOT_FOUND, \
                    f"Unexpected error while waiting for get_role to succeed: {e}"
                return False

        wait_until(lambda: get_role_succeeds(self.role_name1),
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg="Get role hasn't succeeded in time")

        self.logger.debug(
            "Test that get_role succeeds with an existing role that has members"
        )
        self.superuser_admin.update_role_members(role=self.role_name1,
                                                 add=[alice])

        wait_until(lambda: get_role_succeeds(self.role_name1,
                                             expected_members=[alice]),
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg="Get role hasn't succeeded in time")

    @cluster(num_nodes=3)
    def test_delete_role(self):
        self.logger.debug("Test that delete_role succeeds with existing role")
        self._create_and_wait_for_role(role=self.role_name0)

        res = self.superuser_admin.delete_role(role=self.role_name0)
        assert res.status_code == 204, f"Unexpected HTTP status code: {res.status_code}"

        wait_until(lambda: not self._role_exists(self.role_name0),
                   timeout_sec=5,
                   backoff_sec=0.5)

        self.logger.debug(
            "Test that delete_role succeeds with non-existing role for idempotency"
        )
        self.superuser_admin.delete_role(role=self.role_name0)
        assert res.status_code == 204, f"Unexpected HTTP status code: {res.status_code}"

    @cluster(num_nodes=3)
    def test_members_endpoint(self):
        alice = RoleMember.User('alice')
        bob = RoleMember.User('bob')

        self.logger.debug(
            "Test that update_role_members can create the role as a side effect"
        )
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert member_update.role == self.role_name0, f"Incorrect role name: {member_update.role}"
        assert member_update.created, "Expected created flag to be set"
        assert len(member_update.added
                   ) == 1, f"Incorrect 'added' result: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Incorrect 'removed' result: {member_update.removed}"
        assert alice in member_update.added, f"Incorrect member added: {member_update.added[0]}"

        self.logger.debug("And check that we can query the role we created")
        res = wait_until_result(lambda: self.superuser_admin.list_role_members(
            role=self.role_name0),
                                timeout_sec=10,
                                backoff_sec=1,
                                retry_on_exc=True)
        assert res is not None, f"Failed to get members for newly created role"

        assert res.status_code == 200, "Expected 200 (OK)"
        members = RoleMemberList.from_response(res)
        assert len(members) == 1, f"Unexpected members list: {members}"
        assert alice in members, f"Missing expected member, got: {members}"

        self.logger.debug("Now add a new member to the role")
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[bob],
                                                       create=False)

        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert len(member_update.added
                   ) == 1, f"Incorrect 'added' result: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Incorrect 'removed' result: {member_update.removed}"
        assert bob in member_update.added

        def until_members(role,
                          expected: list[RoleMember] = [],
                          excluded: list[RoleMember] = []):
            res = self.superuser_admin.list_role_members(role=role)
            assert res.status_code == 200, "Expected 200 (OK)"
            members = RoleMemberList.from_response(res)
            exp = all(m in members for m in expected)
            excl = not any(m in members for m in excluded)
            return exp and excl, members

        self.logger.debug(
            "And verify that the members list eventually reflects that change")
        members = wait_until_result(
            lambda: until_members(self.role_name0, expected=[alice, bob]),
            timeout_sec=5,
            backoff_sec=1,
            retry_on_exc=True)

        assert members is not None, "Failed to get members"
        for m in [bob, alice]:
            assert m in members, f"Missing member {m}, got: {members}"

        self.logger.debug("Remove a member from the role")
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       remove=[alice])
        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)

        assert len(
            member_update.removed
        ) == 1, f"Incorrect 'removed' result: {member_update.removed}"
        assert len(member_update.added
                   ) == 0, f"Incorrect 'added' result: {member_update.added}"
        assert alice in member_update.removed, f"Expected {alice} to be removed, got {member_update.removed}"

        self.logger.debug(
            "And verify that the members list eventually reflects the removal")
        members = wait_until_result(lambda: until_members(
            self.role_name0, expected=[bob], excluded=[alice]),
                                    timeout_sec=5,
                                    backoff_sec=1,
                                    retry_on_exc=True)

        assert members is not None
        assert len(members) == 1, f"Unexpected member: {members}"
        assert alice not in members, f"Unexpected member {alice}, got: {members}"

        self.logger.debug(
            "Test update idempotency - no-op update should succeed")
        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[bob])
        assert res.status_code == 200, "Expected 200 (OK)"
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert len(member_update.added
                   ) == 0, f"Unexpectedly added members: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Unexpectedly removed members: {member_update.removed}"

        self.logger.debug(
            "Check that the create flag works even when add/remove lists are empty"
        )
        res = self.superuser_admin.update_role_members(role=self.role_name1,
                                                       create=True)
        assert res.status_code == 200, "Expected 200 (OK)"  # TODO(oren): should be 201??
        member_update = RoleMemberUpdateResponse.from_response(res)
        assert len(member_update.added
                   ) == 0, f"Unexpectedly added members: {member_update.added}"
        assert len(
            member_update.removed
        ) == 0, f"Unexpectedly removed members: {member_update.removed}"
        assert member_update.created, "Expected created flag to be set"

    @cluster(num_nodes=3)
    def test_members_endpoint_errors(self):
        alice = RoleMember.User('alice')
        bob = RoleMember.User('bob')

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.list_role_members(role=self.role_name0)

        self.logger.debug(
            "ROLE_NOT_FOUND whether create flag is defaulted or explicitly set false"
        )
        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role_members(role=self.role_name0,
                                                     add=[alice])

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role_members(role=self.role_name0,
                                                     add=[alice],
                                                     create=False)

        self.logger.debug(
            "MEMBER_LIST_CONFLICT even if the role doesn't exist")
        with expect_role_error(RoleErrorCode.MEMBER_LIST_CONFLICT):
            self.superuser_admin.update_role_members(role=self.role_name0,
                                                     add=[alice],
                                                     remove=[alice],
                                                     create=True)

        self.logger.debug("Check that errored update has no effect")
        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.list_role_members(role=self.role_name0)

        self.logger.debug("POST body must be a JSON object")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data='["json list not an object"]')

        self.superuser_admin.update_role_members(role=self.role_name0,
                                                 create=True)

        def role_exists(role):
            self.superuser_admin.list_role_members(role=role)
            return True

        wait_until(lambda: self.superuser_admin.list_role_members(
            role=self.role_name0).status_code == 200,
                   timeout_sec=5,
                   backoff_sec=1,
                   retry_on_exc=True)

        self.logger.debug("Role members must be JSON objects")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps({'add': ["foo"]}))

        self.logger.debug("Role members must have name field")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps({'add': [{}]}))

        self.logger.debug("Role members must have principal_type field")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps({'add': [{
                    'name': 'foo',
                }]}))

        self.logger.debug("principal_type field must be 'User'")
        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request(
                "post",
                f"security/roles/{self.role_name0}/members",
                data=json.dumps(
                    {'add': [{
                        'name': 'foo',
                        'principal_type': 'user',
                    }]}))

        self.logger.debug("A valid raw request")
        res = self.superuser_admin._request(
            "post",
            f"security/roles/{self.role_name0}/members",
            data=json.dumps(
                {'add': [{
                    'name': 'foo',
                    'principal_type': 'User',
                }]}))

        assert res.status_code == 200, f"Request unexpectedly failed with status {res.status_code}"

    @cluster(num_nodes=3)
    def test_list_user_roles(self):
        username = ALICE.username
        alice = RoleMember.User(username)

        res = self.user_admin.list_user_roles()
        assert res.status_code == 200, f"Unexpected status {res.status_code}"
        assert len(
            RolesList.from_response(res)) == 0, "Unexpected roles for user"

        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, f"Unexpected status {res.status_code}"

        res = self.superuser_admin.update_role_members(role=self.role_name1,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, f"Unexpected status {res.status_code}"

        def list_roles(n_expected: int):
            res = self.user_admin.list_user_roles()
            ls = RolesList.from_response(res)
            return len(ls) == n_expected, ls

        roles_list = wait_until_result(lambda: list_roles(2),
                                       timeout_sec=5,
                                       backoff_sec=1,
                                       retry_on_exc=True)

        assert roles_list is not None, "Roles list never resolved"

        assert len(roles_list) == 2, f"Unexpected roles list {roles_list}"
        assert all(
            RoleDescription(n) in roles_list
            for n in [self.role_name0, self.role_name1
                      ]), f"Unexpected roles list {roles_list}"

        self.logger.debug("Test '?filter' parameter")

        res = self.user_admin.list_user_roles(filter="f")
        assert res.status_code == 200, f"Unexpected status code: {res.status_code}"

        roles_list = RolesList.from_response(res)
        assert len(roles_list) == 1, f"Unexpected roles list {roles_list}"
        assert RoleDescription(
            self.role_name0
        ) in roles_list, f"Unexpected roles list {roles_list}"
        assert RoleDescription(
            self.role_name1
        ) not in roles_list, f"Unexpected roles list {roles_list}"

        bogus_admin = Admin(self.redpanda, auth=("bob", "1234"))

        with expect_http_error(401):
            bogus_admin.list_user_roles()

    @cluster(num_nodes=3)
    def test_list_user_roles_no_authn(self):
        noauth_admin = Admin(self.redpanda)

        with expect_http_error(401):
            noauth_admin.list_user_roles()

        self.redpanda.set_cluster_config({'admin_api_require_auth': False})

        res = noauth_admin.list_user_roles()
        assert res.status_code == 200, f"Unexpected status {res.status_code}"
        roles = RolesList.from_response(res)
        assert len(roles) == 0, f"Unexpected roles: {str(roles)}"


class RBACTelemetryTest(RBACTestBase):
    def __init__(self, test_ctx, **kwargs):
        self.metrics = MetricsReporterServer(test_ctx)
        super().__init__(test_ctx,
                         extra_rp_conf={**self.metrics.rp_conf()},
                         **kwargs)

    def setUp(self):
        self.metrics.start()
        super().setUp()

    @cluster(num_nodes=2)
    def test_telemetry(self):
        def wait_for_new_report():
            report_count = len(self.metrics.requests())
            wait_until(lambda: len(self.metrics.requests()) > report_count,
                       timeout_sec=20,
                       backoff_sec=1)
            self.logger.debug(f'New report: {self.metrics.reports()[-1]}')
            return self.metrics.reports()[-1]

        assert wait_for_new_report()['rbac_role_count'] == 0

        names = ['a', 'b', 'c', 'd', 'e', 'f']

        for n in names:
            self.superuser_admin.create_role(role=n)

        wait_until(
            lambda: wait_for_new_report()['rbac_role_count'] == len(names),
            timeout_sec=20,
            backoff_sec=1)
        self.metrics.stop()


class RBACLicenseTest(RBACTestBase):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_ctx, **kwargs):
        super().__init__(test_ctx, **kwargs)
        self.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{self.LICENSE_CHECK_INTERVAL_SEC}'
        })

    def _has_license_nag(self):
        return self.redpanda.search_log_any(
            "license is required to use enterprise features")

    def _license_nag_is_set(self):
        return self.redpanda.search_log_all(
            f"Overriding default license log annoy interval to: {self.LICENSE_CHECK_INTERVAL_SEC}s"
        )

    @cluster(num_nodes=1)
    @skip_fips_mode  # See NOTE below
    def test_license_nag(self):
        wait_until(self._license_nag_is_set,
                   timeout_sec=30,
                   err_msg="Failed to set license nag internal")

        self.logger.debug("Ensuring no license nag")
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        # NOTE: This assertion will FAIL if running in FIPS mode because
        # being in FIPS mode will trigger the license nag
        assert not self._has_license_nag()

        self.logger.debug("Adding a role")
        self.superuser_admin.create_role(role=self.role_name0)

        self.logger.debug("Waiting for license nag")
        wait_until(self._has_license_nag,
                   timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
                   err_msg="License nag failed to appear")


class RBACEndToEndTest(RBACTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.security = SecurityConfig()
        self.security.enable_sasl = True
        self.security.kafka_enable_authorization = True
        self.security.endpoint_authn_method = 'sasl'
        self.security.require_client_auth = True

        self.su_rpk = RpkTool(self.redpanda,
                              username=self.superuser.username,
                              password=self.superuser.password,
                              sasl_mechanism=self.superuser.algorithm)
        self.alice_rpk = RpkTool(self.redpanda,
                                 username=ALICE.username,
                                 password=ALICE.password,
                                 sasl_mechanism=ALICE.algorithm)

        self.topic0 = 'some-topic'
        self.topic1 = 'other-topic'

    def setUp(self):
        self.redpanda.set_security_settings(self.security)
        super().setUp()

    def role_for_user(self, role: str, user: RoleMember):
        res = self.superuser_admin.list_roles(principal=user.name)
        return RoleDescription(role) in RolesList.from_response(res)

    def has_topics(self, client: RpkTool):
        tps = client.list_topics()
        return list(tps)

    @cluster(num_nodes=3)
    def test_rbac(self):
        alice = RoleMember.User('alice')

        self.logger.debug(
            f"Create a couple of roles, one with {alice} and one without")

        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, "Failed to create role"
        res = self.superuser_admin.update_role_members(role=self.role_name1,
                                                       add=[],
                                                       create=True)
        assert res.status_code == 200, "Failed to create role"

        wait_until(lambda: self.role_for_user(self.role_name0, alice),
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True)

        self.su_rpk.create_topic(self.topic0)
        self.su_rpk.create_topic(self.topic1)

        self.logger.debug(
            "Since No permissions have been added to either role, expect authZ failed"
        )
        with expect_exception(RpkException,
                              lambda e: 'AUTHORIZATION_FAILED' in str(e)):
            self.alice_rpk.produce(self.topic0, 'foo', 'bar')

        self.logger.debug("Now add topic access rights for user")
        self.su_rpk.sasl_allow_principal(f"RedpandaRole:{self.role_name0}",
                                         ['all'], 'topic', '*')

        self.logger.debug(
            "And a deny ACL to the role which is NOT assigned to the user")
        self.su_rpk.sasl_deny_principal(f"RedpandaRole:{self.role_name1}",
                                        ['read'], 'topic', self.topic1)

        topics = wait_until_result(lambda: self.has_topics(self.alice_rpk),
                                   timeout_sec=10,
                                   backoff_sec=1,
                                   retry_on_exc=True)

        assert self.topic0 in topics
        assert self.topic1 in topics

        self.logger.debug("Confirm that the user can produce to both topics")

        self.alice_rpk.produce(self.topic0, 'foo', 'bar')
        self.alice_rpk.produce(self.topic1, 'baz', 'qux')

        self.logger.debug("Confirm that the user can consume both topics")

        rec = json.loads(self.alice_rpk.consume(self.topic0, n=1))
        assert rec['topic'] == self.topic0, f"Unexpected topic {rec['topic']}"
        assert rec['key'] == 'foo', f"Unexpected key {rec['key']}"
        assert rec['value'] == 'bar', f"Unexpected value {rec['value']}"

        rec = json.loads(self.alice_rpk.consume(self.topic1, n=1))
        assert rec['topic'] == self.topic1, f"Unexpected topic {rec['topic']}"
        assert rec['key'] == 'baz', f"Unexpected key {rec['key']}"
        assert rec['value'] == 'qux', f"Unexpected value {rec['value']}"

        self.logger.debug(
            "Now add user to the role with the deny ACL and confirm change in access"
        )

        res = self.superuser_admin.update_role_members(role=self.role_name1,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, "Failed to update role"

        wait_until(lambda: self.role_for_user(self.role_name1, alice),
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True)

        wait_until(lambda: "DENY" in self.su_rpk.acl_list(),
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True)

        with expect_exception(RpkException,
                              lambda e: 'AUTHORIZATION_FAILED' in str(e)):
            self.alice_rpk.consume(self.topic1, n=1)

        self.logger.debug(
            "And finally confirm that the user retains read rights on the other topic"
        )

        rec = json.loads(self.alice_rpk.consume(self.topic0, n=1))
        assert rec['topic'] == self.topic0, f"Unexpected topic {rec['topic']}"
        assert rec['key'] == 'foo', f"Unexpected key {rec['key']}"
        assert rec['value'] == 'bar', f"Unexpected value {rec['value']}"

    @cluster(num_nodes=3)
    @parametrize(delete_acls=True)
    @parametrize(delete_acls=False)
    @parametrize(delete_acls=None)
    def test_delete_role_acls(self, delete_acls):
        alice = RoleMember.User('alice')
        r0_principal = f"RedpandaRole:{self.role_name0}"
        r1_principal = f"RedpandaRole:{self.role_name1}"

        res = self.superuser_admin.update_role_members(role=self.role_name0,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, "Failed to create role"
        res = self.superuser_admin.update_role_members(role=self.role_name1,
                                                       add=[alice],
                                                       create=True)
        assert res.status_code == 200, "Failed to create role"

        self.su_rpk.sasl_allow_principal(r0_principal, ['read'], 'topic', '*')
        self.su_rpk.sasl_allow_principal(r0_principal, ['all'], 'group', '*')
        self.su_rpk.acl_create_allow_cluster(username=self.role_name0,
                                             op='all',
                                             principal_type="RedpandaRole")
        self.su_rpk.sasl_allow_principal(r0_principal, ['all'],
                                         'transactional-id', '*')
        self.su_rpk.sasl_allow_principal(r1_principal, ['write'], 'topic', '*')

        for r in [self.role_name0, self.role_name1]:
            wait_until(lambda: self.role_for_user(r, alice),
                       timeout_sec=10,
                       backoff_sec=1,
                       retry_on_exc=True)

        def acl_list():
            return list(
                filter(lambda l: l != '' and 'PRINCIPAL' not in l,
                       self.su_rpk.acl_list().split('\n')))

        wait_until(lambda: len(acl_list()) == 5,
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True)

        self.superuser_admin.delete_role(role=self.role_name0,
                                         delete_acls=delete_acls)

        wait_until(lambda: not self.role_for_user(self.role_name0, alice),
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True)

        def expect_acls_deleted():
            return wait_until_result(lambda:
                                     (len(acl_list()) == 1, acl_list()),
                                     timeout_sec=10,
                                     backoff_sec=1,
                                     retry_on_exc=True)

        if delete_acls:
            acls = expect_acls_deleted()
            assert not any(r0_principal in a for a in acls)
        else:
            with expect_exception(ducktape.errors.TimeoutError,
                                  lambda e: True):
                expect_acls_deleted()

        roles = RolesList.from_response(self.superuser_admin.list_roles())

        assert len(roles) == 1, f"Wrong number of roles {str(roles)}"


class RolePersistenceTest(RBACTestBase):
    def _wait_for_everything_snapshotted(self, nodes: list, admin: Admin):
        controller_max_offset = max(
            admin.get_controller_status(n)['committed_index'] for n in nodes)
        self.logger.debug(f"controller max offset is {controller_max_offset}")

        for n in nodes:
            self.redpanda.wait_for_controller_snapshot(
                node=n, prev_start_offset=(controller_max_offset - 1))

        return controller_max_offset

    @cluster(num_nodes=3)
    def test_role_survives_restart(self):
        self.redpanda.set_feature_active('controller_snapshots',
                                         True,
                                         timeout_sec=10)

        self.redpanda.set_cluster_config(
            {'controller_snapshot_max_age_sec': 1})

        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for the cluster to elect a controller leader after the restart
        self.redpanda.wait_until(self.redpanda.healthy,
                                 timeout_sec=30,
                                 backoff_sec=1)

        admin = self.superuser_admin

        names = [
            'a',
            'b',
            'c',
            'd',
            'e',
            'f',
        ]

        for n in names:
            admin.create_role(role=n)

        rand_role = random.choice(names)

        users = [
            'u1',
            'u2',
            'u3',
            'u4',
            'u5',
            'u6',
        ]

        self.logger.debug(
            "Submit several updates, each of which is destructive.")

        for u in users:
            admin.update_role_members(role=rand_role, add=[RoleMember.User(u)])

        partition = len(users) // 2

        to_remove = [RoleMember.User(u) for u in users[partition:]]

        admin.update_role_members(role=rand_role, remove=to_remove)

        self._wait_for_everything_snapshotted(self.redpanda.nodes, admin)

        r = wait_until_result(
            lambda: Role.from_response(admin.get_role(role=rand_role)),
            timeout_sec=10,
            backoff_sec=1,
            retry_on_exc=True)

        assert r.name == rand_role

        self.redpanda.restart_nodes(self.redpanda.nodes)

        for n in names:
            r = wait_until_result(
                lambda: Role.from_response(admin.get_role(role=n)),
                timeout_sec=10,
                backoff_sec=1,
                retry_on_exc=True)
            assert r.name == n

        expected = [RoleMember.User(u) for u in users[:partition]]

        wait_until(lambda: set(
            RoleMemberList.from_response(
                admin.list_role_members(role=rand_role)).members) == set(
                    expected),
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True)
