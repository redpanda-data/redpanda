# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from requests.exceptions import HTTPError

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin, RoleUpdate, RoleErrorCode, RoleError, RolesList
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.util import expect_exception, expect_http_error

ALICE = SaslCredentials("alice", "itsMeH0nest", "SCRAM-SHA-256")


def expect_role_error(status_code: RoleErrorCode):
    return expect_exception(
        HTTPError, lambda e: RoleError.from_http_error(e).code == status_code)


class RBACTest(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"

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

    @cluster(num_nodes=3)
    def test_superuser_access(self):
        # a superuser may access the RBAC API

        role_name0 = 'foo'
        role_name1 = 'bar'

        res = self.superuser_admin.list_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin.create_role(role=role_name0)

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.get_role(role=role_name0)

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role(role=role_name0,
                                             update=RoleUpdate(role_name1))

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role_members(role=role_name1,
                                                     add=[ALICE.username])

        res = self.superuser_admin.list_roles(filter='ba',
                                              principal=ALICE.username)
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.list_role_members(role=role_name1)

        res = self.superuser_admin.list_user_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected user roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.delete_role(role=role_name1)

    @cluster(num_nodes=3)
    def test_regular_user_access(self):
        # a regular user may NOT access the RBAC API

        role_name0 = 'foo'
        role_name1 = 'bar'

        with expect_http_error(403):
            self.user_admin.list_roles()

        with expect_http_error(403):
            self.user_admin.create_role(role=role_name0)

        with expect_http_error(403):
            self.user_admin.get_role(role=role_name0)

        with expect_http_error(403):
            self.user_admin.update_role(role=role_name0,
                                        update=RoleUpdate(role_name1))

        with expect_http_error(403):
            self.user_admin.update_role_members(role=role_name1,
                                                add=[ALICE.username])

        with expect_http_error(403):
            self.user_admin.list_role_members(role=role_name1)

        res = self.user_admin.list_user_roles()
        assert len(
            RolesList.from_response(res)) == 0, "Unexpected roles for user"

        with expect_http_error(403):
            self.user_admin.delete_role(role=role_name1)
