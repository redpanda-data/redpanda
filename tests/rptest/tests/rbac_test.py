# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.util import expect_exception, expect_http_error

import json

ALICE = SaslCredentials("alice", "itsMeH0nest", "SCRAM-SHA-256")


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

    @cluster(num_nodes=1)
    def test_superuser_access(self):
        def print_response(rsp, command=""):
            print(
                f"{command} rsp: {json.dumps(rsp.json(), indent=1) if rsp.content else 'None'}\n"
            )

        # a superuser may access the RBAC API

        print_response(self.superuser_admin.create_role(role='foo'))
        print_response(self.superuser_admin.list_roles())
        print_response(self.superuser_admin.get_role(role='foo'))
        print_response(
            self.superuser_admin.update_role_members(role='foo',
                                                     add=[ALICE.username]))
        print_response(self.superuser_admin.get_role(role='foo'))
        print_response(
            self.superuser_admin.update_role(role='foo',
                                             update={'role': 'bar'}))
        print_response(self.superuser_admin.list_role_members(role='bar'))
        print_response(self.superuser_admin.list_roles())
        print_response(self.superuser_admin.delete_role(role='bar'), "delete")
        print_response(self.superuser_admin.list_roles())

    @cluster(num_nodes=3)
    def test_regular_user_access(self):
        # a regular user may NOT access the RBAC API

        with expect_http_error(403):
            self.user_admin.list_roles()

        with expect_http_error(403):
            self.user_admin.create_role(role='foo')

        with expect_http_error(403):
            self.user_admin.get_role(role='foo')

        with expect_http_error(403):
            self.user_admin.update_role(role='foo', update={'role': 'bar'})

        with expect_http_error(403):
            self.user_admin.update_role_members(role='bar',
                                                add=[ALICE.username])

        with expect_http_error(403):
            self.user_admin.list_role_members(role='bar')

        with expect_http_error(501):
            self.user_admin.list_user_roles()

        with expect_http_error(403):
            self.user_admin.delete_role(role='bar')
