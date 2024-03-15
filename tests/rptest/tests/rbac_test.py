# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin, RoleUpdate, RoleErrorCode, RoleError, RolesList
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.tests.metrics_reporter_test import MetricsReporterServer
from rptest.util import expect_exception, expect_http_error

ALICE = SaslCredentials("alice", "itsMeH0nest", "SCRAM-SHA-256")


def expect_role_error(status_code: RoleErrorCode):
    return expect_exception(
        HTTPError, lambda e: RoleError.from_http_error(e).code == status_code)


class RBACTestBase(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"
    role_name0 = 'foo'
    role_name1 = 'bar'

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
    @cluster(num_nodes=3)
    def test_superuser_access(self):
        # a superuser may access the RBAC API
        res = self.superuser_admin.list_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.get_role(role=self.role_name0)

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role(role=self.role_name0,
                                             update=RoleUpdate(
                                                 self.role_name1))

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.update_role_members(role=self.role_name1,
                                                     add=[ALICE.username])

        res = self.superuser_admin.list_roles(filter='ba',
                                              principal=ALICE.username)
        assert len(RolesList.from_response(res)) == 0, "Unexpected roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
            self.superuser_admin.list_role_members(role=self.role_name1)

        res = self.superuser_admin.list_user_roles()
        assert len(RolesList.from_response(res)) == 0, "Unexpected user roles"

        with expect_role_error(RoleErrorCode.ROLE_NOT_FOUND):
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
            self.user_admin.update_role(role=self.role_name0,
                                        update=RoleUpdate(self.role_name1))

        with expect_http_error(403):
            self.user_admin.update_role_members(role=self.role_name1,
                                                add=[ALICE.username])

        with expect_http_error(403):
            self.user_admin.list_role_members(role=self.role_name1)

        res = self.user_admin.list_user_roles()
        assert len(
            RolesList.from_response(res)) == 0, "Unexpected roles for user"

        with expect_http_error(403):
            self.user_admin.delete_role(role=self.role_name1)

    @cluster(num_nodes=3)
    def test_create_role(self):
        res = self.superuser_admin.create_role(role=self.role_name0)
        created_role = res.json()['role']
        assert created_role == self.role_name0, f"Incorrect create role response: {res.json()}"

        # Also verify idempotency
        res = self.superuser_admin.create_role(role=self.role_name0)
        created_role = res.json()['role']
        assert created_role == self.role_name0, f"Incorrect create role response: {res.json()}"

    @cluster(num_nodes=3)
    def test_invalid_create_role(self):
        with expect_http_error(400):
            self.superuser_admin._request("post", "security/roles")

        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request("post",
                                          "security/roles",
                                          data='["json list not object"]')

        with expect_role_error(RoleErrorCode.MALFORMED_DEF):
            self.superuser_admin._request("post",
                                          "security/roles",
                                          json=dict())

        # Two ordinals (corresponding to ',' and '=') are explicitly excluded from role names
        for ordinal in [0x2c, 0x3d]:
            invalid_rolename = f"john{chr(ordinal)}doe"

            with expect_http_error(400):
                self.superuser_admin.create_role(role=invalid_rolename)


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

        assert wait_for_new_report()['has_rbac'] is False

        self.superuser_admin.create_role(role=self.role_name0)

        wait_until(lambda: wait_for_new_report()['has_rbac'] is True,
                   timeout_sec=20,
                   backoff_sec=1)
        self.metrics.stop()
