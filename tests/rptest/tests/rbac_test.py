# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool, ClusterAuthorizationError, RpkException
from rptest.services.admin import Admin
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.util import expect_exception, expect_http_error
from rptest.util import wait_until, wait_until_result

import json

ALICE = SaslCredentials("alice", "itsMeH0nest", "SCRAM-SHA-256")


class RBACTest(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = Admin(self.redpanda,
                                     auth=(self.superuser.username,
                                           self.superuser.password))
        self.user_admin = Admin(self.redpanda,
                                auth=(ALICE.username, ALICE.password))

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

    def setUp(self):
        self.redpanda.set_security_settings(self.security)
        super().setUp()
        create_user_and_wait(self.redpanda, self.superuser_admin, ALICE)

        self.redpanda.set_cluster_config({'admin_api_require_auth': True})

    def print_response(self, rsp, command=""):
        print(
            f"{command} rsp: {json.dumps(rsp.json(), indent=1) if rsp.content else 'None'}\n"
        )

    @cluster(num_nodes=1)
    def test_superuser_access(self):
        # a superuser may access the RBAC API

        self.print_response(self.superuser_admin.create_role(role='foo'))
        self.print_response(self.superuser_admin.list_roles())
        self.print_response(self.superuser_admin.get_role(role='foo'))
        self.print_response(
            self.superuser_admin.update_role_members(role='foo',
                                                     add=[ALICE.username]))
        self.print_response(self.superuser_admin.get_role(role='foo'))
        self.print_response(
            self.superuser_admin.update_role(role='foo',
                                             update={'role': 'bar'}))
        self.print_response(self.superuser_admin.list_role_members(role='bar'))
        self.print_response(self.superuser_admin.list_roles())
        self.print_response(self.superuser_admin.delete_role(role='bar'),
                            "delete")
        self.print_response(self.superuser_admin.list_roles())

    @cluster(num_nodes=1)
    def test_rbac(self):
        TOPIC_NAME = 'some-topic'
        ROLE_NAME = 'foo'
        self.print_response(self.superuser_admin.create_role(role=ROLE_NAME))
        self.print_response(
            self.superuser_admin.update_role_members(role=ROLE_NAME,
                                                     add=[ALICE.username]))
        self.su_rpk.create_topic(TOPIC_NAME)
        with expect_exception(RpkException,
                              lambda e: 'AUTHORIZATION_FAILED' in str(e)):
            self.alice_rpk.produce(TOPIC_NAME, 'foo', 'bar')

        self.logger.debug("Now add topic access rights for user 'alice'")

        self.su_rpk.sasl_allow_principal(f"Role:{ROLE_NAME}", ['all'], 'topic',
                                         TOPIC_NAME)

        print(self.su_rpk.acl_list())

        wait_until(lambda: self.alice_rpk.list_topics() != [],
                   timeout_sec=10,
                   backoff_sec=1)

        print(json.dumps([i for i in self.alice_rpk.list_topics()], indent=1))
        self.alice_rpk.produce(TOPIC_NAME, 'foo', 'bar')

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
