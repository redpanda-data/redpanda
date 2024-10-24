# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.redpanda import SecurityConfig
from rptest.util import expect_exception


class RpkRoleTest(RedpandaTest):
    username = "red"
    password = "panda"
    mechanism = "SCRAM-SHA-256"

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = 'sasl'
        super(RpkRoleTest,
              self).__init__(test_ctx,
                             security=security,
                             extra_rp_conf={'election_timeout_ms': 10000},
                             *args,
                             **kwargs)
        self._rpk = RpkTool(redpanda=self.redpanda,
                            username=self.username,
                            password=self.password,
                            sasl_mechanism=self.mechanism)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS

    def _superclient(self):
        return RpkTool(self.redpanda,
                       username=self.superuser.username,
                       password=self.superuser.password,
                       sasl_mechanism=self.mechanism)

    @cluster(num_nodes=1)
    def test_create_list_delete(self):
        """
        Basic test to ensure that the command work properly
        and that it can create/list/delete roles.
        """
        role_name = "redpanda-role"
        superclient = self._superclient()
        created = superclient.create_role(role_name)
        assert role_name in created["roles"]

        listed = superclient.list_roles()
        assert role_name in listed["roles"]

        superclient.delete_role(role_name)

        listed = superclient.list_roles()
        assert role_name not in listed["roles"]

    @cluster(num_nodes=1)
    def test_assign_describe(self):
        """
        Test role assignment and describe role command
        """
        role_name = "redpanda-role"
        topic_name = "red-topic"
        superclient = self._superclient()

        superclient.create_topic(topic_name)

        # Create the role
        created = superclient.create_role(role_name)
        assert role_name in created["roles"]

        # Create user and verify it can't produce
        superclient.sasl_create_user(self.username, self.password)

        with expect_exception(RpkException,
                              lambda e: 'AUTHORIZATION_FAILED' in str(e)):
            self._rpk.produce(topic_name, 'foo', 'bar')

        superclient.sasl_allow_principal(f"RedpandaRole:{role_name}", ['all'],
                                         'topic', topic_name)

        # Now we assign the role, the user should be able to produce without errors.
        superclient.assign_role(role_name, [self.username])
        self._rpk.produce(topic_name, 'foo', 'bar')

        # Verify we can describe the role and it contains members and permissions.
        described = superclient.describe_role(role_name)
        assert len(described["permissions"]) > 0
        assert self.username == described["members"][0]["name"]

        # Now, unassign, error should be back:
        superclient.unassign_role(role_name, [self.username])
        with expect_exception(RpkException,
                              lambda e: 'AUTHORIZATION_FAILED' in str(e)):
            self._rpk.produce(topic_name, 'foo', 'bar')
