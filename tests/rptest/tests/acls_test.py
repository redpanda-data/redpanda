# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, ClusterAuthorizationError
from rptest.services.redpanda import SecurityConfig


class AccessControlListTest(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, test_context):
        security = SecurityConfig()
        security.enable_sasl = True
        super(AccessControlListTest,
              self).__init__(test_context,
                             num_brokers=3,
                             security=security,
                             extra_node_conf={'developer_mode': True})

    def get_client(self, username):
        return RpkTool(self.redpanda,
                       username=username,
                       password=self.password,
                       sasl_mechanism=self.algorithm)

    def get_super_client(self):
        username, password, _ = self.redpanda.SUPERUSER_CREDENTIALS
        return RpkTool(self.redpanda,
                       username=username,
                       password=password,
                       sasl_mechanism=self.algorithm)

    def prepare_users(self):
        """
        Create users and ACLs

        TODO:
          - wait for users to propogate
        """
        admin = Admin(self.redpanda)
        client = self.get_super_client()

        # base case user is not a superuser and has no configured ACLs
        admin.create_user("base", self.password, self.algorithm)

        admin.create_user("cluster_describe", self.password, self.algorithm)
        client.acl_create_allow_cluster("cluster_describe", "describe")

    @cluster(num_nodes=3)
    def test_describe_acls(self):
        """
        security::acl_operation::describe, security::default_cluster_name
        """
        self.prepare_users()

        try:
            self.get_client("base").acl_list()
            assert False, "list acls should have failed"
        except ClusterAuthorizationError:
            pass

        self.get_client("cluster_describe").acl_list()
        self.get_super_client().acl_list()
