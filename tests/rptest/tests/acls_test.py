# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.mark.resource import cluster
from rptest.services.admin import Admin
from rptest.clients.kafka_cli_tools import KafkaCliTools, ClusterAuthorizationError


class AccessControlListTest(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, test_context):
        extra_rp_conf = dict(
            developer_mode=True,
            enable_sasl=True,
        )
        super(AccessControlListTest,
              self).__init__(test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

    def get_client(self, user):
        return KafkaCliTools(self.redpanda, user=user, passwd=self.password)

    def get_super_client(self):
        user, password, _ = self.redpanda.SUPERUSER_CREDENTIALS
        return KafkaCliTools(self.redpanda, user=user, passwd=password)

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
        client.create_cluster_acls("cluster_describe", "describe")

    @cluster(num_nodes=3)
    def test_describe_acls(self):
        """
        security::acl_operation::describe, security::default_cluster_name
        """
        self.prepare_users()

        try:
            self.get_client("base").list_acls()
            assert False, "list acls should have failed"
        except ClusterAuthorizationError:
            pass

        self.get_client("cluster_describe").list_acls()
        self.get_super_client().list_acls()
