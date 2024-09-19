# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time
from ducktape.utils.util import wait_until
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from ducktape.mark import matrix
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable


class ClusterJoinTests(RedpandaTest):
    """
    Test that exercises cluster formation when node IDs are automatically
    assigned by Redpanda.
    """
    def __init__(self, test_context):
        super(ClusterJoinTests, self).__init__(test_context=test_context,
                                               num_brokers=4)
        self.admin = self.redpanda._admin

    def setUp(self):
        pass
        self._create_initial_topics()

    def node_ids(self):
        return [self.redpanda.node_id(n) for n in self.redpanda.nodes]

    def get_brokers(self):
        admin = Admin(self.redpanda)
        node_brokers = {}
        for n in self.redpanda.started_nodes():
            node_brokers[n.account.hostname] = admin.get_brokers()

        return node_brokers

    def broker_count_consistent(self, expected_brokers):
        node_brokers = self.get_brokers()
        if len(node_brokers) != expected_brokers:
            self.logger.info(
                f"Inconsistent number of broker lists in {node_brokers}, expected {expected_brokers} brokers"
            )
            return False
        for n, brokers in node_brokers.items():
            if len(brokers) != expected_brokers:
                self.logger.info(
                    f"Inconsistent number of brokers returned from  {n}, expected {expected_brokers} brokers, have: {brokers}"
                )
                return False
        return True

    def start_nodes(self, node_count, **kwargs):
        self.redpanda.set_seed_servers(self.redpanda.nodes[:node_count])
        self.redpanda.start(nodes=self.redpanda.nodes[:node_count], **kwargs)

    @cluster(num_nodes=4)
    @matrix(auto_assign_ids=[True, False])
    def test_simple_cluster_join(self, auto_assign_ids):
        self.start_nodes(3,
                         auto_assign_node_id=auto_assign_ids,
                         omit_seeds_on_idx_one=False)

        assert self.broker_count_consistent(3)

        self.redpanda.start_node(self.redpanda.nodes[-1],
                                 auto_assign_node_id=auto_assign_ids,
                                 omit_seeds_on_idx_one=False)

        wait_until(lambda: self.broker_count_consistent(4),
                   10,
                   err_msg="Error waiting for cluster to have 4 brokers")

    @cluster(num_nodes=4)
    def test_rejoin_with_the_same_id_and_wiped_disk(self):
        """
        Reusing the same node id for the node with different identity is forbidden 
        """
        # start all nodes
        self.start_nodes(4,
                         auto_assign_node_id=False,
                         omit_seeds_on_idx_one=False)

        assert self.broker_count_consistent(4)
        stopped_node = self.redpanda.nodes[-1]
        stopped_node_id = self.redpanda.node_id(stopped_node)
        self.redpanda.stop_node(stopped_node)
        self.redpanda.clean_node(stopped_node,
                                 preserve_logs=False,
                                 preserve_current_install=True)
        # node should not be allowed to interact with the cluster as its identity (UUID)
        # changed with the disk wipe
        self.redpanda.start_node(stopped_node,
                                 auto_assign_node_id=False,
                                 omit_seeds_on_idx_one=False,
                                 skip_readiness_check=True)
        admin = Admin(self.redpanda)

        def stopped_node_is_offline():
            statuses = []
            for n in self.redpanda.nodes[0:3]:
                brokers = admin.get_brokers(n)
                statuses += [
                    b['is_alive'] == False for b in brokers
                    if b['node_id'] == stopped_node_id
                ]

            return all(statuses)

        wait_until(stopped_node_is_offline,
                   20,
                   err_msg="Stopped node is still reported as alive")

        # check if node is unable to join the cluster
        for i in range(0, 10):
            brokers = admin.get_brokers(random.choice(self.redpanda.nodes[:3]))
            for b in brokers:
                if b['node_id'] == stopped_node_id:
                    assert b['is_alive'] == False
            time.sleep(1)

        # validate that Admin API is unreachable
        exception_raised = False
        try:
            client = KafkaAdminClient(
                bootstrap_servers=self.redpanda.broker_address(stopped_node))
            client.list_topics()
            assert False, "Kafka API call expected to throw an exception as the node should be unavailable and its Kafka listener should not be started"
        except NoBrokersAvailable as e:
            exception_raised = True

        assert exception_raised, "Broker should not start Kafka listener before joining the cluster"