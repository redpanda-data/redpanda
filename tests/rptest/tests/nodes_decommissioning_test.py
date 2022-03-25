# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST


class NodesDecommissioningTest(EndToEndTest):
    """
    Basic nodes decommissioning test.
    """
    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_working_node(self):
        self.start_redpanda(num_nodes=4)
        topics = []
        for partition_count in range(1, 5):
            for replication_factor in (1, 3):
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)
        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission['node_id'])

        def node_removed():
            brokers = admin.get_brokers()
            for b in brokers:
                if b['node_id'] == to_decommission['node_id']:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decommissioning_crashed_node(self):

        self.start_redpanda(num_nodes=4)
        topics = []
        for partition_count in range(1, 5):
            for replication_factor in (1, 3):
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        to_decommission = self.redpanda.nodes[1]
        node_id = 2
        self.redpanda.stop_node(node=to_decommission)
        self.logger.info(f"decommissioning node: {node_id}", )
        admin.decommission_broker(id=node_id)

        def node_removed():
            brokers = admin.get_brokers()
            for b in brokers:
                if b['node_id'] == node_id:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)
