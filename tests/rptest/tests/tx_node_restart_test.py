# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec

from rptest.tests.redpanda_test import RedpandaTest
from confluent_kafka.cimpl import KafkaException, KafkaError
from confluent_kafka import Producer


class TxNodeRestartTest(RedpandaTest):
    topics = [TopicSpec(partition_count=1, replication_factor=3)]

    def __init__(self, test_context):
        extra_rp_conf = {"transaction_coordinator_partitions": 1}

        super(TxNodeRestartTest, self).__init__(test_context=test_context,
                                                extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_transactions_on_different_coordinators(self):
        producers = []
        for i in range(20):
            p = Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': f"tx_{i}",
            })
            p.init_transactions()
            producers.append(p)

        for p in producers:
            p.begin_transaction()
            p.produce(self.topic, "key", "value", partition=0)

        admin = Admin(self.redpanda)

        c_tx = admin.find_tx_coordinator("tx_0")
        node = self.redpanda.get_node_by_id(c_tx['coordinator'])
        self.logger.info(f"Transactional coordinator on node {c_tx}")
        self.redpanda.stop_node(node, forced=True)

        c_tx = admin.find_tx_coordinator("tx_0")
        self.logger.info(
            f"Transactional coordinator info after restart: {c_tx}")
        for p in producers:
            p.commit_transaction()
