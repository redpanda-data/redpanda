# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec

from rptest.tests.redpanda_test import RedpandaTest
from confluent_kafka.cimpl import KafkaException, KafkaError
from confluent_kafka import Producer
from rptest.clients.rpk import RpkTool


class TxOverflowTest(RedpandaTest):
    topics = [TopicSpec(partition_count=1, replication_factor=3)]

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
            "transaction_coordinator_partitions": 1
        }

        super(TxOverflowTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf,
                                             log_level="trace")

    def set_max_transactions_per_coordinator(self, n):
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("max_transactions_per_coordinator", str(n))

    @cluster(num_nodes=3)
    def underflow_test(self):
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
            p.commit_transaction()

    @cluster(num_nodes=3)
    def overflow_test(self):
        self.set_max_transactions_per_coordinator(10)

        producers = []
        for i in range(20):
            p = Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': f"tx_{i}",
            })
            p.init_transactions()
            producers.append(p)

        oldest_producer = producers[0]
        oldest_producer.begin_transaction()
        oldest_producer.produce(self.topic, "key", "value", partition=0)
        try:
            oldest_producer.commit_transaction()
            assert False, ""
        except KafkaException as e:
            assert e.args[0].code(
            ) == KafkaError.INVALID_PRODUCER_ID_MAPPING, f"observed code={e.args[0].code()}"
