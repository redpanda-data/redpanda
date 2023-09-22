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
from rptest.clients.kafka_cat import KafkaCat
from ducktape.utils.util import wait_until
from rptest.clients.default import DefaultClient


class TxListOffsets(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
            "transaction_coordinator_partitions": 1
        }

        super(TxListOffsets, self).__init__(test_context=test_context,
                                            extra_rp_conf=extra_rp_conf,
                                            log_level="trace")

    @cluster(num_nodes=3)
    def commit_a_test(self):
        client = DefaultClient(self.redpanda)
        self.topics = [
            TopicSpec(partition_count=1,
                      replication_factor=3,
                      cleanup_policy=TopicSpec.CLEANUP_COMPACT)
        ]
        client.create_topic(self.topics[0])

        wait_until(lambda: len(self.redpanda.started_nodes()) == 3,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Failed to get a stable cluster")

        kc = KafkaCat(self.redpanda)
        start, end = kc.list_offsets(self.topics[0].name, 0)
        self.logger.info(f"SHAI: start:{start} end:{end}")

        producer = Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': f"tx0",
        })
        producer.init_transactions()

        for x in range(2):
            producer.begin_transaction()
            for i in range(8):
                producer.produce(self.topic,
                                 f"key{i}",
                                 f"value{i}",
                                 partition=0)
                producer.flush()
            producer.commit_transaction()

        start, end = kc.list_offsets(self.topics[0].name, 0)
        self.logger.info(f"SHAI: start:{start} end:{end}")

        assert len(self.redpanda.started_nodes(
        )) == 3, f"only {len(self.redpanda.started_nodes())} nodes are running"

        nodes = []
        for node in list(self.redpanda.started_nodes()):
            nodes.append(node)
            self.logger.info(f"SHAI: stoping {node.account.hostname}")
            self.redpanda.stop_node(node)

        for node in nodes:
            self.logger.info(f"SHAI: starting {node.account.hostname}")
            self.redpanda.start_node(node)

        start, end = kc.list_offsets(self.topics[0].name, 0)
        self.logger.info(f"SHAI: start:{start} end:{end}")

    @cluster(num_nodes=3)
    def commit_b_test(self):
        client = DefaultClient(self.redpanda)
        self.topics = [
            TopicSpec(partition_count=1,
                      replication_factor=3,
                      cleanup_policy=TopicSpec.CLEANUP_DELETE)
        ]
        client.create_topic(self.topics[0])

        wait_until(lambda: len(self.redpanda.started_nodes()) == 3,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Failed to get a stable cluster")

        kc = KafkaCat(self.redpanda)
        start, end = kc.list_offsets(self.topics[0].name, 0)
        self.logger.info(f"SHAI: start:{start} end:{end}")

        producer = Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': f"tx0",
        })
        producer.init_transactions()

        for x in range(2):
            producer.begin_transaction()
            for i in range(8):
                producer.produce(self.topic,
                                 f"key{i}",
                                 f"value{i}",
                                 partition=0)
                producer.flush()
            producer.commit_transaction()

        start, end = kc.list_offsets(self.topics[0].name, 0)
        self.logger.info(f"SHAI: start:{start} end:{end}")

        assert len(self.redpanda.started_nodes(
        )) == 3, f"only {len(self.redpanda.started_nodes())} nodes are running"

        nodes = []
        for node in list(self.redpanda.started_nodes()):
            nodes.append(node)
            self.logger.info(f"SHAI: stoping {node.account.hostname}")
            self.redpanda.stop_node(node)

        for node in nodes:
            self.logger.info(f"SHAI: starting {node.account.hostname}")
            self.redpanda.start_node(node)

        start, end = kc.list_offsets(self.topics[0].name, 0)
        self.logger.info(f"SHAI: start:{start} end:{end}")
