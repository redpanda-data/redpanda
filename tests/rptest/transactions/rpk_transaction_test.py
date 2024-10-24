# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
import confluent_kafka as ck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
import time
import json


class TxRpkTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3, replication_factor=3),
              TopicSpec(partition_count=3, replication_factor=3))

    def __init__(self, test_context):
        super(TxRpkTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf={
                                 "tx_timeout_delay_ms": 10000000,
                                 "abort_timed_out_transactions_interval_ms":
                                 10000000,
                                 "enable_leader_balancer": False,
                                 "transaction_coordinator_partitions": 4
                             })

        self._rpk = RpkTool(self.redpanda)

    def extract_producer(self, tx):
        return (tx["producer_id"], tx["producer_epoch"], tx["last_sequence"])

    @cluster(num_nodes=3)
    def test_describe_producers(self):
        producer1 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        })
        producer2 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '1',
        })
        producer1.init_transactions()
        producer2.init_transactions()
        producer1.begin_transaction()
        producer2.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producer1.produce(topic.name, '0', '0', partition)
                producer2.produce(topic.name, '0', '1', partition)

        producer1.flush()
        producer2.flush()

        expected_producers = None

        for topic in self.topics:
            for partition in range(topic.partition_count):

                txs_info = self._rpk.describe_txn_producers([topic.name],
                                                            [partition])
                if expected_producers is None:
                    expected_producers = set(
                        map(self.extract_producer, txs_info))
                    assert (len(txs_info) == 2)

                assert (len(expected_producers) == len(txs_info))
                for producer in txs_info:
                    assert (self.extract_producer(producer)
                            in expected_producers)

    @cluster(num_nodes=3)
    def test_last_timestamp_of_describe_producers(self):
        producer1 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        })
        producer1.init_transactions()
        producer1.begin_transaction()

        for _ in range(2):
            for topic in self.topics:
                for partition in range(topic.partition_count):
                    producer1.produce(topic.name, '0', '0', partition)
            producer1.flush()

        now_ms = int(time.time() * 1000)

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producers = self._rpk.describe_txn_producers([topic.name],
                                                             [partition])
                self.redpanda.logger.debug(json.dumps(producers))
                for producer in producers:
                    assert isinstance(producer, dict) and \
                        int(producer["last_sequence"]) > 0
                    # checking that the producer's info was recently updated
                    assert abs(now_ms -
                               int(producer["last_timestamp"])) < 120 * 1000

    @cluster(num_nodes=3)
    def test_describe_transactions(self):
        tx_id = "0"
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
        })
        producer.init_transactions()
        producer.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producer.produce(topic.name, '0', '0', partition)

        producer.flush()

        txn_partition = self._rpk.describe_txn(tx_id)
        for p in txn_partition:
            assert isinstance(p, dict) and p["transaction_id"] == tx_id
            assert isinstance(p, dict) and p["timeout"] == 60000

        # Check that we describe every topic-partition combination.
        for topic in self.topics:
            tx_filter = [
                txn for txn in txn_partition
                if isinstance(txn, dict) and txn['topic'] == topic.name
            ]
            assert len(tx_filter) == topic.partition_count
            for p in range(topic.partition_count):
                p_filter = [txn for txn in tx_filter if txn['partition'] == p]
                assert len(p_filter) == 1

    @cluster(num_nodes=3)
    def test_empty_list_transactions(self):
        txs_info = self._rpk.list_txn()
        assert len(txs_info) == 0

    @cluster(num_nodes=3)
    def test_list_transactions(self):
        producer1 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        })
        producer2 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '1',
        })
        producer1.init_transactions()
        producer2.init_transactions()
        producer1.begin_transaction()
        producer2.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producer1.produce(topic.name, '0', '0', partition)
                producer2.produce(topic.name, '0', '1', partition)

        producer1.flush()
        producer2.flush()

        txn_list = self._rpk.list_txn()
        assert len(txn_list) > 0

        for txn in txn_list:
            assert isinstance(txn, dict), \
                "Abnormal type returned when querying for txns"
            txn_described = self._rpk.describe_txn(txn["transaction_id"])
            for described in txn_described:
                assert isinstance(described, dict) and \
                    int(described["producer_id"]) == int(txn["producer_id"])
