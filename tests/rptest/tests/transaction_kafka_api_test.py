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
from ducktape.utils.util import wait_until
import confluent_kafka as ck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool


class TxKafkaApiTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3, replication_factor=3),
              TopicSpec(partition_count=3, replication_factor=3))

    def __init__(self, test_context):
        super(TxKafkaApiTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf={
                                 "tx_timeout_delay_ms": 10000000,
                                 "abort_timed_out_transactions_interval_ms":
                                 10000000,
                                 'enable_leader_balancer': False
                             })

        self.kafka_cli = KafkaCliTools(self.redpanda, "3.0.0")

    def extract_producer(self, tx):
        return (tx["ProducerId"], tx["ProducerEpoch"], tx["LastSequence"])

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

                txs_info = self.kafka_cli.describe_producers(
                    topic.name, partition)

                if expected_producers == None:
                    expected_producers = set(
                        map(self.extract_producer, txs_info))
                    assert (len(txs_info) == 2)

                assert (len(expected_producers) == len(txs_info))
                for producer in txs_info:
                    assert (self.extract_producer(producer)
                            in expected_producers)

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

        tx = self.kafka_cli.describe_transaction(tx_id)

        assert tx["TransactionalId"] == tx_id
        assert tx["TransactionTimeoutMs"] == str(60000)

        partitions = tx["TopicPartitions"].strip().split(",")
        expected_partitions = set(partitions)

        for topic in self.topics:
            for partition in range(topic.partition_count):
                tpoic_partition = f"{topic}-{partition}"
                assert tpoic_partition in expected_partitions

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

        txs_info = self.kafka_cli.list_transactions()
        for tx in txs_info:
            tx_info = self.kafka_cli.describe_transaction(
                tx["TransactionalId"])
            assert int(tx_info["ProducerId"]) == int(tx["ProducerId"])
