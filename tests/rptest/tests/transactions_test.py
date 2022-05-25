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

import time

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_consumer import RpkConsumer
import confluent_kafka as ck
from rptest.clients.kafka_cat import KafkaCat

import subprocess


class TransactionsTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "enable_transactions": True,
            "transaction_coordinator_replication": 3,
            "id_allocator_replication": 3,
            "enable_leader_balancer": False,
            "enable_auto_rebalance_on_node_add": False
        }

        super(TransactionsTest, self).__init__(test_context=test_context,
                                               extra_rp_conf=extra_rp_conf)

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100

    def generate_data(self, topic, num_records):
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
        })

        for i in range(num_records):
            producer.produce(topic.name, str(i), str(i), 0)

        producer.flush()

    @cluster(num_nodes=3)
    def simple_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        producer.init_transactions()

        consumer.subscribe([self.input_t])

        num_consumed_records = 0
        consume_for_one_it = 10

        while num_consumed_records != self.max_records:
            records = consumer.consume(consume_for_one_it)
            assert (len(records) != 0)

            producer.begin_transaction()

            for record in records:
                assert (record.error() == None)
                producer.produce(self.output_t.name, record.value(),
                                 record.key(), 0)

            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata())

            producer.commit_transaction()

            num_consumed_records += consume_for_one_it

        consumer = ck.Consumer({
            'group.id': "testtest",
            'bootstrap.servers': self.redpanda.brokers(),
            'auto.offset.reset': 'earliest',
        })
        consumer.subscribe([self.output_t])

        final_consume_cnt = 0
        while True:
            record = consumer.poll(consume_for_one_it)
            if record is None:
                break

            expected = bytes(str(final_consume_cnt), 'UTF-8')
            assert (record.key() == expected)
            assert (record.value() == expected)
            final_consume_cnt += 1

        assert (final_consume_cnt == self.max_records)

    @cluster(num_nodes=3)
    def rejoin_member_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        group_name = "test"
        consumer1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        producer.init_transactions()

        consumer1.subscribe([self.input_t])
        records = consumer1.consume(10)
        assert (len(records) != 0)

        producer.begin_transaction()

        for record in records:
            assert (record.error() == None)
            producer.produce(self.output_t.name, record.value(), record.key(),
                             0)

        offsets = consumer1.position(consumer1.assignment())
        metadata = consumer1.consumer_group_metadata()

        consumer2 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        consumer2.subscribe([self.input_t])
        records = consumer2.consume(10)

        try:
            producer.send_offsets_to_transaction(offsets, metadata, 2)
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError.UNKNOWN_MEMBER_ID

        producer.abort_transaction()

    @cluster(num_nodes=3)
    def change_static_member_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        group_name = "test"
        static_group_id = "123"
        consumer1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'group.instance.id': static_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        producer.init_transactions()

        consumer1.subscribe([self.input_t])
        records = consumer1.consume(10)
        assert (len(records) != 0)

        producer.begin_transaction()

        for record in records:
            assert (record.error() == None)
            producer.produce(self.output_t.name, record.value(), record.key(),
                             0)

        offsets = consumer1.position(consumer1.assignment())
        metadata = consumer1.consumer_group_metadata()

        consumer2 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'group.instance.id': static_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        consumer2.subscribe([self.input_t])
        records = consumer2.consume(10)

        try:
            producer.send_offsets_to_transaction(offsets, metadata, 2)
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError.FENCED_INSTANCE_ID

        producer.abort_transaction()
