# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.util import wait_until_result
from rptest.clients.types import TopicSpec

import time

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_consumer import RpkConsumer
import confluent_kafka as ck
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.admin import Admin

import subprocess

from rptest.clients.rpk import RpkTool


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
            "partition_autobalancing_mode": "off",
        }

        super(TransactionsTest, self).__init__(test_context=test_context,
                                               extra_rp_conf=extra_rp_conf)

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100

    def on_delivery(self, err, msg):
        assert err == None, msg

    def generate_data(self, topic, num_records):
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
        })

        for i in range(num_records):
            producer.produce(topic.name,
                             str(i),
                             str(i),
                             on_delivery=self.on_delivery)

        producer.flush()

    def consume(self, consumer, max_records=10, timeout_s=2):
        def consume_records():
            records = consumer.consume(max_records, timeout_s)

            if (records != None) and (len(records) != 0):
                return True, records
            else:
                False

        return wait_until_result(consume_records,
                                 timeout_sec=30,
                                 backoff_sec=2,
                                 err_msg="Can not consume data")

    @cluster(num_nodes=3)
    def simple_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        consumer1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        producer.init_transactions()

        consumer1.subscribe([self.input_t])

        num_consumed_records = 0
        consumed_from_input_topic = []
        while num_consumed_records != self.max_records:
            # Imagine that consume got broken, we read the same record twice and overshoot the condition
            assert num_consumed_records < self.max_records

            records = self.consume(consumer1)

            producer.begin_transaction()

            for record in records:
                assert (record.error() == None)
                consumed_from_input_topic.append(record)
                producer.produce(self.output_t.name,
                                 record.value(),
                                 record.key(),
                                 on_delivery=self.on_delivery)

            producer.send_offsets_to_transaction(
                consumer1.position(consumer1.assignment()),
                consumer1.consumer_group_metadata())

            producer.commit_transaction()

            num_consumed_records += len(records)

        producer.flush()
        consumer1.close()
        assert len(consumed_from_input_topic) == self.max_records

        consumer2 = ck.Consumer({
            'group.id': "testtest",
            'bootstrap.servers': self.redpanda.brokers(),
            'auto.offset.reset': 'earliest',
        })
        consumer2.subscribe([self.output_t])

        index_from_input = 0

        while index_from_input < self.max_records:
            records = self.consume(consumer2)

            for record in records:
                assert record.key(
                ) == consumed_from_input_topic[index_from_input].key(
                ), f'Records key does not match from input {consumed_from_input_topic[index_from_input].key()}, from output {record.key()}'
                assert record.value(
                ) == consumed_from_input_topic[index_from_input].value(
                ), f'Records value does not match from input {consumed_from_input_topic[index_from_input].value()}, from output {record.value()}'
                index_from_input += 1

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
        records = self.consume(consumer1)

        producer.begin_transaction()

        for record in records:
            assert (record.error() == None)
            producer.produce(self.output_t.name, record.value(), record.key())

        offsets = consumer1.position(consumer1.assignment())
        metadata = consumer1.consumer_group_metadata()

        consumer2 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        consumer2.subscribe([self.input_t])
        # Rejoin can take some time, so we should pass big timeout
        self.consume(consumer2, timeout_s=360)

        try:
            producer.send_offsets_to_transaction(offsets, metadata, 2)
            assert False, "send_offsetes should fail"
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
        records = self.consume(consumer1)

        producer.begin_transaction()

        for record in records:
            assert (record.error() == None)
            producer.produce(self.output_t.name, record.value(), record.key())

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
        self.consume(consumer2)

        try:
            producer.send_offsets_to_transaction(offsets, metadata, 2)
            assert False, "send_offsetes should fail"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError.FENCED_INSTANCE_ID

        producer.abort_transaction()

    @cluster(num_nodes=3)
    def delete_topic_with_active_txns_test(self):

        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)
        rpk.create_topic("t1")
        rpk.create_topic("t2")

        # Transactional
        producer_t = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            # Intentional long timeout to avoid cleanup of state.
            'transaction.timeout.ms': 10000000,
        })

        # Non transactional
        producer_nt = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
        })

        # Two consumers for 't2' with different isolation levels.
        consumer_1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'group.id': 'test1',
            'isolation.level': 'read_committed',
        })

        consumer_2 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'group.id': 'test2',
            'isolation.level': 'read_uncommitted',
        })

        consumer_1.subscribe([TopicSpec(name='t2')])
        consumer_2.subscribe([TopicSpec(name='t2')])

        producer_t.init_transactions()
        producer_t.begin_transaction()

        def add_records(topic, producer):
            for i in range(0, 100):
                producer.produce(topic,
                                 str(i),
                                 str(i),
                                 partition=0,
                                 on_delivery=self.on_delivery)
            producer.flush()

        add_records("t1", producer_t)
        add_records("t2", producer_t)

        rpk.delete_topic("t1")

        try:
            producer_t.commit_transaction()
            assert False, "commit_tx did not throw"
        except Exception as e:
            self.redpanda.logger.error(e)

        # Attempt to write some non transactional batches
        add_records("t2", producer_nt)

        committed_records = 100  # from nt producer
        all_records = 200  # Includes non committed flush

        def consume_records(consumer, count):
            total = 0
            while total != count:
                total += len(self.consume(consumer))

        # Make sure that the consumer on a different topic is not blocked due to LSO
        # .. and the abort marker correctly written in the remaining partitions
        consume_records(consumer_1, committed_records)
        consume_records(consumer_2, all_records)

        no_txns = lambda: len(admin.get_all_transactions()) == 0
        try:
            wait_until_result(no_txns,
                              timeout_sec=30,
                              backoff_sec=2,
                              err_msg="Failed to expire transactions.")
        except TimeoutError:
            assert False, admin.get_all_transactions()
