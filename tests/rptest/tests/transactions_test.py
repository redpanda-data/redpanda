# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import confluent_kafka as ck
import time
import uuid
import random
import string
import subprocess

from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.rpk_consumer import RpkConsumer
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.upgrade_with_workload import MixedVersionWorkloadRunner
from rptest.util import wait_until_result


class TransactionsTestBase(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context, extra_rp_conf=None):
        if not extra_rp_conf:
            extra_rp_conf = {
                "enable_idempotence": True,
                "enable_transactions": True,
                "transaction_coordinator_replication": 3,
                "id_allocator_replication": 3,
                "enable_leader_balancer": False,
                "partition_autobalancing_mode": "off",
            }

        super(TransactionsTestBase, self).__init__(test_context=test_context,
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


class TransactionsTest(TransactionsTestBase):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        super(TransactionsTest, self).__init__(test_context)

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


class UpgradeTransactionTest(RedpandaTest):
    def consume(self, consumer, max_records=10, timeout_s=2):
        def consume_records():
            records = consumer.consume(max_records, timeout_s)

            if records:
                return True, records
            else:
                False

        return wait_until_result(consume_records,
                                 timeout_sec=30,
                                 backoff_sec=2,
                                 err_msg="Can not consume data")

    def check_consume(self, topic_name, max_tx):
        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': f"consumer-{uuid.uuid4()}",
            'auto.offset.reset': 'earliest',
        })

        consumer.subscribe([topic_name])

        num_consumed = 0
        prev_rec = bytes("0", 'UTF-8')

        while num_consumed < max_tx:
            self.redpanda.logger.debug(
                f"Consumed {num_consumed}. Should consume at the end {max_tx}")
            records = self.consume(consumer)

            for record in records:
                assert prev_rec == record.key()
                prev_rec = bytes(str(int(prev_rec) + 1), 'UTF-8')

            num_consumed += len(records)

        assert num_consumed == max_tx

    @cluster(num_nodes=2, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def check_parsing_test(self):
        self.redpanda = RedpandaService(
            self.test_context,
            1,
            extra_rp_conf={
                "enable_idempotence": True,
                "enable_transactions": True,
                "transaction_coordinator_replication": 1,
                "id_allocator_replication": 1,
                "enable_leader_balancer": False,
                "enable_auto_rebalance_on_node_add": False,
            },
            environment={"__REDPANDA_LOGICAL_VERSION": 5})

        self.redpanda.start()

        spec = TopicSpec(partition_count=1, replication_factor=1)
        self._client = DefaultClient(self.redpanda)
        self.client().create_topic(spec)
        topic_name = spec.name

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '123',
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()

        def on_del(err, msg):
            assert err == None

        max_tx = 100
        for i in range(max_tx):
            producer.begin_transaction()
            producer.produce(topic_name, str(i), str(i), 0, on_del)
            producer.commit_transaction()
        producer.flush()

        self.check_consume(topic_name, max_tx)

        self.redpanda.set_environment({"__REDPANDA_LOGICAL_VERSION": 6})
        for n in self.redpanda.nodes:
            self.redpanda.restart_nodes(n, stop_timeout=60)

        self.check_consume(topic_name, max_tx)


class UpgradeWithMixedVeersionTransactionTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "enable_transactions": True,
            "transaction_coordinator_replication": 1,
            "id_allocator_replication": 1,
            "enable_leader_balancer": False,
        }

        super(UpgradeWithMixedVeersionTransactionTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

        self.installer = self.redpanda._installer

    def on_delivery(self, err, msg):
        assert err == None, msg

    def check_consume(self, max_records):
        topic_name = self.topics[0].name

        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': f"consumer-{uuid.uuid4()}",
            'auto.offset.reset': 'earliest',
        })

        consumer.subscribe([topic_name])
        num_consumed = 0
        prev_rec = bytes("0", 'UTF-8')

        while num_consumed != max_records:
            max_consume_records = 10
            timeout = 10
            records = consumer.consume(max_consume_records, timeout)

            for record in records:
                assert prev_rec == record.key(), f"{prev_rec}, {record.key()}"
                prev_rec = bytes(str(int(prev_rec) + 1), 'UTF-8')

            num_consumed += len(records)

        consumer.close()

    def setUp(self):
        self.installer.install(self.redpanda.nodes, (22, 1, 5))
        super(UpgradeWithMixedVeersionTransactionTest, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def upgrade_test(self):
        topic_name = self.topics[0].name
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.5" in unique_versions, unique_versions

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(topic_name, "0", "0", 0, self.on_delivery)
        producer.commit_transaction()
        producer.flush()

        self.check_consume(1)

        def get_tx_manager_broker():
            admin = Admin(self.redpanda)
            leader_id = admin.get_partition_leader(namespace="kafka_internal",
                                                   topic="tx",
                                                   partition=0)
            return self.redpanda.get_node(leader_id)

        node_with_tx_manager = get_tx_manager_broker()

        # Update node with tx manager
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(node_with_tx_manager)
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert "v22.1.5" in unique_versions, unique_versions

        # Init dispatch by using old node. Transaction should work
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(topic_name, "1", "1", 0, self.on_delivery)
        producer.commit_transaction()
        producer.flush()

        self.check_consume(2)

        self.installer.install(self.redpanda.nodes, (22, 1, 5))
        self.redpanda.restart_nodes(node_with_tx_manager)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.5" in unique_versions, unique_versions

        self.check_consume(2)


class MixedVersionTransactionsTest(TransactionsTestBase):
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "enable_auto_rebalance_on_node_add": False,
            "enable_idempotence": True,
            "enable_leader_balancer": False,
            "enable_transactions": True,
            "id_allocator_replication": 3,
            "group_topic_partitions": 1,
            "transaction_coordinator_replication": 3,
        }
        super(MixedVersionTransactionsTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)
        self.initial_version = MixedVersionWorkloadRunner.PRE_SERDE_VERSION
        self.txn_id = 0

    def setUp(self):
        self.redpanda._installer.install(self.redpanda.nodes,
                                         self.initial_version)
        super(MixedVersionTransactionsTest, self).setUp()

    def txn_workload(self, src_node, dst_node):
        """
        Performs several transactional operations. This exercises requests sent
        by the transaction coordinator, and received at the consumer group and
        topic leaders.
        """
        src_id = self.redpanda.idx(src_node)
        dst_id = self.redpanda.idx(dst_node)
        admin = self.redpanda._admin
        # Have the transaction coordinator start out on 'src_node'.
        wait_until(
            lambda: admin.transfer_leadership_to(namespace="kafka_internal",
                                                 topic="tx",
                                                 partition=0,
                                                 target_id=src_id),
            timeout_sec=30,
            backoff_sec=1)
        # The receivers of transaction coordinator RPCs (the leader of the
        # topic being written to and the group topic) should be on 'dst_node'.
        wait_until(
            lambda: admin.transfer_leadership_to(namespace="kafka",
                                                 topic="__consumer_offsets",
                                                 partition=0,
                                                 target_id=dst_id),
            timeout_sec=30,
            backoff_sec=1)
        wait_until(
            lambda: admin.transfer_leadership_to(namespace="kafka",
                                                 topic=self.output_t.name,
                                                 partition=0,
                                                 target_id=dst_id),
            timeout_sec=30,
            backoff_sec=1)

        def run_txn(should_commit):
            """
            Runs through the steps of a transaction, using consumer groups. If
            'should_commit' is True, commits the transactions, otherwise
            aborts.

            NOTE: this uses confluent_kafka, which doesn't do anything fancy in
            the background, e.g. aggressively fetching coordinator leaders.
            """
            self.generate_data(self.output_t, self.max_records)
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': str(self.txn_id),
            })
            producer.init_transactions()
            consumer = ck.Consumer({
                'bootstrap.servers': self.redpanda.brokers(),
                'group.id': "test",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            })

            consumer.subscribe([self.output_t])
            num_consumed_records = 0

            producer.begin_transaction()
            while num_consumed_records < self.max_records:
                records = self.consume(consumer)
                for r in records:
                    producer.produce(self.output_t.name, r.value(), r.key())
                num_consumed_records += len(records)

            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata())

            if should_commit:
                producer.commit_transaction()
            else:
                producer.abort_transaction()
            producer.flush()
            consumer.close()

            self.txn_id += 1

        run_txn(should_commit=True)
        run_txn(should_commit=False)

    @cluster(num_nodes=3)
    def test_txn_rpcs_with_upgrade(self):
        # Initialize a transaction to bootstrap our transaction internal topic.
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': str(self.txn_id),
        })
        producer.init_transactions()
        self.generate_data(self.output_t, self.max_records)
        self.txn_id += 1
        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "dummy",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        consumer.subscribe([self.output_t])
        self.consume(consumer)

        MixedVersionWorkloadRunner.upgrade_with_workload(
            self.redpanda, MixedVersionWorkloadRunner.PRE_SERDE_VERSION,
            self.txn_workload)
