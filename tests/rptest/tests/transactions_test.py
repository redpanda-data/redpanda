# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from rptest.services.cluster import cluster
from rptest.util import wait_until_result
from rptest.clients.types import TopicSpec
from time import sleep, time
from os.path import join

import uuid
import random

from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.redpanda import RedpandaService, make_redpanda_service
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
import confluent_kafka as ck
from rptest.services.admin import Admin
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.clients.rpk import RpkTool

from rptest.tests.cluster_features_test import FeaturesTestBase


class TransactionsMixin:
    def find_coordinator(self, txid, node=None):
        if node == None:
            node = random.choice(self.redpanda.started_nodes())

        def find_tx_coordinator():
            r = self.admin.find_tx_coordinator(txid, node=node)
            return r["ec"] == 0, r

        return wait_until_result(
            find_tx_coordinator,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Can't find a coordinator for tx.id={txid}")


class TransactionsTest(RedpandaTest, TransactionsMixin):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(TransactionsTest, self).__init__(test_context=test_context,
                                               extra_rp_conf=extra_rp_conf,
                                               log_level="trace")

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100
        self.admin = Admin(self.redpanda)

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
    def find_coordinator_creates_tx_topics_test(self):
        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert not node.account.exists(path)

        self.find_coordinator("tx0")

        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert node.account.exists(path)
                assert node.account.isdir(path)

    @cluster(num_nodes=3)
    def init_transactions_creates_eos_topics_test(self):
        for node in self.redpanda.started_nodes():
            for tx_topic in ["id_allocator", "tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert not node.account.exists(path)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()

        for node in self.redpanda.started_nodes():
            for tx_topic in ["id_allocator", "tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert node.account.exists(path)
                assert node.account.isdir(path)

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
            assert kafka_error.code() == ck.cimpl.KafkaError._FENCED

        try:
            # if abort fails an app should recreate a producer otherwise
            # it may continue to use the original producer
            producer.abort_transaction()
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError._FENCED

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
    def expired_tx_test(self):
        # confluent_kafka client uses the same timeout both for init_transactions
        # and produce; we want to test expiration on produce so we need to keep
        # the timeout low to avoid long sleeps in the test but when we set it too
        # low init_transactions throws NOT_COORDINATOR. using explicit reties on
        # it to overcome the problem
        #
        # for explanation see
        # https://github.com/redpanda-data/redpanda/issues/7991

        timeout_s = 30
        begin = time()
        while True:
            assert time(
            ) - begin <= timeout_s, f"Can't init transactions within {timeout_s} sec"
            try:
                producer = ck.Producer({
                    'bootstrap.servers':
                    self.redpanda.brokers(),
                    'transactional.id':
                    '0',
                    'transaction.timeout.ms':
                    5000,
                })
                producer.init_transactions()
                break
            except ck.cimpl.KafkaException as e:
                self.redpanda.logger.debug(f"error on init_transactions",
                                           exc_info=True)
                kafka_error = e.args[0]
                assert kafka_error.code() in [
                    ck.cimpl.KafkaError.NOT_COORDINATOR,
                    ck.cimpl.KafkaError._TIMED_OUT
                ]

        producer.begin_transaction()

        for i in range(0, 10):
            producer.produce(self.input_t.name,
                             str(i),
                             str(i),
                             partition=0,
                             on_delivery=self.on_delivery)
        producer.flush()
        sleep(10)
        try:
            producer.commit_transaction()
            assert False, "tx is expected to be expired"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError._FENCED

    @cluster(num_nodes=3)
    def graceful_leadership_transfer_test(self):

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 60000,
        })

        producer.init_transactions()
        producer.begin_transaction()

        count = 0
        partition = 0
        records_per_add = 10

        def add_records():
            nonlocal count
            nonlocal partition
            for i in range(count, count + records_per_add):
                producer.produce(self.input_t.name,
                                 str(i),
                                 str(i),
                                 partition=partition,
                                 on_delivery=self.on_delivery)
            producer.flush()
            count = count + records_per_add

        def graceful_transfer():
            # Issue a graceful leadership transfer.
            old_leader = self.admin.get_partition_leader(
                namespace="kafka",
                topic=self.input_t.name,
                partition=partition)
            self.admin.transfer_leadership_to(namespace="kafka",
                                              topic=self.input_t.name,
                                              partition=partition,
                                              target_id=None)

            def leader_is_changed():
                new_leader = self.admin.get_partition_leader(
                    namespace="kafka",
                    topic=self.input_t.name,
                    partition=partition)
                return (new_leader != -1) and (new_leader != old_leader)

            wait_until(leader_is_changed,
                       timeout_sec=30,
                       backoff_sec=2,
                       err_msg="Failed to establish current leader")

        # Add some records
        add_records()
        # Issue a leadership transfer
        graceful_transfer()
        # Add some more records
        add_records()
        # Issue another leadership transfer
        graceful_transfer()
        # Issue a commit
        producer.commit_transaction()

        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        try:
            consumer.subscribe([self.input_t])
            records = []
            while len(records) != count:
                records.extend(
                    self.consume(consumer, max_records=count, timeout_s=10))
            assert len(
                records
            ) == count, f"Not all records consumed, expected {count}"
            keys = set([int(r.key()) for r in records])
            assert all(i in keys
                       for i in range(0, count)), f"Missing records {keys}"
        finally:
            consumer.close()

    @cluster(num_nodes=3)
    def graceful_leadership_transfer_tx_coordinator_test(self):

        p_count = 10
        producers = [
            ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': str(i),
                'transaction.timeout.ms': 1000000,
            }) for i in range(0, p_count)
        ]

        # Initiate the transactions, should hit the existing tx coordinator.
        for p in producers:
            p.init_transactions()
            p.begin_transaction()

        count = 0
        partition = 0
        records_per_add = 10

        def add_records():
            nonlocal count
            nonlocal partition
            for p in producers:
                for i in range(count, count + records_per_add):
                    p.produce(self.input_t.name,
                              str(i),
                              str(i),
                              partition=partition,
                              on_delivery=self.on_delivery)
                p.flush()
                count = count + records_per_add

        def graceful_transfer():
            # Issue a graceful leadership transfer of tx coordinator
            old_leader = self.admin.get_partition_leader(
                namespace="kafka_internal", topic="tx",
                partition="0")  # Fix this when we partition tx coordinator.
            self.admin.transfer_leadership_to(namespace="kafka_internal",
                                              topic="tx",
                                              partition="0",
                                              target_id=None)

            def leader_is_changed():
                new_leader = self.admin.get_partition_leader(
                    namespace="kafka_internal", topic="tx", partition="0")
                return (new_leader != -1) and (new_leader != old_leader)

            wait_until(leader_is_changed,
                       timeout_sec=30,
                       backoff_sec=2,
                       err_msg="Failed to establish current leader")

        # Add some records
        add_records()
        # Issue a leadership transfer
        graceful_transfer()
        # Add some more records
        add_records()
        # Issue another leadership transfer
        graceful_transfer()
        # Issue a commit on half of the producers
        for p in range(0, int(p_count / 2)):
            producers[p].commit_transaction()
        # Issue a leadership transfer and then commit the rest.
        graceful_transfer()
        for p in range(int(p_count / 2), p_count):
            producers[p].commit_transaction()

        # Verify that all the records are ingested correctly.
        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        try:
            consumer.subscribe([self.input_t])
            records = []
            while len(records) != count:
                records.extend(
                    self.consume(consumer, max_records=count, timeout_s=10))
            assert len(
                records
            ) == count, f"Not all records consumed, expected {count}"
            keys = set([int(r.key()) for r in records])
            assert all(i in keys
                       for i in range(0, count)), f"Missing records {keys}"
        finally:
            consumer.close()

    @cluster(num_nodes=3)
    def delete_topic_with_active_txns_test(self):

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("t1")
        rpk.create_topic("t2")

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        })

        # Non transactional
        producer_nt = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
        })

        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'group.id': 'test1',
            'isolation.level': 'read_committed',
        })

        consumer.subscribe([TopicSpec(name='t2')])

        producer.init_transactions()
        producer.begin_transaction()

        def add_records(topic, producer):
            for i in range(0, 100):
                producer.produce(topic,
                                 str(i),
                                 str(i),
                                 partition=0,
                                 on_delivery=self.on_delivery)
            producer.flush()

        add_records("t1", producer)
        add_records("t2", producer)

        # To make sure LSO is not blocked.
        add_records("t2", producer_nt)

        rpk.delete_topic("t1")

        # Should not throw
        producer.commit_transaction()

        def consume_records(consumer, count):
            total = 0
            while total != count:
                total += len(self.consume(consumer))

        consume_records(consumer, 200)

    @cluster(num_nodes=3)
    def check_sequence_table_cleaning_after_eviction_test(self):
        segment_size = 1024 * 1024
        topic_spec = TopicSpec(partition_count=1, segment_bytes=segment_size)
        topic = topic_spec.name

        # make segments small
        self.client().create_topic(topic_spec)

        producers_count = 20

        message_size = 128
        segments_per_producer = 5
        message_count = int(segments_per_producer * segment_size /
                            message_size)
        msg_body = random.randbytes(message_size)

        producers = []
        self.logger.info(f"producing {message_count} messages per producer")
        for i in range(producers_count):
            p = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'enable.idempotence': True,
            })
            producers.append(p)
            for m in range(message_count):
                p.produce(topic,
                          str(f"p-{i}-{m}"),
                          msg_body,
                          partition=0,
                          on_delivery=self.on_delivery)
            p.flush()

        def get_tx_metrics():
            return self.redpanda.metrics_sample(
                "tx_mem_tracker_consumption_bytes",
                self.redpanda.started_nodes())

        metrics = get_tx_metrics()
        consumed_per_node = defaultdict(int)
        for m in metrics.samples:
            id = self.redpanda.node_id(m.node)
            consumed_per_node[id] += int(m.value)
        self.logger.info(
            f"Bytes consumed by transactional subsystem: {consumed_per_node}")

        self.client().alter_topic_config(
            topic=topic, key=TopicSpec.PROPERTY_RETENTION_BYTES, value=128)
        self.client().alter_topic_config(
            topic=topic,
            key=TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
            value=128)

        def segments_removed():
            removed_per_node = defaultdict(int)
            metric_sample = self.redpanda.metrics_sample(
                "log_segments_removed", self.redpanda.started_nodes())
            metric = metric_sample.label_filter(
                dict(namespace="kafka", topic=topic))
            for m in metric.samples:
                removed_per_node[m.node] += m.value
            return all([v > 0 for v in removed_per_node.values()])

        wait_until(segments_removed, timeout_sec=60, backoff_sec=1)
        # produce until next segment roll
        #
        # TODO: change this when we will implement cleanup on current,
        # not the next eviction
        last_producer = ck.Producer({
            'bootstrap.servers':
            self.redpanda.brokers(),
            'enable.idempotence':
            False,
        })

        message_count_to_roll_segment = int(
            message_count / segments_per_producer) + 100
        # produce enough data to roll the single segment
        for m in range(message_count_to_roll_segment):
            last_producer.produce(topic,
                                  str(f"last-mile-{m}"),
                                  msg_body,
                                  partition=0,
                                  on_delivery=self.on_delivery)
            last_producer.flush()
        # restart redpanda to make sure rm_stm recovers state from snapshot,
        # which should be now cleaned and do not contain expired producer ids
        self.redpanda.restart_nodes(self.redpanda.nodes)

        metrics = get_tx_metrics()
        consumed_per_node_after = defaultdict(int)
        for m in metrics.samples:
            id = self.redpanda.node_id(m.node)
            consumed_per_node_after[id] += int(m.value)

        self.logger.info(
            f"Bytes consumed by transactional subsystem before eviction: {consumed_per_node}, after eviction: {consumed_per_node_after}"
        )

        assert all([
            consumed_bytes > consumed_per_node_after[n]
            for n, consumed_bytes in consumed_per_node.items()
        ])

    @cluster(num_nodes=3)
    def check_pids_overflow_test(self):
        rpk = RpkTool(self.redpanda)
        max_concurrent_producer_ids = 10
        ans = rpk.cluster_config_set("max_concurrent_producer_ids",
                                     str(max_concurrent_producer_ids))

        test_producer = ck.Producer({
            'bootstrap.servers':
            self.redpanda.brokers(),
            'enable.idempotence':
            True,
        })

        topic = self.topics[0].name
        test_producer.produce(topic,
                              '0',
                              '0',
                              partition=0,
                              on_delivery=self.on_delivery)
        test_producer.flush()

        max_producers = 51
        producers = []
        for i in range(max_producers - 1):
            p = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'enable.idempotence': True,
            })
            p.produce(topic,
                      str(i + 1),
                      str(i + 1),
                      partition=0,
                      on_delivery=self.on_delivery)
            p.flush()
            producers.append(p)

        try:
            test_producer.produce(topic,
                                  'test',
                                  'test',
                                  partition=0,
                                  on_delivery=self.on_delivery)
            test_producer.flush()
            assert False, "We can not produce after cleaning in rm_stm"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            kafka_error.code(
            ) == ck.cimpl.KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER

        last_worked_producers = max_producers - max_concurrent_producer_ids - 1
        for i in range(max_concurrent_producer_ids):
            producers[last_worked_producers + i].produce(
                topic,
                str(max_producers + i),
                str(max_producers + i),
                partition=0,
                on_delivery=self.on_delivery)
            producers[last_worked_producers + i].flush()

        should_be_consumed = max_producers + max_concurrent_producer_ids - 1
        num_consumed = 0
        prev_rec = bytes("0", 'UTF-8')

        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "123",
            'auto.offset.reset': 'earliest',
        })

        consumer.subscribe([topic])

        while num_consumed < should_be_consumed:
            self.redpanda.logger.debug(
                f"Consumed {num_consumed}. Should consume at the end {should_be_consumed}"
            )
            records = self.consume(consumer)

            for record in records:
                assert prev_rec == record.key(
                ), f"Expected {prev_rec}. Got {record.key()}"
                prev_rec = bytes(str(int(prev_rec) + 1), 'UTF-8')

            num_consumed += len(records)

        assert num_consumed == should_be_consumed


class GATransaction_v22_1_UpgradeTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "enable_transactions": True,
            "transaction_coordinator_replication": 1,
            "id_allocator_replication": 1,
            "enable_leader_balancer": False,
        }

        super(GATransaction_v22_1_UpgradeTest,
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
        self.old_version, self.old_version_str = self.installer.install(
            self.redpanda.nodes, (22, 1))
        super(GATransaction_v22_1_UpgradeTest, self).setUp()

    def do_upgrade_with_tx(self, selector):
        topic_name = self.topics[0].name
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

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

        node_to_upgrade = selector()

        # Update node with tx manager
        self.installer.install(self.redpanda.nodes, (22, 2))
        self.redpanda.restart_nodes(node_to_upgrade)
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.old_version_str in unique_versions, unique_versions

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

        self.installer.install(self.redpanda.nodes, self.old_version)
        self.redpanda.restart_nodes(node_to_upgrade)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        self.check_consume(2)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def upgrade_coordinator_test(self):
        def get_tx_coordinator():
            admin = Admin(self.redpanda)
            leader_id = admin.get_partition_leader(namespace="kafka_internal",
                                                   topic="tx",
                                                   partition=0)
            return self.redpanda.get_node(leader_id)

        self.do_upgrade_with_tx(get_tx_coordinator)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def upgrade_topic_test(self):
        topic_name = self.topics[0].name

        def get_topic_leader():
            admin = Admin(self.redpanda)
            leader_id = admin.get_partition_leader(namespace="kafka",
                                                   topic=topic_name,
                                                   partition=0)
            return self.redpanda.get_node(leader_id)

        self.do_upgrade_with_tx(get_topic_leader)


class StaticPartitioning_MixedVersionsTest(RedpandaTest, TransactionsMixin):
    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
            "enable_auto_rebalance_on_node_add": False,
        }

        environment = {
            "__REDPANDA_LATEST_LOGICAL_VERSION": 9,
            "__REDPANDA_EARLIEST_LOGICAL_VERSION": 9
        }

        super(StaticPartitioning_MixedVersionsTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf,
                             log_level="trace",
                             environment=environment)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def find_coordinator_on_old_node_doesnt_create_tx_registry_test(self):
        self.redpanda.set_environment({
            "__REDPANDA_LATEST_LOGICAL_VERSION": 10,
            "__REDPANDA_EARLIEST_LOGICAL_VERSION": 9
        })
        old_node = self.redpanda.started_nodes()[0]
        new_node = self.redpanda.started_nodes()[1]
        self.redpanda.restart_nodes([new_node], stop_timeout=60)

        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert not node.account.exists(path)

        self.find_coordinator("tx0", node=old_node)

        for node in self.redpanda.started_nodes():
            path = join(RedpandaService.DATA_DIR, "kafka_internal",
                        "tx_registry")
            assert not node.account.exists(path)
            path = join(RedpandaService.DATA_DIR, "kafka_internal", "tx")
            assert node.account.exists(path)
            assert node.account.isdir(path)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def find_coordinator_on_new_node_doesnt_create_tx_registry_test(self):
        self.redpanda.set_environment({
            "__REDPANDA_LATEST_LOGICAL_VERSION": 10,
            "__REDPANDA_EARLIEST_LOGICAL_VERSION": 9
        })
        new_node = self.redpanda.started_nodes()[1]
        self.redpanda.restart_nodes([new_node], stop_timeout=60)

        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert not node.account.exists(path)

        self.find_coordinator("tx0", node=new_node)

        for node in self.redpanda.started_nodes():
            path = join(RedpandaService.DATA_DIR, "kafka_internal",
                        "tx_registry")
            assert not node.account.exists(path)
            path = join(RedpandaService.DATA_DIR, "kafka_internal", "tx")
            assert node.account.exists(path)
            assert node.account.isdir(path)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def find_coordinator_creates_tx_topics_after_upgrade_test(self):
        self.redpanda.set_environment({
            "__REDPANDA_LATEST_LOGICAL_VERSION": 10,
            "__REDPANDA_EARLIEST_LOGICAL_VERSION": 9
        })
        nodes = list(self.redpanda.started_nodes())
        self.redpanda.restart_nodes(nodes, stop_timeout=60)

        FeaturesTestBase._wait_for_version_everywhere(self, 10)

        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert not node.account.exists(path)

        self.find_coordinator("tx0")

        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx", "tx_registry"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert node.account.exists(path)
                assert node.account.isdir(path)


class StaticPartitioning_v23_1_UpgradeTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "enable_transactions": True,
            "transaction_coordinator_replication": 3,
            "id_allocator_replication": 1,
            "enable_leader_balancer": False,
            "kafka_enable_partition_reassignment": False,
            "transaction_coordinator_log_segment_size": 100,
        }

        super(StaticPartitioning_v23_1_UpgradeTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

        self.installer = self.redpanda._installer
        self.producer_cur_index = 0

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
        self.old_version, self.old_version_str = self.installer.install(
            self.redpanda.nodes, (23, 1))
        super(StaticPartitioning_v23_1_UpgradeTest, self).setUp()

    def produce_and_consume(self, tx_amount=1):

        topic_name = self.topics[0].name
        for _ in range(tx_amount):
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': '0',
                'transaction.timeout.ms': 10000,
            })
            producer.init_transactions()
            producer.begin_transaction()
            producer.produce(topic_name, str(self.producer_cur_index),
                             str(self.producer_cur_index), 0, self.on_delivery)
            producer.commit_transaction()
            producer.flush()
            self.producer_cur_index += 1

        self.check_consume(self.producer_cur_index)

    def get_tx_coordinator(self):
        admin = Admin(self.redpanda)
        leader_id = admin.get_partition_leader(namespace="kafka_internal",
                                               topic="tx",
                                               partition=0)
        return self.redpanda.get_node(leader_id)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def do_upgrade_and_downgrade_one_node_test(self):
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions
        self.redpanda.logger.debug(
            f"Initial version of cluster: {self.old_version_str}")

        self.produce_and_consume(10)

        node_to_upgrade = self.get_tx_coordinator()
        node_to_upgrade_id = self.redpanda.idx(node_to_upgrade)
        self.redpanda.logger.debug(
            f"Initial tx coordinator node: {node_to_upgrade_id}")

        # Update node with tx manager
        self.redpanda.logger.debug(
            f"Upgrading node {node_to_upgrade_id} to version {RedpandaInstaller.HEAD}"
        )
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(node_to_upgrade)
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.old_version_str in unique_versions, unique_versions
        self.redpanda.logger.debug(
            f"Cluster versions after upgrade: {unique_versions}")

        self.produce_and_consume(100)

        self.redpanda.logger.debug(
            f"Txn coordinator node after upgrade: {self.redpanda.idx(self.get_tx_coordinator())}"
        )
        if self.get_tx_coordinator() != node_to_upgrade:
            self.redpanda.logger.debug(
                f"Moving txn coordinator node to: {node_to_upgrade_id}")
            admin = Admin(self.redpanda)
            admin.partition_transfer_leadership(namespace="kafka_internal",
                                                topic="tx",
                                                partition=0,
                                                target_id=node_to_upgrade_id)
            wait_until(lambda: self.redpanda.idx(self.get_tx_coordinator()) ==
                       node_to_upgrade_id,
                       timeout_sec=60,
                       backoff_sec=2,
                       err_msg="Leadership did not stabilize")

        self.produce_and_consume(100)

        self.redpanda.logger.debug(
            f"Downgrade cluster version to: {self.old_version}")
        self.installer.install(self.redpanda.nodes, self.old_version)
        self.redpanda.restart_nodes(node_to_upgrade)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        self.check_consume(self.producer_cur_index)

        self.produce_and_consume(10)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def do_upgrade_cluster_test(self):
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions
        self.redpanda.logger.debug(
            f"Initial version of cluster: {self.old_version_str}")

        self.produce_and_consume(100)

        # Update node with tx manager
        self.redpanda.logger.debug(
            f"Upgrading cluster to version {RedpandaInstaller.HEAD}")
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        self.produce_and_consume(100)

    @cluster(
        num_nodes=3,
        log_allow_list=RESTART_LOG_ALLOW_LIST + [
            "Incompatible downgrade detected", "unknown fence record version",
            "unsupported tx_snapshot_header version",
            "unsupported seq_snapshot_header version",
            "assert - Backtrace below:",
            "Version 5 is not supported. We only support versions up to 4"
        ])
    def fail_to_downgrade_full_cluster_test(self):
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions
        self.redpanda.logger.debug(
            f"Initial version of cluster: {self.old_version_str}")

        self.produce_and_consume(10)

        # Update node with tx manager
        self.redpanda.logger.debug(
            f"Upgrading cluster to version {RedpandaInstaller.HEAD}")
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        wait_until(lambda: self.old_version_str not in wait_for_num_versions(
            self.redpanda, 1),
                   timeout_sec=60,
                   retry_on_exc=True)

        self.produce_and_consume(100)

        self.redpanda.logger.debug(
            f"Downgrade cluster version to: {self.old_version}")
        self.installer.install(self.redpanda.nodes, self.old_version)
        try:
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                                start_timeout=30)
        except Exception as e:
            self.logger.info(f"As expected, redpanda failed to start ({e})")
        else:
            raise RuntimeError(
                "Redpanda started: it should have failed to start!")
