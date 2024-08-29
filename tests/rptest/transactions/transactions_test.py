# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from contextlib import contextmanager
from enum import Enum
import string
from threading import Lock, Semaphore, Thread
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.cluster import cluster
from rptest.util import wait_until_result, expect_exception
from rptest.clients.types import TopicSpec
from time import sleep, time
from typing import Optional
from os.path import join
import json

import uuid
import random

from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError

from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.redpanda import RedpandaService, SecurityConfig, SaslCredentials
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
import confluent_kafka as ck

from rptest.services.admin import Admin
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.clients.rpk import RpkTool, AclList

from rptest.utils.mode_checks import skip_debug_mode


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

    def on_delivery(self, err, _):
        assert err is None, err

    def generate_data(self, topic, num_records, extra_cfg={}):
        producer_cfg = {
            'bootstrap.servers': self.redpanda.brokers(),
        }
        producer_cfg.update(extra_cfg)
        producer = ck.Producer(producer_cfg)

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
                return False, records

        return wait_until_result(consume_records,
                                 timeout_sec=30,
                                 backoff_sec=2,
                                 err_msg="Can not consume data")


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
        self.kafka_cli = KafkaCliTools(self.redpanda, "3.0.0")

    def wait_for_eviction(self, max_concurrent_producer_ids, num_to_evict):
        samples = [
            "idempotency_pid_cache_size",
            "producer_state_manager_evicted_producers"
        ]
        brokers = self.redpanda.started_nodes()
        metrics = self.redpanda.metrics_samples(samples, brokers)
        producers_per_node = defaultdict(int)
        evicted_per_node = defaultdict(int)
        for pattern, metric in metrics.items():
            for m in metric.samples:
                id = self.redpanda.node_id(m.node)
                if pattern == "idempotency_pid_cache_size":
                    producers_per_node[id] += int(m.value)
                elif pattern == "producer_state_manager_evicted_producers":
                    evicted_per_node[id] += int(m.value)

        self.redpanda.logger.debug(f"active producers: {producers_per_node}")
        self.redpanda.logger.debug(f"evicted producers: {evicted_per_node}")

        remaining_match = all([
            num == max_concurrent_producer_ids
            for num in producers_per_node.values()
        ])

        evicted_match = all(
            [val == num_to_evict for val in evicted_per_node.values()])

        return len(producers_per_node) == len(
            brokers) and remaining_match and evicted_match

    def no_running_transactions(self):
        tx_list = self.admin.get_all_transactions()
        # Filter out killed / aborting transactions.
        # Note: killed (timedout) transactions are weirdly reported
        # as 'aborting' for some reason.
        tx_list = [
            tx for tx in tx_list
            if tx['status'] in ['ready', 'ongoing', 'preparing', 'prepared']
        ]
        return len(tx_list) == 0

    @cluster(num_nodes=3)
    def find_coordinator_creates_tx_topics_test(self):
        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert not node.account.exists(path)

        self.find_coordinator("tx0")

        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert node.account.exists(path)
                assert node.account.isdir(path)

    @cluster(num_nodes=3)
    def init_transactions_creates_eos_topics_test(self):
        for node in self.redpanda.started_nodes():
            for tx_topic in ["id_allocator", "tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert not node.account.exists(path)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        })

        producer.init_transactions()

        for node in self.redpanda.started_nodes():
            for tx_topic in ["id_allocator", "tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal",
                            tx_topic)
                assert node.account.exists(path)
                assert node.account.isdir(path)

    @cluster(num_nodes=3)
    def test_max_timeout(self):
        rpk = RpkTool(self.redpanda)
        max_timeout_ms = int(
            rpk.cluster_config_get("transaction_max_timeout_ms"))
        test_timeout_ms = max_timeout_ms + 100

        def init_producer(timeout_ms: int):
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': '0',
                'transaction.timeout.ms': test_timeout_ms,
            })
            producer.init_transactions()

        try:
            init_producer(test_timeout_ms)
            assert False, "producer session established with a timeout larger than allowed limit"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code(
            ) == ck.KafkaError.INVALID_TRANSACTION_TIMEOUT, f"Unexpected error {kafka_error.code()}"

        # Bump timeout and check again.
        rpk.cluster_config_set("transaction_max_timeout_ms", test_timeout_ms)
        init_producer(test_timeout_ms)

    @cluster(num_nodes=3)
    def simple_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
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

        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.started_nodes():
            records = log_viewer.read_kafka_records(node=node,
                                                    topic=self.input_t.name)
            self.logger.info(f"Read {len(records)} from node {node.name}")

    @cluster(num_nodes=3)
    def rejoin_member_test(self):
        self.redpanda.set_cluster_config(
            {"group_new_member_join_timeout": 5000})
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        })

        group_name = "test"
        consumer1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 10000,
            'session.timeout.ms': 8000
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
            'max.poll.interval.ms': 10000,
            'session.timeout.ms': 8000
        })

        consumer2.subscribe([self.input_t])
        # Rejoin can take some time, so we should pass big timeout
        self.consume(consumer2, timeout_s=60)

        try:
            producer.send_offsets_to_transaction(offsets, metadata, 2)
            assert False, "send_offsetes should fail"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.KafkaError.UNKNOWN_MEMBER_ID

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
    def transaction_id_expiration_test(self):
        admin = Admin(self.redpanda)
        rpk = RpkTool(self.redpanda)
        # Create an open transaction.
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 900000,  # to avoid timing out
        })
        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(self.output_t.name, "x", "y")
        producer.flush()

        # Default transactional id expiration is 7d, so the transaction
        # should be hung.
        wait_timeout_s = 20
        try:
            wait_until(self.no_running_transactions,
                       timeout_sec=wait_timeout_s,
                       backoff_sec=2,
                       err_msg="Transactions still running")
            assert False, "No running transactions found."
        except TimeoutError as e:
            assert "Transactions still running" in str(e)

        # transaction should be aborted.
        rpk.cluster_config_set("transactional_id_expiration_ms", 5000)
        wait_until(self.no_running_transactions,
                   timeout_sec=wait_timeout_s,
                   backoff_sec=2,
                   err_msg="Transactions still running")

        try:
            producer.commit_transaction()
            assert False, "transaction should have been aborted by now."
        except ck.KafkaException as e:
            assert e.args[0].code(
            ) == ck.KafkaError.INVALID_PRODUCER_ID_MAPPING, f"Invalid error thrown on expiration {e}"

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

        # Wait for transactions to hit expiration timeout.
        wait_until(
            self.no_running_transactions,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Transactions are still running, expected to be expired.")

        try:
            producer.produce(self.input_t.name,
                             'test-post-expire',
                             'test-post-expire',
                             partition=0,
                             on_delivery=self.on_delivery)
            producer.flush()
            assert False, "tx is expected to be expired"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError._FENCED

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
                'transaction.timeout.ms': 900000,
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

        # Issue a leadership transfer
        graceful_transfer()
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

        # Capture the proudcer info before evicting the segments
        producers_before = self.kafka_cli.describe_producers(topic=topic,
                                                             partition=0)
        assert len(
            producers_before) == producers_count, "Producer metadata mismatch"

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
            True,
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

        producers_after = self.kafka_cli.describe_producers(topic=topic,
                                                            partition=0)
        assert len(producers_after) < len(
            producers_before
        ), f"Incorrect number of producers restored from snapshot {len(producers_after)}"

    @cluster(num_nodes=3)
    def check_progress_after_fencing_test(self):
        """Checks that a fencing producer makes progress after fenced producers are evicted."""

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': 'test',
            'transaction.timeout.ms': 100000,
        })

        topic_name = self.topics[0].name

        # create a pid, do not commit/abort transaction.
        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(topic_name, "0", "0", 0, self.on_delivery)
        producer.flush()

        # fence the above pid with another producer
        producer0 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': 'test',
            'transaction.timeout.ms': 100000,
        })
        producer0.init_transactions()
        producer0.begin_transaction()
        producer0.produce(topic_name, "0", "0", 0, self.on_delivery)

        max_concurrent_pids = 1
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("max_concurrent_producer_ids",
                               str(max_concurrent_pids))

        self.wait_for_eviction(max_concurrent_pids, 1)

        producer0.commit_transaction()

    @cluster(num_nodes=3)
    def check_pids_overflow_test(self):
        rpk = RpkTool(self.redpanda)
        max_concurrent_producer_ids = 10
        ans = rpk.cluster_config_set("max_concurrent_producer_ids",
                                     str(max_concurrent_producer_ids))

        topic = self.topics[0].name

        def _produce_one(producer, idx):
            self.logger.debug(f"producing using {idx} producer")
            producer.produce(topic,
                             f"record-key-producer-{idx}",
                             f"record-value-producer-{idx}",
                             partition=0,
                             on_delivery=self.on_delivery)
            producer.flush()

        max_producers = 50
        producers = []
        for i in range(max_producers):
            p = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'enable.idempotence': True,
            })
            _produce_one(p, i)
            producers.append(p)

        evicted_count = max_producers - max_concurrent_producer_ids

        wait_until(lambda: self.wait_for_eviction(max_concurrent_producer_ids,
                                                  evicted_count),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Producers not evicted in time")

        # validate that the producers are evicted with LRU policy,
        # starting from this producer there should be no sequence
        # number errors as those producer state should not be evicted
        last_not_evicted_producer_idx = max_producers - max_concurrent_producer_ids + 1
        for i in range(last_not_evicted_producer_idx, len(producers)):
            _produce_one(producers[i], i)

        expected_records = len(
            producers) - last_not_evicted_producer_idx + max_producers
        num_consumed = 0

        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "123",
            'auto.offset.reset': 'earliest',
        })

        consumer.subscribe([topic])

        while num_consumed < expected_records:
            self.redpanda.logger.debug(
                f"Consumed {num_consumed} of of {expected_records}")
            records = self.consume(consumer)
            num_consumed += len(records)

        assert num_consumed == expected_records


class TransactionsStreamsTest(RedpandaTest, TransactionsMixin):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        extra_rp_conf = {
            'unsafe_enable_consumer_offsets_delete_retention': True,
            'group_topic_partitions': 1,  # to reduce log noise
            'log_segment_size_min': 99,
            # to be able to make changes to CO
            'kafka_nodelete_topics': [],
            'kafka_noproduce_topics': [],
        }
        super(TransactionsStreamsTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)
        self.input_t = self.topics[0]
        self.output_t = self.topics[1]

    def setup_consumer_offsets(self, rpk: RpkTool):
        # initialize consumer groups topic
        rpk.consume(topic=self.input_t.name, n=1, group="test-group")
        topic = "__consumer_offsets"
        # Aggressive roll settings to clear multiple small segments
        rpk.alter_topic_config(topic, TopicSpec.PROPERTY_CLEANUP_POLICY,
                               TopicSpec.CLEANUP_DELETE)
        rpk.alter_topic_config(topic, TopicSpec.PROPERTY_SEGMENT_SIZE, 100)

    @cluster(num_nodes=3)
    def consumer_offsets_retention_test(self):
        """Ensure consumer offsets replays correctly after transactional offset commits"""
        input_records = 10000
        self.generate_data(self.input_t, input_records)
        rpk = RpkTool(self.redpanda)
        self.setup_consumer_offsets(rpk)
        # Populate consumer offsets with transactional offset commits/aborts
        producer_conf = {
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': 'streams',
        }
        producer = ck.Producer(producer_conf)
        consumer_conf = {
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        consumer = ck.Consumer(consumer_conf)
        consumer.subscribe([self.input_t])

        producer.init_transactions()
        consumed = 0
        while consumed != input_records:
            records = self.consume(consumer)
            producer.begin_transaction()
            for record in records:
                producer.produce(self.output_t.name,
                                 record.value(),
                                 record.key(),
                                 on_delivery=self.on_delivery)

            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata())

            producer.flush()

            if random.randint(0, 9) < 5:
                producer.commit_transaction()
            else:
                producer.abort_transaction()
            consumed += len(records)

        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.started_nodes():
            co_records = log_viewer.read_consumer_offsets(node=node)
            self.logger.info(f"Read {len(co_records)} from node {node.name}")

        admin = Admin(self.redpanda)
        co_topic = "__consumer_offsets"

        def get_offsets():
            topic_info = list(rpk.describe_topic(co_topic))[0]
            assert topic_info
            return (topic_info.start_offset, topic_info.high_watermark)

        # trim prefix, change leadership and validate the log is replayed successfully on
        # the new leader.
        attempts = 30
        truncate_offset = 100
        while attempts > 0:
            (start, end) = get_offsets()
            self.redpanda.logger.debug(f"Current offsets: {start} - {end}")
            if truncate_offset > end:
                break
            rpk.trim_prefix(co_topic, truncate_offset, partitions=[0])
            admin.partition_transfer_leadership("kafka", co_topic, partition=0)
            admin.await_stable_leader(topic=co_topic,
                                      replication=3,
                                      timeout_s=30)
            truncate_offset += 200
            attempts = attempts - 1


@contextmanager
def expect_kafka_error(err: Optional[ck.KafkaError] = None):
    try:
        yield
    except ck.KafkaException as e:
        if e.args[0].code() != err:
            raise
    else:
        if err is not None:
            raise RuntimeError("Expected an exception!")


@contextmanager
def try_transaction(producer: ck.Producer,
                    consumer: ck.Consumer,
                    send_offset_err: Optional[ck.KafkaError] = None,
                    commit_err: Optional[ck.KafkaError] = None):
    producer.begin_transaction()

    yield

    producer.flush(0.0)

    with expect_kafka_error(send_offset_err):
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()),
            consumer.consumer_group_metadata())

    with expect_kafka_error(commit_err):
        producer.commit_transaction()

    if send_offset_err is not None or commit_err is not None:
        producer.abort_transaction()


class TransactionsAuthorizationTest(RedpandaTest, TransactionsMixin):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    USER_1 = SaslCredentials("user-1", "password", "SCRAM-SHA-256")
    USER_2 = SaslCredentials("user-2", "password", "SCRAM-SHA-256")

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super().__init__(test_context=test_context,
                         extra_rp_conf=extra_rp_conf,
                         log_level="trace")

        self.security = SecurityConfig()
        self.security.kafka_enable_authorization = True
        self.security.enable_sasl = True
        self.security.require_client_auth = True
        self.security.endpoint_authn_method = 'sasl'

        self.redpanda.set_security_settings(self.security)

        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100
        self.admin = Admin(self.redpanda)

        self.rpk = RpkTool(self.redpanda,
                           username=self.superuser.username,
                           password=self.superuser.password,
                           sasl_mechanism=self.superuser.algorithm)

    def setUp(self):
        super().setUp()
        self.admin.create_user(self.USER_1.username, self.USER_1.password,
                               self.USER_1.algorithm)
        self.admin.create_user(self.USER_2.username, self.USER_2.password,
                               self.USER_2.algorithm)

    def sasl_cfg(self, user):
        return {
            'sasl.username': user.username,
            'sasl.password': user.password,
            'sasl.mechanism': user.algorithm,
            'security.protocol': 'sasl_plaintext',
        }

    def sasl_txn_producer(self, user, cfg={}):
        cfg.update(self.sasl_cfg(user))
        p = ck.Producer(cfg)
        p.init_transactions()
        return p

    def sasl_consumer(self, user, cfg={}):
        cfg.update(self.sasl_cfg(user))
        return ck.Consumer(cfg)

    def allow_principal_sync(self, principal, operations, resource,
                             resource_name):

        self.rpk.sasl_allow_principal(principal, operations, resource,
                                      resource_name)

        def acl_ready():
            lst = AclList.parse_raw(self.rpk.acl_list())
            return [
                lst.has_permission(principal, op, resource, resource_name)
                for op in operations
            ]

        wait_until(lambda: acl_ready(),
                   timeout_sec=5,
                   backoff_sec=1,
                   err_msg="ACL not updated in time")

    @cluster(num_nodes=3)
    def init_transactions_authz_test(self):
        producer_cfg = {
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        }

        user = self.USER_1

        self.redpanda.logger.debug("init_transactions should fail without ACL")
        with expect_kafka_error(
                ck.KafkaError.TRANSACTIONAL_ID_AUTHORIZATION_FAILED):
            producer = self.sasl_txn_producer(user, cfg=producer_cfg)

        self.allow_principal_sync(user.username, ['write'], 'transactional-id',
                                  '0')

        producer = self.sasl_txn_producer(user, cfg=producer_cfg)

    @cluster(num_nodes=3)
    def simple_authz_test(self):
        consume_user = self.USER_1
        produce_user = self.USER_2

        self.allow_principal_sync(produce_user.username, ['all'], 'topic',
                                  self.input_t.name)
        self.generate_data(self.input_t,
                           self.max_records,
                           extra_cfg=self.sasl_cfg(produce_user))

        producer_cfg = {
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        }
        consumer_cfg = {
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }

        self.allow_principal_sync(consume_user.username, ['read'], 'topic',
                                  self.input_t.name)
        self.allow_principal_sync(consume_user.username, ['read'], 'group',
                                  'test')
        self.allow_principal_sync(produce_user.username, ['write'],
                                  'transactional-id', '0')
        # TODO(oren): what's this one for?
        self.allow_principal_sync(produce_user.username, ['read'], 'topic',
                                  self.output_t.name)

        consumer = self.sasl_consumer(consume_user, cfg=consumer_cfg)
        consumer.subscribe([self.input_t])
        records = self.consume(consumer)
        assert records is not None

        producer = self.sasl_txn_producer(produce_user, cfg=producer_cfg)

        def on_delivery_purged(err, _):
            assert err is not None and err.code() == ck.KafkaError._PURGE_QUEUE

        def process_records(producer, records, on_delivery, history=[]):
            for record in records:
                assert record.error(
                ) is None, f"Consume error: {record.error()}"
                history.append(record)
                producer.produce(self.output_t.name,
                                 record.value(),
                                 record.key(),
                                 on_delivery=on_delivery)

        with try_transaction(
                producer,
                consumer,
                send_offset_err=ck.KafkaError.GROUP_AUTHORIZATION_FAILED,
                commit_err=ck.KafkaError.GROUP_AUTHORIZATION_FAILED):
            process_records(producer, records, on_delivery_purged)

        self.allow_principal_sync(produce_user.username, ['read'], 'group',
                                  'test')

        producer = self.sasl_txn_producer(produce_user, cfg=producer_cfg)
        with try_transaction(
                producer,
                consumer,
                commit_err=ck.KafkaError.TOPIC_AUTHORIZATION_FAILED):
            process_records(producer, records, on_delivery_purged)

        self.allow_principal_sync(produce_user.username, ['write'], 'topic',
                                  self.output_t.name)

        # Now we have all the requisite permissions set up, and we should be able to
        # make progress

        producer = self.sasl_txn_producer(produce_user, cfg=producer_cfg)

        num_consumed_records = 0
        consumed_from_input_topic = []

        # Process the records we have sitting in memory

        with try_transaction(producer, consumer):
            process_records(producer, records, self.on_delivery,
                            consumed_from_input_topic)
            num_consumed_records += len(records)

        # then consume the rest, transactionwise

        while num_consumed_records != self.max_records:
            # Imagine that consume got broken, we read the same record twice and overshoot the condition
            assert num_consumed_records < self.max_records

            records = self.consume(consumer)
            assert records is not None

            with try_transaction(producer, consumer):
                process_records(producer, records, self.on_delivery,
                                consumed_from_input_topic)

            num_consumed_records += len(records)

        consumer.close()
        assert len(consumed_from_input_topic) == self.max_records

        self.allow_principal_sync(consume_user.username, ['read'], 'topic',
                                  self.output_t.name)
        self.allow_principal_sync(consume_user.username, ['read'], 'group',
                                  'testtest')

        consumer = self.sasl_consumer(
            consume_user,
            cfg={
                'group.id': 'testtest',
                'bootstrap.servers': self.redpanda.brokers(),
                'auto.offset.reset': 'earliest',
            },
        )
        consumer.subscribe([self.output_t])

        index_from_input = 0

        while index_from_input < self.max_records:
            records = self.consume(consumer)
            for record in records:
                assert record.error(
                ) is None, f"Consume error: {record.error()}"
                assert record.key(
                ) == consumed_from_input_topic[index_from_input].key(
                ), f'Records key does not match from input {consumed_from_input_topic[index_from_input].key()}, from output {record.key()}'
                assert record.value(
                ) == consumed_from_input_topic[index_from_input].value(
                ), f'Records value does not match from input {consumed_from_input_topic[index_from_input].value()}, from output {record.value()}'
                index_from_input += 1

        assert consumer.poll(timeout=3) is None


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


def remote_path_exists(node, path):
    wait_until(lambda: node.account.exists(path),
               timeout_sec=20,
               backoff_sec=2,
               err_msg=f"Can't find \"{path}\" on {node.account.hostname}")


class TxUpgradeTest(RedpandaTest):
    """
    Basic test verifying if mapping between transaction coordinator and transaction_id is preserved across the upgrades
    """
    def __init__(self, test_context):
        super(TxUpgradeTest, self).__init__(test_context=test_context,
                                            num_brokers=3)
        self.installer = self.redpanda._installer
        self.partition_count = 10
        self.msg_sent = 0
        self.producers_count = 100

    def setUp(self):
        self.old_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)

        self.old_version_str = f"v{self.old_version[0]}.{self.old_version[1]}.{self.old_version[2]}"
        self.installer.install(self.redpanda.nodes, self.old_version)
        super(TxUpgradeTest, self).setUp()

    def _tx_id(self, idx):
        return f"test-producer-{idx}"

    def _populate_tx_coordinator(self, topic):
        def delivery_callback(err, msg):
            if err:
                assert False, "failed to deliver message: %s" % err

        for i in range(self.producers_count):
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': self._tx_id(i),
            })
            producer.init_transactions()
            producer.begin_transaction()
            for m in range(random.randint(1, 50)):
                producer.produce(topic,
                                 f"p-{i}-key-{m}",
                                 f"p-{i}-value-{m}",
                                 random.randint(0, self.partition_count - 1),
                                 callback=delivery_callback)
            producer.commit_transaction()
            producer.flush()

    def _get_tx_id_mapping(self):
        mapping = {}
        admin = Admin(self.redpanda)
        for idx in range(self.producers_count):
            c = admin.find_tx_coordinator(self._tx_id(idx))
            mapping[self._tx_id(
                idx)] = f"{c['ntp']['topic']}/{c['ntp']['partition']}"

        return mapping

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def upgrade_does_not_change_tx_coordinator_assignment_test(self):
        topic = TopicSpec(partition_count=self.partition_count)
        self.client().create_topic(topic)

        self._populate_tx_coordinator(topic=topic.name)
        initial_mapping = self._get_tx_id_mapping()
        self.logger.info(f"Initial mapping {initial_mapping}")

        first_node = self.redpanda.nodes[0]
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str in unique_versions, unique_versions

        # Upgrade one node to the head version.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.old_version_str in unique_versions, unique_versions
        assert self._get_tx_id_mapping(
        ) == initial_mapping, "Mapping changed after upgrading one of the nodes"

        # verify if txs are handled correctly with mixed versions
        self._populate_tx_coordinator(topic.name)

        # Only once we upgrade the rest of the nodes do we converge on the new
        # version.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.old_version_str not in unique_versions, unique_versions
        assert self._get_tx_id_mapping(
        ) == initial_mapping, "Mapping changed after full upgrade"


class TxUpgradeRevertTest(RedpandaTest):
    """Tests that the local snapshot is compatible after the upgrade is reverted"""
    class TxStateGenerator():
        """A traffic generating utility for transactions. Traffic can be paused and resumed as needed to see a consistent snapshot
        of the transactions and tally the state as seen by clients vs the brokers."""
        def __init__(self, num_producers: int, topic_name: str,
                     num_partitions: int, redpanda: RedpandaService) -> None:
            self.num_producers = num_producers
            self.topic_name = topic_name
            self.tx_id_counter = 0
            self.redpanda = redpanda
            self.num_partitions = num_partitions
            self.tx_states = {}
            # Populate initial states
            for p in range(0, num_partitions):
                self.tx_states[p] = dict()
            self.stopped = False
            self.admin = Admin(self.redpanda)
            self.lock = Lock()
            self.thread = Thread(target=self.start_workload, daemon=True)
            self.semaphore = Semaphore(num_producers)
            self.workload_paused = False
            self.failed = False
            self.thread.start()

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            self.resume()
            self.stop()
            self.thread.join(timeout=30)
            assert not self.failed, "A subset of transactional producers failed, check test log output"
            self.redpanda.logger.debug(
                json.dumps(self.tx_states, sort_keys=True, indent=4))

        class TxState(str, Enum):
            INIT = 'init',
            BEGIN = 'begin',
            PRODUCED = 'produced',
            COMMITTED = 'committed',
            ABORTED = 'aborted',

        def random_string(self):
            return ''.join(
                random.choice(string.ascii_letters) for _ in range(5))

        def pause(self):
            self.workload_paused = True
            for _ in range(0, self.num_producers):
                self.semaphore.acquire()
            self.redpanda.logger.info("Paused workload")

        def resume(self):
            self.workload_paused = False
            self.semaphore.release(self.num_producers)
            self.redpanda.logger.info("Workload unpaused")

        def stop(self):
            self.stopped = True

        def tx_id(self):
            with self.lock:
                id = str(self.tx_id_counter)
                self.tx_id_counter += 1
                return id

        def do_transaction(self, producer: ck.Producer, partitions: list[int]):

            producer.begin_transaction()
            yield self.TxState.BEGIN

            for partition in partitions:
                producer.produce(topic=self.topic_name,
                                 value=self.random_string(),
                                 key=self.random_string(),
                                 partition=partition)
            producer.flush()
            yield self.TxState.PRODUCED

            if random.choice([True, False]):
                producer.commit_transaction()
                yield self.TxState.COMMITTED
            else:
                producer.abort_transaction()
                yield self.TxState.ABORTED

        def update_tx_state(self, producer_id, state, partitions: list[int],
                            sequence: int):
            with self.lock:
                for p in partitions:
                    self.tx_states[p][producer_id] = dict(state=state,
                                                          sequence=sequence)

        def dump_debug_transaction_state(self):
            self.redpanda.logger.debug("---- test producer state state ----")
            self.redpanda.logger.debug(
                json.dumps(self.tx_states, sort_keys=True, indent=4))
            self.redpanda.logger.debug("----- broker partition state ----")
            for partition in range(0, self.num_partitions):
                partition_txes = self.admin.get_transactions(
                    topic=self.topic_name,
                    partition=partition,
                    namespace="kafka")
                self.redpanda.logger.debug(partition_txes)

        def random_transaction(self):
            id = self.tx_id()
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': id,
                'transaction.timeout.ms': 1000000
            })

            producer.init_transactions()
            self.update_tx_state(producer_id=id,
                                 state=self.TxState.INIT,
                                 partitions=[],
                                 sequence=-1)

            sequence = 0
            try:
                while not self.stopped:
                    sleep(random.randint(1, 10) / 1000.0)
                    if self.workload_paused:
                        continue
                    with self.semaphore:
                        partitions = random.sample(
                            range(0, self.num_partitions),
                            random.randint(0, 5))
                        for state in self.do_transaction(
                                producer=producer, partitions=partitions):
                            self.update_tx_state(id,
                                                 state,
                                                 partitions,
                                                 sequence=sequence)
                        sequence += 1
            except Exception as e:
                self.failed = True
                self.dump_debug_transaction_state()
                self.redpanda.logger.error(
                    f"Exception running transactions with producer {id}",
                    exc_info=True)

        def start_workload(self):
            producers = []
            for producer in range(0, self.num_producers):
                t = Thread(target=self.random_transaction)
                t.start()
                producers.append(t)

            for producer in producers:
                producer.join()

        def validate_active_tx_states(self):
            def do_check():
                for p in range(0, self.num_partitions):
                    self.redpanda.logger.debug(
                        f"Validating partition tx state for {self.topic_name}/{p}"
                    )
                    rp_tx_state = self.admin.get_transactions(
                        topic=self.topic_name, partition=p,
                        namespace="kafka").get("active_transactions", [])
                    local_tx_state = self.tx_states[p]
                    local_active_pids = [
                        int(pid) for pid, tx_state in local_tx_state.items()
                        if tx_state["state"] in ["begin", "produced"]
                    ]
                    local_active_pids.sort()
                    rp_active_pids = [
                        int(tx["producer_id"]["id"]) for tx in rp_tx_state
                    ]
                    rp_active_pids.sort()
                    self.redpanda.logger.debug(
                        f"Local pids: {rp_active_pids}, broker reported: {local_active_pids}"
                    )
                    return rp_active_pids == local_active_pids

            try:
                wait_until(
                    do_check,
                    timeout_sec=20,
                    backoff_sec=2,
                    err_msg=
                    "Invalid active transaction state, check log for details")
            except TimeoutError as e:
                self.dump_debug_transaction_state()
                raise e

    def __init__(self, test_context):
        super(TxUpgradeRevertTest, self).__init__(test_context=test_context,
                                                  num_brokers=3)
        self.installer = self.redpanda._installer
        self.partition_count = 10
        self.msg_sent = 0
        self.producers_count = 100

    def setUp(self):
        self.old_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)

        self.old_version_str = f"v{self.old_version[0]}.{self.old_version[1]}.{self.old_version[2]}"
        # Install and upgrade from an older version.
        self.installer.install(self.redpanda.nodes, self.old_version)
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        super(TxUpgradeRevertTest, self).setUp()

    def install_one_node(self, node, version, topic):
        node_idx = self.redpanda.idx(node)
        # Drain leadership of the node to be upgraded to ensure tx partitions are flushed
        # This is a (unfortunate) hack to workaround transaction coordinator's inability
        # to survive restarts. Here we drain all partition leadership (which ensures everything
        # is flushed to disk) before we upgrade/restart.
        self.rpk.cluster_maintenance_enable(node=node_idx, wait=True)
        self.installer.install([node], version)
        self.redpanda.restart_nodes([node])
        # Disable maintenance mode
        self.rpk.cluster_maintenance_disable(node=node_idx)
        self.admin.await_stable_leader(topic=topic,
                                       replication=3,
                                       timeout_s=30)

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_snapshot_compatibility(self):
        """Test validates that a broker can be upgraded and downgraded while keeping the transaction state consistent.
        Particularly the snapshot state should be compatible across these operations."""
        partition_count = 50
        topic = TopicSpec(partition_count=50)
        self.client().create_topic(topic)
        with self.TxStateGenerator(num_producers=20,
                                   topic_name=topic.name,
                                   num_partitions=50,
                                   redpanda=self.redpanda) as traffic:
            # Populate some transactions state.
            sleep(30)
            # Pause the workload and upgrade one of the nodes
            traffic.pause()
            traffic.validate_active_tx_states()
            first_node = self.redpanda.nodes[0]
            wait_for_num_versions(self.redpanda, 1)
            # do the upgrade
            self.install_one_node(first_node, RedpandaInstaller.HEAD,
                                  topic.name)
            wait_for_num_versions(self.redpanda, 2)
            traffic.validate_active_tx_states()
            # Ensure things can progress from where they were paused.
            traffic.resume()
            sleep(30)
            # Downgrade the node again
            traffic.pause()
            traffic.validate_active_tx_states()
            self.install_one_node(first_node, self.old_version, topic.name)
            wait_for_num_versions(self.redpanda, 1)
            traffic.validate_active_tx_states()
            # Ensure progress
            traffic.resume()
            sleep(30)
