# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
import string
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from time import time
from os.path import join

import random

from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError

from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.transactions.util import TransactionsMixin
from rptest.services.redpanda import RedpandaService
import confluent_kafka as ck

from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.services.metrics_check import MetricCheck


class TransactionsTest(RedpandaTest, TransactionsMixin):
    topics = (
        TopicSpec(partition_count=1, replication_factor=3),
        TopicSpec(partition_count=1, replication_factor=3),
    )

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(TransactionsTest, self).__init__(
            test_context=test_context, extra_rp_conf=extra_rp_conf, log_level="trace"
        )

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100
        self.admin = Admin(self.redpanda)
        self.kafka_cli = KafkaCliTools(self.redpanda, "3.0.0")

    def wait_for_eviction(self, max_concurrent_producer_ids, num_to_evict):
        samples = [
            "idempotency_pid_cache_size",
            "producer_state_manager_evicted_producers",
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

        remaining_match = all(
            [num == max_concurrent_producer_ids for num in producers_per_node.values()]
        )

        evicted_match = all([val == num_to_evict for val in evicted_per_node.values()])

        return (
            len(producers_per_node) == len(brokers)
            and remaining_match
            and evicted_match
        )

    def no_running_transactions(self):
        tx_list = self.admin.get_all_transactions()
        # Filter out killed / aborting transactions.
        # Note: killed (timedout) transactions are weirdly reported
        # as 'aborting' for some reason.
        tx_list = [
            tx
            for tx in tx_list
            if tx["status"] in ["ready", "ongoing", "preparing", "prepared"]
        ]
        return len(tx_list) == 0

    @cluster(num_nodes=3)
    def find_coordinator_creates_tx_topics_test(self):
        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal", tx_topic)
                assert not node.account.exists(path)

        self.find_coordinator("tx0")

        for node in self.redpanda.started_nodes():
            for tx_topic in ["tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal", tx_topic)
                assert node.account.exists(path)
                assert node.account.isdir(path)

    @cluster(num_nodes=3)
    def init_transactions_creates_eos_topics_test(self):
        for node in self.redpanda.started_nodes():
            for tx_topic in ["id_allocator", "tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal", tx_topic)
                assert not node.account.exists(path)

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
            }
        )

        producer.init_transactions()

        for node in self.redpanda.started_nodes():
            for tx_topic in ["id_allocator", "tx"]:
                path = join(RedpandaService.DATA_DIR, "kafka_internal", tx_topic)
                assert node.account.exists(path)
                assert node.account.isdir(path)

    @cluster(num_nodes=3)
    def test_max_timeout(self):
        rpk = RpkTool(self.redpanda)
        max_timeout_ms = int(rpk.cluster_config_get("transaction_max_timeout_ms"))
        test_timeout_ms = max_timeout_ms + 100

        def init_producer(timeout_ms: int):
            producer = ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "transactional.id": "0",
                    "transaction.timeout.ms": test_timeout_ms,
                }
            )
            producer.init_transactions()

        try:
            init_producer(test_timeout_ms)
            assert False, (
                "producer session established with a timeout larger than allowed limit"
            )
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.KafkaError.INVALID_TRANSACTION_TIMEOUT, (
                f"Unexpected error {kafka_error.code()}"
            )

        # Bump timeout and check again.
        self.redpanda.set_cluster_config(
            {"transaction_max_timeout_ms": test_timeout_ms}
        )
        init_producer(test_timeout_ms)

    @cluster(num_nodes=3)
    def simple_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
            }
        )

        consumer1 = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": "test",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

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
                assert record.error() == None
                consumed_from_input_topic.append(record)
                producer.produce(
                    self.output_t.name,
                    record.value(),
                    record.key(),
                    on_delivery=self.on_delivery,
                )

            producer.send_offsets_to_transaction(
                consumer1.position(consumer1.assignment()),
                consumer1.consumer_group_metadata(),
            )

            producer.commit_transaction()

            num_consumed_records += len(records)

        producer.flush()
        consumer1.close()
        assert len(consumed_from_input_topic) == self.max_records

        consumer2 = ck.Consumer(
            {
                "group.id": "testtest",
                "bootstrap.servers": self.redpanda.brokers(),
                "auto.offset.reset": "earliest",
            }
        )
        consumer2.subscribe([self.output_t])

        index_from_input = 0

        while index_from_input < self.max_records:
            records = self.consume(consumer2)

            for record in records:
                assert (
                    record.key() == consumed_from_input_topic[index_from_input].key()
                ), (
                    f"Records key does not match from input {consumed_from_input_topic[index_from_input].key()}, from output {record.key()}"
                )
                assert (
                    record.value()
                    == consumed_from_input_topic[index_from_input].value()
                ), (
                    f"Records value does not match from input {consumed_from_input_topic[index_from_input].value()}, from output {record.value()}"
                )
                index_from_input += 1

        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.started_nodes():
            records = log_viewer.read_kafka_records(node=node, topic=self.input_t.name)
            self.logger.info(f"Read {len(records)} from node {node.name}")

    @cluster(num_nodes=3)
    def group_deletion_with_ongoing_transaction_test(self):
        self.redpanda.set_cluster_config({"group_new_member_join_timeout": 5000})
        self.generate_data(self.input_t, self.max_records)

        group_name = "test_group"

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "group_deletion_test_id",
            }
        )

        group_name = "test"
        consumer = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": group_name,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        consumer.subscribe([self.input_t])
        _ = self.consume(consumer)
        producer.init_transactions()
        producer.begin_transaction()
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()), consumer.consumer_group_metadata()
        )
        producer.flush()
        # leave the consumer group
        consumer.close()
        # Attempt to delete the group, should fail
        rpk = RpkTool(self.redpanda)
        out = rpk.group_delete(group=group_name)
        assert "NON_EMPTY_GROUP" in out, (
            f"Group deletion should fail with inprogress transaction: {out}"
        )
        producer.commit_transaction()
        out = rpk.group_delete(group=group_name)
        assert "OK" in out, (
            f"Group deletion expected to succeed after committing transaction: {out}"
        )

    @cluster(num_nodes=3)
    def rejoin_member_test(self):
        self.redpanda.set_cluster_config({"group_new_member_join_timeout": 5000})
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
            }
        )

        group_name = "test"
        consumer1 = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": group_name,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 10000,
                "session.timeout.ms": 8000,
            }
        )

        producer.init_transactions()

        consumer1.subscribe([self.input_t])
        records = self.consume(consumer1)

        producer.begin_transaction()

        for record in records:
            assert record.error() == None
            producer.produce(self.output_t.name, record.value(), record.key())

        offsets = consumer1.position(consumer1.assignment())
        metadata = consumer1.consumer_group_metadata()

        consumer2 = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": group_name,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 10000,
                "session.timeout.ms": 8000,
            }
        )

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

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
            }
        )

        group_name = "test"
        static_group_id = "123"
        consumer1 = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": group_name,
                "group.instance.id": static_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        producer.init_transactions()

        consumer1.subscribe([self.input_t])
        records = self.consume(consumer1)

        producer.begin_transaction()

        for record in records:
            assert record.error() == None
            producer.produce(self.output_t.name, record.value(), record.key())

        offsets = consumer1.position(consumer1.assignment())
        metadata = consumer1.consumer_group_metadata()

        consumer2 = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": group_name,
                "group.instance.id": static_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

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
        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
                "transaction.timeout.ms": 900000,  # to avoid timing out
            }
        )
        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(self.output_t.name, "x", "y")
        producer.flush()

        # Default transactional id expiration is 7d, so the transaction
        # should be hung.
        wait_timeout_s = 20
        try:
            wait_until(
                self.no_running_transactions,
                timeout_sec=wait_timeout_s,
                backoff_sec=2,
                err_msg="Transactions still running",
            )
            assert False, "No running transactions found."
        except TimeoutError as e:
            assert "Transactions still running" in str(e)

        # transaction should be aborted.
        rpk.cluster_config_set("transactional_id_expiration_ms", 5000)
        wait_until(
            self.no_running_transactions,
            timeout_sec=wait_timeout_s,
            backoff_sec=2,
            err_msg="Transactions still running",
        )

        try:
            producer.commit_transaction()
            assert False, "transaction should have been aborted by now."
        except ck.KafkaException as e:
            assert e.args[0].code() == ck.KafkaError.INVALID_PRODUCER_ID_MAPPING, (
                f"Invalid error thrown on expiration {e}"
            )

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
            assert time() - begin <= timeout_s, (
                f"Can't init transactions within {timeout_s} sec"
            )
            try:
                producer = ck.Producer(
                    {
                        "bootstrap.servers": self.redpanda.brokers(),
                        "transactional.id": "0",
                        "transaction.timeout.ms": 5000,
                    }
                )
                producer.init_transactions()
                break
            except ck.cimpl.KafkaException as e:
                self.redpanda.logger.debug(f"error on init_transactions", exc_info=True)
                kafka_error = e.args[0]
                assert kafka_error.code() in [
                    ck.cimpl.KafkaError.NOT_COORDINATOR,
                    ck.cimpl.KafkaError._TIMED_OUT,
                ]

        producer.begin_transaction()

        for i in range(0, 10):
            producer.produce(
                self.input_t.name,
                str(i),
                str(i),
                partition=0,
                on_delivery=self.on_delivery,
            )
        producer.flush()

        # Wait for transactions to hit expiration timeout.
        wait_until(
            self.no_running_transactions,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Transactions are still running, expected to be expired.",
        )

        try:
            producer.produce(
                self.input_t.name,
                "test-post-expire",
                "test-post-expire",
                partition=0,
                on_delivery=self.on_delivery,
            )
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
        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
                "transaction.timeout.ms": 60000,
            }
        )

        producer.init_transactions()
        producer.begin_transaction()

        count = 0
        partition = 0
        records_per_add = 10

        def add_records():
            nonlocal count
            nonlocal partition
            for i in range(count, count + records_per_add):
                producer.produce(
                    self.input_t.name,
                    str(i),
                    str(i),
                    partition=partition,
                    on_delivery=self.on_delivery,
                )
            producer.flush()
            count = count + records_per_add

        def graceful_transfer():
            # Issue a graceful leadership transfer.
            old_leader = self.admin.get_partition_leader(
                namespace="kafka", topic=self.input_t.name, partition=partition
            )
            self.admin.transfer_leadership_to(
                namespace="kafka",
                topic=self.input_t.name,
                partition=partition,
                target_id=None,
            )

            def leader_is_changed():
                new_leader = self.admin.get_partition_leader(
                    namespace="kafka", topic=self.input_t.name, partition=partition
                )
                return (new_leader != -1) and (new_leader != old_leader)

            wait_until(
                leader_is_changed,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Failed to establish current leader",
            )

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

        consumer = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": "test",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        try:
            consumer.subscribe([self.input_t])
            records = []
            while len(records) != count:
                records.extend(self.consume(consumer, max_records=count, timeout_s=10))
            assert len(records) == count, f"Not all records consumed, expected {count}"
            keys = set([int(r.key()) for r in records])
            assert all(i in keys for i in range(0, count)), f"Missing records {keys}"
        finally:
            consumer.close()

    @cluster(num_nodes=3)
    def graceful_leadership_transfer_tx_coordinator_test(self):
        p_count = 10
        producers = [
            ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "transactional.id": str(i),
                    "transaction.timeout.ms": 900000,
                }
            )
            for i in range(0, p_count)
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
                    p.produce(
                        self.input_t.name,
                        str(i),
                        str(i),
                        partition=partition,
                        on_delivery=self.on_delivery,
                    )
                p.flush()
                count = count + records_per_add

        def graceful_transfer():
            # Issue a graceful leadership transfer of tx coordinator
            old_leader = self.admin.get_partition_leader(
                namespace="kafka_internal", topic="tx", partition="0"
            )  # Fix this when we partition tx coordinator.
            self.admin.transfer_leadership_to(
                namespace="kafka_internal", topic="tx", partition="0", target_id=None
            )

            def leader_is_changed():
                new_leader = self.admin.get_partition_leader(
                    namespace="kafka_internal", topic="tx", partition="0"
                )
                return (new_leader != -1) and (new_leader != old_leader)

            wait_until(
                leader_is_changed,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Failed to establish current leader",
            )

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
        consumer = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": "test",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        try:
            consumer.subscribe([self.input_t])
            records = []
            while len(records) != count:
                records.extend(self.consume(consumer, max_records=count, timeout_s=10))
            assert len(records) == count, f"Not all records consumed, expected {count}"
            keys = set([int(r.key()) for r in records])
            assert all(i in keys for i in range(0, count)), f"Missing records {keys}"
        finally:
            consumer.close()

    @cluster(num_nodes=3)
    def delete_topic_with_active_txns_test(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("t1")
        rpk.create_topic("t2")

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
            }
        )

        # Non transactional
        producer_nt = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
            }
        )

        consumer = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "group.id": "test1",
                "isolation.level": "read_committed",
            }
        )

        consumer.subscribe([TopicSpec(name="t2")])

        producer.init_transactions()
        producer.begin_transaction()

        def add_records(topic, producer):
            for i in range(0, 100):
                producer.produce(
                    topic, str(i), str(i), partition=0, on_delivery=self.on_delivery
                )
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
        message_count = int(segments_per_producer * segment_size / message_size)
        msg_body = random.randbytes(message_size)

        producers = []
        self.logger.info(f"producing {message_count} messages per producer")
        for i in range(producers_count):
            p = ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "enable.idempotence": True,
                }
            )
            producers.append(p)
            for m in range(message_count):
                p.produce(
                    topic,
                    str(f"p-{i}-{m}"),
                    msg_body,
                    partition=0,
                    on_delivery=self.on_delivery,
                )
            p.flush()

        # Capture the proudcer info before evicting the segments
        producers_before = self.kafka_cli.describe_producers(topic=topic, partition=0)
        assert len(producers_before) == producers_count, "Producer metadata mismatch"

        self.client().alter_topic_config(
            topic=topic, key=TopicSpec.PROPERTY_RETENTION_BYTES, value=128
        )
        self.client().alter_topic_config(
            topic=topic, key=TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES, value=128
        )

        def segments_removed():
            removed_per_node = defaultdict(int)
            metric_sample = self.redpanda.metrics_sample(
                "log_segments_removed", self.redpanda.started_nodes()
            )
            metric = metric_sample.label_filter(dict(namespace="kafka", topic=topic))
            for m in metric.samples:
                removed_per_node[m.node] += m.value
            return all([v > 0 for v in removed_per_node.values()])

        wait_until(segments_removed, timeout_sec=60, backoff_sec=1)
        # produce until next segment roll
        #
        # TODO: change this when we will implement cleanup on current,
        # not the next eviction
        last_producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "enable.idempotence": True,
            }
        )

        message_count_to_roll_segment = int(message_count / segments_per_producer) + 100
        # produce enough data to roll the single segment
        for m in range(message_count_to_roll_segment):
            last_producer.produce(
                topic,
                str(f"last-mile-{m}"),
                msg_body,
                partition=0,
                on_delivery=self.on_delivery,
            )
            last_producer.flush()
        # restart redpanda to make sure rm_stm recovers state from snapshot,
        # which should be now cleaned and do not contain expired producer ids
        self.redpanda.restart_nodes(self.redpanda.nodes)

        producers_after = self.kafka_cli.describe_producers(topic=topic, partition=0)
        assert len(producers_after) < len(producers_before), (
            f"Incorrect number of producers restored from snapshot {len(producers_after)}"
        )

    @cluster(num_nodes=3)
    def check_progress_with_fencing_and_eviction_test(self):
        """Ensures a fenced producer can make progress with eviction running in the background."""

        def one_tx(tx_id: str, keep_open: bool = False):
            producer = ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "transactional.id": tx_id,
                    "transaction.timeout.ms": 15 * 60 * 1000,
                }
            )
            producer.init_transactions()
            producer.begin_transaction()
            producer.produce(self.input_t.name, "test", "test")
            producer.flush()
            if keep_open:
                return
            producer.commit_transaction()

        metric = "vectorized_cluster_producer_state_manager_producer_manager_total_active_producers"
        rpk = RpkTool(self.redpanda)
        topic_leader = self.redpanda.partitions(self.input_t.name)[0].leader
        active_producers_metric = MetricCheck(
            self.redpanda.logger, self.redpanda, topic_leader, [metric], reduce=sum
        )

        def wait_for_active_producers(count: int):
            wait_until(
                lambda: active_producers_metric.evaluate(
                    [(metric, lambda _, val: val == count)]
                ),
                timeout_sec=30,
                backoff_sec=5,
                err_msg=f"Timed out waiting for active producers to reach: {count}",
            )

        def evict_all_producers():
            rpk.cluster_config_set("max_concurrent_producer_ids", 1)
            wait_for_active_producers(1)
            rpk.cluster_config_set("max_concurrent_producer_ids", 1000000)

        # Hack to workaround max_concurrent_producer_ids to be atleast 1
        one_tx(tx_id="producer_cannot_remove", keep_open=True)

        # Each iteration fences the old producer from previous iteration by running
        # init transactions
        # Seed some producers
        tx_id = "fence_test"
        for _ in range(5):
            one_tx(tx_id)

        # Evict all producers in each iteration and ensure the fencing can still make progress
        for _ in range(5):
            evict_all_producers()
            one_tx(tx_id)

    @cluster(num_nodes=3)
    def check_progress_after_fencing_test(self):
        """Checks that a fencing producer makes progress after fenced producers are evicted."""

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "test",
                "transaction.timeout.ms": 100000,
            }
        )

        topic_name = self.topics[0].name

        # create a pid, do not commit/abort transaction.
        producer.init_transactions()
        producer.begin_transaction()
        producer.produce(topic_name, "0", "0", 0, self.on_delivery)
        producer.flush()

        # fence the above pid with another producer
        producer0 = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "test",
                "transaction.timeout.ms": 100000,
            }
        )
        producer0.init_transactions()
        producer0.begin_transaction()
        producer0.produce(topic_name, "0", "0", 0, self.on_delivery)

        max_concurrent_pids = 1
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("max_concurrent_producer_ids", str(max_concurrent_pids))

        self.wait_for_eviction(max_concurrent_pids, 1)

        producer0.commit_transaction()

    @cluster(num_nodes=3)
    def check_pids_overflow_test(self):
        rpk = RpkTool(self.redpanda)
        max_concurrent_producer_ids = 10
        ans = rpk.cluster_config_set(
            "max_concurrent_producer_ids", str(max_concurrent_producer_ids)
        )

        topic = self.topics[0].name

        def _produce_one(producer, idx):
            self.logger.debug(f"producing using {idx} producer")
            producer.produce(
                topic,
                f"record-key-producer-{idx}",
                f"record-value-producer-{idx}",
                partition=0,
                on_delivery=self.on_delivery,
            )
            producer.flush()

        max_producers = 50
        producers = []
        for i in range(max_producers):
            p = ck.Producer(
                {
                    "bootstrap.servers": self.redpanda.brokers(),
                    "enable.idempotence": True,
                }
            )
            _produce_one(p, i)
            producers.append(p)

        evicted_count = max_producers - max_concurrent_producer_ids

        wait_until(
            lambda: self.wait_for_eviction(max_concurrent_producer_ids, evicted_count),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Producers not evicted in time",
        )

        # validate that the producers are evicted with LRU policy,
        # starting from this producer there should be no sequence
        # number errors as those producer state should not be evicted
        last_not_evicted_producer_idx = max_producers - max_concurrent_producer_ids + 1
        for i in range(last_not_evicted_producer_idx, len(producers)):
            _produce_one(producers[i], i)

        expected_records = (
            len(producers) - last_not_evicted_producer_idx + max_producers
        )
        num_consumed = 0

        consumer = ck.Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": "123",
                "auto.offset.reset": "earliest",
            }
        )

        consumer.subscribe([topic])

        while num_consumed < expected_records:
            self.redpanda.logger.debug(
                f"Consumed {num_consumed} of of {expected_records}"
            )
            records = self.consume(consumer)
            num_consumed += len(records)

        assert num_consumed == expected_records

    @cluster(num_nodes=3)
    def unsafe_abort_group_transaction_test(self):
        def random_group_name():
            return "".join(random.choice(string.ascii_uppercase) for _ in range(16))

        def wait_for_active_producers(count: int):
            def describe_active_producers():
                active_producers = []
                for partition in range(0, 16):
                    desc = self.kafka_cli.describe_producers(
                        "__consumer_offsets", partition
                    )
                    for producer in desc:
                        tx_start_offset = producer["CurrentTransactionStartOffset"]
                        if "None" in tx_start_offset:
                            continue
                        if int(tx_start_offset) >= 0:
                            active_producers.append(producer)
                return active_producers

            wait_until(
                lambda: len(describe_active_producers()) == count,
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Timed out waiting for producer count to reach {count}",
            )

        group_name = random_group_name()
        input_records = 10
        self.generate_data(self.input_t, input_records)

        # setup consumer offsets
        rpk = RpkTool(self.redpanda)
        rpk.consume(topic=self.input_t.name, n=1, group="test-group")

        wait_for_active_producers(0)

        # Setup a consumer to consume from ^^ topic and
        # produce to a target topic.
        producer_conf = {
            "bootstrap.servers": self.redpanda.brokers(),
            "transactional.id": "test-repro",
            # Large-ish timeout
            "transaction.timeout.ms": 300000,
        }
        producer = ck.Producer(producer_conf)
        consumer_conf = {
            "bootstrap.servers": self.redpanda.brokers(),
            "group.id": group_name,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = ck.Consumer(consumer_conf)
        consumer.subscribe([self.input_t])

        # Consume - identity transform - produce
        producer.init_transactions()
        _ = self.consume(consumer)
        # Start a transaction and flush some offsets
        producer.begin_transaction()
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()), consumer.consumer_group_metadata()
        )
        producer.flush()

        wait_until(
            lambda: len(self.admin.get_all_transactions()) == 1,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timed out waiting for transaction to appear",
        )

        wait_for_active_producers(1)

        self.admin.unsafe_abort_group_transaction(
            group_id=group_name, pid=1, epoch=0, sequence=0
        )
        wait_for_active_producers(0)
        producer.commit_transaction()

        wait_until(
            lambda: self.no_running_transactions(),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timed out waiting for running transactions to wind down.",
        )
