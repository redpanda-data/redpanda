# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

import confluent_kafka as ck
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import DEFAULT_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.type_utils import rcast

TIMEOUT_ALLOW_LIST = [re.compile(r".*Unexpected tx_error error: {tx::errc::timeout}.*")]


class TxDisableTopicTest(RedpandaTest):
    topics = (
        TopicSpec(partition_count=3, replication_factor=3),
        TopicSpec(partition_count=3, replication_factor=3),
    )

    def __init__(self, test_context):
        super(TxDisableTopicTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            log_level="trace",
            extra_rp_conf={
                "tx_timeout_delay_ms": 10000000,
                "abort_timed_out_transactions_interval_ms": 10000000,
                "enable_leader_balancer": False,
            },
        )

        self.admin = Admin(self.redpanda)

    # check the first topic's 0th partition for ongoing transactions
    # sufficient for this test
    def get_transactions(self, log_prefix=""):
        topic = self.topics[0].name
        partition = 0
        transaction_info = self.admin.get_transactions(topic, partition, "kafka")
        self.logger.info(
            f"{log_prefix} fetched transactions for {topic}/{partition}: {transaction_info}"
        )
        return transaction_info

    # wait predicate for waiting until there are no active transactions
    def wait_until_no_transaction(self):
        transaction_info = self.get_transactions()
        return transaction_info.get("active_transactions", None) is None

    # wait predicate for waiting until there is an active transaction
    def wait_until_transaction(self):
        transaction_info = self.get_transactions()
        return transaction_info.get("active_transactions", None) is not None

    # wait until no transactions otherwise timeout exception
    def check_no_transaction(self):
        wait_until(
            self.wait_until_no_transaction,
            timeout_sec=5,
            backoff_sec=1,
            err_msg="transaciton never closed",
        )

    # wait until at least one active transaction is found otherwise timeout
    def check_transaction(self):
        wait_until(
            self.wait_until_transaction,
            timeout_sec=5,
            backoff_sec=1,
            err_msg="transaction never found",
        )

    # wait predicate for waiting until a transaction topic state change
    # is visible on all brokers
    def wait_until_tx_disable_state(self, should_be_disabled=True):
        admin = Admin(self.redpanda)

        nodes = self.redpanda.nodes

        # we need consistency on all nodes
        for node in nodes:

            def all_disabled():
                tx_partitions = admin.get_partitions(
                    topic="tx", namespace="kafka_internal", node=node
                )
                for partition in tx_partitions:
                    is_disabled = bool(partition["disabled"])
                    if is_disabled != should_be_disabled:
                        return False
                return True

            wait_until(
                all_disabled,
                timeout_sec=20,
                backoff_sec=1,
                err_msg="kafka_internal/tx never disabled",
            )

    # change kafka_internal/tx state to enabled or disabled, wati until
    # visible on all brokers
    def change_tx_topic_state(self, should_disable=True):
        log_prefix = "Disabl"
        if not should_disable:
            log_prefix = "Enabl"
        self.logger.debug(f"{log_prefix}ing kafka_internal/tx")
        self.admin.set_partitions_disabled("kafka_internal", "tx", value=should_disable)
        self.logger.debug(f"{log_prefix}ed kafka_internal/tx")

        self.wait_until_tx_disable_state(should_disable)

    # wait and assert transactions state, if not should_have_transaction assert that
    # eventually there are no active transactions
    # if should_have_transaction, assert that an active transaction is eventually found
    def get_assert_transaction_state(self, should_have_transaction, log_prefix=""):
        # wait until something is found
        if not should_have_transaction:
            self.check_no_transaction()
        else:
            self.check_transaction()

        # actually perform the assertion
        transaction_info = self.get_transactions(log_prefix=log_prefix)
        if should_have_transaction:
            active_transactions = transaction_info["active_transactions"]
            assert len(active_transactions) == 1, (
                f"expected one transaction but found {active_transactions}"
            )
            active_transaction = active_transactions[0]
            assert active_transaction["status"] == "ongoing", (
                f"expected status ongoing found status {active_transaction['status']}"
            )

    # consume from the first topic's 0th partition, return the found records
    def consume(self, count: int):
        default_topic = self.topics[0].name
        payloads = []

        def deserialize_str(value: bytes, ctx) -> str:
            return value.decode("utf-8")

        consumed = 0
        consumer = ck.DeserializingConsumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "key.deserializer": StringDeserializer("utf_8"),
                "value.deserializer": deserialize_str,
                "group.id": "self_group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([default_topic])

        self.logger.info(f"Consuming from {default_topic}")
        while consumed < count:
            msg = consumer.poll(1)
            if msg is None:
                continue
            payload = msg.value()
            self.logger.debug(f"Consumed {payload}")
            payloads.append(payload)
            consumed += 1

        consumer.close()
        return payloads

    # needed to wake kafka_internal/tx, otherwise it wont be found
    # on a fresh cluster
    def initial_transaction(self):
        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "1776",
                "transaction.timeout.ms": 900000,  # avoid auto timeout
            }
        )
        producer.init_transactions()
        producer.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producer.produce(topic.name, "wake_data", "wake_data", partition)

        producer.flush(5)
        producer.commit_transaction(5)
        self.wait_until_no_transaction()

        # force move offset past the intial payloads
        self.consume(3)

    @cluster(num_nodes=3, log_allow_list=DEFAULT_LOG_ALLOW_LIST + TIMEOUT_ALLOW_LIST)
    def test_tx_topic_disable(self):
        """checks if kafka_internal/tx can be disabled and reenabled
        1. transaction remains valid between disable and reenable
            a. start transaction
            b. disable tx
            c. commit and check timeout
            d. enable tx
            e. commit and check commit by consuming
        """

        blocked_producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "1771",
                "transaction.timeout.ms": 900000,  # avoid auto timeout
            }
        )
        blocked_producer.init_transactions()
        # a. start transaction
        blocked_producer.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                blocked_producer.produce(
                    topic.name, "hello_world", "hello_world", partition
                )
        blocked_producer.flush(5)

        self.get_assert_transaction_state(
            should_have_transaction=True, log_prefix="before disable"
        )

        # b. disable tx
        self.change_tx_topic_state(should_disable=True)

        # c. commit and check timeout
        did_time_out = False
        try:
            blocked_producer.commit_transaction(10)
        except KafkaException as e:
            did_time_out = True
            self.logger.debug(f"Exception and args were {e} with args: {e.args}")
            assert rcast(KafkaError, e.args[0]).retriable(), (
                "The error should be retryable"
            )
            assert not rcast(KafkaError, e.args[0]).txn_requires_abort(), (
                "The commit shouldn't be irrevocable"
            )

        assert did_time_out, "The commit should have timed out"

        self.get_assert_transaction_state(
            should_have_transaction=True, log_prefix="before reenable"
        )
        # d. enable tx
        self.change_tx_topic_state(should_disable=False)
        self.get_assert_transaction_state(
            should_have_transaction=True, log_prefix="after reenable"
        )

        # e. check that commit can resume as normal and results can be consumed
        blocked_producer.commit_transaction(5)
        self.get_assert_transaction_state(
            should_have_transaction=False, log_prefix="after reenable and commit"
        )

        found_payloads = self.consume(3)
        for payload in found_payloads:
            assert payload == "hello_world", f"expected hello_world, received {payload}"

    @cluster(num_nodes=3, log_allow_list=DEFAULT_LOG_ALLOW_LIST + TIMEOUT_ALLOW_LIST)
    def test_tx_after_disable(self):
        """check that transactions proceed normally after disable and reenable
        a. disable tx
        b. enable tx
        c. check that the producer can init and transact normally
        """
        # wake the transaction topic
        self.initial_transaction()

        # a. disable tx
        self.change_tx_topic_state(should_disable=True)
        # b. enable tx
        self.change_tx_topic_state(should_disable=False)

        # c. check nomral production & consumption
        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
                "transaction.timeout.ms": 900000,  # avoid auto timeout
            }
        )

        producer.init_transactions()
        producer.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producer.produce(
                    topic.name, "goodbye_world", "goodbye_world", partition
                )

        producer.flush(5)
        self.get_assert_transaction_state(should_have_transaction=True)
        producer.commit_transaction(5)
        self.get_assert_transaction_state(should_have_transaction=False)

        found_payloads = self.consume(3)
        for payload in found_payloads:
            assert payload == "goodbye_world", (
                f"expected goodbye_world, received {payload}"
            )

        self.get_assert_transaction_state(should_have_transaction=False)

    @cluster(num_nodes=3, log_allow_list=DEFAULT_LOG_ALLOW_LIST + TIMEOUT_ALLOW_LIST)
    def test_tx_init_while_disabled(self):
        """check reasonable timeout behavior if init occurs while disabled
        a. disable tx
        b. try init transactions
        c. assert failure
        d. enable tx
        e. check that the producer can init and transact normally
        """
        # wake the transaction topic
        self.initial_transaction()

        # a. disable tx
        self.change_tx_topic_state(should_disable=True)

        producer = ck.Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "transactional.id": "0",
                "transaction.timeout.ms": 900000,  # avoid auto timeout
            }
        )

        # b & c try init transactions, assert failure
        did_time_out = False
        try:
            producer.init_transactions(5)
        except KafkaException as e:
            did_time_out = True
            assert rcast(KafkaError, e.args[0]).retriable(), (
                "The error should be retryable"
            )
            assert not rcast(KafkaError, e.args[0]).txn_requires_abort(), (
                "The commit shouldn't be irrevocable"
            )
        assert did_time_out, "should have timed out"

        # d. enable tx
        self.change_tx_topic_state(should_disable=False)

        # e. check normal transactional produce
        producer.init_transactions()
        producer.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producer.produce(
                    topic.name, "goodbye_world", "goodbye_world", partition
                )

        producer.flush(5)
        self.get_assert_transaction_state(should_have_transaction=True)
        producer.commit_transaction(5)
        self.get_assert_transaction_state(should_have_transaction=False)

        found_payloads = self.consume(3)
        for payload in found_payloads:
            assert payload == "goodbye_world", (
                f"expected goodbye_world, received {payload}"
            )

        self.get_assert_transaction_state(should_have_transaction=False)
