# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
import confluent_kafka as ck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import DEFAULT_LOG_ALLOW_LIST
import re
import requests
from ducktape.utils.util import wait_until

TIMEOUT_ALLOW_LIST = [
    re.compile(r".*Unexpected tx_error error: {tx::errc::timeout}.*")
]


class TxAdminTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3, replication_factor=3),
              TopicSpec(partition_count=3, replication_factor=3))

    def __init__(self, test_context):
        super(TxAdminTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             log_level="trace",
                             extra_rp_conf={
                                 "tx_timeout_delay_ms": 10000000,
                                 "abort_timed_out_transactions_interval_ms":
                                 10000000,
                                 'enable_leader_balancer': False
                             })

        self.admin = Admin(self.redpanda)

    def extract_pid(self, tx):
        return tx["producer_id"]

    @cluster(num_nodes=3)
    def test_simple_get_transaction(self):
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

        expected_pids = None

        for topic in self.topics:
            for partition in range(topic.partition_count):
                txs_info = self.admin.get_transactions(topic.name, partition,
                                                       "kafka")
                assert ('expired_transactions' not in txs_info)
                if expected_pids == None:
                    expected_pids = list(
                        map(self.extract_pid, txs_info['active_transactions']))
                    assert (len(expected_pids) == 2)

                assert (len(expected_pids) == len(
                    txs_info['active_transactions']))
                for tx in txs_info['active_transactions']:
                    assert (self.extract_pid(tx) in expected_pids)
                    assert (tx['status'] == 'ongoing')
                    assert (tx['timeout_ms'] == 60000)

    @cluster(num_nodes=3,
             log_allow_list=DEFAULT_LOG_ALLOW_LIST + TIMEOUT_ALLOW_LIST)
    def test_mark_transaction_expired(self):
        producer1 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 900000,  # avoid auto timeout
        })
        producer2 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '1',
            'transaction.timeout.ms': 900000,  # avoid auto timeout
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

        txs_info = self.admin.get_transactions(topic.name, partition, "kafka")
        expected_pids = list(
            map(self.extract_pid, txs_info['active_transactions']))
        assert len(expected_pids) == 2, expected_pids

        # pick a pid to mark for expiration
        producer_to_expire = expected_pids[0]
        untouched_producer = expected_pids[1]

        def mark_expired(topic, partition):
            try:
                self.admin.mark_transaction_expired(
                    topic.name, partition, {
                        "id": producer_to_expire['id'],
                        "epoch": producer_to_expire['epoch'],
                    }, "kafka")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 404:
                    raise
            return True

        # Pick a random partition and expire the producer
        # Note that each producer is producing to all ntps
        # so the expiration can be triggered from any of the ntps
        # and should be propagated to all the ntps part of the
        # the transaction.
        random_topic = random.choice(self.topics)
        random_partition = random.randint(0, random_topic.partition_count - 1)
        wait_until(lambda: mark_expired(random_topic, random_partition),
                   timeout_sec=10,
                   backoff_sec=1,
                   retry_on_exc=True)

        def transaction_expired():
            for topic in self.topics:
                for partition in range(topic.partition_count):
                    txs_info = self.admin.get_transactions(namespace="kafka",
                                                           topic=topic.name,
                                                           partition=partition)
                    self.logger.info(
                        f"{topic.name}/{partition} transactions: {txs_info}")
                    if 'expired_transactions' in txs_info:
                        return False

                    if len(txs_info['active_transactions']) != 1:
                        return False
                    for tx in txs_info['active_transactions']:
                        if self.extract_pid(tx) != untouched_producer:
                            return False
                        assert tx['status'] == 'ongoing'
            return True

        wait_until(transaction_expired, timeout_sec=30, backoff_sec=0.5)

    @cluster(num_nodes=3)
    def test_all_transactions(self):
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

        txs_info = self.admin.get_all_transactions()
        assert len(txs_info) == 1

        expected_partitions = dict()
        tx = txs_info[0]

        assert tx["transactional_id"] == tx_id
        assert tx["timeout_ms"] == 60000

        for partition in tx["partitions"]:
            assert partition["ns"] == "kafka"
            if partition["topic"] not in expected_partitions:
                expected_partitions[partition["topic"]] = set()
            expected_partitions[partition["topic"]].add(
                partition["partition_id"])

        for topic in self.topics:
            assert len(
                expected_partitions[topic.name]) == topic.partition_count
            for partition in range(topic.partition_count):
                assert partition in expected_partitions[topic.name]

    @cluster(num_nodes=3)
    def test_delete_topic_from_ongoin_tx(self):
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

        txs_info = self.admin.get_all_transactions()
        assert len(
            txs_info) == 1, "Should be only one transaction in current time"

        rpk = RpkTool(self.redpanda)
        topic_name = self.topics[0].name
        rpk.delete_topic(topic_name)

        tx = txs_info[0]
        assert tx[
            "transactional_id"] == tx_id, f"Expected transactional_id: {tx_id}, but got {tx['transactional_id']}"

        for partition in tx["partitions"]:
            assert (partition["ns"] == "kafka")
            if partition["topic"] == topic_name:
                self.admin.delete_partition_from_transaction(
                    tx["transactional_id"], partition["ns"],
                    partition["topic"], partition["partition_id"],
                    partition["etag"])

        producer.commit_transaction()

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
        })
        producer.init_transactions()
        producer.begin_transaction()

        for topic in self.topics:
            if topic.name is not topic_name:
                for partition in range(topic.partition_count):
                    producer.produce(topic.name, '0', '0', partition)

        producer.commit_transaction()

    @cluster(num_nodes=3)
    def test_delete_non_existent_topic(self):
        tx_id = "0"
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
        })
        producer.init_transactions()
        producer.begin_transaction()

        error_topic_name = "error_topic"

        for topic in self.topics:
            assert error_topic_name is not topic.name
            for partition in range(topic.partition_count):
                producer.produce(topic.name, '0', '0', partition)

        producer.flush()

        try:
            self.admin.delete_partition_from_transaction(
                tx_id, "kafka", error_topic_name, 0, 0)
        except requests.exceptions.HTTPError as e:
            assert e.response.text == '{"message": "Can not find partition({kafka/error_topic/0}) in transaction for delete", "code": 400}'

        producer.commit_transaction()

    @cluster(num_nodes=3)
    def test_delete_non_existent_tid(self):
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

        txs_info = self.admin.get_all_transactions()
        tx = txs_info[0]

        topic_name = self.topics[0].name
        error_tx_id = "1"

        for partition in tx["partitions"]:
            if partition["topic"] == topic_name:
                try:
                    self.admin.delete_partition_from_transaction(
                        error_tx_id, partition["ns"], partition["topic"],
                        partition["partition_id"], partition["etag"])
                    assert False
                except requests.exceptions.HTTPError as e:
                    assert e.response.status_code == 404

        producer.commit_transaction()

    @cluster(num_nodes=3)
    def test_delete_non_existent_etag(self):
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

        txs_info = self.admin.get_all_transactions()
        tx = txs_info[0]

        topic_name = self.topics[0].name

        for partition in tx["partitions"]:
            if partition["topic"] == topic_name:
                try:
                    self.admin.delete_partition_from_transaction(
                        tx_id, partition["ns"], partition["topic"],
                        partition["partition_id"], partition["etag"] + 100)
                except requests.exceptions.HTTPError as e:
                    e.response.text == '{{"message": "Can not find partition({{{}/{}/{}}}) in transaction for delete", "code": 400}}'.format(
                        partition["ns"], partition["topic"],
                        partition["partition_id"])

        producer.commit_transaction()
