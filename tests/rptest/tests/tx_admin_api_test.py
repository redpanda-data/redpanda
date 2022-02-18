# Copyright 2021 Vectorized, Inc.
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
from ducktape.mark import ignore
from rptest.services.admin import Admin
import confluent_kafka as ck
from rptest.tests.redpanda_test import RedpandaTest


class TxAdminTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3, replication_factor=3),
              TopicSpec(partition_count=3, replication_factor=3))

    def __init__(self, test_context):
        super(TxAdminTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf={
                                 "enable_idempotence": True,
                                 "enable_transactions": True,
                                 "tx_timeout_delay_ms": 10000000,
                                 "abort_timed_out_transactions_interval_ms":
                                 10000000,
                                 'enable_leader_balancer': False
                             })

        self.admin = Admin(self.redpanda)

    def extract_pid(self, tx):
        return (tx["producer_id"]["id"], tx["producer_id"]["epoch"])

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
                    expected_pids = set(
                        map(self.extract_pid, txs_info['active_transactions']))
                    assert (len(expected_pids) == 2)

                assert (len(expected_pids) == len(
                    txs_info['active_transactions']))
                for tx in txs_info['active_transactions']:
                    assert (self.extract_pid(tx) in expected_pids)
                    assert (tx['status'] == 'ongoing')
                    assert (tx['timeout_ms'] == 60000)

    @cluster(num_nodes=3)
    @ignore  # https://github.com/redpanda-data/redpanda/issues/3849
    def test_expired_transaction(self):
        '''
        Problem: rm_stm contains timer to run try_abort_old_txs.
        This timer rearm on begin_tx to smallest transaction deadline,
        and after try_abort_old_txs to one of settings 
        abort_timed_out_transactions_interval_ms or tx_timeout_delay_ms.
        If we do sleep to transaction timeout and try to get expired 
        transaction, timer can be signal before our request and after clean
        expire transactions.

        How to solve:
        0) Run transaction
        1) Change leader for parititons 
        (New leader did not get requests for begin_txs,
        so his timer is armed on one of settings)
        2) Get expired transactions
        '''
        assert (len(self.redpanda.nodes) >= 2)

        producer1 = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': '30000'
        })
        producer1.init_transactions()
        producer1.begin_transaction()

        for topic in self.topics:
            for partition in range(topic.partition_count):
                producer1.produce(topic.name, '0', '0', partition)

        producer1.flush()

        # We need to change leader for all partition to another node
        for topic in self.topics:
            for partition in range(topic.partition_count):
                old_leader = self.admin.get_partition_leader(
                    namespace="kafka", topic=topic, partition=partition)

                self.admin.transfer_leadership_to(namespace="kafka",
                                                  topic=topic,
                                                  partition=partition,
                                                  target=None)

                def leader_is_changed():
                    return self.admin.get_partition_leader(
                        namespace="kafka", topic=topic,
                        partition=partition) != old_leader

                wait_until(leader_is_changed,
                           timeout_sec=30,
                           backoff_sec=2,
                           err_msg="Failed to establish current leader")

        expected_pids = None

        for topic in self.topics:
            for partition in range(topic.partition_count):
                txs_info = self.admin.get_transactions(topic.name, partition,
                                                       "kafka")
                assert ('active_transactions' not in txs_info)
                if expected_pids == None:
                    expected_pids = set(
                        map(self.extract_pid,
                            txs_info['expired_transactions']))
                    assert (len(expected_pids) == 1)

                assert (len(expected_pids) == len(
                    txs_info['expired_transactions']))
                for tx in txs_info['expired_transactions']:
                    assert (self.extract_pid(tx) in expected_pids)
                    assert (tx['status'] == 'ongoing')
                    assert (tx['timeout_ms'] == -1)

    @cluster(num_nodes=3)
    def test_mark_transaction_expired(self):
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

        txs_info = self.admin.get_transactions(topic.name, partition, "kafka")

        expected_pids = set(
            map(self.extract_pid, txs_info['active_transactions']))
        assert (len(expected_pids) == 2)

        abort_tx = list(expected_pids)[0]
        expected_pids.discard(abort_tx)

        for topic in self.topics:
            for partition in range(topic.partition_count):
                self.admin.mark_transaction_expired(topic.name, partition, {
                    "id": abort_tx[0],
                    "epoch": abort_tx[1]
                }, "kafka")

                txs_info = self.admin.get_transactions(topic.name, partition,
                                                       "kafka")
                assert ('expired_transactions' not in txs_info)

                assert (len(expected_pids) == len(
                    txs_info['active_transactions']))
                for tx in txs_info['active_transactions']:
                    assert (self.extract_pid(tx) in expected_pids)
                    assert (tx['status'] == 'ongoing')
                    assert (tx['timeout_ms'] == 60000)

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
