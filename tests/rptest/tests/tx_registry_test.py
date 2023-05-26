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

import random

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
import confluent_kafka as ck


class TransactionsMixin:
    def on_delivery_passes(self, err, msg):
        assert err == None, msg

    def on_delivery_fails(self, err, msg):
        assert err != None, err


class TxRegistryTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(TxRegistryTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf,
                                             log_level="trace")

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    def find_coordinator_inits_tx_registry_test(self):
        def find_tx_coordinator_passes():
            r = self.admin.find_tx_coordinator(
                "tx0", node=random.choice(self.redpanda.started_nodes()))
            return r["ec"] == 0

        def describe_tx_registry():
            state = self.admin.describe_tx_registry()
            return state["ec"] != 11, state

        wait_until(find_tx_coordinator_passes, timeout_sec=10, backoff_sec=1)

        info = wait_until_result(describe_tx_registry,
                                 timeout_sec=10,
                                 backoff_sec=1)
        assert info["ec"] == 0
        assert info["version"] == 0
        partitions = set()
        ranges = list()
        for (partition_id, hosted_txs) in info["tx_mapping"].items():
            partitions.add(partition_id)
            for hash_range in hosted_txs["hash_ranges"]:
                ranges.append(hash_range)
        assert len(partitions) > 0
        assert max(partitions) + 1 == len(partitions)
        assert min(partitions) == 0
        assert len(ranges) > 0
        ranges = sorted(ranges, key=lambda x: x["first"])
        assert ranges[0]["first"] == 0
        assert ranges[-1]["last"] == 4294967295
        for i in range(1, len(ranges)):
            assert ranges[i - 1]["last"] + 1 == ranges[i]["first"]


class TxCoordinatorDrainingTest(RedpandaTest, TransactionsMixin):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
            "transaction_coordinator_partitions": 1
        }

        super(TxCoordinatorDrainingTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf,
                             log_level="trace")

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=3)
    def get_draining_transactions_inflight_test(self):
        tx_id = "transaction_0"

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()

        for i in range(10):
            producer.begin_transaction()
            for j in range(10):
                producer.produce(self.topics[0].name,
                                 str(i) + "_" + str(j),
                                 str(j) + "_" + str(j),
                                 on_delivery=self.on_delivery_passes)
            producer.commit_transaction()

        draining_ranges = [(0, 100), (500, 1000), (999, 4294967295)]
        draining_txes = ["tx_1", "tx_2", "tx_3"]
        admin = Admin(self.redpanda)
        admin.start_tx_draining(tm_partition=0,
                                repartitioning_id=100,
                                tx_ids=draining_txes,
                                tx_ranges=draining_ranges)

        draining = admin.get_draining_transactions(tm_partition=0)

        assert draining["operation"]["repartitioning_id"] == 100
        assert len(
            draining["operation"]["hash_ranges"]) == len(draining_ranges)
        assert set([(x["first"], x["last"])
                    for x in draining["operation"]["hash_ranges"]
                    ]) == set(draining_ranges)
        assert len(
            draining["operation"]["transactional_ids"]) == len(draining_txes)
        assert set(
            draining["operation"]["transactional_ids"]) == set(draining_txes)

    @cluster(num_nodes=3)
    @parametrize(drain_type="by_range")
    @parametrize(drain_type="by_tx_id")
    def draining_blocks_new_add_partitions_to_txn_test(self, drain_type):
        tx_id = "transaction_0"

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()

        for i in range(10):
            producer.begin_transaction()
            for j in range(10):
                producer.produce(self.topics[0].name,
                                 str(i) + "_" + str(j),
                                 str(j) + "_" + str(j),
                                 on_delivery=self.on_delivery_passes)
            producer.commit_transaction()

        drain_range = (0, 4294967295)
        admin = Admin(self.redpanda)
        if drain_type == 'by_range':
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[],
                                    tx_ranges=[drain_range])
        else:
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[tx_id],
                                    tx_ranges=[])

        producer.begin_transaction()
        producer.produce(
            self.topics[0].name,
            "key",
            "val",
            on_delivery=self.on_delivery_fails,
        )
        error = False
        try:
            producer.flush()
            producer.commit_transaction()
        except Exception:
            error = True
        assert error, "Error must happen on tx producing + commiting"

    @cluster(num_nodes=3)
    @parametrize(drain_type="by_range")
    @parametrize(drain_type="by_tx_id")
    def draining_blocks_init_producer_id_test(self, drain_type):
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': "0",
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()

        for i in range(10):
            producer.begin_transaction()
            for j in range(10):
                producer.produce(self.topics[0].name,
                                 str(i) + "_" + str(j),
                                 str(j) + "_" + str(j),
                                 on_delivery=self.on_delivery_passes)
            producer.commit_transaction()

        tx_id = "transaction_0"
        drain_range = (0, 4294967295)
        admin = Admin(self.redpanda)
        if drain_type == 'by_range':
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[],
                                    tx_ranges=[drain_range])
        else:
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[tx_id],
                                    tx_ranges=[])

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
            'transaction.timeout.ms': 10000,
        })
        error = False
        try:
            producer.init_transactions()
        except Exception:
            error = True
        assert error, "Error must happen on init transaction"

    @cluster(num_nodes=3)
    @parametrize(drain_type="by_range")
    @parametrize(drain_type="by_tx_id")
    def draining_permits_inflight_add_partitions_to_txn_test(self, drain_type):
        tx_id = "transaction_0"
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()
        producer.begin_transaction()
        for j in range(10):
            producer.produce(self.topics[0].name,
                             f"value{j}",
                             f"key{j}",
                             on_delivery=self.on_delivery_passes)
        producer.flush()

        drain_range = (0, 4294967295)
        admin = Admin(self.redpanda)
        if drain_type == 'by_range':
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[],
                                    tx_ranges=[drain_range])
        else:
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[tx_id],
                                    tx_ranges=[])
        for j in range(10):
            producer.produce(self.topics[0].name,
                             f"value{j}",
                             f"key{j}",
                             on_delivery=self.on_delivery_passes)
        for j in range(10):
            producer.produce(self.topics[1].name,
                             f"value{j}",
                             f"key{j}",
                             on_delivery=self.on_delivery_passes)
        producer.flush()
        producer.commit_transaction()

    @cluster(num_nodes=3)
    @parametrize(drain_type="by_range", finish_type="commit")
    @parametrize(drain_type="by_tx_id", finish_type="commit")
    @parametrize(drain_type="by_range", finish_type="abort")
    @parametrize(drain_type="by_tx_id", finish_type="abort")
    def draining_permits_end_txn_test(self, drain_type, finish_type):
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': "0",
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()

        for i in range(10):
            producer.begin_transaction()
            for j in range(10):
                producer.produce(self.topics[0].name,
                                 str(i) + "_" + str(j),
                                 str(j) + "_" + str(j),
                                 on_delivery=self.on_delivery_passes)
            producer.commit_transaction()

        tx_id = "transaction_0"
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': tx_id,
            'transaction.timeout.ms': 10000,
        })

        producer.init_transactions()
        producer.begin_transaction()
        for j in range(10):
            producer.produce(self.topics[0].name,
                             str(i) + "_" + str(j),
                             str(j) + "_" + str(j),
                             on_delivery=self.on_delivery_passes)
        producer.flush()

        drain_range = (0, 4294967295)
        admin = Admin(self.redpanda)
        if drain_type == 'by_range':
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[],
                                    tx_ranges=[drain_range])
        else:
            admin.start_tx_draining(tm_partition=0,
                                    repartitioning_id=0,
                                    tx_ids=[tx_id],
                                    tx_ranges=[])
        if (finish_type == "commit"):
            producer.commit_transaction()
        else:
            producer.abort_transaction()
