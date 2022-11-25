# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.services import redpanda
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.clients.types import TopicSpec
from ducktape.mark import parametrize


class PartitionBalancerScaleTest(PreallocNodesTest, PartitionMovementMixin):
    NODE_AVAILABILITY_TIMEOUT = 10
    MANY_PARTITIONS = "many_partitions"
    BIG_PARTITIONS = "big_partitions"

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context=test_context,
            node_prealloc_count=1,
            num_brokers=5,
            extra_rp_conf={
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec":
                self.NODE_AVAILABILITY_TIMEOUT,
                "partition_autobalancing_tick_interval_ms": 5000,
                "members_backend_retry_ms": 1000,
                "raft_learner_recovery_rate": 1073741824,
            },
            *args,
            **kwargs)

    def _start_producer(self, topic_name, msg_cnt, msg_size):
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size,
            msg_cnt,
            custom_node=self.preallocated_nodes)
        self.producer.start(clean=False)

        wait_until(lambda: self.producer.produce_status.acked > 10,
                   timeout_sec=120,
                   backoff_sec=1)

    def _start_consumer(self, topic_name, msg_size, consumers):

        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size,
            readers=consumers,
            nodes=self.preallocated_nodes)
        self.consumer.start(clean=False)

    def verify(self, topic_name, msg_size, consumers):
        self.producer.wait()

        # Await the consumer that is reading only the subset of data that
        # was written before it started.
        self.consumer.wait()
        assert self.consumer.consumer_status.validator.invalid_reads == 0
        del self.consumer

        # Start a new consumer to read all data written
        self._start_consumer(topic_name, msg_size, consumers)
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.valid_reads >= self.producer.produce_status.acked
        assert self.consumer.consumer_status.validator.invalid_reads == 0

    def node_replicas(self, topics, node_id):
        topic_descriptions = self.client().describe_topics(topics)

        replicas = set()
        for tp_d in topic_descriptions:
            for p in tp_d.partitions:
                for r in p.replicas:
                    if r == node_id:
                        replicas.add(f'{tp_d.name}/{p}')

        return replicas

    @cluster(num_nodes=6)
    @parametrize(type=MANY_PARTITIONS)
    @parametrize(type=BIG_PARTITIONS)
    def test_partition_balancer_with_many_partitions(self, type):
        replication_factor = 3
        if not self.redpanda.dedicated_nodes:
            # Mini mode, for developers working on the test on their workstation.
            # (not for use in CI)
            message_size = 16384
            message_cnt = 64000
            consumers = 1
            partitions_count = 16
        elif type == self.MANY_PARTITIONS:
            # FIXME: this is not enough data to keep up a background load through
            # the test.
            # https://github.com/redpanda-data/redpanda/issues/6245
            message_size = 128 * (2 ^ 10)
            message_cnt = 2000000
            consumers = 8
            partitions_count = 18000
        else:
            # FIXME: this is not enough data to keep up a background load through
            # the test.
            # https://github.com/redpanda-data/redpanda/issues/6245
            message_size = 512 * (2 ^ 10)
            message_cnt = 5000000
            consumers = 8
            partitions_count = 200

        topic = TopicSpec(partition_count=partitions_count,
                          replication_factor=replication_factor)
        self.client().create_topic(topic)

        self._start_producer(topic.name, message_cnt, message_size)
        self._start_consumer(topic.name, message_size, consumers=consumers)
        self.logger.info(
            f"waiting for {(message_size*message_cnt/2) / (2^20)} MB to be produced to "
            f"{partitions_count} partitions ({((message_size*message_cnt/2) / (2^20)) / partitions_count} MB per partition"
        )
        # wait for the partitions to be filled with data
        self.producer.wait_for_acks(message_cnt // 2,
                                    timeout_sec=300,
                                    backoff_sec=5)

        # stop one of the nodes to trigger partition balancer
        stopped = random.choice(self.redpanda.nodes)
        self.redpanda.stop_node(stopped)

        stopped_id = self.redpanda.node_id(stopped)

        def stopped_node_is_empty():
            replicas = self.node_replicas([topic.name], stopped_id)
            self.logger.debug(
                f"stopped node {stopped_id} hosts {len(replicas)} replicas")
            return len(replicas) == 0

        wait_until(stopped_node_is_empty, 120, 5)
        admin = Admin(self.redpanda)

        def all_reconfigurations_done():
            ongoing = admin.list_reconfigurations()
            self.logger.debug(
                f"Waiting for partition reconfigurations to finish. "
                f"Currently reconfiguring partitions: {len(ongoing)}")

            return len(ongoing) == 0

        wait_until(all_reconfigurations_done, 300, 5)

        self.verify(topic.name, message_size, consumers)

    @cluster(num_nodes=6)
    @parametrize(type=MANY_PARTITIONS)
    @parametrize(type=BIG_PARTITIONS)
    def test_node_operations_at_scale(self, type):
        replication_factor = 3
        if not self.redpanda.dedicated_nodes:
            # Mini mode, for developers working on the test on their workstation.
            # (not for use in CI)
            message_size = 16384
            message_cnt = 64000
            consumers = 1
            partitions_count = 40
            max_concurrent_moves = 5
        elif type == self.MANY_PARTITIONS:
            message_size = 128 * (2 ^ 10)
            message_cnt = 2000000
            consumers = 8
            partitions_count = 18000
            max_concurrent_moves = 400
            timeout = 300
        else:
            message_size = 512 * (2 ^ 10)
            message_cnt = 5000000
            consumers = 8
            partitions_count = 200
            max_concurrent_moves = 200
            timeout = 300

        # set max number of concurrent moves
        self.redpanda.set_cluster_config(
            {"partition_autobalancing_concurrent_moves": max_concurrent_moves})

        topic = TopicSpec(partition_count=partitions_count,
                          replication_factor=replication_factor)
        self.client().create_topic(topic)

        self._start_producer(topic.name, message_cnt, message_size)
        self._start_consumer(topic.name, message_size, consumers=consumers)
        mb = 1024 * 1024
        self.logger.info(
            f"waiting for {(message_size*message_cnt) / mb} MB to be produced to "
            f"{partitions_count} partitions ({((message_size*message_cnt) / mb) / partitions_count} MB per partition"
        )
        # wait for the partitions to be filled with data
        self.producer.wait_for_acks(message_cnt // 2,
                                    timeout_sec=timeout,
                                    backoff_sec=5)

        admin = Admin(self.redpanda)
        brokers = admin.get_brokers()
        # decommission one of the nodes
        decommissioned_id = random.choice(brokers)['node_id']
        self.logger.info(
            f"cluster brokers: {brokers}, decommissioning: {decommissioned_id}"
        )
        admin.decommission_broker(decommissioned_id)

        def decommission_ended():
            replicas = self.node_replicas([topic.name], decommissioned_id)
            self.logger.debug(
                f"decommissioned node {decommissioned_id} hosts {len(replicas)} replicas"
            )
            if len(replicas) != 0:
                return False
            all_brokers = set()
            for n in self.redpanda.nodes:
                current_id = self.redpanda.node_id(n)
                if current_id == decommissioned_id:
                    continue
                brokers = admin.get_brokers(node=n)
                for i in [b['node_id'] for b in brokers]:
                    all_brokers.add(i)
            return decommissioned_id not in all_brokers

        wait_until(decommission_ended, timeout, 5)
        # restart node
        to_restart = None
        for n in self.redpanda.nodes:
            current_id = self.redpanda.node_id(n)
            if current_id == decommissioned_id:
                to_restart = n
                break

        def seed_servers_for(node):
            seeds = map(
                lambda n: {
                    "address": n.account.hostname,
                    "port": 33145
                }, self.redpanda.nodes)

            return list(
                filter(lambda n: n['address'] != node.account.hostname, seeds))

        self.redpanda.stop_node(to_restart)
        self.redpanda.clean_node(to_restart)
        self.redpanda.start_node(to_restart,
                                 override_cfg_params={
                                     "node_id": 10,
                                     "seed_servers":
                                     seed_servers_for(to_restart)
                                 })
        new_node_id = self.redpanda.node_id(to_restart)
        expected_per_node = partitions_count * replication_factor / len(
            self.redpanda.nodes)

        def partitions_moved_to_new_node():
            replicas = self.node_replicas([topic.name], new_node_id)
            self.logger.info(
                f"broker {new_node_id} is a host for {len(replicas)} replicas")
            return len(replicas) > 0.9 * expected_per_node

        wait_until(partitions_moved_to_new_node, timeout, 5)

        def all_reconfigurations_done():
            ongoing = admin.list_reconfigurations()
            self.logger.debug(
                f"Waiting for partition reconfigurations to finish. "
                f"Currently reconfiguring partitions: {len(ongoing)}")

            return len(ongoing) == 0

        wait_until(all_reconfigurations_done, timeout, 5)

        self.verify(topic.name, message_size, consumers)
