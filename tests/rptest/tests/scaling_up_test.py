# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
import random
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest


class ScalingUpTest(EndToEndTest):
    """
    Adding nodes to the cluster should result in partition reallocations to new
    nodes
    """
    rebalance_timeout = 120
    group_topic_partitions = 16

    def _replicas_per_node(self):
        kafkacat = KafkaCat(self.redpanda)
        node_replicas = {}
        md = kafkacat.metadata()
        self.redpanda.logger.debug(f"metadata: {md}")
        for topic in md['topics']:
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    if id not in node_replicas:
                        node_replicas[id] = 0
                    node_replicas[id] += 1

        return node_replicas

    def _topic_replicas_per_node(self):
        kafkacat = KafkaCat(self.redpanda)
        topic_replicas = defaultdict(lambda: defaultdict(int))
        md = kafkacat.metadata()
        self.redpanda.logger.debug(f"metadata: {md}")
        for topic in md['topics']:
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    topic_replicas[topic['topic']][id] += 1

        return topic_replicas

    def wait_for_partitions_rebalanced(self, total_replicas, timeout_sec):

        expected_per_node = total_replicas / len(self.redpanda.started_nodes())
        expected_range = [0.8 * expected_per_node, 1.2 * expected_per_node]

        def partitions_rebalanced():
            per_node = self._replicas_per_node()
            self.redpanda.logger.info(
                f"replicas per node: {per_node}, expected range: [{expected_range[0]},{expected_range[1]}]"
            )
            if len(per_node) < len(self.redpanda.started_nodes()):
                return False

            replicas = sum(per_node.values())
            if replicas != total_replicas:
                return False

            if not all(expected_range[0] <= p[1] <= expected_range[1]
                       for p in per_node.items()):
                return False
            admin = Admin(self.redpanda)

            # make sure that all reconfigurations are finished
            return len(admin.list_reconfigurations()) == 0

        wait_until(partitions_rebalanced,
                   timeout_sec=timeout_sec,
                   backoff_sec=1)

    def create_topics(self, rf, partition_count):
        total_replicas = 0
        topics = []
        for _ in range(1, 5):
            partitions = partition_count
            spec = TopicSpec(partition_count=partition_count,
                             replication_factor=rf)
            total_replicas += partitions * rf
            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        self.topic = random.choice(topics).name

        return total_replicas

    @cluster(num_nodes=5)
    @matrix(partition_count=[1, 20])
    def test_adding_nodes_to_cluster(self, partition_count):
        self.redpanda = RedpandaService(self.test_context,
                                        3,
                                        extra_rp_conf={
                                            "group_topic_partitions":
                                            self.group_topic_partitions,
                                            "partition_autobalancing_mode":
                                            "node_add"
                                        })
        # start single node cluster
        self.redpanda.start(nodes=[self.redpanda.nodes[0]])
        # create some topics
        total_replicas = self.create_topics(rf=1,
                                            partition_count=partition_count)
        # include __consumer_offsets topic replica
        total_replicas += self.group_topic_partitions

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        # add second node
        self.redpanda.start_node(self.redpanda.nodes[1])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)
        # add third node
        self.redpanda.start_node(self.redpanda.nodes[2])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=8)
    @matrix(partition_count=[1, 20])
    def test_adding_multiple_nodes_to_the_cluster(self, partition_count):

        self.redpanda = RedpandaService(self.test_context,
                                        6,
                                        extra_rp_conf={
                                            "partition_autobalancing_mode":
                                            "node_add",
                                            "group_topic_partitions":
                                            self.group_topic_partitions
                                        })
        # start single node cluster
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        # create some topics
        topics = []
        total_replicas = self.create_topics(rf=3,
                                            partition_count=partition_count)
        # add consumer group topic replicas
        total_replicas += self.group_topic_partitions * 3

        throughput = 100000 if not self.debug_mode else 1000
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup(min_records=5 * throughput, timeout_sec=120)
        # add three nodes at once
        for n in self.redpanda.nodes[3:]:
            self.redpanda.clean_node(n)
            self.redpanda.start_node(n)

        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

    @cluster(num_nodes=8)
    @matrix(partition_count=[1, 20])
    def test_on_demand_rebalancing(self, partition_count):
        # start redpanda with disabled rebalancing
        self.redpanda = RedpandaService(self.test_context,
                                        6,
                                        extra_rp_conf={
                                            "partition_autobalancing_mode":
                                            "off",
                                            "group_topic_partitions":
                                            self.group_topic_partitions
                                        })
        # start single node cluster
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        # create some topics
        total_replicas = self.create_topics(rf=3,
                                            partition_count=partition_count)
        # add consumer group topic replicas
        total_replicas += self.group_topic_partitions * 3

        throughput = 100000 if not self.debug_mode else 1000
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup(min_records=5 * throughput,
                           timeout_sec=self.rebalance_timeout)
        # add three nodes
        for n in self.redpanda.nodes[3:]:
            self.redpanda.clean_node(n)
            self.redpanda.start_node(n)

        # verify that all new nodes are empty

        per_node = self._replicas_per_node()

        assert len(per_node) == 3

        # trigger rebalance
        admin = Admin(self.redpanda, retries_amount=20)
        admin.trigger_rebalance()

        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

    @cluster(num_nodes=7)
    def test_topic_hot_spots(self):
        self.redpanda = RedpandaService(self.test_context,
                                        5,
                                        extra_rp_conf={
                                            "group_topic_partitions":
                                            self.group_topic_partitions,
                                            "partition_autobalancing_mode":
                                            "node_add"
                                        })
        # start 3 nodes cluster
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        # create some topics
        total_replicas = 0
        topics = []
        for _ in range(1, 5):
            partitions = 30
            spec = TopicSpec(partition_count=partitions, replication_factor=3)
            total_replicas += partitions * 3
            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        self.topic = random.choice(topics).name

        # include __consumer_offsets topic replica
        total_replicas += self.group_topic_partitions * 3

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        # add second node
        self.redpanda.start_node(self.redpanda.nodes[3])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)
        # add third node
        self.redpanda.start_node(self.redpanda.nodes[4])
        self.wait_for_partitions_rebalanced(total_replicas=total_replicas,
                                            timeout_sec=self.rebalance_timeout)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

        topic_per_node = self._topic_replicas_per_node()
        for t, nodes in topic_per_node.items():
            self.logger.info(f"{t} spans {len(nodes)} nodes")
            # assert that each topic has replicas on all of the nodes
            assert len(nodes) == len(self.redpanda.nodes)
