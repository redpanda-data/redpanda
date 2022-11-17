# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
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
    @cluster(num_nodes=5)
    def test_adding_nodes_to_cluster(self):
        self.redpanda = RedpandaService(self.test_context,
                                        3,
                                        extra_rp_conf={
                                            "group_topic_partitions":
                                            1,
                                            "partition_autobalancing_mode":
                                            "node_add"
                                        })
        # start single node cluster
        self.redpanda.start(nodes=[self.redpanda.nodes[0]])
        # create some topics
        topics = []
        # include __consumer_offsets topic replica
        total_replicas = 1
        for partition_count in range(1, 5):
            name = f"topic{len(topics)}"
            spec = TopicSpec(name=name,
                             partition_count=partition_count,
                             replication_factor=1)
            total_replicas += partition_count
            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        # add second node
        self.redpanda.start_node(self.redpanda.nodes[1])
        kafkacat = KafkaCat(self.redpanda)

        def _replicas_per_node():
            node_replicas = {}
            md = kafkacat.metadata()
            self.redpanda.logger.info(f"metadata: {md}")
            for topic in md['topics']:
                for p in topic['partitions']:
                    for r in p['replicas']:
                        id = r['id']
                        if id not in node_replicas:
                            node_replicas[id] = 0
                        node_replicas[id] += 1

            return node_replicas

        def partitions_rebalanced():
            per_node = _replicas_per_node()
            self.redpanda.logger.info(f"replicas per node: {per_node}")
            if len(per_node) < len(self.redpanda.started_nodes()):
                return False

            replicas = sum(per_node.values())
            if replicas != total_replicas:
                return False

            return all(p[1] > 1 for p in per_node.items())

        wait_until(partitions_rebalanced, timeout_sec=30, backoff_sec=1)
        # add third node
        self.redpanda.start_node(self.redpanda.nodes[2])
        wait_until(partitions_rebalanced, timeout_sec=30, backoff_sec=1)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=8)
    def test_adding_multiple_nodes_to_the_cluster(self):
        group_topic_partitions = 16
        self.redpanda = RedpandaService(self.test_context,
                                        6,
                                        extra_rp_conf={
                                            "partition_autobalancing_mode":
                                            "node_add",
                                            "group_topic_partitions":
                                            group_topic_partitions
                                        })
        # start single node cluster
        self.redpanda.start(nodes=self.redpanda.nodes[0:3])
        # create some topics
        topics = []
        total_replicas = 0
        for _ in range(1, 5):
            partitions = random.randint(10, 20)
            spec = TopicSpec(partition_count=partitions, replication_factor=3)
            total_replicas += partitions * 3
            topics.append(spec)

        total_replicas += group_topic_partitions * 3

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)
            self.topic = spec.name
        throughput = 100000 if not self.debug_mode else 1000
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup(min_records=15 * throughput, timeout_sec=120)
        # add three nodes at once
        for n in self.redpanda.nodes[3:]:
            self.redpanda.start_node(n)
        kafkacat = KafkaCat(self.redpanda)

        def _replicas_per_node():
            node_replicas = {}
            md = kafkacat.metadata()
            for topic in md['topics']:
                for p in topic['partitions']:
                    for r in p['replicas']:
                        id = r['id']
                        if id not in node_replicas:
                            node_replicas[id] = 0
                        node_replicas[id] += 1

            return node_replicas

        expected_per_node = total_replicas / 6
        expected_range = [0.8 * expected_per_node, 1.2 * expected_per_node]

        def partitions_rebalanced():
            per_node = _replicas_per_node()
            self.redpanda.logger.info(
                f"replicas per node: {per_node}, expected range: [{expected_range[0]},{expected_range[1]}]"
            )
            if len(per_node) < len(self.redpanda.started_nodes()):
                return False

            replicas = sum(per_node.values())
            if replicas != total_replicas:
                return False

            return all(expected_range[0] < p[1] < expected_range[1]
                       for p in per_node.items())

        wait_until(partitions_rebalanced, timeout_sec=120, backoff_sec=1)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=120)
