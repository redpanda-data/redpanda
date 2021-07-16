# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time

import requests
from ducktape.mark import ignore
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest


class ScalingUpTest(EndToEndTest):
    """
    Adding nodes to the cluster should result in partition reallocations to new 
    nodes
    """
    @cluster(num_nodes=5)
    def test_adding_nodes_to_cluster(self):
        self.redpanda = RedpandaService(self.test_context, 3, KafkaCliTools)
        # start single node cluster
        self.redpanda.start(nodes=[self.redpanda.nodes[0]])
        # create some topics
        topics = []
        total_replicas = 0
        for partition_count in range(1, 5):
            name = f"topic{len(topics)}"
            spec = TopicSpec(name=name,
                             partition_count=partition_count,
                             replication_factor=1)
            total_replicas += partition_count
            topics.append(spec)

        for spec in topics:
            self.redpanda.create_topic(spec)
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

            return all(p[1] > 1 for p in per_node.items())

        wait_until(partitions_rebalanced, timeout_sec=30, backoff_sec=1)
        # add third node
        self.redpanda.start_node(self.redpanda.nodes[2])
        wait_until(partitions_rebalanced, timeout_sec=30, backoff_sec=1)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)
