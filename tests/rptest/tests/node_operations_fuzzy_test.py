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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest

DECOMMISSION = "decommission"
ADD = "add"
ADD_TOPIC = "add_tp"
DELETE_TOPIC = "delete_tp"
ALLOWED_REPLICATION = [1, 3]


class NodeOperationFuzzyTest(EndToEndTest):
    def generate_random_workload(self, count, skip_nodes):
        op_types = [ADD, DECOMMISSION]
        tp_op_types = [ADD_TOPIC, DELETE_TOPIC]
        # current state
        active_nodes = [1, 2, 3, 4, 5]
        decommissioned_nodes = []
        operations = []
        topics = []

        def eligible_active_nodes():
            return list(
                filter(lambda n: not (n == 1 or n in skip_nodes),
                       active_nodes))

        def decommission(id):
            active_nodes.remove(id)
            decommissioned_nodes.append(id)

        def add(id):
            active_nodes.append(id)
            decommissioned_nodes.remove(id)

        for _ in range(0, count):
            if len(decommissioned_nodes) == 2:
                id = random.choice(decommissioned_nodes)
                operations.append((ADD, id))
                add(id)
            elif len(decommissioned_nodes) == 0:
                id = random.choice(eligible_active_nodes())
                operations.append((DECOMMISSION, id))
                decommission(id)
            else:
                op = random.choice(op_types)
                if op == DECOMMISSION:
                    id = random.choice(eligible_active_nodes())
                    operations.append((DECOMMISSION, id))
                    decommission(id)
                elif op == ADD:
                    id = random.choice(decommissioned_nodes)
                    operations.append((ADD, id))
                    add(id)
            # topic operation
            if len(topics) == 0:
                op = ADD_TOPIC
            else:
                op = random.choice(tp_op_types)

            if op == ADD_TOPIC:
                operations.append((
                    ADD_TOPIC,
                    f"test-topic-{random.randint(0,2000)}-{time.time()*1000.0}",
                    random.choice(ALLOWED_REPLICATION), 3))
            else:
                operations.append((DELETE_TOPIC, random.choice(topics)))

        return operations

    def _create_random_topics(self, count):
        max_partitions = 10

        topics = []
        for i in range(0, count):
            name = f"topic-{i}"
            spec = TopicSpec(
                name=name,
                partition_count=random.randint(1, max_partitions),
                replication_factor=random.choice(ALLOWED_REPLICATION))

            topics.append(spec)

        for spec in topics:
            self.redpanda.create_topic(spec)

        return topics

    """
    Adding nodes to the cluster should result in partition reallocations to new 
    nodes
    """

    @cluster(num_nodes=7)
    def test_node_opeartions(self):
        # allocate 5 nodes for the cluster
        self.redpanda = RedpandaService(
            self.test_context,
            5,
            KafkaCliTools,
            extra_rp_conf={
                "enable_auto_rebalance_on_node_add": True,
                "group_topic_partitions": 3,
                "default_topic_replications": 3,
            })
        # start 3 nodes
        self.redpanda.start()
        # create some topics
        topics = self._create_random_topics(10)
        self.redpanda.logger.info(f"using topics: {topics}")
        # select one of the topics to use in consumer/producer
        self.topic = random.choice(topics).name

        self.start_producer(1, throughput=100)
        self.start_consumer(1)
        self.await_startup()

        def decommission(node_id):
            self.logger.info(f"decommissioning node: {node_id}")
            admin = Admin(self.redpanda)
            r = admin.decommission_broker(id=node_id)
            r.raise_for_status()

            def node_removed():
                admin = Admin(self.redpanda)
                brokers = admin.get_brokers()
                for b in brokers:
                    if b['node_id'] == node_id:
                        return False
                return True

            wait_until(node_removed, timeout_sec=240, backoff_sec=2)

        kafkacat = KafkaCat(self.redpanda)

        def replicas_per_node():
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

        def restart_node(node_id, cleanup=True):
            self.logger.info(f"restarting node: {node_id}")
            self.redpanda.stop_node(self.redpanda.nodes[node_id - 1])
            if cleanup:
                self.redpanda.clean_node(self.redpanda.nodes[node_id - 1])
            self.redpanda.start_node(self.redpanda.nodes[node_id - 1])
            admin = Admin(self.redpanda)
            admin.set_log_level("cluster", "trace")

            def has_new_replicas():
                per_node = replicas_per_node()
                self.logger.info(f"replicas per node: {per_node}")
                return node_id in per_node

            wait_until(has_new_replicas, timeout_sec=240, backoff_sec=2)

        admin = Admin(self.redpanda)
        admin.set_log_level("cluster", "trace")
        work = self.generate_random_workload(10, skip_nodes=set())
        self.redpanda.logger.info(f"node operations to execute: {work}")
        for op in work:
            op_type = op[0]
            self.logger.info(f"executing - {op}")
            if op_type == ADD:
                id = op[1]
                restart_node(id)
            if op_type == DECOMMISSION:
                id = op[1]
                decommission(id)
            elif op_type == ADD_TOPIC:
                spec = TopicSpec(name=op[1],
                                 replication_factor=op[2],
                                 partition_count=op[3])

                self.redpanda.create_topic(spec)
            elif op_type == DELETE_TOPIC:
                self.redpanda.delete_topic(op[1])

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=180)
