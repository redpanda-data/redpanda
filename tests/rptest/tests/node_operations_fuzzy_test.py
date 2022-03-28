# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading
import time
import requests

from ducktape.mark import parametrize
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kcl import KCL
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST
from rptest.tests.end_to_end import EndToEndTest

DECOMMISSION = "decommission"
ADD = "add"
ADD_TOPIC = "add_tp"
DELETE_TOPIC = "delete_tp"
ALLOWED_REPLICATION = [1, 3]


class NodeOperationFuzzyTest(EndToEndTest):
    max_suspend_duration_seconds = 10
    min_inter_failure_time = 30
    max_inter_failure_time = 60

    def generate_random_workload(self, count, skip_nodes, available_nodes):
        op_types = [ADD, DECOMMISSION]
        tp_op_types = [ADD_TOPIC, DELETE_TOPIC]
        # current state
        active_nodes = list(available_nodes)
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
                    f"test-topic-{random.randint(0,2000)}-{round(time.time()*1000000)}",
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
            DefaultClient(self.redpanda).create_topic(spec)

        return topics

    """
    Adding nodes to the cluster should result in partition reallocations to new
    nodes
    """

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @parametrize(enable_failures=False)
    def test_node_operations(self, enable_failures):
        # allocate 5 nodes for the cluster
        self.redpanda = RedpandaService(
            self.test_context,
            5,
            extra_rp_conf={
                "enable_auto_rebalance_on_node_add": True,
                "group_topic_partitions": 3,
                "default_topic_replications": 3,
            })

        self.redpanda.start()
        # create some topics
        topics = self._create_random_topics(10)
        self.redpanda.logger.info(f"using topics: {topics}")
        # select one of the topics to use in consumer/producer
        self.topic = random.choice(topics).name

        self.start_producer(1, throughput=100)
        self.start_consumer(1)
        self.await_startup()
        self.active_nodes = set(
            [self.redpanda.idx(n) for n in self.redpanda.nodes])
        # collect current mapping
        self.ids_mapping = {}
        for n in self.redpanda.nodes:
            self.ids_mapping[self.redpanda.idx(n)] = self.redpanda.idx(n)
        self.next_id = sorted(list(self.ids_mapping.keys()))[-1] + 1
        self.redpanda.logger.info(f"Initial ids mapping: {self.ids_mapping}")
        NODE_OP_TIMEOUT = 360

        def get_next_id():
            id = self.next_id
            self.next_id += 1
            return id

        def failure_injector_loop():
            f_injector = FailureInjector(self.redpanda)
            while enable_failures:
                f_type = random.choice(FailureSpec.FAILURE_TYPES)
                length = 0
                # allow suspending any node
                if f_type == FailureSpec.FAILURE_SUSPEND:
                    length = random.randint(
                        1, NodeOperationFuzzyTest.max_suspend_duration_seconds)
                    node = random.choice(self.redpanda.nodes)
                else:
                    #kill/termianate only active nodes (not to influence the test outcome)
                    idx = random.choice(list(self.active_nodes))
                    node = self.redpanda.get_node(idx)

                f_injector.inject_failure(
                    FailureSpec(node=node, type=f_type, length=length))

                delay = random.randint(
                    NodeOperationFuzzyTest.min_inter_failure_time,
                    NodeOperationFuzzyTest.max_inter_failure_time)
                self.redpanda.logger.info(
                    f"waiting {delay} seconds before next failure")
                time.sleep(delay)

        if enable_failures:
            finjector_thread = threading.Thread(target=failure_injector_loop,
                                                args=())
            finjector_thread.daemon = True
            finjector_thread.start()

        def decommission(idx):
            node_id = self.ids_mapping[idx]
            self.logger.info(f"decommissioning node: {idx} with id: {node_id}")

            def decommissioned():
                try:
                    admin = Admin(self.redpanda)
                    # if broker is already draining, it is suceess

                    brokers = admin.get_brokers()
                    for b in brokers:
                        if b['node_id'] == node_id and b[
                                'membership_status'] == 'draining':
                            return True

                    r = admin.decommission_broker(id=node_id)
                    return r.status_code == 200
                except requests.exceptions.RetryError:
                    return False
                except requests.exceptions.ConnectionError:
                    return False
                except requests.exceptions.HTTPError:
                    return False

            wait_until(decommissioned,
                       timeout_sec=NODE_OP_TIMEOUT,
                       backoff_sec=2)
            admin = Admin(self.redpanda)

            def is_node_removed(idx_to_query, node_id):
                try:
                    brokers = admin.get_brokers(
                        self.redpanda.get_node(idx_to_query))
                    ids = map(lambda broker: broker['node_id'], brokers)
                    return not node_id in ids
                except:
                    return False

            def node_removed():
                node_removed_cnt = 0
                for idx in self.active_nodes:
                    if is_node_removed(idx, node_id):
                        node_removed_cnt += 1

                node_count = len(self.redpanda.nodes)
                majority = int(node_count / 2) + 1
                self.redpanda.logger.debug(
                    f"node {node_id} removed on {node_removed_cnt} nodes, majority: {majority}"
                )
                return node_removed_cnt >= majority

            wait_until(node_removed,
                       timeout_sec=NODE_OP_TIMEOUT,
                       backoff_sec=2)
            self.redpanda.stop_node(self.redpanda.get_node(idx))

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

        def seed_servers_for(idx):
            seeds = map(
                lambda n: {
                    "address": n.account.hostname,
                    "port": 33145
                }, self.redpanda.nodes)

            return list(
                filter(
                    lambda n: n['address'] != self.redpanda.get_node(idx).
                    account.hostname, seeds))

        def add_node(idx, cleanup=True):
            id = get_next_id()
            self.logger.info(f"adding node: {idx} back with new id: {id}")
            self.ids_mapping[idx] = id
            self.redpanda.stop_node(self.redpanda.get_node(idx))
            if cleanup:
                self.redpanda.clean_node(self.redpanda.get_node(idx),
                                         preserve_logs=True)
            # we do not reuse previous node ids and override seed server list
            self.redpanda.start_node(
                self.redpanda.get_node(idx),
                timeout=NodeOperationFuzzyTest.min_inter_failure_time +
                NodeOperationFuzzyTest.max_suspend_duration_seconds + 30,
                override_cfg_params={
                    "node_id": id,
                    "seed_servers": seed_servers_for(idx)
                })

            def has_new_replicas():
                per_node = replicas_per_node()
                self.logger.info(f"replicas per node: {per_node}")
                return id in per_node

            wait_until(has_new_replicas,
                       timeout_sec=NODE_OP_TIMEOUT,
                       backoff_sec=2)

        def is_topic_present(name):
            kcl = KCL(self.redpanda)
            lines = kcl.list_topics().splitlines()
            self.redpanda.logger.debug(
                f"checking if topic {name} is present in {lines}")
            for l in lines:
                if l.startswith(name):
                    return True
            return False

        def create_topic(spec):
            try:
                DefaultClient(self.redpanda).create_topic(spec)
            except Exception as e:
                self.redpanda.logger.warn(
                    f"error creating topic {spec.name} - {e}")
            try:
                return is_topic_present(spec.name)
            except Exception as e:
                self.redpanda.logger.warn(f"error while listing topics - {e}")
                return False

        def delete_topic(name):
            try:
                DefaultClient(self.redpanda).delete_topic(name)
            except Exception as e:
                self.redpanda.logger.warn(f"error deleting topic {name} - {e}")
            try:
                return not is_topic_present(name)
            except Exception as e:
                self.redpanda.logger.warn(f"error while listing topics - {e}")
                return False

        work = self.generate_random_workload(10,
                                             skip_nodes=set(),
                                             available_nodes=self.active_nodes)
        self.redpanda.logger.info(f"node operations to execute: {work}")
        for op in work:
            op_type = op[0]
            self.logger.info(
                f"executing - {op} - current ids: {self.ids_mapping}")
            if op_type == ADD:
                idx = op[1]
                self.active_nodes.add(idx)
                add_node(idx)
            if op_type == DECOMMISSION:
                idx = op[1]
                self.active_nodes.remove(idx)
                decommission(idx)
            elif op_type == ADD_TOPIC:
                spec = TopicSpec(name=op[1],
                                 replication_factor=op[2],
                                 partition_count=op[3])
                wait_until(lambda: create_topic(spec) == True,
                           timeout_sec=180,
                           backoff_sec=2)
            elif op_type == DELETE_TOPIC:
                wait_until(lambda: delete_topic(op[1]) == True,
                           timeout_sec=180,
                           backoff_sec=2)

        enable_failures = False
        self.run_validation(enable_idempotence=False,
                            producer_timeout_sec=60,
                            consumer_timeout_sec=180)
