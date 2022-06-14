# Copyright 2022 Redpanda Data, Inc.
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
from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST, RedpandaService, ResourceSettings
from rptest.clients.default import DefaultClient
from ducktape.utils.util import wait_until

from ducktape.mark import matrix


class ConsumerOffsetsMigrationTest(EndToEndTest):
    max_suspend_duration_sec = 3
    min_inter_failure_time_sec = 30
    max_inter_failure_time_sec = 60

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @matrix(failures=[True, False], cpus=[1, 3])
    def test_migrating_consume_offsets(self, failures, cpus):
        '''
        Validates correctness while executing consumer offsets migration
        '''

        # set redpanda logical version to value without __consumer_offsets support
        self.redpanda = RedpandaService(
            self.test_context,
            5,
            resource_settings=ResourceSettings(num_cpus=cpus),
            extra_rp_conf={
                "group_topic_partitions": 16,
                "default_topic_replications": 3,
            },
            environment={"__REDPANDA_LOGICAL_VERSION": 1})

        self.redpanda.start()
        self._client = DefaultClient(self.redpanda)

        # Set of nodes that are undergoing some destructive operations like fault injections
        # or restarts. We only want to do one of those at a time to avoid conflicts.
        busy_nodes = set()
        # synchronize access to busy_nodes set.
        busy_nodes_lock = threading.RLock()

        # Atomic "mark a node busy if it is not" function.
        def mark_node_busy(node_idx):
            with busy_nodes_lock:
                if node_idx in busy_nodes:
                    return False
                busy_nodes.add(node_idx)
                self.logger.debug(f"Marked {node_idx} busy")
                return True

        def mark_node_free(node_idx):
            with busy_nodes_lock:
                assert node_idx in busy_nodes
                busy_nodes.remove(node_idx)
                self.logger.debug(f"Marked {node_idx} free")

        def failure_injector_loop():
            f_injector = FailureInjector(self.redpanda)
            while failures:
                f_type = random.choice(FailureSpec.FAILURE_TYPES)
                length = 0
                node = random.choice(self.redpanda.nodes)
                while not mark_node_busy(self.redpanda.idx(node)):
                    node = random.choice(self.redpanda.nodes)

                try:
                    # allow suspending any node
                    if f_type == FailureSpec.FAILURE_SUSPEND:
                        length = random.randint(
                            1, ConsumerOffsetsMigrationTest.
                            max_suspend_duration_sec)

                    f_injector.inject_failure(
                        FailureSpec(node=node, type=f_type, length=length))
                finally:
                    mark_node_free(self.redpanda.idx(node))

                delay = random.randint(
                    ConsumerOffsetsMigrationTest.min_inter_failure_time_sec,
                    ConsumerOffsetsMigrationTest.max_inter_failure_time_sec)
                self.redpanda.logger.info(
                    f"waiting {delay} seconds before next failure")
                time.sleep(delay)

        if failures:
            finjector_thread = threading.Thread(target=failure_injector_loop,
                                                args=())
            finjector_thread.daemon = True
            finjector_thread.start()
        spec = TopicSpec(partition_count=6, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=5000)
        self.start_consumer(1)
        self.await_startup()

        def cluster_is_stable():
            admin = Admin(self.redpanda)
            brokers = admin.get_brokers()
            if len(brokers) < 3:
                return False

            for b in brokers:
                self.logger.debug(f"broker:  {b}")
                if not (b['is_alive'] and 'disk_space' in b):
                    return False

            return True

        kcl = KCL(self.redpanda)

        def _group_present():
            return len(kcl.list_groups().splitlines()) > 1

        # make sure that group is there
        wait_until(_group_present, 10, 1)

        # check that consumer offsets topic is not present
        topics = set(kcl.list_topics())

        assert "__consumer_offsets" not in topics

        # enable consumer offsets support
        self.redpanda.set_environment({"__REDPANDA_LOGICAL_VERSION": 2})
        for n in self.redpanda.nodes:
            idx = self.redpanda.idx(n)
            wait_until(lambda: mark_node_busy(idx), 90, backoff_sec=2)
            try:
                self.redpanda.restart_nodes(n, stop_timeout=60)
            finally:
                mark_node_free(idx)
            # wait for leader balancer to start evening out leadership
            wait_until(cluster_is_stable, 90, backoff_sec=2)

        def _consumer_offsets_present():
            try:
                partitions = list(
                    self.client().describe_topic("__consumer_offsets"))
                return len(partitions) > 0
            except:
                return False

        wait_until(_consumer_offsets_present, timeout_sec=90, backoff_sec=3)

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=180)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_cluster_is_available_during_upgrade_without_group_topic(self):
        '''
        Validates that cluster is available and healthy during 
        upgrade when `kafka_internal::group` topic is not present
        '''

        # set redpanda logical version to value without __consumer_offsets support
        self.redpanda = RedpandaService(
            self.test_context,
            5,
            extra_rp_conf={
                "group_topic_partitions": 16,
                "default_topic_replications": 3,
            },
            environment={"__REDPANDA_LOGICAL_VERSION": 1})

        self.redpanda.start()
        self._client = DefaultClient(self.redpanda)

        spec = TopicSpec(partition_count=6, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        def cluster_is_stable():
            admin = Admin(self.redpanda)
            brokers = admin.get_brokers()
            if len(brokers) < 3:
                return False

            for b in brokers:
                self.logger.debug(f"broker:  {b}")
                if not (b['is_alive'] and 'disk_space' in b):
                    return False

            return True

        def node_stopped(node_id):
            admin = Admin(self.redpanda)
            brokers = admin.get_brokers()

            for b in brokers:
                self.logger.debug(f"broker:  {b}")
                if b['node_id'] == node_id:
                    return b['is_alive'] == False

            return False

        kcl = KCL(self.redpanda)

        # check that consumer offsets topic is not present
        topics = set(kcl.list_topics())

        assert "__consumer_offsets" not in topics

        # enable consumer offsets support
        self.redpanda.set_environment({"__REDPANDA_LOGICAL_VERSION": 2})

        def get_raft0_follower():
            ctrl = self.redpanda.controller
            node = random.choice(self.redpanda.nodes)
            while self.redpanda.idx(node) == self.redpanda.idx(ctrl):
                node = random.choice(self.redpanda.nodes)

            return node

        # restart node that is not controller
        n = get_raft0_follower()
        self.logger.info(f"restarting node {n.account.hostname}")
        self.redpanda.stop_node(n, timeout=60)
        # wait for leader balancer to start evening out leadership
        wait_until(lambda: node_stopped(self.redpanda.idx(n)),
                   90,
                   backoff_sec=2)
        self.redpanda.start_node(n)
        wait_until(cluster_is_stable, 90, backoff_sec=2)
