# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from collections import Counter
from math import ceil

import requests
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkException
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.maintenance import MaintenanceTestBase


class MaintenanceTest(MaintenanceTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Vary partition count relative to num_cpus. This is to ensure that
        # leadership is moved back to a node that exits maintenance.
        num_cpus = self.redpanda.get_node_cpu_count()
        self.topics = (
            TopicSpec(partition_count=num_cpus * 5, replication_factor=3),
            TopicSpec(partition_count=num_cpus * 10, replication_factor=3),
        )

    @cluster(num_nodes=3)
    @matrix(use_rpk=[True, False])
    def test_maintenance(self, use_rpk):
        self._use_rpk = use_rpk
        target = random.choice(self.redpanda.nodes)
        self._enable_maintenance(target)
        self._maintenance_disable(target)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(use_rpk=[True, False])
    def test_maintenance_sticky(self, use_rpk):
        self._use_rpk = use_rpk
        nodes = random.sample(self.redpanda.nodes, len(self.redpanda.nodes))
        for node in nodes:
            self._enable_maintenance(node)
            self._verify_cluster(node, True)

            self.redpanda.restart_nodes(node)
            self._verify_cluster(node, True)

            self._maintenance_disable(node)
            self._verify_cluster(node, False)

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._verify_cluster(None, False)

    @cluster(num_nodes=3)
    @matrix(use_rpk=[True, False])
    def test_exclusive_maintenance(self, use_rpk):
        self._use_rpk = use_rpk
        target, other = random.sample(self.redpanda.nodes, k=2)
        assert target is not other
        self._enable_maintenance(target)
        try:
            self._enable_maintenance(other)
        except RpkException as e:
            assert self._use_rpk
            if "invalid state transition" in e.msg and "400" in e.msg:
                return
        except requests.exceptions.HTTPError as e:
            assert not self._use_rpk
            if (
                "invalid state transition" in e.response.text
                and e.response.status_code == 400
            ):
                return
            raise
        except:
            raise
        else:
            raise Exception("Expected maintenance enable to fail")

    @cluster(num_nodes=3)
    @matrix(use_rpk=[True, False])
    def test_maintenance_with_single_replicas(self, use_rpk):
        self._use_rpk = use_rpk
        single_replica_topic = TopicSpec(partition_count=18, replication_factor=1)
        DefaultClient(self.redpanda).create_topic(single_replica_topic)

        target = random.choice(self.redpanda.nodes)

        self._enable_maintenance(target)
        self.redpanda.restart_nodes(target)

        def all_partitions_have_leaders():
            partitions = list(
                self.rpk.describe_topic(single_replica_topic.name, tolerant=True)
            )
            for p in partitions:
                self.logger.info(f"DBG: {p.high_watermark}")
            return len(partitions) == single_replica_topic.partition_count and all(
                [p.high_watermark is not None for p in partitions]
            )

        wait_until(
            all_partitions_have_leaders,
            30,
            backoff_sec=1,
            err_msg="Error waiting for all partitions to have leaders",
        )

    @cluster(num_nodes=3)
    @matrix(use_rpk=[True, False])
    def test_maintenance_mode_of_stopped_node(self, use_rpk):
        self._use_rpk = use_rpk

        target = random.choice(self.redpanda.nodes)
        target_id = self.redpanda.node_id(target)

        self._enable_maintenance(target)
        self.redpanda.stop_node(target)

        def _node_is_not_alive():
            all_brokers = []
            for n in self.redpanda.started_nodes():
                all_brokers += self.admin.get_brokers(n)

            return all(
                [
                    b["is_alive"] == False
                    for b in all_brokers
                    if b["node_id"] == target_id
                ]
            )

        wait_until(
            _node_is_not_alive,
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"Timeout waiting for node {target_id} status update. Node should be marked as stopped.",
        )

        def _check_maintenance_status_on_each_broker(status):
            all_brokers = []
            for n in self.redpanda.started_nodes():
                all_brokers += self.admin.get_brokers(n)

            return all(
                [
                    b["maintenance_status"]["draining"] == status
                    for b in all_brokers
                    if b["node_id"] == target_id
                ]
            )

        assert _check_maintenance_status_on_each_broker(True), (
            "All the nodes should keep reporting the state of node in maintenance mode"
        )

        if self._use_rpk:
            self.rpk.cluster_maintenance_disable(target)
        else:
            self.admin.maintenance_stop(target)

        wait_until(
            lambda: _check_maintenance_status_on_each_broker(False),
            timeout_sec=30,
            backoff_sec=5,
            err_msg=f"Timeout waiting for maintenance mode to be disabled on node {target_id}",
        )


class MaintenanceCycleTest(MaintenanceTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(num_brokers=7, *args, **kwargs)

    def _get_leaders_stats(self):
        json = self.admin.get_cluster_partitions()
        leader_stats = Counter(p["leader_id"] for p in json if "leader_id" in p)
        self.redpanda.logger.info(f"{leader_stats=}")
        return leader_stats

    def _leaders_distributed_evenly(self, tolerance_coefficient):
        leaders_stats = self._get_leaders_stats()
        average = sum(leaders_stats.values()) / len(self.redpanda.nodes)
        acceptable = ceil(average * tolerance_coefficient)
        return all(c <= acceptable for c in leaders_stats.values())

    @cluster(num_nodes=7)
    @matrix(use_rpk=[False])
    def test_leader_distribution(self, use_rpk):
        # Rolling restart of a cluster balanced by leaders will not bring more
        # than 3x avg leaders onto a node
        # (theoretical estimate is e if there are sufficiently many partitions)

        self._use_rpk = use_rpk

        topic = TopicSpec(partition_count=1000, replication_factor=3)
        self.client().create_topic(topic)

        wait_until(
            lambda: self._leaders_distributed_evenly(1.1),
            timeout_sec=90,
            backoff_sec=2,
            err_msg="Leaders distributed unevenly",
        )
        self.redpanda.set_cluster_config({"enable_leader_balancer": False})

        all_nodes_but_one = len(self.redpanda.nodes) - 1
        for n in random.sample(self.redpanda.nodes, k=all_nodes_but_one):
            id = self.redpanda.node_id(n)
            self._enable_maintenance(n)
            self.redpanda.logger.info(f"node {id} in maintenance mode")
            self._maintenance_disable(n, check_leadership=False)
            self.redpanda.logger.info(f"node {id} out of maintenance mode")

        self.redpanda.logger.info(f"{self._get_leaders_stats()=}")
        wait_until(
            lambda: self._leaders_distributed_evenly(3),
            timeout_sec=10,
            backoff_sec=2,
            err_msg="Leaders distributed very unevenly",
        )
