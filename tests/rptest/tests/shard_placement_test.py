# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.services.redpanda import ResourceSettings
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.util import wait_until_result


class ShardPlacementTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=5, **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    def get_replica_shard_map(self, nodes, admin=None):
        """Return map of topic -> partition -> [(node_id, core)]"""

        if admin is None:
            admin = Admin(self.redpanda)

        topic2partition2shard = dict()
        for node in nodes:
            partitions = admin.get_partitions(node=node)
            for p in partitions:
                topic2partition2shard.setdefault(
                    p["topic"],
                    dict()).setdefault(p["partition_id"], list()).append(
                        (self.redpanda.node_id(node), p["core"]))

        for partitions in topic2partition2shard.values():
            for replicas in partitions.values():
                # sort replicas for the ease of comparison
                replicas.sort()

        for topic, partitions in sorted(topic2partition2shard.items()):
            for p, replicas in sorted(partitions.items()):
                self.logger.debug(f"ntp: {topic}/{p} replicas: {replicas}")

        return topic2partition2shard

    def get_shard_counts_by_topic(self, shard_map, node_id):
        core_count = self.redpanda.get_node_cpu_count()
        topic2shard2count = dict()
        for topic, partitions in shard_map.items():
            for replicas in partitions.values():
                for replica_id, core in replicas:
                    if replica_id == node_id:
                        topic2shard2count.setdefault(
                            topic,
                            list(0 for _ in range(core_count)))[core] += 1
        return topic2shard2count

    def print_shard_stats(self, shard_map):
        node_ids = set()
        for partitions in shard_map.values():
            for replicas in partitions.values():
                for n_id, _ in replicas:
                    node_ids.add(n_id)

        core_count = self.redpanda.get_node_cpu_count()
        for node_id in sorted(node_ids):
            shard_counts = self.get_shard_counts_by_topic(shard_map, node_id)
            total_counts = list(0 for _ in range(core_count))
            self.logger.info(f"shard replica counts on node {node_id}:")
            for t, counts in sorted(shard_counts.items()):
                self.logger.info(f"topic {t}: {counts}")
                for i, c in enumerate(counts):
                    total_counts[i] += c
            self.logger.info(f"total: {total_counts}")

    def wait_shard_map_stationary(self,
                                  nodes,
                                  admin=None,
                                  timeout_sec=10,
                                  backoff_sec=1):
        shard_map = None

        def is_stationary():
            nonlocal shard_map
            new_map = self.get_replica_shard_map(nodes, admin)
            if new_map == shard_map:
                return True
            else:
                shard_map = new_map

        wait_until(is_stationary,
                   timeout_sec=timeout_sec,
                   backoff_sec=backoff_sec)
        return shard_map

    @cluster(num_nodes=5)
    def test_upgrade(self):
        # Disable partition balancer in this test, as we need partitions
        # to remain stationary.
        self.redpanda.add_extra_rp_conf(
            {'partition_autobalancing_mode': 'off'})

        seed_nodes = self.redpanda.nodes[0:3]
        joiner_nodes = self.redpanda.nodes[3:]

        # Create a cluster that doesn't support node-local core assignment yet
        # and add some topics.

        installer = self.redpanda._installer
        installer.install(seed_nodes, (24, 1))
        self.redpanda.start(nodes=seed_nodes)

        admin = Admin(self.redpanda, default_node=seed_nodes[0])
        rpk = RpkTool(self.redpanda)

        n_partitions = 10

        for topic in ["foo", "bar"]:
            rpk.create_topic(topic, partitions=n_partitions, replicas=3)

        self.logger.info("created cluster and topics.")
        initial_map = self.get_replica_shard_map(seed_nodes, admin)
        self.print_shard_stats(initial_map)

        # Upgrade the cluster and enable the feature.

        installer.install(seed_nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(seed_nodes)
        self.redpanda.wait_for_membership(first_start=False)

        self.redpanda.await_feature("node_local_core_assignment",
                                    "available",
                                    timeout_sec=15,
                                    nodes=seed_nodes)
        admin.put_feature("node_local_core_assignment", {"state": "active"})
        self.redpanda.await_feature("node_local_core_assignment",
                                    "active",
                                    timeout_sec=15,
                                    nodes=seed_nodes)

        self.logger.info(
            "feature enabled, checking that shard map is stable...")
        map_after_upgrade = self.get_replica_shard_map(seed_nodes, admin)
        self.print_shard_stats(map_after_upgrade)
        assert map_after_upgrade == initial_map

        # Manually move replicas of one topic on one node to shard 0

        moved_replica_id = self.redpanda.node_id(seed_nodes[-1])
        for p in range(n_partitions):
            admin.set_partition_replica_core(topic="foo",
                                             partition=p,
                                             replica=moved_replica_id,
                                             core=0,
                                             node=seed_nodes[p % 3])

        # check that they indeed moved
        self.logger.info(
            f"manually moved some replicas on node {moved_replica_id}, "
            "checking shard map...")
        map_after_manual_move = self.wait_shard_map_stationary(
            seed_nodes, admin)
        self.print_shard_stats(map_after_manual_move)
        foo_shard_counts = self.get_shard_counts_by_topic(
            map_after_manual_move, moved_replica_id)["foo"]
        assert foo_shard_counts[0] == n_partitions
        assert sum(foo_shard_counts) == n_partitions

        # Add more nodes to the cluster and create another topic

        self.redpanda.start(nodes=joiner_nodes)
        self.redpanda.wait_for_membership(first_start=True)

        rpk.create_topic("quux", partitions=n_partitions, replicas=3)

        # check that shard counts are balanced
        self.logger.info(f"added 2 nodes and a topic, checking shard map...")
        map_after_join = self.get_replica_shard_map(joiner_nodes, admin)
        self.print_shard_stats(map_after_join)
        for joiner in joiner_nodes:
            joiner_id = self.redpanda.node_id(joiner)
            shard_counts = self.get_shard_counts_by_topic(
                map_after_join, joiner_id)["quux"]
            assert max(shard_counts) - min(shard_counts) <= 1

        # Check that joiner nodes support manual partition moves as well

        joiner_id = self.redpanda.node_id(joiner_nodes[0])
        quux_partitions_on_joiner = [
            p for p, rs in map_after_join["quux"].items()
            if any(n == joiner_id for n, _ in rs)
        ]
        for p in quux_partitions_on_joiner:
            admin.set_partition_replica_core(topic="quux",
                                             partition=p,
                                             replica=joiner_id,
                                             core=0,
                                             node=self.redpanda.nodes[p % 5])

        # check that they indeed moved
        self.logger.info(f"manually moved some replicas on node {joiner_id}, "
                         "checking shard map...")
        map_after_manual_move2 = self.wait_shard_map_stationary(
            joiner_nodes, admin)
        self.print_shard_stats(map_after_manual_move2)
        quux_shard_counts = self.get_shard_counts_by_topic(
            map_after_manual_move2, joiner_id)["quux"]
        assert quux_shard_counts[0] > 0
        assert sum(quux_shard_counts) == quux_shard_counts[0]

        # Restart and check that the shard map remains stable
        # (if we turn off rebalancing on startup)

        self.redpanda.set_cluster_config(
            {"core_balancing_on_core_count_change": False})

        map_before_restart = self.get_replica_shard_map(
            self.redpanda.nodes, admin)
        self.print_shard_stats(map_before_restart)

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda.wait_for_membership(first_start=False)

        self.logger.info("restarted cluster, checking shard map...")
        map_after_restart = self.get_replica_shard_map(self.redpanda.nodes,
                                                       admin)
        self.print_shard_stats(map_after_restart)
        assert map_after_restart == map_before_restart

    @cluster(num_nodes=5)
    def test_manual_rebalance(self):
        self.redpanda.start()

        admin = Admin(self.redpanda)
        rpk = RpkTool(self.redpanda)

        n_partitions = 10

        for topic in ["foo", "bar"]:
            rpk.create_topic(topic, partitions=n_partitions, replicas=5)

        # Manually move some partitions to create artificial imbalance

        node = self.redpanda.nodes[0]
        moved_replica_id = self.redpanda.node_id(node)

        core_count = self.redpanda.get_node_cpu_count()
        for p in range(n_partitions):
            admin.set_partition_replica_core(topic="foo",
                                             partition=p,
                                             replica=moved_replica_id,
                                             core=0)
            admin.set_partition_replica_core(topic="bar",
                                             partition=p,
                                             replica=moved_replica_id,
                                             core=core_count - 1)

        self.logger.info(
            f"manually moved some replicas on node {moved_replica_id}, "
            "checking shard map...")
        shard_map = self.wait_shard_map_stationary(self.redpanda.nodes, admin)
        self.print_shard_stats(shard_map)
        counts_by_topic = self.get_shard_counts_by_topic(
            shard_map, moved_replica_id)
        assert counts_by_topic["foo"][0] == n_partitions
        assert sum(counts_by_topic["foo"]) == n_partitions
        assert counts_by_topic["bar"][core_count - 1] == n_partitions
        assert sum(counts_by_topic["bar"]) == n_partitions

        admin.trigger_cores_rebalance(node)
        self.logger.info(
            f"trigger manual shard rebalance on node {node.name} (id: {moved_replica_id})"
            ", checking shard map...")
        shard_map = self.wait_shard_map_stationary(self.redpanda.nodes, admin)
        self.print_shard_stats(shard_map)
        counts_by_topic = self.get_shard_counts_by_topic(
            shard_map, moved_replica_id)
        for topic, shard_counts in counts_by_topic.items():
            assert max(shard_counts) - min(shard_counts) <= 1

    @cluster(num_nodes=5)
    def test_core_count_change(self):
        self.redpanda.set_resource_settings(ResourceSettings(num_cpus=1))
        self.redpanda.start()

        admin = Admin(self.redpanda)
        rpk = RpkTool(self.redpanda)

        n_partitions = 10

        for topic in ["foo", "bar"]:
            # create topics with rf=5 for ease of accounting
            rpk.create_topic(topic, partitions=n_partitions, replicas=5)

        # increase cpu count on one node, restart it and
        # check that new shards are in use.
        self.logger.info("increasing cpu count and restarting...")

        node = self.redpanda.nodes[0]
        node_id = self.redpanda.node_id(node)
        self.redpanda.stop_node(node)
        self.redpanda.set_resource_settings(ResourceSettings(num_cpus=2))
        self.redpanda.start_node(node)
        self.redpanda.wait_for_membership(first_start=False)

        # check that the node moved partitions to the new core
        shard_map = self.get_replica_shard_map([node], admin)
        self.print_shard_stats(shard_map)
        counts_by_topic = self.get_shard_counts_by_topic(shard_map, node_id)
        assert len(counts_by_topic) > 0
        for topic, shard_counts in counts_by_topic.items():
            assert max(shard_counts) - min(shard_counts) <= 1

        # do some manual moves and check that their effects remain
        # if the core count doesn't change.
        self.logger.info("doing some manual moves...")

        foo_partitions_on_node = [
            p for p, rs in shard_map["foo"].items()
            if any(n == node_id for n, _ in rs)
        ]
        for p in foo_partitions_on_node:
            admin.set_partition_replica_core(topic="foo",
                                             partition=p,
                                             replica=node_id,
                                             core=0)
        shard_map = self.wait_shard_map_stationary([node], admin)
        self.print_shard_stats(shard_map)

        self.logger.info(
            "restarting and checking manual assignments are still there...")

        self.redpanda.restart_nodes([node])
        self.redpanda.wait_for_membership(first_start=False)

        map_after_restart = self.get_replica_shard_map([node], admin)
        self.print_shard_stats(map_after_restart)
        assert map_after_restart == shard_map

        # TODO: core count decrease (not supported yet)

    @cluster(num_nodes=5)
    def test_node_join(self):
        self.redpanda.add_extra_rp_conf({
            "core_balancing_continuous": True,
        })
        seed_nodes = self.redpanda.nodes[0:3]
        joiner_nodes = self.redpanda.nodes[3:]
        self.redpanda.start(nodes=seed_nodes)

        admin = Admin(self.redpanda, default_node=seed_nodes[0])
        rpk = RpkTool(self.redpanda)

        n_partitions = 10

        topics = ["foo", "bar", "quux"]
        for topic in topics:
            rpk.create_topic(topic, partitions=n_partitions, replicas=3)

        self.logger.info(f"created topics: {topics}")
        initial_shard_map = self.wait_shard_map_stationary(seed_nodes, admin)
        self.print_shard_stats(initial_shard_map)

        self.redpanda.start(nodes=joiner_nodes)

        def node_rebalance_finished():
            in_progress = admin.list_reconfigurations(node=seed_nodes[0])
            if len(in_progress) > 0:
                return False

            for n in joiner_nodes:
                num_partitions = len(admin.get_partitions(node=n))
                if num_partitions < 5:
                    return False

            return True

        wait_until(node_rebalance_finished, timeout_sec=60, backoff_sec=2)
        self.logger.info("node rebalance finished")

        def shard_rebalance_finished():
            nodes = self.redpanda.nodes
            shard_map = self.get_replica_shard_map(nodes, admin)
            self.print_shard_stats(shard_map)
            for n in nodes:
                node_id = self.redpanda.node_id(n)
                shard_counts = self.get_shard_counts_by_topic(
                    shard_map, node_id)
                for topic in topics:
                    topic_counts = shard_counts[topic]
                    if max(topic_counts) - min(topic_counts) > 1:
                        return False

            return (True, shard_map)

        shard_map_after_balance = wait_until_result(shard_rebalance_finished,
                                                    timeout_sec=60,
                                                    backoff_sec=2)
        self.logger.info("shard rebalance finished")
        self.print_shard_stats(shard_map_after_balance)
