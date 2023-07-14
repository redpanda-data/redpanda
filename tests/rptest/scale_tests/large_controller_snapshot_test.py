# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures

from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, LoggingConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode


class LargeControllerSnapshotTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            num_brokers=4,
            # This configuration allows dangerously high partition counts. That's okay
            # because we want to stress the controller itself, so we won't apply
            # produce load.
            extra_rp_conf={
                'controller_snapshot_max_age_sec': 3,
                'topic_partitions_per_shard': 10_000,
                'topic_memory_per_partition': None,
                'partition_autobalancing_concurrent_moves': 1000,
            },
            # Reduce per-partition log spam
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'storage': 'warn',
                                         'storage-gc': 'warn',
                                         'raft': 'warn',
                                         'offset_translator': 'warn'
                                     }),
            **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    @skip_debug_mode
    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_join_restart(self):
        """
        Test that Redpanda can handle controller snapshots with a lot of objects:
        * no new oversized allocations and controller stalls
        * snapshot is persisted without problems
        * admin commands can still be executed
        * cluster is able to stabilize (rebalancing on node addition finishes)
        """

        seed_nodes = self.redpanda.nodes[0:3]
        joiner_node = self.redpanda.nodes[3]

        self.redpanda.set_seed_servers(seed_nodes)
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)

        admin = Admin(self.redpanda, default_node=seed_nodes[0])
        rpk = RpkTool(self.redpanda)

        if self.redpanda.dedicated_nodes:
            # approx. 5k partition replicas per shard
            n_topics = 50
            n_partitions = min(100 * self.redpanda.get_node_cpu_count(), 1000)
            n_users = 5_000
        else:
            # more benign numbers for running the test locally
            n_topics = 10
            n_partitions = 100
            n_users = 500

        # create a lot of topics
        self.logger.info(
            f"creating {n_topics} topics with {n_partitions} partitions each..."
        )

        topic_names = [f"test_{i:06d}" for i in range(0, n_topics)]
        for tn in topic_names:
            self.logger.debug(f"creating topic {tn}")
            rpk.create_topic(tn, partitions=n_partitions, replicas=3)

        for n in seed_nodes:
            self.redpanda.wait_for_controller_snapshot(node=n)

        # create a lot of users
        self.logger.info(f"creating {n_users} users...")

        def create_users(names):
            # create own admin and rpk instances to avoid concurrent accesses
            # to common ones
            admin = Admin(self.redpanda, default_node=seed_nodes[0])
            rpk = RpkTool(self.redpanda)
            for un in names:
                admin.create_user(username=un,
                                  password='p4ssw0rd',
                                  algorithm='SCRAM-SHA-256')
                rpk.acl_create_allow_cluster(username=un, op='describe')

        with concurrent.futures.ThreadPoolExecutor() as executor:
            step = 10
            user_names = [[
                f"user_{i:06d}" for i in range(start, start + step)
            ] for start in range(0, n_users, step)]
            executor.map(create_users, user_names)

        # wait until everything is snapshotted
        self.logger.info(f"waiting until all commands are snapshotted...")

        controller_max_offset = max(
            admin.get_controller_status(n)['committed_index']
            for n in seed_nodes)
        self.logger.info(f"controller max offset is {controller_max_offset}")

        for n in seed_nodes:
            self.redpanda.wait_for_controller_snapshot(
                node=n, prev_start_offset=(controller_max_offset - 1))

        # add a node to the cluster
        self.logger.info(f"adding a node to the cluster...")

        # explicit clean step is needed because we are starting the node manually and not
        # using redpanda.start()
        self.redpanda.clean_node(joiner_node)
        self.redpanda.start_node(joiner_node)
        wait_until(lambda: self.redpanda.registered(joiner_node),
                   timeout_sec=60,
                   backoff_sec=5)

        # reboot the cluster to test that the cluster stabilizes after rebooting with high
        # partition count and a large controller snapshot
        self.logger.info(f"rebooting the cluster...")

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.redpanda.nodes)) as executor:
            futs = []
            for node in self.redpanda.nodes:
                futs.append(
                    executor.submit(self.redpanda.restart_nodes,
                                    nodes=[node],
                                    start_timeout=60,
                                    stop_timeout=60 * 5))

            for f in futs:
                # Raise on error
                f.result()

        self.redpanda.wait_for_membership(first_start=False, timeout_sec=60)

        # check that all created users are there
        for n in self.redpanda.nodes:
            actual = len(admin.list_users(node=n))
            # + 1 comes from the admin user which is created autmatically
            assert actual == n_users + 1, f"unexpected number of users {actual}"

        # wait until rebalance on node addition is finished
        self.logger.info(
            f"waiting until partitions are rebalanced to the new node...")

        def rebalance_finished():
            in_progress = admin.list_reconfigurations(node=seed_nodes[0])
            self.logger.info(
                f"current number of reconfigurations: {len(in_progress)}")
            if len(in_progress) > 0:
                return False

            partitions_on_joiner = len(admin.get_partitions(node=joiner_node))
            self.logger.info(f"partitions on joiner: {partitions_on_joiner}")
            return partitions_on_joiner > (n_topics * n_partitions / 10)

        wait_until(rebalance_finished, timeout_sec=300, backoff_sec=5)
