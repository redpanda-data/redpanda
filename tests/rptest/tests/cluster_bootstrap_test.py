# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import ducktape.errors
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from requests.exceptions import ConnectionError
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import (RedpandaInstaller,
                                                wait_for_num_versions)
from rptest.tests.redpanda_test import RedpandaTest


def set_seeds_for_cluster(redpanda, num_seeds):
    seeds = [redpanda.nodes[i] for i in range(num_seeds)]
    redpanda.set_seed_servers(seeds)


class ClusterBootstrapNew(RedpandaTest):
    """
    Tests verifying new cluster bootstrap in Seed Driven Cluster Bootstrap mode
    """
    def __init__(self, test_context):
        super(ClusterBootstrapNew, self).__init__(test_context=test_context,
                                                  num_brokers=3)
        self.admin = self.redpanda._admin

    def setUp(self):
        # Defer startup to test body.
        pass

    @cluster(num_nodes=3, log_allow_list=["seed_servers cannot be empty"])
    def test_misconfigured_root_driven_bootstrap(self):
        """
        Test that empty_seed_starts_cluster=False prevents root-driven
        bootstrap from occurring.
        """
        for node in self.redpanda.nodes:
            self.redpanda.set_extra_node_conf(
                node, {"empty_seed_starts_cluster": False})
        try:
            self.redpanda.start(omit_seeds_on_idx_one=True)
            assert False, "Should have been unable to start"
        except ducktape.errors.TimeoutError:
            # The cluster should be unable to start.
            pass

        for node in self.redpanda.nodes:
            idx = self.redpanda.idx(node)
            try:
                cluster_uuid = self.redpanda._admin.get_cluster_uuid(node)
                assert idx != 1, "Expected failure on idx 1"
                assert cluster_uuid == None, f"Unexpected cluster UUID: {cluster_uuid}"
            except ConnectionError:
                assert 1 == idx, f"Should have failed on idx 1, failed on {idx}"

    @cluster(num_nodes=3)
    @matrix(num_seeds=[1, 2, 3],
            auto_assign_node_ids=[False, True],
            empty_seed_starts_cluster=[False, True])
    def test_three_node_bootstrap(self, num_seeds, auto_assign_node_ids,
                                  empty_seed_starts_cluster):
        set_seeds_for_cluster(self.redpanda, num_seeds)
        for node in self.redpanda.nodes:
            self.redpanda.set_extra_node_conf(
                node, {"empty_seed_starts_cluster": empty_seed_starts_cluster})
        self.redpanda.start(auto_assign_node_id=auto_assign_node_ids,
                            omit_seeds_on_idx_one=False)
        node_ids_per_idx = {}
        for n in self.redpanda.nodes:
            idx = self.redpanda.idx(n)
            node_ids_per_idx[idx] = self.redpanda.node_id(n)

        brokers = self.admin.get_brokers()
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"

        # Restart our nodes and make sure our node IDs persist across restarts.
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=auto_assign_node_ids,
                                    omit_seeds_on_idx_one=False)
        for idx in node_ids_per_idx:
            n = self.redpanda.get_node(idx)
            expected_node_id = node_ids_per_idx[idx]
            node_id = self.redpanda.node_id(n)
            assert expected_node_id == node_id, f"Expected {expected_node_id} but got {node_id}"


class ClusterBootstrapUpgrade(RedpandaTest):
    """
    Tests verifying upgrade of cluster from a pre-Seed-Driven-Bootstrap version
    """
    def __init__(self, test_context):
        super(ClusterBootstrapUpgrade,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.redpanda._installer
        self.admin = self.redpanda._admin

    def setUp(self):
        # NOTE: `rpk redpanda admin brokers list` requires versions v22.1.x and
        # above.
        _, self.oldversion_str = self.installer.install(
            self.redpanda.nodes, (22, 2))
        set_seeds_for_cluster(self.redpanda, 3)
        super(ClusterBootstrapUpgrade, self).setUp()

    @cluster(num_nodes=3)
    @matrix(empty_seed_starts_cluster=[False, True])
    def test_change_bootstrap_configs_after_upgrade(self,
                                                    empty_seed_starts_cluster):
        # Upgrade the cluster to begin using the new binary, but don't change
        # any configs yet.
        self.installer.install(self.redpanda.nodes, (22, 3))
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)

        # Now update the configs.
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            override_cfg_params={
                                                "empty_seed_starts_cluster":
                                                empty_seed_starts_cluster
                                            },
                                            omit_seeds_on_idx_one=False)

    @cluster(num_nodes=3)
    @matrix(empty_seed_starts_cluster=[False, True])
    def test_change_bootstrap_configs_during_upgrade(
            self, empty_seed_starts_cluster):
        # Upgrade the cluster as we change the configs node-by-node.
        self.installer.install(self.redpanda.nodes, (22, 3))
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            override_cfg_params={
                                                "empty_seed_starts_cluster":
                                                empty_seed_starts_cluster
                                            },
                                            omit_seeds_on_idx_one=False)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_cluster_uuid_upgrade(self):
        """
        Check that cluster_uuid is not available until the entire cluster is
        upgraded, and after the upgrade is complete that cluster_uuid is here
        """
        def get_cluster_uuid():
            u_prev = None
            for n in self.redpanda.nodes:
                u = self.admin.get_cluster_uuid(n)
                if u is None:
                    self.redpanda.logger.debug(f"No cluster UUID in {n.name}")
                    return None
                if u_prev is not None:
                    assert u == u_prev, f"Different cluster UUID values:"\
                        " {u} of {n.name}, and {u_prev} of the previous node"
                u_prev = u
            return u_prev

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.oldversion_str in unique_versions, unique_versions

        # Upgrade all but 1 node, verify cluster UUID is not created in 10s
        one_node = [self.redpanda.nodes[0]]
        but_one_node = self.redpanda.nodes[1:]
        self.installer.install(but_one_node, (22, 3))
        self.redpanda.restart_nodes(but_one_node)
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert self.oldversion_str in unique_versions, unique_versions
        try:
            wait_until(lambda: get_cluster_uuid() is not None,
                       timeout_sec=10,
                       backoff_sec=1)
            assert False, "Cluster UUID created before the cluster has completed an upgrade"
        except ducktape.errors.TimeoutError:
            pass

        # Upgrade all nodes, verify the cluster has a UUID now
        self.installer.install(one_node, (22, 3))
        self.redpanda.restart_nodes(one_node)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert self.oldversion_str not in unique_versions, unique_versions
        wait_until(lambda: get_cluster_uuid() is not None, timeout_sec=30)
