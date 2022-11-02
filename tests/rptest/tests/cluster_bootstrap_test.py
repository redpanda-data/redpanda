# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller


def set_seeds_for_cluster(redpanda, num_seeds):
    seeds = [redpanda.nodes[i] for i in range(num_seeds)]
    redpanda.set_seed_servers(seeds)


class ClusterBootstrapTest(RedpandaTest):
    def __init__(self, test_context):
        super(ClusterBootstrapTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_node_conf={
                                 "empty_seed_starts_cluster": False,
                             })
        self.admin = self.redpanda._admin

    def setUp(self):
        # Defer startup to test body.
        pass

    @cluster(num_nodes=3)
    @matrix(num_seeds=[1, 2, 3], auto_assign_node_ids=[False, True])
    def test_three_node_bootstrap(self, num_seeds, auto_assign_node_ids):
        set_seeds_for_cluster(self.redpanda, num_seeds)
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


class ClusterBootstrapUpgradeTest(RedpandaTest):
    def __init__(self, test_context):
        super(ClusterBootstrapUpgradeTest,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.redpanda._installer
        self.admin = self.redpanda._admin

    def setUp(self):
        # Start out on a version that did not have seeds-driven bootstrap.
        self.installer.install(self.redpanda.nodes, (22, 2, 1))
        set_seeds_for_cluster(self.redpanda, 3)
        super(ClusterBootstrapUpgradeTest, self).setUp()

    @cluster(num_nodes=3)
    def test_change_bootstrap_configs_after_upgrade(self):
        # Upgrade the cluster to begin using the new binary, but don't change
        # any configs yet.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)

        # Now update the configs.
        self.redpanda.rolling_restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={"empty_seed_starts_cluster": False},
            omit_seeds_on_idx_one=False)

    @cluster(num_nodes=3)
    def test_change_bootstrap_configs_during_upgrade(self):
        # Upgrade the cluster as we change the configs node-by-node.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={"empty_seed_starts_cluster": False},
            omit_seeds_on_idx_one=False)
