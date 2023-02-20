# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.errors import TimeoutError
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller


def wipe_and_restart(redpanda, node):
    """
    Stops, clears, and restarts a node, priming it to be assigned a node ID.
    """
    redpanda.stop_node(node)
    redpanda.clean_node(node,
                        preserve_logs=True,
                        preserve_current_install=True)
    redpanda.start_node(node, auto_assign_node_id=True)


def check_node_ids_persist(redpanda):
    """
    Checks that all node IDs remain the same after restarting, where after
    restarting we omit the node IDs from the node config files.
    """
    original_node_id_by_idx = {}
    node_ids = set()
    for node in redpanda.nodes:
        original_node_id = redpanda.node_id(node, force_refresh=True)
        original_node_id_by_idx[redpanda.idx(node)] = original_node_id
        assert original_node_id not in node_ids
        node_ids.add(original_node_id)

    redpanda.restart_nodes(redpanda.nodes, auto_assign_node_id=True)
    for node in redpanda.nodes:
        original_node_id = original_node_id_by_idx[redpanda.idx(node)]
        new_node_id = redpanda.node_id(node, force_refresh=True)
        assert new_node_id == original_node_id, f"{new_node_id} vs {original_node_id}"


def wait_for_node_removed(admin, node_id):
    def node_removed():
        brokers = admin.get_brokers()
        return not any(b["node_id"] == node_id for b in brokers)

    wait_until(node_removed, timeout_sec=30, backoff_sec=1)


class NodeIdAssignment(RedpandaTest):
    """
    Test that exercises cluster formation when node IDs are automatically
    assigned by Redpanda.
    """
    def __init__(self, test_context):
        super(NodeIdAssignment, self).__init__(test_context=test_context,
                                               num_brokers=3)
        self.admin = self.redpanda._admin

    def setUp(self):
        self.redpanda.set_seed_servers(self.redpanda.nodes)
        self.redpanda.start(auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
        self._create_initial_topics()

    def node_ids(self):
        return [self.redpanda.node_id(n) for n in self.redpanda.nodes]

    @cluster(num_nodes=3)
    def test_basic_assignment(self):
        brokers = self.admin.get_brokers()
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"
        check_node_ids_persist(self.redpanda)

    @cluster(num_nodes=3)
    def test_assign_after_decommission(self):
        """
        We should be able to get a new node ID after wiping and decommissioning
        a node.
        """
        original_node_ids = self.node_ids()
        replaced_node = self.redpanda.nodes[-1]
        replaced_node_id = self.redpanda.node_id(replaced_node)

        self.admin.decommission_broker(replaced_node_id)
        wait_for_node_removed(self.admin, replaced_node_id)

        wipe_and_restart(self.redpanda, replaced_node)
        new_node_id = self.redpanda.node_id(replaced_node, force_refresh=True)
        assert new_node_id not in original_node_ids, f"Cleaned node came back with node ID {new_node_id}"

    @cluster(num_nodes=3)
    def test_rejoin_after_decommission(self):
        """
        If we only decommission a node without wiping it, it will still use its
        assigned node ID.
        """
        original_node = self.redpanda.nodes[-1]
        original_node_id = self.redpanda.node_id(original_node)

        self.admin.decommission_broker(original_node_id)
        wait_for_node_removed(self.admin, original_node_id)

        new_node_id = self.redpanda.node_id(original_node, force_refresh=True)
        assert original_node_id == new_node_id, f"Node came back with different node ID {new_node_id}"

    @cluster(num_nodes=3)
    def test_assign_after_clear(self):
        """
        Wiping a node that doesn't have a configured node ID should result in a
        new node ID being assigned to that node.
        """
        original_node_ids = self.node_ids()
        brokers = self.admin.get_brokers()
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"

        clean_node = self.redpanda.nodes[-1]
        wipe_and_restart(self.redpanda, clean_node)

        brokers = self.admin.get_brokers()
        assert 4 == len(brokers), f"Got {len(brokers)} brokers"
        new_node_id = self.redpanda.node_id(clean_node, force_refresh=True)
        assert new_node_id not in original_node_ids, f"Cleaned node came back with node ID {new_node_id}"
        check_node_ids_persist(self.redpanda)

    @cluster(num_nodes=3, log_allow_list=["doesn't match stored node ID"])
    def test_rejoin_with_wrong_node_id(self):
        """
        Nodes should fail to start up if attempting to use a different node ID
        than the one it's previously used, according to its kv-store.
        """
        node = self.redpanda.nodes[-1]
        self.redpanda.stop_node(node)
        self.redpanda.start_node(node,
                                 override_cfg_params=dict({"node_id": 10}),
                                 expect_fail=True)


class NodeIdAssignmentParallel(RedpandaTest):
    """
    Test that adds several nodes at once, assigning each a unique node ID.
    """
    def __init__(self, test_context):
        super(NodeIdAssignmentParallel,
              self).__init__(test_context=test_context, num_brokers=4)

    def setUp(self):
        # Start just one node so we can add several at the same time.
        self.redpanda.start([self.redpanda.nodes[0]],
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)

    @cluster(num_nodes=4)
    def test_assign_multiple_nodes(self):
        self.redpanda.start(self.redpanda.nodes[1:],
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False,
                            parallel=True)

        def check_num_nodes():
            brokers = self.redpanda._admin.get_brokers()
            return len(self.redpanda.nodes) == len(brokers)

        wait_until(check_num_nodes, timeout_sec=30, backoff_sec=1)
        check_node_ids_persist(self.redpanda)


class NodeIdAssignmentUpgrade(RedpandaTest):
    """
    Test that exercises cluster formation when node IDs are automatically
    assigned by Redpanda after an upgrade.
    """
    def __init__(self, test_context):
        super(NodeIdAssignmentUpgrade,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.redpanda._installer
        self.admin = self.redpanda._admin

    def setUp(self):
        self.installer.install(self.redpanda.nodes, (22, 2))
        super(NodeIdAssignmentUpgrade, self).setUp()

    @cluster(num_nodes=3)
    def test_omit_node_ids_after_upgrade(self):
        """
        In restarting the cluster, even if we omit the node IDs from their
        configs after upgrading, they should still persist.
        """
        self.installer.install(self.redpanda.nodes, (22, 3))
        check_node_ids_persist(self.redpanda)

    @cluster(num_nodes=3)
    def test_assign_after_upgrade(self):
        """
        Upgrade a cluster and then immediately start using the node ID
        assignment feature.
        """
        original_node_ids = [
            self.redpanda.node_id(n) for n in self.redpanda.nodes
        ]
        self.installer.install(self.redpanda.nodes, (22, 3))
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True)
        wait_until(
            lambda: self.admin.supports_feature("node_id_assignment"),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for cluster to support 'license' feature")

        clean_node = self.redpanda.nodes[-1]
        wipe_and_restart(self.redpanda, clean_node)

        brokers = self.admin.get_brokers()
        assert 4 == len(brokers), f"Got {len(brokers)} brokers"
        new_node_id = self.redpanda.node_id(clean_node, force_refresh=True)
        assert new_node_id not in original_node_ids, f"Cleaned node came back with node ID {new_node_id}"
