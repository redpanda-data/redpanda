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
    for node in redpanda.nodes:
        original_node_id = redpanda.node_id(node, force_refresh=True)
        original_node_id_by_idx[redpanda.idx(node)] = original_node_id

    redpanda.restart_nodes(redpanda.nodes, auto_assign_node_id=True)
    for node in redpanda.nodes:
        original_node_id = original_node_id_by_idx[redpanda.idx(node)]
        new_node_id = redpanda.node_id(node, force_refresh=True)
        assert new_node_id == original_node_id, f"{new_node_id} vs {original_node_id}"


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
        self.redpanda.start(auto_assign_node_id=True)
        self._create_initial_topics()

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
        replaced_node = self.redpanda.nodes[-1]
        replaced_node_id = self.redpanda.node_id(replaced_node)
        self.admin.decommission_broker(replaced_node_id)
        wipe_and_restart(self.redpanda, replaced_node)
        new_node_id = self.redpanda.node_id(replaced_node, force_refresh=True)
        assert replaced_node_id != new_node_id, f"Cleaned node came back with node ID {new_node_id}"

    @cluster(num_nodes=3)
    def test_rejoin_after_decommission(self):
        """
        If we only decommission a node without wiping it, it will still use its
        assigned node ID.
        """
        original_node = self.redpanda.nodes[-1]
        original_node_id = self.redpanda.node_id(original_node)
        self.admin.decommission_broker(original_node_id)
        new_node_id = self.redpanda.node_id(original_node, force_refresh=True)
        assert original_node_id == new_node_id, f"Node came back with different node ID {new_node_id}"

    @cluster(num_nodes=3)
    def test_assign_after_clear(self):
        """
        Wiping a node that doesn't have a configured node ID should result in a
        new node ID being assigned to that node.
        """
        brokers = self.admin.get_brokers()
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"

        clean_node = self.redpanda.nodes[-1]
        original_node_id = self.redpanda.node_id(clean_node)
        wipe_and_restart(self.redpanda, clean_node)

        brokers = self.admin.get_brokers()
        assert 4 == len(brokers), f"Got {len(brokers)} brokers"
        new_node_id = self.redpanda.node_id(clean_node, force_refresh=True)
        assert original_node_id != new_node_id, f"Cleaned node came back with node ID {new_node_id}"
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
        self.installer.install(self.redpanda.nodes, (22, 2, 7))
        super(NodeIdAssignmentUpgrade, self).setUp()

    @cluster(num_nodes=3)
    def test_omit_node_ids_after_upgrade(self):
        """
        In restarting the cluster, even if we omit the node IDs from their
        configs after upgrading, they should still persist.
        """
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        check_node_ids_persist(self.redpanda)

    @cluster(num_nodes=3)
    def test_assign_after_upgrade(self):
        """
        Upgrade a cluster and then immediately start using the node ID
        assignment feature.
        """
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True)
        wait_until(
            lambda: self.admin.supports_feature("node_id_assignment"),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for cluster to support 'license' feature")

        clean_node = self.redpanda.nodes[-1]
        original_node_id = self.redpanda.node_id(clean_node)
        wipe_and_restart(self.redpanda, clean_node)

        brokers = self.admin.get_brokers()
        assert 4 == len(brokers), f"Got {len(brokers)} brokers"
        new_node_id = self.redpanda.node_id(clean_node, force_refresh=True)
        assert original_node_id != new_node_id, f"Cleaned node came back with node ID {new_node_id}"
