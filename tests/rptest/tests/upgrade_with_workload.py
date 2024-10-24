# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.redpanda import RedpandaService, RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller


class MixedVersionWorkloadRunner():
    ALLOWED_LOGS = RESTART_LOG_ALLOW_LIST

    # For testing RPC compatibility, pick a version that doesn't have serde
    # enabled.
    PRE_SERDE_VERSION = (22, 1)

    @staticmethod
    def upgrade_with_workload(redpanda: RedpandaService, initial_version,
                              workload_fn):
        """
        Runs through an upgrade while running the given workload during the
        intermediate mixed-cluster stages.

        It's expected that before starting, the version from which Redpanda is
        being upgraded has already been installed and deployed (nodes started
        with new bits) on the service.

        'workload_fn' is a function that takes two nodes and deterministically
        performs work between nodes, for instance, by having RPCs go from one
        node to the other.
        """
        num_nodes = len(redpanda.nodes)
        assert num_nodes >= 2, f"Expected at least two nodes, got {num_nodes}"
        installer: RedpandaInstaller = redpanda._installer
        nodes = redpanda.nodes
        node0 = nodes[0]
        node1 = nodes[1]

        # Upgrade one node and send RPCs in both directions.
        installer.install([node0], (22, 2))
        redpanda.restart_nodes([node0])
        workload_fn(node0, node1)
        workload_fn(node1, node0)

        # Continue on with the upgrade. The versions are identical at this
        # point so just run the workload in one direction.
        installer.install([node1], (22, 2))
        redpanda.restart_nodes([node1])
        workload_fn(node0, node1)

        # Partial roll back and make sure we can still run the workload.
        installer.install([node1], initial_version)
        redpanda.restart_nodes([node1])
        workload_fn(node0, node1)
        workload_fn(node1, node0)

        # Complete the upgrade. The versions are identical again so just run
        # through the workload in one direction.
        installer.install(nodes, (22, 2))
        redpanda.restart_nodes(nodes)
        workload_fn(node0, node1)
