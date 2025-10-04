# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Network chaos utilities - reusable functions.

This module provides reusable network partition functions that can be used
across different test classes for simulating network failures.
"""


class NetworkChaos:
    """Utility class for network-related chaos operations."""

    @staticmethod
    def create_partial_partition(
        source_cluster, target_cluster, source_node_idx=0, logger=None
    ):
        """
        Create partial network partition: specific source node cannot talk to any target nodes.

        This simulates a realistic scenario where one source node loses connectivity
        to the target cluster while other nodes maintain their connections.

        Args:
            source_cluster: Source cluster object
            target_cluster: Target cluster object
            source_node_idx: Index of source node to partition (default: 0)
            logger: Optional logger instance
        """
        source_node = source_cluster.service.nodes[source_node_idx]

        if logger:
            logger.info(
                f"Creating partial network partition for source node {source_node_idx}"
            )

        for target_node in target_cluster.service.nodes:
            # Block traffic both ways from specified source node only
            cmd = f"iptables -A INPUT -s {target_node.account.hostname} -j DROP"
            source_node.account.ssh(cmd, allow_fail=True)
            cmd = f"iptables -A OUTPUT -d {target_node.account.hostname} -j DROP"
            source_node.account.ssh(cmd, allow_fail=True)

    @staticmethod
    def create_complete_partition(source_cluster, target_cluster, logger=None):
        """
        Create complete network partition between all source and target nodes.

        This simulates a complete network split between data centers where
        no communication is possible between the two clusters.

        TODO: This implementation creates N×M iptables rules which is inefficient.
        Should be simplified with one of these approaches:
        1. Block by subnet if clusters are on different subnets
        2. Use ipset to group all target IPs and block with a single rule
        3. Block at interface level if dedicated interfaces exist
        4. For more realistic testing, consider implementing at router/switch level
        5. This of cross-region VPC peering with route table manipulation
        6. Use a proxy tool like toxiproxy to simulate network failures
        7. Use of 3rd party network chaos tool like Chaos Mesh
        8  Think of cross-cloud scenarios (one cluster on AWS, another on GCP/Azure)

        Args:
            source_cluster: Source cluster object
            target_cluster: Target cluster object
            logger: Optional logger instance
        """
        if logger:
            logger.info("Creating complete network partition between clusters")

        for source_node in source_cluster.service.nodes:
            for target_node in target_cluster.service.nodes:
                # Block all traffic between every source and target node pair
                cmd = f"iptables -A INPUT -s {target_node.account.hostname} -j DROP"
                source_node.account.ssh(cmd, allow_fail=True)
                cmd = f"iptables -A OUTPUT -d {target_node.account.hostname} -j DROP"
                source_node.account.ssh(cmd, allow_fail=True)

        if logger:
            logger.info("Complete network partition created")

    @staticmethod
    def heal_partial_partition(source_cluster, source_node_idx=0, logger=None):
        """
        Remove partial network partition rules from specified source node.

        Args:
            source_cluster: Source cluster object
            source_node_idx: Index of source node to heal (default: 0)
            logger: Optional logger instance
        """
        source_node = source_cluster.service.nodes[source_node_idx]

        if logger:
            logger.info(
                f"Healing partial network partition on source node {source_node_idx}"
            )

        source_node.account.ssh("iptables -F", allow_fail=True)

    @staticmethod
    def heal_complete_partition(source_cluster, logger=None):
        """
        Remove network partition rules from all source nodes.

        Args:
            source_cluster: Source cluster object
            logger: Optional logger instance
        """
        if logger:
            logger.info("Healing complete network partition")

        for source_node in source_cluster.service.nodes:
            # Flush iptables rules on all source nodes
            source_node.account.ssh("iptables -F", allow_fail=True)

        if logger:
            logger.info("Network partition healed")

    @staticmethod
    def create_asymmetric_partition(
        source_cluster, target_cluster, direction="source_to_target", logger=None
    ):
        """
        Create asymmetric network partition (one-way block).

        Args:
            source_cluster: Source cluster object
            target_cluster: Target cluster object
            direction: Either "source_to_target" or "target_to_source"
            logger: Optional logger instance
        """
        if logger:
            logger.info(f"Creating asymmetric partition: {direction}")

        if direction == "source_to_target":
            # Source can't send to target, but can receive
            for source_node in source_cluster.service.nodes:
                for target_node in target_cluster.service.nodes:
                    cmd = (
                        f"iptables -A OUTPUT -d {target_node.account.hostname} -j DROP"
                    )
                    source_node.account.ssh(cmd, allow_fail=True)
        else:
            # Source can't receive from target, but can send
            for source_node in source_cluster.service.nodes:
                for target_node in target_cluster.service.nodes:
                    cmd = f"iptables -A INPUT -s {target_node.account.hostname} -j DROP"
                    source_node.account.ssh(cmd, allow_fail=True)

    @staticmethod
    def add_network_delay(
        cluster, delay_ms=100, jitter_ms=10, node_idx=None, logger=None
    ):
        """
        Add network delay to cluster nodes using tc (traffic control).

        Args:
            cluster: Cluster object
            delay_ms: Base delay in milliseconds
            jitter_ms: Jitter/variance in milliseconds
            node_idx: Specific node index, or None for all nodes
            logger: Optional logger instance
        """
        nodes = (
            [cluster.service.nodes[node_idx]]
            if node_idx is not None
            else cluster.service.nodes
        )

        if logger:
            logger.info(
                f"Adding {delay_ms}ms delay (±{jitter_ms}ms jitter) to {len(nodes)} nodes"
            )

        for node in nodes:
            # Add delay to eth0 interface
            cmd = f"tc qdisc add dev eth0 root netem delay {delay_ms}ms {jitter_ms}ms"
            node.account.ssh(cmd, allow_fail=True)

    @staticmethod
    def remove_network_delay(cluster, node_idx=None, logger=None):
        """
        Remove network delay from cluster nodes.

        Args:
            cluster: Cluster object
            node_idx: Specific node index, or None for all nodes
            logger: Optional logger instance
        """
        nodes = (
            [cluster.service.nodes[node_idx]]
            if node_idx is not None
            else cluster.service.nodes
        )

        if logger:
            logger.info(f"Removing network delay from {len(nodes)} nodes")

        for node in nodes:
            # Remove tc rules from eth0
            cmd = "tc qdisc del dev eth0 root"
            node.account.ssh(cmd, allow_fail=True)
