# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Node chaos utilities - reusable functions.

This module provides reusable node failure functions that can be used
across different test classes for simulating node failures and restarts.
"""

import random


class NodeChaos:
    """Utility class for node-related chaos operations."""

    @staticmethod
    def kill_node(cluster, node_idx=None, logger=None):
        """
        Kill a specific node or random node in the cluster.

        Args:
            cluster: Cluster object
            node_idx: Specific node index to kill, or None for random
            logger: Optional logger instance

        Returns:
            The node that was killed
        """
        if node_idx is None:
            node_idx = random.randint(0, len(cluster.service.nodes) - 1)

        node = cluster.service.nodes[node_idx]

        if logger:
            logger.info(f"Killing node {node_idx}: {node.account.hostname}")

        cluster.service.stop_node(node)
        return node

    @staticmethod
    def restart_node(cluster, node, logger=None):
        """
        Restart a specific node.

        Args:
            cluster: Cluster object
            node: Node to restart
            logger: Optional logger instance
        """
        if logger:
            logger.info(f"Restarting node: {node.account.hostname}")

        cluster.service.start_node(node)

    @staticmethod
    def rolling_restart(cluster, wait_between_nodes=10, logger=None):
        """
        Perform a rolling restart of all nodes in the cluster.

        Args:
            cluster: Cluster object
            wait_between_nodes: Seconds to wait between restarting nodes
            logger: Optional logger instance
        """
        if logger:
            logger.info(
                f"Starting rolling restart of {len(cluster.service.nodes)} nodes"
            )

        for i, node in enumerate(cluster.service.nodes):
            if logger:
                logger.info(
                    f"Restarting node {i + 1}/{len(cluster.service.nodes)}: {node.account.hostname}"
                )

            cluster.service.stop_node(node)
            time.sleep(5)  # Brief pause after stop
            cluster.service.start_node(node)

            if i < len(cluster.service.nodes) - 1:  # Don't wait after last node
                time.sleep(wait_between_nodes)

        if logger:
            logger.info("Rolling restart completed")

    @staticmethod
    def kill_partition_leader(
        cluster, topic_name, partition_id=0, rpk_tool=None, logger=None
    ):
        """
        Kill the leader node for a specific partition.

        Args:
            cluster: Cluster object
            topic_name: Name of the topic
            partition_id: Partition ID (default: 0)
            rpk_tool: RpkTool instance for the cluster
            logger: Optional logger instance

        Returns:
            The leader node that was killed, or None if not found
        """
        if rpk_tool is None:
            from rptest.clients.rpk import RpkTool

            rpk_tool = RpkTool(cluster.service)

        partitions = rpk_tool.describe_topic(topic_name)
        target_partition = next((p for p in partitions if p.id == partition_id), None)

        if not target_partition:
            if logger:
                logger.warning(
                    f"Partition {partition_id} not found for topic {topic_name}"
                )
            return None

        leader_node = None
        for node in cluster.service.nodes:
            node_id = cluster.service.idx(node)
            if node_id == target_partition.leader:
                leader_node = node
                break

        if leader_node:
            if logger:
                logger.info(
                    f"Killing leader node {leader_node.account.hostname} for partition {partition_id}"
                )
            cluster.service.stop_node(leader_node)

        return leader_node

    @staticmethod
    def pause_node(cluster, node_idx=None, duration_sec=30, logger=None):
        """
        Pause a node using SIGSTOP (simulate long GC pause).

        Args:
            cluster: Cluster object
            node_idx: Node index to pause, or None for random
            duration_sec: How long to pause the node
            logger: Optional logger instance

        Returns:
            The node that was paused
        """
        if node_idx is None:
            node_idx = random.randint(0, len(cluster.service.nodes) - 1)

        node = cluster.service.nodes[node_idx]

        if logger:
            logger.info(f"Pausing node {node_idx} for {duration_sec} seconds")

        # Get the Redpanda process PID
        pids = cluster.service.pids(node)
        if pids:
            for pid in pids:
                node.account.ssh(f"kill -STOP {pid}", allow_fail=True)

        # Schedule resume
        import threading

        def resume():
            if pids:
                for pid in pids:
                    node.account.ssh(f"kill -CONT {pid}", allow_fail=True)
            if logger:
                logger.info(f"Resumed node {node_idx}")

        timer = threading.Timer(duration_sec, resume)
        timer.start()

        return node


# Import time at module level
import time
