# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
import time
import random
import json
import os

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierConsumerGroupConsumer,
)
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.tests.chaos.network_chaos import NetworkChaos


class SimpleChaosTest(ShadowLinkTestBase):
    """
    Simple chaos tests with minimal data for quick testing.
    Perfect for starting small and gradually increasing complexity.
    """

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, *args, **kwargs)

        # Load configuration if provided
        config_path = os.path.join(
            os.path.dirname(__file__), "configs/simple_chaos.json"
        )
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                self.chaos_config = json.load(f).get("simple_chaos_config", {})
        else:
            self.chaos_config = {}

        # Default configuration with overrides from config file
        self.max_replication_lag_percent = self.chaos_config.get("max_lag_percent", 5.0)
        self.chaos_duration_sec = self.chaos_config.get("chaos_duration_sec", 30)
        self.recovery_wait_sec = self.chaos_config.get("recovery_wait_sec", 20)
        self.message_count = self.chaos_config.get("message_count", 1000)
        self.message_size = self.chaos_config.get("message_size", 512)
        self.rate_limit_kb_per_sec = self.chaos_config.get("rate_limit_kb_per_sec", 10)

    @cluster(num_nodes=8)  # 6 for clusters + 2 for workload
    def test_simple_kill_node(self):
        """
        Simplest chaos test: kill one node and verify data integrity.
        Quick test with small amount of data.
        """

        self.logger.info("=== SIMPLE CHAOS TEST: Kill Single Node ===")

        # Create one small topic
        topic = "simple-chaos-topic"
        topic_spec = TopicSpec(name=topic, partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic_spec)

        # Create shadow link
        self.create_link("simple-dr-link")

        # Wait for topic to replicate
        wait_until(lambda: self.topic_exists_in_target(topic), timeout_sec=30)

        # Start small producer (just 1000 messages)
        self.logger.info("Starting producer with 1000 messages...")
        producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            msg_size=self.message_size,
            msg_count=self.message_count,
            rate_limit_bps=self.rate_limit_kb_per_sec * 1024,
        )
        producer.start(clean=False)

        # Wait a bit for some production
        time.sleep(10)

        # INJECT CHAOS: Kill one source node
        source_nodes = self.source_cluster.service.nodes
        node_to_kill = source_nodes[1]  # Kill second node

        self.logger.info(f"\n>>> CHAOS: Killing node {node_to_kill.account.hostname}")
        self.source_cluster.service.stop_node(node_to_kill)

        # Let it run with node down
        self.logger.info(
            f"Running with node down for {self.chaos_duration_sec} seconds..."
        )
        time.sleep(self.chaos_duration_sec)

        # Restart the node
        self.logger.info(
            f"\n>>> RECOVERY: Restarting node {node_to_kill.account.hostname}"
        )
        self.source_cluster.service.start_node(node_to_kill)

        # Wait for producer to finish
        producer.wait()

        # Start consumer on target to verify
        self.logger.info("\nStarting consumer on target cluster to verify data...")
        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.target_cluster.service,
            topic=topic,
            msg_size=512,
            readers=1,
            max_msgs=1000,
            group_name="verify-group",
        )
        consumer.start(clean=False)
        consumer.wait()

        # Check results
        self._print_simple_results(producer, consumer)

        # Verify no data loss
        assert consumer.consumer_status.validator.invalid_reads == 0, (
            "Data corruption detected!"
        )
        assert consumer.consumer_status.validator.valid_reads > 900, (
            "Too few messages consumed"
        )

        self.logger.info(
            "\n✓ TEST PASSED: Data integrity maintained despite node failure"
        )

    @cluster(num_nodes=8)
    def test_simple_network_partition(self):
        """
        Simple network partition test between clusters.
        """

        self.logger.info("=== SIMPLE CHAOS TEST: Network Partition ===")

        # Setup
        topic = "network-partition-topic"
        topic_spec = TopicSpec(name=topic, partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic_spec)

        self.create_link("partition-dr-link")
        wait_until(lambda: self.topic_exists_in_target(topic), timeout_sec=30)

        # Start producer
        producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            msg_size=512,
            msg_count=2000,
            rate_limit_bps=20480,  # 20KB/s
        )
        producer.start(clean=False)

        # Wait for some data
        time.sleep(15)

        # INJECT CHAOS: Partial network partition (only source node 0)
        self.logger.info(
            "\n>>> CHAOS: Creating partial network partition (source node 0 isolated from target cluster)"
        )
        NetworkChaos.create_partial_partition(
            self.source_cluster, self.target_cluster, logger=self.logger
        )

        # Run with partition
        self.logger.info(
            f"Running with network partition for {self.chaos_duration_sec} seconds..."
        )
        time.sleep(self.chaos_duration_sec)

        # Heal partial partition
        self.logger.info("\n>>> RECOVERY: Healing partial network partition")
        NetworkChaos.heal_partial_partition(self.source_cluster, logger=self.logger)

        # Wait for producer and replication
        producer.wait()
        time.sleep(self.recovery_wait_sec)  # Extra time for replication to catch up

        # Verify data
        self._verify_replication(topic, producer.produce_status.acked)

        self.logger.info(
            "\n✓ TEST PASSED: Replication recovered after network partition"
        )

    @cluster(num_nodes=8)
    def test_simple_rolling_restart(self):
        """
        Simple rolling restart of source cluster during replication.
        """

        self.logger.info("=== SIMPLE CHAOS TEST: Rolling Restart ===")

        # Setup
        topic = "rolling-restart-topic"
        topic_spec = TopicSpec(name=topic, partition_count=6, replication_factor=3)
        self.source_default_client().create_topic(topic_spec)

        self.create_link("rolling-dr-link")
        wait_until(lambda: self.topic_exists_in_target(topic), timeout_sec=30)

        # Start continuous producer
        producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            msg_size=1024,
            msg_count=5000,
            rate_limit_bps=51200,  # 50KB/s
        )
        producer.start(clean=False)

        # Start rolling restart after some data
        time.sleep(10)

        self.logger.info("\n>>> CHAOS: Starting rolling restart")
        for i, node in enumerate(self.source_cluster.service.nodes):
            self.logger.info(f"Restarting node {i + 1}/3: {node.account.hostname}")
            self.source_cluster.service.stop_node(node)
            time.sleep(5)
            self.source_cluster.service.start_node(node)
            time.sleep(10)  # Wait for recovery

        # Wait for producer
        producer.wait()
        time.sleep(15)

        # Verify
        self._verify_replication(topic, producer.produce_status.acked)

        self.logger.info(
            "\n✓ TEST PASSED: Replication maintained during rolling restart"
        )

    @cluster(num_nodes=8)
    def test_simple_leader_kill(self):
        """
        Kill partition leaders and verify replication continues.
        """

        self.logger.info("=== SIMPLE CHAOS TEST: Kill Partition Leader ===")

        # Setup topic with more partitions
        topic = "leader-kill-topic"
        topic_spec = TopicSpec(name=topic, partition_count=9, replication_factor=3)
        self.source_default_client().create_topic(topic_spec)

        self.create_link("leader-dr-link")
        wait_until(lambda: self.topic_exists_in_target(topic), timeout_sec=30)

        # Start producer
        producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            msg_size=512,
            msg_count=3000,
            rate_limit_bps=30720,  # 30KB/s
        )
        producer.start(clean=False)

        # Wait for some production
        time.sleep(10)

        # Find and kill a leader
        rpk = RpkTool(self.source_cluster.service)
        partitions = rpk.describe_topic(topic)

        # Pick partition 0's leader
        partition_0 = next(p for p in partitions if p.id == 0)
        leader_node = None

        for node in self.source_cluster.service.nodes:
            node_id = self.source_cluster.service.idx(node)
            if node_id == partition_0.leader:
                leader_node = node
                break

        if leader_node:
            self.logger.info(
                f"\n>>> CHAOS: Killing leader node {leader_node.account.hostname} for partition 0"
            )
            self.source_cluster.service.stop_node(leader_node)

            # Run without leader
            time.sleep(self.chaos_duration_sec)

            # Restart
            self.logger.info(f"\n>>> RECOVERY: Restarting leader node")
            self.source_cluster.service.start_node(leader_node)

        # Wait for completion
        producer.wait()
        time.sleep(15)

        # Verify
        self._verify_replication(topic, producer.produce_status.acked)

        self.logger.info("\n✓ TEST PASSED: Replication handled leader failure")

    @cluster(num_nodes=8)
    def test_complete_network_partition(self):
        """
        Test complete network partition between source and target clusters.
        This simulates a complete data center network split.
        """

        self.logger.info("=== SIMPLE CHAOS TEST: Complete Network Partition ===")

        # Setup
        topic = "complete-partition-topic"
        topic_spec = TopicSpec(name=topic, partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic_spec)

        self.create_link("complete-partition-dr-link")
        wait_until(lambda: self.topic_exists_in_target(topic), timeout_sec=30)

        # Start producer
        producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            msg_size=1024,
            msg_count=3000,
            rate_limit_bps=51200,  # 50KB/s
        )
        producer.start(clean=False)

        # Wait for some data
        time.sleep(15)

        # INJECT CHAOS: Complete network partition
        self.logger.info(
            "\n>>> CHAOS: Creating COMPLETE network partition between clusters"
        )
        NetworkChaos.create_complete_partition(
            self.source_cluster, self.target_cluster, logger=self.logger
        )

        # Run with complete partition
        self.logger.info(
            f"Running with complete network partition for {self.chaos_duration_sec} seconds..."
        )
        time.sleep(self.chaos_duration_sec)

        # Heal partition
        self.logger.info("\n>>> RECOVERY: Healing complete network partition")
        NetworkChaos.heal_complete_partition(self.source_cluster, logger=self.logger)

        # Wait for producer and replication
        producer.wait()
        time.sleep(
            self.recovery_wait_sec * 2
        )  # Extra time for complete partition recovery

        # Verify data with higher lag tolerance for complete partition
        rpk_source = RpkTool(self.source_cluster.service)
        rpk_target = RpkTool(self.target_cluster.service)

        source_hw = sum(p.high_watermark for p in rpk_source.describe_topic(topic))
        target_hw = sum(p.high_watermark for p in rpk_target.describe_topic(topic))

        lag = source_hw - target_hw
        lag_pct = (lag / source_hw * 100) if source_hw > 0 else 0

        self.logger.info(f"\nREPLICATION STATUS after complete partition:")
        self.logger.info(f"  Source messages: {source_hw}")
        self.logger.info(f"  Target messages: {target_hw}")
        self.logger.info(f"  Lag: {lag} messages ({lag_pct:.1f}%)")

        # Allow higher lag for complete partition (up to 40%)
        max_lag = self.chaos_config.get("complete_partition_max_lag_percent", 40.0)
        assert lag_pct <= max_lag, (
            f"Excessive replication lag: {lag_pct}% (max allowed: {max_lag}%)"
        )

        self.logger.info(
            "\n✓ TEST PASSED: Replication recovered after complete network partition"
        )

    def _verify_replication(self, topic, expected_messages):
        """Verify data was replicated to target cluster."""
        rpk_source = RpkTool(self.source_cluster.service)
        rpk_target = RpkTool(self.target_cluster.service)

        # Get high watermarks
        source_hw = sum(p.high_watermark for p in rpk_source.describe_topic(topic))
        target_hw = sum(p.high_watermark for p in rpk_target.describe_topic(topic))

        lag = source_hw - target_hw
        lag_pct = (lag / source_hw * 100) if source_hw > 0 else 0

        self.logger.info(f"\nREPLICATION STATUS:")
        self.logger.info(f"  Source messages: {source_hw}")
        self.logger.info(f"  Target messages: {target_hw}")
        self.logger.info(f"  Lag: {lag} messages ({lag_pct:.1f}%)")
        self.logger.info(f"  Expected: ~{expected_messages} messages")

        # Allow configurable lag during chaos recovery
        max_lag = self.chaos_config.get("network_partition_max_lag_percent", 25.0)
        assert lag_pct <= max_lag, (
            f"Excessive replication lag: {lag_pct}% (max allowed: {max_lag}%)"
        )

        # Run consumer to verify data integrity
        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.target_cluster.service,
            topic=topic,
            msg_size=512,
            readers=1,
            max_msgs=expected_messages,
            group_name="final-verify",
        )
        consumer.start(clean=False)
        consumer.wait()

        invalid = consumer.consumer_status.validator.invalid_reads
        valid = consumer.consumer_status.validator.valid_reads

        self.logger.info(f"\nDATA VERIFICATION:")
        self.logger.info(f"  Valid reads: {valid}")
        self.logger.info(f"  Invalid reads: {invalid}")

        assert invalid == 0, f"Data corruption detected: {invalid} invalid reads"

    def _print_simple_results(self, producer, consumer):
        """Print simple test results."""
        self.logger.info("\n" + "=" * 50)
        self.logger.info("SIMPLE CHAOS TEST RESULTS")
        self.logger.info("=" * 50)
        self.logger.info(f"Producer:")
        self.logger.info(f"  Messages sent: {producer.produce_status.sent}")
        self.logger.info(f"  Messages acked: {producer.produce_status.acked}")
        self.logger.info(f"  Errors: {producer.produce_status.bad_offsets}")
        self.logger.info(f"Consumer:")
        self.logger.info(
            f"  Valid reads: {consumer.consumer_status.validator.valid_reads}"
        )
        self.logger.info(
            f"  Invalid reads: {consumer.consumer_status.validator.invalid_reads}"
        )
        self.logger.info(
            f"  Data integrity: {'✓ PASSED' if consumer.consumer_status.validator.invalid_reads == 0 else '✗ FAILED'}"
        )
        self.logger.info("=" * 50)
