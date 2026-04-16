# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.kgo_verifier_services import KgoVerifierProducer
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    get_cloud_storage_type,
)
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase
import rptest.tests.cloud_topics.utils as ct_utils


class CloudTopicsRetentionTest(EndToEndCloudTopicsBase):
    """
    Test retention policies for cloud topics.

    Cloud topics store data in object storage (L0) with metadata in a metastore (L1).
    These tests verify that retention.bytes and retention.ms policies correctly
    delete old data using offset checks and consumption verification.
    """

    topic_name = "cloud_topic_retention_test"

    # Override base class topics - we create our own test topic manually
    topics = ()

    def __init__(self, test_context: TestContext):
        self.test_context = test_context

        # Extra Redpanda configuration
        extra_rp_conf = {
            # Fast reconciliation (L0 -> L1 data movement)
            "cloud_topics_reconciliation_interval": 2000,  # 2s
            # Fast housekeeping for testing (default: 5min)
            # This controls how often retention enforcement runs
            "cloud_storage_housekeeping_interval_ms": 5000,  # 5s
        }

        super(CloudTopicsRetentionTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
        )

    def _wait_for_retention_to_apply(
        self, topic: str, partition: int, timeout_sec: int = 30
    ):
        """
        Wait for retention to apply by checking that start_offset advances.
        For cloud topics, start_offset > 0 means retention has deleted old data.
        """
        rpk = RpkTool(self.redpanda)

        def retention_applied() -> bool:
            parts = list(rpk.describe_topic(topic))
            start_offset = None
            for part in parts:
                if part.id == partition:
                    start_offset = part.start_offset
                    break

            self.logger.info(
                f"Current start_offset for {topic}:{partition} = {start_offset}"
            )
            return start_offset is not None and start_offset > 0

        wait_until(
            retention_applied,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=f"Retention did not apply to {topic}:{partition}",
        )

    def _produce(self, topic: str, bytes_to_produce: int):
        assert self.redpanda
        required_messages = bytes_to_produce // 1024
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=1024,
            msg_count=required_messages,
        )

    def _get_partition_info(self, topic: str, partition: int):
        """Get partition info (start_offset, high_watermark) from rpk."""
        rpk = RpkTool(self.redpanda)
        parts = list(rpk.describe_topic(topic))
        for part in parts:
            if part.id == partition:
                return part
        raise ValueError(f"Partition {partition} not found in topic {topic}")

    def _verify_offset_not_consumable(self, topic: str, offset: int):
        """Verify that consuming from a deleted offset fails."""
        rpk = RpkTool(self.redpanda)
        try:
            rpk.consume(
                topic, n=1, offset=f"{offset}-{offset + 1}", timeout=5, quiet=True
            )
            assert False, f"Should not be able to consume deleted offset {offset}"
        except Exception as e:
            self.logger.info(f"Expected: offset {offset} not consumable: {e}")

    def _verify_offset_consumable(self, topic: str, offset: int):
        """Verify that consuming from a valid offset succeeds."""
        rpk = RpkTool(self.redpanda)
        result = rpk.consume(
            topic, n=1, offset=f"{offset}-{offset + 1}", timeout=10, quiet=True
        )
        assert result, f"Should be able to consume from offset {offset}"
        self.logger.info(f"Successfully consumed from offset {offset}")

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(),
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_size_based_retention(
        self, cloud_storage_type: CloudStorageType, storage_mode: str
    ):
        """
        Test that size-based retention (retention.bytes) correctly deletes
        old data from cloud topics.
        """
        # Configuration
        num_messages = 3000
        total_bytes = num_messages * 1024  # 3000KB
        batch_bytes = total_bytes // 2  # 1500KB per batch
        initial_retention = 1024 * 1024 * 1024  # 1GB - won't delete anything
        final_retention = 1500 * 1024  # 1500KB - will delete half the data

        # Create cloud topic with large retention.bytes initially
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic=self.topic_name,
            partitions=1,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                "retention.bytes": str(initial_retention),
            },
        )

        # Produce first batch of data
        self.logger.info(f"Producing first batch: {batch_bytes} bytes")
        self._produce(topic=self.topic_name, bytes_to_produce=batch_bytes)

        # Wait for first batch to be reconciled to metastore
        self.logger.info("Waiting for first batch reconciliation to L1 metastore")
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        # Produce second batch of data
        self.logger.info(f"Producing second batch: {batch_bytes} bytes")
        self._produce(topic=self.topic_name, bytes_to_produce=batch_bytes)

        # Wait for second batch to be reconciled to metastore
        self.logger.info("Waiting for second batch reconciliation to L1 metastore")
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        # Verify initial state - no data should be deleted yet
        part_before = self._get_partition_info(self.topic_name, partition=0)
        self.logger.info(
            f"Initial state: start_offset={part_before.start_offset}, "
            f"hwm={part_before.high_watermark}"
        )
        assert part_before.start_offset == 0, (
            f"Expected start_offset=0 with large retention, got {part_before.start_offset}"
        )

        # Alter retention.bytes to trigger deletion
        self.logger.info(
            f"Altering retention.bytes to {final_retention} to trigger deletion"
        )
        rpk.alter_topic_config(self.topic_name, "retention.bytes", str(final_retention))

        # Wait for retention to apply
        self.logger.info("Waiting for retention to apply")
        self._wait_for_retention_to_apply(topic=self.topic_name, partition=0)

        # Verify final state
        part_after = self._get_partition_info(self.topic_name, partition=0)
        self.logger.info(
            f"Final state: start_offset={part_after.start_offset}, "
            f"hwm={part_after.high_watermark}"
        )

        # Verify retention worked: old data was deleted
        assert part_after.start_offset > 0, (
            "Retention should have advanced start_offset"
        )

        # Verify offset boundaries
        # Old offset (0) should not be consumable
        self._verify_offset_not_consumable(self.topic_name, offset=0)

        # New start offset should be consumable
        self._verify_offset_consumable(self.topic_name, offset=part_after.start_offset)

        self.logger.info(
            f"Size-based retention test passed: "
            f"start_offset advanced from 0 to {part_after.start_offset}"
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(),
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_time_based_retention(
        self, cloud_storage_type: CloudStorageType, storage_mode: str
    ):
        """
        Test that time-based retention (retention.ms) correctly deletes
        old data from cloud topics based on message timestamps.
        """
        # Configuration - start with 3000 messages like size-based test
        num_messages = 3000
        total_bytes = num_messages * 1024  # 3000KB

        # Create cloud topic with large retention.ms initially
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic=self.topic_name,
            partitions=1,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                "retention.ms": str(3600000),  # 1 hour - data won't be deleted yet
            },
        )

        # Produce data
        self.logger.info(f"Producing {total_bytes} bytes")
        self._produce(topic=self.topic_name, bytes_to_produce=total_bytes)

        # Wait for data to be reconciled to metastore
        self.logger.info("Waiting for reconciliation to L1 metastore")
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        # Verify initial state - no data should be deleted yet
        part_before = self._get_partition_info(self.topic_name, partition=0)
        self.logger.info(
            f"Initial state: start_offset={part_before.start_offset}, "
            f"hwm={part_before.high_watermark}"
        )
        assert part_before.start_offset == 0, (
            f"Expected start_offset=0 with large retention, got {part_before.start_offset}"
        )

        # Waits until the the partition size reaches a reported positive sign
        ct_utils.wait_until_l1_partition_size(
            self.admin, self.topic_name, 0, lambda size: size > 0
        )

        # Alter retention.ms to trigger time-based deletion
        self.logger.info("Setting retention.ms to 10ms to trigger deletion")
        rpk.alter_topic_config(self.topic_name, TopicSpec.PROPERTY_RETENTION_TIME, "10")

        # Wait for retention to apply
        self.logger.info("Waiting for time-based retention to apply")
        self._wait_for_retention_to_apply(topic=self.topic_name, partition=0)

        # Verify final state
        part_after = self._get_partition_info(self.topic_name, partition=0)
        self.logger.info(
            f"Final state: start_offset={part_after.start_offset}, "
            f"hwm={part_after.high_watermark}"
        )

        # Verify retention worked: old data was deleted
        assert part_after.start_offset > 0, (
            "Time-based retention should have advanced start_offset"
        )

        self._verify_offset_not_consumable(self.topic_name, offset=0)

        def all_data_expired() -> bool:
            part = self._get_partition_info(self.topic_name, partition=0)
            return part.high_watermark == part.start_offset

        def failed_to_expire_all_data() -> str:
            part = self._get_partition_info(self.topic_name, partition=0)
            return f"failed to remove all data from {self.topic_name}: hwm={part.high_watermark}, start_offset={part.start_offset}"

        self.logger.info("Waiting for all data to be expired")
        wait_until(
            all_data_expired,
            timeout_sec=30,
            backoff_sec=3,
            err_msg=failed_to_expire_all_data,
        )

        # Waits until the the partition size reaches a reported size of 0
        ct_utils.wait_until_l1_partition_size(
            self.admin, self.topic_name, 0, lambda size: size == 0
        )
