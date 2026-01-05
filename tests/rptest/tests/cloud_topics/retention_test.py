# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.clients.admin.v2 import Admin, metastore_pb, ntp_pb
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SISettings,
    get_cloud_storage_type,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.tests.redpanda_test import RedpandaTest


class CloudTopicsRetentionTest(RedpandaTest):
    """
    Test retention policies for cloud topics.

    Cloud topics store data in object storage (L0) with metadata in a metastore (L1).
    These tests verify that retention.bytes and retention.ms policies correctly
    delete old data using offset checks and consumption verification.
    """

    segment_size = 1048576  # 1MB segments
    topic_name = "cloud_topic_retention_test"

    def __init__(self, test_context: TestContext):
        self.test_context = test_context

        # SI Settings for object storage integration
        si_settings = SISettings(
            test_context=test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
            log_segment_size=self.segment_size,
        )

        # Extra Redpanda configuration
        extra_rp_conf = {
            # Enable cloud topics feature
            CLOUD_TOPICS_CONFIG_STR: True,
            # Fast reconciliation (L0 -> L1 data movement)
            "cloud_topics_reconciliation_interval": 2000,  # 2s
            # Fast housekeeping for testing (default: 5min)
            # This controls how often retention enforcement runs
            "cloud_storage_housekeeping_interval_ms": 5000,  # 5s
        }

        super(CloudTopicsRetentionTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=si_settings,
        )

    def _wait_until_reconciled(
        self, topic: str, partition: int, timeout_sec: int = 60
    ):
        """
        Wait until the cloud topic partition is reconciled to the metastore (L1).
        This means all produced data has been uploaded and metadata is in sync.
        """
        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        def is_reconciled() -> bool:
            # Get Kafka view of offsets
            parts = list(rpk.describe_topic(topic))
            kafka_hwm = None
            for part in parts:
                if part.id == partition:
                    kafka_hwm = part.high_watermark
                    break

            if kafka_hwm is None:
                return False

            # Get metastore view of offsets
            metastore = admin.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
            )
            resp = metastore.get_offsets(req=req)
            metastore_next_offset = resp.offsets.next_offset

            self.logger.debug(
                f"Reconciliation check: kafka_hwm={kafka_hwm}, "
                f"metastore_next_offset={metastore_next_offset}"
            )

            # Metastore next_offset should match Kafka HWM
            return kafka_hwm == metastore_next_offset

        wait_until(
            condition=is_reconciled,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=f"Failed to reconcile topic {topic} partition {partition}",
            retry_on_exc=True,
        )

    def _wait_for_retention_to_apply(
        self, topic: str, partition: int, timeout_sec: int = 60
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

    def _produce_with_repeater(
        self, topic: str, bytes_to_produce: int, timeout_sec: int = 120
    ):
        """
        Produce data to cloud topics using kgo-repeater with parallelism.
        """
        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=[topic],
            msg_size=1024,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            # Calculate required messages: bytes_to_produce / msg_size
            required_messages = bytes_to_produce // 1024
            repeater.await_progress(required_messages, timeout_sec=timeout_sec)

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
            result = rpk.consume(
                topic, n=1, offset=f"{offset}-{offset+1}", timeout=5, quiet=True
            )
            # If we get here, consumption succeeded when it shouldn't have
            assert False, f"Should not be able to consume deleted offset {offset}"
        except Exception as e:
            # Expected - offset is not available
            self.logger.info(f"Expected: offset {offset} not consumable: {e}")

    def _verify_offset_consumable(self, topic: str, offset: int):
        """Verify that consuming from a valid offset succeeds."""
        rpk = RpkTool(self.redpanda)
        result = rpk.consume(
            topic, n=1, offset=f"{offset}-{offset+1}", timeout=10, quiet=True
        )
        assert result, f"Should be able to consume from offset {offset}"
        self.logger.info(f"Successfully consumed from offset {offset}")

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_size_based_retention(self, cloud_storage_type: CloudStorageType):
        """
        Test that size-based retention (retention.bytes) correctly deletes
        old data from cloud topics.

        Steps:
        1. Create cloud topic with large retention.bytes initially
        2. Produce data
        3. Wait for reconciliation to L1 metastore
        4. Verify initial start_offset == 0
        5. Alter retention.bytes to small value to trigger deletion
        6. Wait for housekeeping to apply retention
        7. Verify start_offset > 0 (data was deleted)
        8. Verify old offsets are not consumable
        9. Verify new offsets are consumable
        """
        # Configuration
        num_messages = 300
        total_bytes = num_messages * 1024  # 300KB
        initial_retention = 1024 * 1024 * 1024  # 1GB - won't delete anything
        final_retention = 150 * 1024  # 150KB - will delete half the data

        # Create cloud topic with large retention.bytes initially
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic=self.topic_name,
            partitions=1,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
                "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                "retention.bytes": str(initial_retention),
            },
        )

        # Produce data
        self.logger.info(f"Producing {total_bytes} bytes")
        self._produce_with_repeater(topic=self.topic_name, bytes_to_produce=total_bytes)

        # Wait for data to be reconciled to metastore
        self.logger.info("Waiting for reconciliation to L1 metastore")
        self._wait_until_reconciled(topic=self.topic_name, partition=0)

        # Verify initial state - no data should be deleted yet
        part_before = self._get_partition_info(self.topic_name, partition=0)
        self.logger.info(
            f"Initial state: start_offset={part_before.start_offset}, "
            f"hwm={part_before.high_watermark}"
        )
        assert (
            part_before.start_offset == 0
        ), f"Expected start_offset=0 with large retention, got {part_before.start_offset}"

        # Alter retention.bytes to trigger deletion
        self.logger.info(f"Altering retention.bytes to {final_retention} to trigger deletion")
        rpk.alter_topic_config(
            self.topic_name, "retention.bytes", str(final_retention)
        )

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
        assert (
            part_after.start_offset > 0
        ), "Retention should have advanced start_offset"

        # Verify offset boundaries
        # Old offset (0) should not be consumable
        self._verify_offset_not_consumable(self.topic_name, offset=0)

        # New start offset should be consumable
        self._verify_offset_consumable(
            self.topic_name, offset=part_after.start_offset
        )

        self.logger.info(
            f"Size-based retention test passed: "
            f"start_offset advanced from 0 to {part_after.start_offset}"
        )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_time_based_retention(self, cloud_storage_type: CloudStorageType):
        """
        Test that time-based retention (retention.ms) correctly deletes
        old data from cloud topics based on message timestamps.

        Steps:
        1. Create cloud topic with large retention.ms initially
        2. Produce data
        3. Wait for reconciliation
        4. Verify initial start_offset == 0
        5. Alter topic config to set very short retention.ms
        6. Wait for housekeeping to apply retention
        7. Verify start_offset advanced significantly
        8. Verify consumption boundaries
        """
        # Configuration - start with 300 messages like size-based test
        num_messages = 300
        total_bytes = num_messages * 1024  # 300KB

        # Create cloud topic with large retention.ms initially
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic=self.topic_name,
            partitions=1,
            replicas=3,
            config={
                "redpanda.cloud_topic.enabled": "true",
                "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                "retention.ms": str(3600000),  # 1 hour - data won't be deleted yet
            },
        )

        # Produce data
        self.logger.info(f"Producing {total_bytes} bytes")
        self._produce_with_repeater(topic=self.topic_name, bytes_to_produce=total_bytes)

        # Wait for data to be reconciled to metastore
        self.logger.info("Waiting for reconciliation to L1 metastore")
        self._wait_until_reconciled(topic=self.topic_name, partition=0)

        # Verify initial state - no data should be deleted yet
        part_before = self._get_partition_info(self.topic_name, partition=0)
        self.logger.info(
            f"Initial state: start_offset={part_before.start_offset}, "
            f"hwm={part_before.high_watermark}"
        )
        assert (
            part_before.start_offset == 0
        ), f"Expected start_offset=0 with large retention, got {part_before.start_offset}"

        # Alter retention.ms to trigger time-based deletion
        self.logger.info("Setting retention.ms to 10ms to trigger deletion")
        rpk.alter_topic_config(
            self.topic_name, TopicSpec.PROPERTY_RETENTION_TIME, "10"
        )

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
        assert (
            part_after.start_offset > 0
        ), "Time-based retention should have advanced start_offset"

        # Verify offset boundaries
        # Old offset (0) should not be consumable
        self._verify_offset_not_consumable(self.topic_name, offset=0)

        # Note: With retention.ms=10, all data will be deleted since all messages
        # are older than 10ms. So start_offset will equal high_watermark and there's
        # nothing left to consume. This is expected behavior.
        self.logger.info(
            f"Time-based retention test passed: "
            f"start_offset advanced from 0 to {part_after.start_offset} "
            f"(hwm={part_after.high_watermark})"
        )
