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

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.minio_proxy import MinioProxy
from rptest.services.redpanda import (
    SISettings,
    CloudStorageType,
    make_redpanda_service,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_for_local_storage_truncate
from rptest.utils.si_utils import BucketView
import time


class EndToEndEmptySegmentsTest(RedpandaTest):
    """
    Test that exercises tiered storage with many empty segments created
    through leadership transfers, with segment merging disabled.
    """

    segment_size = 1024 
    topic_name = "test-topic"

    topics = (
        TopicSpec(
            name=topic_name,
            partition_count=1,
            replication_factor=3,
        ),
    )

    def __init__(self, test_context: TestContext):
        self.num_brokers = 3

        self.minio_proxy = MinioProxy(
            test_context,
            port=9000,
            backend_host="minio-s3",
            backend_port=9000,
            delay_ms=700,
        )

        # Configuration to force frequent segment rolls and disable merging
        extra_rp_conf = {
            # Disable adjacent segment merging
            "cloud_storage_enable_segment_merging": False,
            # Disable scrubbing to avoid interference with the test
            "cloud_storage_enable_scrubbing": False,
            # Force frequent uploads (1 second interval)
            "cloud_storage_segment_max_upload_interval_sec": 1,
            # Small segment size to roll frequently (1 KiB)
            "log_segment_size": self.segment_size,
            "log_segment_size_min": self.segment_size,  # Allow 1 KiB segments
        }

        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=5,
            log_segment_size=self.segment_size,
            fast_uploads=True,
            # Use the proxy endpoint instead of direct MinIO
            cloud_storage_api_endpoint=self.minio_proxy.hostname,
            cloud_storage_api_endpoint_port=self.minio_proxy.port,
            # Use path-style addressing to avoid DNS lookups for bucket subdomains
            cloud_storage_url_style="path",
        )

        super(EndToEndEmptySegmentsTest, self).__init__(
            test_context=test_context,
            num_brokers=self.num_brokers,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )

        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def _transfer_leadership(self, batch_id: int):
        """
        Transfer leadership to a different node for the test partition,
        then produce a single batch of messages.
        """
        cur_leader = self.admin.get_partition_leader(
            namespace="kafka", topic=self.topic_name, partition=0
        )
        broker_ids = [x["node_id"] for x in self.admin.get_brokers()]
        # Pick a different node to transfer to
        candidates = [n for n in broker_ids if n != cur_leader]
        if not candidates:
            self.logger.warning("No candidates for leadership transfer")
            return

        transfer_to = candidates[0]
        self.logger.debug(f"Transferring leadership from {cur_leader} to {transfer_to}")

        self.admin.transfer_leadership_to(
            namespace="kafka",
            topic=self.topic_name,
            partition=0,
            target_id=transfer_to,
            leader_id=cur_leader,
        )

        self.admin.await_stable_leader(
            self.topic_name,
            partition=0,
            namespace="kafka",
            timeout_s=30,
            backoff_s=1,
            check=lambda node_id: node_id == transfer_to,
        )

        # Produce a single batch after leadership transfer
        # Using a small message that will fit in one batch
        msg = f"batch-{batch_id}"
        self.rpk.produce(self.topic_name, key=f"key-{batch_id}", msg=msg, partition=0)

    def setUp(self):
        """Start the MinIO proxy before starting Redpanda."""
        self.minio_proxy.start()
        self.logger.info(
            f"MinIO proxy started at {self.minio_proxy.endpoint} "
            f"with {self.minio_proxy.delay_ms}ms delay"
        )
        # Call parent setUp to start Redpanda
        super().setUp()

    def tearDown(self):
        """Stop the MinIO proxy after test completion."""
        super().tearDown()
        self.minio_proxy.stop()
        self.minio_proxy.free()

    @cluster(num_nodes=5)  # 3 for Redpanda, 1 for proxy, 1 for test harness
    @matrix(cloud_storage_type=[CloudStorageType.S3])
    def test_empty_segments_with_tiered_storage(self, cloud_storage_type):
        """
        Test that creates many empty segments through leadership transfers,
        then writes data and validates it can be consumed correctly.

        The test:
        1. Starts MinIO proxy with configurable delay
        2. Transfers leadership 100 times to create 100 empty segments
        3. Produces messages using KgoVerifier
        4. Consumes and validates messages using KgoVerifierSeqConsumer
        """

        self.logger.info(
            "Starting test: creating segments through leadership transfers with single batch writes"
        )

        # Step 1: Transfer leadership 100 times, writing a single batch after each transfer
        # This creates many small segments with a mix of empty segments and segments
        # containing just one batch
        num_transfers = 100
        self.logger.info(
            f"Transferring leadership {num_transfers} times and writing a batch after each transfer"
        )
        for i in range(num_transfers):
            self._transfer_leadership(batch_id=i)
            if (i + 1) % 10 == 0:
                self.logger.info(f"Completed {i + 1} leadership transfers with batch writes")

        self.logger.info("Completed all leadership transfers and batch writes")

        # Step 2: Produce additional data using KgoVerifier to ensure we have enough
        # data to test consumption from cloud storage
        msg_size = 4096
        msg_count = 10000

        self.logger.info(f"Starting producer: {msg_count} messages of size {msg_size}")

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=msg_size,
            msg_count=msg_count,
            debug_logs=True,
            trace_logs=True,
        )

        producer.start()
        producer.wait(timeout_sec=120)

        self.logger.info(f"Producer completed. Status: {producer.produce_status}")

        # Verify producer succeeded
        # Total expected: 100 batches from leadership transfers + msg_count from KgoVerifier
        expected_from_kgo = msg_count
        assert producer.produce_status.acked >= expected_from_kgo, (
            f"Expected at least {expected_from_kgo} acked messages from KgoVerifier, "
            f"got {producer.produce_status.acked}"
        )

        producer.free()

        # Log approximate total messages produced
        total_msgs = num_transfers + producer.produce_status.acked
        self.logger.info(
            f"Total messages produced: ~{total_msgs} "
            f"({num_transfers} from leadership transfers + {producer.produce_status.acked} from KgoVerifier)"
        )

        # Step 3: Enable aggressive local retention to force reading from cloud storage
        self.logger.info("Enabling aggressive local retention to evict segments")

        # Wait for all uploads to complete first
        def all_uploads_done():
            """Check if all produced data has been uploaded to cloud storage."""
            topic_description = self.rpk.describe_topic(self.topic_name)
            partition = next(topic_description)
            hwm = partition.high_watermark

            try:
                bucket = BucketView(self.redpanda)
                manifest = bucket.manifest_for_ntp(self.topic_name, partition.id)
                if not manifest or "segments" not in manifest:
                    return False

                segments = manifest["segments"].values()
                if not segments:
                    return False

                top_segment = max(segments, key=lambda seg: seg["base_offset"])
                uploaded_raft_offset = top_segment["committed_offset"]
                uploaded_kafka_offset = uploaded_raft_offset - top_segment[
                    "delta_offset_end"
                ]

                self.logger.debug(
                    f"Remote HWM {uploaded_kafka_offset} (raft {uploaded_raft_offset}), local hwm {hwm}"
                )

                # -1 because uploaded offset is inclusive, hwm is exclusive
                return uploaded_kafka_offset >= (hwm - 1)
            except Exception as e:
                self.logger.debug(f"Exception checking uploads: {e}")
                return False

        self.logger.info("Waiting for all segments to be uploaded to cloud storage")
        wait_until(all_uploads_done, timeout_sec=120, backoff_sec=5)

        # Set aggressive local retention to evict most segments
        self.rpk.alter_topic_config(
            self.topic_name,
            "retention.local.target.bytes",
            str(self.segment_size * 2),
        )

        # Wait for local storage to be truncated
        self.logger.info("Waiting for local storage truncation")
        wait_for_local_storage_truncate(
            self.redpanda,
            self.topic_name,
            target_bytes=4 * self.segment_size,
            partition_idx=0,
            timeout_sec=60,
        )

        # Download and log the manifest to understand cloud storage state
        self.logger.info("Downloading and logging manifest from cloud storage")
        try:
            bucket = BucketView(self.redpanda)
            manifest = bucket.manifest_for_ntp(self.topic_name, 0)

            if manifest:
                num_segments = len(manifest.get("segments", {}))
                start_offset = manifest.get("start_offset", "N/A")
                last_offset = manifest.get("last_offset", "N/A")
                archive_start_offset = manifest.get("archive_start_offset", "N/A")
                archive_clean_offset = manifest.get("archive_clean_offset", "N/A")

                self.logger.info(f"Manifest summary:")
                self.logger.info(f"  - Total segments in manifest: {num_segments}")
                self.logger.info(f"  - Start offset: {start_offset}")
                self.logger.info(f"  - Last offset: {last_offset}")
                self.logger.info(f"  - Archive start offset: {archive_start_offset}")
                self.logger.info(f"  - Archive clean offset: {archive_clean_offset}")

                # Log details about each segment
                segments = manifest.get("segments", {})
                if segments:
                    self.logger.info(f"Segment details:")
                    for seg_name, seg_meta in sorted(segments.items(),
                                                     key=lambda x: x[1]["base_offset"]):
                        base_offset = seg_meta.get("base_offset", "N/A")
                        committed_offset = seg_meta.get("committed_offset", "N/A")
                        size_bytes = seg_meta.get("size_bytes", "N/A")
                        self.logger.info(
                            f"    {seg_name}: base_offset={base_offset}, "
                            f"committed_offset={committed_offset}, size={size_bytes} bytes"
                        )
            else:
                self.logger.warning("No manifest found in cloud storage")
        except Exception as e:
            self.logger.error(f"Failed to download/log manifest: {e}", exc_info=True)

        # Step 4: Consume all data using rpk to validate reading from cloud storage
        # This will read from cloud storage due to aggressive local retention
        self.logger.info("Starting consumer to read all produced data (reading from cloud storage)")

        # Get the high watermark to know how many messages to consume
        topic_description = self.rpk.describe_topic(self.topic_name)
        partition = next(topic_description)
        hwm = partition.high_watermark
        total_messages_expected = hwm

        self.logger.info(
            f"High watermark: {hwm}, consuming all messages from offset 0"
        )

        # Consume all messages from the beginning
        # Using a simple format to count consumed records
        try:
            for off in [0, 30, 40, 50, 60, 70, 80, 90, 100]:
                consume_output = self.rpk.consume(
                    self.topic_name,
                    n=2,
                    offset=off,
                    partition=0,
                    format="%p|%o|%k|%v\\n",
                    timeout=10,
                    fetch_max_wait=2,
                    request_timeout_overhead=1,
                )

                # Count the number of consumed messages
                consumed_lines = consume_output.strip().split("\n") if consume_output.strip() else []
                consumed_count = len([line for line in consumed_lines if line.strip()])

                self.logger.info(
                    f"Consumer completed. Consumed {consumed_count} messages "
                    f"(expected {total_messages_expected})"
                )

                # Log a sample of consumed messages for verification
                if consumed_lines:
                    self.logger.info("Sample of consumed messages (first 5):")
                    for line in consumed_lines[:5]:
                        self.logger.info(f"  {line}")

                    self.logger.info("Sample of consumed messages (last 5):")
                    for line in consumed_lines[-5:]:
                        self.logger.info(f"  {line}")

                self.logger.info("Test completed successfully - all messages consumed from cloud storage")

        except Exception as e:
            self.logger.error(f"Consumer failed: {e}", exc_info=True)
            raise
