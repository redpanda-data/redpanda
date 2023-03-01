# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings, MetricsEndpoint
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.utils.si_utils import BucketView
import time


class TieredStorageSinglePartitionTest(RedpandaTest):
    log_segment_size = 128 * 1024 * 1024
    segment_upload_interval = 30
    manifest_upload_interval = 10

    topics = (TopicSpec(replication_factor=3, partition_count=1), )

    def __init__(self, test_context, *args, **kwargs):
        self.si_settings = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
        )
        kwargs['si_settings'] = self.si_settings

        # Use interval uploads so that at end of test we may do an "everything
        # was uploaded" success condition.
        kwargs['extra_rp_conf'] = {
            # We do not intend to do interval-triggered uploads during produce,
            # but we set this property so that at the end of the test we may
            # do a simple "everything was uploaded" check after the interval.
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval,
            # The test will assert that the number of manifest uploads does
            # not exceed what we would expect based on this interval.
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval,
        }
        super().__init__(test_context, *args, **kwargs)

    @cluster(num_nodes=4)
    def throughput_test(self):
        """
        Stress the tiered storage upload path on a single partition.  This
        corresponds to a workload in which the user does not create many
        partitions, and consequently will upload segments very frequently
        from a single partition.

        We are looking to ensure that the uploads keep up, and that the
        manifest uploads are done efficiently (e.g. not re-uploading
        the manifest with each segment).
        """

        rpk = RpkTool(self.redpanda)

        produce_byte_rate = 50 * 1024 * 1024

        # Large messages, this is a bandwith test for uploads, we do not
        # want CPU handling of small records to be a bottleneck
        msg_size = 32768

        # Enough data to run for >5min to see some kind of steady state
        target_runtime = 300
        write_bytes = produce_byte_rate * target_runtime

        msg_count = write_bytes // msg_size

        # The producer should achieve throughput within this factor of what we asked for:
        # if this is violated then it is something wrong with the client or test environment.
        throughput_tolerance_factor = 2

        expect_duration = (write_bytes //
                           produce_byte_rate) * throughput_tolerance_factor

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       batch_max_bytes=512 * 1024,
                                       rate_limit_bps=produce_byte_rate)

        self.logger.info(f"Producing {msg_count} msgs ({write_bytes} bytes)")
        t1 = time.time()
        producer.start()
        producer.wait(timeout_sec=expect_duration)
        produce_duration = time.time() - t1
        actual_byte_rate = (write_bytes / produce_duration)
        mbps = int(actual_byte_rate / (1024 * 1024))
        self.logger.info(
            f"Produced {write_bytes} in {produce_duration}s, {mbps}MiB/s")

        # Producer should be within a factor of two of the intended byte rate, or something
        # is wrong with the test (running on nodes that can't keep up?) or with Redpanda
        # (some instability interrupted produce?)
        assert actual_byte_rate > produce_byte_rate / throughput_tolerance_factor
        # Check the workload is respecting rate limit
        assert actual_byte_rate < produce_byte_rate * throughput_tolerance_factor

        # Read the highest timestamp in local storage
        partition_describe = next(rpk.describe_topic(self.topic,
                                                     tolerant=True))
        hwm = partition_describe.high_watermark
        assert hwm >= msg_count
        consume_out = rpk.consume(topic=self.topic,
                                  n=1,
                                  offset=hwm - 1,
                                  partition=0,
                                  format="%d\\n")
        local_ts = int(consume_out.strip())
        self.logger.info(f"Max local ts = {local_ts}")

        # Measure how far behind the tiered storage uploads are: success condition
        # should be that they are within some time range of the most recently
        # produced data
        bucket = BucketView(self.redpanda)
        manifest = bucket.manifest_for_ntp(self.topic, 0)
        uploaded_ts = list(manifest['segments'].values())[-1]['max_timestamp']
        self.logger.info(f"Max uploaded ts = {uploaded_ts}")

        lag_seconds = (local_ts - uploaded_ts) / 1000.0
        self.logger.info(f"Upload lag: {lag_seconds}s")
        assert lag_seconds < (self.manifest_upload_interval +
                              (self.log_segment_size / actual_byte_rate))

        # Wait for all uploads to complete: this should take roughly segment_max_upload_interval_sec
        # plus manifest_max_upload_interval_sec
        def all_uploads_done():
            bucket.reset()
            manifest = bucket.manifest_for_ntp(self.topic, 0)
            top_segment = list(manifest['segments'].values())[-1]
            uploaded_ts = top_segment['max_timestamp']
            self.logger.info(f"Remote ts {uploaded_ts}, local ts {local_ts}")
            uploaded_raft_offset = top_segment['committed_offset']
            uploaded_kafka_offset = uploaded_raft_offset - top_segment[
                'delta_offset_end']
            self.logger.info(
                f"Remote HWM {uploaded_kafka_offset} (raft {uploaded_raft_offset}), local hwm {hwm}"
            )

            # -1 because uploaded offset is inclusive, hwm is exclusive
            return uploaded_kafka_offset >= (hwm - 1)

        self.redpanda.wait_until(all_uploads_done,
                                 timeout_sec=self.manifest_upload_interval +
                                 self.segment_upload_interval,
                                 backoff_sec=5)

        # Check manifest upload metrics:
        #  - we should not have uploaded the manifest more times
        #    then there were manifest upload intervals in the runtime.
        manifest_uploads = self.redpanda.metric_sum(
            metric_name=
            "redpanda_cloud_storage_partition_manifest_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        segment_uploads = self.redpanda.metric_sum(
            metric_name="redpanda_cloud_storage_segment_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        self.logger.info(
            f"Upload counts: {manifest_uploads} manifests, {segment_uploads} segments"
        )
        assert manifest_uploads > 0
        assert segment_uploads > 0

        # Check the kafka-raft offset delta: this is an indication of how
        # many extra raft records we wrote for archival_metadata_stm.
        #
        # At the maximum, this should be:
        # - 1 per segment (although could be as low as 1 for every four segments
        #   due to ntp_archiver::_concurrency)
        # - 1 per upload round (i.e. per 1-4 segments) for marking the manifest
        #   clean.
        # - 1 per raft term, for raft config batches written by new leaders
        #
        # This helps to assure us that ntp_archiver and archival_metadata_stm
        # are doing an efficient job of keeping the state machine up to date.
        manifest = bucket.manifest_for_ntp(self.topic, 0)
        top_segment = list(manifest['segments'].values())[-1]
        offset_delta = top_segment['delta_offset_end']
        segment_count = len(manifest['segments'])
        last_term = top_segment['segment_term']
        self.logger.info(
            f"Delta: {offset_delta}, Segments: {segment_count}, Last term {last_term}"
        )
        assert offset_delta <= (2 * segment_count + last_term)

        # +3 because:
        # - 1 empty upload at start
        # - 1 extra upload from runtime % upload interval
        # - 1 extra upload after the final interval_sec driven uploads
        expect_manifest_uploads = (
            (int(produce_duration) // self.manifest_upload_interval) + 3)

        assert manifest_uploads <= expect_manifest_uploads
