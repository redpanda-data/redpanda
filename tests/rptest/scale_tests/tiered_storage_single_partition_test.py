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
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.redpanda import SISettings, MetricsEndpoint
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.utils.si_utils import BucketView, quiesce_uploads, NTP
from ducktape.mark import parametrize
import time


class TieredStorageSinglePartitionTest(RedpandaTest):
    log_segment_size = 128 * 1024 * 1024
    segment_upload_interval = 30
    manifest_upload_interval = 10

    topics = (TopicSpec(replication_factor=3, partition_count=1), )

    def __init__(self, test_context, *args, **kwargs):
        kwargs['si_settings'] = SISettings(
            test_context=test_context,
            log_segment_size=self.log_segment_size,
        )

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
            # Ensure spillover can happen promptly during test
            'cloud_storage_housekeeping_interval_ms': 5000,
            # But allow the test runtime to configure it.
            'cloud_storage_spillover_manifest_size': None,
        }
        super().__init__(test_context, *args, **kwargs)

    @cluster(num_nodes=4)
    @parametrize(restart_stress=True)
    @parametrize(restart_stress=False)
    def throughput_test(self, restart_stress):
        """
        Stress the tiered storage upload path on a single partition.  This
        corresponds to a workload in which the user does not create many
        partitions, and consequently will upload segments very frequently
        from a single partition.

        We are looking to ensure that the uploads keep up, and that the
        manifest uploads are done efficiently (e.g. not re-uploading
        the manifest with each segment).

        :param restart_stress: if true, additionally restart nodes during
                               writes, and waive throughput success conditions:
                               in this mode we are checking for stability and
                               data durability.
        """

        rpk = RpkTool(self.redpanda)

        produce_byte_rate = 50 * 1024 * 1024

        # Large messages, this is a bandwith test for uploads, we do not
        # want CPU handling of small records to be a bottleneck
        msg_size = 32768

        # Enough data to run for >5min to see some kind of steady state
        target_runtime = 300
        write_bytes = produce_byte_rate * target_runtime

        # A high throughput partition will tend to write enough metadata to trigger
        # metadata spilling.  For the purposes of a short running test, set a low
        # spill threshold so that this happens during the test.
        expect_segments = write_bytes // self.log_segment_size
        spillover_segments = max(expect_segments // 10, 1)
        self.logger.info(
            f"Configuring to spill metadata after {spillover_segments} segments"
        )
        self.redpanda.set_cluster_config({
            'cloud_storage_spillover_manifest_size':
            None,
            'cloud_storage_spillover_manifest_max_segments':
            spillover_segments
        })

        # We will consume data back at the end: set a large enough cache size
        # to accommodate the streaming bandwidth
        self.redpanda.set_cluster_config({
            'cloud_storage_cache_size':
            # Enough cache space to consume twice as fast as we produced
            SISettings.cache_size_for_throughput(produce_byte_rate * 2)
        })

        # Set local retention to half the write quantity, so that when we eventually
        # read data back, it'll be half remote storage and half local storage
        rpk.alter_topic_config(self.topic, 'retention.local.target.bytes',
                               str(write_bytes // 2))

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

        if restart_stress:
            while producer.produce_status.acked < msg_count:
                time.sleep(5)
                for node in self.redpanda.nodes:
                    self.redpanda.restart_nodes([node])

            def metadata_readable():
                return next(rpk.describe_topic(
                    self.topic, tolerant=True)).high_watermark is not None

            # Wait for the cluster to recover after final restart, so that subsequent
            # post-stress success conditions can count on their queries succeeding.
            self.redpanda.wait_until(metadata_readable,
                                     timeout_sec=30,
                                     backoff_sec=1)

        producer.wait(timeout_sec=expect_duration)
        producer.free()

        produce_duration = time.time() - t1
        actual_byte_rate = (write_bytes / produce_duration)
        mbps = int(actual_byte_rate / (1024 * 1024))
        self.logger.info(
            f"Produced {write_bytes} in {produce_duration}s, {mbps}MiB/s")

        # Producer should be within a factor of two of the intended byte rate, or something
        # is wrong with the test (running on nodes that can't keep up?) or with Redpanda
        # (some instability interrupted produce?)
        if not restart_stress:
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
        uploaded_ts = max(s['max_timestamp']
                          for s in manifest['segments'].values())
        self.logger.info(f"Max uploaded ts = {uploaded_ts}")

        lag_seconds = (local_ts - uploaded_ts) / 1000.0
        self.logger.info(f"Upload lag: {lag_seconds}s")
        assert lag_seconds < (self.manifest_upload_interval +
                              (self.log_segment_size / actual_byte_rate))

        # Wait for all uploads to complete: this should take roughly segment_max_upload_interval_sec
        # plus manifest_max_upload_interval_sec
        quiesce_uploads(self.redpanda, [self.topic],
                        timeout_sec=self.manifest_upload_interval +
                        self.segment_upload_interval)

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
        # - 1 per segment uploaded
        # - 1 per upload round (i.e. per 1-4 segments) for marking the manifest
        #   clean.
        # - 1 per upload round (i.e. per 1-4 segments) for updating the highest
        #   producer ID.
        # - 1 per raft term, for raft config batches written by new leaders
        #
        # This helps to assure us that ntp_archiver and archival_metadata_stm
        # are doing an efficient job of keeping the state machine up to date.
        manifest = bucket.manifest_for_ntp(self.topic, 0)
        top_segment = list(manifest['segments'].values())[-1]
        offset_delta = top_segment['delta_offset_end']
        segment_count = len(manifest['segments'])

        spill_manifests = bucket.get_spillover_manifests(
            NTP(ns='kafka', topic=self.topic, partition=0))
        if spill_manifests is not None:
            for spill_meta, spill_manifest in spill_manifests.items():
                segment_count += len(spill_manifest['segments'])

        last_term = top_segment['segment_term']
        self.logger.info(
            f"Delta: {offset_delta}, Segments: {segment_count}, Last term {last_term}"
        )

        # If we upload in batches of four segments at a time
        min_offset_delta = (segment_count * 1.5) + last_term

        # If we upload one segment at a time
        max_offset_delta = (segment_count * 3) + last_term
        assert min_offset_delta <= offset_delta <= max_offset_delta

        # +3 because:
        # - 1 empty upload at start
        # - 1 extra upload from runtime % upload interval
        # - 1 extra upload after the final interval_sec driven uploads
        upload_intervals_elapsed = int(
            produce_duration) // self.manifest_upload_interval
        expect_manifest_uploads = len(
            spill_manifests) + upload_intervals_elapsed + 3
        self.logger.info(
            f"Spill manifests: {len(spill_manifests)}, upload intervals elapsed: {upload_intervals_elapsed}"
        )

        assert manifest_uploads <= expect_manifest_uploads, f"Too many manifest uploads: {manifest_uploads} > {expect_manifest_uploads}"

        # No point writing data if we can't read it back!  Wrap up the test by
        # validating all data.
        consumer = KgoVerifierSeqConsumer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic,
            loop=False,
            timeout_sec=produce_duration * throughput_tolerance_factor)
        assert consumer.consumer_status.validator.valid_reads == msg_count
        assert consumer.consumer_status.validator.invalid_reads == 0
        assert consumer.consumer_status.validator.out_of_scope_invalid_reads == 0
