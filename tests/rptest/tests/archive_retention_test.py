# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import sleep
import time
from ducktape.errors import TimeoutError
from ducktape.mark import matrix, parametrize
from ducktape.utils.util import wait_until

from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings, MetricsEndpoint, get_cloud_storage_type
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (produce_until_segments, produce_total_bytes,
                         wait_for_local_storage_truncate, segments_count,
                         expect_exception)
from rptest.utils.si_utils import BucketView, NTP, quiesce_uploads


class CloudArchiveRetentionTest(RedpandaTest):
    """Test retention with spillover enabled"""
    def __init__(self, test_context):
        self.extra_rp_conf = dict(
            log_compaction_interval_ms=1000,
            cloud_storage_spillover_manifest_max_segments=5,
            cloud_storage_spillover_manifest_size=None,
            cloud_storage_enable_segment_merging=False)

        self.next_base_timestamp = 1664453149000  # arbitrary value
        self.timestamp_step_ms = 1000 * 60 * 2  # 2 minutes

        si_settings = SISettings(
            test_context,
            log_segment_size=1024 * 1024,  # 1 MiB
            cloud_storage_housekeeping_interval_ms=1000,
            fast_uploads=True)

        super(CloudArchiveRetentionTest,
              self).__init__(test_context=test_context,
                             si_settings=si_settings,
                             extra_rp_conf=self.extra_rp_conf,
                             log_level="debug")

        self.rpk = RpkTool(self.redpanda)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

    def produce(self, topic, msg_count):
        msg_size = 1024 * 256
        topic_name = topic.name
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size=msg_size,
            msg_count=msg_count,
            fake_timestamp_ms=self.next_base_timestamp,
            fake_timestamp_step_ms=self.timestamp_step_ms)

        self.next_base_timestamp += (msg_count + 1) * self.timestamp_step_ms

    def num_manifests_uploaded(self):
        s = self.redpanda.metric_sum(
            metric_name=
            "redpanda_cloud_storage_spillover_manifest_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        self.logger.info(
            f"redpanda_cloud_storage_spillover_manifest_uploads = {s}")
        return s

    def num_segments_deleted(self):
        s = self.redpanda.metric_sum(
            metric_name="redpanda_cloud_storage_deleted_segments_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        self.logger.info(
            f"redpanda_cloud_storage_deleted_segments_total = {s}")
        return s

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type(),
            retention_type=['retention.bytes', 'retention.ms'])
    def test_delete(self, cloud_storage_type, retention_type):
        topic = TopicSpec(cleanup_policy=TopicSpec.CLEANUP_DELETE,
                          retention_ms=-1)

        self.client().create_topic(topic)
        self.client().alter_topic_config(topic.name, 'message.timestamp.type',
                                         "CreateTime")

        def wait_for_topic():
            wait_until(lambda: len(
                list(self.client().describe_topic(topic.name))) > 0,
                       30,
                       backoff_sec=2)

        def produce_until_spillover():
            initial_uploaded = self.num_manifests_uploaded()

            def new_manifest_spilled():
                self.produce(topic, 300)
                num_spilled = self.num_manifests_uploaded()
                return num_spilled > initial_uploaded + 10

            wait_until(new_manifest_spilled,
                       timeout_sec=120,
                       backoff_sec=10,
                       err_msg="Manifests were not created")

        wait_for_topic()
        produce_until_spillover()

        # Wait for all uploads to complete before fetching segment summaries.
        quiesce_uploads(self.redpanda, [topic.name], timeout_sec=60)

        view = BucketView(self.redpanda, [topic], scan_segments=True)

        ntp0 = NTP(ns='kafka', topic=topic.name, partition=0)
        summaries = view.segment_summaries(ntp0)
        expected_so = None
        # Try to remove 7 segments. Spillover manifest is configured to
        # have 5 segments so this will guarantee that retention will
        # truncate one full spillover manifest.
        index = min(len(summaries) - 1, 7)
        if retention_type == 'retention.bytes':
            retention_value = 0
            log_size = sum([s.size_bytes for s in summaries])
            retention_value = log_size
            for s in summaries[0:index]:
                retention_value -= s.size_bytes
            expected_so = summaries[index].base_offset
            self.logger.debug(
                f"Setting retention.bytes to {retention_value}, cloud log size for ntp: {log_size}, expected start offset: {expected_so}"
            )
        else:
            current_time = round(time.time() * 1000)
            retention_value = current_time - summaries[index].base_timestamp
            expected_so = summaries[index].base_offset
            self.logger.debug(
                f"Setting retention.ms to {retention_value}, current_time: {current_time}, expected start offset: {expected_so}"
            )

        segments_to_delete = 0
        for part_id in range(0, topic.partition_count):
            ntp = NTP(ns='kafka', topic=topic.name, partition=part_id)
            summaries = view.segment_summaries(ntp)
            for s in summaries:
                if s.base_offset < expected_so:
                    segments_to_delete += 1

        self.client().alter_topic_config(topic.name, retention_type,
                                         retention_value)

        self.logger.debug(
            f"waiting until {segments_to_delete} segments will be removed")
        # Wait for the first truncation to the middle of the archive
        wait_until(
            lambda: self.num_segments_deleted() == segments_to_delete,
            timeout_sec=100,
            backoff_sec=5,
            err_msg=
            f"Segments were not removed from the cloud, expected {segments_to_delete} deletions"
        )

        view.reset()
        for partition_id in range(0, topic.partition_count):
            assert view.is_archive_cleanup_complete(
                NTP(ns='kafka', topic=topic.name, partition=partition_id))

        # Try to truncate the entire archive. In order to do this
        # we need to set retention value to very small value and correctly
        # calculate number or deleted segments.

        # Need to sleep for double retention time to ensure that all segments
        # will be deleted (in case of retention.ms)
        time.sleep(2)

        for part_id in range(0, topic.partition_count):
            ntp = NTP(ns='kafka', topic=topic.name, partition=part_id)
            summaries = view.segment_summaries(ntp)
            if retention_type == 'retention.bytes':
                retention_value = max(
                    min(summaries, key=lambda s: s.size_bytes).size_bytes - 1,
                    0)
                segments_to_delete = len(summaries)
            else:
                retention_value = 1000  # 1s
                current_time = round(time.time() * 1000)
                segments_to_delete = len([
                    s for s in summaries
                    if s.base_timestamp < (current_time - retention_value)
                ])

        self.logger.debug(f"Setting {retention_type} to {retention_value}")

        # Note: altering the topic config will re-init the archiver and
        # reset the metric tracking segment deletion. This is why we assign
        # to `segments_to_delete` instead of adding.
        self.client().alter_topic_config(topic.name, retention_type,
                                         retention_value)

        self.logger.debug(
            f"waiting until {segments_to_delete} segments will be removed")
        # Wait for the second truncation of the entire archive
        wait_until(
            lambda: self.num_segments_deleted() == segments_to_delete,
            timeout_sec=120,
            backoff_sec=5,
            err_msg=
            f"Segments were not removed from the cloud, expected {segments_to_delete} "
            f"segments to be removed but only {self.num_segments_deleted()} was actually removed"
        )

        view.reset()
        for partition_id in range(0, topic.partition_count):
            ntp = NTP(ns='kafka', topic=topic.name, partition=partition_id)
            assert view.is_archive_cleanup_complete(ntp)
            view.check_archive_integrity(ntp)
