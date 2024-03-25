# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import random
import time
from collections import defaultdict
from collections import deque

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import MetricsEndpoint, SISettings
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.si_utils import BucketView, NTP, quiesce_uploads


class CloudStorageUsageTest(RedpandaTest, PartitionMovementMixin):
    message_size = 32 * 1024  # 32KiB
    log_segment_size = 1024 * 1024  # 256KiB
    produce_byte_rate_per_ntp = 512 * 1024  # 512 KiB
    target_runtime = 60  # seconds
    check_interval = 5  # seconds

    topics = [
        TopicSpec(name="test-topic-1",
                  partition_count=3,
                  replication_factor=3,
                  retention_bytes=3 * log_segment_size),
        TopicSpec(name="test-topic-2",
                  partition_count=1,
                  replication_factor=1,
                  retention_bytes=3 * log_segment_size,
                  cleanup_policy=TopicSpec.CLEANUP_COMPACT)
    ]

    def __init__(self, test_context):
        extra_rp_conf = dict(log_compaction_interval_ms=2000,
                             compacted_log_segment_size=self.log_segment_size)

        super(CloudStorageUsageTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=SISettings(test_context,
                                   log_segment_size=self.log_segment_size,
                                   cloud_storage_housekeeping_interval_ms=2000,
                                   fast_uploads=True))

        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        self.s3_port = self.si_settings.cloud_storage_api_endpoint_port

    def _create_producers(self) -> list[KgoVerifierProducer]:
        producers = []

        for topic in self.topics:
            bps = self.produce_byte_rate_per_ntp * topic.partition_count
            bytes_count = bps * self.target_runtime
            msg_count = bytes_count // self.message_size

            self.logger.info(f"Will produce {bytes_count / 1024}KiB at"
                             f"{bps / 1024}KiB/s on topic={topic.name}")
            producers.append(
                KgoVerifierProducer(self.test_context,
                                    self.redpanda,
                                    topic,
                                    msg_size=self.message_size,
                                    msg_count=msg_count,
                                    rate_limit_bps=bps,
                                    batch_max_bytes=self.log_segment_size //
                                    2))

        return producers

    def _check_usage(self, timeout_sec):
        bucket_view = BucketView(self.redpanda)

        # The usage inferred from the uploaded manifest
        # lags behind the actual reported usage. For this reason,
        # we maintain a sliding window of reported usages and check whether
        # the manifest inferred usage can be found in it.
        reported_usage_sliding_window = deque(maxlen=10)

        def check():
            manifest_usage = bucket_view.cloud_log_sizes_sum().accessible(
                no_archive=True)

            reported_usage = self.admin.cloud_storage_usage()
            reported_usage_sliding_window.append(reported_usage)

            self.logger.info(
                f"Expected {manifest_usage} bytes of cloud storage usage")
            self.logger.info(
                f"Reported usages in sliding window: {reported_usage_sliding_window}"
            )
            return manifest_usage in reported_usage_sliding_window

        wait_until(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=0.2,
            err_msg=
            "Reported cloud storage usage did not match the manifest inferred usage"
        )

    def _test_epilogue(self):
        # Assert tht retention was active
        self.redpanda.metric_sum(
            "redpanda_cloud_storage_deleted_segments",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS) > 0

        # Assert that compacted segment re-upload operated during the test
        bucket_view = BucketView(self.redpanda, topics=self.topics)
        bucket_view.assert_at_least_n_uploaded_segments_compacted(
            self.topics[1].name, partition=0, revision=None, n=1)

    def _check_describe_log_dirs(self):
        quiesce_uploads(self.redpanda, [t.name for t in self.topics],
                        timeout_sec=30)

        describe_items = self.rpk.describe_log_dirs()
        by_ntp: defaultdict[NTP, list] = defaultdict(list)
        for i in describe_items:
            ntp = NTP(ns='kafka', topic=i.topic, partition=i.partition)
            by_ntp[ntp].append(i)

        bucket_view = BucketView(self.redpanda)
        for ntp, items in by_ntp.items():
            remote_items = list(i for i in items
                                if i.dir.startswith("remote://"))
            self.logger.info(f"{ntp} {len(items)} describelogdirs items")

            ntp_remote_size = max(i.size
                                  for i in remote_items) if remote_items else 0
            actual_size = bucket_view.cloud_log_size_for_ntp(
                ntp.topic, ntp.partition).accessible(no_archive=True)

            assert ntp_remote_size == actual_size, f"{ntp_remote_size=} != {actual_size=}"

    @cluster(num_nodes=5)
    def test_cloud_storage_usage_reporting(self):
        """
        This test uses a diverse cloud storage write-only workload
        (includes retention and compacted re-uploads). It periodically,
        checks that the cloud storage usage reported by `/v1/debug/cloud_storage_usage`
        is in line with the contents of the uploaded manifest.
        """
        assert self.admin.cloud_storage_usage() == 0

        producers = self._create_producers()
        for p in producers:
            p.start()

        producers_done = lambda: all([p.is_complete() for p in producers])
        while not producers_done():
            self._check_usage(timeout_sec=5)

            time.sleep(self.check_interval)

        for p in producers:
            p.wait()

        self._test_epilogue()

        self._check_describe_log_dirs()

    @cluster(num_nodes=5)
    @skip_debug_mode
    def test_cloud_storage_usage_reporting_with_partition_moves(self):
        """
        This test has the same workload as test_cloud_storage_usage_reporting,
        but also includes random partition movements.
        """
        assert self.admin.cloud_storage_usage() == 0

        producers = self._create_producers()
        for p in producers:
            p.start()

        partitions = []
        for topic in self.topics:
            partitions.extend([(topic.name, pid)
                               for pid in range(topic.partition_count)])

        producers_done = lambda: all([p.is_complete() for p in producers])

        while not producers_done():
            ntp_to_move = random.choice(partitions)
            self._dispatch_random_partition_move(ntp_to_move[0],
                                                 ntp_to_move[1])

            self._check_usage(timeout_sec=10)

            time.sleep(self.check_interval)

        for p in producers:
            p.wait()

        self._check_describe_log_dirs()
