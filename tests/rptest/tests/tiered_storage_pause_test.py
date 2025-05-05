# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings, MetricsEndpoint

from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils import si_utils
from rptest.utils.mode_checks import skip_debug_mode

from collections import defaultdict
from typing import Optional
import time


def gaps_allowed(cluster_level_allow_gaps: bool,
                 topic_level_override: Optional[bool] = None):
    """Return True if the gaps are allowed.
    'topic_level_override' can be set to True, False, or None. The last
    value means that the override is not set.
    """
    if topic_level_override is not None:
        return topic_level_override
    return cluster_level_allow_gaps


class TestTieredStoragePause(PreallocNodesTest):
    """Tiered storage can be put on 'pause' by setting
    'cloud_storage_enable_segment_uploads' cluster config to 'True'.
    There are two options there.
    1. If 'cloud_storage_enable_remote_allow_gaps' is set to False (default)
       the local retention is not allowed to remove segments which are not
       uploaded to the cloud storage.
    2. If 'cloud_storage_enable_remote_allow_gaps' is set to True the
       tiered-storage is allowed to remove segments which are not uploaded
       to the cloud storage yet.
    3. The topic level override 'redpanda.remote.allowgaps' could be used
       to enable or disable the no-gaps policy.
    """
    def __init__(self, test_context):
        super().__init__(test_context,
                         num_brokers=3,
                         node_prealloc_count=1,
                         si_settings=SISettings(
                             test_context=test_context,
                             cloud_storage_segment_max_upload_interval_sec=5,
                             cloud_storage_enable_remote_read=True,
                             cloud_storage_enable_remote_write=True,
                             cloud_storage_housekeeping_interval_ms=500,
                             fast_uploads=True))
        self._msg_size = 128
        self._segment_size = 1024 * 1024
        # num messages to produce (50 MiB)
        self._msg_count = 50 * int(1024 * 1024 / self._msg_size)
        # amount of data to keep per partition
        self._retention_bytes = 10 * 1024 * 1024

    def get_metric(self, metric_name, endpoint):
        res = defaultdict(int)
        for n in self.redpanda.nodes:
            if n in self.redpanda._started:
                resp = self.redpanda.metrics(n, endpoint)
                for family in resp:
                    for sample in family.samples:
                        res[sample.name] += sample.value
        # should be safe due to GIL
        if endpoint == MetricsEndpoint.PUBLIC_METRICS:
            self.logger.info(f"Public metrics {res}")
        else:
            self.logger.info(f"Private metrics {res}")
        return res.get(metric_name)

    def start_producer(self):
        self.logger.info(
            f"starting kgo-verifier producer with {self._msg_count} messages of size {self._msg_size}, retention bytes {self._retention_bytes}"
        )
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self._topic,
            self._msg_size,
            self._msg_count,
            # Limit tput to 1MiB per second
            rate_limit_bps=2 * 1024 * 1024,
            custom_node=self.preallocated_nodes)

        self.producer.start(clean=False)

        self.producer.wait_for_acks(
            2 * self._retention_bytes // self._msg_size, 60, 1)

    def start_producer_lite(self):
        """Small scale producer"""

        # Couple of segments to ensure that retention can remove whole segments
        # before these new ones.
        msg_cnt = 3 * self._segment_size // self._msg_size
        self.logger.info(
            f"starting kgo-verifier producer with {msg_cnt} messages")
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self._topic,
            self._msg_size,
            msg_cnt,
            custom_node=self.preallocated_nodes)

        self.producer.start(clean=False)

        self.producer.wait_for_acks(msg_cnt, 60, 1)

    def wait_archivers_paused(self, expected_num_paused):
        # Check that metric is showing that the ntp archiver is paused
        def _archiver_paused():
            num_paused = self.get_metric(
                'redpanda_cloud_storage_paused_archivers',
                MetricsEndpoint.PUBLIC_METRICS)
            # only one partition is created
            return num_paused == expected_num_paused

        wait_until(
            _archiver_paused,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"number of paused archivers is not {expected_num_paused}")

    def get_uploaded_bytes(self):
        uploaded_bytes = self.get_metric(
            'vectorized_ntp_archiver_uploaded_bytes_total',
            MetricsEndpoint.METRICS)
        return uploaded_bytes

    def expect_no_uploaded_bytes(self):
        """Check that nothing is uploaded to the cloud storage"""
        uploaded_bytes = self.get_uploaded_bytes()
        assert uploaded_bytes == 0, f"{uploaded_bytes} bytes are uploaded, expected all uploads to be paused"

    def expect_start_from_offset_zero(self, expected):
        """If 'expected' set to True then the expectation is that the start offset is
        zero. Otherwise the expectation is that start offset is greater than zero"""
        rpk = RpkTool(self.redpanda)
        partitions = rpk.describe_topic(self._topic)
        for p in partitions:
            if expected:
                res = p.start_offset == 0
            else:
                res = p.start_offset > 0
            assert res, f"local retention have unexpected value, expected to start from zero = {expected}, start offset = {p.start_offset}"

    def wait_until_start_offset_advances(self):
        """
        Check that start offset moved forward because of the retention.
        """
        rpk = RpkTool(self.redpanda)

        def _start_offset_updated():
            partitions = rpk.describe_topic(self._topic)
            return all([p.start_offset > 0 for p in partitions])

        wait_until(_start_offset_updated,
                   timeout_sec=120,
                   backoff_sec=10,
                   err_msg="timed out waiting for segments to be evicted")

    def get_start_offset(self) -> int:
        rpk = RpkTool(self.redpanda)
        partitions = list(rpk.describe_topic(self._topic))
        assert len(partitions) == 1, "expecting only one partition"
        for p in partitions:
            return p.start_offset
        assert False, "unreachable code"

    @cluster(num_nodes=4)
    @skip_debug_mode
    @matrix(allow_gaps_topic_level=[None, True, False],
            allow_gaps_cluster_level=[True, False])
    def test_safe_pause_resume(self, allow_gaps_cluster_level,
                               allow_gaps_topic_level):
        """
        The test performs pause/resume operation. If the gaps are not allowed
        it checks that the range is full. In this test we starts with paused
        uploads. No segments should be uploaded to S3 so the Tiered storage
        shouldn't be able to influence 'start_offset'. Because of that if the
        local retention is allowed to run we will see 'start_offset' advancing
        forward. So in case if the gaps are not allowed we just need to check
        that the 'start_offset' is equal to zero while uploads are paused.

        Otherwise it expects that the gap in the offset range is created. This
        is checked by examining the 'start_offset' as well.
        The test also validates the behavior of the 'paused_archivers' metric.
        The metric is expected to be GT zero when some archivers are paused.
        Otherwise it should be set to zero.
        """
        # create topic with tiered storage enabled
        topic = TopicSpec(partition_count=1,
                          segment_bytes=self._segment_size,
                          retention_bytes=self._retention_bytes)

        self.client().create_topic(topic)
        if allow_gaps_topic_level is not None:
            value = 'true' if allow_gaps_topic_level else 'false'
            self.client().alter_topic_config(topic.name,
                                             "redpanda.remote.allowgaps",
                                             value)
        self._topic = topic.name

        # Pause all segment uploads
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_segment_uploads": False})
        self.redpanda.set_cluster_config({
            "cloud_storage_enable_remote_allow_gaps":
            allow_gaps_cluster_level
        })
        self.redpanda.set_cluster_config(
            {"cloud_storage_housekeeping_interval_ms": 1000})

        gaps_expected = gaps_allowed(allow_gaps_cluster_level,
                                     allow_gaps_topic_level)

        self.start_producer()

        # At this point retention.bytes x2 are produced to the partition.
        # Nothing is uploaded yet so if local retention is allowed to remove
        # data start_offset will move forward.
        self.expect_start_from_offset_zero(not gaps_expected)

        # Check that metric is showing that the ntp archiver is paused
        self.wait_archivers_paused(expected_num_paused=1)

        # Check that nothing is uploaded
        self.expect_no_uploaded_bytes()

        # Resume segment uploads
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_segment_uploads": True})
        self.redpanda.wait_for_manifest_uploads()

        # check that start offset moved forward because of the retention
        self.wait_until_start_offset_advances()

        # check that metric is showing that the ntp archiver is no longer paused
        self.wait_archivers_paused(expected_num_paused=0)

        # Wait until all messages are produced.
        # This is not strictly necessary for the test. But uploading some more
        # data may trigger some unexpected behavior.
        self.producer.wait_for_acks(self._msg_count, 120, 1)

        # Check that all data was successfully uploaded to the cloud storage.
        si_utils.quiesce_uploads(self.redpanda, [self._topic], 120)

    @cluster(num_nodes=4)
    @skip_debug_mode
    @matrix(allow_gaps_topic_level=[None],
            allow_gaps_cluster_level=[True, False])
    def test_resume(self, allow_gaps_cluster_level, allow_gaps_topic_level):
        """
        The test performs pause/resume operation. Unlike the previous test it
        starts with some data in the cloud storage. Because of that if the gaps
        are allowed during the pause the gap is actually created.
        """
        # create topic with tiered storage enabled
        topic = TopicSpec(partition_count=1,
                          segment_bytes=self._segment_size,
                          retention_bytes=self._retention_bytes)

        self.client().create_topic(topic)
        if allow_gaps_topic_level is not None:
            value = 'true' if allow_gaps_topic_level else 'false'
            self.client().alter_topic_config(topic.name,
                                             "redpanda.remote.allowgaps",
                                             value)
        self._topic = topic.name

        # Pre-populate the topic
        self.start_producer()
        self.producer.wait_for_acks(self._msg_count, 120, 1)
        self.producer.free()

        # Check that pre-populated data is uploaded to the cloud storage.
        si_utils.quiesce_uploads(self.redpanda, [self._topic], 120)

        # save the start offset before pausing the uploads
        start_offset = self.get_start_offset()

        # Pause all segment uploads
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_segment_uploads": False})
        self.redpanda.set_cluster_config({
            "cloud_storage_enable_remote_allow_gaps":
            allow_gaps_cluster_level
        })
        self.redpanda.set_cluster_config(
            {"cloud_storage_housekeeping_interval_ms": 1000})

        # Check that metric is showing that the ntp archiver is paused
        self.wait_archivers_paused(expected_num_paused=1)

        # Start second round of producing.
        # We don't need a lot of data here. Just need to check that
        # uploads are not stalled.
        self.start_producer_lite()

        def _start_offset_advanced():
            pause_start_offset = self.get_start_offset()
            return pause_start_offset > start_offset

        wait_until(_start_offset_advanced,
                   timeout_sec=120,
                   backoff_sec=10,
                   err_msg="timed out waiting for segments to be evicted")

        # Wait for local storage housekeeping to remove some local segments.
        # The housekeeping interval is set to 1s.
        time.sleep(15)

        # Resume segment uploads.
        # If the gaps are allowed we expect to see one.
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_segment_uploads": True})
        self.redpanda.wait_for_manifest_uploads()

        # check that metric is showing that the ntp archiver is no longer paused
        self.wait_archivers_paused(expected_num_paused=0)

        # Check that all data was successfully uploaded to the cloud storage.
        si_utils.quiesce_uploads(self.redpanda, [self._topic], 120)
