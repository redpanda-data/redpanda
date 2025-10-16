# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import bisect

from rptest.util import wait_until_result
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from ducktape.tests.test import TestContext
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest


class LogSegmentSizeTest(RedpandaTest):
    """
    Validates that the `segment_size` histogram metric accurately captures segment size data.
    """

    def __init__(self, test_context: TestContext):
        extra_rp_conf = {
            "aggregate_metrics": True,
            "storage_segment_size_refresh_rate_ms": "1000",
        }
        super().__init__(
            test_context=test_context, num_brokers=1, extra_rp_conf=extra_rp_conf
        )

        self.segment_size = 1024 * 1024
        self.topic_name = "tapioca"
        self.topics = [
            TopicSpec(
                name=self.topic_name,
                replication_factor=1,
                segment_bytes=self.segment_size,
            )
        ]

        self.segment_count = 10
        self.record_size = 1024
        self.record_count = self.segment_size * self.segment_count // self.record_size

    def wait_for_non_empty_segment_size_hist(self):
        def get_segment_size_hist() -> dict[int, int]:
            metrics = self.redpanda.metrics(
                self.redpanda.nodes[0], MetricsEndpoint.METRICS
            )

            segment_size_metric_name = "vectorized_storage_manager_segment_size"
            segment_size_metric = next(
                filter(lambda m: m.name == segment_size_metric_name, metrics), None
            )
            assert segment_size_metric is not None

            cumulative_segment_size_hist = [
                (s.value, s.labels["le"])
                for s in segment_size_metric.samples
                if s.name == f"{segment_size_metric_name}_bucket"
            ]

            prev = 0
            segment_size_hist = {}
            for v, le in cumulative_segment_size_hist:
                try:
                    segment_size_hist[int(float(le))] = int(v - prev)
                    prev = v
                except:
                    continue

            return segment_size_hist

        def get_non_empty_segment_size_hist():
            hist = get_segment_size_hist()
            if all([v == 0 for v in hist.values()]):
                return False
            return True, hist

        return wait_until_result(
            get_non_empty_segment_size_hist,
            timeout_sec=15,
            backoff_sec=1,
            err_msg="Didn't see non-empty segment size histogram",
        )

    @cluster(num_nodes=2)
    def test_segment_size_metric(self):
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic_name,
            self.record_size,
            self.record_count,
        )

        segment_size_hist = self.wait_for_non_empty_segment_size_hist()

        bucket_keys = list(segment_size_hist.keys())

        # Get the bucket corresponding to `self.segment_size` (i.e first bucket GE than 1024_KiB)
        bucket = bucket_keys[bisect.bisect_left(bucket_keys, self.segment_size)]

        # Expect to see at least `self.segment_count` produced segments with that size.
        assert segment_size_hist[bucket] >= self.segment_count

        # Prefix truncate the partition up to HWM
        rpk = RpkTool(self.redpanda)
        max_offset = next(rpk.describe_topic(self.topic_name)).high_watermark
        rpk.trim_prefix(self.topic_name, max_offset)

        def prefix_truncated():
            segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", self.topic_name, 0
            )
            self.logger.debug(f"Segments: {segs}")
            return len(segs) <= 1

        self.redpanda.wait_until(prefix_truncated, timeout_sec=30, backoff_sec=1)

        # Restart node to force a refresh of metrics
        self.redpanda.restart_nodes(self.redpanda.nodes[0])

        # Sample metric again post-restart
        segment_size_hist = self.wait_for_non_empty_segment_size_hist()

        # Build expected histogram from segment sizes on disk
        expected_segment_hist = {bucket: 0 for bucket in bucket_keys}
        storage = self.redpanda.node_storage(self.redpanda.nodes[0], sizes=True)
        for ns in storage.ns.values():
            for topic in ns.topics.values():
                if topic.name == "kvstore":
                    continue
                for partition in topic.partitions.values():
                    p = partition.num
                    segs = storage.segments(ns.name, topic.name, p)
                    for s in segs:
                        expected_bucket = bucket_keys[
                            bisect.bisect_left(bucket_keys, s.size)
                        ]
                        expected_segment_hist[expected_bucket] += 1

        # Assert the constructed histogram is equivalent to the reported histogram
        assert expected_segment_hist == segment_size_hist
