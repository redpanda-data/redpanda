# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import bisect
import time
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest


class ReadDistributionTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {'aggregate_metrics': True}
        super().__init__(test_context=test_context,
                         num_brokers=1,
                         extra_rp_conf=extra_rp_conf)

        self.topic_name = 'tapioca'
        self.topics = [TopicSpec(name=self.topic_name, replication_factor=1)]

        self.record_size = 1024
        self.record_count = 10
        self.records_per_batch = 2
        self.timespan_ms = 24 * 60 * 60 * 1000  #One day in ms.
        self.timestamp_step_ms = self.timespan_ms // (self.record_count - 1)
        self.curr_ts = round(time.time() * 1000)
        self.base_ts = self.curr_ts - self.timespan_ms

    @cluster(num_nodes=2)
    def test_read_distribution_metric(self):
        '''
        Validate that Kafka reads are being correctly set in the read distribution histogram.
        '''
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic_name,
            self.record_size,
            self.record_count,
            batch_max_bytes=self.record_size * self.records_per_batch - 1,
            fake_timestamp_ms=self.base_ts,
            fake_timestamp_step_ms=self.timestamp_step_ms)
        kcat = KafkaCat(self.redpanda)

        # The list of timestamps to be produced/consumed from.
        timestamps_ms = [
            self.base_ts + i * self.timestamp_step_ms
            for i in range(0, self.record_count)
        ]

        for timestamp in timestamps_ms:
            kcat.consume_one(self.topic_name,
                             0,
                             offset=None,
                             first_timestamp=timestamp)

        metrics = self.redpanda.metrics(self.redpanda.nodes[0],
                                        MetricsEndpoint.METRICS)

        read_dist_metric_name = "vectorized_kafka_fetch_read_distribution"
        read_dist_metric = next(
            filter(lambda m: m.name == read_dist_metric_name, metrics), None)
        assert read_dist_metric is not None

        cumulative_topic_read_dist = [
            (s.value, s.labels['le']) for s in read_dist_metric.samples
            if s.name == f"{read_dist_metric_name}_bucket"
        ]

        num_expected_buckets = 16
        # There should be 16 + 1 buckets in the read distribution histogram (including inf bucket)
        assert len(cumulative_topic_read_dist) == num_expected_buckets + 1

        prev = 0
        topic_read_dist = {}
        for v, le in cumulative_topic_read_dist:
            try:
                topic_read_dist[int(float(le))] = int(v - prev)
                prev = v
            except:
                continue

        first_bucket_lower_bound = 4
        # Build up expected bucket list:
        # 3, 7, 15, 31, 63, 127, 255, 511, 1023,
        # 2047, 4095, 8191, 16383, 32767, 65535, 131071
        expected_bucket_list = [(first_bucket_lower_bound * 2**i) - 1
                                for i in range(0, num_expected_buckets)]

        bucket_list = list(topic_read_dist.keys())

        # There should be 16 buckets in the bucket_list (after filtering out inf bucket),
        # and the values should be equal to the expected list built above.
        assert bucket_list == expected_bucket_list

        # Hardcoded, expected bucket values for the timestamps below.
        expected_bucket_order = [
            2047, 2047, 2047, 1023, 1023, 1023, 511, 511, 255, 3
        ]
        assert len(expected_bucket_order) == len(timestamps_ms)
        expected_read_dist = {bucket: 0 for bucket in topic_read_dist.keys()}
        for expected_bucket, timestamp_ms in zip(expected_bucket_order,
                                                 timestamps_ms):
            delta_minutes = round((self.curr_ts - timestamp_ms) / (1000 * 60))

            #Find the bucket into which the timestamp delta in minutes
            #is going to fall, using lower bound.
            bucket = bucket_list[bisect.bisect_left(bucket_list,
                                                    delta_minutes)]
            assert bucket == expected_bucket
            expected_read_dist[bucket] += 1

        for bucket, count in expected_read_dist.items():
            assert topic_read_dist[bucket] == count
