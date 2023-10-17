# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.cluster import cluster

from ducktape.mark import matrix

from rptest.utils.mode_checks import skip_debug_mode

MB = 1024 * 1024


class IdempotencyStressTest(PreallocNodesTest):
    def __init__(self, *args, **kwargs):
        kwargs['extra_rp_conf'] = {
            # Enable segment size jitter as this is a stress test and does not
            # rely on exact segment counts.
            'log_segment_size_jitter_percent': 5,
        }
        super().__init__(
            *args,
            **kwargs,
            node_prealloc_count=1,
        )

    @property
    def is_scale_test(self):
        return self.redpanda.dedicated_nodes

    @property
    def partition_count(self):
        return 64 if self.is_scale_test else 3

    @property
    def segment_size(self):
        return 32 * (1024 * 1024)

    @property
    def msg_size(self):
        return 4069

    @property
    def msg_cnt(self):
        return int(self.total_bytes / self.msg_size)

    @property
    def total_bytes(self):
        return 2048 * MB if self.is_scale_test else 256 * MB

    @property
    def msgs_per_producer(self):
        return 20

    @property
    def throughput(self):
        return 64 * MB if self.is_scale_test else 5 * MB

    def _create_producer(self):
        self.logger.info(
            f"starting producer with: message_size: {self.msg_size}, message count: {self.msg_cnt}, throughput: {self.throughput}, total bytes: {self.total_bytes}, messages per producer: {self.msgs_per_producer}"
        )
        return KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=self.msg_size,
            # we use an arbitrary large number here, test scale is controlled by
            # total_bytes property, producer is stopped after desired number of bytes is sent
            msg_count=10000000000,
            custom_node=[self.preallocated_nodes[0]],
            rate_limit_bps=self.throughput,
            debug_logs=False,
            msgs_per_producer_id=self.msgs_per_producer)

    def validate_metrics(self, expected_size):
        metrics = self.redpanda.metrics_sample("idempotency_pid_cache_size")
        values = [s.value for s in metrics.samples]
        assert all(
            [v <= expected_size for v in values]
        ), f"Max cache size exceeded, sample values: {values}, expected cache size: {expected_size}"

    @cluster(num_nodes=4)
    @matrix(max_producer_ids=[100, 1000, 3000])
    @skip_debug_mode
    def producer_id_stress_test(self, max_producer_ids):
        """
        Check that Redpanda is handling gracefully large number of producer ids
        """

        self.topic_name = "idempotency_stress_test"

        self.redpanda.set_cluster_config(
            {"max_concurrent_producer_ids": max_producer_ids})

        self.client().create_topic(
            TopicSpec(name=self.topic_name,
                      partition_count=self.partition_count,
                      segment_bytes=self.segment_size))

        producer = self._create_producer()
        producer.start()
        producer.wait_for_acks(self.msg_cnt, 600, 1)

        self.validate_metrics(max_producer_ids)
