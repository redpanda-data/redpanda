# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import time
from rptest.services.cluster import cluster
from rptest.tests.end_to_end import EndToEndTest
from kafka import TopicPartition, KafkaConsumer
from rptest.services.verifiable_producer import VerifiableProducer
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import MetricsEndpoint


class JavaCompressionTest(EndToEndTest):
    def __init__(self, test_context):
        self.test_context = test_context
        self.extra_rp_conf = {
            "log_segment_size": 2 * 1024**2,  # 2 MiB
            "compacted_log_segment_size": 1024**2,  # 1 MiB
            "log_compaction_interval_ms": 10000,
        }

        super().__init__(test_context=test_context, extra_rp_conf=self.extra_rp_conf)

    def partition_segments(self) -> int:
        assert len(self.redpanda.nodes) == 1, self.redpanda.nodes
        node = self.redpanda.nodes[0]
        storage = self.redpanda.node_storage(node)
        topic_partitions = storage.partitions("kafka", self.topic_spec.name)
        assert len(topic_partitions) == 1, len(topic_partitions)
        segment_count = len(topic_partitions[0].segments)
        self.redpanda.logger.debug(f"Current segment count: {segment_count}")
        return segment_count

    def produce_until_segment_count(self, count, compression_type, timeout_sec=120):
        producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_spec.name,
            compression_types=[compression_type],
        )
        producer.start()
        try:
            wait_until(
                lambda: self.partition_segments() >= count,
                timeout_sec=timeout_sec,
                err_msg=f"Timed out waiting for {count} segments to be produced.",
            )
        finally:
            producer.stop()
            producer.clean()
            producer.free()

    def consume(self, num_messages=100, timeout_sec=120):
        deadline = time() + timeout_sec
        consumer = KafkaConsumer(
            self.topic_spec.name,
            bootstrap_servers=self.redpanda.brokers(),
            group_id="0",
            consumer_timeout_ms=1000,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        cur_messages_amount = 0
        while True:
            poll_result = consumer.poll(timeout_ms=1000)
            cur_messages_amount += sum(map(lambda tr: len(tr), poll_result.values()))
            if cur_messages_amount >= num_messages:
                return
            if time() > deadline:
                assert False, f"Failed to consume messages"

    def get_compacted_segments(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_compacted_segment_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )

    def wait_for_compacted_segments(self, num_segments, timeout_sec=120):
        wait_until(
            lambda: self.get_compacted_segments() >= num_segments,
            timeout_sec=timeout_sec,
            err_msg=f"Timed out waiting for compacted segments.",
        )

    @cluster(num_nodes=2)
    @matrix(
        compression_type=[
            TopicSpec.CompressionTypes.GZIP,
            TopicSpec.CompressionTypes.LZ4,
            TopicSpec.CompressionTypes.SNAPPY,
            TopicSpec.CompressionTypes.ZSTD,
        ]
    )
    def test_java_compression(self, compression_type):
        """
        Produces messages using compression via a Java VerifiableProducer client,
        then waits for `redpanda` to compact some segments. The compaction process will
        decompress and recompress batches using our `redpanda` compression implementations.
        Then, we consume using a `KafkaConsumer` from `kafka-python` to ensure compatibility.
        The main motivation for adding this test was ensuring `snappy` compression correctness
        (see issue: https://github.com/redpanda-data/redpanda/issues/25091),
        but this test is parameterized with all compression types for completion's sake.
        """
        self.start_redpanda(num_nodes=1)
        self.topic_spec = TopicSpec(
            replication_factor=1, cleanup_policy=TopicSpec.CLEANUP_COMPACT
        )
        self.client().create_topic(self.topic_spec)

        expected_num_segments = 4
        self.produce_until_segment_count(
            expected_num_segments, compression_type=compression_type.value
        )
        self.wait_for_compacted_segments(expected_num_segments)

        self.consume()
