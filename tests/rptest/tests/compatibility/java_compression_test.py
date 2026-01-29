# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from kafka import KafkaConsumer

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.services.redpanda_installer import (
    InstallOptions,
    RedpandaVersionTriple,
)
from rptest.services.verifiable_producer import VerifiableProducer
from rptest.tests.end_to_end import EndToEndTest


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
        # Arbitrarily high key set cardinality
        key_set_cardinality = 100000
        producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_spec.name,
            compression_types=[compression_type],
            repeating_keys=key_set_cardinality,
        )
        producer.start()
        try:
            wait_until(
                lambda: self.partition_segments() >= count,
                timeout_sec=timeout_sec,
                backoff_sec=1,
                err_msg=f"Timed out waiting for {count} segments to be produced.",
            )
        finally:
            producer.clean()
            producer.free()

    def consume(self, num_messages=10, timeout_sec=120):
        deadline = time() + timeout_sec
        consumer = KafkaConsumer(
            self.topic_spec.name,
            bootstrap_servers=self.redpanda.brokers(),
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
                assert False, "Failed to consume messages"

    def get_compacted_segments(self):
        num_compacted_segments = self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_compacted_segment_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )
        self.redpanda.logger.debug(f"Saw {num_compacted_segments} compacted segments")
        return num_compacted_segments

    def wait_for_compacted_segments(self, num_segments, timeout_sec=360):
        self.redpanda.logger.debug(f"Waiting for {num_segments} compacted segments")
        wait_until(
            lambda: self.get_compacted_segments() >= num_segments,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=f"Timed out waiting for {num_segments} compacted segments.",
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
            replication_factor=1,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            min_cleanable_dirty_ratio=0.0,
        )
        self.client().create_topic(self.topic_spec)

        expected_num_segments = 4
        self.produce_until_segment_count(
            expected_num_segments, compression_type=compression_type.value
        )
        self.wait_for_compacted_segments(expected_num_segments - 1)

        self.consume()

    @cluster(num_nodes=2)
    @matrix(
        compression_type=[
            TopicSpec.CompressionTypes.GZIP,
            TopicSpec.CompressionTypes.LZ4,
            TopicSpec.CompressionTypes.SNAPPY,
            TopicSpec.CompressionTypes.ZSTD,
        ]
    )
    def test_upgrade_java_compression(self, compression_type):
        """
        For ensuring backwards compatibility, this test uses an earlier version of `redpanda`
        (pre 25.1) to produce some compressed batches, and then upgrades to the current version
        of `redpanda`. Similar to the test above, compaction is used to force the decompression
        and recompression of batches using the internal `redpanda` compression implementations.
        This test guarantees that `redpanda` is backwards compatible w/r/t changes in compression.
        The main motivation for adding this test was ensuring `snappy` compression correctness,
        as there was a change in header encoding in version 25.1 (see issue:
        https://github.com/redpanda-data/redpanda/issues/25091), but this test is parameterized
        with all compression types for completion's sake.
        """
        pre_big_endian_snappy_fix_version = RedpandaVersionTriple((24, 3, 5))
        self.start_redpanda(
            num_nodes=1,
            install_opts=InstallOptions(version=pre_big_endian_snappy_fix_version),
        )

        self.topic_spec = TopicSpec(
            replication_factor=1, cleanup_policy=TopicSpec.CLEANUP_COMPACT
        )
        self.client().create_topic(self.topic_spec)

        expected_num_segments = 4
        self.produce_until_segment_count(
            expected_num_segments, compression_type=compression_type.value
        )
        self.wait_for_compacted_segments(expected_num_segments - 1)

        self.consume()

        # Install the latest version of `redpanda` and restart nodes
        for version in self.redpanda._installer.upgrade_path_to_head(
            pre_big_endian_snappy_fix_version
        ):
            self.redpanda._installer.install(self.redpanda.nodes, version)
            self.redpanda.stop_node(self.redpanda.nodes[0])
            self.redpanda.start_node(self.redpanda.nodes[0])
        self.redpanda.stop_node(self.redpanda.nodes[0])

        # Delete all the compaction indices to force self compaction of segments.
        storage = self.redpanda.storage(nodes=self.redpanda.nodes)
        partitions = storage.partitions("kafka", self.topic_spec.name)
        for p in partitions:
            p.delete_indices(allow_fail=True)

        self.redpanda.start_node(self.redpanda.nodes[0])

        # Repeat the process
        expected_num_segments = 8
        self.produce_until_segment_count(
            expected_num_segments, compression_type=compression_type.value
        )

        # The min_cleanable_dirty_ratio cluster config was not in the original
        # version of redpanda installed above, it was added in v25.1.0.
        # Patch its value here to ensure that every snappy batch will be
        # re-written by compaction. Also push compaction to happen quite rapidly.
        self.redpanda._admin.patch_cluster_config(
            upsert={"min_cleanable_dirty_ratio": 0.0, "log_compaction_interval_ms": 500}
        )

        def consumer_succeeds():
            """
            In the case of `snappy`, every single batch should eventually be decompressed
            and recompressed using the new big-endian fix during compaction.
            Expect to see consuming with `kafka-python` eventually succeed.
            """
            try:
                self.consume()
                return True
            except Exception:
                return False

        wait_until(
            consumer_succeeds,
            timeout_sec=360,
            backoff_sec=5,
            err_msg="Timed out waiting for consuming to succeed.",
        )
