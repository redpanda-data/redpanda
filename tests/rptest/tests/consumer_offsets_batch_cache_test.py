# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random

from confluent_kafka import Consumer, TopicPartition

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import DefaultClient, RedpandaTest


class ConsumerOffsetsCacheTest(RedpandaTest):
    def _consumer_offsets_cache_hit_ratio(self):
        cached_read_metrics = self.redpanda.metrics_sample(
            "vectorized_storage_log_cached_read_bytes", nodes=self.redpanda.nodes
        )
        read_metrics = self.redpanda.metrics_sample(
            "vectorized_storage_log_read_bytes", nodes=self.redpanda.nodes
        )

        def _sum_metric_value(metrics):
            return sum(
                s.value
                for s in metrics.samples
                if s.labels["topic"] == "__consumer_offsets"
            )

        total_read = _sum_metric_value(read_metrics)
        cache_read = _sum_metric_value(cached_read_metrics)
        self.logger.info(f"Read from cache: {cache_read}, total read: {total_read}")
        self.logger.info(f"Read metrics: {_sum_metric_value(read_metrics)}")
        return cache_read / total_read if total_read > 0 else None

    def _commit_random_offsets(self, topic: str, partition: int):
        # Create a consumer that will commit random offsets without consuming
        consumer = Consumer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "group.id": "test-consumer-group",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )

        # Commit random offsets for the topic partition
        try:
            partition = 0
            for _ in range(100):
                # Generate a random offset between 0 and 1000
                random_offset = random.randint(0, 1000)

                # Commit the random offset
                consumer.commit(
                    offsets=[TopicPartition(topic, partition, offset=random_offset)],
                    asynchronous=False,
                )
        finally:
            consumer.close()

    @cluster(num_nodes=3)
    def test_enabling_consumer_offsets_cache_test(self):
        topic = TopicSpec(name="test-topic", partition_count=1, replication_factor=3)

        DefaultClient(self.redpanda).create_topic(topic)
        self._commit_random_offsets(topic.name, 0)

        ratio_no_cache = self._consumer_offsets_cache_hit_ratio()
        assert ratio_no_cache == 0.0, (
            "By default consumer offsets cache should not be enabled"
        )
        self.redpanda.set_cluster_config(
            {
                "consumer_offsets_topic_batch_cache_enabled": True,
            },
            expect_restart=True,
        )
        self._commit_random_offsets(topic.name, 0)
        hit_ratio = self._consumer_offsets_cache_hit_ratio()
        # hit ratio is not exactly 1.0 as the topic is read once during the
        # startup, the cache is cold during that operation
        assert hit_ratio > 0.75, "Consumer offsets topic should use cache now"
