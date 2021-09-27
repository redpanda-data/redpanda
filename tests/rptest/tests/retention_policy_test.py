# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import matrix, ignore

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools


class RetentionPolicyTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=5000,
            log_segment_size=1048576,
        )

        super(RetentionPolicyTest, self).__init__(test_context=test_context,
                                                  num_brokers=3,
                                                  extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    @matrix(property=[
        TopicSpec.PROPERTY_RETENTION_TIME, TopicSpec.PROPERTY_RETENTION_BYTES
    ],
            acks=[1, -1])
    def test_changing_topic_retention(self, property, acks):
        """
        Test changing topic retention duration for topics with data produced 
        with ACKS=1 and ACKS=-1. This test produces data until 10 segments 
        appear, then it changes retention topic property and waits for 
        segments to be removed
        """
        # operate on a partition. doesn't matter which one
        partition = self.redpanda.partitions(self.topic)[0]

        # produce until segments have been compacted
        self._produce_until_segments(self.topic, 0, 10, acks)
        kafka_tools = KafkaCliTools(self.redpanda)
        # change retention time
        kafka_tools.alter_topic_config(self.topic, {
            property: 10000,
        })
        self._wait_for_segments_removal(self.topic, 0, 5)

    def _segments_count(self, topic, partition_idx):
        storage = self.redpanda.storage()
        topic_partitions = storage.partitions("kafka", topic)

        return map(lambda p: len(p.segments),
                   filter(lambda p: p.num == partition_idx, topic_partitions))

    def _produce_until_segments(self, topic, partition_idx, count, acks):
        """
        Produce into the topic until given number of segments will appear 
        """
        kafka_tools = KafkaCliTools(self.redpanda)

        def done():
            kafka_tools.produce(topic, 10000, 1024, acks=acks)
            topic_partitions = self._segments_count(topic, partition_idx)
            partitions = []
            for p in topic_partitions:
                partitions.append(p >= count)
            return all(partitions)

        wait_until(done,
                   timeout_sec=60,
                   backoff_sec=2,
                   err_msg="Segments were not created")

    def _wait_for_segments_removal(self, topic, partition_idx, count):
        """
        Wait until only given number of segments will left in a partitions
        """
        def done():
            topic_partitions = self._segments_count(topic, partition_idx)
            partitions = []
            for p in topic_partitions:
                partitions.append(p <= count)
            return all(partitions)

        wait_until(done,
                   timeout_sec=120,
                   backoff_sec=5,
                   err_msg="Segments were not removed")

    @cluster(num_nodes=3)
    def test_changing_topic_retention_with_restart(self):
        """
        Test changing topic retention duration for topics with data produced 
        with ACKS=1 and ACKS=-1. This test produces data until 10 segments 
        appear, then it changes retention topic property and waits for some 
        segmetnts to be removed
        """
        segment_size = 1048576

        # produce until segments have been compacted
        self._produce_until_segments(self.topic, 0, 20, -1)

        # restart all nodes to force replicating raft configuration
        self.redpanda.restart_nodes(self.redpanda.nodes)

        kafka_tools = KafkaCliTools(self.redpanda)
        # change retention bytes to preserve 15 segments
        kafka_tools.alter_topic_config(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES: 15 * segment_size,
            })
        self._wait_for_segments_removal(self.topic, 0, 16)

        # change retention bytes again to preserve 10 segments
        kafka_tools.alter_topic_config(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES: 10 * segment_size,
            })
        self._wait_for_segments_removal(self.topic, 0, 11)

        # change retention bytes again to preserve 5 segments
        kafka_tools.alter_topic_config(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES: 4 * segment_size,
            })
        self._wait_for_segments_removal(self.topic, 0, 5)

    def _segments_count(self, topic, partition_idx):
        storage = self.redpanda.storage()
        topic_partitions = storage.partitions("kafka", topic)

        return map(lambda p: len(p.segments),
                   filter(lambda p: p.num == partition_idx, topic_partitions))

    def _produce_until_segments(self, topic, partition_idx, count, acks):
        """
        Produce into the topic until given number of segments will appear 
        """
        kafka_tools = KafkaCliTools(self.redpanda)

        def done():
            kafka_tools.produce(topic, 10000, 1024, acks=acks)
            topic_partitions = self._segments_count(topic, partition_idx)
            partitions = []
            for p in topic_partitions:
                partitions.append(p >= count)
            return all(partitions)

        wait_until(done,
                   timeout_sec=120,
                   backoff_sec=2,
                   err_msg="Segments were not created")

    def _wait_for_segments_removal(self, topic, partition_idx, count):
        """
        Wait until only given number of segments will left in a partitions
        """
        def done():
            topic_partitions = self._segments_count(topic, partition_idx)
            partitions = []
            for p in topic_partitions:
                partitions.append(p <= count)
            return all(partitions)

        wait_until(done,
                   timeout_sec=120,
                   backoff_sec=5,
                   err_msg="Segments were not removed")
