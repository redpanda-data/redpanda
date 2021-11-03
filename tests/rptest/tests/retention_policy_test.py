# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.mark import matrix

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import (
    produce_until_segments,
    wait_for_segments_removal,
)


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
        kafka_tools = KafkaCliTools(self.redpanda)

        # produce until segments have been compacted
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
            acks=acks,
        )
        # change retention time
        kafka_tools.alter_topic_config(self.topic, {
            property: 10000,
        })
        wait_for_segments_removal(self.redpanda,
                                  self.topic,
                                  partition_idx=0,
                                  count=5)

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
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=20,
            acks=-1,
        )

        # restart all nodes to force replicating raft configuration
        self.redpanda.restart_nodes(self.redpanda.nodes)

        kafka_tools = KafkaCliTools(self.redpanda)
        # Wait for controller, alter configs doesn't have a retry loop
        kafka_tools.describe_topic(self.topic)

        # change retention bytes to preserve 15 segments
        kafka_tools.alter_topic_config(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES: 15 * segment_size,
            })
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=16)

        # change retention bytes again to preserve 10 segments
        kafka_tools.alter_topic_config(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES: 10 * segment_size,
            })
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=11)

        # change retention bytes again to preserve 5 segments
        kafka_tools.alter_topic_config(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES: 4 * segment_size,
            })
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=5)
