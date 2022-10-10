# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import itertools

from rptest.services.cluster import cluster
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import (produce_until_segments, produce_total_bytes,
                         wait_for_segments_removal, segments_count,
                         expect_exception)


def bytes_for_segments(want_segments, segment_size):
    """
    Work out what to set retention.bytes to in order to retain
    just this number of segments (assuming all segments are written
    to their size limit).
    """
    return int(want_segments * segment_size)


class RetentionPolicyTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=5000,
            log_segment_size=1048576,
            enable_transactions=True,
            enable_idempotence=True,
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
        # produce until segments have been compacted
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
            acks=acks,
        )
        # change retention time
        self.client().alter_topic_configs(self.topic, {
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
        self.client().alter_topic_configs(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES:
                bytes_for_segments(15, segment_size)
            })
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=16)

        # change retention bytes again to preserve 10 segments
        self.client().alter_topic_configs(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES:
                bytes_for_segments(10, segment_size),
            })
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=11)

        # change retention bytes again to preserve 5 segments
        self.client().alter_topic_configs(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES:
                bytes_for_segments(4, segment_size),
            })
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=5)

    @cluster(num_nodes=3)
    def test_timequery_after_segments_eviction(self):
        """
        Test checking if the offset returned by time based index is
        valid during applying log cleanup policy
        """
        segment_size = 1048576

        # produce until segments have been compacted
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
            acks=-1,
        )

        # restart all nodes to force replicating raft configuration
        self.redpanda.restart_nodes(self.redpanda.nodes)

        kafka_tools = KafkaCliTools(self.redpanda)
        # Wait for controller, alter configs doesn't have a retry loop
        kafka_tools.describe_topic(self.topic)

        # change retention bytes to preserve 15 segments
        self.client().alter_topic_configs(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES:
                bytes_for_segments(2, segment_size),
            })

        def validate_time_query_until_deleted():
            def done():
                kcat = KafkaCat(self.redpanda)
                ts = 1638748800  # 12.6.2021 - old timestamp, query first offset
                offset = kcat.query_offset(self.topic, 0, ts)
                # assert that offset is valid
                assert offset >= 0

                topic_partitions = segments_count(self.redpanda, self.topic, 0)
                partitions = []
                for p in topic_partitions:
                    partitions.append(p <= 5)
                return all([p <= 5 for p in topic_partitions])

            wait_until(done,
                       timeout_sec=30,
                       backoff_sec=5,
                       err_msg="Segments were not removed")

        validate_time_query_until_deleted()


class ShadowIndexingRetentionTest(RedpandaTest):
    segment_size = 1000000  # 1MB
    default_retention_segments = 2
    retention_segments = 4
    total_segments = 10
    topic_name = "si_test_topic"

    def __init__(self, test_context):
        extra_rp_conf = dict(log_compaction_interval_ms=1000,
                             retention_local_target_bytes_default=self.
                             default_retention_segments * self.segment_size)

        super(ShadowIndexingRetentionTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=SISettings(log_segment_size=self.segment_size),
            extra_rp_conf=extra_rp_conf,
            log_level="trace")

        self.rpk = RpkTool(self.redpanda)

    def segments_removed(self, limit: int):
        segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
            "kafka", self.topic_name, 0)
        self.logger.debug(f"Current segments: {segs}")

        return len(segs) <= limit

    @cluster(num_nodes=1)
    @matrix(cluster_remote_write=[True, False],
            topic_remote_write=["true", "false", "-1"])
    def test_shadow_indexing_default_local_retention(self,
                                                     cluster_remote_write,
                                                     topic_remote_write):
        """
        Test the default local retention on topics with remote write enabled.
        The retention.local-target topic configuration properties control
        the local retention of topics with remote write enabled. The defaults
        for these topic configs are controlled via cluster level configs:
        * retention_local_target_bytes_ms_default
        * retention_local_target_bytes_bytes_default

        This test goes through all possible combinations of cluster and topic
        level remote write configurations and checks if segments were removed
        at the end.
        """
        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_remote_write": cluster_remote_write},
            expect_restart=True)

        self.rpk.create_topic(topic=self.topic_name,
                              partitions=1,
                              replicas=1,
                              config={
                                  "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                                  "redpanda.remote.write": topic_remote_write
                              })

        expect_deletion = cluster_remote_write or topic_remote_write == "true"

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=self.total_segments *
                            self.segment_size)

        if expect_deletion:
            wait_until(
                lambda: self.segments_removed(self.default_retention_segments),
                timeout_sec=5,
                backoff_sec=1,
                err_msg=f"Segments were not removed")
        else:
            with expect_exception(TimeoutError, lambda e: True):
                wait_until(lambda: self.segments_removed(
                    self.default_retention_segments),
                           timeout_sec=5,
                           backoff_sec=1)

    @cluster(num_nodes=1)
    def test_shadow_indexing_non_default_local_retention(self):
        """
        Test that the topic level retention.local-target.bytes config
        overrides the cluster level default.
        """
        self.rpk.create_topic(topic=self.topic_name,
                              partitions=1,
                              replicas=1,
                              config={
                                  "cleanup.policy":
                                  TopicSpec.CLEANUP_DELETE,
                                  "redpanda.remote.write":
                                  "true",
                                  "retention.local-target.bytes":
                                  str(self.segment_size *
                                      self.retention_segments)
                              })

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=self.total_segments *
                            self.segment_size)

        wait_until(lambda: self.segments_removed(self.retention_segments),
                   timeout_sec=5,
                   backoff_sec=1,
                   err_msg=f"Segments were not removed")
