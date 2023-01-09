# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import sleep
from ducktape.errors import TimeoutError
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (produce_until_segments, produce_total_bytes,
                         wait_for_segments_removal, segments_count,
                         expect_exception)
from rptest.utils.si_utils import S3Snapshot


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


class ShadowIndexingLocalRetentionTest(RedpandaTest):
    segment_size = 1000000  # 1MB
    default_retention_segments = 2
    retention_segments = 4
    total_segments = 10
    topic_name = "si_test_topic"

    def __init__(self, test_context):
        extra_rp_conf = dict(log_compaction_interval_ms=1000,
                             retention_local_target_bytes_default=self.
                             default_retention_segments * self.segment_size)

        si_settings = SISettings(log_segment_size=self.segment_size)
        super(ShadowIndexingLocalRetentionTest,
              self).__init__(test_context=test_context,
                             num_brokers=1,
                             si_settings=si_settings,
                             extra_rp_conf=extra_rp_conf,
                             log_level="trace")

        self.rpk = RpkTool(self.redpanda)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

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
        The retention.local.target topic configuration properties control
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

        if topic_remote_write != "-1":
            expect_deletion = topic_remote_write == "true"
        else:
            expect_deletion = cluster_remote_write

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=self.total_segments *
                            self.segment_size)

        if expect_deletion:
            # Wait up to just a bit longer than the 10s default config value
            # for `cloud_storage_upload_loop_max_backoff_ms`.
            wait_until(
                lambda: self.segments_removed(self.default_retention_segments),
                timeout_sec=15,
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
        Test that the topic level retention.local.target.bytes config
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
                                  "retention.local.target.bytes":
                                  str(self.segment_size *
                                      self.retention_segments)
                              })

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=self.total_segments *
                            self.segment_size)

        # Wait up to just bit a longer than the 10s default config value for
        # `cloud_storage_upload_loop_max_backoff_ms`.
        wait_until(lambda: self.segments_removed(self.retention_segments),
                   timeout_sec=15,
                   backoff_sec=1,
                   err_msg=f"Segments were not removed")

    @cluster(num_nodes=1)
    @matrix(local_retention_ms=[3600000, -1])
    def test_local_time_based_retention_is_overridden(self,
                                                      local_retention_ms):
        """
        Checks if local time based retention is overridden 
        by cloud based retention settings if cloud based retention is more strict
        """
        # set cloud retention to 10 seconds
        self.redpanda.set_cluster_config({"delete_retention_ms": 10000},
                                         expect_restart=False)

        # create topic with large local retention
        self.rpk.create_topic(topic=self.topic_name,
                              partitions=1,
                              replicas=1,
                              config={
                                  "cleanup.policy":
                                  TopicSpec.CLEANUP_DELETE,
                                  "redpanda.remote.write":
                                  "true",
                                  "retention.local.target.ms":
                                  str(local_retention_ms)
                              })

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=self.total_segments *
                            self.segment_size)

        wait_until(lambda: self.segments_removed(1),
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg=f"Segments were not removed")


class ShadowIndexingCloudRetentionTest(RedpandaTest):
    segment_size = 1000000  # 1MB
    topic_name = "si_test_topic"

    def __init__(self, test_context):
        extra_rp_conf = dict(log_compaction_interval_ms=1000)

        si_settings = SISettings(log_segment_size=self.segment_size)
        super(ShadowIndexingCloudRetentionTest,
              self).__init__(test_context=test_context,
                             si_settings=si_settings,
                             extra_rp_conf=extra_rp_conf,
                             log_level="trace")

        self.rpk = RpkTool(self.redpanda)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

    @cluster(num_nodes=3)
    def test_cloud_retention_deleted_segments_count(self):
        """
        Test that retention deletes the right number of segments. The test sets the
        cloud retention limit to 10 segments and then produces 20 segments.
        
        We check via the redpanda_cloud_storage_deleted_segments that 10 segments
        have been deleted from the cloud.
        """
        self.redpanda.set_cluster_config(
            {
                "cloud_storage_enable_remote_write": True,
                "cloud_storage_housekeeping_interval_ms": 100
            },
            expect_restart=True)

        self.rpk.create_topic(topic=self.topic_name,
                              partitions=1,
                              replicas=1,
                              config={
                                  "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                                  "retention.bytes": 10 * self.segment_size,
                              })

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=20 * self.segment_size)

        def deleted_segments_count() -> int:
            metrics = self.redpanda.metrics_sample(
                "deleted_segments",
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)

            assert metrics, "Deleted segments metric is missing"
            assert len(metrics.samples) == 1

            deleted = int(metrics.samples[0].value)
            self.logger.debug(f"Samples: {metrics.samples}")
            self.logger.debug(f"Deleted {deleted} segments from the cloud")
            return deleted

        wait_until(lambda: deleted_segments_count() == 10,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg=f"Segments were not removed from the cloud")

    @cluster(num_nodes=3)
    def test_cloud_size_based_retention(self):
        """
        Test that retention is enforced in the cloud log by checking
        the total size of segments in the manifest.
        """
        retention_bytes = 10 * self.segment_size
        total_bytes = 20 * self.segment_size

        topic = TopicSpec(name=self.topic_name,
                          partition_count=1,
                          replication_factor=3,
                          cleanup_policy=TopicSpec.CLEANUP_DELETE)

        self.redpanda.set_cluster_config(
            {
                "cloud_storage_enable_remote_write": True,
                "cloud_storage_housekeeping_interval_ms": 100
            },
            expect_restart=True)

        self.rpk.create_topic(topic=topic.name,
                              partitions=topic.partition_count,
                              replicas=topic.replication_factor,
                              config={
                                  "cleanup.policy": topic.cleanup_policy,
                              })

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=total_bytes)

        def cloud_log_size() -> int:
            s3_snapshot = S3Snapshot([topic],
                                     self.redpanda.cloud_storage_client,
                                     self.s3_bucket_name, self.logger)
            cloud_log_size = s3_snapshot.cloud_log_size_for_ntp(topic.name, 0)
            self.logger.debug(f"Current cloud log size is: {cloud_log_size}")
            return cloud_log_size

        # Wait for everything to be uploaded to the cloud.
        wait_until(lambda: cloud_log_size() >= total_bytes,
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Segments not uploaded")

        # Alter the topic's retention.bytes config to trigger removal of
        # segments in the cloud.
        self.client().alter_topic_configs(
            topic.name, {TopicSpec.PROPERTY_RETENTION_BYTES: retention_bytes})

        # Test that the size of the cloud log is below the retention threshold
        # by querying the manifest.
        wait_until(lambda: cloud_log_size() <= retention_bytes,
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Too many bytes in the cloud")

    @cluster(num_nodes=3)
    def test_cloud_time_based_retention(self):
        """
        Test that retention is enforced in the cloud log by checking
        the total size of segments in the manifest. The test steps are:
        1. Produce a fixed number of bytes to a partition
        2. Get the local segment count
        3. Wait until the number of segments in the cloud is the same as locally
        4. Set a short 'retention.ms' period for the topic
        5. Wait for all segments to be deleted from the cloud
        """
        total_bytes = 20 * self.segment_size

        topic = TopicSpec(name=self.topic_name,
                          partition_count=1,
                          replication_factor=3,
                          cleanup_policy=TopicSpec.CLEANUP_DELETE)

        self.redpanda.set_cluster_config(
            {
                "cloud_storage_enable_remote_write": True,
                "cloud_storage_housekeeping_interval_ms": 100
            },
            expect_restart=True)

        self.rpk.create_topic(topic=topic.name,
                              partitions=topic.partition_count,
                              replicas=topic.replication_factor,
                              config={
                                  "cleanup.policy": topic.cleanup_policy,
                              })

        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=total_bytes)

        def cloud_log_segment_count() -> int:
            s3_snapshot = S3Snapshot([topic],
                                     self.redpanda.cloud_storage_client,
                                     self.s3_bucket_name, self.logger)
            count = s3_snapshot.cloud_log_segment_count_for_ntp(topic.name, 0)
            self.logger.debug(
                f"Current count of segments in manifest is: {count}")
            return count

        local_seg_count = len(
            self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", topic.name, 0))

        self.logger.info(
            f"Waiting for {local_seg_count - 1} segments to be uploaded to the cloud"
        )
        # Wait for everything to be uploaded to the cloud.
        wait_until(lambda: cloud_log_segment_count() == local_seg_count - 1,
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Segments not uploaded")

        # Alter the topic's retention.ms config to trigger removal of
        # all segments in the cloud.
        self.client().alter_topic_configs(
            topic.name, {TopicSpec.PROPERTY_RETENTION_TIME: 10})

        # Check that all segments have been removed
        wait_until(lambda: cloud_log_segment_count() == 0,
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Not all segments were removed from the cloud")

    @cluster(num_nodes=1)
    def test_cloud_size_based_retention_application(self):
        """
        Test that retention is enforced when applied to topics that initially
        have all SI settings disabled.
        """
        total_bytes = 20 * self.segment_size
        retention_bytes = 10 * self.segment_size
        cs_housekeeping_interval = 100

        topic = TopicSpec(name=self.topic_name,
                          partition_count=1,
                          replication_factor=1,
                          cleanup_policy=TopicSpec.CLEANUP_DELETE)

        # Cluster-wide settings have remote_read & remote_write enabled
        self.redpanda.set_cluster_config(
            {
                "cloud_storage_enable_remote_write": True,
                "cloud_storage_enable_remote_read": True,
                "cloud_storage_housekeeping_interval_ms":
                cs_housekeeping_interval
            },
            expect_restart=True)

        # However, topic settings have main SI functionality disabled
        self.rpk.create_topic(topic=topic.name,
                              partitions=topic.partition_count,
                              replicas=topic.replication_factor,
                              config={
                                  "cleanup.policy": topic.cleanup_policy,
                                  "redpanda.remote.write": "false",
                                  "redpanda.remote.read": "false",
                              })

        before_alter = self.rpk.describe_topic_configs(topic.name)
        assert before_alter['redpanda.remote.write'][0] == 'false'
        assert before_alter['redpanda.remote.read'][0] == 'false'

        # Write some data to the topic, and assert that nothing has been
        # written out to S3
        produce_total_bytes(self.redpanda,
                            topic=self.topic_name,
                            partition_index=0,
                            bytes_to_produce=total_bytes)

        def ntp_in_manifest() -> int:
            s3_snapshot = S3Snapshot([topic],
                                     self.redpanda.cloud_storage_client,
                                     self.s3_bucket_name, self.logger)
            return s3_snapshot.is_ntp_in_manifest(topic.name, 0)

        # Sleep for 4 cloud_storage_housekeeping_interval_ms, then assert ntp does
        # not exist in manifest
        sleep(4 * cs_housekeeping_interval * 0.001)
        assert not ntp_in_manifest(
        ), "Segment uploaded, when it should not exist"

        # Now modify topic properties to enable SI read/write for the topic
        self.rpk.alter_topic_config(topic.name, "redpanda.remote.write",
                                    "true")
        self.rpk.alter_topic_config(topic.name, "redpanda.remote.read", "true")
        after_alter = self.rpk.describe_topic_configs(topic.name)
        assert after_alter['redpanda.remote.write'][0] == 'true'
        assert after_alter['redpanda.remote.read'][0] == 'true'

        # Wait for upload to occur first
        wait_until(lambda: ntp_in_manifest(), timeout_sec=10)

        def cloud_log_size() -> int:
            s3_snapshot = S3Snapshot([topic],
                                     self.redpanda.cloud_storage_client,
                                     self.s3_bucket_name, self.logger)
            cloud_log_size = s3_snapshot.cloud_log_size_for_ntp(topic.name, 0)
            self.logger.debug(f"Current cloud log size is: {cloud_log_size}")
            return cloud_log_size

        # Wait for everything to be uploaded to the cloud.
        wait_until(lambda: cloud_log_size() >= total_bytes,
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Segments not uploaded")

        # Modify retention settings
        self.client().alter_topic_configs(
            topic.name, {TopicSpec.PROPERTY_RETENTION_BYTES: retention_bytes})

        # Assert that retention policy has kicked in and with the desired
        # effect, i.e. total bytes is <= retention settings applied
        wait_until(lambda: cloud_log_size() <= retention_bytes,
                   timeout_sec=10,
                   backoff_sec=2,
                   err_msg=f"Too many bytes in the cloud")
