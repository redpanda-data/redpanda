# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import sleep
import time
from ducktape.errors import TimeoutError
from ducktape.mark import matrix, parametrize
from ducktape.utils.util import wait_until

from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings, MetricsEndpoint, get_cloud_storage_type
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (produce_until_segments, produce_total_bytes,
                         wait_for_local_storage_truncate, segments_count,
                         expect_exception)
from rptest.utils.si_utils import BucketView


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
            acks=[1, -1],
            cloud_storage_type=get_cloud_storage_type())
    def test_changing_topic_retention(self, property, acks,
                                      cloud_storage_type):
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
        if property == TopicSpec.PROPERTY_RETENTION_BYTES:
            local_retention = 10000
            self.client().alter_topic_configs(self.topic, {
                property: local_retention,
            })
            wait_for_local_storage_truncate(redpanda=self.redpanda,
                                            topic=self.topic,
                                            target_bytes=local_retention)
        else:
            # Set a tiny time retention, and wait for some local segments
            # to be removed.
            self.client().alter_topic_configs(self.topic, {
                property: 10000,
            })
            wait_until(lambda: next(
                segments_count(self.redpanda, self.topic, 0)) <= 5,
                       timeout_sec=120)

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_changing_topic_retention_with_restart(self, cloud_storage_type):
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

        # Retain progressively less data, and validate that retention policy is applied
        for retain_segments in (15, 10, 5):
            local_retention = bytes_for_segments(retain_segments, segment_size)
            self.client().alter_topic_configs(
                self.topic,
                {TopicSpec.PROPERTY_RETENTION_BYTES: local_retention})
            wait_for_local_storage_truncate(self.redpanda,
                                            self.topic,
                                            target_bytes=local_retention)


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

        si_settings = SISettings(test_context,
                                 log_segment_size=self.segment_size)
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
            topic_remote_write=["true", "false", "-1"],
            cloud_storage_type=get_cloud_storage_type())
    def test_shadow_indexing_default_local_retention(self,
                                                     cluster_remote_write,
                                                     topic_remote_write,
                                                     cloud_storage_type):
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
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_shadow_indexing_non_default_local_retention(
            self, cloud_storage_type):
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
                            bytes_to_produce=self.total_segments *
                            self.segment_size)

        # Wait up to just bit a longer than the 10s default config value for
        # `cloud_storage_upload_loop_max_backoff_ms`.
        wait_until(lambda: self.segments_removed(self.retention_segments),
                   timeout_sec=15,
                   backoff_sec=1,
                   err_msg=f"Segments were not removed")

    @cluster(num_nodes=1)
    @matrix(local_retention_ms=[3600000, -1],
            cloud_storage_type=get_cloud_storage_type())
    def test_local_time_based_retention_is_overridden(self, local_retention_ms,
                                                      cloud_storage_type):
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

        si_settings = SISettings(test_context,
                                 log_segment_size=self.segment_size,
                                 fast_uploads=True)
        super(ShadowIndexingCloudRetentionTest,
              self).__init__(test_context=test_context,
                             si_settings=si_settings,
                             extra_rp_conf=extra_rp_conf,
                             log_level="trace")

        self.rpk = RpkTool(self.redpanda)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_cloud_retention_deleted_segments_count(self, cloud_storage_type):
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

        # https://github.com/redpanda-data/redpanda/issues/8658#issuecomment-1420905967
        wait_until(lambda: 9 <= deleted_segments_count() <= 10,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg=f"Segments were not removed from the cloud")

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_cloud_size_based_retention(self, cloud_storage_type):
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
                            bytes_to_produce=total_bytes)

        def cloud_log_size() -> int:
            s3_snapshot = BucketView(self.redpanda, topics=[topic])
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
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_cloud_time_based_retention(self, cloud_storage_type):
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
                            bytes_to_produce=total_bytes)

        def cloud_log_segment_count() -> int:
            s3_snapshot = BucketView(self.redpanda, topics=[topic])
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
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_cloud_size_based_retention_application(self, cloud_storage_type):
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
                            bytes_to_produce=total_bytes)

        def ntp_in_manifest() -> int:
            s3_snapshot = BucketView(self.redpanda, topics=[topic])
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
            s3_snapshot = BucketView(self.redpanda, topics=[topic])
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


class BogusTimestampTest(RedpandaTest):
    segment_size = 1048576
    topics = (
        TopicSpec(
            partition_count=1,
            replication_factor=1,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
            # 1 megabyte segments
            segment_bytes=segment_size,
            # 1 second retention: any non-open segment should be collected almost immediately
            retention_ms=1000), )

    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         extra_rp_conf={'log_compaction_interval_ms': 1000},
                         **kwargs)

    @cluster(num_nodes=2)
    @parametrize(mixed_timestamps=False)
    @parametrize(mixed_timestamps=True)
    def test_bogus_timestamps(self, mixed_timestamps):
        """
        :param mixed_timestamps: if true, test with a mixture of valid and invalid
        timestamps in the same segment (i.e. timestamp adjustment should use the
        valid timestamps rather than falling back to mtime)
        """
        self.client().alter_topic_config(self.topic, 'segment.bytes',
                                         str(self.segment_size))

        # A fictional artificial timestamp base in milliseconds
        future_timestamp = (int(time.time()) + 24 * 3600) * 1000

        # Produce a run of messages with CreateTime-style timestamps, each
        # record having a timestamp 1ms greater than the last.
        msg_size = 14000
        segments_count = 10
        msg_count = (self.segment_size // msg_size) * segments_count

        if mixed_timestamps:
            valid_records = (self.segment_size // msg_size) // 2

            # Write enough valid-timestamped records that one should appear in the index
            producer = KgoVerifierProducer(context=self.test_context,
                                           redpanda=self.redpanda,
                                           topic=self.topic,
                                           msg_size=msg_size,
                                           msg_count=valid_records,
                                           batch_max_bytes=msg_size * 2)
            producer.start()
            producer.wait()
            producer.free()

            # Write the rest of the messages with invalid timestamps
            producer = KgoVerifierProducer(context=self.test_context,
                                           redpanda=self.redpanda,
                                           topic=self.topic,
                                           msg_size=msg_size,
                                           msg_count=msg_count - valid_records,
                                           fake_timestamp_ms=future_timestamp,
                                           batch_max_bytes=msg_size * 2)
            producer.start()
            producer.wait()
        else:
            # Write msg_count messages with timestamps in the future
            producer = KgoVerifierProducer(
                context=self.test_context,
                redpanda=self.redpanda,
                topic=self.topic,
                msg_size=msg_size,
                msg_count=(self.segment_size // msg_size) * segments_count,
                fake_timestamp_ms=future_timestamp,
                batch_max_bytes=msg_size * 2)
            producer.start()
            producer.wait()

        # We should have written the expected number of segments, and nothing can
        # have been gc'd yet because all the segments have max timestmap in the future
        segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
            "kafka", self.topic, 0)
        self.logger.debug(f"Segments after write: {segs}")
        assert len(segs) >= segments_count

        # Give retention code some time to kick in: we are expecting that it does not
        # remove anything because the timestamps are too far in the future.
        with self.redpanda.monitor_log(self.redpanda.nodes[0]) as mon:
            # Time period much larger than what we set log_compaction_interval_ms to
            sleep(10)

            # Even without setting the storage_ignore_timestamps_in_future_sec parameter,
            # we should get warnings about the timestamps.
            mon.wait_until("found segment with bogus retention timestamp",
                           timeout_sec=30,
                           backoff_sec=1)

        # The GC should not have deleted anything
        segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
            "kafka", self.topic, 0)
        self.logger.debug(f"Segments after first GC: {segs}")
        assert len(segs) >= segments_count

        # Enable the escape hatch to tell Redpanda to correct timestamps that are
        # too far in the future
        with self.redpanda.monitor_log(self.redpanda.nodes[0]) as mon:
            self.redpanda.set_cluster_config({
                'storage_ignore_timestamps_in_future_sec':
                60,
            })

            if mixed_timestamps:
                mon.wait_until(
                    "Adjusting retention timestamp.*to max valid record",
                    timeout_sec=30,
                    backoff_sec=1)
            else:
                mon.wait_until("Adjusting retention timestamp.*to file mtime",
                               timeout_sec=30,
                               backoff_sec=1)

        def prefix_truncated():
            segs = self.redpanda.node_storage(self.redpanda.nodes[0]).segments(
                "kafka", self.topic, 0)
            self.logger.debug(f"Segments: {segs}")
            return len(segs) <= 1

        # Segments should be cleaned up now that we've switched on force-correction
        # of timestamps in the future
        self.redpanda.wait_until(prefix_truncated,
                                 timeout_sec=30,
                                 backoff_sec=1)
