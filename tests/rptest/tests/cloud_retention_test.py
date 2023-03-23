# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from ducktape.mark import matrix, parametrize
from ducktape.errors import TimeoutError

from rptest.services.cluster import cluster
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import CloudStorageType, SISettings, MetricsEndpoint, CloudStorageType, CHAOS_LOG_ALLOW_LIST, get_cloud_storage_type
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer, KgoVerifierProducer)
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.si_utils import BucketView
from rptest.services.action_injector import ActionConfig, random_process_kills


class CloudRetentionTest(PreallocNodesTest):
    default_retention_segments = 2
    topic_name = "si_test_topic"

    def __init__(self, test_context):
        super(CloudRetentionTest, self).__init__(test_context=test_context,
                                                 node_prealloc_count=1,
                                                 num_brokers=3)

    def setUp(self):
        # Defer cluster startup so we can tweak configs based on the type of
        # test run.
        pass

    @cluster(num_nodes=4)
    @matrix(max_consume_rate_mb=[20, None],
            cloud_storage_type=get_cloud_storage_type())
    @skip_debug_mode
    def test_cloud_retention(self, max_consume_rate_mb, cloud_storage_type):
        """
        Test cloud retention with an ongoing workload. The consume load comes
        in two flavours:
        * an ordinary fast consumer that repeatedly consumes the tail few
          segments
        * a slow consumer that can't keep up with retention and has to start
          over when retention overcomes it
        """
        if self.redpanda.dedicated_nodes:
            num_partitions = 100
            msg_size = 128 * 1024
            msg_count = int(100 * 1024 * 1024 * 1024 / msg_size)
            segment_size = 10 * 1024 * 1024
        else:
            num_partitions = 1
            msg_size = 1024
            msg_count = int(1024 * 1024 * 1024 / msg_size)
            segment_size = 1024 * 1024

        if max_consume_rate_mb is None:
            max_read_msgs = 25000
        else:
            max_read_msgs = 2000

        si_settings = SISettings(self.test_context,
                                 log_segment_size=segment_size,
                                 fast_uploads=True)
        self.redpanda.set_si_settings(si_settings)

        extra_rp_conf = dict(retention_local_target_bytes_default=self.
                             default_retention_segments * segment_size,
                             log_segment_size_jitter_percent=5,
                             group_initial_rebalance_delay=300,
                             cloud_storage_segment_max_upload_interval_sec=60,
                             cloud_storage_housekeeping_interval_ms=10_000)
        self.redpanda.add_extra_rp_conf(extra_rp_conf)

        self.redpanda.start()
        self._create_initial_topics()

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic=self.topic_name,
                         partitions=num_partitions,
                         replicas=3,
                         config={
                             "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                             "retention.bytes": 5 * segment_size,
                         })

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       custom_node=self.preallocated_nodes)

        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size,
            readers=3,
            loop=True,
            max_msgs=max_read_msgs,
            nodes=self.preallocated_nodes)

        producer.start(clean=False)

        producer.wait_for_offset_map()
        consumer.start(clean=False)

        producer.wait_for_acks(msg_count, timeout_sec=600, backoff_sec=5)
        producer.wait()
        self.logger.info("finished producing")
        topics = (TopicSpec(name=self.topic_name,
                            partition_count=num_partitions), )

        def first_segment_missing():
            s3_snapshot = BucketView(self.redpanda, topics=topics)
            try:
                manifest = s3_snapshot.manifest_for_ntp(self.topic_name, 0)
            except:
                return False

            if "segments" not in manifest:
                return False
            return "0-1-v1.log" not in manifest["segments"], manifest

        # Wait for some segments to be deleted.
        wait_until(first_segment_missing, timeout_sec=60, backoff_sec=5)

        def check_bucket_size():
            try:
                size = sum(obj.content_length
                           for obj in self.cloud_storage_client.list_objects(
                               si_settings.cloud_storage_bucket))
                self.logger.info(f"bucket size: {size}")
                # check that for each partition there is more than 1
                # and less than 10 segments in the cloud (generous limits)
                return size >= segment_size * num_partitions \
                    and size <= 10 * segment_size * num_partitions
            except Exception as e:
                self.logger.warn(f"error getting bucket size: {e}")
                return False

        wait_until(check_bucket_size, timeout_sec=60, backoff_sec=5)

        consumer.wait()
        self.logger.info("finished consuming")
        assert consumer.consumer_status.validator.valid_reads > \
            segment_size * num_partitions / msg_size

    @cluster(num_nodes=4)
    @skip_debug_mode
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_gc_entire_manifest(self, cloud_storage_type):
        """
        Regression test for #8945, where GCing all cloud segments could prevent
        further uploads from taking place.
        """
        small_segment_size = 1024 * 1024
        num_partitions = 16
        si_settings = SISettings(self.test_context,
                                 log_segment_size=small_segment_size,
                                 fast_uploads=True)
        self.redpanda.set_si_settings(si_settings)
        extra_rp_conf = dict(retention_local_target_bytes_default=self.
                             default_retention_segments * small_segment_size,
                             log_segment_size_jitter_percent=5,
                             group_initial_rebalance_delay=300,
                             cloud_storage_segment_max_upload_interval_sec=1,
                             cloud_storage_housekeeping_interval_ms=1000)
        self.redpanda.add_extra_rp_conf(extra_rp_conf)
        self.redpanda.start()
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic=self.topic_name,
                         partitions=num_partitions,
                         replicas=3,
                         config={
                             "cleanup.policy":
                             TopicSpec.CLEANUP_DELETE,
                             "retention.local.target.bytes":
                             2 * small_segment_size,
                         })

        # Write more data than we intend to retain.
        msg_size = 4 * 1024
        msg_count = int(num_partitions * 1024 * 1024 / msg_size)
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       custom_node=self.preallocated_nodes)
        producer.start(clean=False)
        producer.wait()

        topics = (TopicSpec(name=self.topic_name,
                            partition_count=num_partitions), )

        def uploaded_all_partitions():
            s3_snapshot = BucketView(self.redpanda, topics=topics)
            for partition in range(0, num_partitions):
                size = s3_snapshot.cloud_log_size_for_ntp(
                    self.topic_name, partition)
                if size == 0:
                    self.logger.info(f"Partition {partition} has size 0")
                    return False

            return True

        wait_until(uploaded_all_partitions,
                   timeout_sec=60,
                   backoff_sec=5,
                   err_msg="Waiting for all parents to upload cloud data")

        def gced_all_segments():
            s3_snapshot = BucketView(self.redpanda, topics=topics)
            for partition in range(0, num_partitions):
                try:
                    manifest = s3_snapshot.manifest_for_ntp(
                        self.topic_name, partition)
                except:
                    self.logger.info(
                        f"Partition {partition} has no uploaded manifest")
                    return False

                # Wait for the manifest to have uploaded some offsets, but not have
                # any segments, indicating we truncated.
                if "last_offset" not in manifest or manifest[
                        "last_offset"] == 0:
                    self.logger.info(
                        f"Partition {partition} doesn't have last_offset")
                    return False

                if not "segments" not in manifest:
                    self.logger.info(
                        f"Partition {partition} still has segments")
                    return False

            return True

        rpk.alter_topic_config(self.topic_name, "retention.bytes", "1")
        wait_until(gced_all_segments,
                   timeout_sec=120,
                   backoff_sec=5,
                   err_msg="Waiting for all cloud segments to be GC")
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       custom_node=self.preallocated_nodes)
        producer.start(clean=False)
        producer.wait()


class CloudRetentionTimelyGCTest(RedpandaTest):
    segment_size = 256 * 1024
    retention_bytes = 10 * segment_size

    topics = (TopicSpec(name="panda-topic",
                        partition_count=1,
                        replication_factor=3,
                        retention_bytes=retention_bytes,
                        segment_bytes=segment_size), )

    def __init__(self, test_context):
        si_settings = SISettings(test_context,
                                 log_segment_size=self.segment_size,
                                 fast_uploads=True)
        self.s3_bucket_name = si_settings.cloud_storage_bucket
        super().__init__(
            test_context,
            si_settings=si_settings,
            extra_rp_conf={'cloud_storage_housekeeping_interval_ms': 1000 * 2})

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @skip_debug_mode
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_retention_with_node_failures(self, cloud_storage_type):
        max_overshoot_percentage = 100
        runtime = 120

        # Write fast enough that in the test's runtime, we would certainly
        # overshoot the retention size substantially if retention wasn't working.
        write_rate = (self.retention_bytes * 4.0) / runtime

        # Write enough data to last the runtime.
        msg_size = 128
        msg_count = write_rate * runtime // msg_size

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_count=msg_count,
                                       msg_size=msg_size,
                                       rate_limit_bps=write_rate)
        producer.start()

        def cloud_log_size() -> int:
            s3_snapshot = BucketView(self.redpanda, topics=self.topics)
            if not s3_snapshot.is_ntp_in_manifest(self.topic, 0):
                self.logger.debug(f"No manifest present yet")
                return 0

            cloud_log_size = s3_snapshot.cloud_log_size_for_ntp(self.topic, 0)
            ratio = cloud_log_size / self.retention_bytes
            overshoot_percentage = max(ratio - 1, 0) * 100

            self.logger.debug(f"Current cloud log size is: {cloud_log_size}")
            self.logger.debug(f"Overshot by {overshoot_percentage}%")

            if overshoot_percentage > max_overshoot_percentage:
                raise RuntimeError(
                    f"Cloud log size {overshoot_percentage}% greater than configured"
                    f" retention (max allowed {max_overshoot_percentage}%)")

            return cloud_log_size

        pkill_config = ActionConfig(cluster_start_lead_time_sec=10,
                                    min_time_between_actions_sec=10,
                                    max_time_between_actions_sec=20)
        with random_process_kills(self.redpanda, pkill_config) as ctx:
            try:
                wait_until(lambda: cloud_log_size() == -1,
                           timeout_sec=runtime,
                           backoff_sec=5)
            except TimeoutError as e:
                # This is the success path. Timing out means that
                # we've stayed below the max cloud log size threshold
                # for the duration of the test.
                pass
            finally:
                producer.stop()

        ctx.assert_actions_triggered()
