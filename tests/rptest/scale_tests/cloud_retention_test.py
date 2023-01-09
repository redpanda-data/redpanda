# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.services.cluster import cluster
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import SISettings, MetricsEndpoint
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer, KgoVerifierProducer)


class CloudRetentionTest(PreallocNodesTest):
    segment_size = 10 * 1024 * 1024
    default_retention_segments = 2
    topic_name = "si_test_topic"

    def __init__(self, test_context):
        extra_rp_conf = dict(retention_local_target_bytes_default=self.
                             default_retention_segments * self.segment_size,
                             log_segment_size_jitter_percent=5,
                             group_initial_rebalance_delay=300,
                             cloud_storage_segment_max_upload_interval_sec=60,
                             cloud_storage_housekeeping_interval_ms=10_000)

        super(CloudRetentionTest, self).__init__(
            test_context=test_context,
            node_prealloc_count=1,
            num_brokers=3,
            si_settings=SISettings(log_segment_size=self.segment_size),
            extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=4)
    @matrix(max_consume_rate_mb=[20, None])
    def test_cloud_retention(self, max_consume_rate_mb):
        """
        Test cloud retention under load. The consume load comes in two flavours:
        * an ordinary fast consumer that repeatedly consumes the tail few segments
        * a slow consumer that can't keep up with retention and has to start over
          when retention overcomes it
        """
        num_partitions = 100
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic=self.topic_name,
                         partitions=num_partitions,
                         replicas=3,
                         config={
                             "cleanup.policy": TopicSpec.CLEANUP_DELETE,
                             "retention.bytes": 5 * self.segment_size,
                         })

        msg_size = 128 * 1024
        msg_count = int(100 * 1024 * 1024 * 1024 / msg_size)
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       custom_node=self.preallocated_nodes)

        if max_consume_rate_mb is None:
            max_read_msgs = 25000
        else:
            max_read_msgs = 2000
        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size,
            readers=3,
            loop=True,
            max_msgs=max_read_msgs,
            max_throughput_mb=max_consume_rate_mb,
            nodes=self.preallocated_nodes)

        producer.start(clean=False)

        producer.wait_for_offset_map()
        consumer.start(clean=False)

        producer.wait_for_acks(msg_count, timeout_sec=600, backoff_sec=5)
        producer.wait()
        self.logger.info("finished producing")

        def deleted_segments_count() -> int:
            metrics = self.redpanda.metrics_sample(
                "deleted_segments",
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)

            assert metrics, "Deleted segments metric is missing"
            self.logger.debug(f"Samples: {metrics.samples}")

            deleted = sum(int(s.value) for s in metrics.samples)
            self.logger.debug(f"Deleted {deleted} segments from the cloud")
            return deleted

        def check_num_deleted():
            num_deleted = deleted_segments_count()
            self.logger.info(f"number of deleted segments: {num_deleted}")
            return num_deleted > 0

        wait_until(check_num_deleted, timeout_sec=60, backoff_sec=5)

        def check_bucket_size():
            try:
                size = sum(obj.content_length
                           for obj in self.s3_client.list_objects(
                               self.si_settings.cloud_storage_bucket))
                self.logger.info(f"bucket size: {size}")
                # check that for each partition there is more than 1
                # and less than 10 segments in the cloud (generous limits)
                return size >= self.segment_size * num_partitions \
                    and size <= 10 * self.segment_size * num_partitions
            except Exception as e:
                self.logger.warn(f"error getting bucket size: {e}")
                return False

        wait_until(check_bucket_size, timeout_sec=60, backoff_sec=5)

        consumer.wait()
        self.logger.info("finished consuming")
        assert consumer.consumer_status.validator.valid_reads > \
            self.segment_size * num_partitions / msg_size
