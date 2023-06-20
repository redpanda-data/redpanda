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


class CloudArchiveRetentionTest(RedpandaTest):
    """Test retention with spillover enabled"""
    def __init__(self, test_context):
        self.extra_rp_conf = dict(
            log_compaction_interval_ms=1000,
            cloud_storage_spillover_manifest_max_segments=5)

        si_settings = SISettings(test_context,
                                 cloud_storage_housekeeping_interval_ms=1000,
                                 log_segment_size=4096,
                                 fast_uploads=True)

        super(CloudArchiveRetentionTest,
              self).__init__(test_context=test_context,
                             si_settings=si_settings,
                             extra_rp_conf=self.extra_rp_conf,
                             log_level="debug")

        self.rpk = RpkTool(self.redpanda)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

    def produce(self, topic, msg_count):
        msg_size = 1024 * 256
        topic_name = topic.name
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count)

        producer.start()
        producer.wait()
        producer.free()

        def all_partitions_spilled():
            return self.num_manifests_uploaded() > 0

        wait_until(all_partitions_spilled, timeout_sec=180, backoff_sec=10)

    def num_manifests_uploaded(self):
        s = self.redpanda.metric_sum(
            metric_name=
            "redpanda_cloud_storage_spillover_manifest_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        self.logger.info(
            f"redpanda_cloud_storage_spillover_manifest_uploads = {s}")
        return s

    def num_segments_deleted(self):
        s = self.redpanda.metric_sum(
            metric_name="redpanda_cloud_storage_deleted_segments_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        self.logger.info(
            f"redpanda_cloud_storage_deleted_segments_total = {s}")
        return s

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type(),
            retention_type=['retention.ms', 'retention.bytes'])
    def test_delete(self, cloud_storage_type, retention_type):
        topic = TopicSpec(redpanda_remote_read=True,
                          redpanda_remote_write=True,
                          cleanup_policy=TopicSpec.CLEANUP_DELETE)

        self.client().create_topic(topic)

        def wait_for_topic():
            wait_until(lambda: len(
                list(self.client().describe_topic(topic.name))) > 0,
                       30,
                       backoff_sec=2)

        def produce_until_spillover():
            initial_uploaded = self.num_manifests_uploaded()

            def new_manifest_spilled():
                time.sleep(5)
                self.produce(topic, 250)
                num_spilled = self.num_manifests_uploaded()
                return num_spilled > initial_uploaded

            wait_until(new_manifest_spilled,
                       timeout_sec=120,
                       backoff_sec=2,
                       err_msg="Manifests were not created")

        wait_for_topic()
        produce_until_spillover()

        # Enable cloud retention for the topic to force deleting data
        # from the 'archive'
        wait_for_topic()
        self.client().alter_topic_config(topic.name, retention_type, 1000)

        wait_until(lambda: self.num_segments_deleted() > 50,
                   timeout_sec=100,
                   backoff_sec=5,
                   err_msg=f"Segments were not removed from the cloud")
