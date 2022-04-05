# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid

from rptest.services.cluster import cluster
from rptest.archival.s3_client import S3Client
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST


class MultiRestartTest(EndToEndTest):

    log_segment_size = 5048576  # 5MB
    log_compaction_interval_ms = 25000

    s3_host_name = "minio-s3"
    s3_access_key = "panda-user"
    s3_secret_key = "panda-secret"
    s3_region = "panda-region"
    s3_topic_name = "panda-topic"

    def __init__(self, test_context):
        self.s3_bucket_name = f"panda-bucket-{uuid.uuid1()}"
        self._extra_rp_conf = dict(
            cloud_storage_enabled=True,
            cloud_storage_access_key=MultiRestartTest.s3_access_key,
            cloud_storage_secret_key=MultiRestartTest.s3_secret_key,
            cloud_storage_region=MultiRestartTest.s3_region,
            cloud_storage_bucket=self.s3_bucket_name,
            cloud_storage_disable_tls=True,
            cloud_storage_api_endpoint=MultiRestartTest.s3_host_name,
            cloud_storage_api_endpoint_port=9000,
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_compaction_interval_ms=self.log_compaction_interval_ms,
            log_segment_size=self.log_segment_size,
        )

        super(MultiRestartTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=self._extra_rp_conf)

        self.s3_client = S3Client(
            region='panda-region',
            access_key=u"panda-user",
            secret_key=u"panda-secret",
            endpoint=f'http://{MultiRestartTest.s3_host_name}:9000',
            logger=self.logger)

    def setUp(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        self.s3_client.create_bucket(self.s3_bucket_name)

        super(MultiRestartTest, self).setUp()

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        super().tearDown()

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_recovery_after_multiple_restarts(self):
        self.start_redpanda(3, extra_rp_conf=self._extra_rp_conf)
        spec = TopicSpec(partition_count=60, replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)
        self.topic = spec.name

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(spec.name, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(spec.name, 'redpanda.remote.read', 'true')

        self.start_producer(1, throughput=100)
        self.start_consumer(1)
        self.await_startup()

        def no_under_replicated_partitions():
            metric_sample = self.redpanda.metrics_sample("under_replicated")
            for s in metric_sample.samples:
                if s.value > 0:
                    return False
            return True

        # restart all the nodes and wait for recovery
        for i in range(0, 10):
            for n in self.redpanda.nodes:
                self.redpanda.signal_redpanda(n)
                self.redpanda.start_node(n)
            wait_until(no_under_replicated_partitions, 30, 2)

        self.run_validation(enable_idempotence=False,
                            producer_timeout_sec=60,
                            consumer_timeout_sec=180)
