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
from rptest.services.redpanda import SISettings
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST


class MultiRestartTest(EndToEndTest):
    log_segment_size = 5048576  # 5MB
    log_compaction_interval_ms = 25000

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=self.log_compaction_interval_ms, )

        super(MultiRestartTest, self).__init__(test_context=test_context,
                                               extra_rp_conf=extra_rp_conf)

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        super().tearDown()

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_recovery_after_multiple_restarts(self):
        # If a debug build has to do a restart across a significant
        # number of partitions, it gets slow.  Use fewer partitions
        # on debug builds.
        partition_count = 10 if self.debug_mode else 60

        si_settings = SISettings(cloud_storage_reconciliation_interval_ms=500,
                                 cloud_storage_max_connections=5,
                                 log_segment_size=self.log_segment_size)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

        self.start_redpanda(3,
                            extra_rp_conf=self._extra_rp_conf,
                            si_settings=si_settings)
        spec = TopicSpec(partition_count=partition_count, replication_factor=3)

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
