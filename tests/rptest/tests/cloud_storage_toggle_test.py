# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.services.kgo_verifier_services import KgoVerifierSeqConsumer

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import CloudStorageType, SISettings, MetricsEndpoint, CloudStorageType, CHAOS_LOG_ALLOW_LIST
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.si_utils import S3Snapshot


class CloudStorageToggleTest(RedpandaTest):
    partition_count = 16
    log_segment_size = 1024 * 1024

    topics = (TopicSpec(
        partition_count=partition_count,
        replication_factor=3,
    ), )

    def __init__(self, test_context, *args, **kwargs):
        self.si_settings = SISettings(test_context,
                                      log_segment_size=self.log_segment_size)
        super().__init__(test_context, si_settings=self.si_settings)

    @cluster(num_nodes=5)
    @skip_debug_mode
    def test_cloud_storage_toggle(self):
        """
        Check stability of the system when switching tiered storage on and off repeatedly
        while traffic is ongoing.
        """

        rpk = RpkTool(self.redpanda)

        # This many config changes
        cycles = 16
        # Must be even, to leave topic back in its enabled state.
        assert cycles % 2 == 0

        msg_size = 1024
        data_rate = 1024 * 1024 * 10

        # Drop local data quickly
        rpk.alter_topic_config(self.topic, "retention.local.target.bytes",
                               str(self.log_segment_size * 2))

        # A sequential consumer in a loop, to drive reads to tiered storage: we want reads in
        # flight at the time we are disabling tiered storage, to check for lifetime issues
        seq_consumer = KgoVerifierSeqConsumer(self.test_context,
                                              self.redpanda,
                                              self.topic,
                                              0,
                                              max_throughput_mb=data_rate //
                                              (1024 * 1024))
        seq_consumer.start()

        # Modest traffic: 8 clients, sending + receiving 128KiB batches at total 10MiB/s
        with repeater_traffic(context=self.test_context,
                              redpanda=self.redpanda,
                              topic=self.topic,
                              rate_limit_bps=data_rate,
                              msg_size=msg_size,
                              max_buffered_records=128 * 1024 // msg_size,
                              workers=8) as traffic:
            traffic.await_group_ready()

            for k in range(0, cycles):

                if k % 2 == 0:
                    # Even: disable tiered storage
                    self.logger.info("Disabling tiered storage")
                    rpk.alter_topic_config(self.topic, "redpanda.remote.read",
                                           "false")
                    rpk.alter_topic_config(self.topic, "redpanda.remote.write",
                                           "false")
                else:
                    # Odd: enable tiered storage
                    self.logger.info("Enabling tiered storage")
                    rpk.alter_topic_config(self.topic, "redpanda.remote.read",
                                           "true")
                    rpk.alter_topic_config(self.topic, "redpanda.remote.write",
                                           "true")

                # Write enough traffic to drive roughly one segment per partition, so
                # that we're doing some tiered storage uploads.
                await_bytes = (self.log_segment_size * self.partition_count)

                # Factor of 4 to accommodate heavily oversubscribed test systems that
                # might not keep up with our intended throughput
                progress_time = (await_bytes // data_rate) * 4

                traffic.await_progress(await_bytes // msg_size,
                                       timeout_sec=progress_time)

        seq_consumer.wait()

        # Sanity: we should have uploaded something to S3.
        bucket_data = S3Snapshot(self.topics,
                                 self.redpanda.cloud_storage_client,
                                 self.si_settings.cloud_storage_bucket,
                                 self.logger)
        for p in range(0, self.partition_count):
            assert bucket_data.cloud_log_size_for_ntp(self.topic, p) > 0
