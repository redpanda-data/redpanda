# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from ducktape.mark import parametrize
from rptest.services.redpanda import SISettings


class TSReadOpenmessagingTest(RedpandaTest):
    BENCHMARK_WAIT_TIME_MIN = 15

    def __init__(self, ctx):
        extra_rp_conf = {
            "retention_local_target_bytes_default": 16 * 1_000_000,  # 16 MB
            "cloud_storage_spillover_manifest_size": None,
        }
        si_settings = SISettings(
            test_context=ctx,
            log_segment_size=16 * 1_000_000,  # 16 MB
            cloud_storage_cache_chunk_size=8 * 1_000_000,  # 8 MB
            cloud_storage_spillover_manifest_max_segments=10,
        )
        self._ctx = ctx
        super(TSReadOpenmessagingTest,
              self).__init__(test_context=ctx,
                             num_brokers=3,
                             si_settings=si_settings,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=6)
    @parametrize(driver_idx="ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT")
    def test_perf(self, driver_idx):
        """
        An OMB perf regression test that has TS reads.
        """

        assert self.redpanda.dedicated_nodes

        validator = {
            OMBSampleConfigurations.PUB_LATENCY_MIN:
            [OMBSampleConfigurations.lte(1)],
            OMBSampleConfigurations.PUB_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(3)],
            OMBSampleConfigurations.PUB_LATENCY_75PCT:
            [OMBSampleConfigurations.lte(4)],
            OMBSampleConfigurations.PUB_LATENCY_95PCT:
            [OMBSampleConfigurations.lte(8)],
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS:
            [OMBSampleConfigurations.gte(600)]
        }

        workload = {
            "name": "SmokeLoad625kReleaseCert",
            "topics": 1,
            "partitions_per_topic": 100,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 8,
            "producers_per_topic": 16,
            "producer_rate": 625000,
            "consumer_backlog_size_GB": 10,
            "test_duration_minutes": 5,
            "warmup_duration_minutes": 5,
        }

        wide_read_workload = {
            "name": "WideRead625kLoad",
            "topics": 1,
            "partitions_per_topic": 600,
            "subscriptions_per_topic": 2,
            "consumer_per_subscription": 5,
            "producers_per_topic": 16,
            "producer_rate": 625000,
            "consumer_backlog_size_GB": 10,
            "test_duration_minutes": 10,
            "warmup_duration_minutes": 5,
        }

        benchmark = OpenMessagingBenchmark(
            self._ctx,
            self.redpanda,
            driver=driver_idx,
            workload=[wide_read_workload, validator])
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + TSReadOpenmessagingTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
