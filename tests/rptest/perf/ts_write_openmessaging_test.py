# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from ducktape.mark import parametrize
from rptest.services.redpanda import SISettings


class TSWriteOpenmessagingTest(RedpandaTest):
    BENCHMARK_WAIT_TIME_MIN = 10

    def __init__(self, ctx):
        si_settings = SISettings(
            test_context=ctx,
            log_segment_size=16 * 1_000_000,  # 16 MB
            cloud_storage_cache_chunk_size=8 * 1_000_000,  # 8 MB
            cloud_storage_spillover_manifest_max_segments=10,
        )
        self._ctx = ctx
        super(TSWriteOpenmessagingTest, self).__init__(test_context=ctx,
                                                       num_brokers=3,
                                                       si_settings=si_settings)

    @cluster(num_nodes=6)
    @parametrize(driver_idx="ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT",
                 workload_idx="RELEASE_CERT_SMOKE_LOAD_625k")
    def test_perf(self, driver_idx, workload_idx):
        """
        This adds TS writes to the OMB perf regression tests
        """

        assert self.redpanda.dedicated_nodes

        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda,
                                           driver_idx, workload_idx)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + TSWriteOpenmessagingTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
