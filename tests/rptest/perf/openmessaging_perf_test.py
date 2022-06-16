# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import statistics

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from ducktape.mark import parametrize


class RedPandaOpenMessagingBenchmarkPerf(RedpandaTest):
    # We need to be a little generous with the wait time here as the benchmark
    # run often takes longer than the configured time. This is often the case
    # when consumers are catching up with the produced data, when
    # consumerBacklogSizeGB is configured (check OMB documentation for details).
    BENCHMARK_WAIT_TIME_MIN = 30

    def __init__(self, ctx):
        self._ctx = ctx
        super(OpenBenchmarkTest, self).__init__(test_context=ctx,
                                                num_brokers=3)

    @cluster(num_nodes=9)
    @parametrize(driver_idx="ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT",
                 workload_idx="TOPIC1_PART100_1KB_4PROD_1250K_RATE")
    def test_perf_with_idempotence_and_max_in_flight(self, driver_idx,
                                                     workload_idx):
        """
        This test runs ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT driver with
        TOPIC1_PART100_1KB_4PROD_1250K_RATE workload. Has a runtime of ~1hr.
        """

        # Make sure this is running in a dedicated environment as the perf run
        # is long running and requires significant resources.
        assert self.redpanda.dedicated_nodes

        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda,
                                           driver_idx, workload_idx)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
