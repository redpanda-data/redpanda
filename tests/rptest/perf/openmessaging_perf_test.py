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

    BENCHMARK_WAIT_TIME_MIN = 10

    def __init__(self, ctx):
        self._ctx = ctx
        super(RedPandaOpenMessagingBenchmarkPerf,
              self).__init__(test_context=ctx, num_brokers=3)

    @cluster(num_nodes=6)
    @parametrize(driver_idx="ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT",
                 workload_idx="RELEASE_CERT_SMOKE_LOAD_625k")
    def test_perf(self, driver_idx, workload_idx):
        """
        This test is run as a part of nightly perf suite to detect
        regressions.
        """

        # Make sure this is running in a dedicated environment as the perf
        # run validator metrics are based on a production grade deployment.
        # Check validator for specifics.
        assert self.redpanda.dedicated_nodes

        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda,
                                           driver_idx, workload_idx)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + RedPandaOpenMessagingBenchmarkPerf.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
