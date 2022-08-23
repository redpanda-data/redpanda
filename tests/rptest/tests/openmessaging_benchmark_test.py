# Copyright 2020 Vectorized, Inc.
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
from ducktape.mark import matrix


class OpenBenchmarkTest(RedpandaTest):
    BENCHMARK_WAIT_TIME_MIN = 5

    def __init__(self, ctx):
        self._ctx = ctx
        super(OpenBenchmarkTest, self).__init__(test_context=ctx,
                                                num_brokers=3)

    @cluster(num_nodes=6)
    @matrix(driver=["SIMPLE_DRIVER", "ACK_ALL_GROUP_LINGER_1MS"],
            workload=["SIMPLE_WORKLOAD"])
    def test_default_omb_configuration(self, driver, workload):
        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda, driver,
                                           workload)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
