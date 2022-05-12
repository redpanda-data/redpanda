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


class OpenBenchmarkTest(RedpandaTest):
    BENCHMARK_WAIT_TIME_MIN = 5

    def __init__(self, ctx):
        self._ctx = ctx
        super(OpenBenchmarkTest, self).__init__(test_context=ctx,
                                                num_brokers=3)

    @cluster(num_nodes=6)
    def test_default_omb_configuration(self):
        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=8)
    def test_multiple_topics_omb(self):
        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda)
        benchmark.set_configuration({"topics": 2})
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=8)
    def test_multiple_producers_consumers_omb(self):
        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda)
        benchmark.set_configuration({
            "producers_per_topic": 2,
            "consumer_per_subscription": 2
        })
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=7)
    def test_multiple_subscriptions_omb(self):
        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda)
        benchmark.set_configuration({"subscriptions_per_topic": 2})
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
