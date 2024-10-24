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
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations
from ducktape.mark import parametrize


class RedPandaOpenMessagingBenchmarkPerf(RedpandaTest):

    BENCHMARK_WAIT_TIME_MIN = 10

    def __init__(self, ctx):
        self._ctx = ctx
        super(RedPandaOpenMessagingBenchmarkPerf,
              self).__init__(test_context=ctx, num_brokers=3)

    @cluster(num_nodes=6)
    def omb_test(self):
        workload = {
            "name": "CommonWorkload",
            "topics": 1,
            "partitions_per_topic": 100,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 25,
            "producers_per_topic": 25,
            "producer_rate": 75_000,
            "message_size": 1024,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 2,
            "warmup_duration_minutes": 2,
        }
        driver = {
            "name": "CommonWorkloadDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
                "batch.size": 16384,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "max.partition.fetch.bytes": 131072
            },
        }
        validator = {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS:
            [OMBSampleConfigurations.gte(70)]
        }

        benchmark = OpenMessagingBenchmark(ctx=self._ctx,
                                           redpanda=self.redpanda,
                                           driver=driver,
                                           workload=(workload, validator),
                                           topology="ensemble")

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
