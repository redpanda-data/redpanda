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
from rptest.services.redpanda import SISettings


class TSWriteOpenmessagingTest(RedpandaTest):
    BENCHMARK_WAIT_TIME_MIN = 10

    def __init__(self, ctx):
        extra_rp_conf = {
            "cloud_storage_spillover_manifest_size": None,
        }
        si_settings = SISettings(
            test_context=ctx,
            log_segment_size=16 * 1_000_000,  # 16 MB
            cloud_storage_cache_chunk_size=8 * 1_000_000,  # 8 MB
            cloud_storage_spillover_manifest_max_segments=10,
        )
        self._ctx = ctx
        super(TSWriteOpenmessagingTest,
              self).__init__(test_context=ctx,
                             num_brokers=3,
                             si_settings=si_settings,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=6)
    def test_perf(self):
        """
        This adds TS writes to the OMB perf regression tests
        """

        assert self.redpanda.dedicated_nodes
        workload = {
            "name": "TSWriteWorkload",
            "topics": 1,
            "partitions_per_topic": 100,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 25,
            "producers_per_topic": 25,
            "producer_rate": 45_000,
            "message_size": 1024,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 2,
            "warmup_duration_minutes": 2,
        }
        driver = {
            "name": "TSWriteDriver",
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
            [OMBSampleConfigurations.gte(40)]
        }

        benchmark = OpenMessagingBenchmark(ctx=self._ctx,
                                           redpanda=self.redpanda,
                                           driver=driver,
                                           workload=(workload, validator))
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + TSWriteOpenmessagingTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
