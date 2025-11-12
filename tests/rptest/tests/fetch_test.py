# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from typing import Any
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations


class FetchTests(RedpandaTest):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, num_brokers=1, **kwargs)

    @cluster(num_nodes=4)
    def test_early_response_on_max_bytes(self):
        """
        Tests that Redpanda will send a fetch response when max.partition.fetch.bytes
        has been read from a partition. Even if min bytes hasn't been met and max wait
        hasn't passed.
        """
        throughput_bytes = 10 * 1024
        workload = {
            "name": "workload",
            "topics": 1,
            "partitions_per_topic": 1,
            "subscriptions_per_topic": 1,
            "consumer_per_subscription": 1,
            "producers_per_topic": 1,
            "producer_rate": throughput_bytes // 1024,
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 1,
            "warmup_duration_minutes": 1,
            "message_size": 1024,
            "payload_file": "payload/payload-1Kb.data",
        }
        driver = {
            "name": "driver",
            "replication_factor": 1,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "max.in.flight.requests.per.connection": 5,
                "batch.size": 1024,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "max.partition.fetch.bytes": 1024,
                "fetch.min.bytes": throughput_bytes,
                "fetch.max.wait.ms": 1000,
            },
        }
        validator = {
            OMBSampleConfigurations.CON_RATE_AVG: [OMBSampleConfigurations.gte(2)],
        }
        benchmark = OpenMessagingBenchmark(
            ctx=self.test_context,
            redpanda=self.redpanda,
            driver=driver,
            workload=(workload, validator),
            topology="ensemble",
        )
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time_mins() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
