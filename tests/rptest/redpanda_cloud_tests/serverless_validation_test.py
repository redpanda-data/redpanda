# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
from typing import Any

from rptest.services.cluster import cluster
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from ducktape.tests.test import TestContext
from rptest.services.redpanda_cloud import ThroughputTierInfo
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations
from rptest.services.machinetype import get_machine_info

KiB = 1024
MiB = KiB * KiB
KB = 10**3
MB = 10**6


class ServerlessValidationTest(RedpandaCloudTest):
    # The numbers of nodes we expect to run with - this value (10) is the default
    # for duck.py so these tests should just work with that default, but not necessarily
    # any less than that.
    CLUSTER_NODES = 10

    # no expectations at this time, we are simply measuring for now so we have historical data
    EXPECTED_MAX_LATENCIES: dict[str, float] = {}

    # Mapping of result keys from specific series to their expected max latencies
    # Key is a series (Ex: endToEndLatency999pct and value is mapped to OMBSampleConfigurations.E2E_LATENCY_999PCT)
    LATENCY_SERIES_AND_MAX = {
        "endToEndLatency50pct": OMBSampleConfigurations.E2E_LATENCY_50PCT,
        "endToEndLatency75pct": OMBSampleConfigurations.E2E_LATENCY_75PCT,
        "endToEndLatency99pct": OMBSampleConfigurations.E2E_LATENCY_99PCT,
        "endToEndLatency999pct": OMBSampleConfigurations.E2E_LATENCY_999PCT,
    }

    def run_benchmark_with_retries(
        self, benchmark: OpenMessagingBenchmark, max_retries: int
    ):
        for try_count in range(1, max_retries + 1):
            self.logger.info(f"Starting benchmark attempt {try_count}/{max_retries}.")
            benchmark.start()
            benchmark_time_min = benchmark.benchmark_time_mins() + 5
            benchmark.wait(timeout_sec=benchmark_time_min * 60)

            res = benchmark.check_succeed(raise_exceptions=False)
            assert res  # help type checker
            is_valid, validation_results = res
            if is_valid:
                self.logger.info("Benchmark passed without significant latency issues.")
                return True
            else:
                self.handle_failed_attempt(
                    try_count, max_retries, benchmark, validation_results
                )

        return False

    def handle_failed_attempt(
        self,
        try_count: int,
        max_retries: int,
        benchmark: OpenMessagingBenchmark,
        validation_results: list[str],
    ):
        self.logger.info(f"Benchmark test attempt {try_count} failed.")
        for result in validation_results:
            self.logger.info(f"Result is: {result}")

        self.logger.info("Checking test results for possible latency spikes.")
        latency_metrics = self.extract_latency_metrics(benchmark)
        spikes_detected = benchmark.detect_spikes_by_percentile(
            latency_metrics, expected_max_latencies=self.expected_max_latencies()
        )

        if spikes_detected and try_count < max_retries:
            self.logger.info(
                f"Latency spikes detected. Preparing for attempt {try_count + 1}/{max_retries}."
            )
            self.logger.info("Stopping and freeing worker nodes and retrying.")
            benchmark.stop()
            benchmark.clean()
            assert benchmark.workers  # help type checker
            benchmark.workers.free()
        else:
            self.logger.info(
                "Persistent high latency detected or maximum retries reached. No further retries."
            )
            benchmark.check_succeed()

    def extract_latency_metrics(self, benchmark: OpenMessagingBenchmark):
        latency_metrics = {
            key: benchmark.metrics[key]
            for key in self.LATENCY_SERIES_AND_MAX
            if key in benchmark.metrics
        }
        self.log_latency_metrics(latency_metrics)
        return latency_metrics

    def log_latency_metrics(self, latency_metrics: dict[str, Any]):
        self.logger.info("Latency metrics for spikes detection:")
        for key, value in latency_metrics.items():
            series_values = ", ".join(str(v) for v in value)
            self.logger.info(f"{key}: [{series_values}]")

    def expected_max_latencies(self):
        expected = self.EXPECTED_MAX_LATENCIES
        return {
            series_key: expected[config_key]
            for series_key, config_key in ServerlessValidationTest.LATENCY_SERIES_AND_MAX.items()
        }

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        self._ctx = test_ctx

        self._cloud_provider = self._ctx.globals["cloud_provider"]

        if self._cloud_provider == "gcp":
            self.tier_machine_info = get_machine_info("n2d-standard-4")
            self.num_brokers = 6
            # tier-3-gcp-v2-x86 / GCP / integration & preprod
            self._tier_limits = ThroughputTierInfo(
                max_ingress=100000000,
                max_egress=200000000,
                max_connections_count=45000,
                max_partition_count=11200,
            )
        elif self._cloud_provider == "aws":
            self.tier_machine_info = get_machine_info("m7gd.large")
            self.num_brokers = 3
            # tier-1-aws-v3-arm / AWS / integration & preprod
            self._tier_limits = ThroughputTierInfo(
                max_ingress=20000000,
                max_egress=60000000,
                max_connections_count=9000,
                max_partition_count=2000,
            )
        else:
            raise ValueError(
                f"Unsupported or unspecified cloud provider: {self._cloud_provider}"
            )

        super().__init__(test_ctx, *args, **kwargs)

    def setup(self):
        super().setup()
        self.redpanda.clean_cluster()

    def tearDown(self):
        super().tearDown()
        self.redpanda.clean_cluster()

    @staticmethod
    def base_validator(cloud_provider: str, multiplier: float = 1) -> dict[str, Any]:
        """Return a validator object with reasonable latency targets for
        healthy systems. Optionally accepts a multiplier value which will multiply
        all the latencies by the given value, which could be used to accept higher
        latencies in cases we know this is reasonable (e.g., a system running at
        its maximum partition count."""

        latency_limits = ServerlessValidationTest.EXPECTED_MAX_LATENCIES

        # use dict comprehension to generate dict of latencies to list of validation functions
        # e.g. { 'aggregatedEndToEndLatency50pct': [OMBSampleConfigurations.lte(20.0 * multiplier)] }
        return {
            k: [OMBSampleConfigurations.lte(v * multiplier)]
            for k, v in latency_limits.items()
        }

    def _partition_count(self) -> int:
        machine_config = self.tier_machine_info
        return 5 * self.num_brokers * machine_config.num_shards

    def _producer_count(self, ingress_rate: int) -> int:
        """Determine the number of producers based on the ingress rate (in bytes).
        We assume that each producer is capable of 5 MB/s."""
        return max(ingress_rate // (5 * MB), 1)

    def _consumer_count(self, egress_rate: int) -> int:
        """Determine the number of consumers based on the egress rate (in bytes).
        We assume that each consumer is capable of 5 MB/s."""
        return max(egress_rate // (5 * MB), 1)

    def _mb_to_mib(self, mb: float | int):
        return math.floor(0.9537 * mb)

    @cluster(num_nodes=CLUSTER_NODES)
    def test_serverless_workload(self):
        subscriptions = max(
            self._tier_limits.max_egress // self._tier_limits.max_ingress, 1
        )
        partitions = self._partition_count()
        total_producers = self._producer_count(self._tier_limits.max_ingress)
        total_consumers = self._consumer_count(self._tier_limits.max_egress)

        validator = self.base_validator(self._cloud_provider) | {}

        workload = {
            "topics": 1,
            "message_size": 1 * KiB,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 1,
            "warmup_duration_minutes": 1,
            "name": "ServerlessTestWorkload",
            "partitions_per_topic": partitions,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions, 1),
            "producers_per_topic": total_producers,
            "producer_rate": self._tier_limits.max_ingress // (1 * KiB),
        }

        driver = {
            "name": "ServerlessTestDriver",
            "reset": "true",
            "replication_factor": 3,
            "request_timeout": 400000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            },
            "topic_config": {
                "write.caching": "false",
            },
        }

        benchmark = OpenMessagingBenchmark(
            self._ctx,
            self.redpanda,
            driver,
            (workload, validator),
            num_workers=self.CLUSTER_NODES - 1,
            topology="ensemble",
        )
        # Latency spikes detection and retry
        max_retries = 1
        self.run_benchmark_with_retries(benchmark, max_retries)
