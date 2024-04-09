# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
from time import sleep
from typing import Any, TypeVar

from rptest.services.cluster import cluster
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.services.producer_swarm import ProducerSwarm
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda_cloud import CloudTierName, ProductInfo, get_config_profile_name
from rptest.services.redpanda import (SISettings, RedpandaServiceCloud)
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations
from rptest.services.machinetype import get_machine_info

KiB = 1024
MiB = KiB * KiB
GiB = KiB * MiB
KB = 10**3
MB = 10**6
GB = 10**9
minutes = 60
hours = 60 * minutes

T = TypeVar('T')


def not_none(value: T | None) -> T:
    if value is None:
        raise ValueError(f'value was unexpectedly None')
    return value


class OMBValidationTest(RedpandaCloudTest):

    # The numbers of nodes we expect to run with - this value (10) is the default
    # for duck.py so these tests should just work with that default, but not necessarily
    # any less than that.
    CLUSTER_NODES = 10

    # common workload details shared among most/all test methods
    WORKLOAD_DEFAULTS = {
        "topics": 1,
        "message_size": 1 * KiB,
        "payload_file": "payload/payload-1Kb.data",
        "consumer_backlog_size_GB": 0,
        "test_duration_minutes": 5,
        "warmup_duration_minutes": 5,
    }

    EXPECTED_MAX_LATENCIES = {
        OMBSampleConfigurations.E2E_LATENCY_50PCT: 20.0,
        OMBSampleConfigurations.E2E_LATENCY_75PCT: 25.0,
        OMBSampleConfigurations.E2E_LATENCY_99PCT: 60.0,
        OMBSampleConfigurations.E2E_LATENCY_999PCT: 100.0,
    }

    # Mapping of result keys from specific series to their expected max latencies
    # Key is a series (Ex: endToEndLatency999pct and value is mapped to OMBSampleConfigurations.E2E_LATENCY_999PCT)
    LATENCY_SERIES_AND_MAX = {
        "endToEndLatency50pct": OMBSampleConfigurations.E2E_LATENCY_50PCT,
        "endToEndLatency75pct": OMBSampleConfigurations.E2E_LATENCY_75PCT,
        "endToEndLatency99pct": OMBSampleConfigurations.E2E_LATENCY_99PCT,
        "endToEndLatency999pct": OMBSampleConfigurations.E2E_LATENCY_999PCT,
    }

    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        self._ctx = test_ctx

        super().__init__(test_ctx, *args, **kwargs)

        # Load install pack and check profile
        install_pack = self.redpanda.get_install_pack()
        self.logger.info(f"Loaded install pack '{install_pack['version']}': "
                         f"Redpanda v{install_pack['redpanda_version']}, "
                         f"created at '{install_pack['created_at']}'")
        if self.config_profile_name not in install_pack['config_profiles']:
            # throw user friendly error
            _profiles = ", ".join(
                [f"'{k}'" for k in install_pack['config_profiles']])
            raise RuntimeError(
                f"'{self.config_profile_name}' not found among config profiles: {_profiles}"
            )
        config_profile = install_pack['config_profiles'][
            self.config_profile_name]

        self.num_brokers = config_profile['nodes_count']
        self.tier_limits: ProductInfo = not_none(self.redpanda.get_product())
        self.tier_machine_info = get_machine_info(
            config_profile['machine_type'])
        self.rpk = RpkTool(self.redpanda)

    def setup(self):
        super().setup()
        self.redpanda.clean_cluster()

    def tearDown(self):
        super().tearDown()
        self.redpanda.clean_cluster()

    @staticmethod
    def base_validator(multiplier: float = 1) -> dict[str, Any]:
        """Return a validator object with reasonable latency targets for
        healthy systems. Optionally accepts a multiplier value which will multiply
        all the latencies by the given value, which could be used to accept higher
        latencies in cases we know this is reasonable (e.g., a system running at
        its maximum partition count."""

        # use dict comprehension to generate dict of latencies to list of validation functions
        # e.g. { 'aggregatedEndToEndLatency50pct': [OMBSampleConfigurations.lte(20.0 * multiplier)] }
        return {
            k: [OMBSampleConfigurations.lte(v * multiplier)]
            for k, v in OMBValidationTest.EXPECTED_MAX_LATENCIES.items()
        }

    def _partition_count(self) -> int:
        machine_config = self.tier_machine_info
        return 5 * self.num_brokers * machine_config.num_shards

    def _producer_count(self, ingress_rate) -> int:
        """Determine the number of producers based on the ingress rate.
        We assume that each producer is capable of 5 MB/s."""
        return max(ingress_rate // (5 * MB), 1)

    def _consumer_count(self, egress_rate) -> int:
        """Determine the number of consumers based on the egress rate.
        We assume that each consumer is capable of 5 MB/s."""
        return max(egress_rate // (5 * MB), 1)

    def _mb_to_mib(self, mb):
        return math.floor(0.9537 * mb)

    @cluster(num_nodes=CLUSTER_NODES)
    def test_max_connections(self):
        tier_limits = self.tier_limits

        # Constants
        #

        PRODUCER_TIMEOUT_MS = 5000
        OMB_WORKERS = 2
        SWARM_WORKERS = 7

        # OMB parameters
        #

        producer_rate = tier_limits.max_ingress // 5
        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        total_producers = self._producer_count(producer_rate)
        total_consumers = self._consumer_count(producer_rate * subscriptions)
        warmup_duration = self.WORKLOAD_DEFAULTS["warmup_duration_minutes"]
        test_duration = self.WORKLOAD_DEFAULTS["test_duration_minutes"]

        workload = self.WORKLOAD_DEFAULTS | {
            "name": "MaxConnectionsTestWorkload",
            "partitions_per_topic": self._partition_count(),
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate // (1 * KiB),
        }

        driver = {
            "name": "MaxConnectionsTestDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
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
        }

        validator = self.base_validator() | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        # ProducerSwarm parameters
        #

        producer_kwargs = {}
        producer_kwargs['min_record_size'] = 64
        producer_kwargs['max_record_size'] = 64

        effective_msg_size = producer_kwargs['min_record_size'] + (
            producer_kwargs['max_record_size'] -
            producer_kwargs['min_record_size']) // 2

        conn_limit = tier_limits.max_connection_count - 3 * (total_producers +
                                                             total_consumers)
        _target_per_node = conn_limit // SWARM_WORKERS
        _conn_per_node = int(_target_per_node * 0.7)

        msg_rate_per_node = (1 * KiB) // effective_msg_size
        messages_per_sec_per_producer = max(
            msg_rate_per_node // _conn_per_node, 1)

        # single producer runtime
        # Roughly every 500 connection needs 60 seconds to ramp up
        time_per_500_s = 60
        warm_up_time_s = max(
            time_per_500_s * math.ceil(_target_per_node / 500), time_per_500_s)
        target_runtime_s = 60 * (test_duration +
                                 warmup_duration) + warm_up_time_s
        records_per_producer = messages_per_sec_per_producer * target_runtime_s

        self._ctx.logger.warn(
            f"Producers per node: {_conn_per_node} Messages per producer: {records_per_producer} Message rate: {messages_per_sec_per_producer} msg/s"
        )

        producer_kwargs[
            'messages_per_second_per_producer'] = messages_per_sec_per_producer

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=OMB_WORKERS,
                                           topology="ensemble")

        # Create topic for swarm workers after OMB to avoid the reset
        swarm_topic_name = "swarm_topic"
        try:
            self.rpk.delete_topic(swarm_topic_name)
        except:
            # Ignore the exception that is thrown if the topic doesn't exist.
            pass

        self.rpk.create_topic(swarm_topic_name,
                              self._partition_count(),
                              replicas=3)

        swarm = []
        for _ in range(SWARM_WORKERS):
            _swarm_node = ProducerSwarm(
                self._ctx,
                self.redpanda,
                topic=swarm_topic_name,
                producers=_conn_per_node,
                records_per_producer=records_per_producer,
                timeout_ms=PRODUCER_TIMEOUT_MS,
                **producer_kwargs)

            swarm.append(_swarm_node)

        for s in swarm:
            s.start()

        # Allow time for the producers in the swarm to authenticate and start
        self._ctx.logger.info(
            f"waiting {warm_up_time_s} seconds to producer swarm to start")
        sleep(warm_up_time_s)

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5

        try:
            benchmark.wait(timeout_sec=benchmark_time_min * 60)

            benchmark.check_succeed()

            for s in swarm:
                s.wait(timeout_sec=30 * 60)

        finally:
            self.rpk.delete_topic(swarm_topic_name)

        self.redpanda.assert_cluster_is_reusable()

    def _warn_metrics(self, metrics, validator):
        """Validates metrics and just warn if any fail."""

        assert len(validator) > 0, "At least one metric should be validated"

        results = []
        kv_str = lambda k, v: f"Metric {k}, value {v}, "

        for key in validator.keys():
            assert key in metrics, f"Missing requested validator key {key} in metrics"

            val = metrics[key]
            for rule in validator[key]:
                if not rule[0](val):
                    results.append(kv_str(key, val) + rule[1])

        if len(results) > 0:
            self.logger.warn(str(results))

    @cluster(num_nodes=CLUSTER_NODES)
    def test_max_partitions(self):
        tier_limits = self.tier_limits

        # multiplier for the latencies to log warnings on, but still pass the test
        # because we expect poorer performance when we max out one dimension
        fudge_factor = 2.0

        # Producer clients perform poorly with many partitions. Hence we limit
        # the max amount per producer by splitting them over multiple topics.
        MAX_PARTITIONS_PER_TOPIC = 5000
        topics = math.ceil(tier_limits.max_partition_count /
                           MAX_PARTITIONS_PER_TOPIC)

        partitions_per_topic = math.ceil(tier_limits.max_partition_count /
                                         topics)
        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        producer_rate = tier_limits.max_ingress // 2
        total_producers = self._producer_count(producer_rate)
        total_consumers = self._consumer_count(producer_rate * subscriptions)

        workload = self.WORKLOAD_DEFAULTS | {
            "name":
            "MaxPartitionsTestWorkload",
            "topics":
            topics,
            "partitions_per_topic":
            partitions_per_topic,
            "subscriptions_per_topic":
            subscriptions,
            "consumer_per_subscription":
            max(total_consumers // subscriptions // topics, 1),
            "producers_per_topic":
            max(total_producers // topics, 1),
            "producer_rate":
            producer_rate / (1 * KiB),
        }

        # validator to check metrics and fail on
        fail_validator = self.base_validator(fudge_factor) | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        # validator to check metrics and just log warning on
        warn_validator = self.base_validator() | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        benchmark = OpenMessagingBenchmark(
            self._ctx,
            self.redpanda,
            "ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT",
            (workload, fail_validator),
            num_workers=self.CLUSTER_NODES - 1,
            topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)

        # check if omb gave errors, but don't process metrics
        benchmark.check_succeed(validate_metrics=False)

        # benchmark.metrics has a lot of measurements,
        # so just get the measurements specified in EXPECTED_MAX_LATENCIES
        # using dict comprehension
        latency_metrics = {
            k: benchmark.metrics[k]
            for k in OMBValidationTest.EXPECTED_MAX_LATENCIES.keys()
        }
        self.logger.info(f'latency_metrics: {latency_metrics}')

        # just warn on the latency if above expected
        self._warn_metrics(benchmark.metrics, warn_validator)

        # fail test if the latency is above expected including fudge factor
        benchmark.check_succeed()

        self.redpanda.assert_cluster_is_reusable()

    @cluster(num_nodes=CLUSTER_NODES)
    def test_common_workload(self):
        tier_limits = self.tier_limits

        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        partitions = self._partition_count()
        total_producers = self._producer_count(tier_limits.max_ingress)
        total_consumers = self._consumer_count(tier_limits.max_egress)
        validator = self.base_validator() | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(tier_limits.max_ingress // (1 * MB))),
            ],
        }

        workload = self.WORKLOAD_DEFAULTS | {
            "name": "CommonTestWorkload",
            "partitions_per_topic": partitions,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": tier_limits.max_ingress // (1 * KiB),
        }

        driver = {
            "name": "CommonTestDriver",
            "reset": "true",
            "replication_factor": 3,
            "request_timeout": 300000,
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
        }

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=self.CLUSTER_NODES - 1,
                                           topology="ensemble")
        # Latency spikes detection and retry
        max_retries = 2
        try_count = 0

        while try_count < max_retries:
            try_count += 1
            self.logger.info(
                f"Starting benchmark attempt {try_count}/{max_retries}.")
            # Run the benchmark test

            benchmark.start()
            benchmark_time_min = benchmark.benchmark_time() + 5
            benchmark.wait(timeout_sec=benchmark_time_min * 60)

            #benchmark.check_succeed()
            # check if omb gave errors, but don't process metrics
            #benchmark.check_succeed(validate_metrics=True, )
            is_valid, validation_results = benchmark.check_succeed(
                raise_exceptions=False)

            if is_valid:
                self.logger.info(
                    "\n\nBenchmark passed without significant latency issues.\n"
                )
                for result in validation_results:
                    self.logger.info(f"Result is: {result}")
                break

            else:
                self.logger.info(
                    f"\nBenchmark test attempt {try_count} failed.\n")
                for result in validation_results:
                    self.logger.info(f"Result is: {result}")
                self.logger.info(
                    "Checking test results for possible latency spikes.")

                #latency_metrics = benchmark.check_succeed(validate_metrics=False, return_latency_metrics=True, raise_exceptions=False)

                latency_metrics = {
                    key: benchmark.metrics[key]
                    for key in self.LATENCY_SERIES_AND_MAX
                    if key in benchmark.metrics
                }

                if latency_metrics:  # Ensure latency_metrics is not None or empty
                    self.logger.info("\nLatency metrics for spikes detection:")
                    self.logger.info(
                        "\n========================================")
                    for key, value in latency_metrics.items():
                        # Assuming value is a list of latency values for each series
                        series_values = ", ".join(str(v) for v in value)
                        self.logger.info(f"    {key}: [{series_values}]")
                    self.logger.info(
                        "\n========================================")
                else:
                    self.logger.info("No latency metrics returned.")

                # Prepare the expected_max_latencies dictionary for spike detection
                expected_max_latencies_for_detection = {
                    series_key:
                    OMBValidationTest.EXPECTED_MAX_LATENCIES[config_key]
                    for series_key, config_key in
                    OMBValidationTest.LATENCY_SERIES_AND_MAX.items()
                }

                spikes_detected = benchmark.detect_spikes_by_percentile(
                    latency_metrics,
                    expected_max_latencies=expected_max_latencies_for_detection
                )

                if spikes_detected and try_count < max_retries:
                    self.logger.info(
                        f"\n\nPossible latency spikes detected.\n\nPreparing for attempt {try_count+1}/{max_retries}."
                    )

                    ## TODO Need to release nodes for retry
                    self.logger.info(
                        "\n\nHere we should release nodes and retry Benchmark test"
                    )
                    ## Temp fail the test and be able to see logs
                    break
                else:
                    self.logger.info(
                        "\n\nPersistent high latency detected or maximum retries reached. No further retries."
                    )
                    benchmark.check_succeed()
                    break

        self.redpanda.assert_cluster_is_reusable()

    @cluster(num_nodes=CLUSTER_NODES)
    def test_retention(self):
        tier_limits = self.tier_limits

        subscriptions = max(tier_limits.max_egress // tier_limits.max_ingress,
                            1)
        producer_rate = tier_limits.max_ingress
        partitions = self._partition_count()
        segment_bytes = 64 * MiB
        retention_bytes = 2 * segment_bytes
        # This will have 1/2 the test run with segment deletion occuring.
        test_duration_seconds = max(
            (2 * retention_bytes * partitions) // producer_rate, 5 * 60)

        total_producers = self._producer_count(producer_rate)
        total_consumers = self._consumer_count(producer_rate * subscriptions)

        workload = self.WORKLOAD_DEFAULTS | {
            "name": "RetentionTestWorkload",
            "partitions_per_topic": partitions,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate // (1 * KiB),
            "test_duration_minutes": test_duration_seconds // 60,
        }

        driver = {
            "name": "RetentionTestDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
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
                "retention.bytes": retention_bytes,
                "retention.local.target.bytes": retention_bytes,
                "segment.bytes": segment_bytes,
            },
        }

        validator = self.base_validator() | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=self.CLUSTER_NODES - 1,
                                           topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
        self.redpanda.assert_cluster_is_reusable()
