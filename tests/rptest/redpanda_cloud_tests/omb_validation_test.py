# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
from time import time
from typing import Any, TypeVar

from rptest.services.cluster import cluster
from rptest.services.redpanda import get_cloud_provider
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from ducktape.tests.test import TestContext
from rptest.services.producer_swarm import ProducerSwarm
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda_cloud import ProductInfo
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations
from rptest.services.machinetype import get_machine_info
from rptest.utils.type_utils import rcast

# pyright: strict

KiB = 1024
MiB = KiB * KiB
GiB = KiB * MiB
KB = 10**3
MB = 10**6
GB = 10**9
minutes = 60
hours = 60 * minutes

T = TypeVar('T')

REJECTED_METRIC = "vectorized_kafka_rpc_connections_rejected_total"
ACTIVE_METRIC = "vectorized_kafka_rpc_active_connections"


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

    DEFAULT_EXPECTED_MAX_LATENCIES = {
        OMBSampleConfigurations.E2E_LATENCY_50PCT: 20.0,
        OMBSampleConfigurations.E2E_LATENCY_75PCT: 25.0,
        OMBSampleConfigurations.E2E_LATENCY_99PCT: 60.0,
        OMBSampleConfigurations.E2E_LATENCY_999PCT: 100.0,
    }

    AZURE_EXPECTED_MAX_LATENCIES = DEFAULT_EXPECTED_MAX_LATENCIES | {
        OMBSampleConfigurations.E2E_LATENCY_99PCT: 75.0,
        OMBSampleConfigurations.E2E_LATENCY_999PCT: 175.0,
    }

    # Mapping of result keys from specific series to their expected max latencies
    # Key is a series (Ex: endToEndLatency999pct and value is mapped to OMBSampleConfigurations.E2E_LATENCY_999PCT)
    LATENCY_SERIES_AND_MAX = {
        "endToEndLatency50pct": OMBSampleConfigurations.E2E_LATENCY_50PCT,
        "endToEndLatency75pct": OMBSampleConfigurations.E2E_LATENCY_75PCT,
        "endToEndLatency99pct": OMBSampleConfigurations.E2E_LATENCY_99PCT,
        "endToEndLatency999pct": OMBSampleConfigurations.E2E_LATENCY_999PCT,
    }

    def run_benchmark_with_retries(self, benchmark: OpenMessagingBenchmark,
                                   max_retries: int):
        for try_count in range(1, max_retries + 1):
            self.logger.info(
                f"Starting benchmark attempt {try_count}/{max_retries}.")
            benchmark.start()
            benchmark_time_min = benchmark.benchmark_time() + 5
            benchmark.wait(timeout_sec=benchmark_time_min * 60)

            res = benchmark.check_succeed(raise_exceptions=False)
            assert res  # help type checker
            is_valid, validation_results = res
            if is_valid:
                self.logger.info(
                    "Benchmark passed without significant latency issues.")
                return True
            else:
                self.handle_failed_attempt(try_count, max_retries, benchmark,
                                           validation_results)

        return False

    def handle_failed_attempt(self, try_count: int, max_retries: int,
                              benchmark: OpenMessagingBenchmark,
                              validation_results: list[str]):
        self.logger.info(f"Benchmark test attempt {try_count} failed.")
        for result in validation_results:
            self.logger.info(f"Result is: {result}")

        self.logger.info("Checking test results for possible latency spikes.")
        latency_metrics = self.extract_latency_metrics(benchmark)
        spikes_detected = benchmark.detect_spikes_by_percentile(
            latency_metrics,
            expected_max_latencies=self.expected_max_latencies())

        if spikes_detected and try_count < max_retries:
            self.logger.info(
                f"Latency spikes detected. Preparing for attempt {try_count+1}/{max_retries}."
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
            for key in self.LATENCY_SERIES_AND_MAX if key in benchmark.metrics
        }
        self.log_latency_metrics(latency_metrics)
        return latency_metrics

    def log_latency_metrics(self, latency_metrics: dict[str, Any]):
        self.logger.info("Latency metrics for spikes detection:")
        for key, value in latency_metrics.items():
            series_values = ", ".join(str(v) for v in value)
            self.logger.info(f"{key}: [{series_values}]")

    def expected_max_latencies(self):
        return {
            series_key:
            OMBValidationTest.DEFAULT_EXPECTED_MAX_LATENCIES[config_key]
            for series_key, config_key in
            OMBValidationTest.LATENCY_SERIES_AND_MAX.items()
        }

    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
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

        self.num_brokers: int = config_profile['nodes_count']
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

        cloud_provider = get_cloud_provider()
        latency_limits = OMBValidationTest.DEFAULT_EXPECTED_MAX_LATENCIES

        if cloud_provider == "azure":
            latency_limits = OMBValidationTest.AZURE_EXPECTED_MAX_LATENCIES

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
        omb_producer_count = self._producer_count(producer_rate)
        omb_consumer_count = self._consumer_count(producer_rate *
                                                  subscriptions)
        workload = self.WORKLOAD_DEFAULTS | {
            "name":
            "MaxConnectionsTestWorkload",
            "partitions_per_topic":
            self._partition_count(),
            "subscriptions_per_topic":
            subscriptions,
            "consumer_per_subscription":
            max(omb_consumer_count // subscriptions, 1),
            "producers_per_topic":
            omb_producer_count,
            "producer_rate":
            producer_rate // (1 * KiB),
        }

        warmup_duration = rcast(int, workload["warmup_duration_minutes"])
        omb_test_duration = rcast(int, workload["test_duration_minutes"])

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

        rejected_start = self._rejected_count()

        def assert_no_rejected():
            """Assert that there have been no rejected connections since the
            start of the test."""
            rejected_now = self._rejected_count()
            assert rejected_now == rejected_start, f"Rejected connections detected, start={rejected_start}, now={rejected_now}"

        # ProducerSwarm parameters
        #

        record_size = 64

        # very rough estimated number of connections for OMB
        omb_connections = 3 * (omb_producer_count + omb_consumer_count)

        # We have a certain advertised connection count (ACC) figure, and this way this works
        # in practice is that we set the per-broker limit to 1.2 ACC/N where N is the number of brokers
        # and 1.2 is a fudge factor that makes it more likely that particular workload can actually get
        # to the ACC figure globally even when connection counts aren't exactly balanced across brokers.

        # The remainder of our connection budget after OMB connections are accounted for we will
        # fill with swarm connections: we add 10% to the nominal amount to ensure we test the
        # advertised limit and this uses up ~half the slack we have in the enforcement (we currently
        # set the per broker limit to 1.2x of what it would be if enforced exactly).
        swarm_target_connections = int(
            (tier_limits.max_connection_count - omb_connections) * 1.1)

        # we expect each swarm producer to create 1 connection per broker, plus 1 additional connection
        # for metadata
        conn_per_swarm_producer = self.num_brokers + 1

        producer_per_swarm_node: int = swarm_target_connections // conn_per_swarm_producer // SWARM_WORKERS

        messages_per_sec_per_producer = 1
        msg_rate_per_node = messages_per_sec_per_producer * producer_per_swarm_node

        # single producer runtime
        # Each swarm will throttle the client creation rate to about 30 connections/second
        warm_up_time_s = (producer_per_swarm_node *
                          ProducerSwarm.CLIENT_SPAWN_WAIT_MS // 1000) + 60
        target_runtime_s = 60 * (omb_test_duration +
                                 warmup_duration) + warm_up_time_s

        swarm_runtime_multiplier = 2.5
        swarm_runtime = target_runtime_s * swarm_runtime_multiplier
        # We want the swarm to run "past" the end of the test, so we use this multiplier
        # of 2.5 the total expected runtime. We don't use infinite because DT cleanup is
        # often not reliable (e.g., there may be an exception during cleanup which prevents
        # swarm cleanup from running, or the test may be abruptly halted, skipping cleanup),
        # so having some time limit is helpful to avoid runaway swarm processes (though if
        # cleanup does fail it is likely to affect subsequent tests).
        records_per_producer = math.ceil(messages_per_sec_per_producer *
                                         swarm_runtime)

        total_target = omb_connections + swarm_target_connections

        self.logger.warn(
            f"Connections (before start): {self._connection_count()}, target conn: {total_target} "
            f"(OMB: {omb_connections}, swarm: {swarm_target_connections}), per-broker: {total_target / self.num_brokers:.0f}"
        )

        self.logger.warn(
            f"OMB nodes: {OMB_WORKERS}, omb producers: {omb_producer_count}, omb consumers: "
            f"{omb_consumer_count}, producer rate: {producer_rate / 10**6} MB/s"
        )

        self.logger.warn(
            f"target_runtime: {target_runtime_s / 60:.2f}m, omb test_duration: {omb_test_duration}m, "
            f"OMB warmup: {warmup_duration}m, swarm warmup: {warm_up_time_s / 60:.2f}m"
        )

        self.logger.warn(
            f"Swarm nodes: {SWARM_WORKERS}, producers per node: {producer_per_swarm_node}, messages per producer: "
            f"{records_per_producer} Message rate: {messages_per_sec_per_producer} msg/s"
        )

        # Before the test even starts, assert that there are a "small" number of connections.
        # This number is not zero because various things maintain or make occasional connections
        # to cloud clusters like kminion, so I observe typically ~45 connections on an idle T1 and 450
        # on an idle T7 cluster. So we just use a SWAG of 100 + 2% of the connection target as the
        # threshold above which we complain.
        # If this fails it is probably because some other test left services running
        # against the cluster.
        count_before = self._connection_count()
        maximum_allowed = int(tier_limits.max_connection_count * 0.02 + 100)
        assert count_before <= maximum_allowed, (
            f"Wanted less than {maximum_allowed} "
            f"connections to start but got {count_before}")

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

        def make_swarm():
            return ProducerSwarm(
                self._ctx,
                self.redpanda,
                topic=swarm_topic_name,
                producers=producer_per_swarm_node,
                records_per_producer=records_per_producer,
                timeout_ms=PRODUCER_TIMEOUT_MS,
                min_record_size=record_size,
                max_record_size=record_size,
                messages_per_second_per_producer=messages_per_sec_per_producer)

        swarm = [make_swarm() for _ in range(SWARM_WORKERS)]

        time_before_swarm = time()

        for s in swarm:
            s.start()

        for s in swarm:
            # wait for the swarm to report that all producers have started (sent at least 1 message)
            s.wait_for_all_started()

        assert_no_rejected()

        # Now wait for up to five minutes to hit our target connection count: even though all producers
        # have started, it's possible that the connections haven't hit their target yet because some
        # brokers haven't been produced to by some clients: at 1 message/sec it can take some time
        # for all brokers to be connected to by all producers. If we assume a maximum of 30 brokers,
        # and well-distributed partitions and random keys, after 5 minutes each broker will have
        # received one message with probability 1 - (1 - 1/30)^300 = ~0.99996.
        # In practice the cloud there will be some additional external connections on top of those
        # created by the test because of connections from kminion, etc.
        def target_connections_reached():
            # Empirically, we get slightly fewer connections than calculated, perhaps due to the same
            # underlying issue as redpanda#15475: even when S swarm producers have started, we observe
            # slightly less than S * (B + 1) connections cluster-wide, indicating that some swarm clients
            # did not connect to all brokers, so only require this fraction of the target connection count
            # in order to consider us ready to start. Since we already applied a 1.1x factor to the target
            # connection count above the advertised count, we are still well above the advertised limit.
            SWARM_TARGET_CONNECTIONS_FUDGE = 0.96
            ccount = self._connection_count()
            self.logger.debug(
                'Waiting for connections to reach target, current:'
                f'{ccount}, target: {swarm_target_connections}')
            return self._connection_count(
            ) >= swarm_target_connections * SWARM_TARGET_CONNECTIONS_FUDGE

        def not_reached():
            return f"Failed to reach target connections, actual: {self._connection_count()}, target: {swarm_target_connections}"

        self.redpanda.wait_until(target_connections_reached,
                                 timeout_sec=5 * 60,
                                 backoff_sec=10,
                                 err_msg=not_reached)

        time_before_body = time()

        self.logger.warn(
            f"swarm startup complete in {time_before_body - time_before_swarm} (expected: {warm_up_time_s})"
        )

        assert_no_rejected()

        # run the OMB portion of the benchmark and ensure it succeeded
        benchmark.start()
        omb_seconds = benchmark.benchmark_time() * 60
        benchmark.wait(timeout_sec=omb_seconds + 300)

        assert_no_rejected()

        body_runtime = time() - time_before_body

        assert body_runtime >= benchmark.benchmark_time(), \
            f"unexpectedly short runtime: {body_runtime} vs {benchmark.benchmark_time()}"

        assert time() - time_before_swarm < swarm_runtime, (
            f"test ran too long and so swarm will have stopped: "
            f"{time() - time_before_swarm} >= {swarm_runtime}")

        # Get and print all the swarm metrics now so if we fail in check_succeed()
        # we have this info in the log. Specify the lookback window to be the time
        # of the OMB run so we exclude the initialization part of swarm which is
        # going to have unstable metrics
        swarm_metrics = [(s, s.get_metrics_summary(seconds=omb_seconds))
                         for s in swarm]

        # print all metrics first
        for i, metrics in enumerate(swarm_metrics):
            self.logger.debug(f"Metrics for swarm {i}: {metrics}")

        benchmark.check_succeed()

        # now check that the swarm producers were also reasonably successful
        # they may not be _completely_ successful because of:
        # https://github.com/redpanda-data/redpanda/issues/15475
        # which means that some swarm producers appear to get into a state
        # where some significant fraction of messages time out, which will keep
        # the overall throughput of those producers much below average
        for s, metrics in swarm_metrics:
            sname = str(s)
            assert metrics.clients_alive == producer_per_swarm_node, \
                f"On {sname} bad clients_alive: {metrics.clients_alive} != {producer_per_swarm_node}"
            assert metrics.clients_stopped == 0, f"clients unexpectedly stopped"

            def in_range(name: str, value: float, nominal: float,
                         max_range: float):
                lb = nominal * (1 - max_range)
                ub = nominal * (1 + max_range)
                assert lb <= value <= ub, f"On {sname} for {name} value {value} is outside allowed range [{lb}, {ub}]"

            in_range("msg/s p50", metrics.p50, messages_per_sec_per_producer,
                     0.01)

            # no more than 1% error rate
            error_rate = metrics.total_error / (metrics.total_attempts)
            assert error_rate < 0.01, f"On {s} error rate {error_rate:.2} is too high: {metrics}"

            # we should have at least 95% of the expected success messages
            expected_messages = msg_rate_per_node * body_runtime
            assert metrics.total_success >= 0.95 * expected_messages, \
                f"On {s} expected {expected_messages} but only got {metrics.total_success}"

            if (metrics.p0 < messages_per_sec_per_producer / 2):
                # This probably means that the slowest producer was running at less that half the expected
                # rate. Normally this does not occur (except perhaps transiently when the swarm)
                # is first starting up, and probably indicates that redpanda-data/redpanda#15475
                # has occurred, so just log it and move on
                self.logger.warn(
                    f"On {sname} p0 rate was very slow, probably redpanda-data/redpanda#15475 : "
                    f"p0={metrics.p0}")

        for s in swarm:
            s.stop()

        self.redpanda.assert_cluster_is_reusable()

    def _warn_metrics(self, metrics: dict[str, Any], validator: dict[str,
                                                                     Any]):
        """Validates metrics and just warn if any fail."""

        assert len(validator) > 0, "At least one metric should be validated"

        results: list[str] = []

        def kv_str(k: str, v: Any):
            return f"Metric {k}, value {v}, "

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
        # so just get the measurements specified in DEFAULT_EXPECTED_MAX_LATENCIES
        # using dict comprehension
        latency_metrics = {
            k: benchmark.metrics[k]
            for k in OMBValidationTest.DEFAULT_EXPECTED_MAX_LATENCIES.keys()
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
        self.run_benchmark_with_retries(benchmark, max_retries)

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

        max_retries = 2
        self.run_benchmark_with_retries(benchmark, max_retries)
        self.redpanda.assert_cluster_is_reusable()

    def _connection_count(self):
        return self.redpanda.metric_sum(ACTIVE_METRIC)

    def _rejected_count(self):
        return self.redpanda.metric_sum(REJECTED_METRIC)
