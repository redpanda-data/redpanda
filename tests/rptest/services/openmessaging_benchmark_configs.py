# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, Callable

OMBValiatorFunction = Callable[[float], bool]
OMBValidator = tuple[OMBValiatorFunction, str]
ValidatorDict = dict[str, list[Any]]


# Pre populated OMB perf runner configurations.
class OMBSampleConfigurations:
    """Includes template config driver and workload definitions.
    We run a bunch of validation rules for each workload that defined here."""

    # These are copied over from OMB/bin/generate_charts.py
    # All metrics are in milliseconds unless specially suffixed.
    PUB_LATENCY_MIN = "publishLatencyMin"
    PUB_LATENCY_AVG = "aggregatedPublishLatencyAvg"
    PUB_LATENCY_50PCT = "aggregatedPublishLatency50pct"
    PUB_LATENCY_75PCT = "aggregatedPublishLatency75pct"
    PUB_LATENCY_95PCT = "aggregatedPublishLatency95pct"
    PUB_LATENCY_MAX = "aggregatedPublishLatencyMax"
    PUB_DELAY_LATENCY_AVG = "aggregatedPublishDelayLatencyAvg"
    PUB_DELAY_LATENCY_50PCT = "aggregatedPublishDelayLatency50pct"
    PUB_DELAY_LATENCY_99PCT = "aggregatedPublishDelayLatency99pct"
    E2E_LATENCY_MIN = "endToEndLatencyMin"
    E2E_LATENCY_AVG = "aggregatedEndToEndLatencyAvg"
    E2E_LATENCY_50PCT = "aggregatedEndToEndLatency50pct"
    E2E_LATENCY_75PCT = "aggregatedEndToEndLatency75pct"
    E2E_LATENCY_95PCT = "aggregatedEndToEndLatency95pct"
    E2E_LATENCY_99PCT = "aggregatedEndToEndLatency99pct"
    E2E_LATENCY_999PCT = "aggregatedEndToEndLatency999pct"
    E2E_LATENCY_MAX = "aggregatedEndToEndLatencyMax"
    AVG_THROUGHPUT_MBPS = "throughputMBps"

    @staticmethod
    def range(min_val: float, max_val: float) -> OMBValidator:
        return (lambda x: x >= min_val and x <= max_val,
                f"Expected in range [{min_val}, {max_val}], check failed.")

    @staticmethod
    def lte(max_val: float) -> OMBValidator:
        return (lambda x: x <= max_val,
                f"Expected to be <= {max_val}, check failed.")

    @staticmethod
    def gte(min_val: float) -> OMBValidator:
        return (lambda x: x >= min_val,
                f"Expected to be >= {min_val}, check failed.")

    # Values here are empirical based on historical runs across releases.
    # Typically e2e latency numbers are sub 8-9ms.
    # This is for tuned environment without any noise.
    PROD_ENV_VALIDATOR = {
        E2E_LATENCY_50PCT: [lte(5)],
        E2E_LATENCY_AVG: [lte(10)],
        AVG_THROUGHPUT_MBPS: [gte(1024)],
    }

    # Relaxed checks to avoid flaky tests in a non-tuned test environment.
    # There is usually a lot of variance.
    UNIT_TEST_LATENCY_VALIDATOR = {
        E2E_LATENCY_50PCT: [lte(30)],
        E2E_LATENCY_AVG: [lte(50)],
    }

    # As benchmarked on
    # - i3en.6xlarge for 3 Redpanda;
    # - c5n.9xlarge for 2 clients
    #
    # Publish latencies
    #   min <= 1ms
    #   p50 <= 3ms
    #   p75 <= 4ms
    #   p95 <= 8ms
    #
    # E2E latencies
    #   min <= 1ms
    #   p50 <= 4ms
    #   p75 <= 5ms
    #   p95 <= 10ms
    RELEASE_SMOKE_TEST_VALIDATOR = {
        PUB_LATENCY_MIN: [lte(1)],
        PUB_LATENCY_50PCT: [lte(3)],
        PUB_LATENCY_75PCT: [lte(4)],
        PUB_LATENCY_95PCT: [lte(8)],
        E2E_LATENCY_MIN: [lte(1)],
        E2E_LATENCY_50PCT: [lte(4)],
        E2E_LATENCY_75PCT: [lte(5)],
        E2E_LATENCY_95PCT: [lte(10)],
        AVG_THROUGHPUT_MBPS: [gte(600)]
    }

    @staticmethod
    def validate_metrics(metrics,
                         validator: ValidatorDict,
                         raise_exceptions=True):
        """Validates some predefined metrics rules against the metrics data.

        Args:
            metrics: The metrics to validate.
            validator: A dictionary containing the validation rules.
            raise_exceptions: If True, raises an exception when validation fails. If False, returns the validation results.

        Returns:
            A tuple (is_valid, results), where is_valid is a boolean indicating if all metrics passed validation,
            and results is a list of validation failures.
        """
        assert len(validator) > 0, "At least one metric should be validated"

        results: list[str] = []
        kv_str = lambda k, v: f"Metric {k}, value {v}, "

        for key in validator.keys():
            assert key in metrics, f"Missing requested validator key {key} in metrics"

            val = metrics[key]
            for rule in validator[key]:
                if not rule[0](val):
                    results.append(kv_str(key, val) + rule[1])

        is_valid = len(results) == 0

        if raise_exceptions and not is_valid:
            assert is_valid, str(results)
        return is_valid, results

    # ------ Driver configurations --------
    SIMPLE_DRIVER = {
        "name": "simple-driver",
        "replication_factor": 3,
        "request_timeout": 300000,
        "producer_config": {
            "enable.idempotence": "false",
            "acks": "1",
            "linger.ms": 1,
            "max.in.flight.requests.per.connection": 1,
            "batch.size": 131072,
        },
        "consumer_config": {
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "max.partition.fetch.bytes": 131072
        },
    }

    ACK_ALL_GROUP_LINGER_1MS = {
        "name": "ack-all-group-linger-1-ms-no-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "producer_config": {
            "enable.idempotence": "false",
            "acks": "all",
            "linger.ms": 1,
            "max.in.flight.requests.per.connection": 1,
            "batch.size": 131072,
        },
        "consumer_config": {
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "max.partition.fetch.bytes": 131072
        },
    }

    ACK_ALL_GROUP_LINGER_1MS_WITH_IDEMPOTENCE = {
        "name": "ack-all-group-linger-1-ms-enable-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "producer_config": {
            "enable.idempotence": "true",
            "acks": "all",
            "linger.ms": 1,
            "max.in.flight.requests.per.connection": 1,
            "batch.size": 131072,
        },
        "consumer_config": {
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "max.partition.fetch.bytes": 131072
        },
    }

    ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT = {
        "name": "ack-all-group-linger-1-ms-enable-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "producer_config": {
            "enable.idempotence": "true",
            "acks": "all",
            "linger.ms": 1,
            "max.in.flight.requests.per.connection": 5,
            "batch.size": 131072,
        },
        "consumer_config": {
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "max.partition.fetch.bytes": 131072
        },
    }

    ACK_ALL_GROUP_LINGER_10MS = {
        "name": "ack-all-group-linger-10-ms-no-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "producer_config": {
            "enable.idempotence": "false",
            "acks": "all",
            "linger.ms": 10,
            "max.in.flight.requests.per.connection": 1,
            "batch.size": 131072,
        },
        "consumer_config": {
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "max.partition.fetch.bytes": 131072
        },
    }

    ACK_ALL_GROUP_LINGER_10MS_WITH_IDEMPOTENCE = {
        "name": "ack-all-group-linger-10-ms-enable-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "producer_config": {
            "enable.idempotence": "true",
            "acks": "all",
            "linger.ms": 1,
            "max.in.flight.requests.per.connection": 1,
            "batch.size": 131072,
        },
        "consumer_config": {
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "max.partition.fetch.bytes": 131072
        },
    }

    # ------ Driver configurations end--------

    # ------- Workload configurations --------

    TOPIC1_PART100_1KB_4PROD_1250K_RATE = {
        "name": "100-partitions-1000K-rate-4-producer",
        "topics": 1,
        "partitions_per_topic": 100,
        "subscriptions_per_topic": 1,
        "consumer_per_subscription": 16,
        "producers_per_topic": 16,
        "producer_rate": 1250000,
        "consumer_backlog_size_GB": 0,
        "test_duration_minutes": 60,
        "warmup_duration_minutes": 5
    }

    SIMPLE_WORKLOAD = {
        "name": "Simple-workload-config",
        "topics": 1,
        "partitions_per_topic": 1,
        "subscriptions_per_topic": 1,
        "consumer_per_subscription": 1,
        "producers_per_topic": 1,
        "producer_rate": 1,
        "consumer_backlog_size_GB": 0,
        "test_duration_minutes": 1,
        "warmup_duration_minutes": 1,
    }

    DEDICATED_NODE_WORKLOAD = {
        "name": "Dedicated-node-workload",
        "topics": 1,
        "partitions_per_topic": 30,
        "subscriptions_per_topic": 1,
        "consumer_per_subscription": 10,
        "producers_per_topic": 1,
        "producer_rate": 100000,
        "consumer_backlog_size_GB": 0,
        "test_duration_minutes": 15,
        "warmup_duration_minutes": 5,
    }

    # Release certifying smoke test we use for perf run.
    RELEASE_CERT_SMOKE_LOAD_625k = {
        "name": "SmokeLoad625kReleaseCert",
        "topics": 1,
        "partitions_per_topic": 100,
        "subscriptions_per_topic": 1,
        "consumer_per_subscription": 8,
        "producers_per_topic": 16,
        "producer_rate": 625000,
        "consumer_backlog_size_GB": 0,
        "test_duration_minutes": 5,
        "warmup_duration_minutes": 5,
    }

    # ------- Workload configurations end--------

    # We have another level of indirection from name -> driver/workload
    # rather than passing those dicts directly to the test methods.
    # This is because ducktape tries to create a results directory with name
    # as the stringified representation of the dict, which is too long and the
    # underlying OS command fails. As a workaround we pass these names as
    # the test parameters and use them as keys to lookup the corresponding
    # driver and workload combination.
    DRIVERS = {
        "SIMPLE_DRIVER":
        SIMPLE_DRIVER,
        "ACK_ALL_GROUP_LINGER_1MS":
        ACK_ALL_GROUP_LINGER_1MS,
        "ACK_ALL_GROUP_LINGER_1MS_WITH_IDEMPOTENCE":
        ACK_ALL_GROUP_LINGER_1MS_WITH_IDEMPOTENCE,
        "ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT":
        ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT,
        "ACK_ALL_GROUP_LINGER_10MS":
        ACK_ALL_GROUP_LINGER_10MS,
        "ACK_ALL_GROUP_LINGER_10MS_WITH_IDEMPOTENCE":
        ACK_ALL_GROUP_LINGER_10MS_WITH_IDEMPOTENCE,
    }

    # Workload key maps to the actual workload and the corresponding
    # metric validator.
    WORKLOADS = {
        "SIMPLE_WORKLOAD": (SIMPLE_WORKLOAD, UNIT_TEST_LATENCY_VALIDATOR),
        "DEDICATED_NODE_WORKLOAD":
        (DEDICATED_NODE_WORKLOAD, UNIT_TEST_LATENCY_VALIDATOR),
        "TOPIC1_PART100_1KB_4PROD_1250K_RATE":
        (TOPIC1_PART100_1KB_4PROD_1250K_RATE, PROD_ENV_VALIDATOR),
        "RELEASE_CERT_SMOKE_LOAD_625k":
        (RELEASE_CERT_SMOKE_LOAD_625k, RELEASE_SMOKE_TEST_VALIDATOR),
    }
