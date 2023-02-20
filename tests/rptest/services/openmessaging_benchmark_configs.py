# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


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
    E2E_LATENCY_MAX = "aggregatedEndToEndLatencyMax"
    AVG_THROUGHPUT_MBPS = "throughputMBps"

    def range(min_val, max_val):
        return (lambda x: x >= min_val and x <= max_val,
                f"Expected in range [{min_val}, {max_val}], check failed.")

    def lte(max_val):
        return (lambda x: x <= max_val,
                f"Expected to be <= {max_val}, check failed.")

    def gte(min_val):
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

    def validate_metrics(metrics, validator):
        """ Validates some predefined metrics rules against the metrics data and throws if any of the rules fail."""
        assert len(metrics) == 1, "Unexpected metrics output {metrics}}"
        metrics = next(iter(metrics.values()))
        results = []
        kv_str = lambda k, v: f"Metric {k}, value {v}, "
        count = 0
        for key in metrics.keys():
            if key not in validator:
                continue
            val = metrics[key]
            count = count + 1
            for rule in validator[key]:
                if not rule[0](val):
                    results.append(kv_str(key, val) + rule[1])
        assert count > 0, f"At least one metric should be validated {metrics}"
        assert len(results) == 0, str(results)

    # ------ Driver configurations --------
    SIMPLE_DRIVER = {
        "name": "simple-driver",
        "replication_factor": 3,
        "request_timeout": 300000,
        "enable_idempotence": "false",
        "acks": "1",
        "linger_ms": 1,
        "max_in_flight": 1,
        "batch_size": 131072,
        "auto_offset_earliest": "earliest",
        "auto_commit": "false",
        "max_partition_fetch": 131072
    }

    ACK_ALL_GROUP_LINGER_1MS = {
        "name": "ack-all-group-linger-1-ms-no-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "enable_idempotence": "false",
        "acks": "all",
        "linger_ms": 1,
        "max_in_flight": 1,
        "batch_size": 131072,
        "auto_offset_earliest": "earliest",
        "auto_commit": "false",
        "max_partition_fetch": 131072
    }

    ACK_ALL_GROUP_LINGER_1MS_WITH_IDEMPOTENCE = {
        "name": "ack-all-group-linger-1-ms-enable-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "enable_idempotence": "true",
        "acks": "all",
        "linger_ms": 1,
        "max_in_flight": 1,
        "batch_size": 131072,
        "auto_offset_earliest": "earliest",
        "auto_commit": "false",
        "max_partition_fetch": 131072
    }

    ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT = {
        "name": "ack-all-group-linger-1-ms-enable-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "enable_idempotence": "true",
        "acks": "all",
        "linger_ms": 1,
        "max_in_flight": 5,
        "batch_size": 131072,
        "auto_offset_earliest": "earliest",
        "auto_commit": "false",
        "max_partition_fetch": 131072
    }

    ACK_ALL_GROUP_LINGER_10MS = {
        "name": "ack-all-group-linger-10-ms-no-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "enable_idempotence": "false",
        "acks": "all",
        "linger_ms": 10,
        "max_in_flight": 1,
        "batch_size": 131072,
        "auto_offset_earliest": "earliest",
        "auto_commit": "false",
        "max_partition_fetch": 131072
    }

    ACK_ALL_GROUP_LINGER_10MS_WITH_IDEMPOTENCE = {
        "name": "ack-all-group-linger-10-ms-enable-idem",
        "replication_factor": 3,
        "request_timeout": 300000,
        "enable_idempotence": "true",
        "acks": "all",
        "linger_ms": 1,
        "max_in_flight": 1,
        "batch_size": 131072,
        "auto_offset_earliest": "earliest",
        "auto_commit": "false",
        "max_partition_fetch": 131072
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
        (RELEASE_CERT_SMOKE_LOAD_625k, RELEASE_SMOKE_TEST_VALIDATOR)
    }
