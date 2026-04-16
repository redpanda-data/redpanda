# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from rptest.services.cluster import cluster
from rptest.services.redpanda import ResourceSettings
from rptest.services.rpk_benchmark_service import RpkBenchmarkService
from rptest.perf.redpanda_perf_test import RedpandaPerfTest


class RpkBenchmarkPerf(RedpandaPerfTest):
    PARTITIONS = 18
    REPLICAS = 3
    CLIENTS = 60
    MSG_SIZE = 100

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # drop shards to two to reduce noise
        resource_settings = ResourceSettings(num_cpus=2)
        super().__init__(
            *args,
            num_brokers=3,
            resource_settings=resource_settings,
            **kwargs,
        )

    def run_workload(self, mode: str) -> None:
        svc = RpkBenchmarkService(
            self.test_context,
            self.redpanda,
            topic="rpk-bench-topic",
            mode=mode,
            partitions=self.PARTITIONS,
            replicas=self.REPLICAS,
            clients=self.CLIENTS,
            record_size=self.MSG_SIZE,
            warmup_s=20,
            duration_s=60,
        )
        svc.start()
        svc.wait(timeout_sec=300)

        metrics = svc.metrics()
        assert metrics.errors == 0, f"Unexpected benchmark errors: {metrics.errors}"
        assert metrics.requests_per_sec > 0, "Expected requests/s > 0"
        assert metrics.mb_per_sec > 0, "Expected MB/s > 0"

        svc.write_metrics_result(metrics)

    @cluster(num_nodes=6)
    def test_produce(self) -> None:
        self.run_workload("produce")
