# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
from uuid import uuid4

from rptest.services.cluster import cluster
from rptest.services.rpk_benchmark_service import RpkBenchmarkService
from rptest.tests.redpanda_test import RedpandaTest


class RpkBenchmarkServiceSelfTest(RedpandaTest):
    """Smoke test for the RpkBenchmarkService wrapper."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, num_brokers=1, **kwargs)

    def run_smoke(self, mode: str) -> None:
        topic = f"rpk-bench-smoke-{uuid4().hex[:8]}"
        svc = RpkBenchmarkService(
            self.test_context,
            self.redpanda,
            topic=topic,
            mode=mode,
            partitions=6,
            replicas=1,
            clients=5,
            record_size=100,
            warmup_s=5,
            duration_s=5,
        )

        svc.start()
        svc.wait(timeout_sec=120)

        metrics = svc.metrics()
        self.logger.debug(f"Rpk benchmark service metrics: {metrics}")

        assert metrics.errors == 0, f"Unexpected benchmark errors: {metrics.errors}"
        assert metrics.requests_per_sec > 0, "Expected requests/s > 0"
        assert metrics.mb_per_sec > 0, "Expected MB/s > 0"

        svc.write_metrics_result(metrics)

    @cluster(num_nodes=2)
    def test_produce_smoke(self) -> None:
        self.run_smoke("produce")
