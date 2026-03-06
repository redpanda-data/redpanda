# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from typing import Any

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.rdkafka_performance import RdkafkaPerformanceService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import ResourceSettings


class RdkafkaPerf(RedpandaTest):
    # Run the test for approximately 2 minutes
    MSG_COUNT = 1000000
    MSG_SIZE = 100

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Reduce core count to reduce shard variance and noise
        resource_settings = ResourceSettings(num_cpus=2)
        super().__init__(
            *args, num_brokers=3, resource_settings=resource_settings, **kwargs
        )

    def run_workload(self, spec: TopicSpec, msg_count: int, write_metrics: bool):
        svc = RdkafkaPerformanceService(
            self.test_context,
            self.redpanda,
            topic=spec.name,
            msg_count=msg_count,
            msg_size=self.MSG_SIZE,
            num_nodes=1,
            clients_per_node=1,
            warmup_msg_count=50000,
            extra_config={
                "enable.idempotence": "true",
                "batch.size": "1",
                "linger.ms": "0",
                # otherwise things become very unstable
                "queue.buffering.max.messages": "1000",
            },
        )
        svc.start()
        svc.wait(timeout_sec=300)

        m = svc.metrics()

        expected_messages = msg_count

        assert m.msgs == expected_messages, (
            f"Expected {expected_messages} messages produced, got {m.msgs}"
        )
        assert m.dr == expected_messages, (
            f"Expected {expected_messages} deliveries, got {m.dr}"
        )
        assert m.dr_err == 0, f"Unexpected delivery errors: {m.dr_err}"

        if write_metrics:
            svc.write_metrics_result(m)
        else:
            svc.stop()
            svc.free()

    def run_warmup(self, spec: TopicSpec) -> None:
        start_time = time.time()
        iterations = 0
        while (time.time() - start_time) < 90:
            self.run_workload(spec, 100000, write_metrics=False)
            iterations += 1
        self.logger.info(f"Warmup complete after {iterations} iteration(s)")

    @cluster(num_nodes=6)
    def test_produce(self) -> None:
        spec = TopicSpec(name="rdkafka", partition_count=36, replication_factor=3)
        self.client().create_topic(spec)

        self.run_warmup(spec)

        self.run_workload(spec, self.MSG_COUNT, write_metrics=True)
