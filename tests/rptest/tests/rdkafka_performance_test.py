# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.rdkafka_performance import (
    RdkafkaPerformanceMode,
    RdkafkaPerformanceService,
)
from rptest.tests.redpanda_test import RedpandaTest


class RdkafkaPerformanceSelfTest(RedpandaTest):
    """Smoke test for the RdkafkaPerformanceService wrapper.

    This validates the service itself (start / wait / metrics parsing),
    not Redpanda behaviour.
    """

    MSG_COUNT = 10_000
    MSG_SIZE = 512

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, num_brokers=1, **kwargs)

    def _run_producer(self, topic: str) -> None:
        producer = RdkafkaPerformanceService(
            self.test_context,
            self.redpanda,
            topic=topic,
            msg_count=self.MSG_COUNT,
            msg_size=self.MSG_SIZE,
            warmup_msg_count=1000,
            num_nodes=2,
        )
        producer.start()
        producer.wait(timeout_sec=60)

        pm = producer.metrics()

        self.logger.debug(f"Rdkafka performance producer metrics: {pm}")

        assert pm.msgs == self.MSG_COUNT, (
            f"Expected {self.MSG_COUNT} messages produced, got {pm.msgs}"
        )
        assert pm.dr == self.MSG_COUNT, (
            f"Expected {self.MSG_COUNT} deliveries, got {pm.dr}"
        )
        assert pm.dr_err == 0, f"Unexpected delivery errors: {pm.dr_err}"
        assert pm.tx_err == 0, f"Unexpected produce failures: {pm.tx_err}"

    def _run_consumer(self, topic: str) -> None:
        consumer = RdkafkaPerformanceService(
            self.test_context,
            self.redpanda,
            topic=topic,
            msg_count=self.MSG_COUNT,
            msg_size=self.MSG_SIZE,
            mode=RdkafkaPerformanceMode.CONSUME,
            group_name="group",
            extra_config={
                "auto.offset.reset": "earliest",
            },
        )
        consumer.start()
        consumer.wait(timeout_sec=60)

        cm = consumer.metrics()

        self.logger.debug(f"Rdkafka performance consumer metrics: {cm}")

        assert cm.msgs == self.MSG_COUNT, (
            f"Expected {self.MSG_COUNT} messages consumed, got {cm.msgs}"
        )
        assert cm.rx_err == 0, f"Unexpected receive errors: {cm.rx_err}"

    @cluster(num_nodes=4)
    def test_produce_consume(self) -> None:
        spec = TopicSpec(
            name="rdkafka_perf_test", partition_count=1, replication_factor=1
        )
        self.client().create_topic(spec)

        self._run_producer(spec.name)
        self._run_consumer(spec.name)
