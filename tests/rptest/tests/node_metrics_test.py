# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from time import time
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.utils.node_metrics import NodeMetrics


def assert_lists_equal(l1: list[float], l2: list[float]):
    assert l1 == l2


class NodeMetricsTest(RedpandaTest):
    """ Basic tests for node-level metrics. See partitions_metrics_test.py for
    per-partition metrics. """

    topics = (TopicSpec(), )  # override

    def __init__(self, test_ctx):
        super().__init__(test_context=test_ctx)
        self.node_metrics = NodeMetrics(self.redpanda)

    def _count_greater(self, l1: list[float], l2: list[float]) -> int:
        """ return number of elements in l1 that were *strictly greater* than their
        counterpart in l2. """
        num_greater = 0
        for v, u in zip(l1, l2):
            if v > u:
                num_greater += 1
            self.redpanda.logger.debug(
                f"count_greater({v} - {u} = {v - u}) -> {num_greater}")

        self.redpanda.logger.debug(
            f"count_greater: {l1} / {l2} -> {num_greater}")
        return num_greater

    def _produce_consumed_space(self, orig_free: list[float]) -> bool:
        """ Test helper: produce about 10MiB of data and return true if any of
        the nodes saw a reduction in free space.
        """
        num_records = 10240
        record_size = 1024

        # Produce data and confirm metrics update
        ktools = KafkaCliTools(self.redpanda)
        ktools.produce(self.topic, num_records, record_size, acks=-1)

        new_free = self.node_metrics.disk_free_bytes()
        return self._count_greater(orig_free, new_free) > 0

    @cluster(num_nodes=3)
    def test_node_storage_metrics(self):

        # disk metrics are updated via health monitor's periodic tick().
        t0 = time()
        self.node_metrics.wait_until_ready()
        t1 = time()

        # check that metrics exist and remember the values
        orig_total = self.node_metrics.disk_total_bytes()
        orig_free = self.node_metrics.disk_free_bytes()

        self.redpanda.logger.debug(
            f'orig total {orig_total}, orig free {orig_free}')

        # for alert field, just confirm it exists and in valid range. Actual
        # logic is covered by local_monitor_test.cc
        alerts = self.node_metrics.disk_space_alert()
        assert len(alerts) == 3  # for each node
        assert all([x >= 0 for x in alerts])

        # We want to use up storage and verify that our free space decreases.
        # This is currently complicated by our docker test setup; running it
        # locally, all containers report space free on my primary laptop drive
        # (which they see via overlayfs).  When running, my tests see varying
        # free space caused by other processes on the system. This required a
        # long retry loop below to eventually use up that space, about 5% of the
        # time. We should look using separate per-container volumes, at least
        # for RP data dir.

        # If this is flaky in practice, you may just remove this test. The
        # internal space monitoring logic is covered by unit tests, so just
        # validating that these metrics exist and are non-zero is a good
        # integration test.
        wait_until(lambda: self._produce_consumed_space(orig_free),
                   timeout_sec=120,
                   err_msg="Failed to consume free space metric via producer")

        t2 = time()

        # Assert total space has not changed
        new_total = self.node_metrics.disk_total_bytes()
        assert_lists_equal(orig_total, new_total)
        self.redpanda.logger.info(
            f"Elapsed: {t1-t0} sec to first metrics, {t2-t1} to consume space metric"
        )
