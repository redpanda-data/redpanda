# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import threading
import time

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest


class DevelopmentMetricsToggleTest(RedpandaTest):
    """
    Toggles `enable_development_metrics` on and off repeatedly.
    """

    INTERNAL_RPC_LATENCY = "vectorized_internal_rpc_latency"

    topics = (TopicSpec(partition_count=20, replication_factor=3),)

    def __init__(self, test_context: TestContext):
        super().__init__(test_context=test_context, num_brokers=3)

    def _rpc_latency_series(self) -> int:
        """Total internal_rpc_latency series across all nodes."""
        total = 0
        for node in self.redpanda.nodes:
            for family in self.redpanda.metrics(
                node,
                metrics_endpoint=MetricsEndpoint.METRICS,
                name=self.INTERNAL_RPC_LATENCY,
            ):
                total += len(family.samples)
        return total

    @cluster(num_nodes=4)
    def test_toggle_under_load(self):
        # This test intentionally writes a config record per toggle, which
        # bloats the controller log well past the default guard. That growth is
        # expected here, so raise the limit.
        self.redpanda.set_expected_controller_records(50_000)

        admin = Admin(self.redpanda)

        # Sustained produce traffic to drive raft replication RPCs while we
        # toggle. msg_count is large enough to outlast the toggling window.
        producer = RpkProducer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=1024,
            msg_count=10_000_000,
            acks=-1,
        )
        producer.start()

        stop = threading.Event()
        toggle_errors: list[Exception] = []
        toggle_count = [0]

        def toggler():
            value = True
            while not stop.is_set():
                try:
                    admin.patch_cluster_config(
                        upsert={"enable_development_metrics": value}
                    )
                except Exception as e:
                    toggle_errors.append(e)
                    return
                value = not value
                toggle_count[0] += 1
                # Small backoff so we don't drown the controller in config
                # writes, while still flipping fast enough to interleave with
                # in-flight RPC recording.
                time.sleep(0.05)

        toggle_time = 30
        t = threading.Thread(target=toggler, daemon=True)
        t.start()
        try:
            # Concurrently scrape metrics so a read races register/deregister.
            deadline = time.time() + toggle_time
            while time.time() < deadline:
                # Don't assert on the value (it's racing the toggler); just
                # ensure scraping never errors / crashes a node.
                self._rpc_latency_series()
                time.sleep(0.5)
        finally:
            stop.set()
            t.join(timeout=30)

        assert not toggle_errors, f"toggler failed: {toggle_errors}"
        assert toggle_count[0] > 10, f"expected many toggles, got {toggle_count[0]}"
        self.logger.info(f"completed {toggle_count[0]} config toggles")

        producer.stop()

        # The cluster must have survived the toggling intact.
        assert self.redpanda.all_up()
        wait_until(
            self.redpanda.healthy,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="cluster not healthy after toggling",
        )

        # Deterministic final state checks. Enabled => series are registered.
        self.redpanda.set_cluster_config({"enable_development_metrics": True})
        wait_until(
            lambda: self._rpc_latency_series() > 0,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="internal_rpc_latency absent after enabling",
        )

        # Disabled => series are deregistered.
        self.redpanda.set_cluster_config({"enable_development_metrics": False})
        wait_until(
            lambda: self._rpc_latency_series() == 0,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="internal_rpc_latency still present after disabling",
        )
