# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest


class ControllerRecoveryTest(RedpandaTest):
    """
    Test controller failover.
    """
    def _failover(self):
        """
        stop controller node and wait for failover
        """
        prev = self.redpanda.controller()
        self.redpanda.stop_node(prev)

        def new_controller_elected():
            curr = self.redpanda.controller()
            return curr and curr != prev

        wait_until(new_controller_elected,
                   timeout_sec=20,
                   backoff_sec=1,
                   err_msg="Controller did not failover")

        return prev

    def _restart(self, controller):
        self.redpanda.start_node(controller)

        wait_until(
            lambda: self.redpanda.healthy(),
            timeout_sec=20,
            backoff_sec=2,
            err_msg=f"Cluster did not become healthy after {controller} restart"
        )

    @cluster(num_nodes=3)
    def test_controller_recovery(self):
        big = self.scale.ci or self.scale.release
        iterations = 12 if big else 2

        for _ in range(iterations):
            controller = self._failover()
            self._restart(controller)
