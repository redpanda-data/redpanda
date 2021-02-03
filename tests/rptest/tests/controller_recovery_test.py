# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest


class ControllerRecoveryTest(RedpandaTest):
    """
    Shutdown the controller node and verify that a new controller is elected.
    """
    @cluster(num_nodes=3)
    def test_controller_recovery(self):
        prev = self.redpanda.controller()
        self.redpanda.stop_node(prev)

        def new_controller_elected():
            curr = self.redpanda.controller()
            return curr and curr != prev

        wait_until(new_controller_elected,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Controller did not failover")
