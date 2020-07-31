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

        wait_until(lambda: new_controller_elected(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Controller did not failover")
