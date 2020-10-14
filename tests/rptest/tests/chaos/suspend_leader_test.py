import sys
sys.path.append("/opt/v/src/consistency-testing/gobekli")
sys.path.append("/opt/v/src/consistency-testing/chaostest")
from chaostest.faults import SuspendServiceRecoverableFault
from rptest.tests.chaos.chaos_base_test import BaseChaosTest
from ducktape.mark.resource import cluster


class SuspendLeaderTest(BaseChaosTest):
    def __init__(self, test_context, num_nodes=3):
        super(SuspendLeaderTest, self).__init__(test_context, num_nodes)

    @cluster(num_nodes=3)
    def test_suspend_leader(self):
        failure_factory = lambda: SuspendServiceRecoverableFault(
            lambda x: x.get_leader(), "leader")
        self.run(failure_factory)
