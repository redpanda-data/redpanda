from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster


class DummyProbeTest(RedpandaTest):
    @cluster(num_nodes=1)
    def dummy_test(self):
        assert False
