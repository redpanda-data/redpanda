from ducktape.tests.test import Test
from rptest.services.redpanda import RedpandaService


class RedpandaTest(Test):
    """
    Base class for tests that use the Redpanda service.
    """
    def __init__(self,
                 test_context,
                 num_brokers=3,
                 extra_rp_conf=dict(),
                 topics=None):
        super(RedpandaTest, self).__init__(test_context)

        self.redpanda = RedpandaService(test_context,
                                        num_brokers=num_brokers,
                                        extra_rp_conf=extra_rp_conf,
                                        topics=topics)

    def setUp(self):
        self.redpanda.start()
