from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool


class RpkToolTest(RedpandaTest):
    def test_create_topic(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic")

        wait_until(lambda: "topic" in rpk.list_topics(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Topic never appeared.")
