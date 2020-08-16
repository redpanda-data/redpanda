import uuid
import requests
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.pandaproxy import PandaProxyService


class PandaProxyTest(RedpandaTest):
    """
    Test pandaproxy against a redpanda cluster.
    """
    def __init__(self, context):
        super(PandaProxyTest, self).__init__(context, num_brokers=1)
        self._pandaproxy = PandaProxyService(context, self.redpanda)

    def setup(self):
        super(PandaProxyTest, self).setup()
        self._pandaproxy.start()

    def _get_topics(self):
        proxy = self._pandaproxy.nodes[0]
        url = "http://{}:8082/topics".format(proxy.account.hostname)
        return requests.get(url).json()

    @cluster(num_nodes=2)
    def test_list_topics(self):
        """
        Create some topics and verify that pandaproxy lists them.
        """
        names = set("pandaproxy-topic-{}".format(uuid.uuid4())
                    for _ in range(3))
        self.logger.debug("Topic names %s", names)

        prev = set(self._get_topics())
        self.logger.debug("Existing topics %s", prev)
        assert prev.isdisjoint(names)

        self.logger.debug("Creating test topics")
        kafka_tools = KafkaCliTools(self.redpanda)
        for name in names:
            kafka_tools.create_topic(name, replication_factor=1)

        curr = set(self._get_topics())
        self.logger.debug("Current topics %s", curr)
        assert names <= curr
