from ducktape.mark import matrix
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.kafka_cli_tools import KafkaCliTools


class KafkaCliToolsTest(RedpandaTest):
    """
    Verify that kafka cli tools works for all supported versions.
    """
    @matrix(version=KafkaCliTools.VERSIONS)
    def test_create_topic(self, version):
        tools = KafkaCliTools(self.redpanda, version)
        topics = ["v{}.{}".format(version, i) for i in range(3)]
        for topic in topics:
            tools.create_topic(topic)
        assert set(topics) <= set(tools.list_topics())
