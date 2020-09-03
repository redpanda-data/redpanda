from ducktape.mark import matrix
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.kafka_cli_tools import KafkaCliTools


class DescribeTopicsTest(RedpandaTest):
    def test_describe_topics(self):
        tools = KafkaCliTools(self.redpanda)
        tools.create_topic("topic", partitions=2, replication_factor=3)
        output = tools.describe_topics()
        assert "partition_count=2" in output
        assert "replication_factor=3" in output
