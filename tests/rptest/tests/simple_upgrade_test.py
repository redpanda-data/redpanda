from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from ducktape.utils.util import wait_until
from ducktape.mark import ignore
from rptest.services.admin import Admin
import confluent_kafka as ck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
import time

class SimpleUpgradeTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3, replication_factor=3), )

    def __init__(self, test_context):
        super(SimpleUpgradeTest,
              self).__init__(test_context=test_context,
                             num_brokers=3)

    @cluster(num_nodes=4)
    def simple_upgrade_test(self):
        kaf_producer = KafProducer(self.test_context, self.redpanda, self.topic, 1000)
        kaf_producer.start()
        kaf_producer.wait()

        # Consume smth
        rpk = RpkConsumer(context=self.test_context, redpanda=self.redpanda, topic=self.topic, group="foo")
        rpk.start()
        time.sleep(10)
        rpk.stop()
        rpk.wait()

        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node, timeout=300)
            node.account.ssh("curl -1sLf https://packages.vectorized.io/E4xN1tVe3Xy60GTx/redpanda-unstable/setup.deb.sh | sudo -E bash", allow_fail=False)
            node.account.ssh('sudo apt -o  Dpkg::Options::="--force-confnew" install redpanda', allow_fail=False)
            node.account.ssh("sudo systemctl stop redpanda")
            self.redpanda.start_node(node, None, timeout=300)