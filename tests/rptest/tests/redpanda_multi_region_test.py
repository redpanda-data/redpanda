# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.types import TopicSpec

from ducktape.utils.util import wait_until
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.cluster_topology import ClusterTopology, NetemSpec, Rack, Region, TopologyConnectionSpec, TopologyNode


class RedpandaNetworkDelayTest(PreallocNodesTest):
    """
    Tests that Redpanda is able to operate in multi-region environment
    """
    def __init__(self, test_context):
        self._message_size = 128
        self._message_cnt = 30000
        self._consumers = 2
        super(RedpandaNetworkDelayTest,
              self).__init__(test_context=test_context,
                             node_prealloc_count=1,
                             num_brokers=5)

    def setUp(self):
        # defer starting redpanda to test body
        pass

    def _start_producer(self, topic_name):
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            self._message_size,
            self._message_cnt,
            custom_node=self.preallocated_nodes)
        self.producer.start(clean=False)

        wait_until(lambda: self.producer.produce_status.acked > 10,
                   timeout_sec=30,
                   backoff_sec=1)

    def _start_consumer(self, topic_name):
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic_name,
            self._message_size,
            readers=self._consumers,
            nodes=self.preallocated_nodes)
        self.consumer.start(clean=False)

    def _verify(self):
        self.producer.wait()

        self.consumer.wait()
        assert self.consumer.consumer_status.validator.invalid_reads == 0
        del self.consumer

    @cluster(num_nodes=6)
    def test_basic_traffic_shaping(self):

        with ClusterTopology() as topology:
            topology.add_region(Region("region-a"))
            topology.add_region(Region("region-b"))
            topology.add_region(Region("region-c"))
            topology.add_rack("region-a", Rack('az-1'))
            topology.add_rack("region-a", Rack('az-2'))
            topology.add_rack("region-b", Rack('az-1'))
            topology.add_rack("region-b", Rack('az-2'))
            topology.add_rack("region-c", Rack('az-1'))

            topology.add_node("region-a", 'az-1', self.redpanda.nodes[0])
            topology.add_node("region-a", 'az-2', self.redpanda.nodes[1])
            topology.add_node("region-b", 'az-1', self.redpanda.nodes[2])
            topology.add_node("region-b", 'az-2', self.redpanda.nodes[3])
            topology.add_node("region-c", 'az-1', self.redpanda.nodes[4])

            topology.add_connection_spec(
                TopologyConnectionSpec(spec=NetemSpec(20, 10),
                                       source=TopologyNode(region='region-a'),
                                       target=TopologyNode(region='region-b')))

            topology.add_connection_spec(
                TopologyConnectionSpec(spec=NetemSpec(20, 10),
                                       source=TopologyNode(region='region-b'),
                                       target=TopologyNode(region='region-a')))
            topology.add_connection_spec(
                TopologyConnectionSpec(spec=NetemSpec(100, 20, loss=1.0),
                                       source=TopologyNode(region='region-c'),
                                       target=TopologyNode(region='region-a')))

            topology.add_connection_spec(
                TopologyConnectionSpec(spec=NetemSpec(100, 20, loss=1.0),
                                       source=TopologyNode(region='region-c'),
                                       target=TopologyNode(region='region-b')))

            topology.enable_traffic_shaping()

            topology.start_with_topology(self.redpanda)
            topic = TopicSpec(partition_count=16, replication_factor=5)

            self.client().create_topic(topic)
            self._start_producer(topic.name)
            self._start_consumer(topic.name)
            wait_until(lambda: self.producer.produce_status.acked > self.
                       _message_cnt / 2,
                       timeout_sec=180,
                       backoff_sec=1)

            admin = Admin(self.redpanda)

            brokers = admin.get_brokers()

            for b in brokers:
                n = self.redpanda.get_node_by_id(b['node_id'])
                placement = topology.placement_of(n)
                # TODO: change when we add support for regions in redpanda
                assert b['rack'] == f"{placement.region}.{placement.rack}"

            self.producer.wait()

            self.consumer.wait()
            assert self.consumer.consumer_status.validator.invalid_reads == 0
