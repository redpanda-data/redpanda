# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


class KafkaCliClientCompatTest(RedpandaTest):
    @cluster(num_nodes=3)
    def test_create_topic(self):
        for client_factory in KafkaCliTools.instances():
            client = client_factory(self.redpanda)
            topics = [TopicSpec() for _ in range(3)]
            for topic in topics:
                client.create_topic(topic)
            for topic in topics:
                spec = client.describe_topic(topic.name)
                assert spec == topic

    @cluster(num_nodes=3)
    def test_describe_broker_configs(self):
        # this uses the latest kafka client. older clients still need some work.
        # it seems as though at the protocol layer things work fine, but the
        # interface to cli clients are different. so some work generalizing the
        # client interface is needed.
        client_factory = KafkaCliTools.instances()[0]
        client = client_factory(self.redpanda)
        res = client.describe_broker_config()
        assert res.count("All configs for broker") == len(self.redpanda.nodes)
