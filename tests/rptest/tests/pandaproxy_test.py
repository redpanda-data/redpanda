# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import uuid
import requests
import time
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.pandaproxy import PandaProxyService


class PandaProxyTest(RedpandaTest):
    """
    Test pandaproxy against a redpanda cluster.
    """
    def __init__(self, context):
        super(PandaProxyTest, self).__init__(context, num_brokers=3)
        self._pandaproxy = PandaProxyService(context, self.redpanda)

    def setup(self):
        super(PandaProxyTest, self).setup()
        self._pandaproxy.start()

    def _get_topics(self):
        proxy = self._pandaproxy.nodes[0]
        url = "http://{}:8082/topics".format(proxy.account.hostname)
        return requests.get(url).json()

    def _produce_topic(self, topic, data):
        proxy = self._pandaproxy.nodes[0]
        url = "http://{}:8082/topics/{}".format(proxy.account.hostname, topic)
        self.logger.info(data)
        return requests.post(url, data).json()

    @cluster(num_nodes=4)
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
            kafka_tools.create_topic(TopicSpec(name=name,
                                               replication_factor=1))

        curr = set(self._get_topics())
        self.logger.debug("Current topics %s", curr)
        assert names <= curr

    @cluster(num_nodes=4)
    def test_produce_topic(self):
        """
        Create a topic and verify that pandaproxy can produce to it.
        """
        name = "pandaproxy-topic-{}".format(uuid.uuid4())
        self.logger.debug("Topic name %s", name)

        prev = set(self._get_topics())
        self.logger.debug("Existing topics %s", prev)
        assert prev.isdisjoint(name)

        data = '{"records": [{"value": "dmVjdG9yaXplZA==", "partition": 0},{"value": "cGFuZGFwcm94eQ==", "partition": 1},{"value": "bXVsdGlicm9rZXI=", "partition": 2}]}'

        self.logger.debug("Producing to non-existant topic")
        produce_result = self._produce_topic(name, data)
        for o in produce_result["offsets"]:
            assert o["error_code"] == 3
            assert o["offset"] == -1

        kc = KafkaCat(self.redpanda)

        self.logger.debug("Creating test topic")
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.create_topic(
            TopicSpec(name=name, replication_factor=1, partition_count=3))

        self.logger.debug("Waiting for leaders to settle")
        has_leaders = False
        while not has_leaders:
            topics = kc.metadata()["topics"]
            maybe_leaders = True
            for t in topics:
                if t["topic"] == name:
                    for p in t["partitions"]:
                        if p["leader"] == -1:
                            maybe_leaders = False
            has_leaders = maybe_leaders
        # TODO:
        #  Despite the above test, Pandaproxy can still get back no leaders
        #  Query Pandaproxy metadata to see when leaders have settled
        #  The retry logic for produce should have sufficient time for this
        #  additional settle time.

        self.logger.debug("Producing to topic")
        produce_result = self._produce_topic(name, data)
        self.logger.debug("Producing to topic: %s", produce_result)
        for o in produce_result["offsets"]:
            assert o["offset"] == 1, f'error_code {o["error_code"]}'

        self.logger.debug(f"Consuming topic: {name}")
        assert kc.consume_one(name, 0, 1)["payload"] == "vectorized"
        assert kc.consume_one(name, 1, 1)["payload"] == "pandaproxy"
        assert kc.consume_one(name, 2, 1)["payload"] == "multibroker"
