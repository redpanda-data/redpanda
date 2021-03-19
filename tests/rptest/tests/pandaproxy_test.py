# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import http.client
import json
import logging
import uuid
import requests
from ducktape.mark.resource import cluster

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.tests.redpanda_test import RedpandaTest


def create_topic_names(count):
    return list(f"pandaproxy-topic-{uuid.uuid4()}" for _ in range(count))


class Consumer:
    def __init__(self, res):
        self.instance_id = res["instance_id"]
        self.base_uri = res["base_uri"]

    def subscribe(self, topics):
        res = requests.post(f"{self.base_uri}/subscription",
                            json.dumps({"topics": topics}))
        return res

    def remove(self):
        res = requests.delete(self.base_uri)
        return res


class PandaProxyTest(RedpandaTest):
    """
    Test pandaproxy against a redpanda cluster.
    """
    def __init__(self, context):
        super(PandaProxyTest, self).__init__(
            context,
            num_brokers=3,
            enable_pp=True,
            extra_rp_conf={"auto_create_topics_enabled": False})

        http.client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.getLogger().level)
        requests_log.propagate = True

    def _base_uri(self):
        return f"http://{self.redpanda.nodes[0].account.hostname}:8082"

    def _create_topics(self,
                       names=create_topic_names(1),
                       partitions=1,
                       replicas=1):
        self.logger.debug(f"Creating topics: {names}")
        kafka_tools = KafkaCliTools(self.redpanda)
        for name in names:
            kafka_tools.create_topic(
                TopicSpec(name=name,
                          partition_count=partitions,
                          replication_factor=replicas))
        return names

    def _wait_for_topic(self, name):
        kc = KafkaCat(self.redpanda)
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

    def _get_topics(self):
        return requests.get(f"{self._base_uri()}/topics").json()

    def _produce_topic(self, topic, data):
        return requests.post(f"{self._base_uri()}/topics/{topic}", data).json()

    def _create_consumer(self, group_id):
        res = requests.post(
            f"{self._base_uri()}/consumers/{group_id}", '''
            {
                "format": "binary",
                "auto.offset.reset": "earliest",
                "auto.commit.enable": "false",
                "fetch.min.bytes": "1",
                "consumer.request.timeout.ms": "10000"
            }''')
        return res

    @cluster(num_nodes=3)
    def test_list_topics(self):
        """
        Create some topics and verify that pandaproxy lists them.
        """
        prev = set(self._get_topics())
        self.logger.debug(f"Existing topics: {prev}")
        names = create_topic_names(3)
        assert prev.isdisjoint(names)
        self.logger.info(f"Creating test topics: {names}")
        names = set(self._create_topics(names))
        curr = set(self._get_topics())
        self.logger.debug(f"Current topics: {curr}")
        assert names <= curr

    @cluster(num_nodes=3)
    def test_produce_topic(self):
        """
        Create a topic and verify that pandaproxy can produce to it.
        """
        name = create_topic_names(1)[0]
        data = '''
        {
            "records": [
                {"value": "dmVjdG9yaXplZA==", "partition": 0},
                {"value": "cGFuZGFwcm94eQ==", "partition": 1},
                {"value": "bXVsdGlicm9rZXI=", "partition": 2}
            ]
        }'''

        self.logger.info(f"Producing to non-existant topic: {name}")
        produce_result = self._produce_topic(name, data)
        for o in produce_result["offsets"]:
            assert o["error_code"] == 3
            assert o["offset"] == -1

        self.logger.info(f"Creating test topic: {name}")
        self._create_topics([name], partitions=3)

        self.logger.debug("Waiting for leaders to settle")
        self._wait_for_topic(name)

        self.logger.info(f"Producing to topic: {name}")
        produce_result = self._produce_topic(name, data)
        for o in produce_result["offsets"]:
            assert o["offset"] == 1, f'error_code {o["error_code"]}'

        self.logger.info(f"Consuming from topic: {name}")
        kc = KafkaCat(self.redpanda)
        assert kc.consume_one(name, 0, 1)["payload"] == "vectorized"
        assert kc.consume_one(name, 1, 1)["payload"] == "pandaproxy"
        assert kc.consume_one(name, 2, 1)["payload"] == "multibroker"

    @cluster(num_nodes=3)
    def test_consumer_group(self):
        """
        Create a consumer group and use it
        """

        group_id = f"pandaproxy-group-{uuid.uuid4()}"

        # Create 3 topics
        topics = self._create_topics(create_topic_names(3), 3, 3)

        # Create a consumer
        self.logger.info("Create a consumer")
        cc_res = self._create_consumer(group_id)
        assert cc_res.status_code == requests.codes.ok
        c0 = Consumer(cc_res.json())

        # Subscribe a consumer
        self.logger.info(f"Subscribe consumer to topics: {topics}")
        sc_res = c0.subscribe(topics)
        assert sc_res.status_code == requests.codes.ok

        # Remove consumer
        self.logger.info("Remove consumer")
        rc_res = c0.remove()
        assert rc_res.status_code == requests.codes.no_content
