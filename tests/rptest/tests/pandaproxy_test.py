# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import http.client
import json
import uuid
import requests
from rptest.services.cluster import cluster
from ducktape.mark import ok_to_fail
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SecurityConfig


def create_topic_names(count):
    return list(f"pandaproxy-topic-{uuid.uuid4()}" for _ in range(count))


HTTP_GET_BROKERS_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_GET_TOPICS_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_FETCH_TOPIC_HEADERS = {
    "Accept": "application/vnd.kafka.binary.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_PRODUCE_BINARY_V2_TOPIC_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.binary.v2+json"
}

HTTP_PRODUCE_JSON_V2_TOPIC_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.json.v2+json"
}

HTTP_CREATE_CONSUMER_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_SUBSCRIBE_CONSUMER_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_REMOVE_CONSUMER_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_CONSUMER_FETCH_BINARY_V2_HEADERS = {
    "Accept": "application/vnd.kafka.binary.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_CONSUMER_FETCH_JSON_V2_HEADERS = {
    "Accept": "application/vnd.kafka.json.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_CONSUMER_GET_OFFSETS_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}

HTTP_CONSUMER_SET_OFFSETS_HEADERS = {
    "Accept": "application/vnd.kafka.v2+json",
    "Content-Type": "application/vnd.kafka.v2+json"
}


class Consumer:
    def __init__(self, res):
        self.instance_id = res["instance_id"]
        self.base_uri = res["base_uri"]

    def subscribe(self, topics, headers=HTTP_SUBSCRIBE_CONSUMER_HEADERS):
        res = requests.post(f"{self.base_uri}/subscription",
                            json.dumps({"topics": topics}),
                            headers=headers)
        return res

    def remove(self, headers=HTTP_REMOVE_CONSUMER_HEADERS):
        res = requests.delete(self.base_uri, headers=headers)
        return res

    def fetch(self, headers=HTTP_CONSUMER_FETCH_BINARY_V2_HEADERS):
        res = requests.get(f"{self.base_uri}/records", headers=headers)
        return res

    def get_offsets(self,
                    data=None,
                    headers=HTTP_CONSUMER_GET_OFFSETS_HEADERS):
        return requests.request(method='get',
                                url=f"{self.base_uri}/offsets",
                                data=data,
                                headers=headers)

    def set_offsets(self,
                    data=None,
                    headers=HTTP_CONSUMER_SET_OFFSETS_HEADERS):
        return requests.post(f"{self.base_uri}/offsets",
                             data=data,
                             headers=headers)


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
        http.client.print = lambda *args: self.logger.debug(" ".join(args))

    def _base_uri(self):
        return f"http://{self.redpanda.nodes[0].account.hostname}:8082"

    def _get_brokers(self, headers=HTTP_GET_BROKERS_HEADERS):
        return requests.get(f"{self._base_uri()}/brokers", headers=headers)

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
        assert set(names).issubset(self._get_topics().json())
        return names

    def _get_topics(self, headers=HTTP_GET_TOPICS_HEADERS):
        return requests.get(f"{self._base_uri()}/topics", headers=headers)

    def _produce_topic(self,
                       topic,
                       data,
                       headers=HTTP_PRODUCE_BINARY_V2_TOPIC_HEADERS):
        return requests.post(f"{self._base_uri()}/topics/{topic}",
                             data,
                             headers=headers)

    def _fetch_topic(self,
                     topic,
                     partition=0,
                     offset=0,
                     max_bytes=1024,
                     timeout_ms=1000,
                     headers=HTTP_FETCH_TOPIC_HEADERS):
        return requests.get(
            f"{self._base_uri()}/topics/{topic}/partitions/{partition}/records?offset={offset}&max_bytes={max_bytes}&timeout={timeout_ms}",
            headers=headers)

    def _create_consumer(self, group_id, headers=HTTP_CREATE_CONSUMER_HEADERS):
        res = requests.post(f"{self._base_uri()}/consumers/{group_id}",
                            '''
            {
                "format": "binary",
                "auto.offset.reset": "earliest",
                "auto.commit.enable": "false",
                "fetch.min.bytes": "1",
                "consumer.request.timeout.ms": "10000"
            }''',
                            headers=headers)
        return res

    def _create_named_consumer(self,
                               group_id,
                               name,
                               headers=HTTP_CREATE_CONSUMER_HEADERS):
        res = requests.post(f"{self._base_uri()}/consumers/{group_id}",
                            json.dumps({
                                "format": "binary",
                                "name": name,
                                "auto.offset.reset": "earliest",
                                "auto.commit.enable": "false",
                                "fetch.min.bytes": "1",
                                "consumer.request.timeout.ms": "10000"
                            }),
                            headers=headers)
        return res

    @cluster(num_nodes=3)
    def test_get_brokers(self):
        """
        Test get_brokers returns the set of node_ids
        """
        brokers_raw = self._get_brokers()
        brokers = brokers_raw.json()["brokers"]

        nodes = enumerate(self.redpanda.nodes, 1)
        node_idxs = [node[0] for node in nodes]

        assert sorted(brokers) == sorted(node_idxs)

    @cluster(num_nodes=3)
    def test_list_topics_validation(self):
        """
        Acceptable headers:
        * Accept: "", "*.*", "application/vnd.kafka.v2+json"

        """
        self.logger.debug(f"List topics with no accept header")
        result_raw = self._get_topics(
            {"Content-Type": "application/vnd.kafka.v2+json"})
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.headers[
            "Content-Type"] == "application/vnd.kafka.v2+json"

        self.logger.debug(f"List topics with no content-type header")
        result_raw = self._get_topics({
            "Accept":
            "application/vnd.kafka.v2+json",
        })
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.headers[
            "Content-Type"] == "application/vnd.kafka.v2+json"

        self.logger.debug(f"List topics with generic accept header")
        result_raw = self._get_topics({"Accept": "*/*"})
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.headers[
            "Content-Type"] == "application/vnd.kafka.v2+json"

        self.logger.debug(f"List topics with generic content-type header")
        result_raw = self._get_topics({"Content-Type": "*/*"})
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.headers[
            "Content-Type"] == "application/vnd.kafka.v2+json"

        self.logger.debug(f"List topics with invalid accept header")
        result_raw = self._get_topics({"Accept": "application/json"})
        assert result_raw.status_code == requests.codes.not_acceptable
        assert result_raw.headers["Content-Type"] == "application/json"

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
        result_raw = self._get_topics()
        assert result_raw.status_code == requests.codes.ok
        curr = set(result_raw.json())
        self.logger.debug(f"Current topics: {curr}")
        assert names <= curr

    @cluster(num_nodes=3)
    def test_produce_topic_validation(self):
        """
        Acceptable headers:
        * Accept: "", "*.*", "application/vnd.kafka.v2+json"
        * Content-Type: "application/vnd.kafka.binary.v2+json"

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

        self.logger.info(f"Producing with no accept header")
        produce_result_raw = self._produce_topic(
            name,
            data,
            headers={"Content-Type": "application/vnd.kafka.binary.v2+json"})
        assert produce_result_raw.status_code == requests.codes.ok
        produce_result = produce_result_raw.json()
        assert produce_result["offsets"][0][
            "error_code"] == 3  # topic not found

        self.logger.info(f"Producing with unsupported accept header")
        produce_result_raw = self._produce_topic(
            name,
            data,
            headers={
                "Accept": "application/vnd.kafka.binary.v2+json",
                "Content-Type": "application/vnd.kafka.binary.v2+json"
            })
        assert produce_result_raw.status_code == requests.codes.not_acceptable
        produce_result = produce_result_raw.json()
        assert produce_result["error_code"] == requests.codes.not_acceptable

        self.logger.info(f"Producing with no content-type header")
        produce_result_raw = self._produce_topic(
            name, data, headers={"Accept": "application/vnd.kafka.v2+json"})
        assert produce_result_raw.status_code == requests.codes.unsupported_media_type
        produce_result = produce_result_raw.json()
        assert produce_result[
            "error_code"] == requests.codes.unsupported_media_type

        self.logger.info(f"Producing with unsupported content-type header")
        produce_result_raw = self._produce_topic(
            name,
            data,
            headers={
                "Accept": "application/vnd.kafka.v2+json",
                "Content-Type": "application/vnd.kafka.v2+json"
            })
        assert produce_result_raw.status_code == requests.codes.unsupported_media_type
        produce_result = produce_result_raw.json()
        assert produce_result[
            "error_code"] == requests.codes.unsupported_media_type

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
        produce_result_raw = self._produce_topic(name, data)
        assert produce_result_raw.status_code == requests.codes.ok
        produce_result = produce_result_raw.json()
        for o in produce_result["offsets"]:
            assert o["error_code"] == 3
            assert o["offset"] == -1

        self.logger.info(f"Creating test topic: {name}")
        self._create_topics([name], partitions=3)

        self.logger.info(f"Producing to topic: {name}")
        produce_result_raw = self._produce_topic(name, data)
        assert produce_result_raw.status_code == requests.codes.ok
        assert produce_result_raw.headers[
            "Content-Type"] == "application/vnd.kafka.v2+json"

        produce_result = produce_result_raw.json()
        for o in produce_result["offsets"]:
            assert o["offset"] == 0, f'error_code {o["error_code"]}'

        self.logger.info(f"Consuming from topic: {name}")
        kc = KafkaCat(self.redpanda)
        assert kc.consume_one(name, 0, 0)["payload"] == "vectorized"
        assert kc.consume_one(name, 1, 0)["payload"] == "pandaproxy"
        assert kc.consume_one(name, 2, 0)["payload"] == "multibroker"

        self.logger.info(f"Producing to topic without partition: {name}")
        produce_result_raw = self._produce_topic(
            name, '''
        {
            "records": [
                {"value": "dmVjdG9yaXplZA=="},
                {"value": "cGFuZGFwcm94eQ=="},
                {"value": "bXVsdGlicm9rZXI="}
            ]
        }''')

        assert produce_result_raw.status_code == requests.codes.ok
        produce_result = produce_result_raw.json()
        for o in produce_result["offsets"]:
            assert o["offset"] == 1, f'error_code {o["error_code"]}'

    @cluster(num_nodes=3)
    def test_fetch_topic_validation(self):
        """
        Acceptable headers:
        * Accept: "application/vnd.kafka.binary.v2+json"
        * Content-Type: "application/vnd.kafka.v2+json"
        Required Params:
        * Path:
          * topic
          * partition
        * Query:
          * offset
          * timeout
          * max_bytes
        """
        self.logger.info(f"Consuming with empty topic param")
        fetch_raw_result = self._fetch_topic("", 0)
        assert fetch_raw_result.status_code == requests.codes.bad_request

        name = create_topic_names(1)[0]

        self.logger.info(f"Consuming with empty offset param")
        fetch_raw_result = self._fetch_topic(name, 0, "")
        assert fetch_raw_result.status_code == requests.codes.bad_request

        self.logger.info(f"Consuming from unknown topic: {name}")
        fetch_raw_result = self._fetch_topic(name, 0)
        assert fetch_raw_result.status_code == requests.codes.not_found
        fetch_result = fetch_raw_result.json()
        assert fetch_result["error_code"] == 40402

        self.logger.info(f"Consuming with no content-type header")
        fetch_raw_result = self._fetch_topic(
            name,
            0,
            headers={"Accept": "application/vnd.kafka.binary.v2+json"})
        assert fetch_raw_result.status_code == requests.codes.not_found
        fetch_result = fetch_raw_result.json()
        assert fetch_result["error_code"] == 40402

        self.logger.info(f"Consuming with no accept header")
        fetch_raw_result = self._fetch_topic(
            name, 0, headers={"Content-Type": "application/vnd.kafka.v2+json"})
        assert fetch_raw_result.status_code == requests.codes.not_acceptable
        fetch_result = fetch_raw_result.json()
        assert fetch_result["error_code"] == requests.codes.not_acceptable

        self.logger.info(f"Consuming with unsupported accept header")
        fetch_raw_result = self._fetch_topic(
            name,
            0,
            headers={
                "Accept": "application/vnd.kafka.v2+json",
                "Content-Type": "application/vnd.kafka.v2+json"
            })
        assert fetch_raw_result.status_code == requests.codes.not_acceptable
        fetch_result = fetch_raw_result.json()
        assert fetch_result["error_code"] == requests.codes.not_acceptable

    @cluster(num_nodes=3)
    def test_fetch_topic(self):
        """
        Create a topic, publish to it, and verify that pandaproxy can fetch
        from it.
        """
        name = create_topic_names(1)[0]

        self.logger.info(f"Creating test topic: {name}")
        self._create_topics([name], partitions=3)

        self.logger.info(f"Producing to topic: {name}")
        data = '''
        {
            "records": [
                {"value": "dmVjdG9yaXplZA==", "partition": 0},
                {"value": "cGFuZGFwcm94eQ==", "partition": 1},
                {"value": "bXVsdGlicm9rZXI=", "partition": 2}
            ]
        }'''
        produce_result_raw = self._produce_topic(name, data)
        assert produce_result_raw.status_code == requests.codes.ok
        produce_result = produce_result_raw.json()
        for o in produce_result["offsets"]:
            assert o["offset"] == 0, f'error_code {o["error_code"]}'

        self.logger.info(f"Consuming from topic: {name}")
        fetch_raw_result_0 = self._fetch_topic(name, 0)
        assert fetch_raw_result_0.status_code == requests.codes.ok
        fetch_result_0 = fetch_raw_result_0.json()
        expected = json.loads(data)
        assert len(fetch_result_0) == 1
        assert fetch_result_0[0]["topic"] == name
        assert fetch_result_0[0]["key"] is None
        assert fetch_result_0[0]["value"] == expected["records"][0]["value"]
        assert fetch_result_0[0]["partition"] == expected["records"][0][
            "partition"]
        assert fetch_result_0[0]["offset"] == 0

    @cluster(num_nodes=3)
    def test_create_consumer_validation(self):
        """
        Acceptable headers:
        * Accept: "", "*/*", "application/vnd.kafka.v2+json"
        * Content-Type: "application/vnd.kafka.v2+json"
        Required Params:
        * Path:
          * group
        """
        group_id = f"pandaproxy-group-{uuid.uuid4()}"

        self.logger.info("Create a consumer with no accept header")
        cc_res = self._create_consumer(
            group_id,
            headers={
                "Content-Type": HTTP_CREATE_CONSUMER_HEADERS["Content-Type"]
            })
        assert cc_res.status_code == requests.codes.ok
        assert cc_res.headers["Content-Type"] == HTTP_CREATE_CONSUMER_HEADERS[
            "Accept"]

        self.logger.info("Create a consumer with invalid accept header")
        cc_res = self._create_consumer(
            group_id,
            headers={
                "Content-Type": HTTP_CREATE_CONSUMER_HEADERS["Content-Type"],
                "Accept": "application/vnd.kafka.binary.v2+json"
            })
        assert cc_res.status_code == requests.codes.not_acceptable
        assert cc_res.json()["error_code"] == requests.codes.not_acceptable
        assert cc_res.headers["Content-Type"] == "application/json"

        self.logger.info("Create a consumer with no content-type header")
        cc_res = self._create_consumer(
            group_id,
            headers={"Accept": HTTP_CREATE_CONSUMER_HEADERS["Accept"]})
        assert cc_res.status_code == requests.codes.unsupported_media_type
        assert cc_res.json(
        )["error_code"] == requests.codes.unsupported_media_type

        self.logger.info("Create a consumer with no group parameter")
        cc_res = self._create_consumer("",
                                       headers=HTTP_CREATE_CONSUMER_HEADERS)
        # It's not possible to return an error body in this case due to the way
        # ss::httpd::path_description and routing works - path can't be matched
        assert cc_res.status_code == requests.codes.not_found

        self.logger.info("Create a named consumer")
        cc_res = self._create_named_consumer(group_id, "my_consumer")
        assert cc_res.status_code == requests.codes.ok

        self.logger.info("Create a consumer with duplicate name")
        cc_res = self._create_named_consumer(group_id, "my_consumer")
        assert cc_res.status_code == requests.codes.conflict
        assert cc_res.json()["error_code"] == 40902

    @cluster(num_nodes=3)
    def test_subscribe_consumer_validation(self):
        """
        Acceptable headers:
        * Accept: "", "*/*", "application/vnd.kafka.v2+json"
        * Content-Type: "application/vnd.kafka.v2+json"
        Required Params:
        * Path:
          * group
          * instance
        """
        group_id = f"pandaproxy-group-{uuid.uuid4()}"

        self.logger.info("Create 3 topics")
        topics = self._create_topics(create_topic_names(3), 3, 3)

        self.logger.info("Create a consumer group")
        cc_res = self._create_consumer(group_id)
        assert cc_res.status_code == requests.codes.ok

        c0 = Consumer(cc_res.json())

        self.logger.info("Subscribe a consumer with no accept header")
        sc_res = c0.subscribe(
            topics,
            headers={
                "Content-Type": HTTP_SUBSCRIBE_CONSUMER_HEADERS["Content-Type"]
            })
        assert sc_res.status_code == requests.codes.no_content
        assert sc_res.headers[
            "Content-Type"] == HTTP_SUBSCRIBE_CONSUMER_HEADERS["Accept"]

        self.logger.info("Subscribe a consumer with invalid accept header")
        sc_res = c0.subscribe(
            topics,
            headers={
                "Content-Type":
                HTTP_SUBSCRIBE_CONSUMER_HEADERS["Content-Type"],
                "Accept": "application/vnd.kafka.binary.v2+json"
            })
        assert sc_res.status_code == requests.codes.not_acceptable
        assert sc_res.json()["error_code"] == requests.codes.not_acceptable
        assert sc_res.headers["Content-Type"] == "application/json"

        self.logger.info("Subscribe a consumer with no content-type header")
        sc_res = c0.subscribe(
            topics,
            headers={"Accept": HTTP_SUBSCRIBE_CONSUMER_HEADERS["Accept"]})
        assert sc_res.status_code == requests.codes.unsupported_media_type
        assert sc_res.json(
        )["error_code"] == requests.codes.unsupported_media_type

        self.logger.info("Subscribe a consumer with invalid group parameter")
        sc_res = requests.post(
            f"{self._base_uri()}/consumers/{group_id}-invalid/instances/{c0.instance_id}/subscription",
            json.dumps({"topics": topics}),
            headers=HTTP_SUBSCRIBE_CONSUMER_HEADERS)
        assert sc_res.status_code == requests.codes.not_found
        assert sc_res.json()["error_code"] == 40403

        self.logger.info(
            "Subscribe a consumer with invalid instance parameter")
        sc_res = requests.post(
            f"{self._base_uri()}/consumers/{group_id}/instances/{c0.instance_id}-invalid/subscription",
            json.dumps({"topics": topics}),
            headers=HTTP_SUBSCRIBE_CONSUMER_HEADERS)
        assert sc_res.status_code == requests.codes.not_found
        assert sc_res.json()["error_code"] == 40403

    @cluster(num_nodes=3)
    def test_remove_consumer_validation(self):
        """
        Acceptable headers:
        * Accept: "", "*/*", "application/vnd.kafka.v2+json"
        * Content-Type: "application/vnd.kafka.v2+json"
        Required Params:
        * Path:
          * group
          * instance
        """
        group_id = f"pandaproxy-group-{uuid.uuid4()}"

        self.logger.info("Create 3 topics")
        topics = self._create_topics(create_topic_names(3), 3, 3)

        self.logger.info("Create a consumer group")
        cc_res = self._create_consumer(group_id)
        assert cc_res.status_code == requests.codes.ok

        c0 = Consumer(cc_res.json())

        self.logger.info("Remove a consumer with invalid accept header")
        sc_res = c0.remove(
            headers={
                "Content-Type": HTTP_REMOVE_CONSUMER_HEADERS["Content-Type"],
                "Accept": "application/vnd.kafka.binary.v2+json"
            })
        assert sc_res.status_code == requests.codes.not_acceptable
        assert sc_res.json()["error_code"] == requests.codes.not_acceptable
        assert sc_res.headers["Content-Type"] == "application/json"

        self.logger.info("Remove a consumer with no content-type header")
        sc_res = c0.remove(
            headers={"Accept": HTTP_REMOVE_CONSUMER_HEADERS["Accept"]})
        assert sc_res.status_code == requests.codes.unsupported_media_type
        assert sc_res.json(
        )["error_code"] == requests.codes.unsupported_media_type

        self.logger.info("Remove a consumer with invalid group parameter")
        sc_res = requests.delete(
            f"{self._base_uri()}/consumers/{group_id}-invalid/instances/{c0.instance_id}",
            headers=HTTP_REMOVE_CONSUMER_HEADERS)
        assert sc_res.status_code == requests.codes.not_found
        assert sc_res.json()["error_code"] == 40403

        self.logger.info("Remove a consumer with invalid instance parameter")
        sc_res = requests.delete(
            f"{self._base_uri()}/consumers/{group_id}/instances/{c0.instance_id}-invalid",
            headers=HTTP_REMOVE_CONSUMER_HEADERS)
        assert sc_res.status_code == requests.codes.not_found
        assert sc_res.json()["error_code"] == 40403

        self.logger.info("Remove a consumer with no accept header")
        sc_res = c0.remove(
            headers={
                "Content-Type": HTTP_REMOVE_CONSUMER_HEADERS["Content-Type"]
            })
        assert sc_res.status_code == requests.codes.no_content

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/3454
    @cluster(num_nodes=3)
    def test_consumer_group_binary_v2(self):
        """
        Create a consumer group and use it
        """

        group_id = f"pandaproxy-group-{uuid.uuid4()}"

        # Create 3 topics
        topics = self._create_topics(create_topic_names(3), 3, 3)

        for name in topics:
            self.logger.info(f"Producing to topic: {name}")
            produce_result_raw = self._produce_topic(
                name, '''
            {
                "records": [
                    {"value": "dmVjdG9yaXplZA==", "partition": 0},
                    {"value": "cGFuZGFwcm94eQ==", "partition": 1},
                    {"value": "bXVsdGlicm9rZXI=", "partition": 2}
                ]
            }''')
            assert produce_result_raw.status_code == requests.codes.ok

        # Create a consumer
        self.logger.info("Create a consumer")
        cc_res = self._create_consumer(group_id)
        assert cc_res.status_code == requests.codes.ok
        c0 = Consumer(cc_res.json())

        # Subscribe a consumer
        self.logger.info(f"Subscribe consumer to topics: {topics}")
        sc_res = c0.subscribe(topics)
        assert sc_res.status_code == requests.codes.no_content

        # Get consumer offsets
        co_req = dict(partitions=[
            dict(topic=t, partition=p) for t in topics for p in [0, 1, 2]
        ])
        self.logger.info(f"Get consumer offsets")
        co_res_raw = c0.get_offsets(data=json.dumps(co_req))
        assert co_res_raw.status_code == requests.codes.ok
        co_res = co_res_raw.json()
        assert len(co_res["offsets"]) == 9
        for i in range(len(co_res["offsets"])):
            assert co_res["offsets"][i]["offset"] == -1

        # Fetch from a consumer
        self.logger.info(f"Consumer fetch")
        cf_res = c0.fetch()
        assert cf_res.status_code == requests.codes.ok
        fetch_result = cf_res.json()
        # 3 topics * 3 msg
        assert len(fetch_result) == 3 * 3
        print(fetch_result)

        self.logger.info(f"Get consumer offsets")
        co_res_raw = c0.get_offsets(data=json.dumps(co_req))
        assert co_res_raw.status_code == requests.codes.ok
        co_res = co_res_raw.json()
        assert len(co_res["offsets"]) == 9
        for i in range(len(co_res["offsets"])):
            assert co_res["offsets"][i]["offset"] == -1

        # Set consumer offsets
        sco_req = dict(partitions=[
            dict(topic=t, partition=p, offset=0) for t in topics
            for p in [0, 1, 2]
        ])
        self.logger.info(f"Set consumer offsets")
        co_res_raw = c0.set_offsets(data=json.dumps(sco_req))
        assert co_res_raw.status_code == requests.codes.no_content

        self.logger.info(f"Get consumer offsets")
        co_res_raw = c0.get_offsets(data=json.dumps(co_req))
        assert co_res_raw.status_code == requests.codes.ok
        co_res = co_res_raw.json()
        assert len(co_res["offsets"]) == 9
        for i in range(len(co_res["offsets"])):
            assert co_res["offsets"][i]["offset"] == 0

        # Remove consumer
        self.logger.info("Remove consumer")
        rc_res = c0.remove()
        assert rc_res.status_code == requests.codes.no_content

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/2501
    @cluster(num_nodes=3)
    def test_consumer_group_json_v2(self):
        """
        Create a consumer group and use it
        """

        group_id = f"pandaproxy-group-{uuid.uuid4()}"

        # Create 3 topics
        topics = self._create_topics(create_topic_names(3), 3, 3)

        for name in topics:
            self.logger.info(f"Producing to topic: {name}")
            produce_result_raw = self._produce_topic(
                name,
                '''
            {
                "records": [
                    {"value": {"object":["vectorized"]}, "partition": 0},
                    {"value": {"object":["pandaproxy"]}, "partition": 0},
                    {"value": {"object":["multibroker"]}, "partition": 0}
                ]
            }''',
                headers=HTTP_PRODUCE_JSON_V2_TOPIC_HEADERS)
            print(produce_result_raw.content)
            assert produce_result_raw.status_code == requests.codes.ok

        # Create a consumer
        self.logger.info("Create a consumer")
        cc_res = self._create_consumer(group_id)
        assert cc_res.status_code == requests.codes.ok
        c0 = Consumer(cc_res.json())

        # Subscribe a consumer
        self.logger.info(f"Subscribe consumer to topics: {topics}")
        sc_res = c0.subscribe(topics)
        assert sc_res.status_code == requests.codes.no_content

        # Fetch from a consumer
        self.logger.info(f"Consumer fetch")
        cf_res = c0.fetch(headers=HTTP_CONSUMER_FETCH_JSON_V2_HEADERS)
        assert cf_res.status_code == requests.codes.ok
        fetch_result = cf_res.json()
        # 3 topics * 3 msg
        assert len(fetch_result) == 3 * 3
        for r in fetch_result:
            assert r["value"]["object"]

        # Remove consumer
        self.logger.info("Remove consumer")
        rc_res = c0.remove()
        assert rc_res.status_code == requests.codes.no_content


class PandaProxySASLTest(RedpandaTest):
    """
    Test pandaproxy can connect using SASL.
    """
    def __init__(self, context):
        extra_rp_conf = dict(auto_create_topics_enabled=False, )

        security = SecurityConfig()
        security.enable_sasl = True

        super(PandaProxySASLTest, self).__init__(context,
                                                 num_brokers=3,
                                                 enable_pp=True,
                                                 security=security,
                                                 extra_rp_conf=extra_rp_conf)

        http.client.HTTPConnection.debuglevel = 1
        http.client.print = lambda *args: self.logger.debug(" ".join(args))

    def _get_super_client(self):
        user, password, _ = self.redpanda.SUPERUSER_CREDENTIALS
        return KafkaCliTools(self.redpanda, user=user, passwd=password)

    def _base_uri(self):
        return f"http://{self.redpanda.nodes[0].account.hostname}:8082"

    def _get_topics(self, headers=HTTP_GET_TOPICS_HEADERS):
        return requests.get(f"{self._base_uri()}/topics", headers=headers)

    @cluster(num_nodes=3)
    def test_list_topics(self):
        client = self._get_super_client()
        topic_specs = [TopicSpec() for _ in range(1)]
        for spec in topic_specs:
            client.create_topic(spec)

        expected_topics = set((t.name for t in topic_specs))

        def topics_appeared():
            listed_topics = set(self._get_topics().json())
            self.logger.debug(
                f"Listed {listed_topics} expected {expected_topics}")
            return listed_topics == expected_topics

        wait_until(topics_appeared,
                   timeout_sec=20,
                   backoff_sec=2,
                   err_msg="Timeout waiting for topics to appear.")
