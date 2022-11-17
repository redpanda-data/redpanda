# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import uuid
import requests
import threading
from rptest.services.cluster import cluster
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SecurityConfig, LoggingConfig, ResourceSettings, ProxyConfig
from rptest.services.admin import Admin
from typing import Optional, List, Dict, Union


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

log_config = LoggingConfig('info',
                           logger_levels={
                               'security': 'trace',
                               'pandaproxy': 'trace',
                               'kafka/client': 'trace'
                           })


class Consumer:
    def __init__(self, res, logger):
        self.instance_id = res["instance_id"]
        self.base_uri = res["base_uri"]
        self.logger = logger

    def subscribe(self,
                  topics,
                  headers=HTTP_SUBSCRIBE_CONSUMER_HEADERS,
                  **kwargs):
        res = requests.post(f"{self.base_uri}/subscription",
                            json.dumps({"topics": topics}),
                            headers=headers,
                            **kwargs)
        return res

    def remove(self, headers=HTTP_REMOVE_CONSUMER_HEADERS, **kwargs):
        res = requests.delete(self.base_uri, headers=headers, **kwargs)
        return res

    def fetch(self, headers=HTTP_CONSUMER_FETCH_BINARY_V2_HEADERS, **kwargs):
        res = requests.get(f"{self.base_uri}/records",
                           headers=headers,
                           **kwargs)
        return res

    def fetch_n(self, count, timeout_sec=10):
        fetch_result = []

        def do_fetch():
            cf_res = self.fetch()
            assert cf_res.status_code == requests.codes.ok
            records = cf_res.json()
            self.logger.debug(f"Fetched {len(records)} records: {records}")
            fetch_result.extend(records)
            if len(fetch_result) != count:
                self.logger.info(f"Fetch Mitigation {len(fetch_result)}")
            return len(fetch_result) == count

        wait_until(lambda: do_fetch(),
                   timeout_sec=timeout_sec,
                   backoff_sec=0,
                   err_msg="Timeout waiting for records to appear")

        return fetch_result

    def get_offsets(self,
                    data=None,
                    headers=HTTP_CONSUMER_GET_OFFSETS_HEADERS,
                    **kwargs):
        return requests.request(method='get',
                                url=f"{self.base_uri}/offsets",
                                data=data,
                                headers=headers,
                                **kwargs)

    def set_offsets(self,
                    data=None,
                    headers=HTTP_CONSUMER_SET_OFFSETS_HEADERS,
                    **kwargs):
        return requests.post(f"{self.base_uri}/offsets",
                             data=data,
                             headers=headers,
                             **kwargs)


class PandaProxyBrokersTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=4, **kwargs)

    def setUp(self):
        # Start the nodes manually for this test.
        pass

    @cluster(num_nodes=4)
    def test_get_brokers_with_stale_health_metadata(self):
        """
        Test that a new member will be returned by /brokers
        requests processed by followers even when the health metadata
        is out of sync.
        """
        # Start first three nodes
        assert len(self.redpanda.nodes) == 4
        self.redpanda.start(self.redpanda.nodes[0:-1])

        # Make the maximum metadata longer than the duration of the
        # tests to ensure it won't be refreshed.
        one_minute = 1000 * 60
        self.redpanda.set_cluster_config(
            {"health_monitor_max_metadata_age": one_minute})

        # Start the late joiner
        self.redpanda.start([self.redpanda.nodes[-1]])

        # Check that the new broker is returned regardless
        # of the health metadata being stale.
        wait_until(lambda: self.redpanda.registered(self.redpanda.nodes[-1]),
                   timeout_sec=5,
                   backoff_sec=1)


class PandaProxyEndpoints(RedpandaTest):
    """
    All the Pandaproxy endpoints
    """
    def __init__(self, context, **kwargs):
        super(PandaProxyEndpoints, self).__init__(
            context,
            num_brokers=3,
            extra_rp_conf={"auto_create_topics_enabled": False},
            log_config=log_config,
            **kwargs)

        http.client.HTTPConnection.debuglevel = 1
        http.client.print = lambda *args: self.logger.debug(" ".join(args))

    def _get_kafka_cli_tools(self):
        sasl_enabled = self.redpanda.sasl_enabled()
        cfg = self.redpanda.security_config() if sasl_enabled else {}
        return KafkaCliTools(self.redpanda,
                             user=cfg.get('sasl_plain_username'),
                             passwd=cfg.get('sasl_plain_password'))

    def _base_uri(self, hostname=None):
        hostname = hostname if hostname else self.redpanda.nodes[
            0].account.hostname
        return f"http://{hostname}:8082"

    def _get_brokers(self, headers=HTTP_GET_BROKERS_HEADERS, **kwargs):
        return requests.get(f"{self._base_uri()}/brokers",
                            headers=headers,
                            **kwargs)

    def _create_topics(self,
                       names=create_topic_names(1),
                       partitions=1,
                       replicas=1):
        self.logger.debug(f"Creating topics: {names}")
        kafka_tools = self._get_kafka_cli_tools()
        for name in names:
            kafka_tools.create_topic(
                TopicSpec(name=name,
                          partition_count=partitions,
                          replication_factor=replicas))

        def has_topics():
            self_topics = self._get_topics()
            self.logger.info(
                f"set(names): {set(names)}, self._get_topics().status_code: {self_topics.status_code}, self_topics.json(): {self_topics.json()}"
            )
            return set(names).issubset(self_topics.json())

        wait_until(has_topics,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Timeout waiting for topics: {names}")

        return names

    def _get_topics(self,
                    headers=HTTP_GET_TOPICS_HEADERS,
                    hostname=None,
                    **kwargs):
        return requests.get(f"{self._base_uri(hostname)}/topics",
                            headers=headers,
                            **kwargs)

    def _produce_topic(self,
                       topic,
                       data,
                       headers=HTTP_PRODUCE_BINARY_V2_TOPIC_HEADERS,
                       **kwargs):
        return requests.post(f"{self._base_uri()}/topics/{topic}",
                             data,
                             headers=headers,
                             **kwargs)

    def _fetch_topic(self,
                     topic,
                     partition=0,
                     offset=0,
                     max_bytes=1024,
                     timeout_ms=1000,
                     headers=HTTP_FETCH_TOPIC_HEADERS,
                     **kwargs):
        return requests.get(
            f"{self._base_uri()}/topics/{topic}/partitions/{partition}/records?offset={offset}&max_bytes={max_bytes}&timeout={timeout_ms}",
            headers=headers,
            **kwargs)

    def _create_consumer(self,
                         group_id,
                         headers=HTTP_CREATE_CONSUMER_HEADERS,
                         **kwargs):
        res = requests.post(f"{self._base_uri()}/consumers/{group_id}",
                            '''
            {
                "format": "binary",
                "auto.offset.reset": "earliest",
                "auto.commit.enable": "false",
                "fetch.min.bytes": "1",
                "consumer.request.timeout.ms": "10000"
            }''',
                            headers=headers,
                            **kwargs)
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


class PandaProxyTestMethods(PandaProxyEndpoints):
    """
    Base class for testing pandaproxy against a redpanda cluster.

    Inherit from this to run the tests.
    """
    def __init__(self, context, **kwargs):
        super(PandaProxyTestMethods, self).__init__(context,
                                                    pandaproxy=ProxyConfig(),
                                                    **kwargs)

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

        c0 = Consumer(cc_res.json(), self.logger)

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

        c0 = Consumer(cc_res.json(), self.logger)

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
        c0 = Consumer(cc_res.json(), self.logger)

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
        # 3 topics * 3 msg
        c0.fetch_n(3 * 3)

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
        c0 = Consumer(cc_res.json(), self.logger)

        # Subscribe a consumer
        self.logger.info(f"Subscribe consumer to topics: {topics}")
        sc_res = c0.subscribe(topics)
        assert sc_res.status_code == requests.codes.no_content

        # Fetch from a consumer
        self.logger.info(f"Consumer fetch")
        # 3 topics * 3 msg
        c0.fetch_n(3 * 3)

        # Remove consumer
        self.logger.info("Remove consumer")
        rc_res = c0.remove()
        assert rc_res.status_code == requests.codes.no_content


class PandaProxySASLTest(PandaProxyEndpoints):
    """
    Test pandaproxy can connect using SASL.
    """
    def __init__(self, context):
        security = SecurityConfig()
        security.enable_sasl = True

        super(PandaProxySASLTest, self).__init__(context,
                                                 security=security,
                                                 pandaproxy=ProxyConfig())

    def _get_super_client(self):
        user, password, _ = self.redpanda.SUPERUSER_CREDENTIALS
        return KafkaCliTools(self.redpanda, user=user, passwd=password)

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


class PandaProxyTest(PandaProxyTestMethods):
    """
    Test pandaproxy against a redpanda cluster without auth.

    This derived class inherits all the tests from PandaProxyTestMethods.
    """
    def __init__(self, context):
        super(PandaProxyTest, self).__init__(context)


class PandaProxyBasicAuthTest(PandaProxyEndpoints):
    username = 'red'
    password = 'panda'

    def __init__(self, context):

        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = 'sasl'

        proxy_conf = ProxyConfig()
        proxy_conf.authn_method = 'http_basic'

        super(PandaProxyBasicAuthTest, self).__init__(context,
                                                      security=security,
                                                      pandaproxy=proxy_conf)

    @cluster(num_nodes=3)
    def test_get_brokers(self):
        # Regular user without authz priviledges
        # should fail
        res = self._get_brokers(auth=(self.username, self.password)).json()
        assert res['error_code'] == 40101

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS
        brokers_raw = self._get_brokers(auth=(super_username, super_password))
        brokers = brokers_raw.json()['brokers']

        nodes = enumerate(self.redpanda.nodes, 1)
        node_idxs = [node[0] for node in nodes]

        assert sorted(brokers) == sorted(node_idxs)

    @cluster(num_nodes=3)
    def test_list_topics(self):
        # Regular user without authz priviledges
        # should fail
        result = self._get_topics(auth=(self.username, self.password)).json()
        assert result['error_code'] == 40101

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        # First check that no topics exist
        result_raw = self._get_topics(auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert len(result_raw.json()) == 0

        self.topics = [TopicSpec()]
        self._create_initial_topics()

        # Check that one topic exists
        result_raw = self._get_topics(auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()[0] == self.topic

    @cluster(num_nodes=3)
    def test_produce_topic(self):
        self.topics = [TopicSpec(partition_count=3)]
        self._create_initial_topics()

        data = '''
        {
            "records": [
                {"value": "dmVjdG9yaXplZA==", "partition": 0},
                {"value": "cGFuZGFwcm94eQ==", "partition": 1},
                {"value": "bXVsdGlicm9rZXI=", "partition": 2}
            ]
        }'''

        # Regular user without authz priviledges
        # should fail
        result = self._produce_topic(self.topic,
                                     data,
                                     auth=(self.username,
                                           self.password)).json()
        assert result['error_code'] == 40101

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        dne_topic = TopicSpec()
        self.logger.info(f"Producing to non-existant topic: {dne_topic.name}")
        result_raw = self._produce_topic(dne_topic.name,
                                         data,
                                         auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        produce_result = result_raw.json()
        for o in produce_result["offsets"]:
            assert o["error_code"] == 3
            assert o["offset"] == -1

        self.logger.info(f'Producing to topic: {self.topic}')
        result_raw = self._produce_topic(self.topic,
                                         data,
                                         auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.headers[
            'Content-Type'] == 'application/vnd.kafka.v2+json'

        produce_result = result_raw.json()
        for o in produce_result["offsets"]:
            assert o["offset"] == 0, f'error_code {o["error_code"]}'

    @cluster(num_nodes=3)
    def test_fetch_topic(self):
        """
        Create a topic, publish to it, and verify that pandaproxy can fetch
        from it.
        """
        data = '''
        {
            "records": [
                {"value": "dmVjdG9yaXplZA==", "partition": 0},
                {"value": "cGFuZGFwcm94eQ==", "partition": 1},
                {"value": "bXVsdGlicm9rZXI=", "partition": 2}
            ]
        }'''
        self.topics = [TopicSpec(partition_count=3)]
        self._create_initial_topics()

        # Regular user without authz priviledges
        # should fail
        result = self._fetch_topic(self.topic,
                                   data,
                                   auth=(self.username, self.password)).json()
        assert result['error_code'] == 40101

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.info(f"Producing to topic: {self.topic}")
        produce_result_raw = self._produce_topic(self.topic,
                                                 data,
                                                 auth=(super_username,
                                                       super_password))
        assert produce_result_raw.status_code == requests.codes.ok
        produce_result = produce_result_raw.json()
        for o in produce_result["offsets"]:
            assert o["offset"] == 0, f'error_code {o["error_code"]}'

        self.logger.info(f"Consuming from topic: {self.topic}")
        fetch_raw_result_0 = self._fetch_topic(self.topic,
                                               0,
                                               auth=(super_username,
                                                     super_password))
        assert fetch_raw_result_0.status_code == requests.codes.ok
        fetch_result_0 = fetch_raw_result_0.json()
        expected = json.loads(data)
        assert len(fetch_result_0) == 1
        assert fetch_result_0[0]["topic"] == self.topic
        assert fetch_result_0[0]["key"] is None
        assert fetch_result_0[0]["value"] == expected["records"][0]["value"]
        assert fetch_result_0[0]["partition"] == expected["records"][0][
            "partition"]
        assert fetch_result_0[0]["offset"] == 0

    def _offset_data(self, offset_value: Optional[int] = None):
        offset_data = {
            'partitions': [{
                'topic': self.topic,
                'partition': 0
            }, {
                'topic': self.topic,
                'partition': 1
            }, {
                'topic': self.topic,
                'partition': 2
            }]
        }

        if offset_value is not None and type(offset_value) == int:
            for p in offset_data['partitions']:
                p['offset'] = offset_value

        return offset_data

    def _check_offsets(self, offsets: List[Dict[str, Union[str, int, List]]],
                       offset_value: int):
        for o in offsets:
            assert o['topic'] == self.topic
            assert o['partition'] in [0, 1, 2]
            assert o['offset'] == offset_value
            assert o['metadata'] == ''

    @cluster(num_nodes=3)
    def test_consumer(self):
        """
        Create a consumer group, subscribe to topics, fetch records,
        set and get offsets, and remove the consumer
        """

        group_id = f"pandaproxy-group-{uuid.uuid4()}"

        self.topics = [
            TopicSpec(partition_count=3, replication_factor=3),
        ]
        self._create_initial_topics()

        super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        self.logger.info(f"Producing to topic: {self.topic}")
        produce_result_raw = self._produce_topic(
            self.topic,
            '''
        {
            "records": [
                {"value": "Redpanda", "partition": 0},
                {"value": "Pandaproxy", "partition": 1},
                {"value": "Demo", "partition": 2}
            ]
        }''',
            headers={"Content-Type": "application/vnd.kafka.json.v2+json"},
            auth=(super_username, super_password))
        assert produce_result_raw.status_code == requests.codes.ok

        # Create a consumer
        self.logger.info("Create a consumer")
        cc_res = self._create_consumer(group_id,
                                       auth=(self.username,
                                             self.password)).json()
        assert cc_res['error_code'] == 40101

        cc_res = self._create_consumer(group_id,
                                       auth=(super_username, super_password))
        assert cc_res.status_code == requests.codes.ok
        c0 = Consumer(cc_res.json(), self.logger)

        # Subscribe a consumer
        self.logger.info(f"Subscribe consumer to topics: {self.topic}")
        sc_res = c0.subscribe([self.topic],
                              auth=(self.username, self.password)).json()
        assert sc_res['error_code'] == 40101

        sc_res = c0.subscribe([self.topic],
                              auth=(super_username, super_password))
        assert sc_res.status_code == requests.codes.no_content

        # Fetch from a consumer
        self.logger.info(f"Consumer fetch")
        cf_res = c0.fetch(auth=(self.username, self.password)).json()
        assert cf_res['error_code'] == 40101

        cf_res = c0.fetch(auth=(super_username, super_password))
        assert cf_res.status_code == requests.codes.ok
        fetch_result = cf_res.json()
        # 1 topic * 3 msg
        assert len(fetch_result) == 1 * 3
        for r in fetch_result:
            assert r["value"]

        self.logger.info("Get consumer offsets")
        get_offset_data = self._offset_data()
        cof_res = c0.get_offsets(json.dumps(get_offset_data),
                                 auth=(self.username, self.password)).json()
        assert cof_res['error_code'] == 40101

        cof_res = c0.get_offsets(json.dumps(get_offset_data),
                                 auth=(super_username, super_password))
        assert cof_res.status_code == requests.codes.ok
        offsets = cof_res.json()['offsets']
        self._check_offsets(offsets, offset_value=-1)

        self.logger.info("Set consumer offsets")
        set_offset_data = self._offset_data(offset_value=0)
        cos_res = c0.set_offsets(json.dumps(set_offset_data),
                                 auth=(self.username, self.password)).json()
        assert cos_res['error_code'] == 40101

        cos_res = c0.set_offsets(json.dumps(set_offset_data),
                                 auth=(super_username, super_password))
        assert cos_res.status_code == requests.codes.no_content

        # Redo fetch offsets. The offset values should now be 0 instead of -1
        cof_res = c0.get_offsets(json.dumps(get_offset_data),
                                 auth=(super_username, super_password))
        assert cof_res.status_code == requests.codes.ok
        offsets = cof_res.json()['offsets']
        self._check_offsets(offsets, offset_value=0)

        # Remove consumer
        self.logger.info("Remove consumer")
        rc_res = c0.remove(auth=(self.username, self.password)).json()
        assert rc_res['error_code'] == 40101

        rc_res = c0.remove(auth=(super_username, super_password))
        assert rc_res.status_code == requests.codes.no_content

    @cluster(num_nodes=3)
    def test_password_change(self):
        """
        Issue some rest requests as the superuser,
        change the superuser password and then issue more requests.
        """

        self.topics = [
            TopicSpec(partition_count=3, replication_factor=3),
        ]
        self._create_initial_topics()

        super_username, super_password, super_algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        result_raw = self._get_topics(auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()[0] == self.topic

        # Change admin password
        admin = Admin(self.redpanda)
        admin.update_user(super_username, 'new-secret', super_algorithm)

        # Old password should fail
        result_raw = self._get_topics(auth=(super_username, super_password))
        assert result_raw.json()['error_code'] == 40101

        # New password should succeed.
        result_raw = self._get_topics(auth=(super_username, 'new-secret'))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()[0] == self.topic

        # Put the original password back incase future changes to the
        # teardown process in RedpandaService relies on the superuser
        admin.update_user(super_username, super_password, super_algorithm)


class PandaProxyAutoAuthTest(PandaProxyTestMethods):
    """
    Test pandaproxy against a redpanda cluster with Auto Auth enabled.

    This derived class inherits all the tests from PandaProxyTestMethods.
    """
    def __init__(self, context):
        security = SecurityConfig()
        security.kafka_enable_authorization = True
        security.endpoint_authn_method = 'sasl'
        security.auto_auth = True

        super(PandaProxyAutoAuthTest, self).__init__(context,
                                                     security=security)

    @cluster(num_nodes=3)
    def test_restarts(self):
        nodes = self.redpanda.nodes
        node_count = len(nodes)
        restart_node_idx = 0

        def check_connection(hostname: str):
            result_raw = self._get_topics(hostname=hostname)
            self.logger.info(result_raw.status_code)
            self.logger.info(result_raw.json())
            assert result_raw.status_code == requests.codes.ok
            assert result_raw.json() == []

        def restart_node():
            victim = nodes[restart_node_idx]
            self.logger.info(f"Restarting node: {restart_node_idx}")
            self.redpanda.restart_nodes(victim)

        for _ in range(5):
            for n in self.redpanda.nodes:
                check_connection(n.account.hostname)
            restart_node()
            restart_node_idx = (restart_node_idx + 1) % node_count


class PandaProxyClientStopTest(PandaProxyEndpoints):
    username = 'red'
    password = 'panda'
    algorithm = 'SCRAM-SHA-256'

    topics = [TopicSpec()]

    def __init__(self, context):

        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = 'sasl'

        proxy_conf = ProxyConfig()
        proxy_conf.authn_method = 'http_basic'
        proxy_conf.cache_keep_alive = 60000 * 5  # Time in ms
        proxy_conf.cache_max_size = 1

        super(PandaProxyClientStopTest, self).__init__(context,
                                                       security=security,
                                                       pandaproxy=proxy_conf)

    @cluster(num_nodes=3)
    def test_client_stop(self):
        super_username, super_password, super_algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        rpk = RpkTool(self.redpanda)

        o = rpk.sasl_create_user(self.username, self.password, self.algorithm)
        self.logger.debug(f'Sasl create user {o}')

        # Only the super user can add ACLs
        o = rpk.sasl_allow_principal(f'User:{self.username}', ['all'], 'topic',
                                     self.topic, super_username,
                                     super_password, super_algorithm)
        self.logger.debug(f'Allow all topic perms {o}')

        # Issue some request so that the client cache holds a single
        # client for the super user
        result_raw = self._get_topics(auth=(super_username, super_password))
        assert result_raw.status_code == requests.codes.ok
        assert result_raw.json()[0] == self.topic

        data = '''
        {
            "records": [
                {"value": "dmVjdG9yaXplZA==", "partition": 0},
                {"value": "cGFuZGFwcm94eQ==", "partition": 1},
                {"value": "bXVsdGlicm9rZXI=", "partition": 2}
            ]
        }'''

        import time

        def _produce_req(username, userpass, timeout_sec=30):
            start = time.time()
            stop = start + timeout_sec
            while time.time() < stop:
                self.logger.info(
                    f"Producing to topic: {self.topic}, User: {username}")
                produce_result_raw = self._produce_topic(self.topic,
                                                         data,
                                                         auth=(username,
                                                               userpass))
                self.logger.debug(
                    f"Producing to topic: {self.topic}, User: {username}, Result: {produce_result_raw.status_code}"
                )

                if produce_result_raw.status_code != requests.codes.ok:
                    return produce_result_raw.status_code

            return requests.codes.ok

        executor = ThreadPoolExecutor(max_workers=2)

        super_fut = executor.submit(_produce_req,
                                    username=super_username,
                                    userpass=super_password)
        regular_fut = executor.submit(_produce_req,
                                      username=self.username,
                                      userpass=self.password)

        if super_fut.result() != requests.codes.ok:
            raise RuntimeError('Produce failed with super user')

        if regular_fut.result() != requests.codes.ok:
            raise RuntimeError('Produce failed with regular user')


class User:
    def __init__(self, idx: int):
        self.username = f'user_{idx}'
        self.password = f'secret_{self.username}'
        self.algorithm = 'SCRAM-SHA-256'

    def __str__(self):
        return self.username


class GetTopics(threading.Thread):
    def __init__(self, user: User, handle):
        threading.Thread.__init__(self)
        self.user = user
        self._get_topics = handle
        self.result_raw = None

    def run(self):
        self.result_raw = self._get_topics(auth=(self.user.username,
                                                 self.user.password))


class BasicAuthScaleTest(PandaProxyEndpoints):
    topics = [
        TopicSpec(),
    ]

    def __init__(self, context):

        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = 'sasl'

        proxy_conf = ProxyConfig()
        proxy_conf.authn_method = 'http_basic'
        proxy_conf.cache_keep_alive = 60000 * 5  # Time in ms
        proxy_conf.cache_max_size = 10
        super(BasicAuthScaleTest,
              self).__init__(context,
                             security=security,
                             resource_settings=ResourceSettings(num_cpus=4),
                             pandaproxy=proxy_conf)

        self.users_list = []

    @cluster(num_nodes=3)
    @matrix(num_users=[500])
    def test_many_users(self, num_users: int):
        super_username, super_password, super_algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        rpk = RpkTool(self.redpanda)

        # First create all users and their acls
        for idx in range(num_users):
            user = User(idx)
            o = rpk.sasl_create_user(user.username, user.password,
                                     user.algorithm)
            self.logger.debug(f'Sasl create user {o}')

            # Only the super user can add ACLs
            o = rpk.sasl_allow_principal(f'User:{user.username}', ['all'],
                                         'topic', self.topic, super_username,
                                         super_password, super_algorithm)
            self.logger.debug(f'Allow all topic perms {o}')

            self.users_list.append(user)

        tasks = []

        for idx in range(num_users):
            user = self.users_list[idx]
            task = GetTopics(user, self._get_topics)
            task.start()
            tasks.append(task)

        retry_count = 0
        for task in tasks:
            task.join()

            self.logger.debug(
                f'User: {task.user}, Raw Result: {task.result_raw}')
            assert task.result_raw is not None
            res = task.result_raw.json()
            self.logger.debug(f'Content: {res}')

            if task.result_raw.status_code != requests.codes.ok:
                # Retry gate closed exceptions that bubble up to the user.
                if res['error_code'] == 50003 and res[
                        'message'] == 'gate closed':
                    self.logger.debug(f'Gate closed exception, retrying ')
                    retry_count += 1
                    print(f'Retry count {retry_count}')
                    result_raw = self._get_topics(auth=(task.user.username,
                                                        task.user.password))
                    assert result_raw.status_code == requests.codes.ok
                    res = result_raw.json()
                else:
                    raise RuntimeError(
                        f'Get topics failed, user: {task.user} -- {res}')

            assert res[
                0] == self.topic, f'Incorrect topic, user: {task.user} -- {res}'
