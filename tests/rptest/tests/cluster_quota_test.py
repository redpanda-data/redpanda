# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import time
import random
import string
from kafka import KafkaProducer
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import ResourceSettings
from rptest.clients.kcl import RawKCL, KclCreateTopicsRequestTopic, \
    KclCreatePartitionsRequestTopic
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster


class ClusterQuotaPartitionMutationTest(RedpandaTest):
    """
    Ducktape tests for partition mutation quota
    """
    def __init__(self, *args, **kwargs):
        additional_options = {'kafka_admin_topic_api_rate': 1}
        super().__init__(*args,
                         num_brokers=3,
                         extra_rp_conf=additional_options,
                         **kwargs)
        # Use kcl so the throttle_time_ms value in the response can be examined
        self.kcl = RawKCL(self.redpanda)

    @cluster(num_nodes=3)
    def test_partition_throttle_mechanism(self):
        """
        Ensure the partition throttling mechanism (KIP-599) works
        """

        # The kafka_admin_topic_api_rate is 1, quota will be 10. This test will
        # make 1 request within containing three topics to create, each containing
        # different number of partitions.
        #
        # The first topic should succeed, the second will exceed the quota but
        # succeed since the throttling algorithms burst settings will allow it
        # to. The third however should fail since the quota has already exceeded.
        exceed_quota_req = [
            KclCreateTopicsRequestTopic('baz', 1, 1),
            KclCreateTopicsRequestTopic('foo', 10, 1),
            KclCreateTopicsRequestTopic('bar', 2, 1)
        ]

        # Use KCL so that the details about the response can be examined, namely
        # this test must observe that the newly introduce 'throttle_quota_exceeded'
        # response code is used and that 'ThrottleMillis' was approprately set
        response = self.kcl.raw_create_topics(6, exceed_quota_req)
        response = json.loads(response)
        assert response['Version'] == 6
        foo_response = [t for t in response['Topics'] if t['Topic'] == 'foo']
        bar_response = [t for t in response['Topics'] if t['Topic'] == 'bar']
        baz_response = [t for t in response['Topics'] if t['Topic'] == 'baz']
        assert foo_response[0]['ErrorCode'] == 0  # success
        assert baz_response[0]['ErrorCode'] == 0  # success
        assert bar_response[0]['ErrorCode'] == 89  # throttling_quota_exceeded

        # Respect throttle millis response - exhaust the timeout so quota resets
        throttle_ms = response['ThrottleMillis']
        self.redpanda.logger.info(f"First throttle_ms: {throttle_ms}")
        assert throttle_ms > 0
        time.sleep((throttle_ms * 0.001) + 1)

        # Test that throttling works via the CreatePartitions API
        # The first request should exceed the quota, preventing the second from
        # occurring
        exceed_quota_req = [
            KclCreatePartitionsRequestTopic('foo', 21, 1),
            KclCreatePartitionsRequestTopic('baz', 2, 1)
        ]
        response = self.kcl.raw_create_partitions(3, exceed_quota_req)
        response = json.loads(response)
        foo_response = [t for t in response['Topics'] if t['Topic'] == 'foo']
        baz_response = [t for t in response['Topics'] if t['Topic'] == 'baz']
        assert foo_response[0]['ErrorCode'] == 0  # success
        assert baz_response[0]['ErrorCode'] == 89  # throttling_quota_exceeded

        # Respect throttle millis response
        throttle_ms = response['ThrottleMillis']
        self.redpanda.logger.info(f"Second throttle_ms: {throttle_ms}")
        assert throttle_ms > 0
        time.sleep((throttle_ms * 0.001) + 1)

        # Test that throttling works via the DeleteTopics API, 'foo' should at
        # this point contain 21 partitions, deleting this will exceed the quota,
        # any subsequent requests should fail
        exceed_quota_req = ['foo', 'baz']
        response = self.kcl.raw_delete_topics(5, exceed_quota_req)
        response = json.loads(response)
        foo_response = [t for t in response['Topics'] if t['Topic'] == 'foo']
        baz_response = [t for t in response['Topics'] if t['Topic'] == 'baz']
        assert foo_response[0]['ErrorCode'] == 0  # success
        assert baz_response[0]['ErrorCode'] == 89  # throttling_quota_exceeded
        assert response['ThrottleMillis'] > 0


class ClusterRateQuotaTest(RedpandaTest):
    """
    Ducktape tests for rate quota
    """
    topics = (TopicSpec(), )

    def __init__(self, *args, **kwargs):
        self.max_throttle_time = 10
        super().__init__(*args,
                         num_brokers=3,
                         extra_rp_conf={
                             "max_kafka_throttle_delay_ms":
                             self.max_throttle_time
                         },
                         resource_settings=ResourceSettings(num_cpus=1),
                         **kwargs)
        self.rpk = RpkTool(self.redpanda)

    def init_test_data(self):
        wait_until(lambda: len(list(self.rpk.describe_topic(self.topic))) != 0,
                   10)
        wait_until(lambda: next(self.rpk.describe_topic(self.topic)).leader,
                   10)
        self.leader_node = self.redpanda.broker_address(
            self.redpanda.get_node(
                next(self.rpk.describe_topic(self.topic)).leader))
        self.message_size = 1024
        self.msg = "".join(
            random.choice(string.ascii_lowercase)
            for _ in range(self.message_size))

    def produce(self, producer, amount):
        response_futures = [
            producer.send(self.topic, self.msg) for _ in range(amount)
        ]
        for f in response_futures:
            f.get(1)

    @cluster(num_nodes=3)
    def test_client_group_rate_throttle_mechanism(self):
        """
        Ensure group rate throttle works
        """
        self.init_test_data()

        self.redpanda.set_cluster_config({
            "kafka_client_group_byte_rate_quota": [
                {
                    "group_name": "group_1",
                    "clients_prefix": "producer_group_alone_producer",
                    "quota": 10240
                },
                {
                    "group_name": "group_2",
                    "clients_prefix": "producer_group_multiple",
                    "quota": 10240
                },
            ]
        })

        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 retries=1,
                                 client_id="producer_group_alone_producer")

        # Produce under the limit
        self.produce(producer, 10)
        assert producer.metrics(
        )["producer-metrics"]["produce-throttle-time-max"] == 0

        # Produce more than limit
        self.produce(producer, 100)
        producer_throttle_time_max = producer.metrics(
        )["producer-metrics"]["produce-throttle-time-max"]
        assert producer_throttle_time_max > 0 and producer_throttle_time_max <= self.max_throttle_time

        producer_1 = KafkaProducer(acks="all",
                                   bootstrap_servers=self.leader_node,
                                   value_serializer=str.encode,
                                   retries=1,
                                   client_id="producer_group_multiple_1")

        producer_2 = KafkaProducer(acks="all",
                                   bootstrap_servers=self.leader_node,
                                   value_serializer=str.encode,
                                   client_id="producer_group_multiple_2")

        # Produce under the limit
        self.produce(producer_1, 10)
        assert producer_1.metrics(
        )["producer-metrics"]["produce-throttle-time-max"] == 0

        # Produce more than the limit
        self.produce(producer_1, 100)

        # Produce under the limit for client, but more than limit for group
        self.produce(producer_2, 10)
        producer_1_throttle_time_max = producer_1.metrics(
        )["producer-metrics"]["produce-throttle-time-max"]
        assert producer_1_throttle_time_max > 0 and producer_1_throttle_time_max <= self.max_throttle_time
        producer_2_throttle_time_max = producer_2.metrics(
        )["producer-metrics"]["produce-throttle-time-max"]
        assert producer_2_throttle_time_max > 0 and producer_2_throttle_time_max <= self.max_throttle_time

        self.redpanda.set_cluster_config({
            "kafka_client_group_byte_rate_quota": {
                "group_name": "change_config_group",
                "clients_prefix": "new_producer_group",
                "quota": 1024
            }
        })

        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 client_id="new_producer_group_producer")

        self.produce(producer, 100)

        producer_throttle_time_max = producer.metrics(
        )["producer-metrics"]["produce-throttle-time-max"]
        assert producer_throttle_time_max > 0 and producer_throttle_time_max <= self.max_throttle_time
