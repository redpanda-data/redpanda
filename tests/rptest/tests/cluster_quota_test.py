# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import datetime
import json
import time
import random
import string

from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import ResourceSettings
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
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
        self.target_default_quota_byte_rate = 1048576
        self.target_group_quota_byte_rate = 10240
        self.message_size = 1024
        self.break_default_quota_message_amount = int(
            self.target_default_quota_byte_rate / self.message_size) * 11
        self.break_group_quota_message_amount = int(
            self.target_group_quota_byte_rate / self.message_size) * 11
        # Fetch 10 messages per one request (msg_size + headers)
        self.max_partition_fetch_bytes = self.message_size * 11
        additional_options = {
            "max_kafka_throttle_delay_ms": self.max_throttle_time,
            "target_quota_byte_rate": self.target_default_quota_byte_rate,
            "target_fetch_quota_byte_rate":
            self.target_default_quota_byte_rate,
        }
        super().__init__(*args,
                         num_brokers=3,
                         extra_rp_conf=additional_options,
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
        self.msg = "".join(
            random.choice(string.ascii_lowercase)
            for _ in range(self.message_size))

    def check_producer_throttled(self, producer):
        throttle_ms = producer.metrics(
        )["producer-metrics"]["produce-throttle-time-max"]
        assert throttle_ms > 0 and throttle_ms <= self.max_throttle_time

    def check_producer_not_throttled(self, producer):
        throttle_ms = producer.metrics(
        )["producer-metrics"]["produce-throttle-time-max"]
        assert throttle_ms == 0

    def check_consumer_throttled(self, consumer):
        throttle_ms = consumer.metrics(
        )["consumer-fetch-manager-metrics"]["fetch-throttle-time-max"]
        assert throttle_ms > 0 and throttle_ms <= self.max_throttle_time

    def check_consumer_not_throttled(self, consumer):
        throttle_ms = consumer.metrics(
        )["consumer-fetch-manager-metrics"]["fetch-throttle-time-max"]
        assert throttle_ms == 0

    def produce(self, producer, amount):
        response_futures = [
            producer.send(self.topic, self.msg) for _ in range(amount)
        ]
        for f in response_futures:
            f.get(1)

    def fetch(self, consumer, messages_amount, timeout_sec=300):
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)
        cur_messages_amount = 0
        while cur_messages_amount < messages_amount:
            poll_result = consumer.poll(timeout_ms=1000)
            cur_messages_amount += sum(
                map(lambda tr: len(tr), poll_result.values()))
            now = datetime.datetime.now()
            if now > deadline:
                raise TimeoutError()

    @cluster(num_nodes=3)
    def test_client_group_produce_rate_throttle_mechanism(self):
        """
        Ensure group rate throttle works for producers
        """
        self.init_test_data()

        self.redpanda.set_cluster_config({
            "kafka_client_group_byte_rate_quota": [
                {
                    "group_name": "group_1",
                    "clients_prefix": "producer_group_alone_producer",
                    "quota": self.target_group_quota_byte_rate
                },
                {
                    "group_name": "group_2",
                    "clients_prefix": "producer_group_multiple",
                    "quota": self.target_group_quota_byte_rate
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
        self.check_producer_not_throttled(producer)

        # Produce more than limit
        self.produce(producer, self.break_group_quota_message_amount)
        self.check_producer_throttled(producer)

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
        self.check_producer_not_throttled(producer_1)

        # Produce more than the limit
        self.produce(producer_1, self.break_group_quota_message_amount)
        self.check_producer_throttled(producer_1)

        # Produce under the limit for client, but more than limit for group
        self.produce(producer_2, 10)
        self.check_producer_throttled(producer_2)

        self.redpanda.set_cluster_config({
            "kafka_client_group_byte_rate_quota": {
                "group_name": "change_config_group",
                "clients_prefix": "new_producer_group",
                "quota": self.target_group_quota_byte_rate
            }
        })

        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 client_id="new_producer_group_producer")

        self.produce(producer, self.break_group_quota_message_amount)
        self.check_producer_throttled(producer)

    @cluster(num_nodes=3)
    def test_client_group_consume_rate_throttle_mechanism(self):
        """
        Ensure group rate throttle works for consumers
        """
        self.init_test_data()

        self.redpanda.set_cluster_config({
            "kafka_client_group_fetch_byte_rate_quota": [
                {
                    "group_name": "group_1",
                    "clients_prefix": "consumer_alone",
                    "quota": self.target_group_quota_byte_rate
                },
                {
                    "group_name": "group_2",
                    "clients_prefix": "consumer_multiple",
                    "quota": self.target_group_quota_byte_rate
                },
            ]
        })

        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 retries=1)
        self.produce(producer, self.break_group_quota_message_amount * 2)

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="consumer_alone",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        # Fetch more the limit
        self.fetch(consumer, self.break_group_quota_message_amount)
        self.check_consumer_throttled(consumer)

        consumer_1 = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="consumer_multiple_1",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        consumer_2 = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="consumer_multiple_2",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        # Consume under the limit
        self.fetch(consumer_2, 10)
        self.check_consumer_not_throttled(consumer_2)

        # Consume more than the limit by other consumer
        self.fetch(consumer_1, self.break_group_quota_message_amount)
        self.check_consumer_throttled(consumer_1)

        # Consume under the limit for client, but more than limit for group
        self.fetch(consumer_2, 10)
        self.check_consumer_throttled(consumer_2)

        self.redpanda.set_cluster_config({
            "kafka_client_group_fetch_byte_rate_quota": {
                "group_name": "change_config",
                "clients_prefix": "new_consumer",
                "quota": self.target_group_quota_byte_rate
            }
        })

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="new_consumer",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        self.fetch(consumer, self.break_group_quota_message_amount)
        self.check_consumer_throttled(consumer)

    @cluster(num_nodes=3)
    def test_client_response_throttle_mechanism(self):
        """
        Ensure response size rate throttle works
        """
        self.init_test_data()

        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 retries=2,
                                 request_timeout_ms=60000,
                                 client_id="producer")

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="consumer",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        self.produce(producer, self.break_default_quota_message_amount * 2)

        # Consume under the quota
        self.fetch(consumer, 10)
        self.check_consumer_not_throttled(consumer)

        # Consume more than the quota limit
        self.fetch(consumer, self.break_default_quota_message_amount)
        self.check_consumer_throttled(consumer)

    @cluster(num_nodes=3)
    def test_client_response_throttle_mechanism_applies_to_next_request(self):
        """
        Ensure response size rate throttle applies on next request
        """
        self.init_test_data()

        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 retries=1,
                                 client_id="producer")

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="consumer",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.target_default_quota_byte_rate * 10,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        self.produce(producer, self.break_default_quota_message_amount)

        # Consume more than the quota limit, next request must be throttled
        consumer.poll(timeout_ms=1000,
                      max_records=self.break_default_quota_message_amount)
        self.check_consumer_not_throttled(consumer)

        # Consume must be throttled
        consumer.poll(timeout_ms=1000)
        self.check_consumer_throttled(consumer)

    @cluster(num_nodes=3)
    def test_client_response_and_produce_throttle_mechanism(self):
        self.init_test_data()
        self.redpanda.set_cluster_config({
            "kafka_client_group_byte_rate_quota": {
                "group_name": "producer_throttle_group",
                "clients_prefix": "throttle_producer_only",
                "quota": self.target_group_quota_byte_rate
            },
            "kafka_client_group_fetch_byte_rate_quota": {
                "group_name": "producer_throttle_group",
                "clients_prefix": "throttle_producer_only",
                "quota": self.target_group_quota_byte_rate
            }
        })
        # Producer and Consumer same client_id
        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 retries=1,
                                 client_id="throttle_producer_only")

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="throttle_producer_only",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        # Produce more than limit
        self.produce(producer, self.break_group_quota_message_amount)
        self.check_producer_throttled(producer)

        # Fetch must not be throttled
        self.fetch(consumer, 10)
        self.check_consumer_not_throttled(consumer)

        self.redpanda.set_cluster_config({
            "kafka_client_group_byte_rate_quota": {
                "group_name": "consumer_throttle_group",
                "clients_prefix": "throttle_consumer_only",
                "quota": self.target_group_quota_byte_rate
            },
            "kafka_client_group_fetch_byte_rate_quota": {
                "group_name": "consumer_throttle_group",
                "clients_prefix": "throttle_consumer_only",
                "quota": self.target_group_quota_byte_rate
            }
        })
        # Producer and Consumer same client_id
        producer = KafkaProducer(acks="all",
                                 bootstrap_servers=self.leader_node,
                                 value_serializer=str.encode,
                                 retries=1,
                                 client_id="throttle_consumer_only")

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.leader_node,
            client_id="throttle_consumer_only",
            consumer_timeout_ms=1000,
            max_partition_fetch_bytes=self.max_partition_fetch_bytes,
            auto_offset_reset='earliest',
            enable_auto_commit=False)

        # Fetch more than limit
        self.fetch(consumer, self.break_group_quota_message_amount)
        self.check_consumer_throttled(consumer)

        # Produce must not be throttled
        self.produce(producer, 10)
        self.check_producer_not_throttled(producer)
