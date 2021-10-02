# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.errors import DucktapeError

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
import sys
import traceback

from confluent_kafka import (Producer, KafkaException, KafkaError)


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)


class KeyValidationTest(RedpandaTest):
    """
    Verify that redpanda rejects null keys on compacted topics
    """
    def __init__(self, test_context):
        super(KeyValidationTest, self).__init__(test_context=test_context)

    @cluster(num_nodes=3)
    def test_acceptance1(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")

        value = "value1".encode("utf-8")

        bootstrap_servers = self.redpanda.brokers()
        producer = Producer({"bootstrap.servers": bootstrap_servers})

        try:
            producer.produce("topic1",
                             key=None,
                             value=value,
                             callback=on_delivery)
            producer.flush()
        except KafkaException as err:
            raise DucktapeError(str(err.args[0]))

    @cluster(num_nodes=3)
    def test_acceptance2(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1", config={"cleanup.policy": "compact"})

        key = "key1".encode("utf-8")
        value = "value1".encode("utf-8")

        bootstrap_servers = self.redpanda.brokers()
        producer = Producer({"bootstrap.servers": bootstrap_servers})

        try:
            producer.produce("topic1",
                             key=key,
                             value=value,
                             callback=on_delivery)
            producer.flush()
        except KafkaException as err:
            raise DucktapeError(str(err.args[0]))

    @cluster(num_nodes=3)
    def test_acceptance3(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1", config={"cleanup.policy": "compact"})

        key = "key1".encode("utf-8")
        # generating a value with low entropy to trigger compression
        # https://github.com/confluentinc/confluent-kafka-python/issues/480
        value = ""
        for i in range(0, 50):
            value += "vvvv"
        value = value.encode("utf-8")

        bootstrap_servers = self.redpanda.brokers()
        producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "compression.type": "lz4"
        })

        try:
            producer.produce("topic1",
                             key=key,
                             value=value,
                             callback=on_delivery)
            producer.flush()
        except KafkaException as err:
            raise DucktapeError(str(err.args[0]))

    @cluster(num_nodes=3)
    def test_rejection1(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1", config={"cleanup.policy": "compact"})

        value = "value1".encode("utf-8")

        bootstrap_servers = self.redpanda.brokers()
        producer = Producer({"bootstrap.servers": bootstrap_servers})

        try:
            producer.produce("topic1",
                             key=None,
                             value=value,
                             callback=on_delivery)
            producer.flush()
            raise DucktapeError(
                "Redpanda should reject null keys on insert to compacted topic"
            )
        except KafkaException as err:
            code = err.args[0].code()
            if err.args[0].code() != KafkaError.INVALID_RECORD:
                msg = f"code {KafkaError.INVALID_RECORD} expected got {err.args[0]}"
                raise DucktapeError(msg)

    @cluster(num_nodes=3)
    def test_rejection2(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1", config={"cleanup.policy": "compact"})

        # generating a value with low entropy to trigger compression
        # https://github.com/confluentinc/confluent-kafka-python/issues/480
        value = ""
        for i in range(0, 50):
            value += "vvvv"
        value = value.encode("utf-8")

        bootstrap_servers = self.redpanda.brokers()
        producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "compression.type": "lz4"
        })

        try:
            producer.produce("topic1",
                             key=None,
                             value=value,
                             callback=on_delivery)
            producer.flush()
            raise DucktapeError(
                "Redpanda should reject null keys on insert to compacted topic"
            )
        except KafkaException as err:
            code = err.args[0].code()
            if err.args[0].code() != KafkaError.INVALID_RECORD:
                msg = f"code {KafkaError.INVALID_RECORD} expected got {err.args[0]}"
                raise DucktapeError(msg)
