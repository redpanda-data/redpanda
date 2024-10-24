# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import SecurityConfig
from time import sleep

from confluent_kafka import (Producer, KafkaException)

TOPIC_AUTHORIZATION_FAILED = 29
CLUSTER_AUTHORIZATION_FAILED = 31
TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)


class ScramfulEosTest(RedpandaTest):
    topics = [TopicSpec(name="topic1")]

    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        security = SecurityConfig()
        security.enable_sasl = True

        super(ScramfulEosTest, self).__init__(test_context=test_context,
                                              security=security,
                                              extra_rp_conf=extra_rp_conf)

    def retry(self, func, retries, accepted_ec):
        while True:
            retries -= 1
            try:
                func()
                break
            except KafkaException as e:
                if retries == 0:
                    raise e
                assert e.args[0].code(
                ) in accepted_ec, f"only {accepted_ec} error codes are expected"
                sleep(1)

    def write_by_bob(self, algorithm):
        producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5,
            "sasl.mechanism": algorithm,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.username": "bob",
            "sasl.password": "bob"
        })
        producer.produce("topic1",
                         key="key1".encode('utf-8'),
                         value="value1".encode('utf-8'),
                         callback=on_delivery)
        producer.flush()

    @cluster(num_nodes=3)
    def test_idempotent_write_fails(self):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        rpk = RpkTool(self.redpanda)
        rpk.sasl_create_user("bob", "bob", algorithm)

        try:
            self.write_by_bob(algorithm)
            assert False, "bob should not have access to topic1"
        except AssertionError as e:
            raise e
        except KafkaException as e:
            assert e.args[0].code(
            ) == TOPIC_AUTHORIZATION_FAILED, "TOPIC_AUTHORIZATION_FAILED error is expected"

    @cluster(num_nodes=3)
    def test_idempotent_write_passes_1(self):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        rpk = RpkTool(self.redpanda)
        rpk.sasl_create_user("bob", "bob", algorithm)
        rpk.sasl_allow_principal("User:bob", ["write", "read", "describe"],
                                 "topic", "topic1", username, password,
                                 algorithm)
        self.retry(lambda: self.write_by_bob(algorithm), 5,
                   [TOPIC_AUTHORIZATION_FAILED, CLUSTER_AUTHORIZATION_FAILED])

    @cluster(num_nodes=3)
    def test_idempotent_write_passes_2(self):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        rpk = RpkTool(self.redpanda)
        rpk.sasl_create_user("bob", "bob", algorithm)
        rpk.sasl_allow_principal("User:bob", ["write", "read", "describe"],
                                 "topic", "*", username, password, algorithm)
        self.retry(lambda: self.write_by_bob(algorithm), 5,
                   [TOPIC_AUTHORIZATION_FAILED, CLUSTER_AUTHORIZATION_FAILED])

    def init_by_bob(self, tx_id, algorithm):
        producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5,
            "sasl.mechanism": algorithm,
            "transactional.id": tx_id,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.username": "bob",
            "sasl.password": "bob"
        })
        producer.init_transactions()

    @cluster(num_nodes=3)
    def test_tx_init_fails(self):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        rpk = RpkTool(self.redpanda)
        rpk.sasl_create_user("bob", "bob", algorithm)
        rpk.sasl_allow_principal("User:bob", ["write", "read", "describe"],
                                 "topic", "topic1", username, password,
                                 algorithm)

        try:
            self.init_by_bob("tx-id-1", algorithm)
            assert False, "bob should not have access to txns"
        except AssertionError as e:
            raise e
        except KafkaException as e:
            assert e.args[0].code(
            ) == TRANSACTIONAL_ID_AUTHORIZATION_FAILED, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED is expected"

    @cluster(num_nodes=3)
    def test_tx_init_passes_1(self):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        rpk = RpkTool(self.redpanda)
        rpk.sasl_create_user("bob", "bob", algorithm)
        rpk.sasl_allow_principal("User:bob", ["write", "read", "describe"],
                                 "topic", "topic1", username, password,
                                 algorithm)
        rpk.sasl_allow_principal("User:bob", ["write", "describe"],
                                 "transactional-id", "tx-id-1", username,
                                 password, algorithm)

        self.retry(lambda: self.init_by_bob("tx-id-1", algorithm), 5,
                   [TRANSACTIONAL_ID_AUTHORIZATION_FAILED])

    @cluster(num_nodes=3)
    def test_tx_init_passes_2(self):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        rpk = RpkTool(self.redpanda)
        rpk.sasl_create_user("bob", "bob", algorithm)
        rpk.sasl_allow_principal("User:bob", ["write", "read", "describe"],
                                 "topic", "topic1", username, password,
                                 algorithm)
        rpk.sasl_allow_principal("User:bob", ["write", "describe"],
                                 "transactional-id", "*", username, password,
                                 algorithm)

        self.retry(lambda: self.init_by_bob("tx-id-2", algorithm), 5,
                   [TRANSACTIONAL_ID_AUTHORIZATION_FAILED])
