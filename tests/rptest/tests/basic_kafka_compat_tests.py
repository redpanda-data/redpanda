# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from collections.abc import Callable
from typing import Protocol, TypeVar

import kafkatest.version
from rptest.clients.default import DefaultClient
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
import confluent_kafka as ck
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest

from kafkatest.services.kafka import KafkaService

from kafkatest.services.zookeeper import ZookeeperService
import kafkatest
from ducktape.mark import parametrize

KAFKA_VERSION = kafkatest.version.KafkaVersion("3.7.0")
from rptest.services.redpanda_types import RedpandaServiceForClients

T = TypeVar('T')


class CompatTestCaseCallable(Protocol[T]):
    def __call__(self, broker_service: RedpandaServiceForClients) -> T:
        ...


class KafkaCompatTest(EndToEndTest):
    """Base for test validating Redpanda compatibility with Kafka"""
    def __init__(self, test_context):
        super(KafkaCompatTest, self).__init__(test_context)

        self.redpanda: RedpandaService = RedpandaService(self.test_context,
                                                         num_brokers=3)

        self.kafka = KafkaServiceAdapter(
            self.test_context,
            KafkaService(self.test_context,
                         num_nodes=3,
                         zk=None,
                         version=KAFKA_VERSION))

    def setUp(self):
        pass

    def tearDown(self):
        self.kafka.stop()

    def start_brokers(self):
        self.kafka.start()
        if self.redpanda:
            self.redpanda.start()

    def _compat_test_case(self, test_callable: CompatTestCaseCallable[T]):
        self.start_brokers()
        self.logger.info("executing test callable against Kafka broker")
        kafka_result = test_callable(self.kafka)

        self.logger.info("executing test callable against Redpanda broker")
        rp_result = test_callable(self.redpanda)

        self.logger.info(
            f"Redpanda result: {rp_result}, kafka result: {kafka_result}")
        assert rp_result == kafka_result, "Kafka and Redpanda results differs"


class TxKafkaCompatTest(KafkaCompatTest):
    @cluster(num_nodes=6)
    @parametrize(metadata_quorum='COLOCATED_KRAFT')
    def test_concurrent_producer_ids(self, metadata_quorum):
        def test_case(broker_service: RedpandaServiceForClients) -> list[str]:

            partition_count = 1
            topic = TopicSpec(name="test-topic",
                              replication_factor=3,
                              partition_count=partition_count)

            DefaultClient(broker_service).create_topic(topic)

            producer_1 = ck.Producer(
                {
                    'bootstrap.servers': broker_service.brokers(),
                    'transactional.id': "tx-id-1",
                    'client.id': "prod-1"
                },
                logger=self.logger,
                debug='all')

            self.logger.info("Initializing transactions for producer-1")
            producer_1.init_transactions()
            producer_1.begin_transaction()
            for i in range(5):
                self.logger.info(f"Producing message {i} from producer-1")
                producer_1.produce(topic.name,
                                   key="k",
                                   value=f"producer-1-value-{i}")

            self.logger.info("Producer-1 committing transaction...")
            producer_1.commit_transaction()

            self.logger.info("Initializing transactions for producer-2")
            producer_2 = ck.Producer(
                {
                    'bootstrap.servers': broker_service.brokers(),
                    'transactional.id': "tx-id-1",
                    'client.id': "prod-2"
                },
                logger=self.logger,
                debug='all')
            producer_2.init_transactions()

            producer_2.begin_transaction()
            for i in range(5):
                producer_2.produce(topic.name,
                                   key="k",
                                   value=f"producer-2-value-{i}")

            producer_2.commit_transaction()

            consumer = ck.Consumer(
                {
                    "bootstrap.servers": broker_service.brokers(),
                    "group.id": "group-1",
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': True,
                    'isolation.level': 'read_committed',
                },
                logger=self.logger,
            )
            consumer.subscribe([topic.name])

            msgs = []
            timeout = time.time() + 30
            while len(msgs) < 10:
                msg = consumer.poll(timeout=1)
                if time.time() > timeout:
                    break

                if msg:
                    self.logger.info(
                        f"consumed: {msg.value()} at offset: {msg.offset()}")
                    msgs.append(msg.value().decode('utf-8'))
            consumer.close()
            return msgs

        self._compat_test_case(test_case)
