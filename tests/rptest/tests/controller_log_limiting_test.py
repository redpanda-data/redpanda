# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from ducktape.mark import parametrize

from rptest.clients.default import DefaultClient
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.rpk_producer import RpkProducer
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.mirror_maker2 import MirrorMaker2

from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService

from kafkatest.version import V_3_0_0



class CreatePartitionsTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(test_context=test_context,
                         extra_rp_conf={
                            "enable_contoller_log_rate_limiting": True,
                            "rps_limit_topic_operations": 10
                         })

    def _partition_count(self, topic):
        meta = self.client().describe_topic(topic)
        return len(meta.partitions)

    def _create_add_verify(self, topic, new_parts):
        self.logger.info(
            f"Testing topic {topic.name} with partitions {topic.partition_count} replicas {topic.replication_factor} expected partitions {new_parts}"
        )

        self.client().create_topic(topic)

        wait_until(
            lambda: self._partition_count(topic.name) == topic.partition_count,
            timeout_sec=20,
            backoff_sec=2,
            err_msg=
            f"Initial topic doesn't have expected {topic.partition_count} partitions, found {self._partition_count(topic.name)}"
        )

    @cluster(num_nodes=3)
    def test_create_partitions(self):

        partition_count = 10
        topic = TopicSpec(partition_count=partition_count,
                            replication_factor=random.choice((1, 3)))
        self._create_add_verify(topic)

        self.client().alter_topic_partition_count(topic.name, 15)

        expected_parts = 15
        wait_until(
            lambda: self._partition_count(topic.name) == expected_parts,
            timeout_sec=20,
            backoff_sec=2,
            err_msg=
            f"Error waiting for partitions to be created, expected {expected_parts} partitions, found {self._partition_count(topic.name)}"
        )

        self.client().delete_topic(topic.name)


# class TestControllerLogRateLimiterMirrorMaker(EndToEndTest):
#     kafka_source = "kafka"
#     redpanda_source = "redpanda"

#     def __init__(self, test_context):
#         super(TestMirrorMakerService, self).__init__(test_context)

#         self.topic = TopicSpec(replication_factor=3)
#         # create single zookeeper node for Kafka
#         self.zk = ZookeeperService(self.test_context,
#                                    num_nodes=1,
#                                    version=V_3_0_0)
#         self.source_broker = None

#     def setUp(self):
#         self.zk.start()

#     def tearDown(self):
#         # ducktape handle service teardown automatically, but it is hard
#         # to tell what went wrong if one of the services hangs.  Do it
#         # explicitly here with some logging, to enable debugging issues
#         # like https://github.com/redpanda-data/redpanda/issues/4270

#         if self.source_broker is not None:
#             self.logger.info(
#                 f"Stopping source broker ({self.source_broker.__class__.__name__})..."
#             )
#             self.source_broker.stop()
#             self.logger.info(
#                 f"Awaiting source broker ({self.source_broker.__class__.__name__})..."
#             )

#         self.logger.info("Stopping zookeeper...")
#         self.zk.stop()
#         self.logger.info("Awaiting zookeeper...")

#     def start_brokers(self, source_type=kafka_source):
#         if source_type == TestMirrorMakerService.redpanda_source:
#             self.source_broker = RedpandaService(self.test_context,
#                                                  num_brokers=3)
#         else:
#             self.source_broker = KafkaServiceAdapter(
#                 self.test_context,
#                 KafkaService(self.test_context,
#                              num_nodes=3,
#                              zk=self.zk,
#                              version=V_3_0_0))

#         self.redpanda = RedpandaService(self.test_context, num_brokers=3)
#         self.source_broker.start()
#         self.redpanda.start()

#         self.source_client = DefaultClient(self.source_broker)

#         self.topic.partition_count = 1000 if self.redpanda.dedicated_nodes else 10
#         self.source_client.create_topic(self.topic)

#     def start_workload(self):

#         self.consumer = VerifiableConsumer(
#             self.test_context,
#             num_nodes=1,
#             redpanda=self.redpanda,
#             topic=self.topic.name,
#             group_id='consumer_test_group',
#             on_record_consumed=self.on_record_consumed)
#         self.consumer.start()

#         self.producer = VerifiableProducer(
#             self.test_context,
#             num_nodes=1,
#             redpanda=self.source_broker,
#             topic=self.topic.name,
#             throughput=1000,
#             message_validator=is_int_with_prefix)
#         self.producer.start()

#     def wait_for_n_messages(self, n_messages=100):
#         """Wait for a minimum number of messages to be successfully produced."""
#         wait_until(
#             lambda: self.producer.num_acked > n_messages,
#             timeout_sec=10,
#             err_msg=
#             "Producer failed to produce %d messages in a reasonable amount of time."
#             % n_messages)

#     @cluster(num_nodes=10)
#     @parametrize(source_type=kafka_source)
#     @parametrize(source_type=redpanda_source)
#     def test_simple_end_to_end(self, source_type):
#         # start brokers
#         self.start_brokers(source_type=source_type)
#         # start mirror maker
#         self.mirror_maker = MirrorMaker2(self.test_context,
#                                          num_nodes=1,
#                                          source_cluster=self.source_broker,
#                                          target_cluster=self.redpanda)
#         topics = []
#         for i in range(0, 10):
#             topics.append(
#                 TopicSpec(partition_count=random.randint(1, 10),
#                           retention_bytes=random.randint(100000000, 300000000),
#                           retention_ms=random.randint(1 * 3600000,
#                                                       10 * 3600000)))
#         self.source_client.create_topic(topics)
#         self.mirror_maker.start()
#         # start source producer & target consumer
#         self.start_workload()

#         self.run_validation(consumer_timeout_sec=120)
#         self.mirror_maker.stop()
#         target_client = DefaultClient(self.redpanda)
#         for t in topics:
#             desc = target_client.describe_topic(t.name)
#             self.logger.debug(f'source topic: {t}, target topic: {desc}')
#             assert len(desc.partitions) == t.partition_count

