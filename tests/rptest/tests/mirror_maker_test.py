# Copyright 2020 Redpanda Data, Inc.
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


class TestMirrorMakerService(EndToEndTest):
    kafka_source = "kafka"
    redpanda_source = "redpanda"

    def __init__(self, test_context):
        super(TestMirrorMakerService, self).__init__(test_context)

        self.topic = TopicSpec(replication_factor=3)
        # create single zookeeper node for Kafka
        self.zk = ZookeeperService(self.test_context,
                                   num_nodes=1,
                                   version=V_3_0_0)

    def setUp(self):
        self.zk.start()

    def tearDown(self):
        # ducktape handle service teardown automatically, but it is hard
        # to tell what went wrong if one of the services hangs.  Do it
        # explicitly here with some logging, to enable debugging issues
        # like https://github.com/redpanda-data/redpanda/issues/4270

        self.logger.info(
            f"Stopping source broker ({self.source_broker.__class__.__name__})..."
        )
        self.source_broker.stop()
        self.logger.info(
            f"Awaiting source broker ({self.source_broker.__class__.__name__})..."
        )
        self.logger.info(f"tearDown complete")

        self.logger.info("Stopping zookeeper...")
        self.zk.stop()
        self.logger.info("Awaiting zookeeper...")

    def start_brokers(self, source_type=kafka_source):
        if source_type == TestMirrorMakerService.redpanda_source:
            self.source_broker = RedpandaService(self.test_context,
                                                 num_brokers=3)
        else:
            self.source_broker = KafkaServiceAdapter(
                self.test_context,
                KafkaService(self.test_context,
                             num_nodes=3,
                             zk=self.zk,
                             version=V_3_0_0))

        self.redpanda = RedpandaService(self.test_context, num_brokers=3)
        self.source_broker.start()
        self.redpanda.start()

        self.source_client = DefaultClient(self.source_broker)
        self.source_client.create_topic(self.topic)

    def start_workload(self):

        self.consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic.name,
            group_id='consumer_test_group',
            on_record_consumed=self.on_record_consumed)
        self.consumer.start()

        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.source_broker,
            topic=self.topic.name,
            throughput=1000,
            message_validator=is_int_with_prefix)
        self.producer.start()

    def wait_for_n_messages(self, n_messages=100):
        """Wait for a minimum number of messages to be successfully produced."""
        wait_until(
            lambda: self.producer.num_acked > n_messages,
            timeout_sec=10,
            err_msg=
            "Producer failed to produce %d messages in a reasonable amount of time."
            % n_messages)

    @cluster(num_nodes=10)
    @parametrize(source_type=kafka_source)
    @parametrize(source_type=redpanda_source)
    def test_simple_end_to_end(self, source_type):
        # start brokers
        self.start_brokers(source_type=source_type)
        # start mirror maker
        self.mirror_maker = MirrorMaker2(self.test_context,
                                         num_nodes=1,
                                         source_cluster=self.source_broker,
                                         target_cluster=self.redpanda)
        topics = []
        for i in range(0, 10):
            topics.append(
                TopicSpec(partition_count=random.randint(1, 10),
                          retention_bytes=random.randint(100000000, 300000000),
                          retention_ms=random.randint(1 * 3600000,
                                                      10 * 3600000)))
        self.source_client.create_topic(topics)
        self.mirror_maker.start()
        # start source producer & target consumer
        self.start_workload()

        self.run_validation(consumer_timeout_sec=120)
        self.mirror_maker.stop()
        target_client = DefaultClient(self.redpanda)
        for t in topics:
            desc = target_client.describe_topic(t.name)
            self.logger.debug(f'source topic: {t}, target topic: {desc}')
            assert len(desc.partitions) == t.partition_count

    @cluster(num_nodes=9)
    @parametrize(source_type=kafka_source)
    @parametrize(source_type=redpanda_source)
    def test_consumer_group_mirroring(self, source_type):
        # start redpanda
        self.start_brokers(source_type=source_type)
        consumer_group = "test-group-1"
        # start mirror maker
        self.mirror_maker = MirrorMaker2(self.test_context,
                                         num_nodes=1,
                                         source_cluster=self.source_broker,
                                         target_cluster=self.redpanda,
                                         consumer_group_pattern=consumer_group,
                                         log_level="TRACE")
        self.mirror_maker.start()

        msg_cnt = 100
        # produce some messages to source redpanda
        producer = RpkProducer(self.test_context,
                               self.source_broker,
                               self.topic.name,
                               512,
                               msg_cnt,
                               acks=-1)

        producer.start()
        producer.wait()
        producer.free()

        # consume some messages from source redpanda
        consumer = RpkConsumer(self.test_context,
                               self.source_broker,
                               self.topic.name,
                               ignore_errors=False,
                               retries=3,
                               group=consumer_group,
                               num_msgs=20)

        consumer.start()
        consumer.wait()
        consumer.stop()
        source_messages = consumer.messages
        self.logger.info(f"source message count: {len(source_messages)}")
        consumer.free()

        src_rpk = RpkTool(self.source_broker)
        source_group = src_rpk.group_describe(consumer_group)
        target_rpk = RpkTool(self.redpanda)

        def target_group_equal():
            try:
                target_group = target_rpk.group_describe(consumer_group)
            except RpkException as e:
                # e.g. COORDINATOR_NOT_AVAILABLE
                self.logger.info(f"Error describing target cluster group: {e}")
                return False

            self.logger.info(
                f"source {source_group}, target_group: {target_group}")
            return target_group.partitions == source_group.partitions and target_group.name == source_group.name

        # wait for consumer group sync
        wait_until(target_group_equal, 60, backoff_sec=5)

        self.mirror_maker.stop()
