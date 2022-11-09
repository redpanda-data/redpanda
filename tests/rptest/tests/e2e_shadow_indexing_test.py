# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import os
import random

from ducktape.mark import ok_to_fail
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.action_injector import random_process_kills
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierRandomConsumer
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST
from rptest.services.redpanda import SISettings
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import Scale, wait_until_segments
from rptest.util import (
    produce_until_segments,
    wait_for_removal_of_n_segments,
)
from rptest.utils.si_utils import S3Snapshot


class EndToEndShadowIndexingBase(EndToEndTest):
    segment_size = 1048576  # 1 Mb
    s3_topic_name = "panda-topic"

    num_brokers = 3

    topics = (TopicSpec(
        name=s3_topic_name,
        partition_count=1,
        replication_factor=3,
    ), )

    def __init__(self, test_context, extra_rp_conf=None, environment=None):
        super(EndToEndShadowIndexingBase,
              self).__init__(test_context=test_context)

        self.test_context = test_context
        self.topic = self.s3_topic_name

        self.si_settings = SISettings(
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=self.segment_size,  # 1MB
        )
        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)
        self.scale = Scale(test_context)

        self.redpanda = RedpandaService(context=self.test_context,
                                        num_brokers=self.num_brokers,
                                        si_settings=self.si_settings,
                                        extra_rp_conf=extra_rp_conf,
                                        environment=environment)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        for topic in self.topics:
            self.kafka_tools.create_topic(topic)

    def tearDown(self):
        assert self.redpanda and self.redpanda.s3_client
        self.redpanda.s3_client.empty_bucket(self.s3_bucket_name)


class EndToEndShadowIndexingTest(EndToEndShadowIndexingBase):
    @cluster(num_nodes=5)
    def test_write(self):
        """Write at least 10 segments, set retention policy to leave only 5
        segments, wait for segments removal, consume data and run validation,
        that everything that is acked is consumed."""
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        # Get a snapshot of the current segments, before tightening the
        # retention policy.
        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= 10, f"Expected at least 10 segments, but got {len(node_segments)} on {node}"

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        self.start_consumer()
        self.run_validation()


class EndToEndShadowIndexingTestCompactedTopic(EndToEndShadowIndexingBase):
    topics = (TopicSpec(
        name=EndToEndShadowIndexingBase.s3_topic_name,
        partition_count=1,
        replication_factor=3,
        cleanup_policy="compact,delete",
        segment_bytes=EndToEndShadowIndexingBase.segment_size // 2), )

    @ok_to_fail
    @cluster(num_nodes=5)
    def test_write(self):
        # Set compaction interval high at first, so we can get enough segments in log
        rpk_client = RpkTool(self.redpanda)
        rpk_client.cluster_config_set("log_compaction_interval_ms",
                                      f'{1000 * 60 * 60}')

        segment_count = 10
        self.start_producer(throughput=10000, repeating_keys=10)
        wait_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=segment_count,
        )

        # Force compaction every 2 seconds now that we have some data
        rpk_client = RpkTool(self.redpanda)
        rpk_client.cluster_config_set("log_compaction_interval_ms", f'{2000}')

        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= segment_count, f"Expected at least {segment_count} segments, " \
                                f"but got {len(node_segments)} on {node}"

        self.await_num_produced(min_records=5000)
        self.logger.info(
            f"Stopping producer after writing up to offsets {self.producer.last_acked_offsets}"
        )
        self.producer.stop()

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        self.start_consumer(verify_offsets=False)
        self.run_consumer_validation(enable_compaction=True)

        s3_snapshot = S3Snapshot(self.topics, self.redpanda.s3_client,
                                 self.s3_bucket_name, self.logger)
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(self.topic,
                                                                  partition=0,
                                                                  n=1)
        s3_snapshot.assert_segments_replaced(self.topic, partition=0)


class EndToEndShadowIndexingTestWithDisruptions(EndToEndShadowIndexingBase):
    def __init__(self, test_context):
        super().__init__(test_context,
                         extra_rp_conf={
                             'default_topic_replications': self.num_brokers,
                         })

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_write_with_node_failures(self):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        # Get a snapshot of the current segments, before tightening the
        # retention policy.
        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= 10, f"Expected at least 10 segments, but got {len(node_segments)} on {node}"

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size
            },
        )

        assert self.redpanda
        with random_process_kills(self.redpanda) as ctx:
            wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                           topic=self.topic,
                                           partition_idx=0,
                                           n=6,
                                           original_snapshot=original_snapshot)

            self.start_consumer()
            self.run_validation(consumer_timeout_sec=90)
        ctx.assert_actions_triggered()


class ShadowIndexingWhileBusyTest(PreallocNodesTest):
    # With SI enabled, run common operations against a cluster
    # while the system is under load (busy).
    # This class ports Test6 from https://github.com/redpanda-data/redpanda/issues/3572
    # into ducktape.

    segment_size = 20 * 2**20
    topics = [TopicSpec()]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(log_segment_size=self.segment_size,
                                 cloud_storage_cache_size=20 * 2**30,
                                 cloud_storage_bucket='while-busy-bucket',
                                 cloud_storage_enable_remote_read=False,
                                 cloud_storage_enable_remote_write=False)

        super(ShadowIndexingWhileBusyTest,
              self).__init__(test_context=test_context,
                             node_prealloc_count=1,
                             num_brokers=7,
                             si_settings=si_settings)

        if not self.redpanda.dedicated_nodes:
            self.redpanda.set_extra_rp_conf(
                {'cloud_storage_max_readers_per_shard': 10})

    def setUp(self):
        # Dedicated nodes refers to non-container nodes such as EC2 instances
        self.topics[
            0].partition_count = 100 if self.redpanda.dedicated_nodes else 10

        # Topic creation happens here
        super().setUp()

        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes',
                               str(self.segment_size))

    @cluster(num_nodes=8)
    def test_create_or_delete_topics_while_busy(self):
        self.logger.info(f"Environment: {os.environ}")
        if self.debug_mode:
            # Trigger node allocation to avoid test failure on asking for
            # more nodes than it used.
            _ = self.preallocated_nodes

            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        # 100k messages of size 2**18
        # is ~24GB of data. 500k messages
        # of size 2**19 is ~244GB of data.
        msg_size = 2**19 if self.redpanda.dedicated_nodes else 2**18
        msg_count = 500000 if self.redpanda.dedicated_nodes else 100000
        timeout = 600

        random_parallelism = 100 if self.redpanda.dedicated_nodes else 20

        producer = KgoVerifierProducer(self.test_context, self.redpanda,
                                       self.topic, msg_size, msg_count,
                                       self.preallocated_nodes)
        producer.start(clean=False)
        # Block until a subset of records are produced + there is an offset map
        # for the consumer to use.
        producer.wait_for_acks(msg_count // 100,
                               timeout_sec=300,
                               backoff_sec=5)
        producer.wait_for_offset_map()

        rand_consumer = KgoVerifierRandomConsumer(self.test_context,
                                                  self.redpanda, self.topic,
                                                  msg_size, 100,
                                                  random_parallelism,
                                                  self.preallocated_nodes)

        rand_consumer.start(clean=False)

        random_topics = []

        # Do random ops until the producer stops
        def create_or_delete_until_producer_fin() -> bool:
            nonlocal random_topics
            trigger = random.randint(1, 2)

            if trigger == 1:
                some_topic = TopicSpec()
                self.logger.debug(f'Create topic: {some_topic}')
                self.client().create_topic(some_topic)
                random_topics.append(some_topic)

            if trigger == 2:
                if len(random_topics) > 0:
                    random.shuffle(random_topics)
                    some_topic = random_topics.pop()
                    self.logger.debug(f'Delete topic: {some_topic}')
                    self.client().delete_topic(some_topic.name)

            return producer.is_complete()

        # The wait condition will also apply some changes
        # such as topic creation and deletion
        self.redpanda.wait_until(create_or_delete_until_producer_fin,
                                 timeout_sec=timeout,
                                 backoff_sec=0.5,
                                 err_msg='Producer did not finish')

        producer.wait()
        rand_consumer.wait()
