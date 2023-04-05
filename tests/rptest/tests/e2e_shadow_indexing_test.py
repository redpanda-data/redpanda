# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import re
import time
from collections import defaultdict

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.action_injector import random_process_kills
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierRandomConsumer, KgoVerifierSeqConsumer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST
from rptest.services.redpanda import SISettings, get_cloud_storage_type
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.util import Scale, wait_until_segments
from rptest.util import (
    produce_until_segments,
    wait_for_removal_of_n_segments,
)
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.si_utils import nodes_report_cloud_segments, BucketView


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
            test_context,
            cloud_storage_max_connections=5,
            log_segment_size=self.segment_size,  # 1MB
            fast_uploads=True,
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
        assert self.redpanda and self.redpanda.cloud_storage_client
        self.redpanda.cloud_storage_client.empty_bucket(self.s3_bucket_name)


class EndToEndShadowIndexingTest(EndToEndShadowIndexingBase):
    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_write(self, cloud_storage_type):
        """Write at least 10 segments, set retention policy to leave only 5
        segments, wait for segments removal, consume data and run validation,
        that everything that is acked is consumed."""
        brokers = self.redpanda.started_nodes()
        index_metrics = [
            MetricCheck(self.logger,
                        self.redpanda,
                        node, [
                            'vectorized_cloud_storage_index_uploads_total',
                            'vectorized_cloud_storage_index_downloads_total',
                        ],
                        reduce=sum) for node in brokers
        ]

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

        assert any(
            im.evaluate([('vectorized_cloud_storage_index_uploads_total',
                          lambda _, cnt: cnt)]) for im in index_metrics)

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

        assert any(
            im.evaluate([('vectorized_cloud_storage_index_downloads_total',
                          lambda _, cnt: cnt)]) for im in index_metrics)

        # Matches the segment or the index
        cache_expr = re.compile(
            fr'^({self.redpanda.DATA_DIR}/cloud_storage_cache/.*\.log\.\d+)(\.index)?$'
        )

        # Each segment should have the corresponding index present
        for node in self.redpanda.nodes:
            index_segment_pair = defaultdict(lambda: 0)
            for file in self.redpanda.data_checksum(node):
                if (match := cache_expr.match(file)) and self.topic in file:
                    index_segment_pair[match[1]] += 1
            for file, count in index_segment_pair.items():
                assert count == 2, f'expected one index and one log for {file}, found {count}'


class EndToEndShadowIndexingTestCompactedTopic(EndToEndShadowIndexingBase):
    topics = (TopicSpec(
        name=EndToEndShadowIndexingBase.s3_topic_name,
        partition_count=1,
        replication_factor=3,
        cleanup_policy="compact,delete",
        segment_bytes=EndToEndShadowIndexingBase.segment_size // 2), )

    def _prime_compacted_topic(self, segment_count):
        # Set compaction interval high at first, so we can get enough segments in log
        rpk_client = RpkTool(self.redpanda)
        rpk_client.cluster_config_set("log_compaction_interval_ms",
                                      f'{1000 * 60 * 60}')

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

        return original_snapshot

    def _transfer_topic_leadership(self):
        admin = Admin(self.redpanda)
        cur_leader = admin.get_partition_leader(namespace='kafka',
                                                topic=self.topic,
                                                partition=0)
        broker_ids = [x['node_id'] for x in admin.get_brokers()]
        transfer_to = random.choice([n for n in broker_ids if n != cur_leader])
        assert cur_leader != transfer_to, "incorrect partition move in test"
        admin.transfer_leadership_to(namespace="kafka",
                                     topic=self.topic,
                                     partition=0,
                                     target_id=transfer_to,
                                     leader_id=cur_leader)

        admin.await_stable_leader(self.topic,
                                  partition=0,
                                  namespace='kafka',
                                  timeout_s=60,
                                  backoff_s=2,
                                  check=lambda node_id: node_id == transfer_to)

    @skip_debug_mode
    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_write(self, cloud_storage_type):
        original_snapshot = self._prime_compacted_topic(10)

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

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(
            self.topic, partition=0, revision=None, n=1)
        s3_snapshot.assert_segments_replaced(self.topic, partition=0)

    @skip_debug_mode
    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_compacting_during_leadership_transfer(self, cloud_storage_type):
        original_snapshot = self._prime_compacted_topic(10)

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        # Transfer the topic to another node
        self._transfer_topic_leadership()

        # After leadership transfer has completed assert that manifest is OK
        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        self.start_consumer(verify_offsets=False)
        self.run_consumer_validation(enable_compaction=True)

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(
            self.topic, partition=0, revision=None, n=1)
        s3_snapshot.assert_segments_replaced(self.topic, partition=0)


class EndToEndShadowIndexingTestWithDisruptions(EndToEndShadowIndexingBase):
    def __init__(self, test_context):
        super().__init__(test_context,
                         extra_rp_conf={
                             'default_topic_replications': self.num_brokers,
                         })

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_write_with_node_failures(self, cloud_storage_type):
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


class ShadowIndexingInfiniteRetentionTest(EndToEndShadowIndexingBase):
    # Use a small segment size to speed up the test.
    small_segment_size = EndToEndShadowIndexingBase.segment_size // 4
    small_interval_ms = 100
    infinite_topic_name = f"{EndToEndShadowIndexingBase.s3_topic_name}"
    topics = (TopicSpec(name=infinite_topic_name,
                        partition_count=1,
                        replication_factor=1,
                        retention_bytes=-1,
                        retention_ms=-1,
                        segment_bytes=small_segment_size), )

    def __init__(self, test_context):
        self.num_brokers = 1
        super().__init__(
            test_context,
            extra_rp_conf={
                # Trigger housekeeping frequently to encourage segment
                # deletion.
                "cloud_storage_housekeeping_interval_ms":
                self.small_interval_ms,

                # Use small cluster-wide retention settings to
                # encourage cloud segment deletion, as we ensure
                # nothing will be deleted for an infinite-retention
                # topic.
                "delete_retention_ms": self.small_interval_ms,
                "retention_bytes": self.small_segment_size,
            })

    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_segments_not_deleted(self, cloud_storage_type):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=2,
        )

        # Wait for there to be some segments.
        def manifest_has_segments():
            s3_snapshot = BucketView(self.redpanda, topics=self.topics)
            manifest = s3_snapshot.manifest_for_ntp(self.infinite_topic_name,
                                                    0)
            return "segments" in manifest

        wait_until(manifest_has_segments, timeout_sec=10, backoff_sec=1)

        # Give ample time for would-be segment deletions to occur.
        time.sleep(5)
        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        manifest = s3_snapshot.manifest_for_ntp(self.infinite_topic_name, 0)
        assert "0-1-v1.log" in manifest["segments"], manifest


class ShadowIndexingManyPartitionsTest(PreallocNodesTest):
    small_segment_size = 4096
    topic_name = f"{EndToEndShadowIndexingBase.s3_topic_name}"
    topics = (TopicSpec(name=topic_name,
                        partition_count=128,
                        replication_factor=1,
                        redpanda_remote_write=True,
                        redpanda_remote_read=True,
                        retention_bytes=-1,
                        retention_ms=-1,
                        segment_bytes=small_segment_size), )

    def __init__(self, test_context):
        self.num_brokers = 1
        si_settings = SISettings(
            test_context,
            log_segment_size=self.small_segment_size,
            cloud_storage_cache_size=20 * 2**30,
            cloud_storage_segment_max_upload_interval_sec=1)
        super().__init__(
            test_context,
            node_prealloc_count=1,
            extra_rp_conf={
                # Avoid segment merging so we can generate many segments
                # quickly.
                "cloud_storage_enable_segment_merging": False,
                'log_segment_size_min': 1024,
            },
            si_settings=si_settings)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def setUp(self):
        self.redpanda.start()
        for topic in self.topics:
            self.kafka_tools.create_topic(topic)
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic,
                               TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_MS,
                               '1000')

    @skip_debug_mode
    @cluster(num_nodes=2)
    def test_many_partitions_shutdown(self):
        """
        Test that reproduces a slow shutdown when many partitions each with
        many hydrated segments get shut down.
        """
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=1024,
                                       msg_count=1000 * 1000,
                                       custom_node=self.preallocated_nodes)
        producer.start()
        try:
            wait_until(
                lambda: nodes_report_cloud_segments(self.redpanda, 128 * 200),
                timeout_sec=120,
                backoff_sec=3)
        finally:
            producer.stop()
            producer.wait()

        seq_consumer = KgoVerifierSeqConsumer(self.test_context,
                                              self.redpanda,
                                              self.topic,
                                              0,
                                              nodes=self.preallocated_nodes)
        seq_consumer.start(clean=False)
        seq_consumer.wait()
        self.redpanda.stop_node(self.redpanda.nodes[0])


class ShadowIndexingWhileBusyTest(PreallocNodesTest):
    # With SI enabled, run common operations against a cluster
    # while the system is under load (busy).
    # This class ports Test6 from https://github.com/redpanda-data/redpanda/issues/3572
    # into ducktape.

    segment_size = 20 * 2**20
    topics = [TopicSpec()]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context,
                                 log_segment_size=self.segment_size,
                                 cloud_storage_cache_size=20 * 2**30,
                                 cloud_storage_enable_remote_read=False,
                                 cloud_storage_enable_remote_write=False,
                                 fast_uploads=True)

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

    @cluster(num_nodes=8)
    @matrix(short_retention=[False, True],
            cloud_storage_type=get_cloud_storage_type())
    @skip_debug_mode
    def test_create_or_delete_topics_while_busy(self, short_retention,
                                                cloud_storage_type):
        """
        :param short_retention: whether to run with a very short retention globally, or just
               a short target for local retention (see issue #7092)
        """
        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')

        rpk.alter_topic_config(
            self.topic, 'retention.bytes'
            if short_retention else 'retention.local.target.bytes',
            str(self.segment_size))

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
