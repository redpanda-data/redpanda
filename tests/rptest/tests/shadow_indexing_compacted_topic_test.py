import pprint

from rptest.clients.rpk import RpkPartition, RpkTool
from rptest.services.admin import Admin
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import CloudStorageType, SISettings, make_redpanda_service, LoggingConfig, get_cloud_storage_type, MetricsEndpoint
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import wait_until_segments, wait_for_removal_of_n_segments
from rptest.utils.si_utils import BucketView
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
import random
import time


class ShadowIndexingCompactedTopicTest(EndToEndTest):
    segment_size = 1048576
    topics = (TopicSpec(name='panda-topic',
                        partition_count=1,
                        replication_factor=3,
                        cleanup_policy='compact,delete'), )

    def __init__(self, test_context):
        super().__init__(test_context)
        self.num_brokers = 3
        self.si_settings = SISettings(test_context,
                                      cloud_storage_max_connections=5,
                                      fast_uploads=True)
        extra_rp_conf = dict(
            enable_leader_balancer=False,
            partition_autobalancing_mode="off",
            group_initial_rebalance_delay=300,
            compacted_log_segment_size=self.segment_size,
        )
        self.redpanda = make_redpanda_service(context=self.test_context,
                                              num_brokers=self.num_brokers,
                                              si_settings=self.si_settings,
                                              extra_rp_conf=extra_rp_conf)
        self.topic = self.topics[0].name
        self._rpk_client = RpkTool(self.redpanda)

    def setUp(self):
        super().setUp()
        self.redpanda.start()
        for topic in self.topics:
            self._rpk_client.create_topic(
                topic.name, topic.partition_count, topic.replication_factor, {
                    'cleanup.policy': topic.cleanup_policy,
                })

    def describe_topic(self, topic: str) -> list[RpkPartition]:
        description: list[RpkPartition] | None = None

        def capture_description_is_ok():
            nonlocal description
            description = list(self._rpk_client.describe_topic(topic))
            return description is not None and len(description) > 0

        wait_until(capture_description_is_ok,
                   timeout_sec=20,
                   backoff_sec=1,
                   err_msg=f"failed to get describe_topic {topic}")
        assert description is not None, "description is None"
        return description

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_upload(self, cloud_storage_type):
        # Set compaction to happen infrequently initially, so we have several log segments.
        self._rpk_client.cluster_config_set("log_compaction_interval_ms",
                                            f'{1000 * 60 * 60}')

        # observe this metric about compaction removed bytes to check that it increases
        metric = "vectorized_ntp_archiver_compacted_replaced_bytes"
        original_topic_describe = self.describe_topic(self.topic)[0]

        m = MetricCheck(self.logger,
                        self.redpanda,
                        self.redpanda.nodes[original_topic_describe.leader -
                                            1], [metric], {
                                                "namespace": "kafka",
                                                "topic": self.topic,
                                                "partition": "0"
                                            },
                        reduce=sum)

        self.start_producer(throughput=5000, repeating_keys=10)

        expected_segment_count = 10
        wait_until_segments(redpanda=self.redpanda,
                            topic=self.topic,
                            partition_idx=0,
                            count=expected_segment_count,
                            timeout_sec=300)

        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= expected_segment_count, f"Expected at least {expected_segment_count} segments, " \
                                         f"but got {len(node_segments)} on {node}"

        self.await_num_produced(min_records=10000)
        self.logger.info(
            f"Stopping producer after writing up to offsets {self.producer.last_acked_offsets}"
        )
        self.producer.stop()

        # Make sure segments are uploaded by forcing eviction
        self._rpk_client.alter_topic_config(self.topic,
                                            "retention.local.target.bytes",
                                            f'{5 * self.segment_size}')

        # Now that we have enough segments on disk, compact frequently.
        # Compaction for a given segment will only happen after it
        # is uploaded to SI. The compacted segment can then be re-uploaded
        # to SI to replace one of the original non-compacted segments.
        self._rpk_client.cluster_config_set("log_compaction_interval_ms",
                                            f'{2000}')

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        def compacted_segments_uploaded():
            manifest = BucketView(self.redpanda, topics=self.topics) \
                                  .manifest_for_ntp(self.topic,
                                                    partition=0)
            return any(meta['is_compacted']
                       for meta in manifest['segments'].values())

        wait_until(compacted_segments_uploaded,
                   timeout_sec=180,
                   backoff_sec=2,
                   err_msg=lambda: f"Compacted segments not uploaded")

        # check that the metric is increasing. the check needs to account for leadership changes (unlikely in this test),
        # because the current implementation does not persist the value in the manifest or the log
        compacted_replaced_bytes_increasing = m.evaluate([
            (metric, lambda old, new: old < new)
        ])
        new_topic_describe = self.describe_topic(self.topic)[0]
        self.logger.info(
            f"{compacted_replaced_bytes_increasing=}, original leader,epoch: {original_topic_describe.leader},{original_topic_describe.leader_epoch}. new leader,epoch:{new_topic_describe.leader},{new_topic_describe.leader_epoch}"
        )
        if original_topic_describe.leader_epoch == new_topic_describe.leader_epoch:
            assert compacted_replaced_bytes_increasing, f"{metric=} is not increasing"

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(
            self.topic, partition=0, revision=None, n=1)
        s3_snapshot.assert_segments_replaced(self.topic, partition=0)
        self.logger.info(
            f'manifest: {pprint.pformat(s3_snapshot.manifest_for_ntp(self.topic, 0))}'
        )


class TSWithAlreadyCompactedTopic(EndToEndTest):
    segment_size = 2**20
    topics = (TopicSpec(name='panda-topic-no-ts',
                        partition_count=1,
                        replication_factor=3,
                        cleanup_policy='compact',
                        redpanda_remote_read=False,
                        redpanda_remote_write=False), )

    def __init__(self, test_context, extra_rp_conf={}, environment=None):
        super().__init__(test_context)
        self.num_brokers = 3
        self.si_settings = SISettings(test_context,
                                      cloud_storage_max_connections=5,
                                      fast_uploads=True,
                                      cloud_storage_enable_remote_read=False,
                                      cloud_storage_enable_remote_write=False)
        extra_rp_conf.update(
            dict(
                enable_leader_balancer=False,
                partition_autobalancing_mode="off",
                group_initial_rebalance_delay=300,
                compacted_log_segment_size=self.segment_size,
            ))
        self.redpanda = make_redpanda_service(
            context=self.test_context,
            num_brokers=self.num_brokers,
            si_settings=self.si_settings,
            extra_rp_conf=extra_rp_conf,
            environment=environment,
            log_config=LoggingConfig(default_level='debug'))
        self.topic = self.topics[0].name
        self._rpk_client = RpkTool(self.redpanda)

    def setUp(self):
        super().setUp()
        self.redpanda.start()
        for topic in self.topics:
            self._rpk_client.create_topic(
                topic.name, topic.partition_count, topic.replication_factor, {
                    'cleanup.policy': topic.cleanup_policy,
                })

    def describe_topic(self, topic: str) -> list[RpkPartition]:
        description: list[RpkPartition] | None = None

        def capture_description_is_ok():
            nonlocal description
            description = list(self._rpk_client.describe_topic(topic))
            return description is not None and len(description) > 0

        wait_until(capture_description_is_ok,
                   timeout_sec=20,
                   backoff_sec=1,
                   err_msg=f"failed to get describe_topic {topic}")
        assert description is not None, "description is None"
        return description

    @cluster(num_nodes=4)
    def test_initial_upload(self):
        """Test initial upload of the compacted segments. The ntp_archiver
        could start from already compacted data with gaps. We should be able
        to upload such data without triggering validation errors."""
        # This topic has TS turned off. The goal is to produce a lot of data
        # and let it get compacted.
        topic_spec = self.topics[0]
        topic_name = topic_spec.name

        self._rpk_client.cluster_config_set("log_compaction_interval_ms",
                                            "9999999")
        # actually produce data
        self.start_producer(throughput=5000, repeating_keys=2)

        for p in range(1000, 10000, 1000):
            self.await_num_produced(min_records=p)
            # Generate a bunch of config batches and terms
            # to prevent a situation when everything except the
            # last segment is compacted away. We want to have several
            # compacted segments with different terms.
            self._transfer_topic_leadership()

        wait_until_segments(redpanda=self.redpanda,
                            topic=self.topic,
                            partition_idx=0,
                            count=10,
                            timeout_sec=300)

        self.logger.info(
            f"Stopping producer after writing up to offsets {self.producer.last_acked_offsets}"
        )
        self.producer.stop()

        # Collect original file sizes
        original_per_node_stat = {}
        for node in self.redpanda.nodes:
            stats = self.redpanda.data_stat(node)
            total = sum(
                [size for path, size in stats if path.suffix == ".log"])
            original_per_node_stat[node] = total
            self.logger.info(f"Size before compaction {total}")
            assert total > 0, "No data found in the data dir"

        # list redpanda dir before compaction
        self.redpanda.storage(scan_cache=False)
        # wait for compaction to do the job
        self._rpk_client.cluster_config_set("log_compaction_interval_ms",
                                            "500")

        def compacted():
            # Collect original file sizes
            worst_ratio = 0.0
            for node in self.redpanda.nodes:
                new_stats = self.redpanda.data_stat(node)
                new_size = sum([
                    size for path, size in new_stats if path.suffix == ".log"
                ])
                old_size = original_per_node_stat[node]
                ratio = new_size / old_size
                worst_ratio = max(worst_ratio, ratio)
                self.logger.info(
                    f"Old data dir size: {old_size}, new data dir size: {new_size}, ratio: {ratio}"
                )
            self.logger.info(
                f"Worst compaction ration across the cluster is {worst_ratio}")
            return worst_ratio < 0.8

        wait_until(compacted,
                   timeout_sec=120,
                   backoff_sec=2,
                   err_msg=f"Segments were not compacted well enough")

        # enable TS for the topic
        self._rpk_client.alter_topic_config(topic_name, "redpanda.remote.read",
                                            "true")
        self._rpk_client.alter_topic_config(topic_name,
                                            "redpanda.remote.write", "true")

        # Transfer leadership while the data is uploaded.
        for _ in range(0, 10):
            self._transfer_topic_leadership()
            time.sleep(1)

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        self.logger.info(
            f'manifest: {pprint.pformat(s3_snapshot.manifest_for_ntp(topic_name, 0))}'
        )
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(
            topic_name, partition=0, revision=None, n=1)
        self.logger.info(
            f'manifest: {pprint.pformat(s3_snapshot.manifest_for_ntp(topic_name, 0))}'
        )
        # Given that we have multiple terms we shouldn't have only one segment
        # even after compaction.
        assert len(s3_snapshot.manifest_for_ntp(topic_name, 0)['segments']) > 1

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
