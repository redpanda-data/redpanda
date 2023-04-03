import pprint

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings, RedpandaService, LoggingConfig, get_cloud_storage_type
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import wait_until_segments, wait_for_removal_of_n_segments
from rptest.utils.si_utils import BucketView
from ducktape.utils.util import wait_until
from ducktape.mark import matrix


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
        self.redpanda = RedpandaService(context=self.test_context,
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

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_upload(self, cloud_storage_type):
        # Set compaction to happen infrequently initially, so we have several log segments.
        self._rpk_client.cluster_config_set("log_compaction_interval_ms",
                                            f'{1000 * 60 * 60}')
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

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(
            self.topic, partition=0, revision=None, n=1)
        s3_snapshot.assert_segments_replaced(self.topic, partition=0)
        self.logger.info(
            f'manifest: {pprint.pformat(s3_snapshot.manifest_for_ntp(self.topic, 0))}'
        )
