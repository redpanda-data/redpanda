# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings, get_cloud_storage_type
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.utils.mode_checks import skip_debug_mode
from rptest.util import (
    segments_count,
    wait_for_local_storage_truncate,
)
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer


class ShadowIndexingTxTest(RedpandaTest):
    segment_size = 1048576  # 1 Mb
    topics = (TopicSpec(name='panda-topic',
                        partition_count=1,
                        replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            enable_leader_balancer=False,
            partition_autobalancing_mode="off",
            group_initial_rebalance_delay=300,
        )

        si_settings = SISettings(test_context,
                                 cloud_storage_max_connections=5,
                                 log_segment_size=self.segment_size)

        super(ShadowIndexingTxTest, self).__init__(test_context=test_context,
                                                   extra_rp_conf=extra_rp_conf,
                                                   si_settings=si_settings)

    def setUp(self):
        rpk = RpkTool(self.redpanda)
        super(ShadowIndexingTxTest, self).setUp()
        for topic in self.topics:
            rpk.alter_topic_config(topic.name, 'redpanda.remote.write', 'true')
            rpk.alter_topic_config(topic.name, 'redpanda.remote.read', 'true')

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    @skip_debug_mode
    def test_shadow_indexing_aborted_txs(self, cloud_storage_type):
        """Check that messages belonging to aborted transaction are not seen by clients
        when fetching from remote segments."""
        msg_size = 16384
        msg_count = 10000
        per_transaction = 10
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       use_transactions=True,
                                       transaction_abort_rate=0.1,
                                       msgs_per_transaction=per_transaction,
                                       debug_logs=True)
        producer.start()
        producer.wait()
        pstatus = producer.produce_status
        self.logger.info(f"Produce status: {pstatus}")
        committed_messages = pstatus.acked - pstatus.aborted_transaction_messages
        assert pstatus.acked == msg_count
        assert 0 < committed_messages < msg_count

        # Re-use node for consumer later
        traffic_node = producer.nodes[0]

        kafka_tools = KafkaCliTools(self.redpanda)
        local_retention = 3 * self.segment_size
        kafka_tools.alter_topic_config(
            self.topic,
            {TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES: local_retention},
        )
        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        target_bytes=local_retention)

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          msg_size,
                                          loop=False,
                                          nodes=[traffic_node])
        consumer.start(clean=False)
        consumer.wait()
        status = consumer.consumer_status

        assert status.validator.valid_reads == committed_messages
        assert status.validator.invalid_reads == 0
        assert status.validator.out_of_scope_invalid_reads == 0

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    @skip_debug_mode
    def test_txless_segments(self, cloud_storage_type):
        """
        Check that for segments _without_ aborted transactions, we don't
        waste resources issuing object storage GETs or writing empty
        manifests to the cache.
        """

        local_retention = 3 * self.segment_size
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.alter_topic_config(
            self.topic,
            {TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES: local_retention},
        )

        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    self.topic,
                                    msg_size=16384,
                                    msg_count=((10 * self.segment_size) //
                                               16384))

        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        target_bytes=local_retention)

        KgoVerifierSeqConsumer.oneshot(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       16384,
                                       loop=False)

        # There should have been no failures to download
        # vectorized_cloud_storage_failed_manifest_downloads
        metric = self.redpanda.metrics_sample(
            "vectorized_cloud_storage_failed_manifest_downloads")
        assert len(metric.samples)
        for sample in metric.samples:
            assert sample.value == 0, f"Saw >0 failed manifest downloads {sample}"

        # There should be zero .tx files in the cache
        for node in self.redpanda.nodes:
            cached_tx_manifests = int(
                node.account.ssh_output(
                    f"find \"{self.redpanda.cache_dir}\" -type f -name \"*.tx\" | wc -l",
                    combine_stderr=False).strip())
            assert cached_tx_manifests == 0, f"Found {cached_tx_manifests} cached manifests on {node.name}"
