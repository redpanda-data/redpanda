# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import parametrize
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.utils.mode_checks import skip_debug_mode
from rptest.util import (
    segments_count,
    wait_for_segments_removal,
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
    @parametrize(cloud_storage_type=CloudStorageType.ABS)
    @parametrize(cloud_storage_type=CloudStorageType.S3)
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
        kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                3 * self.segment_size,
            },
        )
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=6)

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
