# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import (
    segments_count,
    wait_for_segments_removal,
)
from rptest.utils.si_utils import TxProducer

from ducktape.utils.util import wait_until

import confluent_kafka as ck

import uuid
import random
from itertools import zip_longest


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

        si_settings = SISettings(cloud_storage_reconciliation_interval_ms=500,
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

    @cluster(num_nodes=3)
    def test_shadow_indexing_aborted_txs(self):
        """Check that messages belonging to aborted transaction are not seen by clients
        when fetching from remote segments."""
        topic = self.topics[0]

        producer = TxProducer(self.redpanda.brokers(),
                              "shadow-indexing-tx-test", self.logger)

        def done():
            for _ in range(100):
                try:
                    producer.produce(topic.name)
                except ck.KafkaException as err:
                    self.logger.warn(f"producer error: {err}")
                    producer.reconnect()
            self.logger.info("producer iteration complete")
            topic_partitions = segments_count(self.redpanda,
                                              topic.name,
                                              partition_idx=0)
            partitions = []
            for p in topic_partitions:
                partitions.append(p >= 10)
            return all(partitions)

        wait_until(done,
                   timeout_sec=120,
                   backoff_sec=1,
                   err_msg="producing failed")

        assert producer.num_aborted > 0

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

        consumer = ck.Consumer(
            {
                'bootstrap.servers': self.redpanda.brokers(),
                'group.id': 'shadow-indexing-tx-test',
                'auto.offset.reset': 'earliest',
            },
            logger=self.logger)
        consumer.subscribe([topic.name])

        consumed = []
        while True:
            msgs = consumer.consume(timeout=5.0)
            if len(msgs) == 0:
                break
            consumed.extend([(m.key(), m.offset()) for m in msgs])

        self.logger.info(
            f"consumed {len(consumed)} records, expected {len(producer.keys)} keys"
        )

        first_mismatch = ''
        for p_key, (c_key, c_offset) in zip_longest(producer.keys,
                                                    consumed,
                                                    fillvalue=(None, -1)):
            if p_key != c_key:
                first_mismatch = f"produced: {p_key}, consumed: {c_key} (offset: {c_offset})"
                break

        assert (not first_mismatch), (
            f"produced and consumed messages differ, "
            f"produced length: {len(producer.keys)}, consumed length: {len(consumed)}, "
            f"first mismatch: {first_mismatch}")
