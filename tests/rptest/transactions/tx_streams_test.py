# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec

import random

from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.transactions.util import TransactionsMixin
import confluent_kafka as ck

from rptest.clients.rpk import RpkTool


class TransactionsStreamsTest(RedpandaTest, TransactionsMixin):
    topics = (
        TopicSpec(partition_count=1, replication_factor=3),
        TopicSpec(partition_count=1, replication_factor=3),
    )

    def __init__(self, test_context):
        extra_rp_conf = {
            "unsafe_enable_consumer_offsets_delete_retention": True,
            "group_topic_partitions": 1,  # to reduce log noise
            "log_segment_size_min": 99,
            # to be able to make changes to CO
            "kafka_nodelete_topics": [],
            "kafka_noproduce_topics": [],
        }
        super(TransactionsStreamsTest, self).__init__(
            test_context=test_context, extra_rp_conf=extra_rp_conf
        )
        self.input_t = self.topics[0]
        self.output_t = self.topics[1]

    def setup_consumer_offsets(self, rpk: RpkTool):
        # initialize consumer groups topic
        rpk.consume(topic=self.input_t.name, n=1, group="test-group")
        topic = "__consumer_offsets"
        # Aggressive roll settings to clear multiple small segments
        rpk.alter_topic_config(
            topic, TopicSpec.PROPERTY_CLEANUP_POLICY, TopicSpec.CLEANUP_DELETE
        )
        rpk.alter_topic_config(topic, TopicSpec.PROPERTY_SEGMENT_SIZE, 100)

    @cluster(num_nodes=3)
    def consumer_offsets_retention_test(self):
        """Ensure consumer offsets replays correctly after transactional offset commits"""
        input_records = 10000
        self.generate_data(self.input_t, input_records)
        rpk = RpkTool(self.redpanda)
        self.setup_consumer_offsets(rpk)
        # Populate consumer offsets with transactional offset commits/aborts
        producer_conf = {
            "bootstrap.servers": self.redpanda.brokers(),
            "transactional.id": "streams",
        }
        producer = ck.Producer(producer_conf)
        consumer_conf = {
            "bootstrap.servers": self.redpanda.brokers(),
            "group.id": "test",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = ck.Consumer(consumer_conf)
        consumer.subscribe([self.input_t])

        producer.init_transactions()
        consumed = 0
        while consumed != input_records:
            records = self.consume(consumer)
            producer.begin_transaction()
            for record in records:
                producer.produce(
                    self.output_t.name,
                    record.value(),
                    record.key(),
                    on_delivery=self.on_delivery,
                )

            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata(),
            )

            producer.flush()

            if random.randint(0, 9) < 5:
                producer.commit_transaction()
            else:
                producer.abort_transaction()
            consumed += len(records)

        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.started_nodes():
            co_records = log_viewer.read_consumer_offsets(node=node)
            self.logger.info(f"Read {len(co_records)} from node {node.name}")

        admin = Admin(self.redpanda)
        co_topic = "__consumer_offsets"

        def get_offsets():
            topic_info = list(rpk.describe_topic(co_topic))[0]
            assert topic_info
            return (topic_info.start_offset, topic_info.high_watermark)

        # trim prefix, change leadership and validate the log is replayed successfully on
        # the new leader.
        attempts = 30
        truncate_offset = 100
        while attempts > 0:
            (start, end) = get_offsets()
            self.redpanda.logger.debug(f"Current offsets: {start} - {end}")
            if truncate_offset > end:
                break
            rpk.trim_prefix(co_topic, truncate_offset, partitions=[0])
            admin.partition_transfer_leadership("kafka", co_topic, partition=0)
            admin.await_stable_leader(topic=co_topic, replication=3, timeout_s=30)
            truncate_offset += 200
            attempts = attempts - 1
