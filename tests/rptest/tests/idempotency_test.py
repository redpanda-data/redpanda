# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.default import DefaultClient
from rptest.clients.types import TopicSpec
from rptest.services.action_injector import ActionConfig, random_process_kills
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.node_operations import FailureInjectorBackgroundThread
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool

from confluent_kafka import (Producer, KafkaException)


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)


class IdempotencyTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(IdempotencyTest, self).__init__(test_context=test_context,
                                              extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_idempotency_compacted_topic(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1", config={"cleanup.policy": "compact"})

        producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5
        })
        producer.produce("topic1",
                         key="key1".encode('utf-8'),
                         value="value1".encode('utf-8'),
                         callback=on_delivery)
        producer.flush()


class IdempotencySnapshotDelivery(PreallocNodesTest):
    def __init__(self, test_context):
        extra_rp_conf = {"enable_leader_balancer": False}

        si_settings = SISettings(test_context,
                                 log_segment_size=1024 * 1024,
                                 fast_uploads=True)
        super(IdempotencySnapshotDelivery,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf,
                             node_prealloc_count=1,
                             si_settings=si_settings)

    @cluster(num_nodes=4)
    def test_recovery_after_snapshot_is_delivered(self):
        segment_bytes = 1024 * 1024
        msg_size = 128
        rate_limit = 10 * (1024 * 1024) if not self.debug_mode else 1024 * 1024
        msg_cnt = int(15 * rate_limit / msg_size)

        topic = TopicSpec(partition_count=1,
                          replication_factor=3,
                          segment_bytes=segment_bytes,
                          retention_bytes=2 * segment_bytes,
                          redpanda_remote_read=True,
                          redpanda_remote_write=True)

        # create topic with small segments and short retention
        DefaultClient(self.redpanda).create_topic(topic)

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic.name,
                                       msg_size,
                                       msg_cnt,
                                       custom_node=self.preallocated_nodes,
                                       rate_limit_bps=rate_limit)

        producer.start(clean=False)

        pkill_config = ActionConfig(cluster_start_lead_time_sec=10,
                                    min_time_between_actions_sec=10,
                                    max_time_between_actions_sec=20)
        with random_process_kills(self.redpanda, pkill_config) as ctx:
            wait_until(lambda: producer.produce_status.acked >= msg_cnt, 240,
                       1)
            producer.stop()

        assert producer.produce_status.bad_offsets == 0, "Producer bad offsets detected"


class IdempotencyWriteCachingTest(PreallocNodesTest):
    def __init__(self, test_context):
        super(IdempotencyWriteCachingTest, self).__init__(
            test_context=test_context,
            node_prealloc_count=1,
        )

    @cluster(num_nodes=5)
    def test_idempotent_producers_write_caching(self):

        msg_size = 128
        rate_limit = 10 * (1024 * 1024) if not self.debug_mode else 1024 * 1024
        msg_cnt = int(15 * rate_limit / msg_size)

        topic = TopicSpec(partition_count=4, replication_factor=3)

        DefaultClient(self.redpanda).create_topic(topic)

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(topic.name, TopicSpec.PROPERTY_WRITE_CACHING,
                               "true")
        # configure topic with write caching to delay state machine apply
        rpk.alter_topic_config(topic.name, TopicSpec.PROPERTY_FLUSH_BYTES,
                               1024 * 1024)
        rpk.alter_topic_config(topic.name, TopicSpec.PROPERTY_FLUSH_MS,
                               1024 * 1024)
        fi = FailureInjectorBackgroundThread(self.redpanda,
                                             self.logger,
                                             max_inter_failure_time=30,
                                             min_inter_failure_time=10,
                                             max_suspend_duration_seconds=4)
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic.name,
                                       msg_size,
                                       msg_cnt,
                                       custom_node=self.preallocated_nodes,
                                       rate_limit_bps=rate_limit)
        try:
            producer.start(clean=False)
            fi.start()
            producer.wait()

            consumer = KgoVerifierConsumerGroupConsumer(
                self.test_context,
                self.redpanda,
                topic.name,
                msg_size,
                readers=2,
                max_msgs=msg_cnt,
                group_name="test-group")
            consumer.start()
            consumer.wait()
        finally:
            fi.stop()
