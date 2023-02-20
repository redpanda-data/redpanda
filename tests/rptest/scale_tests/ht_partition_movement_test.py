# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.clients.types import TopicSpec
from ducktape.mark import parametrize


class HighThroughputPartitionMovementTest(PreallocNodesTest,
                                          PartitionMovementMixin):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context,
                         node_prealloc_count=1,
                         num_brokers=5,
                         extra_rp_conf={
                             "raft_learner_recovery_rate": 10 * 1073741824,
                         },
                         *args,
                         **kwargs)

        if not self.redpanda.dedicated_nodes:
            # Mini mode, for developers working on the test on their workstation.
            # (not for use in CI)
            self._partitions = 16
            self._message_size = 16384
            self._message_cnt = 64000
            self._consumers = 1
            self._number_of_moves = 2
        else:
            self._partitions = 32
            self._message_size = 256 * (2**10)  # 256 KB per message
            self._message_cnt = 819200  # 100GB data
            self._consumers = 8
            self._number_of_moves = 5

    def _start_producer(self, topic_name):
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            self._message_size,
            self._message_cnt,
            custom_node=self.preallocated_nodes)
        self.producer.start(clean=False)

        wait_until(lambda: self.producer.produce_status.acked > 10,
                   timeout_sec=30,
                   backoff_sec=1)

    def _start_consumer(self, topic_name):
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic_name,
            self._message_size,
            readers=self._consumers,
            nodes=self.preallocated_nodes)
        self.consumer.start(clean=False)

    def verify(self, topic_name):
        self.producer.wait()

        self.consumer.wait()
        assert self.consumer.consumer_status.validator.invalid_reads == 0
        del self.consumer

        # Create a fresh consumer to read the quiescent state from start to finish
        self._start_consumer(topic_name)
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.valid_reads >= self.producer.produce_status.acked
        assert self.consumer.consumer_status.validator.invalid_reads == 0

    @cluster(num_nodes=6)
    @parametrize(replication_factor=1)
    @parametrize(replication_factor=3)
    def test_moving_single_partition_under_load(self, replication_factor):
        topic = TopicSpec(partition_count=self._partitions,
                          replication_factor=replication_factor)
        self.client().create_topic(topic)

        self._start_producer(topic.name)
        self._start_consumer(topic.name)

        for _ in range(self._number_of_moves):
            # choose a random topic-partition
            metadata = self.client().describe_topics()
            t, partition = self._random_partition(metadata)
            self.logger.info(f"selected partition: {t}/{partition}")
            self._do_move_and_verify(t, partition, 600)

        self.verify(topic.name)

    def _random_move_and_cancel(self, topic, partition):
        previous_assignment, new_assignment = self._dispatch_random_partition_move(
            topic, partition, allow_no_op=False)

        self._request_move_cancel(unclean_abort=False,
                                  topic=topic,
                                  partition=partition,
                                  previous_assignment=previous_assignment,
                                  new_assignment=new_assignment)

    @cluster(num_nodes=6)
    @parametrize(replication_factor=1)
    @parametrize(replication_factor=3)
    def test_interrupting_partition_movement_under_load(
            self, replication_factor):
        topic = TopicSpec(partition_count=self._partitions,
                          replication_factor=replication_factor)
        self.client().create_topic(topic)

        self._start_producer(topic.name)
        self._start_consumer(topic.name)

        for _ in range(self._number_of_moves):
            # choose a random topic-partition
            metadata = self.client().describe_topics()
            t, partition = self._random_partition(metadata)
            self.logger.info(f"selected partition: {t}/{partition}")

            self._random_move_and_cancel(t, partition)

        self.verify(topic.name)
