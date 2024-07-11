# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import tempfile
import time

from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest


class RpkGroupCommandsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(RpkGroupCommandsTest, self).__init__(
            test_ctx,
            *args,
            **kwargs,
        )

    def _rpk_produce_to_topic(self, topic, n=10, create_topic=True):
        self.rpk = RpkTool(self.redpanda)
        if create_topic:
            self.rpk.create_topic(topic)

        for p in range(n):
            self.rpk.produce(topic, f"k-{p}", f"v-{p}")

    def setup_producer(self, p_cnt, topic_name):
        # create topic
        self.topic_spec = TopicSpec(partition_count=p_cnt, name=topic_name)
        self.client().create_topic(specs=self.topic_spec)
        # produce some messages to the topic
        self.producer = RpkProducer(self._ctx, self.redpanda,
                                    self.topic_spec.name, 128, 500)
        self.producer.start()

    def validate_partition(self, partition, exp_current_offset,
                           exp_log_start_offset, exp_log_end_offset, exp_lag):
        assert partition.current_offset == exp_current_offset
        assert partition.log_start_offset == exp_log_start_offset
        assert partition.log_end_offset == exp_log_end_offset
        assert partition.lag == exp_lag

    def _assert_eq(self, lhs, rhs):
        assert lhs == rhs, f"Unexpected: {lhs} != {rhs}"

    @cluster(num_nodes=3)
    def test_group_list_and_delete(self):
        """
        Simple listing and deleting one of the groups. Also tests group state-based filtering.
        """
        topic = "test_group"
        self._rpk_produce_to_topic(topic, 20)

        group_1 = "test-g1"
        self.rpk.consume(topic, group=group_1, n=10)

        group_2 = "test-g2"
        self.rpk.consume(topic, group=group_2, n=10)

        # groups will show up until they're deleted or until all offsets expire
        group_names = self.rpk.group_list_names()
        assert group_1 in group_names
        assert group_2 in group_names

        self.rpk.group_delete(group_1)
        group_names = self.rpk.group_list_names()
        assert group_1 not in group_names

        all_groups = self.rpk.group_list()
        self._assert_eq(all_groups[0].group, group_2)
        self._assert_eq(all_groups[0].state, "Empty")

        e_s_groups = self.rpk.group_list(states=["Empty", "Stable"])
        self._assert_eq(e_s_groups[0].group, group_2)
        self._assert_eq(e_s_groups[0].state, "Empty")

        pr_groups = self.rpk.group_list(states=["PreparingRebalance"])
        self._assert_eq(len(pr_groups), 0)

    @cluster(num_nodes=5)
    def test_group_describe(self):
        """
        Check for group describe when the group is stable.
        """
        topic = "test_group"
        self.setup_producer(3, topic)

        group_1 = "test-g1"
        consumer = RpkConsumer(self._ctx, self.redpanda, topic, group=group_1)
        consumer.start()

        rpk = RpkTool(self.redpanda)

        wait_until(lambda: rpk.group_describe(group_1).state == "Stable",
                   timeout_sec=30,
                   backoff_sec=1)

        self.producer.stop()
        consumer.stop()

    @cluster(num_nodes=1)
    def test_empty_group_describe(self):
        """
        Check for group describe when the group is empty.
        Reproduce: https://github.com/redpanda-data/redpanda/commit/ab6a99162056e2980ebf873ed7192878419769ae
        """
        topic = "test_group"
        self._rpk_produce_to_topic(topic, 20)

        group_1 = "test-g1"
        self.rpk.consume(topic, group=group_1, n=10)

        rpk_group = self.rpk.group_describe(group_1)

        assert rpk_group.state == "Empty"
        assert rpk_group.name == group_1
        self.validate_partition(rpk_group.partitions[0], 10, 0, 20, 10)

    @cluster(num_nodes=1)
    def test_group_seek_to(self):
        """
        Check rpk group seek `--to end` and `--to start`
        """
        topic = "test_group"
        self._rpk_produce_to_topic(topic, 20)

        group_1 = "test-g1"
        self.rpk.consume(topic, group=group_1, n=10)

        rpk_group = self.rpk.group_describe(group_1)
        self.validate_partition(rpk_group.partitions[0], 10, 0, 20, 10)

        self.rpk.group_seek_to(group_1, "end")
        rpk_group = self.rpk.group_describe(group_1)
        self.validate_partition(rpk_group.partitions[0], 20, 0, 20, 0)

        self.rpk.group_seek_to(group_1, "start")
        rpk_group = self.rpk.group_describe(group_1)
        self.validate_partition(rpk_group.partitions[0], 0, 0, 20, 20)

    @cluster(num_nodes=1)
    def test_group_seek_to_group(self):
        """
        Check merge operation of `rpk group seek --to-group`
        """
        topic = "test_group"
        self._rpk_produce_to_topic(topic, 30)

        group_1 = "test-g1"
        self.rpk.consume(topic, group=group_1, n=10)

        group_2 = "test-g2"
        self.rpk.consume(topic, group=group_2, n=20)

        rpk_group_1 = self.rpk.group_describe(group_1)
        rpk_group_2 = self.rpk.group_describe(group_2)

        assert rpk_group_1.partitions != rpk_group_2.partitions

        self.rpk.group_seek_to_group(group_1, group_2)
        rpk_group_1 = self.rpk.group_describe(group_1)
        rpk_group_2 = self.rpk.group_describe(group_2)

        assert rpk_group_1.partitions == rpk_group_2.partitions
        # Now that we know both are equals, we check the values
        # only in group_1
        self.validate_partition(rpk_group_1.partitions[0], 20, 0, 30, 10)

    @cluster(num_nodes=1)
    def test_group_seek_to_file(self):
        topic_1 = "test_1"
        self._rpk_produce_to_topic(topic_1, 30)

        topic_2 = "test_2"
        self._rpk_produce_to_topic(topic_2, 30)

        group_1 = "g1"
        self.rpk.consume(topic_1, group=group_1, n=10)

        file = f"""
{topic_1} 0 29
{topic_2} 0 0"""

        with tempfile.NamedTemporaryFile() as tf:
            tf.write(bytes(file, 'UTF-8'))
            tf.seek(0)

            self.rpk.group_seek_to_file(group_1, tf.name)

        rpk_group = self.rpk.group_describe(group_1)

        # We describe topics that don't have commited offsets too:
        assert len(rpk_group.partitions) == 2
        self.validate_partition(rpk_group.partitions[0], 29, 0, 30, 1)
        self.validate_partition(rpk_group.partitions[1], 0, 0, 30, 30)

    @cluster(num_nodes=1)
    def test_group_seek_to_timestamp(self):
        topic_1 = "test_1"

        time_0 = str(int(time.time()))
        self._rpk_produce_to_topic(topic_1, 5)
        time.sleep(1)

        time_1 = str(int(time.time()))
        self._rpk_produce_to_topic(topic_1, 5, create_topic=False)
        time.sleep(1)

        group_1 = "g1"
        self.rpk.consume(topic_1, group=group_1, n=10)

        # Before seeking:
        rpk_group = self.rpk.group_describe(group_1)
        self.validate_partition(rpk_group.partitions[0], 10, 0, 10, 0)

        # Seek to the beginning
        self.rpk.group_seek_to(group_1, time_0)
        rpk_group = self.rpk.group_describe(group_1)
        self.validate_partition(rpk_group.partitions[0], 0, 0, 10, 10)

        # Seek to the middle
        self.rpk.group_seek_to(group_1, time_1)
        rpk_group = self.rpk.group_describe(group_1)
        self.validate_partition(rpk_group.partitions[0], 5, 0, 10, 5)

        # Seek to the future
        self.rpk.group_seek_to(group_1, str(int(time.time()) + 10))
        rpk_group = self.rpk.group_describe(group_1)
        self.validate_partition(rpk_group.partitions[0], 10, 0, 10, 0)
