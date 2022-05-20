# Copyright 2020 Redpanda Data, Inc.
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from rptest.services.cluster import cluster

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize


class ConsumerGroupTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        self.producer = None
        super(ConsumerGroupTest, self).__init__(
            test_ctx,
            num_brokers=3,
            *args,
            # disable leader balancer to make sure that group will not be realoaded because of leadership changes
            extra_rp_conf={"enable_leader_balancer": False},
            **kwargs)

    def make_consumer_properties(base_properties, instance_id=None):
        properties = {}
        properties.update(base_properties)
        if instance_id:
            properties['group.instance.id'] = instance_id
        return properties

    def create_consumer(self,
                        topic,
                        group,
                        instance_id=None,
                        consumer_properties={}):
        return KafkaCliConsumer(
            self.test_context,
            self.redpanda,
            topic=topic,
            group=group,
            from_beginning=True,
            formatter_properties={
                'print.value': 'false',
                'print.key': 'false',
                'print.partition': 'true',
                'print.offset': 'true',
            },
            consumer_properties=ConsumerGroupTest.make_consumer_properties(
                consumer_properties, instance_id))

    def create_consumers(self,
                         consumer_count,
                         topic,
                         group,
                         static_members,
                         consumer_properties={}):

        consumers = []
        for i in range(0, consumer_count):
            instance_id = f"panda-consumer-{i}" if static_members else None
            consumers.append(
                self.create_consumer(topic,
                                     group=group,
                                     instance_id=instance_id,
                                     consumer_properties=consumer_properties))

        for c in consumers:
            c.start()
        return consumers

    def consumed_at_least(consumers, count):
        return all([len(c._messages) > count for c in consumers])

    def validate_group_state(self, group, expected_state, static_members):
        rpk = RpkTool(self.redpanda)
        # validate group state
        rpk_group = rpk.group_describe(group)

        assert rpk_group.members == 2
        assert rpk_group.state == expected_state

        for p in rpk_group.partitions:
            if static_members:
                assert 'panda-consumer' in p.instance_id
            else:
                assert p.instance_id is None

    def setup_producer(self, p_cnt):
        # create topic
        self.topic_spec = TopicSpec(partition_count=p_cnt,
                                    replication_factor=3)
        self.client().create_topic(specs=self.topic_spec)
        # produce some messages to the topic
        self.producer = RpkProducer(self._ctx, self.redpanda,
                                    self.topic_spec.name, 128, 5000, -1)
        self.producer.start()

    @cluster(num_nodes=6)
    @parametrize(static_members=True)
    @parametrize(static_members=False)
    def test_basic_group_join(self, static_members):
        """
        Test validating that consumers are able to join the group and consume topic
        """

        self.setup_producer(20)
        group = 'test-gr-1'
        # use 2 consumers
        consumers = self.create_consumers(2,
                                          self.topic_spec.name,
                                          group,
                                          static_members=static_members)

        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50),
                   30, 2)
        self.validate_group_state(group,
                                  expected_state="Stable",
                                  static_members=static_members)

        self.producer.wait()
        self.producer.free()

        for c in consumers:
            c.stop()
            c.wait()
            c.free()

    @cluster(num_nodes=6)
    def test_mixed_consumers_join(self):
        """
        Test validating that dynamic and static consumers may exists in the same group
        """
        self.setup_producer(20)
        group = 'test-gr-1'
        consumers = []
        consumers.append(
            self.create_consumer(self.topic_spec.name, group,
                                 "panda-instance"))
        consumers.append(
            self.create_consumer(self.topic_spec.name, group, None))

        for c in consumers:
            c.start()

        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50),
                   30, 2)

        rpk = RpkTool(self.redpanda)
        # validate group state
        rpk_group = rpk.group_describe(group)

        assert rpk_group.members == 2
        assert rpk_group.state == "Stable"

        static_members = set()
        dynamic_members = set()

        for p in rpk_group.partitions:
            if p.instance_id:
                static_members.add(p.client_id)
            else:
                dynamic_members.add(p.client_id)

        assert len(static_members) == 1
        assert len(dynamic_members) == 1

        self.producer.wait()
        self.producer.free()

        for c in consumers:
            c.stop()
            c.wait()
            c.free()

    def wait_for_members(self, group, members_count):
        rpk = RpkTool(self.redpanda)

        def group_stable():
            rpk_group = rpk.group_describe(group)
            return rpk_group.members == members_count and rpk_group.state == "Stable"

        return wait_until(group_stable, 30, 2)

    @cluster(num_nodes=6)
    @parametrize(static_members=True)
    @parametrize(static_members=False)
    def test_consumer_rejoin(self, static_members):
        """
        Test validating that re-joining static member will not casuse rebalance
        """
        self.setup_producer(20)
        group = 'test-gr-1'

        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 40000})

        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50),
                   30, 2)
        rpk = RpkTool(self.redpanda)
        # at this point we have 2 consumers in stable group
        self.validate_group_state(group,
                                  expected_state="Stable",
                                  static_members=static_members)

        # stop one of the consumers
        consumers[0].stop()
        consumers[0].wait()

        rpk_group = rpk.group_describe(group)
        if static_members:
            # with static members group should still be in stable state
            assert rpk_group.state == "Stable"
            assert rpk_group.members == 2
        else:
            # consumer will request group leave when shutdown gracefully and it is dynamic
            self.wait_for_members(group, 1)

        # start the consumer again
        consumers[0].start()
        consumers[0].wait_for_started()
        # wait for consumer to start
        if static_members:
            # with static members group should be stable immediately as the
            # consumer is rejoining with the same instance id
            self.validate_group_state(group,
                                      expected_state="Stable",
                                      static_members=static_members)
        else:
            # group should get back to its original 2 members state
            self.wait_for_members(group, 2)

        self.producer.wait()
        self.producer.free()

        for c in consumers:
            c.stop()
            c.wait()
            c.free()

    @cluster(num_nodes=6)
    @parametrize(static_members=True)
    @parametrize(static_members=False)
    def test_consumer_is_removed_when_timedout(self, static_members):
        """
        Test validating that consumer is evicted if it failed to deliver heartbeat to the broker
        """
        self.setup_producer(20)
        group = 'test-gr-1'
        # using short session timeout to make the test finish faster
        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 6000})

        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50),
                   30, 2)
        rpk = RpkTool(self.redpanda)
        # at this point we have 2 consumers in stable group
        self.validate_group_state(group,
                                  expected_state="Stable",
                                  static_members=static_members)

        # stop one of the consumers
        consumers[0].stop()

        # wait for rebalance
        self.wait_for_members(group, 1)

        # start the consumer again
        consumers[0].start()

        # group should get back to its original 2 members state
        self.wait_for_members(group, 2)
        self.validate_group_state(group,
                                  expected_state="Stable",
                                  static_members=static_members)

        self.producer.wait()
        self.producer.free()

        for c in consumers:
            c.stop()
            c.wait()
            c.free()
