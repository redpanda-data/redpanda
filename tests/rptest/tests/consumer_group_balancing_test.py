# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.utils.util import wait_until
from xxhash import xxh64
import jump  # package jump-consistent-hash
from math import ceil, floor

cgroup_balancing_test_num_brokers = 3
cgroup_balancing_test_num_groups = 3


class ConsumerGroupBalancingTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        self.producer = None
        super(ConsumerGroupBalancingTest, self).__init__(
            test_ctx,
            num_brokers=cgroup_balancing_test_num_brokers,
            *args,
            extra_rp_conf={
                # disable leader balancer until even distribution control
                # of __consumer_offsets leaders is implemented
                "enable_leader_balancer": False,
                # only partition distribution is tested yet, so we need
                # a sigle replica per __consumer_offsets partition.
                # this will change with implementation of
                # even distribution control of __consumer_offsets leaders
                "internal_topic_replication_factor": 1,
                # a __consumer_offsets partiton per cgroup
                "group_topic_partitions": cgroup_balancing_test_num_groups,
            },
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
        return KafkaCliConsumer(self.test_context,
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
                                consumer_properties=ConsumerGroupBalancingTest.
                                make_consumer_properties(
                                    consumer_properties, instance_id))

    def consumed_at_least(consumers, count):
        return all([len(c._messages) > count for c in consumers])

    def validate_group_state(self,
                             group,
                             expected_state,
                             static_members,
                             group_members: int = 2):
        rpk = RpkTool(self.redpanda)
        # validate group state
        rpk_group = rpk.group_describe(group)

        assert rpk_group.members == group_members
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

    @cluster(num_nodes=cgroup_balancing_test_num_brokers +
             cgroup_balancing_test_num_groups + 1)
    def test_coordinator_nodes_balance(self):
        """
        Test checking that consumer group coordinators are distributed evenly across nodes
        """
        static_members = True
        self.logger.info(
            f"brokers: {len(self.redpanda.cluster_spec)} / {len(self.cluster)}"
        )

        # verify that the names of the groups used will map to different
        # partitions internally
        groups = []
        group_hashes = set()
        partition_ids = set()
        for i in range(cgroup_balancing_test_num_groups):
            group_name = f"cgrp-{i}"  # only tested for 1..3 yet
            group_hash = xxh64(group_name).intdigest()
            assert not group_hash in group_hashes
            partition_id = jump.hash(group_hash,
                                     cgroup_balancing_test_num_groups)
            assert not partition_id in partition_ids
            groups.append(group_name)
            group_hashes.add(group_hash)
            partition_ids.add(partition_id)

        self.logger.info(f"starting producers and consumers, groups: {groups}")

        # partition count in data topics does not matter
        self.setup_producer(1)

        # create 1 consumer per group
        consumers_all = []
        consumers_by_group = {}
        for group in groups:
            instance_id = f"panda-consumer-{group}" if static_members else None
            consumer = self.create_consumer(self.topic_spec.name,
                                            group=group,
                                            instance_id=instance_id)
            consumer.start()
            consumers_by_group[group] = [consumer]
            consumers_all.append(consumer)

        # wait for some messages
        self.logger.info(f"waiting for some messages")
        wait_until(
            lambda: ConsumerGroupBalancingTest.consumed_at_least(
                consumers_all, 50), 30, 2)
        # all groups should be stable
        self.logger.info(f"checking groups are stable")
        for group in groups:
            self.validate_group_state(group,
                                      expected_state="Stable",
                                      static_members=static_members,
                                      group_members=1)
        # stop consumers
        self.logger.info(f"stopping producers and consumers")
        for c in consumers_all:
            c.stop()
        for c in consumers_all:
            c.wait()
            c.free()
        self.producer.wait()
        self.producer.free()

        rpk = RpkTool(self.redpanda)
        coord_groups = {}
        # find coordinator of each group
        for group in groups:
            group_desc = rpk.group_describe(group)
            self.logger.info(f"group {group}: {group_desc}")
            coord_groups[group_desc.coordinator] = coord_groups.get(
                group_desc.coordinator, 0) + 1

        # model the ideal cgroup coordinators distribution
        coord_density = sorted(coord_groups.values(), reverse=True)
        expected_coord_density = [
            ceil(len(groups) / len(self.redpanda.nodes))
            for _ in range(len(groups) % len(self.redpanda.nodes))
        ] + [
            floor(len(groups) / len(self.redpanda.nodes)) for _ in range(
                len(groups) %
                len(self.redpanda.nodes), len(self.redpanda.nodes))
        ]

        # actual distribution must match the model
        if coord_density != expected_coord_density:
            self.logger.error(
                "Uneven distribution of group coordinators across nodes. "
                f"coordinators of groups: {coord_groups}, "
                f"groups: {len(groups)}, brokers: {len(self.redpanda.nodes)}, "
                f"coord_density: {coord_density}, expected_coord_density: {expected_coord_density}"
            )
            raise RuntimeError(
                "Uneven distribution of group coordinators across brokers")
