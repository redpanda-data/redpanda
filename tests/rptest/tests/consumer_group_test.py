# Copyright 2020 Redpanda Data, Inc.
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
from typing import Dict, List

from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.services.cluster import cluster

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, RedpandaService
from rptest.services.rpk_producer import RpkProducer
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from rptest.utils.mode_checks import skip_debug_mode

from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from kafka import KafkaConsumer, TopicPartition
from kafka import errors as kerr
from kafka.admin import KafkaAdminClient
from kafka.protocol.commit import OffsetFetchRequest_v3
from kafka.protocol.api import Request, Response
import kafka.protocol.types as types
from confluent_kafka.admin import AdminClient
from confluent_kafka import ConsumerGroupState


class ConsumerGroupTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        self.producer = None
        super(ConsumerGroupTest, self).__init__(
            test_ctx,
            num_brokers=3,
            *args,
            # disable leader balancer to make sure that group will not be realoaded because of leadership changes
            extra_rp_conf={
                "enable_leader_balancer": False,
                "default_topic_replications": 3
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
                        instance_name,
                        instance_id=None,
                        consumer_properties={}):
        return KafkaCliConsumer(
            self.test_context,
            self.redpanda,
            topic=topic,
            group=group,
            from_beginning=True,
            instance_name=instance_name,
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
                                     instance_name=f"cli-consumer-{i}",
                                     consumer_properties=consumer_properties))

        for c in consumers:
            c.start()
        rpk = RpkTool(self.redpanda)

        def group_is_ready():
            gr = rpk.group_describe(group=group, summary=True)
            return gr.members == consumer_count and gr.state == "Stable"

        wait_until(group_is_ready, 60, 1)
        return consumers

    def consumed_at_least(consumers, count):
        return all([c._message_cnt > count for c in consumers])

    def group_consumed_at_least(consumers, count):
        return sum([c._message_cnt for c in consumers]) >= count

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

    def create_topic(self, p_cnt):
        # create topic
        self.topic_spec = TopicSpec(partition_count=p_cnt,
                                    replication_factor=3)

        self.client().create_topic(specs=self.topic_spec)

    def start_producer(self, msg_cnt=5000):

        # produce some messages to the topic
        self.producer = RpkProducer(self._ctx, self.redpanda,
                                    self.topic_spec.name, 128, msg_cnt, -1)
        self.producer.start()

    @cluster(num_nodes=6)
    @parametrize(static_members=True)
    @parametrize(static_members=False)
    def test_basic_group_join(self, static_members):
        """
        Test validating that consumers are able to join the group and consume topic
        """
        self.create_topic(20)
        group = 'test-gr-1'
        # use 2 consumers
        consumers = self.create_consumers(2,
                                          self.topic_spec.name,
                                          group,
                                          static_members=static_members)

        self.start_producer()
        # wait for some messages
        wait_until(
            lambda: ConsumerGroupTest.group_consumed_at_least(
                consumers, 50 * len(consumers)), 30, 2)
        self.validate_group_state(group,
                                  expected_state="Stable",
                                  static_members=static_members)

        self.producer.wait()
        self.producer.free()

        for c in consumers:
            c.stop()
            c.wait()
            c.free()

        gd = RpkTool(self.redpanda).group_describe(group=group)
        viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.nodes:
            consumer_offsets_partitions = viewer.read_consumer_offsets(
                node=node)
            offsets = {}
            groups = set()
            for partition, records in consumer_offsets_partitions.items():
                self.logger.debug(
                    f"processing partition: {partition}, records: {len(records)}"
                )
                for r in records:
                    self.logger.info(f"{r}")
                    if r['key']['type'] == 'group_metadata':
                        groups.add(r['key']['group_id'])
                    elif r['key']['type'] == 'offset_commit':
                        tp = f"{r['key']['topic']}/{r['key']['partition']}"
                        if tp not in offsets:
                            offsets[tp] = -1
                        offsets[tp] = max(r['val']['committed_offset'],
                                          offsets[tp])

            assert len(groups) == 1 and group in groups
            assert all([
                f"{p.topic}/{p.partition}" in offsets
                and offsets[f"{p.topic}/{p.partition}"] == p.current_offset
                for p in gd.partitions
            ])

    @cluster(num_nodes=6)
    def test_mixed_consumers_join(self):
        """
        Test validating that dynamic and static consumers may exists in the same group
        """
        self.create_topic(20)
        group = 'test-gr-1'
        consumers = []
        consumers.append(
            self.create_consumer(topic=self.topic_spec.name,
                                 group=group,
                                 instance_name="static-consumer",
                                 instance_id="panda-instance"))
        consumers.append(
            self.create_consumer(topic=self.topic_spec.name,
                                 group=group,
                                 instance_name="dynamic-consumer",
                                 instance_id=None))

        for c in consumers:
            c.start()
        self.start_producer()
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
        self.create_topic(20)

        group = 'test-gr-1'

        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 40000})
        self.start_producer()
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
        self.create_topic(20)
        group = 'test-gr-1'
        # using short session timeout to make the test finish faster
        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 6000})

        self.start_producer()
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

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_group_recovery(self):
        """
        Test validating that group state is recovered after broker restart.
        """
        self.create_topic(1)

        # Produce some messages.
        self.start_producer(msg_cnt=1000)
        self.producer.wait()
        self.producer.free()

        group_id = 'test-gr-1'

        # Consume all messages and commit offsets.
        self.consumer = VerifiableConsumer(self.test_context,
                                           num_nodes=1,
                                           redpanda=self.redpanda,
                                           topic=self.topic_spec.name,
                                           group_id=group_id,
                                           max_messages=1000)
        self.consumer.start()
        self.consumer.wait()

        test_admin = KafkaTestAdminClient(self.redpanda)
        offsets = test_admin.list_offsets(
            group_id, [TopicPartition(self.topic_spec.name, 0)])

        # Test that the consumer committed what we expected.
        self.logger.info(f"Got offsets: {offsets}")
        assert len(offsets) == 1
        assert offsets[TopicPartition(self.topic_spec.name, 0)].offset == 1000
        assert offsets[TopicPartition(self.topic_spec.name,
                                      0)].leader_epoch > 0

        # Remember the old offsets to compare them after the restart.
        prev_offsets = offsets

        # Restart the broker.
        self.logger.info("Restarting redpanda nodes.")
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Validate that the group state is recovered.
        def try_list_offsets():
            try:
                test_admin = KafkaTestAdminClient(self.redpanda)
                return test_admin.list_offsets(
                    group_id, [TopicPartition(self.topic_spec.name, 0)])
            except Exception as e:
                self.logger.debug(f"Failed to list offsets: {e}")
                return None

        offsets = wait_until_result(
            try_list_offsets,
            timeout_sec=30,
            backoff_sec=3,
            err_msg="Failed to make list_offsets request")

        self.logger.info(f"Got offsets after restart: {offsets}")
        assert len(offsets) == 1
        assert offsets == prev_offsets, \
            f"Expected {prev_offsets}, got {offsets}."

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(static_members=True)
    @parametrize(static_members=False)
    def test_dead_group_recovery(self, static_members):
        """
        Test validating that all offsets persisted in the group are removed when corresponding partition is removed.
        """
        group = 'test-gr-1'
        self.create_topic(20)

        # using short session timeout to make the test finish faster
        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 6000})

        self.start_producer()
        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50),
                   30, 2)
        # at this point we have stable group
        self.validate_group_state(group,
                                  expected_state="Stable",
                                  static_members=static_members)

        # stop consumers
        for c in consumers:
            c.stop()
            c.wait()
            c.free()

        consumers.clear()

        rpk = RpkTool(self.redpanda)

        def group_is_empty():
            rpk_group = rpk.group_describe(group)

            return rpk_group.members == 0 and rpk_group.state == "Empty"

        # group should be empty now

        wait_until(group_is_empty, 30, 2)

        # delete topic
        rpk.delete_topic(self.topic_spec.name)

        def group_is_dead():
            try:
                rpk_group = rpk.group_describe(group)
                return rpk_group.members == 0 and rpk_group.state == "Dead"
            except RpkException as e:
                # allow RPK to throw an exception as redpanda nodes were
                # restarted and the request may require a retry
                return False

        wait_until(group_is_dead, 30, 2)
        self.producer.wait()
        self.producer.free()

        # recreate topic
        self.redpanda.restart_nodes(self.redpanda.nodes)
        # after recovery group should still be dead as it was deleted
        wait_until(group_is_dead, 30, 2)

        self.client().create_topic(self.topic_spec)
        # recreate consumers
        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 6000})

        self.start_producer()
        wait_until(
            lambda: ConsumerGroupTest.consumed_at_least(consumers, 2000), 30,
            2)
        for c in consumers:
            c.stop()
            c.wait()
            c.free()
        self.producer.wait()
        self.producer.free()

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_large_group_count(self):
        self.create_topic(20)
        rounds = 10
        groups_in_round = 100

        import asyncio
        ev_loop = asyncio.new_event_loop()

        def poll_once(i):
            consumer = KafkaConsumer(group_id=f"g-{i}",
                                     bootstrap_servers=self.redpanda.brokers(),
                                     enable_auto_commit=True)
            consumer.subscribe([self.topic_spec.name])
            consumer.poll(1)
            consumer.close(autocommit=True)

        async def create_groups(r):
            await asyncio.gather(*[
                asyncio.to_thread(poll_once, i + r * groups_in_round)
                for i in range(groups_in_round)
            ])

        for r in range(rounds):
            ev_loop.run_until_complete(create_groups(r))

        ev_loop.stop()
        ev_loop.close()

        rpk = RpkTool(self.redpanda)
        list = rpk.group_list()

        assert len(list) == groups_in_round * rounds

        # restart redpanda and check recovery
        self.redpanda.restart_nodes(self.redpanda.nodes)

        list = rpk.group_list()

        assert len(list) == groups_in_round * rounds

    @skip_debug_mode
    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_filtering_groups_by_state(self):
        self.create_topic(10)
        rounds = 10
        groups_in_round = 100
        self.start_producer()
        # create group and close consumer, the group will be empty
        consumer = KafkaConsumer(group_id=f"group-empty",
                                 bootstrap_servers=self.redpanda.brokers(),
                                 enable_auto_commit=True)
        consumer.subscribe([self.topic_spec.name])
        for i in range(5):
            consumer.poll(1)
        consumer.close(autocommit=True)
        # create stable group
        consumer_2 = KafkaConsumer(group_id=f"group-stable",
                                   bootstrap_servers=self.redpanda.brokers(),
                                   enable_auto_commit=True)
        consumer_2.subscribe([self.topic_spec.name])
        for i in range(5):
            consumer_2.poll(1)

        admin = AdminClient({"bootstrap.servers": self.redpanda.brokers()})

        non_filtered = admin.list_consumer_groups(states=set()).result().valid
        assert len(non_filtered) == 2

        groups_with_empty_state = admin.list_consumer_groups(
            states={ConsumerGroupState.EMPTY}).result().valid
        assert len(groups_with_empty_state) == 1
        assert all(g.state == ConsumerGroupState.EMPTY
                   for g in groups_with_empty_state)
        groups_with_stable_state = admin.list_consumer_groups(
            states={ConsumerGroupState.STABLE}).result().valid
        assert len(groups_with_stable_state) == 1
        assert all(g.state == ConsumerGroupState.STABLE
                   for g in groups_with_stable_state)
        groups_with_stable_or_empty_state = admin.list_consumer_groups(
            states={ConsumerGroupState.STABLE, ConsumerGroupState.EMPTY
                    }).result().valid
        assert len(groups_with_stable_or_empty_state) == 2


@dataclass
class OffsetAndMetadata():
    offset: int
    leader_epoch: int
    metadata: str


class KafkaTestAdminClient():
    """
    A wrapper around KafkaAdminClient with support for newer Kafka versions.
    At the time of writing, KafkaAdminClient doesn't support KIP-320
    (leader epoch) for consumer groups.
    """
    def __init__(self, redpanda: RedpandaService):
        self._bootstrap_servers = redpanda.brokers()
        self._admin = KafkaAdminClient(
            bootstrap_servers=self._bootstrap_servers)

    def list_offsets(
        self, group_id: str, partitions: List[TopicPartition]
    ) -> Dict[TopicPartition, OffsetAndMetadata]:
        coordinator = self._admin._find_coordinator_ids([group_id])[group_id]
        future = self._list_offsets_send_request(group_id, coordinator,
                                                 partitions)
        self._admin._wait_for_futures([future])
        response = future.value
        return self._list_offsets_send_process_response(response)

    def _list_offsets_send_request(self, group_id: str, coordinator: int,
                                   partitions: List[TopicPartition]):
        request = OffsetFetchRequest_v5(consumer_group=group_id,
                                        topics=[(p.topic, [p.partition])
                                                for p in partitions])
        return self._admin._send_request_to_node(coordinator, request)

    def _list_offsets_send_process_response(self, response):
        error_type = kerr.for_code(response.error_code)
        if error_type is not kerr.NoError:
            raise error_type("Error in list_offsets response")

        offsets = {}
        for topic, partitions in response.topics:
            for partition, offset, leader_epoch, metadata, error_code in partitions:
                if error_code != 0:
                    raise Exception(f"Error code: {error_code}")
                offsets[(topic, partition)] = OffsetAndMetadata(
                    offset, leader_epoch, metadata)
        return offsets


class OffsetFetchResponse_v5(Response):
    API_KEY = 9
    API_VERSION = 5
    SCHEMA = types.Schema(
        ('throttle_time_ms', types.Int32),
        ('topics',
         types.Array(
             ('topic', types.String('utf-8')),
             ('partitions',
              types.Array(('partition', types.Int32), ('offset', types.Int64),
                          ('leader_epoch', types.Int32),
                          ('metadata', types.String('utf-8')),
                          ('error_code', types.Int16))))),
        ('error_code', types.Int16))


class OffsetFetchRequest_v5(Request):
    API_KEY = 9
    API_VERSION = 5
    RESPONSE_TYPE = OffsetFetchResponse_v5
    SCHEMA = OffsetFetchRequest_v3.SCHEMA
