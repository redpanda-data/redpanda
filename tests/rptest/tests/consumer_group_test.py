# Copyright 2020 Redpanda Data, Inc.
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import asyncio
from contextlib import closing
import random
import threading
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import Dict, List

import kafka.protocol.types as types
import pytest
from confluent_kafka import (
    Consumer,
    ConsumerGroupState,
    ConsumerGroupTopicPartitions,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient
from ducktape.mark import ignore, parametrize
from ducktape.utils.util import wait_until
from kafka import KafkaConsumer
from kafka import errors as kerr
from kafka.admin import KafkaAdminClient
from kafka.protocol.api import Request, Response
from kafka.protocol.commit import OffsetFetchRequest_v3

from rptest.clients.default import DefaultClient
from rptest.clients.kcl import RawKCL
from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (
    RESTART_LOG_ALLOW_LIST,
    LoggingConfig,
    MetricsEndpoint,
    RedpandaService,
)
from rptest.services.rpk_producer import RpkProducer
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from rptest.utils.mode_checks import skip_debug_mode


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
                "default_topic_replications": 3,
            },
            **kwargs,
        )

        self.rpk = RpkTool(self.redpanda)
        self.kcl = RawKCL(self.redpanda)

    def make_consumer_properties(base_properties, instance_id=None):
        properties = {}
        properties.update(base_properties)
        if instance_id:
            properties["group.instance.id"] = instance_id
        return properties

    def create_consumer(
        self, topic, group, instance_name, instance_id=None, consumer_properties={}
    ):
        return KafkaCliConsumer(
            self.test_context,
            self.redpanda,
            topic=topic,
            group=group,
            from_beginning=True,
            instance_name=instance_name,
            formatter_properties={
                "print.value": "false",
                "print.key": "false",
                "print.partition": "true",
                "print.offset": "true",
            },
            consumer_properties=ConsumerGroupTest.make_consumer_properties(
                consumer_properties, instance_id
            ),
        )

    def create_consumers(
        self,
        consumer_count,
        topic,
        group,
        static_members,
        consumer_properties={},
        err_msg="",
    ):
        consumers = []
        for i in range(0, consumer_count):
            instance_id = f"panda-consumer-{i}" if static_members else None
            consumers.append(
                self.create_consumer(
                    topic,
                    group=group,
                    instance_id=instance_id,
                    instance_name=f"cli-consumer-{i}",
                    consumer_properties=consumer_properties,
                )
            )

        for c in consumers:
            c.start()
        wait_until(self.co_topic_is_ready, 10, 1)

        rpk = RpkTool(self.redpanda)

        def group_is_ready():
            gr = rpk.group_describe(group=group, summary=True)
            return gr.members == consumer_count and gr.state == "Stable"

        wait_until(group_is_ready, 60, 1, err_msg)
        return consumers

    def co_topic_is_ready(self):
        return len(self.client().describe_topic("__consumer_offsets").partitions) > 0

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
                assert "panda-consumer" in p.instance_id
            else:
                assert p.instance_id is None

    def create_topic(self, p_cnt):
        # create topic
        self.topic_spec = TopicSpec(partition_count=p_cnt, replication_factor=3)

        self.client().create_topic(specs=self.topic_spec)

    def start_producer(self, msg_cnt=5000):
        # produce some messages to the topic
        self.producer = RpkProducer(
            self._ctx, self.redpanda, self.topic_spec.name, 128, msg_cnt, -1
        )
        self.producer.start()

    @cluster(num_nodes=6)
    @parametrize(static_members=True)
    @parametrize(static_members=False)
    def test_basic_group_join(self, static_members):
        """
        Test validating that consumers are able to join the group and consume topic
        """
        self.create_topic(20)
        group = "test-gr-1"

        # use 2 consumers
        consumers = self.create_consumers(
            2, self.topic_spec.name, group, static_members=static_members
        )

        self.start_producer()
        # wait for some messages
        wait_until(
            lambda: ConsumerGroupTest.group_consumed_at_least(
                consumers, 50 * len(consumers)
            ),
            30,
            2,
        )
        self.validate_group_state(
            group, expected_state="Stable", static_members=static_members
        )

        self.producer.wait()
        self.producer.free()

        for c in consumers:
            c.stop()
            c.wait()
            c.free()

        gd = RpkTool(self.redpanda).group_describe(group=group)
        viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.nodes:
            consumer_offsets_partitions = viewer.read_consumer_offsets(node=node)
            offsets = {}
            groups = set()
            for partition, records in consumer_offsets_partitions.items():
                self.logger.debug(
                    f"processing partition: {partition}, records: {len(records)}"
                )
                for r in records:
                    self.logger.info(f"{r}")
                    if r["key"]["type"] == "group_metadata":
                        groups.add(r["key"]["group_id"])
                    elif r["key"]["type"] == "offset_commit":
                        tp = f"{r['key']['topic']}/{r['key']['partition']}"
                        if tp not in offsets:
                            offsets[tp] = -1
                        offsets[tp] = max(r["val"]["committed_offset"], offsets[tp])

            assert len(groups) == 1 and group in groups
            assert all(
                [
                    f"{p.topic}/{p.partition}" in offsets
                    and offsets[f"{p.topic}/{p.partition}"] == p.current_offset
                    for p in gd.partitions
                ]
            )

    @cluster(num_nodes=6)
    def test_mixed_consumers_join(self):
        """
        Test validating that dynamic and static consumers may exists in the same group
        """
        self.create_topic(20)
        group = "test-gr-1"
        consumers = []
        consumers.append(
            self.create_consumer(
                topic=self.topic_spec.name,
                group=group,
                instance_name="static-consumer",
                instance_id="panda-instance",
            )
        )
        consumers.append(
            self.create_consumer(
                topic=self.topic_spec.name,
                group=group,
                instance_name="dynamic-consumer",
                instance_id=None,
            )
        )

        for c in consumers:
            c.start()
        self.start_producer()
        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50), 30, 2)

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

        wait_until(self.co_topic_is_ready, 10, 1)

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

        group = "test-gr-1"

        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 40000},
        )
        self.start_producer()
        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50), 30, 2)
        rpk = RpkTool(self.redpanda)
        # at this point we have 2 consumers in stable group
        self.validate_group_state(
            group, expected_state="Stable", static_members=static_members
        )

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
            self.validate_group_state(
                group, expected_state="Stable", static_members=static_members
            )
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
        group = "test-gr-1"
        # using short session timeout to make the test finish faster
        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 6000},
        )

        self.start_producer()
        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50), 30, 2)
        # at this point we have 2 consumers in stable group
        self.validate_group_state(
            group, expected_state="Stable", static_members=static_members
        )

        # stop one of the consumers
        consumers[0].stop()

        # wait for rebalance
        self.wait_for_members(group, 1)

        # start the consumer again
        consumers[0].start()

        # group should get back to its original 2 members state
        self.wait_for_members(group, 2)
        self.validate_group_state(
            group, expected_state="Stable", static_members=static_members
        )

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

        group_id = "test-gr-1"

        # Consume all messages and commit offsets.
        self.consumer = VerifiableConsumer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic_spec.name,
            group_id=group_id,
            max_messages=1000,
        )
        self.consumer.start()
        self.consumer.wait()

        test_admin = KafkaTestAdminClient(self.redpanda)
        offsets = test_admin.list_offsets(
            group_id, [TopicPartition(self.topic_spec.name, 0)]
        )

        # Test that the consumer committed what we expected.
        self.logger.info(f"Got offsets: {offsets}")
        assert len(offsets) == 1
        assert offsets[TestTopicPartition(self.topic_spec.name, 0)].offset == 1000
        assert offsets[TestTopicPartition(self.topic_spec.name, 0)].leader_epoch > 0

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
                    group_id, [TopicPartition(self.topic_spec.name, 0)]
                )
            except Exception as e:
                self.logger.debug(f"Failed to list offsets: {e}")
                return None

        offsets = wait_until_result(
            try_list_offsets,
            timeout_sec=30,
            backoff_sec=3,
            err_msg="Failed to make list_offsets request",
        )

        self.logger.info(f"Got offsets after restart: {offsets}")
        assert len(offsets) == 1
        assert offsets == prev_offsets, f"Expected {prev_offsets}, got {offsets}."

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(static_members=True)
    @parametrize(static_members=False)
    def test_dead_group_recovery(self, static_members):
        """
        Test validating that all offsets persisted in the group are removed when corresponding partition is removed.
        """
        group = "test-gr-1"
        self.create_topic(20)

        # using short session timeout to make the test finish faster
        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=static_members,
            consumer_properties={"session.timeout.ms": 6000},
        )

        self.start_producer()
        # wait for some messages
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 50), 30, 2)
        # at this point we have stable group
        self.validate_group_state(
            group, expected_state="Stable", static_members=static_members
        )

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
            except RpkException:
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
            consumer_properties={"session.timeout.ms": 6000},
        )

        self.start_producer()
        wait_until(lambda: ConsumerGroupTest.consumed_at_least(consumers, 2000), 30, 2)
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

        with closing(asyncio.new_event_loop()) as ev_loop:

            def poll_once(i):
                try:
                    consumer = KafkaConsumer(
                        group_id=f"g-{i}",
                        bootstrap_servers=self.redpanda.brokers(),
                        enable_auto_commit=True,
                        client_id=f"python-consumer-client-{i}",
                    )
                    try:
                        consumer.subscribe([self.topic_spec.name])
                        consumer.poll(1)
                    finally:
                        consumer.close(autocommit=True)
                except Exception as e:
                    self.logger.error(f"Failed to poll group g-{i}: {e}")
                    raise

            async def create_groups(r):
                results = await asyncio.gather(
                    *[
                        asyncio.to_thread(poll_once, i + r * groups_in_round)
                        for i in range(groups_in_round)
                    ],
                    return_exceptions=True,
                )
                for res in results:
                    if isinstance(res, BaseException):
                        raise res

            for r in range(rounds):
                ev_loop.run_until_complete(create_groups(r))

        rpk = RpkTool(self.redpanda)
        list = rpk.group_list_names()

        assert len(list) == groups_in_round * rounds

        # restart redpanda and check recovery
        self.redpanda.restart_nodes(self.redpanda.nodes)

        list = rpk.group_list_names()

        assert len(list) == groups_in_round * rounds

    @cluster(num_nodes=5)
    def test_consumer_static_member_update(self):
        """
        Test validating that re-joining static member will update the client id
        """
        self.create_topic(20)

        group = "test-gr-1"

        rpk = RpkTool(self.redpanda)

        # create and start first consumer
        consumer1 = self.create_consumer(
            topic=self.topic_spec.name,
            group=group,
            instance_name="static-consumer",
            instance_id="panda-instance",
            consumer_properties={"client.id": "my-client-1"},
        )

        consumer1.start()
        wait_until(self.co_topic_is_ready, 10, 1)

        self.wait_for_members(group, 1)

        # wait for some messages
        self.start_producer()
        wait_until(
            lambda: ConsumerGroupTest.consumed_at_least([consumer1], 50),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="consumer1 did not consume messages",
        )

        # validate initial state
        rpk_group_1 = rpk.group_describe(group)

        assert rpk_group_1.state == "Stable", f"Describe: {rpk_group_1}"
        assert rpk_group_1.members == 1, f"Describe: {rpk_group_1}"
        for p in rpk_group_1.partitions:
            assert p.client_id == "my-client-1", f"Describe: {p}"

        # clean up
        self.producer.wait()
        self.producer.free()

        consumer1.stop()
        consumer1.wait()
        consumer1.free()

        # create and start consumer with same instance_id but different cliend_id
        consumer2 = self.create_consumer(
            topic=self.topic_spec.name,
            group=group,
            instance_name="static-consumer",
            instance_id="panda-instance",
            consumer_properties={"client.id": "my-client-2"},
        )

        consumer2.start()

        self.wait_for_members(group, 1)

        # wait for some messages
        self.start_producer()
        wait_until(
            lambda: ConsumerGroupTest.consumed_at_least([consumer2], 50),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="consumer2 did not consume messages",
        )

        # validate updated state
        rpk_group_2 = rpk.group_describe(group)

        assert rpk_group_2.state == "Stable", f"Describe: {rpk_group_2}"
        assert rpk_group_2.members == 1, f"Describe: {rpk_group_2}"
        for p in rpk_group_2.partitions:
            assert p.client_id == "my-client-2", f"Describe: {p}"

        # clean up
        consumer2.stop()
        consumer2.wait()
        consumer2.free()

        self.producer.wait()
        self.producer.free()

    @cluster(num_nodes=5)
    def test_last_member_expiry_with_pending_member(self):
        """
        Regression test to demonstrate the behaviour in the case when the last
        member of the consumer group expires while there are pending members in
        the group
        """
        self.create_topic(20)
        group = "test-gr-1"

        self.redpanda._admin.set_log_level("kafka", "trace")
        self.redpanda._admin.set_log_level("kafka-cg", "trace")

        self.redpanda.logger.info("Starting my-consumer-1")
        consumer1 = self.create_consumer(
            topic=self.topic_spec.name,
            group=group,
            instance_name="static-consumer",
            instance_id="panda-instance",
            consumer_properties={"client.id": "client-1", "session.timeout.ms": 10000},
        )
        consumer1.start()

        self.redpanda.logger.info("Starting producer")
        self.start_producer()

        self.redpanda.logger.info(
            "Waiting for the consumer group to become Stable and make progress"
        )
        self.wait_for_members(group, 1)
        wait_until(
            lambda: ConsumerGroupTest.consumed_at_least([consumer1], 50),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="consumer-1 did not consume messages",
        )

        self.redpanda.logger.info(
            "Stop consumer-1, without sending LeaveGroupReq since it is a static consumer"
        )
        consumer1.stop()
        consumer1.wait()
        consumer1.free()

        self.redpanda.logger.info("Send a JoinGroupReq without completing the join")
        # This simulates the first JoinGroupRequest a new consumer sends. It
        # does not include a member id, so the broker responds with a
        # member_id_required error and **adds this consumer to the list of
        # pending members**
        resp = self.kcl.raw_join_group(
            {
                "Version": 5,
                "Group": group,
                "SessionTimeoutMillis": 60000,
                "RebalanceTimeoutMillis": 60000,
                "ProtocolType": "consumer",
                "Protocols": [{"Name": "range"}],
            }
        )
        self.redpanda.logger.debug(f"JoinGroupResponse: {resp}")
        member_id_required = 79
        assert resp["ErrorCode"] == member_id_required, (
            f"Unexpected response ErrorCode: {resp}"
        )

        self.redpanda.logger.info("Wait out the session timeout of consumer-1")
        time.sleep(10)

        self.redpanda.logger.info(
            "Verify that there is no regression: removing the expiring consumer doesn't lead to an error log, and transitions the group to Empty"
        )
        # ERROR ... seastar - Timer callback failed: std::runtime_error (no members in group)
        assert self.redpanda.search_log_any("Removing member panda-instance-")
        assert not self.redpanda.search_log_any("Timer callback failed")

        self.redpanda.logger.info("Verify that the group became Empty")

        def group_is_empty():
            res = self.rpk.group_describe(group)
            return res.members == 0 and res.state == "Empty"

        wait_until(
            group_is_empty,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="The consumer group should become Empty when the last member expires",
        )

        self.producer.wait()
        self.producer.free()

    @cluster(num_nodes=6)
    @parametrize(metrics=[])
    @parametrize(metrics=["group"])
    @parametrize(metrics=["partition"])
    @parametrize(metrics=["consumer_lag"])
    @parametrize(metrics=["group", "partition"])
    @parametrize(metrics=["group", "consumer_lag"])
    @parametrize(metrics=["partition", "consumer_lag"])
    @parametrize(metrics=["group", "partition", "consumer_lag"])
    def test_group_metrics(self, metrics):
        """
        Test validating the behavior of group metrics
        """
        # Make a copy as we modify it later
        enabled_group_metrics = metrics[:]

        def flip_option(option):
            if option in enabled_group_metrics:
                enabled_group_metrics.remove(option)
            else:
                enabled_group_metrics.append(option)

        self.redpanda.set_cluster_config(
            {"enable_consumer_group_metrics": enabled_group_metrics}
        )

        self.create_topic(20)
        group = f"test-gr-{'-'.join(metrics)}-{random.randint(1, 1000)}"
        # use 2 consumers
        consumers = self.create_consumers(
            2,
            self.topic_spec.name,
            group,
            static_members=False,
            err_msg=f"Failed to create consumers for group {group}",
        )

        self.start_producer()
        # wait for some messages
        wait_until(
            lambda: ConsumerGroupTest.group_consumed_at_least(
                consumers, 50 * len(consumers)
            ),
            30,
            2,
            "Test setup failed. Waiting on consumers timed out.",
        )
        self.validate_group_state(group, expected_state="Stable", static_members=False)

        metrics = {
            "group": [
                "redpanda_kafka_consumer_group_consumers",
                "redpanda_kafka_consumer_group_topics",
            ],
            "partition": ["redpanda_kafka_consumer_group_committed_offset"],
            "consumer_lag": [
                "redpanda_kafka_consumer_group_lag_max",
                "redpanda_kafka_consumer_group_lag_sum",
            ],
        }

        def get_group_metrics_from_nodes(patterns):
            samples = self.redpanda.metrics_samples(
                patterns, self.redpanda.started_nodes(), MetricsEndpoint.PUBLIC_METRICS
            )
            success = samples is not None and set(samples.keys()) == set(patterns)
            return success

        for option, patterns in metrics.items():
            expected_value = option in enabled_group_metrics
            wait_until(
                lambda: get_group_metrics_from_nodes(patterns) == expected_value,
                30,
                1,
                err_msg=f"Looking for metrics in '{option}'. Timed-out while expecting value '{expected_value}'",
            )

        for option in metrics.keys():
            flip_option(option)

        self.redpanda.set_cluster_config(
            {"enable_consumer_group_metrics": enabled_group_metrics}
        )

        for option, patterns in metrics.items():
            expected_value = option in enabled_group_metrics
            wait_until(
                lambda: get_group_metrics_from_nodes(patterns) == expected_value,
                30,
                1,
                err_msg=f"Looking for metrics in '{option}'. Timed-out while expecting value '{expected_value}'",
            )

        self.producer.wait()
        self.producer.free()

        for c in consumers:
            c.stop()
            c.wait()
            c.free()

    @cluster(num_nodes=3)
    def test_group_lag_metrics(self):
        """
        Test validating the behavior of group lag metrics
        """
        lag_collection_interval = 5
        health_monitor_max_metadata_age = 5
        wait_for_lag_secs = (
            max(lag_collection_interval, health_monitor_max_metadata_age) + 1
        )
        topic_count = 1
        partition_count = 20
        consumer_count = 4
        group = "test-lag-metrics-group"
        # Use a small batch size to ensure that fetches are distributed across all partitions
        batch_size = 1
        produce_msg_cnt_min = 1000
        consume_count = (topic_count * partition_count * produce_msg_cnt_min) // (
            2 * consumer_count
        )

        self.redpanda.set_cluster_config(
            {
                "enable_consumer_group_metrics": ["group", "partition", "consumer_lag"],
                "consumer_group_lag_collection_interval_sec": lag_collection_interval,
                "health_monitor_max_metadata_age": health_monitor_max_metadata_age,
            }
        )

        self.admin_client = AdminClient({"bootstrap.servers": self.redpanda.brokers()})

        topics = [f"test-lag-metrics-topic-{i}" for i in range(topic_count)]

        self.client().create_topic(
            specs=[
                TopicSpec(
                    name=name, partition_count=partition_count, replication_factor=3
                )
                for name in topics
            ]
        )

        def create_consumer(instance_id: int) -> Consumer:
            return Consumer(
                {
                    "group.id": group,
                    "group.instance.id": f"consumer-{instance_id}",
                    "bootstrap.servers": self.redpanda.brokers(),
                    "session.timeout.ms": 10000,
                    "auto.offset.reset": "earliest",
                    "enable.auto.offset.store": True,
                    "enable.auto.commit": False,
                    "max.partition.fetch.bytes": batch_size,
                    "log_level": 7,
                    "debug": "cgrp",
                },
                logger=self.logger,
            )

        consumers = [create_consumer(i) for i in range(consumer_count)]
        for consumer in consumers:
            consumer.subscribe(topics)

        self.logger.info("Waiting for group to become stable")
        wait_until(
            lambda: self.admin_client.describe_consumer_groups(group_ids=[group])[group]
            .result()
            .state
            == ConsumerGroupState.STABLE,
            20,
            1,
            retry_on_exc=True,
            err_msg="Timeout waiting on group to reach stable state",
        )

        produced_offsets = {
            TopicPartition(topic, partition): random.randint(
                produce_msg_cnt_min, produce_msg_cnt_min * 2
            )
            for topic in topics
            for partition in range(partition_count)
        }

        self.logger.info("Producing")

        producer = Producer(
            {
                "bootstrap.servers": self.redpanda.brokers(),
                "batch.size": batch_size,
                "acks": "all",
            }
        )
        for tp, offset in produced_offsets.items():
            self.logger.debug(f"  Producing {tp.topic}/{tp.partition} ({offset} msgs)")
            for i in range(offset):
                producer.produce(
                    tp.topic, partition=tp.partition, key=None, value=f"message-{i}"
                )
            producer.flush()
            self.logger.debug(f"  Produced {tp} - flushed {offset} msgs")

        self.logger.info("Consuming")
        for consumer in consumers:
            consumer.consume(num_messages=consume_count, timeout=10)
            assert len(consumer.assignment()) != 0, (
                "Consumer was not assigned any partitions"
            )
            self.logger.debug("  Consumed")

        self.logger.info("Waiting for lag_metrics")
        time.sleep(wait_for_lag_secs)

        def get_group_metrics_from_nodes():
            metrics = [
                "redpanda_kafka_max_offset",
                "redpanda_kafka_consumer_group_committed_offset",
                "redpanda_kafka_consumer_group_lag_max",
                "redpanda_kafka_consumer_group_lag_sum",
            ]
            return self.redpanda.metrics_samples(
                metrics, self.redpanda.started_nodes(), MetricsEndpoint.PUBLIC_METRICS
            )

        def metrics_committed(metrics):
            return [
                s.value
                for s in metrics["redpanda_kafka_consumer_group_committed_offset"]
                .label_filter({"redpanda_group": group})
                .samples
            ]

        def metrics_hwm(metrics):
            hwm_by_tp = {}
            for s in metrics["redpanda_kafka_max_offset"].samples:
                if s.labels["redpanda_topic"] in topics:
                    key = tuple((k, v) for k, v in s.labels.items() if k != "node")
                    hwm_by_tp.setdefault(key, []).append(s.value)
            return [max(hwm) for hwm in hwm_by_tp.values()]

        def metrics_lag_sum(metrics):
            # Arbitrarily reduce across nodes with max, there should be only one
            return max(
                s.value
                for s in metrics["redpanda_kafka_consumer_group_lag_sum"]
                .label_filter({"redpanda_group": group})
                .samples
            )

        def metrics_lag_max(metrics):
            # Arbitrarily reduce across nodes with max, there should be only one
            return max(
                s.value
                for s in metrics["redpanda_kafka_consumer_group_lag_max"]
                .label_filter({"redpanda_group": group})
                .samples
            )

        expected_hwm_sum = sum(produced_offsets.values())
        expected_hwm_len = len(produced_offsets)
        metrics = get_group_metrics_from_nodes()
        hwm_metrics = metrics_hwm(metrics)
        hwm_len = len(hwm_metrics)
        hwm_sum = sum(hwm_metrics)

        assert expected_hwm_len == hwm_len, (
            f"Expected {expected_hwm_len}, got {hwm_len}"
        )
        assert expected_hwm_sum == hwm_sum, f"Expected {0}, got {hwm_sum}"

        # Nothing committed yet, expect no metrics
        with pytest.raises(KeyError):
            metrics_committed(metrics)

        # Consumers that have not committed yet should have no lag
        assert metrics_lag_sum(metrics) == 0
        assert metrics_lag_max(metrics) == 0

        self.logger.info("Committing")
        for consumer in consumers:
            consumer.commit(asynchronous=False)
            self.logger.debug(
                f"  Committed: {consumer.committed(consumer.assignment())}"
            )

        self.logger.info("Waiting for lag_metrics")
        time.sleep(wait_for_lag_secs)

        expected_committed_sum = sum(
            max(0, tp.offset)
            for consumer in consumers
            for tp in consumer.committed(consumer.assignment()) or []
        )
        expected_lag_max = max(
            produced_offsets[TopicPartition(tp.topic, tp.partition)] - tp.offset
            for consumer in consumers
            for tp in (consumer.committed(consumer.assignment()) or [])
        )

        def check_metrics():
            metrics = get_group_metrics_from_nodes()
            committed_metrics = metrics_committed(metrics)
            committed_sum = sum(committed_metrics)
            committed_len = len(committed_metrics)
            hwm_metrics = metrics_hwm(metrics)
            hwm_sum = sum(hwm_metrics)
            hwm_len = len(hwm_metrics)
            lag_sum = metrics_lag_sum(metrics)
            lag_max = metrics_lag_max(metrics)

            self.logger.debug(f"Expected HWM sum: {expected_hwm_sum}")
            self.logger.debug(f"Expected committed sum: {expected_committed_sum}")
            self.logger.debug(f"Metrics HWM sum: {hwm_sum}")
            self.logger.debug(f"Metrics committed sum: {committed_sum}")
            self.logger.debug(
                f"Expected lag: {expected_hwm_sum - expected_committed_sum}"
            )
            self.logger.debug(f"Calculated lag: {hwm_sum - committed_sum}")
            self.logger.debug(f"Metrics lag sum: {lag_sum}")
            self.logger.debug(f"Metrics lag max: {lag_max}")

            assert expected_hwm_len == committed_len, (
                f"Expected {expected_hwm_len}, got {committed_len}. Not all partitions were consumed, tweak the produce and consume counts"
            )
            assert expected_hwm_len == hwm_len, (
                f"Expected {expected_hwm_len}, got {hwm_len}. Not all partitions were consumed, tweak the produce and consume counts"
            )

            # Check redpanda_kafka_max_offset
            assert expected_hwm_sum == hwm_sum, (
                f"Expected {expected_hwm_sum}, got {hwm_sum}"
            )
            # Check redpanda_kafka_consumer_group_committed_offset
            assert expected_committed_sum == committed_sum, (
                f"Expected {expected_committed_sum}, got {committed_sum}"
            )
            # Check redpanda_kafka_consumer_group_lag_sum
            assert hwm_sum - committed_sum == lag_sum, (
                f"Expected {hwm_sum - committed_sum}, got {lag_sum}"
            )
            # Check redpanda_kafka_consumer_group_lag_max
            assert expected_lag_max == lag_max, (
                f"Expected {expected_lag_max}, got {lag_max}"
            )

        check_metrics()

        admin = Admin(self.redpanda)

        def move_partition(topic, partition):
            moved = admin.transfer_leadership_to(
                namespace="kafka", topic=topic, partition=partition, target_id=None
            )
            assert moved, "Failed to move leader"
            return moved

        def get_coordinator():
            return (
                self.admin_client.describe_consumer_groups(group_ids=[group])[group]
                .result()
                .coordinator
            )

        coordinator = get_coordinator()
        moved = move_partition(topic="__consumer_offsets", partition=0)
        assert moved, "Failed to move coordinator"
        wait_until(
            lambda: self.admin_client.describe_consumer_groups(group_ids=[group])[group]
            .result()
            .state
            == ConsumerGroupState.STABLE,
            20,
            1,
            retry_on_exc=True,
            err_msg="Timeout waiting on group to reach stable state",
        )

        assert get_coordinator() != coordinator, "Coordinator did not change"

        self.logger.info("Waiting for lag_metrics after coordinator move")
        time.sleep(wait_for_lag_secs)
        check_metrics()

        moved = move_partition(topic=topics[0], partition=0)
        assert moved, "Failed to move partition leader"

        self.logger.info("Waiting for lag_metrics after partition leader move")
        time.sleep(wait_for_lag_secs)
        check_metrics()

        for consumer in consumers:
            consumer.close()


@dataclass
class OffsetAndMetadata:
    offset: int
    leader_epoch: int
    metadata: str


TestTopicPartition = namedtuple("TestTopicPartition", ["topic", "partition"])


class KafkaTestAdminClient:
    """
    A wrapper around KafkaAdminClient with support for newer Kafka versions.
    At the time of writing, KafkaAdminClient doesn't support KIP-320
    (leader epoch) for consumer groups.
    """

    def __init__(self, redpanda: RedpandaService):
        self._bootstrap_servers = redpanda.brokers()
        self._admin = KafkaAdminClient(bootstrap_servers=self._bootstrap_servers)

    def list_offsets(
        self, group_id: str, partitions: List[TopicPartition]
    ) -> Dict[TestTopicPartition, OffsetAndMetadata]:
        coordinator = self._admin._find_coordinator_ids([group_id])[group_id]
        future = self._list_offsets_send_request(group_id, coordinator, partitions)
        self._admin._wait_for_futures([future])
        response = future.value
        return self._list_offsets_send_process_response(response)

    def _list_offsets_send_request(
        self, group_id: str, coordinator: int, partitions: List[TopicPartition]
    ):
        request = OffsetFetchRequest_v5(
            consumer_group=group_id,
            topics=[(p.topic, [p.partition]) for p in partitions],
        )
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
                    offset, leader_epoch, metadata
                )
        return offsets


class OffsetFetchResponse_v5(Response):
    API_KEY = 9
    API_VERSION = 5
    SCHEMA = types.Schema(
        ("throttle_time_ms", types.Int32),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("offset", types.Int64),
                        ("leader_epoch", types.Int32),
                        ("metadata", types.String("utf-8")),
                        ("error_code", types.Int16),
                    ),
                ),
            ),
        ),
        ("error_code", types.Int16),
    )


class OffsetFetchRequest_v5(Request):
    API_KEY = 9
    API_VERSION = 5
    RESPONSE_TYPE = OffsetFetchResponse_v5
    SCHEMA = OffsetFetchRequest_v3.SCHEMA


class TestConsumer:
    def __init__(
        self, bootstrap_servers, group, topic, id, logger, session_timeout_ms=10000
    ):
        self.bootstrap_servers = bootstrap_servers
        self.id = id
        self.group = group
        self.topic = topic
        self.consumer_thread = threading.Thread(
            name=f"consumer-{id}", target=lambda this: this.loop(), args=[self]
        )
        self.stopped = threading.Event()
        self.restart = threading.Event()
        self.logger = logger
        self.consumer_thread.daemon = True
        self.last_consumed = -1
        self.lock = threading.Lock()
        self.restarted = threading.Event()
        self.session_timeout_ms = session_timeout_ms
        self.consumer_thread.start()

    def stop(self):
        self.logger.info(f"stopping consumer with id: {self.id}")
        self.stopped.set()
        self.consumer_thread.join()

    def is_stopped(self):
        return self.stopped.is_set()

    def create_consumer_client(self):
        self.consumer = Consumer(
            {
                "group.id": self.group,
                "group.instance.id": f"consumer-{self.id}",
                "bootstrap.servers": self.bootstrap_servers,
                "session.timeout.ms": self.session_timeout_ms,
                "auto.offset.reset": "earliest",
                "enable.auto.offset.store": True,
                "enable.auto.commit": False,
                "log_level": 7,
                "debug": "cgrp",
            },
            logger=self.logger,
        )
        self.consumer.subscribe([self.topic])

    def loop(self):
        self.create_consumer_client()
        self.logger.info(f"starting consumer with id: {self.id}")
        while not self.stopped.is_set():
            if self.restart.is_set():
                self.logger.info(f"restarting consumer with id: {self.id}")
                self.consumer.close()
                self.create_consumer_client()
                self.consumer.poll(0.5)
                self.restart.clear()
                self.restarted.set()

            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.logger.error(f"consumer {self.id} error - {msg.error()}")
                    continue

                with self.lock:
                    self.last_consumed = msg.offset()

            except Exception as e:
                self.logger.error(f"consumer {self.id} error - {e}")
        self.logger.info(f"closing consumer with id: {self.id}")
        self.consumer.close()

    def get_last_consumed(self):
        with self.lock:
            return self.last_consumed

    def restart_consumer(self):
        self.restart.set()
        self.restarted.wait()
        self.restarted.clear()


class ConsumerGroupStaticMembersRebalance(RedpandaTest):
    def __init__(self, test_context):
        super(ConsumerGroupStaticMembersRebalance, self).__init__(
            test_context=test_context, num_brokers=3
        )
        self.installer = self.redpanda._installer

    def get_group_description(self):
        description = self.admin_client.describe_consumer_groups(
            group_ids=[self.group_id]
        )[self.group_id].result()
        return description

    @cluster(num_nodes=4)
    @skip_debug_mode
    def verify_consumer_group_state_after_action(
        self, disturbance_action, post_rebalance_check, consumer_session_timeout=10000
    ):
        self.consumer_count = 120
        topic = TopicSpec(name="test-topic-1", partition_count=self.consumer_count)
        DefaultClient(self.redpanda).create_topic(topic)
        self.group_id = "test-group-1"

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=128,
            msg_count=5000000,
        )
        producer.start()
        self.consumers: list[TestConsumer] = []

        for c_id in range(self.consumer_count):
            self.consumers.append(
                TestConsumer(
                    bootstrap_servers=self.redpanda.brokers(),
                    group=self.group_id,
                    topic=topic.name,
                    id=c_id,
                    logger=self.logger,
                    session_timeout_ms=consumer_session_timeout,
                )
            )

        self.admin_client = AdminClient({"bootstrap.servers": self.redpanda.brokers()})

        def consumers_made_progress():
            return all(c.get_last_consumed() >= 0 for c in self.consumers)

        def snapshot_consumers_state():
            return {c.id: c.get_last_consumed() for c in self.consumers}

        wait_until(consumers_made_progress, 60, 1)
        progress_snapshot = snapshot_consumers_state()

        state_before = self.get_group_description()
        self.logger.info("group state before restart: %s", state_before.state)
        assert state_before.state == ConsumerGroupState.STABLE
        assert len(state_before.members) == self.consumer_count

        disturbance_action()

        def group_is_in_stable_state():
            gr = self.get_group_description()
            return gr.state == ConsumerGroupState.STABLE

        wait_until(
            group_is_in_stable_state,
            60,
            0.2,
            retry_on_exc=True,
            err_msg="Timeout waiting on group to reach stable state",
        )
        self.logger.info("group rebalanced, waiting for progress")

        def all_consumers_made_progress():
            return all(
                c.get_last_consumed() > progress_snapshot[c.id]
                for c in self.consumers
                if not c.is_stopped()
            )

        wait_until(
            all_consumers_made_progress,
            60,
            0.5,
            err_msg="Timeout waiting for all consumers to make progress",
        )

        assert post_rebalance_check(), "post rebalance check failed"

        producer.stop()
        for c in self.consumers:
            c.stop()

    @cluster(num_nodes=4)
    @skip_debug_mode
    def test_static_member_rejoining_group(self):
        def restart_then_stop_consumer():
            consumer_to_restart = random.choice(self.consumers)
            last_consumed = consumer_to_restart.get_last_consumed()
            consumer_to_restart.restart_consumer()
            after_restart = self.get_group_description()
            self.logger.info("group state after restart: %s", after_restart.state)
            assert after_restart.state == ConsumerGroupState.STABLE
            assert len(after_restart.members) == self.consumer_count

            wait_until(
                lambda: consumer_to_restart.get_last_consumed() > last_consumed, 60, 1
            )
            # now stop consumer, after a timeout group should rebalance and the member should be removed
            consumer_to_restart.stop()

            def group_started_rebalance():
                gr = self.get_group_description()
                return gr.state == ConsumerGroupState.PREPARING_REBALANCING

            wait_until(group_started_rebalance, 60, 0.2, retry_on_exc=True)

        def verify_consumer_is_missing():
            gr = self.get_group_description()
            self.logger.info(
                "post test group state: %s, members count: %s",
                gr.state,
                len(gr.members),
            )
            return len(gr.members) == self.consumer_count - 1

        self.verify_consumer_group_state_after_action(
            restart_then_stop_consumer,
            verify_consumer_is_missing,
            consumer_session_timeout=10000,
        )

    # this test fails as the consumer are fenced when Redpanda is
    @ignore
    @cluster(num_nodes=4)
    @skip_debug_mode
    def test_force_kill_all_redpanda_nodes(self):
        def restart_then_stop_consumer():
            self.logger.info("stopping redpanda")
            for n in self.redpanda.nodes:
                self.redpanda.stop_node(n)
            time.sleep(10)
            self.logger.info("starting redpanda")
            for n in self.redpanda.nodes:
                self.redpanda.start_node(n)

        def verify_all_consumers_are_present():
            gr = self.get_group_description()
            self.logger.info(
                "post test group state: %s, members count: %d",
                gr.state,
                len(gr.members),
            )
            return len(gr.members) == self.consumer_count

        self.verify_consumer_group_state_after_action(
            restart_then_stop_consumer,
            verify_all_consumers_are_present,
            consumer_session_timeout=10000,
        )


class OffsetCommitter:
    def __init__(self, bootstrap_servers, group, topic, id, logger, partition_id: int):
        self.bootstrap_servers = bootstrap_servers
        self.id = id
        self.group = group
        self.topic = topic
        self.consumer_thread = threading.Thread(
            name=f"consumer-{id}", target=lambda this: this.loop(), args=[self]
        )
        self.stopped = threading.Event()

        self.logger = logger
        self.consumer_thread.daemon = True
        self.last_committed = -1
        self.lock = threading.Lock()
        self.restarted = threading.Event()
        self.partition_id = partition_id
        self.next = 0
        self.consumer_thread.start()

    def stop(self):
        self.logger.info(f"stopping consumer with id: {self.id}")
        self.stopped.set()
        self.consumer_thread.join()

    def is_stopped(self):
        return self.stopped.is_set()

    def create_consumer_client(self):
        self.consumer = Consumer(
            {
                "group.id": self.group,
                "client.id": f"consumer-{self.id}",
                "bootstrap.servers": self.bootstrap_servers,
                "auto.offset.reset": "earliest",
                "enable.auto.offset.store": True,
                "enable.auto.commit": False,
                "fetch.wait.max.ms": 50,
            },
            logger=self.logger,
        )

        self.consumer.assign([TopicPartition(self.topic, self.partition_id)])

    def loop(self):
        self.create_consumer_client()
        self.logger.info(f"starting consumer with id: {self.id}")
        while not self.stopped.is_set():
            try:
                to_commit = self.next
                self.next += 1
                ret = self.consumer.commit(
                    offsets=[
                        TopicPartition(self.topic, self.partition_id, offset=to_commit)
                    ],
                    asynchronous=False,
                )
                with self.lock:
                    self.last_committed = ret[0].offset
            except Exception as e:
                self.logger.error(f"consumer {self.id} error - {e}")
        self.logger.info(f"closing consumer with id: {self.id}")
        self.consumer.close()

    def get_last_committed(self):
        with self.lock:
            return self.last_committed


class ConsumerGroupOffsetResetTest(RedpandaTest):
    def __init__(self, test_context):
        super(ConsumerGroupOffsetResetTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                "group_topic_partitions": 1,
                "compacted_log_segment_size": 1024 * 1024,
                "log_compaction_interval_ms": 1000,
                "group_offset_retention_sec": None,
            },
            log_config=LoggingConfig(
                "info",
                {
                    "storage": "warn",
                    "storage-resources": "warn",
                    "storage-gc": "warn",
                    "raft": "debug",
                    "cluster": "debug",
                },
            ),
        )

    def get_group_description(self):
        description = self.admin_client.describe_consumer_groups(
            group_ids=[self.group_id]
        )[self.group_id].result()
        return description

    def list_consumer_group_offsets(self, topic):
        topic_partitions = [
            TopicPartition(topic, p) for p in range(self.consumer_count)
        ]
        cg_tp = ConsumerGroupTopicPartitions(
            self.group_id, topic_partitions=topic_partitions
        )
        offsets = self.admin_client.list_consumer_group_offsets([cg_tp])

        return offsets[self.group_id].result()

    def total_committed(self):
        return sum([c.get_last_committed() for c in self.consumers])

    def wait_for_total_commits(self, total_commits):
        last_total = 0
        while self.total_committed() < total_commits:
            wait_until(
                lambda: self.total_committed() > last_total,
                60,
                2,
                "Timeout waiting for consumers to make progress committing offsets",
            )
            last_total = self.total_committed()
            self.logger.debug(f"Total offsets committed: {last_total}")
            time.sleep(5)

    @cluster(num_nodes=3)
    @skip_debug_mode
    def test_stress_consumer_group_commits(self):
        """
        This tests simulates a large number of consumers trying to commit consumer
        group offsets. The test doesn't produce or fetch any messages,
        it just stress tests OffsetCommit requests and validates the final
        state of the consumer group.
        """
        self.consumer_count = 200

        topic = TopicSpec(name="cg-test-topic-1", partition_count=self.consumer_count)
        DefaultClient(self.redpanda).create_topic(topic)
        self.group_id = "test-group-1"

        self.consumers: list[OffsetCommitter] = []

        for c_id in range(self.consumer_count):
            self.consumers.append(
                OffsetCommitter(
                    bootstrap_servers=self.redpanda.brokers(),
                    group=self.group_id,
                    topic=topic.name,
                    id=c_id,
                    logger=self.logger,
                    partition_id=c_id,
                )
            )

        self.admin_client = AdminClient({"bootstrap.servers": self.redpanda.brokers()})
        total_commits = 2_000_000
        self.wait_for_total_commits(total_commits)

        rp_admin = Admin(self.redpanda)
        for i in range(3):
            # transfer leadership of __consumer_offsets to a different node
            rp_admin.partition_transfer_leadership("kafka", "__consumer_offsets", 0)
            time.sleep(2)

        wait_until(
            lambda: self.get_group_description().state == ConsumerGroupState.EMPTY,
            timeout_sec=60,
            backoff_sec=1,
            retry_on_exc=True,
        )

        for c in self.consumers:
            c.stop()

        # list committed offsets
        gd = self.list_consumer_group_offsets(topic.name)

        for tp in gd.topic_partitions:
            expected = self.consumers[tp.partition].get_last_committed()
            self.logger.info(
                f"Partition {tp.topic}/{tp.partition} committed offset={tp.offset}"
            )
            assert tp.offset == expected, (
                f"Offset mismatch for partition {tp.topic}/{tp.partition}, expected: {expected} got: {tp.offset}"
            )

        olv = OfflineLogViewer(self.redpanda)

        def get_summary(node):
            try:
                summary = olv.consumer_offsets_summary(node)
            except Exception as e:
                self.logger.debug(
                    f"Caught exception {e} while collecting consumer offsets summary, retrying"
                )
                return False

            for _, cg_partition_summary in summary.items():
                assert len(cg_partition_summary["raft_configurations"]) >= 2, (
                    "There must have been at least 1 leadership change"
                )
                offsets = cg_partition_summary["groups"][self.group_id]["offsets"]
                for p in range(self.consumer_count):
                    expected = self.consumers[p].get_last_committed()
                    committed = offsets[f"{topic.name}/{p}"]["committed_offset"]
                    assert committed == self.consumers[p].get_last_committed(), (
                        f"On disk state mismatch, expected: {expected}, got: {expected}"
                    )
            return True

        for n in self.redpanda.nodes:
            wait_until(
                lambda: get_summary(n),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Failed to get consumer offsets summary for node {n.account.hostname}",
            )

    @cluster(num_nodes=3)
    def test_offset_delete_manual_consumers(self):
        """
        Verify that OffsetDelete requests work as expected when consumers
        are being manually managed.
        """
        # Magic value returned by confluent_kafka when it can't find commited offsets
        INVALID_OFFSET = -1001

        rpk = RpkTool(self.redpanda)

        topic = TopicSpec(name="cg-test-topic-1", partition_count=1)
        DefaultClient(self.redpanda).create_topic(topic)
        self.group_id = "test-group-1"

        self.consumer_count = 1
        self.admin_client = AdminClient({"bootstrap.servers": self.redpanda.brokers()})

        self.consumers = [
            OffsetCommitter(
                bootstrap_servers=self.redpanda.brokers(),
                group=self.group_id,
                topic=topic.name,
                id=0,
                logger=self.logger,
                partition_id=0,
            )
        ]

        self.wait_for_total_commits(1000)

        # No consumers have subscribed. Group should be in empty state
        desc = rpk.group_describe(self.group_id)
        assert desc.state == "Empty", (
            f"Expected group.state 'Empty' but got '{desc.state}', instead\n"
            f"Group description: {desc}"
        )

        # Try to delete offsets while there is an assigned consumer
        # who keeps commiting offsets
        res = rpk.offset_delete(self.group_id, {topic.name: [0]})[0]
        assert res.status == "OK", (
            f"Expected res.status 'OK' but got '{res.status}', instead\n"
            f"Response:  {res}"
        )

        def wait_for_new_commits():
            tp = self.list_consumer_group_offsets(topic.name).topic_partitions[0]
            return tp.offset != INVALID_OFFSET

        wait_until(
            wait_for_new_commits,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Timeout while waiting for consumer to commit more offsets",
        )

        self.consumers[0].stop()

        res = rpk.offset_delete(self.group_id, {topic.name: [0]})
        assert res[0].status == "OK", (
            f"Expected res.status 'OK' but got '{res[0].status}', instead\n"
            f"Response:  {res[0]}"
        )

        tp = self.list_consumer_group_offsets(topic.name).topic_partitions[0]
        assert tp.offset == INVALID_OFFSET, (
            f"Expected offset '{INVALID_OFFSET}' but got '{tp.offset}', instead"
        )
