# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import re
from contextlib import nullcontext

from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.multi_cluster_services import (
    Cluster,
    MultiClusterServices,
    ServiceType,
)
from rptest.tests.cluster_linking_test_base import (
    ShadowLinkPreAllocTestBase,
    ShadowLinkTestBase,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import bg_thread_cm, expect_exception, wait_until, wait_until_result


class MultiClusterTestBase(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, *args, **kwargs)

    def basic_ops(self, services: MultiClusterServices):
        def at_least_one_topic_exists(services: MultiClusterServices, node: Cluster):
            topics = services.list_topics(node, detailed=True)
            return len(topics) > 0, topics

        topic = "test-topic"
        services.create_topic(services.primary, topic, partitions=3, replicas=3)
        p_topics = wait_until_result(
            lambda: at_least_one_topic_exists(services, services.primary),
            timeout_sec=30,
            err_msg="Failed to create a single topic on the primary cluster",
        )

        services.create_topic(services.secondary, topic, partitions=3, replicas=3)
        s_topics = wait_until_result(
            lambda: at_least_one_topic_exists(services, services.secondary),
            timeout_sec=30,
            err_msg="Failed to create a single topic on the secondary cluster",
        )

        assert p_topics == s_topics, (
            f"Expected same topics on both clusters, got {p_topics=} vs {s_topics=}"
        )

        assert len(p_topics) == 1 and p_topics[0][0] == topic, (
            f"Expected {topic=}, got {p_topics=}"
        )

        status_json = services.primary.admin.get_status_ready()
        assert status_json["status"] == "ready", f"Expected ready, got {status_json=}"

        if services.secondary.is_redpanda:
            status_json = services.secondary.admin.get_status_ready()
            assert status_json["status"] == "ready", (
                f"Expected ready, got {status_json=}"
            )
        else:
            with expect_exception(NotImplementedError, lambda e: True):
                services.secondary.admin.get_status_ready()


class MultiClusterRedpandaTest(MultiClusterTestBase):
    """
    Just verifies MultiClusterServices for now. rp + rp & rp + kafka
    """

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, num_brokers=3, *args, **kwargs)

        self.test_context = test_context

    def setUp(self):
        # MultiClusterServices will set itself up
        pass

    @cluster(num_nodes=6)
    def test_basic_ops(self):
        with MultiClusterServices(
            self.test_context,
            self.logger,
            self.redpanda,
            secondary_type=ServiceType.REDPANDA,
            num_brokers=3,
        ) as services:
            assert services.secondary.is_redpanda, (
                f"Expected Redpanda service, got {services.secondary}"
            )
            self.basic_ops(services)


class MultiClusterKafkaTest(MultiClusterTestBase):
    """
    Just verifies MultiClusterServices for now. rp + rp & rp + kafka
    """

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, num_brokers=3, *args, **kwargs)

        self.test_context = test_context

    def setUp(self):
        # MultiClusterServices will set itself up
        pass

    @cluster(num_nodes=7)
    def test_basic_ops(self):
        with MultiClusterServices(
            self.test_context,
            self.logger,
            self.redpanda,
            secondary_type=ServiceType.KAFKA,
            num_brokers=3,
        ) as services:
            assert services.secondary.is_kafka, (
                f"Expected Kafka service, got {services.secondary}"
            )
            self.basic_ops(services)


class ShadowLinkBasicTests(ShadowLinkTestBase):
    @cluster(num_nodes=6)
    def test_create_simple_link(self):
        create_res = self.create_link("test-link")
        self.logger.info(f"Create shadow link result: {create_res.shadow_link}")

        links = self.list_links()
        assert len(links) == 1, f"Expected exactly one shadow link, got {len(links)}"
        assert links[0].name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {links[0].name}"
        )

        got_link = self.get_link(name="test-link")
        assert got_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {got_link.name}"
        )

        try:
            self.get_link(name="non-existent-link")
            assert False, "Should not have gotten a non-existent link"
        except ConnectError as e:
            assert e.code == ConnectErrorCode.NOT_FOUND, (
                f"Expected NOT_FOUND error code, got {e.code}"
            )

    @cluster(num_nodes=6)
    def test_can_not_create_more_than_one_link(self):
        resp = self.create_link("test-link")

        assert resp.shadow_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {resp.shadow_link.name}"
        )

        # Now attempt to create a second one with the same name
        try:
            self.create_link("test-link")
            assert False, (
                "Should not have been able to create a second link with the same name"
            )
        except ConnectError as e:
            assert e.code == ConnectErrorCode.ALREADY_EXISTS, (
                f"Expected {ConnectErrorCode.ALREADY_EXISTS}, got {e.code}"
            )

        # Now create a second one with a different name
        try:
            self.create_link("test-link-2")
            assert False, "Should not have been able to create a second link"
        except ConnectError as e:
            assert e.code == ConnectErrorCode.RESOURCE_EXHAUSTED, (
                f"Expected {ConnectErrorCode.RESOURCE_EXHAUSTED}, got {e.code}"
            )

    def create_source_consumer(self, topic, group_name="test_group", consumer_count=1):
        return KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            group_name=group_name,
            msg_size=128,
            readers=consumer_count,
        )

    @cluster(num_nodes=7)
    def test_consumer_groups_mirroring(self):
        # Create a shadow link

        topic = TopicSpec(name="source-topic", partition_count=5, replication_factor=3)

        self.source_default_client().create_topic(topic)
        # produce some data to the source cluster

        KgoVerifierProducer.oneshot(
            self.test_context, self.source_cluster.service, topic.name, 128, 10000
        )

        consumer = self.create_source_consumer(
            topic=topic.name, group_name="test_group", consumer_count=1
        )
        consumer.start()
        consumer.wait()
        consumer.stop()
        source_rpk = RpkTool(self.source_cluster.service)
        description = source_rpk.group_describe(group="test_group")
        self.logger.info(f">>> source_state: {description}")

        self.create_link("test-link")

        def _group_present_in_target_cluster():
            target_rpk = RpkTool(self.target_cluster.service)
            groups = target_rpk.group_list()

            if not any(g.group == "test_group" for g in groups):
                return False, None

            desc = target_rpk.group_describe(
                group="test_group", tolerant=True, summary=False
            )

            return True, desc

        target_cluster_group = wait_until_result(
            lambda: _group_present_in_target_cluster(),
            timeout_sec=20,
            err_msg="Failed to find consumer group in the target cluster",
        )

        assert target_cluster_group.state == "Empty", (
            "Group test_group state expected to be empty on target cluster"
        )

    @cluster(num_nodes=6)
    def test_topic_creation_in_target_cluster(self):
        topics = []
        for i in range(10):
            cleanup_policy = "delete" if i % 2 == 0 else "compact"
            topic = TopicSpec(
                name=f"source-topic-{i}",
                partition_count=i + 3,
                replication_factor=3,
                cleanup_policy=cleanup_policy,
            )
            self.source_default_client().create_topic(topic)
            topics.append(topic)

        self.create_link("test-link")

        def _topics_are_present_in_target_cluster():
            target_rpk = RpkTool(self.target_cluster.service)
            topics_in_target = {t for t in target_rpk.list_topics()}
            self.logger.info(f"Topics in target cluster: {topics_in_target}")
            if len(topics_in_target) < len(topics):
                return False
            for t in topics:
                if t.name not in topics_in_target:
                    return False

            return True

        wait_until(
            lambda: _topics_are_present_in_target_cluster(),
            timeout_sec=20,
            err_msg="Failed to find topics in the target cluster",
        )
        target_rpk = RpkTool(self.target_cluster.service)
        for t in topics:
            target_configs = target_rpk.describe_topic_configs(t.name)
            self.logger.info(f"Target topic {t.name} configs: {target_configs}")
            assert target_configs["cleanup.policy"][0] == t.cleanup_policy, (
                f"Expected cleanup policy {t.cleanup_policy} for topic {t.name}, "
                f"got {target_configs['cleanup.policy']}"
            )


class ShadowLinkingReplicationTests(ShadowLinkPreAllocTestBase):
    def leadership_shuffler(self, redpanda, topic: str, enabled: bool):
        if not enabled:
            return nullcontext()

        @bg_thread_cm
        def leadership_transfer_thread(redpanda, topic: str):
            admin = Admin(redpanda, retry_codes=[503, 504])
            while (yield):
                try:
                    partitions = admin.get_partitions(namespace="kafka", topic=topic)
                    partition = random.choice(partitions)
                    p_id = partition["partition_id"]
                    admin.partition_transfer_leadership(
                        namespace="kafka", topic=topic, partition=p_id
                    )
                except Exception as e:
                    redpanda.logger.info(f"error transferring leadership: {e}")

        return leadership_transfer_thread(redpanda, topic)

    @cluster(num_nodes=7)
    @matrix(shuffle_leadership=[True, False])
    def test_replication_basic(self, shuffle_leadership):
        topic = TopicSpec(name="source-topic", partition_count=5, replication_factor=3)

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )
        with self.leadership_shuffler(
            self.target_cluster.service, topic.name, enabled=shuffle_leadership
        ):
            self.start_producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000)
            self.verify()

    @cluster(
        num_nodes=7,
        log_allow_list=[
            re.compile(".*Failed to sync write_at_offset_stm for partition"),
        ],
    )
    def test_replication_with_failures(self):
        topic = TopicSpec(name="source-topic", partition_count=5, replication_factor=3)

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        self.start_producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000)
        with (
            self.create_source_failure_injector(),
            self.create_target_failure_injector(),
        ):
            self.verify()
