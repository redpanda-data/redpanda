# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import ducktape.errors
import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
import random
import re

from connectrpc.errors import ConnectError, ConnectErrorCode
from contextlib import nullcontext
from ducktape.mark import matrix
from ducktape.mark import ignore

from rptest.clients.admin.proto.redpanda.core.common import acl_pb2
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
)
from rptest.clients.rpk import RpkTool, RPKACLInput, RpkException
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
    SecondaryClusterArgs,
    SecondaryClusterSpec,
    ServiceType,
)
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.cluster_linking_test_base import (
    ShadowLinkPreAllocTestBase,
    ShadowLinkTestBase,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (
    bg_thread_cm,
    contextmanager,
    expect_exception,
    wait_until,
    wait_until_result,
)
from typing import Any

import google.protobuf.duration_pb2


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
            secondary_spec=SecondaryClusterSpec(ServiceType.REDPANDA),
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

    @cluster(num_nodes=6)
    def test_basic_ops(self):
        with MultiClusterServices(
            self.test_context,
            self.logger,
            self.redpanda,
            secondary_spec=SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
            num_brokers=3,
        ) as services:
            assert services.secondary.is_kafka, (
                f"Expected Kafka service, got {services.secondary}"
            )
            self.basic_ops(services)


class ShadowLinkBasicTests(ShadowLinkTestBase):
    def _topics_are_present_in_target_cluster(self, topics):
        target_rpk = RpkTool(self.target_cluster.service)
        topics_in_target = {t for t in target_rpk.list_topics()}
        self.logger.info(f"Topics in target cluster: {topics_in_target}")
        if len(topics_in_target) < len(topics):
            return False
        for t in topics:
            if t.name not in topics_in_target:
                return False

        return True

    @cluster(num_nodes=6)
    def test_create_simple_link(self):
        shadow_link = self.create_link("test-link")
        self.logger.info(f"Create shadow link result: {shadow_link}")

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
        shadow_link = self.create_link("test-link")

        assert shadow_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {shadow_link.name}"
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

        wait_until(
            lambda: self._topics_are_present_in_target_cluster(topics),
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

    @cluster(num_nodes=6)
    def test_topic_creation_restriction(self):
        """
        Test validates that when cluster linking is active, that topics can only be created by superusers
        """
        username = "test-user"
        password = "test-password"
        topic_name_prefix = "test-topic"

        superuser_rpk = RpkTool(
            self.target_cluster_service,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.mechanism,
        )
        normaluser_rpk = RpkTool(
            self.target_cluster_service,
            username=username,
            password=password,
            sasl_mechanism="SCRAM-SHA-256",
        )

        self.logger.debug(f'Creating user "{username}"')
        superuser_rpk.sasl_create_user(new_username=username, new_password=password)
        new_acl = RPKACLInput()
        new_acl.allow_principal = [f"User:{username}"]
        new_acl.operation = ["ALL"]
        new_acl.resource_pattern_type = "prefixed"
        new_acl.topic = [topic_name_prefix]

        self.logger.debug("Enabling SASL on target cluster")

        self.target_cluster_service.set_cluster_config(values={"enable_sasl": True})

        self.logger.debug(f"Creating ACL {new_acl}")
        superuser_rpk.acl_create(acl=new_acl)

        # Verifying that a normal user can create a topic without link being present
        normaluser_rpk.create_topic(f"{topic_name_prefix}-1")

        self.logger.debug("Creating cluster link")
        self.create_link("test-link")

        # Now verify that the user cannot create the topic
        try:
            normaluser_rpk.create_topic(f"{topic_name_prefix}-2")
            assert False, "Should not have been able to create a topic"
        except RpkException:
            pass

        superuser_rpk.create_topic(f"{topic_name_prefix}-3")

    @cluster(num_nodes=6)
    def test_update_link(self):
        """
        This is a simple test to verify that the UpdateShadowLink API works.

        First the test creates 10 topics on the source cluster, then it creates
        a shadow link with no topic filters

        It then verifies that no topics were created, then updates the shadow
        link to add two topic filters: one to select all by prefix and one to
        exclude literally

        Then it verifies that the included topics are replicated and the excluded
        topic is not
        """
        topic_prefix = "source-topic-"
        topics: list[TopicSpec] = []
        for i in range(10):
            topic = TopicSpec(
                name=f"{topic_prefix}{i}", partition_count=3, replication_factor=3
            )
            self.source_default_client().create_topic(topic)
            topics.append(topic)

        shadow_link: shadow_link_pb2.ShadowLink = self.create_link(
            "test-link", mirror_all_topics=False, mirror_all_groups=False
        )

        def _any_topics_are_present_in_target_cluster():
            topics_in_target = {t for t in self.target_cluster_rpk.list_topics()}
            for t in topics:
                if t.name in topics_in_target:
                    return True

            return False

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(_any_topics_are_present_in_target_cluster, timeout_sec=5)

        shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name=topic_prefix,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_EXCLUDE,
                    name=f"{topic_prefix}0",
                ),
            ]
        )

        update_mask: google.protobuf.field_mask_pb2.FieldMask = google.protobuf.field_mask_pb2.FieldMask(
            paths=[
                "configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters"
            ]
        )

        updated_link = self.update_link(
            shadow_link=shadow_link, update_mask=update_mask
        )

        assert (
            updated_link.configurations.topic_metadata_sync_options
            == shadow_link.configurations.topic_metadata_sync_options
        ), (
            f"Expected updated link to be returned, {updated_link.configurations.topic_metadata_sync_options} != {shadow_link.configurations.topic_metadata_sync_options}"
        )

        def _all_but_one_topic_are_present_in_target_cluster():
            topics_in_target = {t for t in self.target_cluster_rpk.list_topics()}
            found_count = 0
            for t in topics:
                if t.name in topics_in_target:
                    if t.name == f"{topic_prefix}0":
                        assert False, f"{topic_prefix}0 should not be mirrored!"
                    found_count += 1

            self.logger.info(f"{found_count} == {len(topics) - 1}")
            return found_count == (len(topics) - 1)

        wait_until(
            _all_but_one_topic_are_present_in_target_cluster,
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Not all topics were mirrored",
        )

    @cluster(num_nodes=6)
    def test_update_not_in_field_mask(self):
        shadow_link: shadow_link_pb2.ShadowLink = self.create_link(
            "test-link", mirror_all_topics=False, mirror_all_groups=False
        )

        shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="*",
                ),
            ]
        )
        expected_duration = google.protobuf.duration_pb2.Duration(seconds=600)
        shadow_link.configurations.topic_metadata_sync_options.interval.CopyFrom(
            expected_duration
        )

        update_mask: google.protobuf.field_mask_pb2.FieldMask = (
            google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.topic_metadata_sync_options.interval"]
            )
        )

        updated_link = self.update_link(
            shadow_link=shadow_link, update_mask=update_mask
        )

        assert (
            updated_link.configurations.topic_metadata_sync_options.interval
            == expected_duration
        ), (
            f"Expected duration to be {expected_duration}, got {updated_link.configurations.topic_metadata_sync_options.interval}"
        )

        assert (
            len(
                updated_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters
            )
            == 0
        ), (
            f"Expected topic filters to not be updated, got {updated_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters}"
        )

    @cluster(num_nodes=6)
    def test_invalid_updates(self):
        shadow_link: shadow_link_pb2.ShadowLink = self.create_link(
            "test-link", mirror_all_topics=False, mirror_all_groups=False
        )

        update_mask: google.protobuf.field_mask_pb2.FieldMask = (
            google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.client_options.bootstrap_servers"]
            )
        )

        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.INVALID_ARGUMENT
        ):
            self.update_link(shadow_link=shadow_link, update_mask=update_mask)

        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.client_options.tls_settings"]
        )

        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.INVALID_ARGUMENT
        ):
            self.update_link(shadow_link=shadow_link, update_mask=update_mask)

        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.client_options.tls_settings.tls_file_settings"]
        )

        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.INVALID_ARGUMENT
        ):
            self.update_link(shadow_link=shadow_link, update_mask=update_mask)

    @cluster(num_nodes=6)
    def test_delete_simple_link(self):
        def get_links_by_name():
            list_links = self.list_links()
            return [l.name for l in list_links]

        empty_link = "empty-link"
        shadow_link = self.create_link(empty_link)
        self.logger.info(f"Create shadow link result: {shadow_link}")

        links = get_links_by_name()
        assert len(links) == 1, (
            f"Expected exactly one shadow link, got {len(links)}. Test setup failed"
        )

        # Verify that a request to delete a non-existent link will fail gracefully
        bad_link_name = "non-existent-link"
        with expect_exception(
            ConnectError,
            lambda e: str(e)
            == f"[not_found] Failed to find cluster link with name '{bad_link_name}'",
        ):
            self.delete_link(bad_link_name)

        # Verify that an empty link can and will be deleted
        self.delete_link(empty_link)
        wait_until(
            lambda: empty_link not in get_links_by_name(),
            timeout_sec=20,
            err_msg=f"Failed to delete {empty_link}",
        )

        # Create some topics to be mirrored.
        topics = []
        for i in range(10):
            topic = TopicSpec(
                name=f"source-topic-{i}",
            )
            self.source_default_client().create_topic(topic)
            topics.append(topic)

        test_link = "test-link"
        self.create_link(test_link)

        wait_until(
            lambda: self._topics_are_present_in_target_cluster(topics),
            timeout_sec=20,
            err_msg="Failed to find topics in the target cluster. Test setup failed",
        )

        # Verify that a request to delete a link with mirrored topics will fail
        with expect_exception(
            ConnectError,
            lambda e: str(e)
            == f"[failed_precondition] Failed to delete cluster link with name '{test_link}'. There are active shadow topics.",
        ):
            self.delete_link(test_link)


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

    @cluster(num_nodes=8)
    @matrix(
        shuffle_leadership=[True, False],
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
    )
    def test_replication_basic(self, shuffle_leadership, source_cluster_spec):
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
        num_nodes=8,
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


class ShadowLinkConsumeGroupsMirroringTest(ShadowLinkTestBase):
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
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ]
    )
    def test_consumer_groups_mirroring(self, source_cluster_spec):
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
        self.logger.info(f"source_state: {description}")

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

    @contextmanager
    def _nop_context_manager(self):
        try:
            yield
        finally:
            pass

    @cluster(num_nodes=7)
    @ignore(
        with_failures=True,
        source_cluster_spec=SecondaryClusterSpec(
            ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
        ),
    )
    @matrix(
        with_failures=[True, False],
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
    )
    def test_continuous_group_sync(self, with_failures, source_cluster_spec):
        partition_count = 120
        topic_count = 6

        topics = [
            TopicSpec(
                name=f"source-topic-{i}",
                partition_count=int(partition_count / topic_count),
                replication_factor=3,
            )
            for i in range(topic_count)
        ]

        groups = [f"test_group_{i}" for i in range(20)]

        self.create_link("test-link")
        source_rpk = RpkTool(self.source_cluster.service)
        target_rpk = RpkTool(self.target_cluster.service)

        def _maybe_failure_injector():
            if with_failures:
                return self.create_source_failure_injector()
            else:
                return self._nop_context_manager()

        def _consume_with_group(topic: str, group_id: str):
            try:
                source_rpk.consume(
                    topic=topic, group=group_id, n=1, timeout=5, offset="start"
                )
            except Exception as e:
                self.logger.debug(
                    f"Failed to consume from topic {topic}, group {group_id}: {e}"
                )

        def _wait_for_group_states_consistent():
            source_groups = {g: source_rpk.group_describe(group=g) for g in groups}
            target_groups = {g: target_rpk.group_describe(group=g) for g in groups}

            for g_name, g_desc in source_groups.items():
                self.logger.debug(f"group: {g_name} - source:  {g_desc.partitions}")
                self.logger.debug(
                    f"group: {g_name} - target:  {target_groups[g_name].partitions if g_name in target_groups else 'N/A'}"
                )
                if g_name not in target_groups:
                    self.logger.debug(f"Group {g_name} not present in target cluster")
                    return False

                t_desc = target_groups[g_name]
                t_partitions = {
                    (p.topic, p.partition): p.current_offset for p in t_desc.partitions
                }
                for p in g_desc.partitions:
                    if (p.topic, p.partition) not in t_partitions:
                        return False
                    if p.current_offset != t_partitions[(p.topic, p.partition)]:
                        self.logger.warn(
                            f"Group {g_name} partition {p.topic}/{p.partition} offsets differ: source {p.current_offset} vs target {t_partitions[(p.topic, p.partition)]}"
                        )
                        return False
            return True

        def _execute_random_updates(cnt: int):
            for _ in range(cnt):
                topic = topics[random.randint(0, len(topics) - 1)].name
                group = groups[random.randint(0, len(groups) - 1)]
                self.logger.debug(f"Consuming from topic {topic}, group {group}")
                _consume_with_group(topic, group)

        for t in topics:
            self.source_default_client().create_topic(t)

        for t in topics:
            KgoVerifierProducer.oneshot(
                self.test_context, self.source_cluster.service, t.name, 40, 1000
            )
        with _maybe_failure_injector():
            for _ in range(5):
                _execute_random_updates(10)
                wait_until(
                    lambda: _wait_for_group_states_consistent(),
                    timeout_sec=120,
                    backoff_sec=3,
                    err_msg="Group states not consistent between source and target clusters",
                    retry_on_exc=True,
                )


class ShadowLinkSecurityTests(ShadowLinkTestBase):
    """
    Tests that verify security settings syncing
    """

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context=test_context,
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=SchemaRegistryConfig()
            ),
            schema_registry_config=SchemaRegistryConfig(),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=6)
    @matrix(check_sr=[False, True])
    def test_acl_sync(self, check_sr: bool):
        """
        This test verifies that Kafka ACLs are synced from source to target cluster
        when a shadow link is created and configured
        """
        req = self.create_default_link_request("test-link")

        resource_type = (
            acl_pb2.ACL_RESOURCE_SR_ANY if check_sr else acl_pb2.ACL_RESOURCE_ANY
        )

        resource_filter = shadow_link_pb2.ACLResourceFilter(
            resource_type=resource_type, pattern_type=acl_pb2.ACL_PATTERN_ANY
        )
        access_filter = shadow_link_pb2.ACLAccessFilter(
            permission_type=acl_pb2.ACL_PERMISSION_TYPE_ANY,
            operation=acl_pb2.ACL_OPERATION_ANY,
        )
        acl_filter = shadow_link_pb2.ACLFilter(
            resource_filter=resource_filter, access_filter=access_filter
        )
        acl_filters: list[shadow_link_pb2.ACLFilter] = [acl_filter]

        security_sync_options = shadow_link_pb2.SecuritySettingsSyncOptions(
            interval=google.protobuf.duration_pb2.Duration(seconds=1),
            acl_filters=acl_filters,
        )
        req.shadow_link.configurations.security_sync_options.CopyFrom(
            security_sync_options
        )

        _ = self.create_link_with_request(req=req)
        self.logger.info("Successfully created link")

        target_acls: Any = self.target_cluster_rpk.acl_list(format="json")
        assert len(target_acls["matches"]) == 0, (
            f"Expected no ACLs on target cluster, got {target_acls}"
        )

        sr_kafka_acl = RPKACLInput(
            allow_principal=["test-user"],
            topic=["foo"],
            registry_subject=["foo-value"],
            operation=["read"],
            resource_pattern_type="literal",
        )
        self.source_cluster_rpk.acl_create(sr_kafka_acl)

        def check_if_acls_synced():
            target_acls: Any = self.target_cluster_rpk.acl_list(format="json")
            if len(target_acls["matches"]) == 1:
                acl = target_acls["matches"][0]
                self.logger.info(f"Found ACL on target cluster: {acl}")
                expected_resource_type = "SUBJECT" if check_sr else "TOPIC"
                expected_resource_name = "foo-value" if check_sr else "foo"
                return (
                    acl["principal"] == "User:test-user"
                    and acl["host"] == "*"
                    and acl["operation"] == "READ"
                    and acl["resource_type"] == expected_resource_type
                    and acl["resource_name"] == expected_resource_name
                    and acl["resource_pattern_type"] == "LITERAL"
                    and acl["permission"] == "ALLOW"
                )
            return False

        wait_until(
            check_if_acls_synced,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Failed to sync acls",
        )
