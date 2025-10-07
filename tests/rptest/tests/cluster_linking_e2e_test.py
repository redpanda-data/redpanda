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
import threading
import time

from ducktape.cluster.cluster_spec import ClusterSpec
from connectrpc.errors import ConnectError, ConnectErrorCode
from contextlib import nullcontext
from ducktape.mark import matrix
from ducktape.mark import ignore

from rptest.clients.admin.proto.redpanda.core.common import acl_pb2
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
)
from rptest.clients.kafka_cli_tools import KafkaCliToolsError
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
    DEFAULT_SYNCED_TOPIC_PROPERTIES,
    DISALLOWED_SYNCED_TOPIC_PROPERTIES,
    REQUIRED_SYNCED_TOPIC_PROPERTIES,
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
from time import sleep
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

        shadow_topics = self.list_shadow_topics(shadow_link_name="test-link")
        assert len(shadow_topics) == len(topics), (
            f"Expected {len(topics)} shadow topics, got {len(shadow_topics)}"
        )

        for t in topics:
            found = False
            for st in shadow_topics:
                if st.name == t.name:
                    found = True
                    break
            assert found, f"Did not find shadow topic for {t.name}"

        for t in topics:
            self.get_shadow_topic(
                shadow_link_name="test-link", shadow_topic_name=t.name
            )

        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.NOT_FOUND
        ):
            self.get_shadow_topic(
                shadow_link_name="test-link", shadow_topic_name="non-existent-topic"
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
            == f"[failed_precondition] Failed to delete cluster link with name '{test_link}'. There are active/promoting shadow topics.",
        ):
            self.delete_link(test_link)

        # Now verify that we can delete the link when force=True
        self.delete_link(test_link, force=True)

        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.NOT_FOUND
        ):
            self.get_link(test_link)

    @cluster(num_nodes=6)
    def test_toggle_cluster_config(self):
        first_topic = TopicSpec(
            name="first-topic", partition_count=1, replication_factor=3
        )
        self.source_default_client().create_topic(first_topic)

        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self._topics_are_present_in_target_cluster([first_topic]),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to find first-topic in target cluster",
        )

        self.logger.info("Disabling cluster linking on target cluster")
        self.target_cluster_service.set_cluster_config({"enable_shadow_linking": False})
        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION
        ):
            self.get_link("test-link")

        # Validate that nothing got replicated
        self.source_cluster_rpk.produce(
            first_topic.name, key="test-first", msg="test-first"
        )

        def _check_hwm(
            rpk: RpkTool, topic_name: str, partition_id: int, expected_hwm: int
        ):
            partition_info = list(rpk.describe_topic(topic_name))
            for p in partition_info:
                if p.id == partition_id:
                    return p.high_watermark >= expected_hwm

            return False

        # Validate that the topic does not contain any data
        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            self.target_cluster_service.wait_until(
                lambda: _check_hwm(self.target_cluster_rpk, first_topic.name, 0, 1),
                timeout_sec=5,
                backoff_sec=1,
            )
        # validate that the topic is still not writable
        with expect_exception(RpkException, lambda _: True):
            self.target_cluster_rpk.produce(first_topic.name, key="test", msg="test")

        second_topic = TopicSpec(
            name="second-topic", partition_count=1, replication_factor=3
        )
        self.source_default_client().create_topic(second_topic)

        # Now verify that the second topic is not replicated
        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            self.target_cluster.service.wait_until(
                lambda: self._topics_are_present_in_target_cluster([second_topic]),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Now re enable shadow linking and wait for the topic to appear
        self.target_cluster_service.set_cluster_config({"enable_shadow_linking": True})
        self.target_cluster.service.wait_until(
            lambda: self._topics_are_present_in_target_cluster([second_topic]),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to find second-topic in target cluster",
        )

        # Now wait for the first topic to have data
        self.target_cluster_service.wait_until(
            lambda: _check_hwm(self.target_cluster_rpk, first_topic.name, 0, 1),
            timeout_sec=20,
            backoff_sec=1,
        )

    @cluster(num_nodes=6)
    def test_rapid_shadow_link_toggling(self):
        self.create_link("test-link")

        def toggle_shadow_linking(times: int):
            state: bool = True
            for _ in range(times):
                state = not state
                self.target_cluster_service.set_cluster_config(
                    {"enable_shadow_linking": state}
                )

        toggle_thread = threading.Thread(target=toggle_shadow_linking, args=(200,))

        toggle_thread.start()
        topics: list[TopicSpec] = []
        for i in range(10):
            topic = TopicSpec(
                name=f"source-topic-{i}", partition_count=3, replication_factor=3
            )
            self.source_default_client().create_topic(topic)
            topics.append(topic)

        self.target_cluster.service.wait_until(
            lambda: self._topics_are_present_in_target_cluster(topics),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Failed to find first-topic in target cluster",
        )

        toggle_thread.join()

    @cluster(num_nodes=6)
    def test_shadow_link_sanctioning(self):
        self.target_cluster.service.set_environment(
            {"__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE": "true"}
        )
        self.target_cluster.service.restart_nodes(self.target_cluster.service.nodes)
        self.target_cluster.service.wait_until(
            self.target_cluster_service.healthy,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Cluster hasn't stabilized",
        )

        Admin(self.target_cluster_service).await_stable_leader(
            namespace="redpanda", topic="controller", partition=0
        )

        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.FAILED_PRECONDITION
        ):
            self.create_link("test-link")

    @cluster(num_nodes=6)
    def test_deny_prefix(self):
        topics = [
            TopicSpec(name="__redpanda-topic", partition_count=3, replication_factor=3),
            TopicSpec(name="_redpanda-topic", partition_count=3, replication_factor=3),
            TopicSpec(name="normal-topic", partition_count=3, replication_factor=3),
        ]

        for topic in topics:
            self.source_default_client().create_topic(topic)

        shadow_link = self.create_link("test-link")

        def _only_normal_topic_present_in_target_cluster():
            topics_in_target = {t for t in self.target_cluster_rpk.list_topics()}
            self.logger.info(f"Topics in target cluster: {topics_in_target}")
            return len(topics_in_target) == 1 and "normal-topic" in topics_in_target

        self.target_cluster_service.wait_until(
            _only_normal_topic_present_in_target_cluster,
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to find only normal-topic in the target cluster",
        )

        # Now attempt to add a filter to specifically include _redpanda.audit_log
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=[
                "configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters"
            ]
        )
        shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="_redpanda.audit_log",
                )
            ]
        )

        with expect_exception(
            ConnectError, lambda e: e.code == ConnectErrorCode.INVALID_ARGUMENT
        ):
            self.update_link(shadow_link=shadow_link, update_mask=update_mask)

        shadow_link = self.get_link("test-link")
        # Now create a link adding the two above topics
        shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="__redpanda-topic",
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="_redpanda-topic",
                ),
            ]
        )
        self.update_link(shadow_link=shadow_link, update_mask=update_mask)

        self.target_cluster_service.wait_until(
            lambda: self._topics_are_present_in_target_cluster(topics),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to find all topics in the target cluster",
        )

    def set_exclude_default(
        self, val: bool, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        # Now update the link to not replicate default properties
        shadow_link.configurations.topic_metadata_sync_options.exclude_default = val

        update_mask: google.protobuf.field_mask_pb2.FieldMask = (
            google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.topic_metadata_sync_options.exclude_default"]
            )
        )
        return self.update_link(shadow_link, update_mask)

    @cluster(num_nodes=6)
    def test_topic_properties_options(self):
        """
        This test verifies the new field added for listing out which topic properties
        are synced and that it's updated appropriately
        """
        topic = TopicSpec(name="source-topic", partition_count=5, replication_factor=3)

        self.source_default_client().create_topic(topic)
        shadow_link = self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        shadow_link = self.get_link("test-link")
        expected_properties_list = (
            REQUIRED_SYNCED_TOPIC_PROPERTIES + DEFAULT_SYNCED_TOPIC_PROPERTIES
        )
        expected_properties_list.sort()

        synced_properties = shadow_link.status.synced_shadow_topic_properties
        synced_properties.sort()

        # By default we should see all those properties
        assert synced_properties == expected_properties_list, (
            f"Expected synced properties to be {expected_properties_list}, got {synced_properties}"
        )

        # Now update the link to not replicate default properties
        self.set_exclude_default(True, shadow_link)

        shadow_link = self.get_link("test-link")

        synced_properties = shadow_link.status.synced_shadow_topic_properties
        synced_properties.sort()
        expected_properties_list = REQUIRED_SYNCED_TOPIC_PROPERTIES
        expected_properties_list.sort()

        assert synced_properties == expected_properties_list, (
            f"Expected synced properties to be {expected_properties_list}, got {synced_properties}"
        )

    @cluster(num_nodes=6)
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
    )
    def test_topic_properties_replicated(self, source_cluster_spec):
        """
        This test verifies that the default topic properties are replicated
        """
        topic_properties: dict[str, Any] = {
            "max.message.bytes": 1024,
            "cleanup.policy": "compact,delete",
            "message.timestamp.type": TopicSpec.TIMESTAMP_LOG_APPEND_TIME,
            "compression.type": TopicSpec.COMPRESSION_ZSTD.value,
            "retention.bytes": 512,
            "retention.ms": 100,
            "delete.retention.ms": 200,
            "min.compaction.lag.ms": 300,
            "max.compaction.lag.ms": 400,
        }
        topic = TopicSpec(
            name="source-topic",
            partition_count=5,
            replication_factor=3,
            max_message_bytes=topic_properties["max.message.bytes"],
            cleanup_policy=topic_properties["cleanup.policy"],
            message_timestamp_type=topic_properties["message.timestamp.type"],
            compression_type=topic_properties["compression.type"],
            retention_bytes=topic_properties["retention.bytes"],
            retention_ms=topic_properties["retention.ms"],
            delete_retention_ms=topic_properties["delete.retention.ms"],
            min_compaction_lag_ms=topic_properties["min.compaction.lag.ms"],
            max_compaction_lag_ms=topic_properties["max.compaction.lag.ms"],
        )
        self.source_default_client().create_topic(topic)
        source_topic = self.source_cluster_rpk.describe_topic_configs(topic.name)

        def validate_topic_properties(properties_to_check: dict[str, tuple[str, str]]):
            for key, val in topic_properties.items():
                assert str(properties_to_check[key][0]) == str(val), (
                    f"Expected {key} to be {str(val)}, got {str(properties_to_check[key][0])}.  Full config: {properties_to_check}"
                )

        validate_topic_properties(source_topic)

        shadow_link = self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        target_topic = self.target_cluster_rpk.describe_topic_configs(topic.name)

        validate_topic_properties(target_topic)

        # Now do not include default properties and update the default ones and one non-default one
        shadow_link = self.set_exclude_default(True, shadow_link)
        modified_configs = {
            "compression.type": "gzip",
            "retention.bytes": 1024 * 1024,
            "retention.ms": 1000 * 60,
            "delete.retention.ms": 1000 * 60 * 10,
            "min.compaction.lag.ms": 1000 * 60 * 20,
            "max.compaction.lag.ms": 1000 * 60 * 30,
            "max.message.bytes": 4096,
        }
        if source_cluster_spec == SecondaryClusterSpec(ServiceType.REDPANDA):
            modified_configs["replication.factor"] = 1
        self.source_default_client().alter_topic_configs(
            "source-topic",
            modified_configs,
        )

        self.target_cluster_service.wait_until(
            lambda: self.target_cluster_rpk.describe_topic_configs(topic.name)[
                "max.message.bytes"
            ][0]
            == "4096",
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} in target cluster did not get updated max.message.bytes",
        )

        topic_properties["max.message.bytes"] = 4096
        target_topic = self.target_cluster_rpk.describe_topic_configs(topic.name)

        validate_topic_properties(target_topic)

        # Now add in "replication.factor" and see that it is changed
        shadow_link.configurations.topic_metadata_sync_options.synced_shadow_topic_properties.extend(
            ["retention.ms"]
        )
        update_mask: google.protobuf.field_mask_pb2.FieldMask = google.protobuf.field_mask_pb2.FieldMask(
            paths=[
                "configurations.topic_metadata_sync_options.synced_shadow_topic_properties"
            ]
        )
        shadow_link = self.update_link(shadow_link, update_mask)

        self.target_cluster_service.wait_until(
            lambda: self.target_cluster_rpk.describe_topic_configs(topic.name)[
                "retention.ms"
            ][0]
            == "60000",
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} in target cluster did not get updated retention.ms",
        )

        topic_properties["retention.ms"] = 60000
        target_topic = self.target_cluster_rpk.describe_topic_configs(topic.name)
        validate_topic_properties(target_topic)

    @cluster(num_nodes=6)
    def test_disallowed_topic_properties(self):
        """
        This test verifies that the disallowed topic properties cannot be added to the
        synced_shadow_topic_properties list
        """
        shadow_link = self.create_link("test-link")

        for prop in DISALLOWED_SYNCED_TOPIC_PROPERTIES:
            shadow_link = self.get_link("test-link")
            shadow_link.configurations.topic_metadata_sync_options.synced_shadow_topic_properties.extend(
                [prop]
            )
            update_mask: google.protobuf.field_mask_pb2.FieldMask = google.protobuf.field_mask_pb2.FieldMask(
                paths=[
                    "configurations.topic_metadata_sync_options.synced_shadow_topic_properties"
                ]
            )
            with expect_exception(
                ConnectError,
                lambda e: e.code == ConnectErrorCode.INVALID_ARGUMENT,
            ):
                self.update_link(shadow_link, update_mask)


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

    def _get_shadow_topic(
        self,
        shadow_link_name: str,
        shadow_topic_name: str,
        expected_partitions: int | None = None,
    ) -> tuple[bool, shadow_link_pb2.ShadowTopic | None]:
        shadow_topic = self.get_shadow_topic(
            shadow_link_name=shadow_link_name, shadow_topic_name=shadow_topic_name
        )
        self.logger.debug(f"Received ShadowTopic: {shadow_topic}")

        if expected_partitions is None:
            return True, shadow_topic

        if len(shadow_topic.status.partition_information) == expected_partitions:
            return True, shadow_topic

        return False, None

    def _check_partitions_match(
        self, rpk: RpkTool, topic_name: str, shadow_topic: shadow_link_pb2.ShadowTopic
    ) -> bool:
        source_topic_info = rpk.describe_topic(topic_name)
        for p in source_topic_info:
            partition_id = p.id
            hwm = p.high_watermark

            for p_info in shadow_topic.status.partition_information:
                if p_info.partition_id == partition_id:
                    self.logger.debug(
                        f"Partition {partition_id}: source hwm={hwm}, shadow_hwm{p_info.source_high_watermark}, last_update={p_info.source_last_updated_timestamp}"
                    )
                    if p_info.source_high_watermark != hwm:
                        return False
        return True

    def _fetch_shadow_topic_and_compare_results(
        self,
        rpk: RpkTool,
        shadow_link_name: str,
        shadow_topic_name: str,
        expected_partitions: int,
    ) -> bool:
        try:
            shadow_topic = wait_until_result(
                lambda: self._get_shadow_topic(
                    shadow_link_name, shadow_topic_name, expected_partitions
                ),
                timeout_sec=5,
                err_msg=f"Shadow topic {shadow_topic_name} not found or does not have expected {expected_partitions} partitions",
            )
        except ducktape.errors.TimeoutError as e:
            self.logger.debug(f"Timeout fetching shadow topic {shadow_topic_name}: {e}")
            return False

        return self._check_partitions_match(rpk, shadow_topic_name, shadow_topic)

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
        partition_count = 5
        topic = TopicSpec(
            name="source-topic", partition_count=partition_count, replication_factor=3
        )

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

        self.logger.info("Starting cycle looking for shadow topic status")
        wait_until(
            lambda: self._fetch_shadow_topic_and_compare_results(
                self.source_cluster_rpk, "test-link", topic.name, partition_count
            ),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Shadow topic {topic.name} partition info does not match source topic",
            retry_on_exc=True,
        )

    @cluster(
        num_nodes=8,
        log_allow_list=[
            re.compile(".*Failed to sync write_at_offset_stm for partition"),
        ],
    )
    def test_replication_with_failures(self):
        partition_count = 5
        topic = TopicSpec(
            name="source-topic", partition_count=partition_count, replication_factor=3
        )

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        encountered_errors: list[str] = []
        running = True

        def poll_shadow_topic_status(shadow_link_name: str, shadow_topic_name: str):
            while running:
                try:
                    shadow_topic = self.get_shadow_topic(
                        shadow_link_name=shadow_link_name,
                        shadow_topic_name=shadow_topic_name,
                    )
                    assert shadow_topic is not None, "Shadow topic not found"
                    assert len(shadow_topic.status.partition_information) > 0, (
                        "No partition information found"
                    )
                except Exception as e:
                    encountered_errors.append(str(e))
                time.sleep(1)

        self.start_producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000)
        poll_thread = threading.Thread(
            target=poll_shadow_topic_status, args=("test-link", topic.name)
        )
        with (
            self.create_source_failure_injector(),
            self.create_target_failure_injector(),
        ):
            poll_thread.start()
            self.verify()

        running = False
        poll_thread.join()

        assert len(encountered_errors) == 0, (
            f"Encountered errors while polling: {encountered_errors}"
        )

        self.logger.info("Starting cycle looking for shadow topic status")
        wait_until(
            lambda: self._fetch_shadow_topic_and_compare_results(
                self.source_cluster_rpk, "test-link", topic.name, partition_count
            ),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Shadow topic {topic.name} partition info does not match source topic",
            retry_on_exc=True,
        )

    @cluster(num_nodes=8)
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
    )
    def test_topic_delete(self, source_cluster_spec):
        topic = TopicSpec(name="source-topic", partition_count=5, replication_factor=3)

        self.source_default_client().create_topic(topic)
        shadow_link = self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )
        self.start_producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000)
        self.verify()

        target_client = self.target_default_client()

        # topic is not deletable as it is covered by the shadow topic autocreate filters
        with expect_exception(
            KafkaCliToolsError, lambda e: "PolicyViolationException" in str(e)
        ):
            target_client.delete_topic(topic.name)

        shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_EXCLUDE,
                    name=topic.name,
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

        # Now the topic should be deletable, as it is not in the autocreate filters
        target_client.delete_topic(topic.name)
        link_state = self.get_link("test-link")
        assert len(link_state.status.shadow_topics) == 0, (
            "Expected empty shadow_topic list. "
            f"Instead got {link_state.status.shadow_topics}"
        )

    @cluster(num_nodes=8)
    def test_replication_with_transactions(self):
        topic = TopicSpec(name="source-topic", partition_count=1, replication_factor=3)

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        self.start_producer_consumer(
            topic=topic.name, msg_size=128, msg_cnt=10000, use_transactions=True
        )
        self.verify()

    @cluster(num_nodes=8)
    def test_replication_with_truncated_topic(self):
        topic = TopicSpec(name="source-topic", partition_count=1, replication_factor=3)
        self.source_default_client().create_topic(topic)
        # Populate some data
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            msg_size=4 * 1024,
            msg_count=10000,
            custom_node=self.preallocated_nodes,
        )
        self.source_cluster_rpk.trim_prefix(
            topic="source-topic", offset=1000, partitions=[0]
        )
        self.create_link("test-link")
        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )
        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.target_cluster.service,
            topic=topic.name,
            group_name="test-group",
            msg_size=4 * 1024,
            readers=1,
            continuous=True,
        )
        consumer.start()
        consumer.wait_total_reads(count=9000, timeout_sec=60, backoff_sec=5)

    def _maybe_failure_injector(self, with_failures: bool):
        if with_failures:
            return self.create_source_failure_injector()
        else:
            return self._nop_context_manager()

    def _perform_auto_prefix_trimming(self, topic_name: str):
        self.start_producer_consumer(topic=topic_name, msg_size=128, msg_cnt=100000)
        offsets = [1000, 1001, 1200, 1500, 2000, 2500]

        def wait_for_records(rpk: RpkTool, offset: int):
            num_parts = 0
            for part in rpk.describe_topic("source-topic"):
                num_parts += 1
                if (part.high_watermark or 0) < offset:
                    return False
            return num_parts == 5 and True

        partitions = list(range(5))

        for o in offsets:
            self.source_cluster.service.wait_until(
                lambda: wait_for_records(self.source_cluster_rpk, offset=o),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Timed out waiting for {o} records in each partition",
            )

            self.logger.info(f"Trimming source topic prefixes to {o}")
            self.source_cluster_rpk.trim_prefix(
                topic="source-topic", partitions=partitions, offset=o
            )

            def wait_for_start_offset(rpk: RpkTool, offset: int):
                num_parts = 0
                for part in rpk.describe_topic("source-topic"):
                    num_parts += 1
                    self.logger.info(
                        f"Offset for source-topic/{part.id} is {part.start_offset}"
                    )
                    if (part.start_offset or 0) != offset:
                        return False
                return num_parts == 5 and True

            self.source_cluster.service.wait_until(
                lambda: wait_for_start_offset(self.source_cluster_rpk, offset=o),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Timed out waiting for start offset to be {o} in each partition",
            )

            # Produce a single message to ensure cluster linking picks up the trim
            for part in range(0, 5):
                self.logger.info(f"Producing trim-trigger message to partition {part}")
                self.source_cluster_rpk.produce(
                    topic="source-topic",
                    key="trim-trigger",
                    msg="trim-trigger",
                    partition=part,
                )

            self.logger.info(
                f"Now waiting for target cluster to get to {o} starting offset"
            )

            self.target_cluster.service.wait_until(
                lambda: wait_for_start_offset(self.target_cluster_rpk, offset=o),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Timed out waiting for target to get start offset to be {o} in each partition",
            )

    @cluster(num_nodes=8)
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
    def test_auto_prefix_trimming(self, with_failures, source_cluster_spec):
        topic = TopicSpec(name="source-topic", partition_count=5, replication_factor=3)

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        with self._maybe_failure_injector(with_failures):
            self._perform_auto_prefix_trimming(topic.name)


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


class ShadowLinkTopicFailoverTests(ShadowLinkPreAllocTestBase):
    def _maybe_failure_injector(self, with_failures: bool):
        if with_failures:
            return self.create_source_failure_injector()
        else:
            return self._nop_context_manager()

    def _produce_to_topics(
        self,
        topics: list[TopicSpec],
        redpanda,
        messages: int = 1000,
        expect_failures: bool = False,
    ):
        for t in topics:
            producer = KgoVerifierProducer(
                self.test_context,
                redpanda,
                t.name,
                128,
                messages,
                self.preallocated_nodes,
            )
            try:
                producer.start()
                producer.stop()
            except ducktape.errors.TimeoutError as e:
                if expect_failures:
                    self.logger.debug(
                        f"Expected failure producing to topic {t.name}: {e}"
                    )
                else:
                    raise
            finally:
                producer.do_free()

    @cluster(num_nodes=7)
    @matrix(with_failures=[True, False])
    def test_link_topic_failover(self, with_failures):
        num_failover_topics = random.choice([1, 3, 5, 10])
        num_non_failover_topics = random.choice([0, 3, 5, 10])

        self.create_link("test-link")
        failover_topics = [
            TopicSpec(
                name=f"failover-topic-{i}",
                partition_count=5,
                replication_factor=random.choice([1, 3]),
            )
            for i in range(num_failover_topics)
        ]
        non_failover_topics = [
            TopicSpec(
                name=f"non-failover-topic-{i}",
                partition_count=5,
                replication_factor=random.choice([1, 3]),
            )
            for i in range(num_non_failover_topics)
        ]

        all_topics = failover_topics + non_failover_topics
        for topic in all_topics:
            self.source_default_client().create_topic(topic)

        count = 1000

        # Seed some data in the source cluster
        self._produce_to_topics(
            all_topics, self.source_cluster.service, expect_failures=False
        )

        # Wait for topics to be created in the target cluster
        for t in all_topics:
            self.target_cluster.service.wait_until(
                lambda: self.topic_exists_in_target(t.name),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Topic {t.name} not found in target cluster",
            )

        # Wait for data to be replicated
        for t in all_topics:
            consumer = KgoVerifierConsumerGroupConsumer(
                self.test_context,
                self.target_cluster.service,
                topic=t.name,
                group_name="test_group",
                msg_size=40,
                max_msgs=count,
                readers=1,
                nodes=self.preallocated_nodes,
            )
            try:
                consumer.start()
                consumer.wait()
            finally:
                consumer.stop()
                consumer.free()
        # Try producing to topics in shadow cluster, should fail
        # Policy violation
        self._produce_to_topics(
            non_failover_topics, self.target_cluster.service, expect_failures=True
        )

        with self._maybe_failure_injector(with_failures=with_failures):
            # Failover a subset of topics
            for topic in failover_topics:
                metadata = self.failover_link_topic(
                    link_name="test-link", topic=topic.name
                )
                self.logger.debug(f"Failover response: {metadata}")

                topic_status = [
                    s.status.state
                    for s in metadata.status.shadow_topics
                    if s.name == topic.name
                ]
                assert next(iter(topic_status), None) in [
                    shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILING_OVER,
                    shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILED_OVER,
                ], (
                    "Topic state should be FAILING_OVER or FAILED_OVER after failover request"
                )

                # Wait for topic to be marked as failed over
                self.wait_for_topic_status(
                    link="test-link",
                    topic=topic.name,
                    target_status=shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILED_OVER,
                )
                sleep(0.5)

        # Produce to failed over topics in target, should succeed
        self._produce_to_topics(
            failover_topics, self.target_cluster.service, expect_failures=False
        )
        # Produce to non-failed over topics in target, should still fail
        self._produce_to_topics(
            non_failover_topics, self.target_cluster.service, expect_failures=True
        )
        # Check non failover topics are still active
        for t in non_failover_topics:
            self.wait_for_topic_status(
                link="test-link",
                topic=t.name,
                target_status=shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_ACTIVE,
            )

    @cluster(num_nodes=7)
    @matrix(with_failures=[True, False])
    def test_link_failover(self, with_failures):
        self.create_link("test-link")
        num_topics = random.choice([0, 1, 3, 5, 10])
        if num_topics == 0:
            # To avoid warning of under allocated nodes
            # In this case no kgo nodes are needed
            self.test_context.cluster.alloc(ClusterSpec.simple_linux(1))
        topics = [
            TopicSpec(
                name=f"test-topic-{i}",
                partition_count=5,
                replication_factor=random.choice([1, 3]),
            )
            for i in range(num_topics)
        ]
        for t in topics:
            self.source_default_client().create_topic(t)

        self._produce_to_topics(
            topics, self.source_cluster.service, expect_failures=False
        )

        for t in topics:
            self.target_cluster.service.wait_until(
                lambda: self.topic_exists_in_target(t.name),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Topic {t.name} not found in target cluster",
            )

        self._produce_to_topics(
            topics, self.target_cluster.service, expect_failures=True
        )

        with self._maybe_failure_injector(with_failures=with_failures):
            # Let some failures kickin
            if with_failures:
                sleep(5)
            self.failover_link(name="test-link")
            self.wait_for_link_failover(link="test-link")

        self._produce_to_topics(
            topics, self.target_cluster.service, expect_failures=False
        )
