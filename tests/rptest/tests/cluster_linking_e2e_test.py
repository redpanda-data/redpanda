# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import ducktape.errors
import google.protobuf.timestamp_pb2
import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
import random
import re
import threading
import time
import json

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.cluster_spec import ClusterSpec
from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.mark import matrix
from ducktape.mark import ignore

from rptest.clients.admin.proto.redpanda.core.common.v1 import acl_pb2, tls_pb2
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
)
from rptest.clients.kafka_cli_tools import KafkaCliToolsError
from rptest.clients.rpk import (
    RpkPartition,
    RpkTool,
    RPKACLInput,
    RpkException,
    RpkGroup,
)
from rptest.clients.types import TopicSpec
from rptest.services.cluster import TestContext
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.multi_cluster_services import (
    Cluster,
    RedpandaCluster,
    MultiClusterServices,
    SecondaryClusterArgs,
    SecondaryClusterSpec,
    ServiceType,
    Service as MultiService,
)
from rptest.services.redpanda import (
    CLOUD_TOPICS_CONFIG_STR,
    MetricSamples,
    MetricsEndpoint,
    RedpandaService,
    SchemaRegistryConfig,
    SecurityConfig,
    SISettings,
)
from rptest.services.tls import TLSCertManager
from rptest.tests.cluster_linking_test_base import (
    CONTROLLER_LOCKED_TASKS,
    DEFAULT_SYNCED_TOPIC_PROPERTIES,
    DISALLOWED_SYNCED_TOPIC_PROPERTIES,
    REQUIRED_SYNCED_TOPIC_PROPERTIES,
    ClusterLinkingTLSProvider,
    ShadowLinkPreAllocTestBase,
    ShadowLinkTestBase,
)
from rptest.tests.full_disk_test import FDT_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (
    expect_exception,
    wait_until,
    wait_until_result,
)
from rptest.utils.full_disk import FullDiskHelper
from typing import Any, Callable, Optional
from time import sleep
import google.protobuf.duration_pb2

FDT_CL_REPLICATION_REJECTION = [
    re.compile(
        ".*Error in fetch_and_replicate.*no disk space; free bytes less than configurable threshold\\)"
    )
]


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
    def _expect_connect_error(self, expected_code: ConnectErrorCode):
        return expect_exception(ConnectError, lambda e: e.code == expected_code)

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
    def test_create_default_link(self):
        """
        This test creates a Shadow Link with all default values and
        verifies that the default values are what are in use
        """
        link_request = self.create_default_link_request(
            link_name="test-link",
            mirror_all_acls=False,
            mirror_all_groups=False,
            mirror_all_topics=False,
        )
        link_request.shadow_link.configurations.topic_metadata_sync_options.interval.CopyFrom(
            google.protobuf.duration_pb2.Duration(seconds=0)
        )
        link_request.shadow_link.configurations.consumer_offset_sync_options.interval.CopyFrom(
            google.protobuf.duration_pb2.Duration(seconds=0)
        )
        link_request.shadow_link.configurations.security_sync_options.interval.CopyFrom(
            google.protobuf.duration_pb2.Duration(seconds=0)
        )

        shadow_link = self.create_link_with_request(req=link_request)

        self.logger.info(f"Shadow link configurations: {shadow_link.configurations}")

        client_options = shadow_link.configurations.client_options
        assert client_options.metadata_max_age_ms == 0, (
            f"Expected 0, got {client_options.metadata_max_age_ms}"
        )
        assert client_options.effective_metadata_max_age_ms == 10000, (
            f"Expected 10000, got {client_options.effective_metadata_max_age_ms}"
        )
        assert client_options.connection_timeout_ms == 0, (
            f"Expected 0, got {client_options.connection_timeout_ms}"
        )
        assert client_options.effective_connection_timeout_ms == 1000, (
            f"Expected 1000, got {client_options.effective_connection_timeout_ms}"
        )
        assert client_options.retry_backoff_ms == 0, (
            f"Expected 0, got {client_options.retry_backoff_ms}"
        )
        assert client_options.effective_retry_backoff_ms == 100, (
            f"Expected 100, got {client_options.effective_retry_backoff_ms}"
        )
        assert client_options.fetch_wait_max_ms == 0, (
            f"Expected 0, got {client_options.fetch_wait_max_ms}"
        )
        assert client_options.effective_fetch_wait_max_ms == 500, (
            f"Expected 500, got {client_options.effective_fetch_wait_max_ms}"
        )
        assert client_options.fetch_min_bytes == 0, (
            f"Expected 0, got {client_options.fetch_min_bytes}"
        )
        assert client_options.effective_fetch_min_bytes == (5 * 1024 * 1024), (
            f"Expected {5 * 1024 * 1024}, got {client_options.effective_fetch_min_bytes}"
        )
        assert client_options.fetch_max_bytes == 0, (
            f"Expected 0, got {client_options.fetch_max_bytes}"
        )
        assert client_options.effective_fetch_max_bytes == (20 * 1024 * 1024), (
            f"Expected {20 * 1024 * 1024}, got {client_options.effective_fetch_max_bytes}"
        )
        assert client_options.fetch_partition_max_bytes == 0, (
            f"Expected 0, got {client_options.fetch_partition_max_bytes}"
        )
        assert client_options.effective_fetch_partition_max_bytes == (
            5 * 1024 * 1024
        ), (
            f"Expected {5 * 1024 * 1024}, got {client_options.effective_fetch_partition_max_bytes}"
        )

        topic_metadata_config = shadow_link.configurations.topic_metadata_sync_options
        assert topic_metadata_config.interval == google.protobuf.duration_pb2.Duration(
            seconds=0
        ), f"Expected 0s, got {topic_metadata_config.interval}"
        assert (
            topic_metadata_config.effective_interval
            == google.protobuf.duration_pb2.Duration(seconds=30)
        ), f"Expected 30s, got {topic_metadata_config.effective_interval}"

        cg_config = shadow_link.configurations.consumer_offset_sync_options
        assert cg_config.interval == google.protobuf.duration_pb2.Duration(seconds=0), (
            f"Expected 0s, got {cg_config.interval}"
        )
        assert cg_config.effective_interval == google.protobuf.duration_pb2.Duration(
            seconds=30
        ), f"Expected 30s, got {cg_config.effective_interval}"

        security_config = shadow_link.configurations.security_sync_options
        assert security_config.interval == google.protobuf.duration_pb2.Duration(
            seconds=0
        ), f"Expected 0s, got {security_config.interval}"
        assert (
            security_config.effective_interval
            == google.protobuf.duration_pb2.Duration(seconds=30)
        ), f"Expected 30s, got {security_config.effective_interval}"

    @cluster(num_nodes=6)
    def test_create_simple_link(self):
        shadow_link = self.create_link("test-link")
        self.logger.info(f"Create shadow link result: {shadow_link}")

        links = self.list_links()
        assert len(links) == 1, f"Expected exactly one shadow link, got {len(links)}"

        test_link = links[0]
        assert test_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {test_link.name}"
        )

        active = shadow_link_pb2.ShadowLinkState.SHADOW_LINK_STATE_ACTIVE
        link_state = test_link.status.state
        assert link_state == active, (
            f"Expected shadow link state to be '{active}', got {link_state}"
        )

        link_uid = test_link.uid
        assert link_uid, "Expected some uid for shadow link"

        got_link = self.get_link(name="test-link")
        assert got_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {got_link.name}"
        )

        assert got_link.uid == link_uid, (
            f"Expected shadow link uid to be '{link_uid}', got {got_link.uid}"
        )

        # Retrieving a non-existent link should fail
        with self._expect_connect_error(ConnectErrorCode.NOT_FOUND):
            self.get_link(name="non-existent-link")

        task_statuses = got_link.status.task_statuses
        self.logger.info(f"Shadow link task_statuses: {task_statuses}")

        # Get the controller leader
        leader_id = Admin(self.target_cluster_service).get_partition_leader(
            namespace="redpanda", topic="controller", partition=0
        )

        for task in task_statuses:
            if task.name in CONTROLLER_LOCKED_TASKS:
                assert task.state == shadow_link_pb2.TASK_STATE_ACTIVE, (
                    f'Expected task "{task.name}" to be running, got {task.state}'
                )
                assert task.broker_id == leader_id, (
                    f'Expected task "{task.name}" to be running on controller node {leader_id} not {task.broker_id}'
                )
                assert task.shard == 0, (
                    f'Expected task "{task.name}" to be running on shard 0 not {task.shard}'
                )

    @cluster(num_nodes=6)
    def test_task_states_change(self):
        topic = TopicSpec(name="test-topic", partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        wait_until(
            lambda: self._topics_are_present_in_target_cluster([topic]),
            timeout_sec=20,
            err_msg="Failed to find topic in target cluster",
        )

        def _wait_for_controller_tasks_state(
            expected_state: shadow_link_pb2.TaskState.ValueType,
        ) -> bool:
            # Get the controller leader
            leader_id = Admin(self.target_cluster_service).get_partition_leader(
                namespace="redpanda", topic="controller", partition=0
            )
            task_statuses = self.get_link("test-link").status.task_statuses
            self.logger.debug(f"Task statuses: {task_statuses}")
            for task in task_statuses:
                if task.name in CONTROLLER_LOCKED_TASKS:
                    assert task.broker_id == leader_id, (
                        f'Expected task "{task.name}" to be running on controller node {leader_id} not {task.broker_id}'
                    )
                    assert task.shard == 0, (
                        f'Expected task "{task.name}" to be running on shard 0 not {task.shard}'
                    )
                    if task.state != expected_state:
                        return False
            return True

        wait_until(
            lambda: _wait_for_controller_tasks_state(shadow_link_pb2.TASK_STATE_ACTIVE),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Controller locked tasks did not become active",
        )

        # Now shut down the source cluster
        self.source_cluster.stop()

        wait_until(
            lambda: _wait_for_controller_tasks_state(
                shadow_link_pb2.TASK_STATE_LINK_UNAVAILABLE
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Controller locked tasks did not become link unavailable",
        )

        # Now restart and expect things to recover
        self.source_cluster.start()
        wait_until(
            lambda: _wait_for_controller_tasks_state(shadow_link_pb2.TASK_STATE_ACTIVE),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Controller locked tasks did not become active after source cluster restart",
        )

    @cluster(num_nodes=6)
    def test_can_not_create_more_than_one_link(self):
        shadow_link = self.create_link("test-link")

        assert shadow_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {shadow_link.name}"
        )

        # Attempting to create a second one with the same name should fail
        with self._expect_connect_error(ConnectErrorCode.ALREADY_EXISTS):
            self.create_link("test-link")

        # Attempting to create a second link should fail.
        # Only one link is supported per cluster
        with self._expect_connect_error(ConnectErrorCode.RESOURCE_EXHAUSTED):
            self.create_link("test-link-2")

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

        with self._expect_connect_error(ConnectErrorCode.NOT_FOUND):
            self.get_shadow_topic(
                shadow_link_name="test-link", shadow_topic_name="non-existent-topic"
            )

    @cluster(num_nodes=6)
    def test_topic_creation_restriction(self):
        """
        Test validates that when cluster linking is active, that topics can only be created by superusers
        """
        username = "test-user"
        password = "test-password0"
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
        shadow_link.configurations.client_options.fetch_wait_max_ms = 100
        shadow_link.configurations.client_options.fetch_min_bytes = 10
        shadow_link.configurations.client_options.fetch_partition_max_bytes = (
            500 * 1024 * 1024
        )
        shadow_link.configurations.client_options.metadata_max_age_ms = 500
        shadow_link.configurations.client_options.connection_timeout_ms = 100
        shadow_link.configurations.client_options.retry_backoff_ms = 200
        shadow_link.configurations.client_options.fetch_max_bytes = 100 * 1024 * 1024
        update_mask: google.protobuf.field_mask_pb2.FieldMask = google.protobuf.field_mask_pb2.FieldMask(
            paths=[
                "configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters",
                "configurations.client_options.fetch_partition_max_bytes",
                "configurations.client_options.fetch_wait_max_ms",
                "configurations.client_options.fetch_min_bytes",
                "configurations.client_options.metadata_max_age_ms",
                "configurations.client_options.connection_timeout_ms",
                "configurations.client_options.retry_backoff_ms",
                "configurations.client_options.fetch_max_bytes",
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
        assert (
            updated_link.configurations.client_options.effective_fetch_wait_max_ms
            == shadow_link.configurations.client_options.fetch_wait_max_ms
        ), (
            f"Expected fetch_wait_max_ms to be {shadow_link.configurations.client_options.fetch_wait_max_ms}, got {updated_link.configurations.client_options.effective_fetch_wait_max_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_fetch_min_bytes
            == shadow_link.configurations.client_options.fetch_min_bytes
        ), (
            f"Expected fetch_min_bytes to be {shadow_link.configurations.client_options.fetch_min_bytes}, got {updated_link.configurations.client_options.effective_fetch_min_bytes}"
        )
        assert (
            updated_link.configurations.client_options.effective_fetch_partition_max_bytes
            == shadow_link.configurations.client_options.fetch_partition_max_bytes
        ), (
            f"Expected fetch_partition_max_bytes to be {shadow_link.configurations.client_options.fetch_partition_max_bytes}, got {updated_link.configurations.client_options.effective_fetch_partition_max_bytes}"
        )
        assert (
            updated_link.configurations.client_options.effective_metadata_max_age_ms
            == shadow_link.configurations.client_options.metadata_max_age_ms
        ), (
            f"Expected metadata_max_age_ms to be {shadow_link.configurations.client_options.metadata_max_age_ms}, got {updated_link.configurations.client_options.effective_metadata_max_age_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_connection_timeout_ms
            == shadow_link.configurations.client_options.connection_timeout_ms
        ), (
            f"Expected connection_timeout_ms to be {shadow_link.configurations.client_options.connection_timeout_ms}, got {updated_link.configurations.client_options.effective_connection_timeout_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_retry_backoff_ms
            == shadow_link.configurations.client_options.retry_backoff_ms
        ), (
            f"Expected retry_backoff_ms to be {shadow_link.configurations.client_options.retry_backoff_ms}, got {updated_link.configurations.client_options.effective_retry_backoff_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_fetch_max_bytes
            == shadow_link.configurations.client_options.fetch_max_bytes
        ), (
            f"Expected fetch_max_bytes to be {shadow_link.configurations.client_options.fetch_max_bytes}, got {updated_link.configurations.client_options.effective_fetch_max_bytes}"
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

    @cluster(
        num_nodes=6,
        log_allow_list=[
            re.compile(".*Cluster link table reporting that link does not exist.*")
        ],
    )
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

        with self._expect_connect_error(ConnectErrorCode.NOT_FOUND):
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

    @cluster(
        num_nodes=6,
        log_allow_list=[
            re.compile(
                ".*Failed to process mirror topic command: cluster::cluster_link::errc::feature_disabled.*"
            )
        ],
    )
    def test_rapid_shadow_link_toggling(self):
        self.create_link("test-link")

        def toggle_shadow_linking(times: int):
            state: bool = True
            for _ in range(times):
                state = not state
                self.target_cluster_service.set_cluster_config(
                    {"enable_shadow_linking": state}
                )

        toggle_thread = threading.Thread(target=toggle_shadow_linking, args=(100,))

        toggle_thread.start()
        topics: list[TopicSpec] = []
        for i in range(10):
            topic = TopicSpec(
                name=f"source-topic-{i}", partition_count=3, replication_factor=3
            )
            self.source_default_client().create_topic(topic)
            topics.append(topic)

        toggle_thread.join()

        self.target_cluster.service.wait_until(
            lambda: self._topics_are_present_in_target_cluster(topics),
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Failed to find all topics in target cluster",
        )

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
            lambda: self.topic_partitions_exists_in_target(topic),
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

        def get_source_topic_properties() -> tuple[bool, dict[str, tuple[str, str]]]:
            try:
                return True, self.source_cluster_rpk.describe_topic_configs(topic.name)
            except RpkException as e:
                self.logger.debug(f"Failed to get topic configs for {topic.name}: {e}")
                return False, {}

        source_topic = wait_until_result(
            get_source_topic_properties,
            timeout_sec=10,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in source cluster",
            retry_on_exc=True,
        )

        def validate_topic_properties(properties_to_check: dict[str, tuple[str, str]]):
            for key, val in topic_properties.items():
                assert str(properties_to_check[key][0]) == str(val), (
                    f"Expected {key} to be {str(val)}, got {str(properties_to_check[key][0])}.  Full config: {properties_to_check}"
                )

        validate_topic_properties(source_topic)

        shadow_link = self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
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

    @cluster(num_nodes=6)
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
    )
    def test_link_creation_checks(self, source_cluster_spec):
        """
        Checks that preflight checks during link creation work as expected. Particularly
        creating links where the remote cluster is not reachable due to connectivity issues
        or incorrect configurations.
        """
        # Test incorrect bootstrap servers
        bad_bootstrap_servers = [
            "non.existent.server:9092",
            "one.more.bad:9092",
            "localhost:1234",
        ]
        bad_link_request = self.create_default_link_request("bad-link")
        bad_link_request.shadow_link.configurations.client_options.bootstrap_servers[
            :
        ] = bad_bootstrap_servers

        with self._expect_connect_error(ConnectErrorCode.FAILED_PRECONDITION):
            self.create_link_with_request(req=bad_link_request)

        # Test invalid TLS settings, source cluster has no TLS
        bad_link_request = self.create_default_link_request("bad-link-tls")
        bad_link_request.shadow_link.configurations.client_options.tls_settings.CopyFrom(
            tls_pb2.TLSSettings(
                enabled=True,
                tls_file_settings=tls_pb2.TLSFileSettings(
                    ca_path=self.redpanda.TLS_CA_CRT_FILE,
                    key_path=self.redpanda.TLS_SERVER_KEY_FILE,
                    cert_path=self.redpanda.TLS_SERVER_CRT_FILE,
                ),
            )
        )
        with self._expect_connect_error(ConnectErrorCode.FAILED_PRECONDITION):
            self.create_link_with_request(req=bad_link_request)

        # Kill one broker on the source cluster to simulate partial connectivity
        # during preflight checks
        node_to_stop = self.source_cluster._service.get_node(idx=1)
        self.source_cluster._service.stop_node(node_to_stop)
        self.create_link("link-with-partial-connectivity")

    @cluster(num_nodes=6)
    def test_link_creation_incompatible_api(self):
        """
        Tests that link creation fails when the source cluster has an incompatible
        kafka API support.
        """
        # Downgrade the source cluster to a version that does not support
        # v10 of metadata request used for cluster linking
        assert isinstance(self.source_cluster.service, RedpandaService), (
            "Invalid source cluster service type"
        )
        rp = self.source_cluster.service
        rp._installer.install(rp.nodes, (25, 1))
        rp.for_nodes(rp.nodes, lambda node: rp.stop_node(node))
        rp.start(rp.nodes, clean_nodes=True)
        with self._expect_connect_error(ConnectErrorCode.FAILED_PRECONDITION):
            self.create_link("link-to-incompatible-cluster")

    @cluster(
        num_nodes=6,
        log_allow_list=FDT_LOG_ALLOW_LIST + FDT_CL_REPLICATION_REJECTION,
    )
    def test_produce_guards(self):
        test_link = "test-link"
        self.create_link(test_link)

        topic = TopicSpec(name="test-topic", partition_count=3, replication_factor=1)
        self.source_default_client().create_topic(topic)
        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        source_rpk = RpkTool(self.source_cluster.service)
        target_rpk = RpkTool(self.target_cluster.service)

        def verify_n_replications(n_expected_logs: int) -> bool:
            desc = list(target_rpk.describe_topic(topic.name))
            n_logs: int = 0
            for partition in desc:
                n_logs += partition.high_watermark
            return n_logs == n_expected_logs

        source_rpk.produce(topic.name, "key", "message1")

        wait_until(
            lambda: verify_n_replications(1),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to replicate shadow topic",
        )

        target_redpanda = self.target_cluster_service
        target_full_disk = FullDiskHelper(self.logger, target_redpanda)
        target_full_disk.trigger_low_space()
        wait_until(
            lambda: target_redpanda.search_log_all("ok -> degraded"),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to change to degraded state",
        )

        source_rpk.produce(topic.name, "key", "message2")
        wait_until(
            lambda: target_redpanda.search_log_any(
                "Replication rejected on {kafka/test-topic/1}. no disk space;"
            ),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Sink replication should have been rejected",
        )

        assert verify_n_replications(1), (
            "Sink replication was not rejected under degraded disk conditions"
        )
        target_full_disk.clear_low_space()

        wait_until(
            lambda: verify_n_replications(2),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to replicate shadow topic",
        )

    @cluster(num_nodes=6)
    def test_no_wasm_deploy_on_shadow_topic(self):
        self.target_cluster_service.set_cluster_config(
            {"data_transforms_enabled": True}, expect_restart=True
        )
        self.create_link("test-link")
        topic = TopicSpec(name="test-topic", partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic)

        wait_until(
            lambda: self._topics_are_present_in_target_cluster([topic]),
            timeout_sec=20,
            err_msg="Failed to find topic in target cluster",
        )

        shadow_topic = self.get_shadow_topic("test-link", topic.name)
        assert shadow_topic.status.state == shadow_link_pb2.SHADOW_TOPIC_STATE_ACTIVE, (
            f"Expected shadow topic to be active, got {shadow_topic.status.state}"
        )

        self.target_cluster_rpk.create_topic(
            topic="wasm-input", partitions=3, replicas=3
        )

        with expect_exception(RpkException, lambda _: True):
            # Now attempt to create a wasm targeting the shadow topic
            self.target_cluster_rpk.deploy_wasm(
                "test-wasm", "wasm-input", [topic.name], file="tinygo/identity.wasm"
            )

    def _execute_task_pausing(self, num_topics: int):
        link_name = "test-link"
        created_link = self.create_link(link_name=link_name)

        for i in range(num_topics):
            # First disable the task
            created_link.configurations.topic_metadata_sync_options.paused = True
            update_mask = google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.topic_metadata_sync_options.paused"]
            )
            self.logger.debug("Disabling topic_metadata_sync task")
            self.update_link(shadow_link=created_link, update_mask=update_mask)

            topic_name = f"source-topic-{i}"
            self.logger.debug(f"Creating topic {topic_name} in source cluster")
            topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=3)
            self.source_default_client().create_topic(topic)

            self.logger.debug(
                f"Verifying that topic {topic_name} is NOT created in target cluster"
            )
            # Verify that the topic is NOT created in the target cluster
            with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
                self.target_cluster.service.wait_until(
                    lambda: self.topic_exists_in_target(topic_name),
                    timeout_sec=10,
                    backoff_sec=1,
                )

            # Now re-enable the task
            created_link.configurations.topic_metadata_sync_options.paused = False
            update_mask = google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.topic_metadata_sync_options.paused"]
            )
            self.logger.debug("Enabling topic_metadata_sync task")
            self.update_link(shadow_link=created_link, update_mask=update_mask)

            self.logger.debug(
                f"Verifying that topic {topic_name} IS created in target cluster"
            )
            self.target_cluster.service.wait_until(
                lambda: self.topic_partitions_exists_in_target(topic),
                timeout_sec=10,
                backoff_sec=1,
            )

    @cluster(num_nodes=6)
    @matrix(shuffle_leadership=[True, False])
    def test_task_pausing(self, shuffle_leadership: bool):
        """
        This test will verify that the pausing and resuming of shadow linking tasks
        works as expected.  The test will create 10 topics, one at a time, pausing
        and unpausing the source topic syncer task and verify that the topic is/is not
        created in the target cluster as expected
        """
        num_topics = 5
        with self.leadership_shuffler(
            redpanda=self.target_cluster.service,
            namespace="redpanda",
            topic="controller",
            enabled=shuffle_leadership,
        ):
            self._execute_task_pausing(num_topics=num_topics)

    @cluster(num_nodes=6)
    def test_remove_shadow_topic_escape_hatch(self):
        """
        This test verifies that the escape hatch to remove the shadow topic from the shadow link
        works as expected
        """
        topic_name = "test-topic"
        topic = TopicSpec(name=topic_name, partition_count=1, replication_factor=1)
        self.source_default_client().create_topic(topic)

        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        self.source_cluster_rpk.produce(topic.name, "key", "message1")

        def wait_for_replication():
            k, v = tuple(
                self.target_cluster_rpk.consume(
                    topic=topic.name, n=1, format="%k,%v"
                ).split(",")
            )
            self.logger.debug(f"Consumed key={k}, value={v} from shadow topic")
            return k == "key" and v == "message1"

        self.target_cluster.service.wait_until(
            wait_for_replication,
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Failed to replicate message to shadow topic",
        )

        self.logger.info("Force remove shadow topic from shadow link")

        self.remove_shadow_topic(
            shadow_link_name="test-link", shadow_topic_name=topic_name
        )

        topics = self.list_shadow_topics("test-link")
        assert len(topics) == 0, f"Expected no shadow topics, got {topics}"

    @cluster(num_nodes=6)
    def test_change_shadow_topic_state(self):
        """
        This test verifies that changing the shadow topic state works as expected
        """
        topic_name = "test-topic"
        topic = TopicSpec(name=topic_name, partition_count=1, replication_factor=1)
        self.source_default_client().create_topic(topic)

        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        self.logger.info("Pausing shadow topic replication")
        self.force_update_shadow_topic_state(
            shadow_link_name="test-link",
            shadow_topic_name=topic_name,
            new_state=shadow_link_pb2.SHADOW_TOPIC_STATE_PAUSED,
        )

        shadow_topic = self.get_shadow_topic("test-link", topic_name)
        assert shadow_topic.status.state == shadow_link_pb2.SHADOW_TOPIC_STATE_PAUSED, (
            f"Expected shadow topic to be paused, got {shadow_topic.status.state}"
        )

        self.logger.info("Resuming shadow topic replication")
        self.force_update_shadow_topic_state(
            shadow_link_name="test-link",
            shadow_topic_name=topic_name,
            new_state=shadow_link_pb2.SHADOW_TOPIC_STATE_ACTIVE,
        )

        shadow_topic = self.get_shadow_topic("test-link", topic_name)
        assert shadow_topic.status.state == shadow_link_pb2.SHADOW_TOPIC_STATE_ACTIVE, (
            f"Expected shadow topic to be active, got {shadow_topic.status.state}"
        )


class ShadowLinkSmallerShadowCluster(ShadowLinkTestBase):
    """
    Tests for when the Shadow Cluster is smaller than the source cluster
    """

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, num_brokers=1, *args, **kwargs)

    def _expect_connect_error(self, expected_code: ConnectErrorCode):
        return expect_exception(ConnectError, lambda e: e.code == expected_code)

    @cluster(num_nodes=4)
    def test_warn_on_smaller_cluster(self):
        self.create_link("test-link")
        assert self.target_cluster_service.search_log_any(
            "Cluster link 'test-link' connecting to source cluster with 3 brokers, which is more than the shadow cluster's 1 nodes"
        ), "Did not find expected warning about smaller shadow cluster"


class ShadowLinkingAuthzTests(ShadowLinkTestBase):
    SUPERUSER_ERROR = "[permission_denied] Forbidden (superuser role required)"

    def expect_superuser_error(self):
        return expect_exception(
            ConnectError,
            lambda e: str(e) == self.SUPERUSER_ERROR,
        )

    @cluster(num_nodes=6)
    def test_shadow_link_admin_api_authz(self):
        topic = TopicSpec(name="source-topic", partition_count=3, replication_factor=1)
        self.source_default_client().create_topic(topic)

        link_name = "test-link"

        shadow_link = self.create_link(link_name)
        links = self.list_links()
        assert len(links) == 1, (
            f"Expected exactly one shadow link, got {len(links)}. Setup failed"
        )

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        self.target_cluster_service.set_cluster_config(
            values={"admin_api_require_auth": True}
        )

        # Verify that an unauthorised user can't access any of the sl api
        with self.expect_superuser_error():
            self.create_link(link_name)

        with self.expect_superuser_error():
            self.list_links()

        with self.expect_superuser_error():
            self.get_link(link_name)

        with self.expect_superuser_error():
            self.update_link(shadow_link)

        with self.expect_superuser_error():
            self.failover_link(link_name)

        with self.expect_superuser_error():
            self.get_shadow_topic(link_name, topic.name)

        with self.expect_superuser_error():
            self.list_shadow_topics(link_name)

        with self.expect_superuser_error():
            self.delete_link(link_name)

        # And a spot check that a superuser can actually use them
        with self.superuser_access():
            links = self.list_links()
            assert len(links) == 1, (
                f"Expected exactly one shadow link, got {len(links)}."
            )


class ShadowLinkingReplicationTests(ShadowLinkPreAllocTestBase):
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
                timeout_sec=60,
                err_msg=f"Shadow topic {shadow_topic_name} not found or does not have expected {expected_partitions} partitions",
            )
        except ducktape.errors.TimeoutError as e:
            self.logger.debug(f"Timeout fetching shadow topic {shadow_topic_name}: {e}")
            return False

        return self._check_partitions_match(rpk, shadow_topic_name, shadow_topic)

    @cluster(num_nodes=7)
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
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )
        with self.leadership_shuffler(
            self.target_cluster.service, topic.name, enabled=shuffle_leadership
        ):
            with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000):
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
        num_nodes=7,
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
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000):
            with (
                self.create_source_failure_injector(),
                self.create_target_failure_injector(),
            ):
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

    @cluster(num_nodes=7)
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
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )
        with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000):
            self.verify()

        target_client = self.target_default_client()

        # topic is not deletable as it is covered by the shadow topic autocreate filters
        with expect_exception(
            KafkaCliToolsError, lambda e: "PolicyViolationException" in str(e)
        ):
            target_client.delete_topic(topic.name)

        def update_link_config(include: bool) -> None:
            shadow_link = self.get_link("test-link")
            shadow_link.configurations.topic_metadata_sync_options.ClearField(
                "auto_create_shadow_topic_filters"
            )
            filter_type = (
                shadow_link_pb2.FILTER_TYPE_INCLUDE
                if include
                else shadow_link_pb2.FILTER_TYPE_EXCLUDE
            )
            shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
                [
                    shadow_link_pb2.NameFilter(
                        pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                        filter_type=filter_type,
                        name=topic.name,
                    ),
                ]
            )
            update_mask: google.protobuf.field_mask_pb2.FieldMask = (
                google.protobuf.field_mask_pb2.FieldMask(
                    paths=["configurations.topic_metadata_sync_options"]
                )
            )
            self.update_link(shadow_link=shadow_link, update_mask=update_mask)

        # Update the link to exclude the topic from autocreation filters
        update_link_config(include=False)
        # Now the topic should be deletable, as it is not in the autocreate filters
        target_client.delete_topic(topic.name)
        link_state = self.get_link("test-link")
        assert len(link_state.status.shadow_topics) == 0, (
            "Expected empty shadow_topic list. "
            f"Instead got {link_state.status.shadow_topics}"
        )
        # Re-add the topic to the autocreation filters
        update_link_config(include=True)
        # Verify that the shadow topic is re-created
        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster after re-adding to autocreation filters",
        )

        # Replicate more data to ensure replication still works
        with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=200000):
            self.verify()

    @cluster(num_nodes=7)
    def test_replication_with_transactions(self):
        topic = TopicSpec(name="source-topic", partition_count=1, replication_factor=3)

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        with self.producer_consumer(
            topic=topic.name,
            msg_size=128,
            msg_cnt=10000,
            use_transactions=True,
            producer_properties={"transaction_abort_rate": "0.3"},
        ):
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
            lambda: self.topic_partitions_exists_in_target(topic),
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

    def _perform_auto_prefix_trimming(self, topic_name: str, partition_count: int):
        offsets = [1000, 1001, 1200, 1500, 2000, 2500]

        def wait_for_records(rpk: RpkTool, offset: int, expected_partition_count: int):
            num_parts = 0
            for part in rpk.describe_topic(topic_name):
                num_parts += 1
                if (part.high_watermark or 0) < offset:
                    return False
            return num_parts == expected_partition_count

        partitions = list(range(partition_count))

        for o in offsets:
            self.source_cluster.service.wait_until(
                lambda: wait_for_records(
                    self.source_cluster_rpk,
                    offset=o,
                    expected_partition_count=partition_count,
                ),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Timed out waiting for {o} records in each partition",
            )

            self.logger.info(f"Trimming source topic prefixes to {o}")
            self.source_cluster_rpk.trim_prefix(
                topic=topic_name, partitions=partitions, offset=o
            )

            def wait_for_start_offset(
                rpk: RpkTool, offset: int, expected_partition_count: int
            ):
                num_parts = 0
                for part in rpk.describe_topic(topic_name):
                    num_parts += 1
                    self.logger.info(
                        f"Offset for source-topic/{part.id} is {part.start_offset}"
                    )
                    if (part.start_offset or 0) != offset:
                        return False
                return num_parts == expected_partition_count

            self.source_cluster.service.wait_until(
                lambda: wait_for_start_offset(
                    self.source_cluster_rpk,
                    offset=o,
                    expected_partition_count=partition_count,
                ),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Timed out waiting for start offset to be {o} in each partition",
            )

            # Produce a single message to ensure cluster linking picks up the trim
            for part in range(0, partition_count):
                self.logger.info(f"Producing trim-trigger message to partition {part}")
                self.source_cluster_rpk.produce(
                    topic=topic_name,
                    key="trim-trigger",
                    msg="trim-trigger",
                    partition=part,
                )

            self.logger.info(
                f"Now waiting for target cluster to get to {o} starting offset"
            )

            self.target_cluster.service.wait_until(
                lambda: wait_for_start_offset(
                    self.target_cluster_rpk,
                    offset=o,
                    expected_partition_count=partition_count,
                ),
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Timed out waiting for target to get start offset to be {o} in each partition",
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
    def test_auto_prefix_trimming(self, with_failures, source_cluster_spec):
        partition_count = 5
        topic = TopicSpec(
            name="source-topic", partition_count=partition_count, replication_factor=3
        )

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        with self._maybe_failure_injector(with_failures):
            with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=100000):
                self._perform_auto_prefix_trimming(topic.name, partition_count)

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
    def test_start_offset_catch_up(self, with_failures, source_cluster_spec):
        """
        Test that verifies shadow link can catch up to a source topic that has been
        prefix-trimmed to its HWM (i.e., all data has been trimmed).

        1. Create a source topic with 5 partitions
        2. Write data to the topic across all partitions
        3. Trim the prefix of each partition of the source topic to the partition's HWM
        4. Create a new Shadow Link on the Shadow Cluster
        5. Wait for the shadow topic to be created on the Shadow Cluster
        6. Verify that the start offset and HWM of all shadow partitions match the source partitions
        7. Write data to the source partitions
        8. Verify that the shadow partitions replicate that data
        """
        partition_count = 5
        topic = TopicSpec(
            name="source-topic", partition_count=partition_count, replication_factor=3
        )
        self.source_default_client().create_topic(topic)

        # Step 2: Write data to the topic across all partitions
        initial_msg_count = 1000
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic=topic.name,
            msg_size=128,
            msg_count=initial_msg_count,
            custom_node=self.preallocated_nodes,
        )

        # Wait for all messages to be written (sum of HWMs across all partitions should equal msg_count)
        def all_messages_written():
            total_hwm = 0
            for part in self.source_cluster_rpk.describe_topic(topic.name):
                total_hwm += part.high_watermark or 0
            return total_hwm >= initial_msg_count

        self.source_cluster.service.wait_until(
            all_messages_written,
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Timed out waiting for {initial_msg_count} messages to be written",
        )

        # Step 3: Trim the prefix of each partition to its HWM
        # First, collect the HWM for each partition
        source_hwms: dict[int, int] = {}
        for part in self.source_cluster_rpk.describe_topic(topic.name):
            source_hwms[part.id] = part.high_watermark
            self.logger.info(f"Source partition {part.id}: HWM={part.high_watermark}")

        # Trim each partition to its HWM
        for part_id, hwm in source_hwms.items():
            self.logger.info(f"Trimming partition {part_id} to offset {hwm}")
            self.source_cluster_rpk.trim_prefix(
                topic=topic.name, offset=hwm, partitions=[part_id]
            )

        # Wait for the trim to take effect on all partitions
        def all_partitions_trimmed():
            for part in self.source_cluster_rpk.describe_topic(topic.name):
                expected_offset = source_hwms[part.id]
                if (part.start_offset or 0) != expected_offset:
                    self.logger.debug(
                        f"Partition {part.id}: start_offset={part.start_offset}, expected={expected_offset}"
                    )
                    return False
            return True

        self.source_cluster.service.wait_until(
            all_partitions_trimmed,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timed out waiting for prefix trim to take effect",
        )

        # Step 4: Create a new Shadow Link on the Shadow Cluster
        with self._maybe_failure_injector(with_failures):
            self.create_link("test-link")

            # Step 5: Wait for the shadow topic to be created on the Shadow Cluster
            self.target_cluster.service.wait_until(
                lambda: self.topic_partitions_exists_in_target(topic),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Topic {topic.name} not found in target cluster",
            )

            # Step 6: Verify that the start offset and HWM of all shadow partitions match the source partitions
            def shadow_partitions_match_source():
                target_parts = {
                    p.id: p for p in self.target_cluster_rpk.describe_topic(topic.name)
                }
                source_parts = {
                    p.id: p for p in self.source_cluster_rpk.describe_topic(topic.name)
                }

                if len(target_parts) != partition_count:
                    self.logger.debug(
                        f"Target partition count mismatch: {len(target_parts)} != {partition_count}"
                    )
                    return False

                for part_id in range(partition_count):
                    if part_id not in target_parts or part_id not in source_parts:
                        return False

                    target_part = target_parts[part_id]
                    source_part = source_parts[part_id]

                    # Start offset should match
                    if target_part.start_offset != source_part.start_offset:
                        self.logger.debug(
                            f"Partition {part_id}: target start_offset={target_part.start_offset}, "
                            f"source start_offset={source_part.start_offset}"
                        )
                        return False

                    # HWM should match (both should be equal to start_offset since topic was trimmed to HWM)
                    if target_part.high_watermark != source_part.high_watermark:
                        self.logger.debug(
                            f"Partition {part_id}: target HWM={target_part.high_watermark}, "
                            f"source HWM={source_part.high_watermark}"
                        )
                        return False

                return True

            self.target_cluster.service.wait_until(
                shadow_partitions_match_source,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Shadow partitions do not match source partitions after prefix trim",
            )

            # Log the final state after matching
            self.logger.info(
                "Shadow partitions match source partitions after prefix trim:"
            )
            for part in self.target_cluster_rpk.describe_topic(topic.name):
                self.logger.info(
                    f"  Partition {part.id}: start_offset={part.start_offset}, HWM={part.high_watermark}"
                )

            # Step 7 & 8: Write data to the source partitions and verify replication
            with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=10000):
                self.verify()

    @cluster(num_nodes=7)
    @matrix(
        timestamp_type=[
            "CreateTime",
            "LogAppendTime",
        ],
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
    )
    def test_replication_timestamps_match(self, timestamp_type, source_cluster_spec):
        partition_count = 1
        topic = TopicSpec(
            name="source-topic",
            partition_count=partition_count,
            replication_factor=3,
            message_timestamp_type=timestamp_type,
            retention_ms=-1,
        )

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )
        msg_cnt = 100
        base_ts = 1664453149000  # Thu Sep 29 2022 12:05:49 GMT
        with self.producer_consumer(
            topic=topic.name,
            msg_size=128,
            msg_cnt=msg_cnt,
            producer_properties={
                "fake_timestamp_ms": base_ts,
                "rate_limit_bps": 1024,
            },
        ):
            self.verify()

        def get_timestamps(rpk: RpkTool, n: int, offset: str):
            return {
                int(o): int(t)
                for o, t in [
                    tuple(s.split(","))
                    for s in rpk.consume(
                        topic=topic.name,
                        n=n,
                        offset=offset,
                        format="%o,%d\n",
                    ).splitlines()
                ]
            }

        expected_timestamps = get_timestamps(
            self.source_cluster_rpk, msg_cnt, offset="start"
        )

        consume_from = msg_cnt // 2
        n_to_consume = msg_cnt - consume_from
        consume_from_ts = expected_timestamps[msg_cnt // 2]

        consumed = get_timestamps(
            self.target_cluster_rpk, n=n_to_consume, offset=f"@{consume_from_ts}"
        )

        assert len(consumed) > 0, "No messages consumed"

        assert min(consumed) == consume_from, (
            f"Expected to {consume_from=}, but min consumed offset was {min(consumed)}"
        )

        assert all(ts == expected_timestamps[o] for o, ts in consumed.items()), (
            f"Timestamps don't match {expected_timestamps=} vs {consumed=}"
        )

    @cluster(num_nodes=7)
    def test_replication_with_large_msgs(self):
        msg_size = 2 * 1024 * 1024
        max_bytes = 10 * msg_size
        topic = TopicSpec(
            name="source-topic",
            partition_count=1,
            replication_factor=3,
            max_message_bytes=max_bytes,
        )

        self.source_default_client().create_topic(topic)
        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        with self.producer_consumer(
            topic=topic.name,
            msg_size=msg_size,
            msg_cnt=20,
            producer_properties={"batch_max_bytes": max_bytes},
        ):
            self.verify()

    @cluster(num_nodes=7)
    def test_replication_with_compaction(self):
        self.logger.info(
            "Create a topic with compaction settings set but without compaction and tombstone removal enabled"
        )
        topic = TopicSpec(
            name="compacted-topic",
            partition_count=1,
            replication_factor=3,
            segment_bytes=1024 * 1024,
            max_compaction_lag_ms=1000,
            min_cleanable_dirty_ratio=0.0,
        )
        self.source_default_client().create_topic(topic)

        req = self.create_default_link_request("test-link")
        req.shadow_link.configurations.topic_metadata_sync_options.synced_shadow_topic_properties.extend(
            ["segment.bytes", "min.cleanable.dirty.ratio"]
        )
        self.create_link_with_request(req=req)

        self.logger.info("Replicate some data with compactable keys and tombstones")
        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        with self.producer_consumer(
            topic=topic.name,
            msg_size=128,
            msg_cnt=10000,
            producer_properties={
                "key_set_cardinality": 600,
                "tombstone_probability": 0.4,
            },
        ):
            self.verify()

        def get_compaction_progress(
            rpk: RpkTool = self.target_cluster_rpk,
        ) -> tuple[int, int]:
            keys: list[str] = []
            tombstones = 0
            for line in rpk.consume(
                topic=topic.name,
                offset=":end",
                format="%k,%v\n",
            ).splitlines():
                key, value = line.split(",", maxsplit=1)
                keys += [key]
                if value == "":
                    tombstones += 1

            self.logger.info(
                f"Data read from target topic: {len(keys)=}, {keys[:5]=}, {tombstones=}"
            )
            return len(keys), tombstones

        self.logger.info("Verifying that replicated records can be compacted")
        pre_compaction_keys, _ = get_compaction_progress()
        self.source_default_client().alter_topic_configs(
            topic.name,
            {"cleanup.policy": "compact"},
        )

        def compaction_made_progress():
            post_compaction_keys, _ = get_compaction_progress()
            self.logger.info(
                f"Compaction progress check: {pre_compaction_keys=}, {post_compaction_keys=}"
            )
            return post_compaction_keys < pre_compaction_keys

        wait_until(
            compaction_made_progress,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Target topic compaction did not make progress",
        )

        self.logger.info("Verifying that replicated tombstones can be removed")
        _, pre_tombstone_removal_tombstones = get_compaction_progress()
        self.source_default_client().alter_topic_configs(
            topic.name,
            {"delete.retention.ms": "1000"},
        )

        def tombstone_removal_made_progress():
            _, post_tombstone_removal_tombstones = get_compaction_progress()
            self.logger.info(
                f"Tombstone removal progress check: {pre_tombstone_removal_tombstones=}, {post_tombstone_removal_tombstones=}"
            )
            return post_tombstone_removal_tombstones < pre_tombstone_removal_tombstones

        wait_until(
            tombstone_removal_made_progress,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Target topic tombstone removal did not make progress",
        )

        self.logger.info(
            "Verifying that compaction state is consistent on both clusters"
        )

        def compaction_states_consistent():
            source_keys, source_tombstones = get_compaction_progress(
                self.source_cluster_rpk
            )
            target_keys, target_tombstones = get_compaction_progress(
                self.target_cluster_rpk
            )
            self.logger.info(
                f"Compaction state check: {source_keys=}, {target_keys=}, {source_tombstones=}, {target_tombstones=}"
            )
            return source_keys == target_keys and source_tombstones == target_tombstones

        wait_until(
            compaction_states_consistent,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Compaction state is not consistent between clusters",
        )

    @cluster(num_nodes=7)
    def test_with_restart(self):
        self.create_link("test-link")

        def restart_nodes(service):
            for n in service.nodes:
                service.restart_nodes([n])
                time.sleep(5)

        topic_1 = TopicSpec(
            name="source-topic-1", partition_count=3, replication_factor=1
        )
        self.source_default_client().create_topic(topic_1)
        with self.producer_consumer(topic=topic_1.name, msg_size=128, msg_cnt=100000):
            restart_nodes(self.target_cluster_service)
            self.verify()

        topic_2 = TopicSpec(
            name="source-topic-2", partition_count=3, replication_factor=1
        )
        self.source_default_client().create_topic(topic_2)
        with self.producer_consumer(topic=topic_2.name, msg_size=128, msg_cnt=100000):
            restart_nodes(self.source_cluster_service)
            self.verify()


class ShadowLinkConsumeGroupsMirroringTest(ShadowLinkTestBase):
    def create_source_consumer(
        self,
        topic: str,
        group_name: str = "test_group",
        consumer_count: int = 1,
        continuous: bool = False,
    ):
        return KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.source_cluster.service,
            topic=topic,
            group_name=group_name,
            msg_size=128,
            readers=consumer_count,
            continuous=continuous,
        )

    def create_target_consumer(
        self,
        topic: str,
        group_name: str = "test_group",
        consumer_count: int = 1,
        continuous: bool = False,
    ):
        return KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.target_cluster.service,
            topic=topic,
            group_name=group_name,
            msg_size=128,
            readers=consumer_count,
            continuous=continuous,
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
        with_failures=[
            True,
            False,
        ],
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
        failure_duration = 10 if with_failures else 0

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
                return self.create_source_failure_injector(
                    max_suspend_duration_seconds=failure_duration
                )
            else:
                return self._nop_context_manager()

        def _consume_with_group(
            topic: str,
            group_id: str,
            fetch_max_wait: float,
            rpk: RpkTool = source_rpk,
            format: str | None = None,
        ) -> str | None:
            try:
                return rpk.consume(
                    topic=topic,
                    group=group_id,
                    n=1,
                    timeout=fetch_max_wait + 5,
                    offset="start",
                    fetch_max_wait=fetch_max_wait,
                    format=format,
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
                        self.logger.debug(
                            f"Group {g_name} partition {p.topic}/{p.partition} not present in target_cluster"
                        )
                        return False
                    if p.current_offset != t_partitions[(p.topic, p.partition)]:
                        self.logger.warn(
                            f"Group {g_name} partition {p.topic}/{p.partition} offsets differ: source {p.current_offset} vs target {t_partitions[(p.topic, p.partition)]}"
                        )
                        return False
            return True

        def _execute_random_updates(cnt: int):
            backoff_sec = 2
            backoff_and_a_bit_sec = backoff_sec + 1
            retries = 3
            fetch_max_wait_sec = failure_duration + 5
            fetch_timeout_sec = fetch_max_wait_sec + 5
            iteration_timeout_sec = (
                fetch_timeout_sec + backoff_and_a_bit_sec
            ) * retries
            for _ in range(cnt):
                topic = topics[random.randint(0, len(topics) - 1)].name
                group = groups[random.randint(0, len(groups) - 1)]
                self.logger.debug(f"Consuming from topic {topic}, group {group}")
                wait_until(
                    lambda: _consume_with_group(
                        topic,
                        group,
                        fetch_max_wait=fetch_max_wait_sec,
                    )
                    is not None,
                    timeout_sec=iteration_timeout_sec,
                    backoff_sec=backoff_sec,
                    err_msg=f"Failed to consume from topic {topic}, group {group}",
                )

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

            # now fail over all the topics and confirm that we start consuming at the right spot
            for topic in topics:
                metadata = self.failover_link_topic(
                    link_name="test-link", topic=topic.name
                )
                self.logger.debug(f"Failover response: {metadata}")
                t_status = [
                    s.status.state
                    for s in metadata.status.shadow_topics
                    if s.name == topic.name
                ]
                assert next(iter(t_status), None) in [
                    shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILING_OVER,
                    shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILED_OVER,
                ], (
                    "Topic state should be FAILING_OVER or FAILED_OVER after failover request"
                )
                self.wait_for_topic_status(
                    link="test-link",
                    topic=topic.name,
                    target_status=shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILED_OVER,
                )

            wait_until(
                lambda: _wait_for_group_states_consistent(),
                timeout_sec=120,
                backoff_sec=3,
                err_msg="Group states not consistent after failover",
                retry_on_exc=True,
            )

            target_groups: dict[str, RpkGroup] = {
                g: target_rpk.group_describe(group=g) for g in groups
            }

            for group_name, g_desc in target_groups.items():
                partitions: dict[tuple[str, int], int | None] = {
                    (p.topic, p.partition): p.current_offset for p in g_desc.partitions
                }

                assigned_topics = set(t for t, _ in partitions)

                # make sure we can consume from every topic in the group
                for topic in assigned_topics:
                    r = _consume_with_group(
                        topic,
                        group_name,
                        rpk=target_rpk,
                        format="%p,%o\n",
                        fetch_max_wait=5,
                    )
                    assert r is not None, f"Failed to consume from {group_name=}"
                    p, consumed = (int(v) for v in r.split(","))
                    # sanity check the result against group description
                    # assume the CG protocol works correctly for the rest of the partitions
                    expected = partitions[(topic, p)] or 0
                    assert consumed == expected, (
                        f"{group_name=}: {topic}/{p} {consumed=} but {expected=}"
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
    def test_consumer_group_rebalance(self, source_cluster_spec):
        partition_count = 120

        topic = TopicSpec(
            name="source-topic",
            partition_count=int(partition_count),
            replication_factor=3,
        )

        group = "test_group"
        self.create_link("test-link")
        source_rpk = RpkTool(self.source_cluster.service)
        target_rpk = RpkTool(self.target_cluster.service)

        n_messages = 1024 * 1024

        self.source_default_client().create_topic(topic)

        producer = KgoVerifierProducer(
            self.test_context,
            self.source_cluster.service,
            topic.name,
            128,
            n_messages,
            rate_limit_bps=1024,
        )
        producer.start()

        n_consumers = 10

        def group_is_ready(rpk: RpkTool):
            gr = rpk.group_describe(group=group, summary=True)
            return gr.members == n_consumers and gr.state == "Stable"

        consumer = self.create_source_consumer(
            topic.name,
            group_name=group,
            consumer_count=n_consumers,
            continuous=True,
        )
        try:
            consumer.start()
            wait_until(
                lambda: group_is_ready(source_rpk),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Group never stabilized on source cluster",
            )
        finally:
            consumer.stop()
            consumer.free()

        metadata = self.failover_link_topic(link_name="test-link", topic=topic.name)
        self.logger.debug(f"Failover response: {metadata}")
        t_status = [
            s.status.state
            for s in metadata.status.shadow_topics
            if s.name == topic.name
        ]
        assert next(iter(t_status), None) in [
            shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILING_OVER,
            shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILED_OVER,
        ], "Topic state should be FAILING_OVER or FAILED_OVER after failover request"
        self.wait_for_topic_status(
            link="test-link",
            topic=topic.name,
            target_status=shadow_link_pb2.ShadowTopicState.SHADOW_TOPIC_STATE_FAILED_OVER,
        )

        consumer = self.create_target_consumer(
            topic.name,
            group_name=group,
            consumer_count=n_consumers,
            continuous=True,
        )
        try:
            consumer.start()
            wait_until(
                lambda: group_is_ready(target_rpk),
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Group never stabilized on target cluster",
            )
            consumer.wait()
        finally:
            consumer.stop()
            consumer.free()

        producer.stop()
        producer.free()


class ShadowLinkConsumerGroupPartitionCountMismatchTest(ShadowLinkTestBase):
    """
    Verifies that consumer group offset mirroring works correctly when the
    __consumer_offsets topic has different partition counts on the source and
    target clusters.
    """

    # Use asymmetric partition counts: 8 on source, 32 on target.
    # This ensures groups map to different __consumer_offsets partitions on
    # each side, exercising the logical-offset-forwarding design.
    SOURCE_GROUP_TOPIC_PARTITIONS = 8
    TARGET_GROUP_TOPIC_PARTITIONS = 32

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context=test_context,
            num_prealloc_nodes=1,
            secondary_cluster_args=SecondaryClusterArgs(
                extra_rp_conf={
                    "group_topic_partitions": self.SOURCE_GROUP_TOPIC_PARTITIONS,
                },
            ),
            extra_rp_conf={
                "group_topic_partitions": self.TARGET_GROUP_TOPIC_PARTITIONS,
            },
            *args,
            **kwargs,
        )

    @cluster(num_nodes=7)
    def test_consumer_group_offsets_with_partition_count_mismatch(self):
        """
        Produce data, consume with multiple groups on the source cluster, then
        create a shadow link and verify that every group's per-partition
        committed offsets are mirrored to the target cluster despite different
        __consumer_offsets partition counts.
        """
        topic = TopicSpec(name="source-topic", partition_count=12, replication_factor=3)
        self.source_default_client().create_topic(topic)

        msg_count = 10000
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic.name,
            128,
            msg_count,
            custom_node=self.preallocated_nodes,
        )

        # Consume with several groups so they span different
        # __consumer_offsets partitions on each cluster
        groups = [f"test-group-{i}" for i in range(5)]
        source_rpk = RpkTool(self.source_cluster.service)
        for group in groups:
            source_rpk.consume(
                topic=topic.name,
                group=group,
                n=1,
                offset="start",
            )

        # Capture source offsets before creating the link
        source_offsets: dict[str, dict[tuple[str, int], int | None]] = {}
        for group in groups:
            desc = source_rpk.group_describe(group=group)
            source_offsets[group] = {
                (p.topic, p.partition): p.current_offset for p in desc.partitions
            }
            self.logger.info(f"Source group {group} offsets: {source_offsets[group]}")

        self.create_link("test-link")

        target_rpk = RpkTool(self.target_cluster.service)

        # Verify the precondition: __consumer_offsets must actually have
        # different partition counts on each cluster, otherwise the test
        # is not exercising the mismatch scenario.
        co_topic = "__consumer_offsets"
        source_co_partitions = len(list(source_rpk.describe_topic(co_topic)))
        target_co_partitions = len(list(target_rpk.describe_topic(co_topic)))
        self.logger.info(
            f"__consumer_offsets partition counts: "
            f"source={source_co_partitions}, target={target_co_partitions}"
        )
        assert source_co_partitions != target_co_partitions, (
            f"Expected different __consumer_offsets partition counts, "
            f"but both clusters have {source_co_partitions}"
        )

        def _offsets_consistent():
            for group in groups:
                try:
                    t_desc = target_rpk.group_describe(group=group)
                except Exception:
                    self.logger.debug(f"Group {group} not yet available on target")
                    return False
                t_partitions = {
                    (p.topic, p.partition): p.current_offset for p in t_desc.partitions
                }
                for key, src_offset in source_offsets[group].items():
                    if key not in t_partitions:
                        self.logger.debug(
                            f"Group {group} partition {key} not in target"
                        )
                        return False
                    if src_offset != t_partitions[key]:
                        self.logger.debug(
                            f"Group {group} partition {key}: "
                            f"source={src_offset} target={t_partitions[key]}"
                        )
                        return False
            return True

        wait_until(
            _offsets_consistent,
            timeout_sec=60,
            backoff_sec=3,
            err_msg=(
                "Consumer group offsets not consistent between source and "
                "target clusters with different __consumer_offsets partition "
                "counts"
            ),
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

    @cluster(num_nodes=6)
    def test_group_acl_sync(self):
        """
        This test verifies that Group: principal ACLs are synced from source
        to target cluster when a shadow link is created and configured
        """
        req = self.create_default_link_request("test-link")

        resource_filter = shadow_link_pb2.ACLResourceFilter(
            resource_type=acl_pb2.ACL_RESOURCE_ANY,
            pattern_type=acl_pb2.ACL_PATTERN_ANY,
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

        # Create a Group ACL on the source cluster
        group_acl = RPKACLInput(
            allow_principal=["Group:test-group"],
            allow_host=["*"],
            topic=["test-topic"],
            operation=["read", "describe"],
            resource_pattern_type="literal",
        )
        self.source_cluster_rpk.acl_create(group_acl)

        def check_if_group_acls_synced():
            target_acls: Any = self.target_cluster_rpk.acl_list(format="json")
            # We expect 2 ACLs (one for read, one for describe)
            group_acls_found = [
                acl
                for acl in target_acls.get("matches", [])
                if acl.get("principal") == "Group:test-group"
            ]
            if len(group_acls_found) != 2:
                self.logger.debug(f"Found {len(group_acls_found)} ACLs")
                return False

            self.logger.info(f"Found Group ACLs on target cluster: {group_acls_found}")
            for acl in group_acls_found:
                if not (
                    acl["host"] == "*"
                    and acl["resource_type"] == "TOPIC"
                    and acl["resource_name"] == "test-topic"
                    and acl["resource_pattern_type"] == "LITERAL"
                    and acl["permission"] == "ALLOW"
                ):
                    return False
            return True

        wait_until(
            check_if_group_acls_synced,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Failed to sync Group ACLs",
        )

        self.logger.info("Group ACLs successfully synced")

    @cluster(num_nodes=6)
    def test_mixed_principal_acl_sync(self):
        """
        This test verifies that a mix of User, Role, and Group ACLs are all
        synced from source to target cluster
        """
        req = self.create_default_link_request("test-link")

        resource_filter = shadow_link_pb2.ACLResourceFilter(
            resource_type=acl_pb2.ACL_RESOURCE_ANY,
            pattern_type=acl_pb2.ACL_PATTERN_ANY,
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

        # Create User ACL
        user_acl = RPKACLInput(
            allow_principal=["test-user"],
            topic=["mixed-topic"],
            operation=["read"],
            resource_pattern_type="literal",
        )
        self.source_cluster_rpk.acl_create(user_acl)

        # Create Role ACL
        role_acl = RPKACLInput(
            allow_role=["test-role"],
            topic=["mixed-topic"],
            operation=["write"],
            resource_pattern_type="literal",
        )
        self.source_cluster_rpk.acl_create(role_acl)

        # Create Group ACL
        group_acl = RPKACLInput(
            allow_principal=["Group:test-group"],
            allow_host=["*"],
            topic=["mixed-topic"],
            operation=["describe"],
            resource_pattern_type="literal",
        )
        self.source_cluster_rpk.acl_create(group_acl)

        def check_if_all_acls_synced():
            target_acls: Any = self.target_cluster_rpk.acl_list(format="json")
            matches = target_acls.get("matches", [])

            user_acl_found = any(
                acl.get("principal") == "User:test-user"
                and acl.get("operation") == "READ"
                for acl in matches
            )
            role_acl_found = any(
                acl.get("principal") == "RedpandaRole:test-role"
                and acl.get("operation") == "WRITE"
                for acl in matches
            )
            group_acl_found = any(
                acl.get("principal") == "Group:test-group"
                and acl.get("operation") == "DESCRIBE"
                for acl in matches
            )

            if user_acl_found and role_acl_found and group_acl_found:
                self.logger.info(f"All ACL types found on target cluster: {matches}")
                return True

            self.logger.debug(
                f"Waiting for ACLs - User: {user_acl_found}, Role: {role_acl_found}, "
                f"Group: {group_acl_found}, matches: {matches}"
            )
            return False

        wait_until(
            check_if_all_acls_synced,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Failed to sync mixed principal ACLs",
        )

        self.logger.info("All mixed principal ACLs successfully synced")


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
    def test_link_topic_failover(self, with_failures, source_cluster_spec):
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
                lambda: self.topic_partitions_exists_in_target(t),
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
    def test_link_failover(self, with_failures, source_cluster_spec):
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
                lambda: self.topic_partitions_exists_in_target(t),
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

    @cluster(num_nodes=7)
    def test_producer_ids_failover(self):
        self.create_link("test-link")
        num_messages = 2
        topic = TopicSpec(name="test-topic", partition_count=1, replication_factor=3)
        self.source_default_client().create_topic(topic)

        def produce(n: int, redpanda: MultiService):
            KgoVerifierProducer.oneshot(
                self.test_context,
                redpanda,
                topic.name,
                128,
                n,
                timeout_sec=30,
            )

        def consume(n: int, offset: str = "start") -> list[int]:
            try:
                raw = self.target_cluster_rpk.consume(
                    topic=topic.name,
                    n=n,
                    partition=0,
                    timeout=5,
                    offset=offset,
                    format="%o,",
                )
                return [int(o) for o in raw.split(",")[0:-1]]
            except Exception:
                return []

        produce(n=num_messages, redpanda=self.source_cluster.service)

        self.target_cluster_service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        wait_until(
            lambda: consume(1, offset=f"{num_messages - 1}") == [num_messages - 1],
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"First messages never appeared: {consume(num_messages)}",
        )

        self.failover_link(name="test-link")
        self.wait_for_link_failover(link="test-link")

        second_round = 2

        produce(n=second_round, redpanda=self.target_cluster.service)

        wait_until(
            lambda: consume(second_round, offset=f"{num_messages}")
            == list(range(num_messages, num_messages + second_round)),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Second Messages never appeared: {consume(num_messages)}",
        )


class ShadowLinkUpdateBrokersTests(ShadowLinkPreAllocTestBase):
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        self.test_context = test_context
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.security.require_client_auth = False

        super().__init__(
            test_context=self.test_context, security=self.security, *args, **kwargs
        )

        self.other_source_cluster = RedpandaCluster.create(
            self.test_context,
            num_brokers=3,
            security=self.security,
        )

    def setUp(self):
        super().setUp()
        self.other_source_cluster.start()

    @property
    def target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service, tls_cert=self.tls.create_cert("target-rpk")
        )

    @property
    def other_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.other_source_cluster.service,
            tls_cert=self.tls.create_cert("other-source-rpk"),
        )

    @cluster(
        num_nodes=9,
        log_allow_list=[re.compile(".*Broker.*does not support list groups API.*")],
    )
    def test_update_brokers(self):
        # Create a link pointing to the old source cluster
        shadow_link = self.create_link("test-link")

        # Update bootstrap_servers
        del shadow_link.configurations.client_options.bootstrap_servers[:]
        shadow_link.configurations.client_options.bootstrap_servers.extend(
            self.other_source_cluster.service.brokers_list()
        )

        # Update tls settings
        shadow_link.configurations.client_options.tls_settings.CopyFrom(
            tls_pb2.TLSSettings(
                enabled=True,
                tls_file_settings=tls_pb2.TLSFileSettings(
                    ca_path=self.redpanda.TLS_CA_CRT_FILE,
                    key_path=self.redpanda.TLS_SERVER_KEY_FILE,
                    cert_path=self.redpanda.TLS_SERVER_CRT_FILE,
                ),
            )
        )

        update_mask: google.protobuf.field_mask_pb2.FieldMask = (
            google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.client_options"]
            )
        )

        # Update the link to point to the new source cluster
        updated_link = self.update_link(
            shadow_link=shadow_link, update_mask=update_mask
        )
        assert (
            updated_link.configurations.client_options
            == shadow_link.configurations.client_options
        ), (
            f"Expected updated link to be returned:\n"
            f"{updated_link.configurations.client_options}!=\n{shadow_link.configurations.client_options}"
        )

        old_source_topic = "old-source-topic"
        new_source_topic = "new-source-topic"

        self.source_cluster_rpk.create_topic(old_source_topic)
        self.other_source_cluster_rpk.create_topic(new_source_topic)

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(
                new_source_topic, 1, self.target_cluster_rpk
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {new_source_topic} not found in target cluster",
        )
        assert not self.topic_exists_in_target(
            old_source_topic, None, self.target_cluster_rpk
        ), f"Topic {old_source_topic} should not be visible to the target cluster"


Validator = Callable[[list[dict[str, MetricSamples]]], bool]


class ShadowLinkingMetricsTests(ShadowLinkPreAllocTestBase):
    SHADOW_TOPIC_STATE = "redpanda_shadow_link_shadow_topic_state"
    TOTAL_RECORDS_FETCHED = "redpanda_shadow_link_total_records_fetched"
    TOTAL_RECORDS_WRITTEN = "redpanda_shadow_link_total_records_written"
    TOTAL_BYTES_FETCHED = "redpanda_shadow_link_total_bytes_fetched"
    TOTAL_BYTES_WRITTEN = "redpanda_shadow_link_total_bytes_written"
    SHADOW_LAG = "redpanda_shadow_link_shadow_lag"
    CLIENT_ERRORS = "redpanda_shadow_link_client_errors"

    def _get_metrics_for_node(
        self,
        node: ClusterNode,
        patterns: list[str],
    ) -> Optional[dict[str, MetricSamples]]:
        samples = self.redpanda.metrics_samples(
            patterns, [node], MetricsEndpoint.PUBLIC_METRICS
        )
        self.logger.debug(f"patterns: {patterns} node: {node.name} samples: {samples}")
        return samples

    def _get_metrics_for_nodes(
        self,
        nodes: list[ClusterNode],
        patterns: list[str],
    ) -> Optional[list[dict[str, MetricSamples]]]:
        metrics: list[dict[str, MetricSamples]] = []
        for n in nodes:
            node_metrics = self._get_metrics_for_node(n, patterns)

            if node_metrics is None:
                continue
            metrics.append(node_metrics)
        return metrics

    def _validate_metrics(
        self, nodes: list[ClusterNode], patterns: list[str], validator: Validator
    ):
        metrics = self._get_metrics_for_nodes(nodes, patterns)
        if metrics is None:
            return False
        return validator(metrics)

    @cluster(num_nodes=7)
    def test_link_metrics(self):
        topic_1 = TopicSpec(
            name="test-topic-1", partition_count=3, replication_factor=1
        )
        self.source_default_client().create_topic(topic_1)
        self.create_link("test-link")

        with self.producer_consumer(topic=topic_1.name, msg_size=128, msg_cnt=1000):
            self.verify()

        def collect_shadow_topic_states(
            node_samples: list[dict[str, MetricSamples]],
        ) -> dict[str, int]:
            by_status: dict[str, int] = {}
            for samples in node_samples:
                if self.SHADOW_TOPIC_STATE not in samples:
                    continue
                for s in samples[self.SHADOW_TOPIC_STATE].samples:
                    status = s.labels["status"]
                    if status not in by_status:
                        by_status[status] = 0
                    by_status[status] += int(s.value)
            return by_status

        def check_shadow_topic_states(
            node_samples: list[dict[str, MetricSamples]],
            expected_states: dict[str, int],
        ) -> bool:
            all_states = [
                "active",
                "failed",
                "paused",
                "failing_over",
                "failed_over",
                "promoting",
                "promoted",
            ]
            expected = {
                **expected_states,
                **{s: 0 for s in all_states if s not in expected_states},
            }

            by_status = collect_shadow_topic_states(node_samples)
            return all(by_status[s] == expected[s] for s in all_states)

        def _get_total_value(
            node_samples: list[dict[str, MetricSamples]], metric_name: str
        ) -> Optional[int]:
            total_value = 0
            for samples in node_samples:
                if metric_name not in samples:
                    return None
                for s in samples[metric_name].samples:
                    total_value += int(s.value)
            return total_value

        def check_total_value(
            node_samples: list[dict[str, MetricSamples]],
            metric_name: str,
            expected_total: int,
        ) -> bool:
            total_records = _get_total_value(node_samples, metric_name)
            return total_records is not None and total_records == expected_total

        # This function only checks that the result is greater than zero. i.e. something has been returned by this metric
        def check_value_positive(
            node_samples: list[dict[str, MetricSamples]], metric_name: str
        ) -> bool:
            total_value = _get_total_value(node_samples, metric_name)
            return total_value is not None and total_value > 0

        # This function checks that the result is at least min_value.
        def check_value_at_least(
            node_samples: list[dict[str, MetricSamples]],
            metric_name: str,
            min_value: int,
        ) -> bool:
            total_value = _get_total_value(node_samples, metric_name)
            return total_value is not None and total_value >= min_value

        def check_metric_exists(
            node_samples: list[dict[str, MetricSamples]], metric_name: str
        ) -> bool:
            for samples in node_samples:
                if metric_name not in samples:
                    return False
            return True

        def active_shadow_topics_1(samples: list[dict[str, MetricSamples]]):
            return check_shadow_topic_states(samples, {"active": 1})

        def active_shadow_topics_2(samples: list[dict[str, MetricSamples]]):
            return check_shadow_topic_states(samples, {"active": 2})

        def failed_over_topics_3(samples: list[dict[str, MetricSamples]]):
            return check_shadow_topic_states(samples, {"failed_over": 3})

        def check_records_fetched_1000(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_FETCHED, 1000)

        def check_records_fetched_2500(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_FETCHED, 2500)

        def check_records_written_1000(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_WRITTEN, 1000)

        def check_records_written_2500(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_WRITTEN, 2500)

        def check_bytes_fetched(samples: list[dict[str, MetricSamples]]):
            return check_value_positive(samples, self.TOTAL_BYTES_FETCHED)

        def check_bytes_fetched_128000(samples: list[dict[str, MetricSamples]]):
            return check_value_at_least(samples, self.TOTAL_BYTES_FETCHED, 128000)

        def check_bytes_written(samples: list[dict[str, MetricSamples]]):
            return check_value_positive(samples, self.TOTAL_BYTES_WRITTEN)

        def check_bytes_written_128000(samples: list[dict[str, MetricSamples]]):
            return check_value_at_least(samples, self.TOTAL_BYTES_WRITTEN, 128000)

        def check_shadow_lag_zero(node_samples: list[dict[str, MetricSamples]]) -> bool:
            return check_total_value(node_samples, self.SHADOW_LAG, 0)

        def check_shadow_lag_positive(
            node_samples: list[dict[str, MetricSamples]],
        ) -> bool:
            return check_value_positive(node_samples, self.SHADOW_LAG)

        def check_client_errors(node_samples: list[dict[str, MetricSamples]]) -> bool:
            return check_metric_exists(node_samples, self.CLIENT_ERRORS)

        def validate_metrics(
            timeout_sec: int, metric_validators: list[tuple[str, Validator]]
        ):
            for metric_name, validator in metric_validators:
                self.logger.debug(
                    f"Validating values of metric: '{metric_name}', method: '{getattr(validator, '__name__')}'"
                )
                wait_until(
                    lambda: self._validate_metrics(
                        target_nodes, [metric_name], validator
                    ),
                    timeout_sec=timeout_sec,
                    backoff_sec=1,
                    err_msg=f"Failed to get the expected metrics value for metric {metric_name}",
                )

        target_nodes = self.target_cluster.service.nodes

        validate_metrics(
            timeout_sec=10,
            metric_validators=[
                (self.SHADOW_TOPIC_STATE, active_shadow_topics_1),
                (self.TOTAL_RECORDS_FETCHED, check_records_fetched_1000),
                (self.TOTAL_RECORDS_WRITTEN, check_records_written_1000),
                (self.TOTAL_BYTES_FETCHED, check_bytes_fetched_128000),
                (self.TOTAL_BYTES_WRITTEN, check_bytes_written_128000),
                (self.SHADOW_LAG, check_shadow_lag_zero),
                (self.CLIENT_ERRORS, check_client_errors),
            ],
        )

        topic_2 = TopicSpec(
            name="test-topic-2", partition_count=3, replication_factor=1
        )
        self.source_default_client().create_topic(topic_2)
        with self.producer_consumer(topic=topic_2.name, msg_size=128, msg_cnt=1500):
            self.verify()

        validate_metrics(
            timeout_sec=10,
            metric_validators=[
                (self.SHADOW_TOPIC_STATE, active_shadow_topics_2),
                (self.TOTAL_RECORDS_FETCHED, check_records_fetched_2500),
                (self.TOTAL_RECORDS_WRITTEN, check_records_written_2500),
                (self.TOTAL_BYTES_FETCHED, check_bytes_fetched),
                (self.TOTAL_BYTES_WRITTEN, check_bytes_written),
                (self.SHADOW_LAG, check_shadow_lag_zero),
                (self.CLIENT_ERRORS, check_client_errors),
            ],
        )

        topic_3 = TopicSpec(
            name="test-topic-3", partition_count=1, replication_factor=3
        )
        self.source_default_client().create_topic(topic_3)
        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic_3),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic_3.name} not found in target cluster",
        )

        with self.producer_consumer(
            topic=topic_3.name,
            msg_size=128,
            msg_cnt=5000000,
            use_transactions=True,
            producer_properties={
                "msgs_per_transaction": "100000",
            },
        ):
            validate_metrics(
                timeout_sec=120,
                metric_validators=[
                    (self.SHADOW_LAG, check_shadow_lag_positive),
                ],
            )
            self.verify()

        validate_metrics(
            timeout_sec=30,
            metric_validators=[
                (self.SHADOW_LAG, check_shadow_lag_zero),
            ],
        )

        self.failover_link(name="test-link")
        self.wait_for_link_failover(link="test-link")

        validate_metrics(
            timeout_sec=10,
            metric_validators=[
                (self.SHADOW_TOPIC_STATE, failed_over_topics_3),
            ],
        )


class ShadowLinkCustomStartOffsetSelectionTests(ShadowLinkPreAllocTestBase):
    earliest_offset = "earliest"
    latest_offset = "latest"
    timequery_offset = "timestamp"
    max_records = 10000

    def setup_starting_offset(self, topic: TopicSpec) -> tuple[float, float]:
        initial = 1000
        assert self.max_records > initial
        self.source_default_client().create_topic(topic)
        start_time = time.time()
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic="source-topic",
            msg_size=4 * 1024,
            msg_count=initial,
            custom_node=self.preallocated_nodes,
        )
        # We produce in 2 phases so a batch cleanly ends at offset 999
        # and the next batch starts at offset 1000
        # This is done to workaround direct consumer limitation of not being
        # able to consume in the middle of a batch. If we don't do this here
        # the replication picks the first batch that contains the offset 1000
        # and hence the start offset may be before offset 1000
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic="source-topic",
            msg_size=4 * 1024,
            msg_count=self.max_records - initial,
            custom_node=self.preallocated_nodes,
        )
        end_time = time.time()

        partitions = [
            p.id
            for p in self.source_cluster_rpk.describe_topic("source-topic", timeout=3)
        ]
        self.logger.info(f"Trimming source topic partitions: {partitions}")
        self.source_cluster_rpk.trim_prefix(
            topic="source-topic", offset=1000, partitions=partitions
        )

        def wait_for_starting_offset_1000(rpk: RpkTool):
            for part in rpk.describe_topic("source-topic"):
                if part.start_offset < 1000:
                    return False
            return True

        wait_until(
            lambda: wait_for_starting_offset_1000(self.source_cluster_rpk),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Failed to trim source topic",
        )

        return (start_time, end_time)

    def find_starting_timestamp(
        self, topic: TopicSpec, start_time: float, end_time: float
    ) -> str:
        self.logger.info(
            f"Attempting to find a timestamp between {start_time} and {end_time}"
        )
        # Start at halfway point and go up 100ms at a time until we get an offset
        current_time = start_time + 1
        while current_time <= end_time:
            iso_timestamp = time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(current_time)
            )
            iso_formatted = f"{iso_timestamp}"
            self.logger.debug(f"Trying timestamp: {iso_formatted}")
            try:
                # Query each partition to see if we get a valid offset for this timestamp
                for part in self.source_cluster_rpk.describe_topic("source-topic"):
                    self.source_cluster_rpk.consume(
                        topic=topic.name,
                        n=1,
                        offset=f"@{iso_formatted}Z",
                        partition=part.id,
                        timeout=2,
                    )

                self.logger.info(f"Found starting offset: {iso_formatted}")
                return iso_formatted
            except Exception as e:
                self.logger.debug(f"Failed to query timestamp {iso_formatted}: {e}")

            current_time += 1  # Move forward 100ms

        # If no valid timestamp found, return the end time
        raise RuntimeError(
            f"Failed to find a valid starting timestamp between {start_time} and {end_time}"
        )

    @cluster(num_nodes=7)
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
        starting_offset=["earliest", "latest", "timestamp"],
        failures=[True, False],
    )
    def test_starting_offset(
        self,
        source_cluster_spec: SecondaryClusterSpec,
        starting_offset: str,
        failures: bool,
    ):
        """
        This test will verify the starting offset configuration.

        1. Pre-populate the source cluster with some data
        2. Prefix-truncate the data
        3. Create a shadow link with a specified starting offset
        4. Verify that the shadow topic is starting at the specified starting offset
        """
        if failures and (
            source_cluster_spec.cluster_type == ServiceType.KAFKA
            or starting_offset == "latest"
        ):
            # 1. Kafka source does not support transient failures injection
            # 2. With failures enabled, in latest offset mode, it is hard to guarantee
            # the replicator will pick the exact latest offset due to retries
            # and back-offs (since latest is a moving target).
            # Avoid warning of not using all allocated nodes
            _ = self.preallocated_nodes
            self.logger.info("Skipping failure injection with Kafka source cluster")
            return
        topic = TopicSpec(name="source-topic", partition_count=1, replication_factor=3)

        (start_time, end_time) = self.setup_starting_offset(topic=topic)

        req = self.create_default_link_request("test-link")

        if starting_offset == self.earliest_offset:
            req.shadow_link.configurations.topic_metadata_sync_options.start_at_earliest.CopyFrom(
                shadow_link_pb2.TopicMetadataSyncOptions.EarliestOffset()
            )
        elif starting_offset == self.latest_offset:
            req.shadow_link.configurations.topic_metadata_sync_options.start_at_latest.CopyFrom(
                shadow_link_pb2.TopicMetadataSyncOptions.LatestOffset()
            )
        elif starting_offset == self.timequery_offset:
            starting_offset = self.find_starting_timestamp(
                topic=topic, start_time=start_time, end_time=end_time
            )
            self.logger.info(f'Using starting offset "{starting_offset}"')
            timestamp_pb = google.protobuf.timestamp_pb2.Timestamp()
            timestamp_pb.FromMilliseconds(
                int(
                    time.mktime(time.strptime(starting_offset, "%Y-%m-%dT%H:%M:%S"))
                    * 1000
                )
            )
            req.shadow_link.configurations.topic_metadata_sync_options.start_at_timestamp.CopyFrom(
                timestamp_pb
            )
        else:
            assert False, f"Invalid starting offset value: {starting_offset}"

        def maybe_failure_injector():
            if failures:
                # Inject failures on source to simulate transient errors during
                # timestamp to offset resolution and subsequent retries.
                return self.create_source_failure_injector()
            else:
                return self._nop_context_manager()

        with maybe_failure_injector():
            self.create_link_with_request(req=req)

            self.target_cluster.service.wait_until(
                lambda: self.topic_partitions_exists_in_target(topic),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Topic {topic.name} not found in target cluster",
            )

            if starting_offset == self.latest_offset:
                # link should start now at the latest offset and should be empty
                def ensure_target_partition_empty():
                    try:
                        for part in self.target_cluster_rpk.describe_topic(topic.name):
                            if part.high_watermark != part.start_offset:
                                return False
                        return True
                    except Exception as e:
                        self.logger.debug(f"Failed to describe topic: {e}")
                        return False

                wait_until(
                    ensure_target_partition_empty,
                    timeout_sec=60,
                    backoff_sec=1,
                    err_msg=f"Target topic {topic.name} partitions not empty",
                )

                def produce_one_key():
                    try:
                        self.source_cluster_rpk.produce(
                            topic=topic.name, key="key", msg="value", partition=0
                        )
                        return True
                    except Exception as e:
                        self.logger.debug(f"Failed to produce to topic: {e}")
                        return False

                wait_until(
                    produce_one_key,
                    timeout_sec=30,
                    backoff_sec=1,
                    err_msg=f"Failed to produce to source topic {topic.name}",
                )

            def get_partitions_starting_offset(
                rpk: RpkTool, offset: str | int
            ) -> dict[int, int]:
                def do_get_partitions_starting_offset():
                    try:
                        offsets: dict[int, int] = {}
                        for part in rpk.describe_topic(topic.name, timeout=3):
                            record = json.loads(
                                rpk.consume(
                                    topic=topic.name,
                                    n=1,
                                    partition=part.id,
                                    offset=offset,
                                )
                            )
                            offsets[part.id] = record["offset"]

                        return offsets
                    except Exception as e:
                        self.logger.debug(
                            f"Failed to get partitions starting offsets: {e}"
                        )
                        return None

                return wait_until_result(
                    do_get_partitions_starting_offset,
                    timeout_sec=60,
                    backoff_sec=1,
                    err_msg=f"Failed to get partitions starting offsets for {topic.name}",
                )

            source_offset_to_fetch = (
                "start"
                if starting_offset == self.earliest_offset
                else "-1"
                if starting_offset == self.latest_offset
                else f"@{starting_offset}Z"
            )
            self.logger.info(
                f"Fetching offset '{source_offset_to_fetch}' from source cluster"
            )
            source_offsets = get_partitions_starting_offset(
                self.source_cluster_rpk, source_offset_to_fetch
            )
            self.logger.info(f"Source cluster offsets: {source_offsets}")

            def do_wait_for_hwm():
                partition_info = list(
                    self.target_cluster_rpk.describe_topic(topic.name)
                )
                for p in partition_info:
                    if p.id == 0:
                        return p.high_watermark >= self.max_records

            self.target_cluster_service.wait_until(
                do_wait_for_hwm,
                timeout_sec=60,
                backoff_sec=2,
                err_msg="Timed out waiting for hwm to catchup on target",
                retry_on_exc=True,
            )

            # If testing for earliest or latest offset, use "start", else
            # use the timequery value.  The batch fetched from the source at
            # that time query may have records starting before that timestamp
            target_offset_to_fetch = (
                "start"
                if starting_offset == self.earliest_offset
                or starting_offset == self.latest_offset
                else f"@{starting_offset}Z"
            )
            self.logger.info(
                f"Fetching offset '{target_offset_to_fetch}' from target cluster"
            )
            target_offsets = get_partitions_starting_offset(
                self.target_cluster_rpk, target_offset_to_fetch
            )
            self.logger.info(f"Target cluster starting offsets: {target_offsets}")

            assert source_offsets == target_offsets, (
                f"Expected source and target offsets to match, got {target_offsets} vs {source_offsets}"
            )

    @cluster(num_nodes=7)
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA,
                kafka_version="3.8.0",
                kafka_quorum="COMBINED_KRAFT",
            ),
        ],
    )
    def test_start_at_future_timestamp(
        self,
        source_cluster_spec: SecondaryClusterSpec,
    ):
        """
        Verify that when a shadow link is configured with a start timestamp
        past the end of the source log, replication begins at the LSO rather
        than offset 0.

        ListOffsets returns offset -1 with error_code=none when the requested
        timestamp exceeds all data in the partition. The fix detects this and
        falls back to the LSO so only new data is replicated.
        """
        _ = source_cluster_spec
        topic = TopicSpec(name="source-topic", partition_count=1, replication_factor=3)
        self.source_default_client().create_topic(topic)

        # Produce historical data that must NOT be replicated to the target.
        initial_msg_count = 1000
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic=topic.name,
            msg_size=4 * 1024,
            msg_count=initial_msg_count,
            custom_node=self.preallocated_nodes,
        )

        def get_partition_0_info(rpk: RpkTool) -> RpkPartition | None:
            try:
                for part in rpk.describe_topic(topic.name, timeout=3):
                    if part.id == 0:
                        return part
            except Exception as e:
                self.logger.debug(f"Failed to describe topic: {e}")
            return None

        def source_hwm_reached_msg_count() -> int | None:
            p_info = get_partition_0_info(self.source_cluster_rpk)
            if p_info and p_info.high_watermark >= initial_msg_count:
                return p_info.high_watermark
            return None

        source_orig_hwm = wait_until_result(
            source_hwm_reached_msg_count,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timed out waiting for source HWM to reach expected count",
        )
        self.logger.info(f"Source HWM before link creation: {source_orig_hwm}")

        # Configure the link with a timestamp far in the future so that
        # ListOffsets returns offset -1 (no record at or after that timestamp).
        req = self.create_default_link_request("test-link")
        timestamp_pb = google.protobuf.timestamp_pb2.Timestamp()
        timestamp_pb.FromMilliseconds(
            int(
                time.mktime(time.strptime("2100-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"))
                * 1000
            )
        )
        req.shadow_link.configurations.topic_metadata_sync_options.start_at_timestamp.CopyFrom(
            timestamp_pb
        )
        self.create_link_with_request(req=req)

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        # Confirm that the link is not replicating historical data (both HWM and start offset stay 0 on the target).
        sleep(5)
        prev_target_info = get_partition_0_info(self.target_cluster_rpk)
        assert prev_target_info is not None, "Failed to get target partition info"
        assert prev_target_info.high_watermark == 0, (
            f"Expected target HWM to be 0, got {prev_target_info.high_watermark}"
        )
        assert prev_target_info.start_offset == 0, (
            f"Expected target start offset to be 0, got {prev_target_info.start_offset}"
        )

        self.logger.info("Producing one new record to the source topic")
        self.source_cluster_rpk.produce(
            topic=topic.name, key="key", msg="value", partition=0
        )

        source_info = get_partition_0_info(self.source_cluster_rpk)
        assert source_info is not None, (
            "Failed to get source partition info after producing new record"
        )
        assert source_info.high_watermark == source_orig_hwm + 1, (
            f"Expected source HWM to advance by 1 after producing new record, "
            f"got {source_info.high_watermark} vs previous {source_orig_hwm}"
        )

        def target_has_new_data():
            target_info = get_partition_0_info(self.target_cluster_rpk)
            if target_info is None:
                return False

            return (
                target_info.high_watermark == source_info.high_watermark,
                target_info,
            )

        target_info: RpkPartition = wait_until_result(
            target_has_new_data,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="New data was not replicated to the target cluster",
            retry_on_exc=True,
        )

        assert target_info.high_watermark == source_info.high_watermark, (
            f"Expected target HWM to be {source_info.high_watermark} after replicating new record, got {target_info.high_watermark}"
        )

        source_last_record = json.loads(
            self.source_cluster_rpk.consume(
                topic=topic.name, n=1, partition=0, offset=-1
            )
        )
        target_first_record = json.loads(
            self.target_cluster_rpk.consume(
                topic=topic.name, n=1, partition=0, offset="start"
            )
        )
        assert source_last_record == target_first_record, (
            f"Record mismatch: source={source_last_record}, target={target_first_record}"
        )


class ShadowLinkingCloudTopicReplicationTests(ShadowLinkPreAllocTestBase):
    """
    Tests cluster linking replication with cloud topics
    (redpanda.storage.mode=cloud and tiered_cloud) on the source cluster.
    """

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        super().__init__(
            test_context,
            si_settings=si_settings,
            extra_rp_conf={
                CLOUD_TOPICS_CONFIG_STR: True,
                "enable_cluster_metadata_upload_loop": False,
            },
            secondary_cluster_args=SecondaryClusterArgs(
                si_settings=si_settings,
                extra_rp_conf={
                    CLOUD_TOPICS_CONFIG_STR: True,
                    "enable_shadow_linking": True,
                    "enable_cluster_metadata_upload_loop": False,
                },
            ),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=7)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_cloud_topic_replication(self, storage_mode):
        """
        Verify that data produced to a cloud/tiered_cloud topic on the source
        cluster is replicated to the target cluster via cluster linking.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.source_cluster_service.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )
            self.target_cluster.service.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        topic = TopicSpec(
            name="ct-topic",
            partition_count=3,
            replication_factor=1,
        )

        source_rpk = RpkTool(self.source_cluster.service)

        def create_source_topic():
            try:
                source_rpk.create_topic(
                    topic=topic.name,
                    partitions=topic.partition_count,
                    replicas=topic.replication_factor,
                    config={
                        TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                    },
                )
                return True
            except Exception as e:
                if "INVALID_CONFIG" in str(e):
                    return False
                raise

        # Retry topic creation: feature flag propagation may lag behind
        # the admin API response on some nodes.
        wait_until(
            create_source_topic,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Failed to create source topic with storage_mode={storage_mode}",
        )

        source_configs = source_rpk.describe_topic_configs(topic.name)
        assert source_configs[TopicSpec.PROPERTY_STORAGE_MODE][0] == storage_mode, (
            f"Source topic storage mode: {source_configs[TopicSpec.PROPERTY_STORAGE_MODE]}"
        )

        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        # Verify target topic has the same storage mode
        target_rpk = RpkTool(self.target_cluster.service)
        target_configs = target_rpk.describe_topic_configs(topic.name)
        assert target_configs[TopicSpec.PROPERTY_STORAGE_MODE][0] == storage_mode, (
            f"Target topic storage mode: {target_configs[TopicSpec.PROPERTY_STORAGE_MODE]}, "
            f"expected: {storage_mode}"
        )

        with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=10000):
            self.verify()
