# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.services.service import Service
from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.admin.proto.redpanda.core.common.v1 import acl_pb2, tls_pb2
from rptest.clients.rpk import RPKACLInput, RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import (
    CLOUD_TOPICS_CONFIG_STR,
    LoggingConfig,
    SchemaRegistryConfig,
    SecurityConfig,
    SISettings,
    TLSProvider,
)
from rptest.services.tls import CertificateAuthority, Certificate, TLSCertManager
from rptest.tests.cluster_linking_test_base import (
    ClusterLinkingTLSProvider,
    ShadowLinkTestBase,
)
from rptest.tests.schema_registry_test import SchemaRegistryRedpandaClient
from rptest.util import expect_exception, wait_until
from typing import Any

import ducktape.errors
import google.protobuf.field_mask_pb2
import json
import re
import socket

import base64
import google.protobuf.timestamp_pb2
import hashlib
import time


class ClusterLinkingTopicSyncingTestBase(ShadowLinkTestBase):
    """
    Base test class that will verify that topics can be synced from the target
    cluster to the source cluster and that properties are properly synced
    """

    def __init__(self, test_context, *args, **kwargs):
        self.default_topic_replication = 1
        extra_rp_conf = {"default_topic_replications": self.default_topic_replication}
        if "extra_rp_conf" in kwargs:
            extra_rp_conf.update(kwargs["extra_rp_conf"])
            kwargs.pop("extra_rp_conf")

        super().__init__(
            test_context,
            extra_rp_conf=extra_rp_conf,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        pass

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        return shadow_link

    def create_default_link_request(
        self, link_name: str, *args, **kwargs
    ) -> shadow_link_pb2.CreateShadowLinkRequest:
        req = super().create_default_link_request(link_name=link_name, *args, **kwargs)
        req.shadow_link.CopyFrom(self.add_credentials_to_link(req.shadow_link))
        return req

    def create_link(
        self, link_name: str, *args, **kwargs
    ) -> shadow_link_pb2.ShadowLink:
        req = self.create_default_link_request(link_name=link_name, *args, **kwargs)
        req.shadow_link.CopyFrom(self.add_credentials_to_link(req.shadow_link))
        return self.create_link_with_request(req=req)

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(self.source_cluster.service)

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(self.target_cluster.service)

    def _topics_are_present(
        self,
        rpk: RpkTool,
        topics_to_expect: list[TopicSpec],
        check_for_validity: bool = False,
        topics_not_expected: list[str] = [],
    ):
        topics_in_cluster = list(rpk.list_topics(detailed=True))
        self.logger.info(f"topics_in_cluster: {topics_in_cluster}")

        def find_by_name(name: str, results: list[list[str]]) -> list[str] | None:
            for r in results:
                if r[0] == name:
                    return r
            return None

        for t in topics_to_expect:
            found = find_by_name(t.name, topics_in_cluster)
            if not found:
                return False
            self.logger.debug(f"Found topic {t.name} in cluster: {found}")
            if check_for_validity:
                if int(found[1]) != t.partition_count:
                    self.logger.info(
                        f"Topic {t.name} partition count {found[1]} does not match expected {t.partition_count}"
                    )
                    return False
                if int(found[2]) != t.replication_factor:
                    self.logger.info(
                        f"Topic {t.name} replication factor {found[2]} does not match expected {t.replication_factor}"
                    )
                    return False

        for t in topics_not_expected:
            if any(t == topic[0] for topic in topics_in_cluster):
                assert False, f"Topic {t} found in cluster but should not be"

        return True

    @cluster(num_nodes=6)
    def test_topic_syncing_include_exclude(self):
        """
        This test verifies that the filters work appropriately
        """
        exclude_topic_prefix = "include-topic-1"
        include_topic_prefix = "include-topic-"
        exclude_topic_specific = "include-topic-0"
        include_topic_specific = "include-this-topic"

        self.logger.info("Creating topics on source cluster")
        topics: list[TopicSpec] = []
        for i in range(11):
            # Create include-topic-{0-10}
            cleanup_policy = "delete" if i % 2 == 0 else "compact"
            topic = TopicSpec(
                name=f"{include_topic_prefix}{i}",
                partition_count=i + 3,
                replication_factor=3,
                cleanup_policy=cleanup_policy,
            )
            self.get_source_cluster_rpk().create_topic(
                topic=topic.name,
                partitions=topic.partition_count,
                replicas=topic.replication_factor,
                config={"cleanup.policy": topic.cleanup_policy},
            )
            topics.append(topic)

        include_specific = TopicSpec(
            name=include_topic_specific,
            partition_count=3,
            replication_factor=3,
            cleanup_policy="delete",
        )

        self.get_source_cluster_rpk().create_topic(
            topic=include_specific.name,
            partitions=include_specific.partition_count,
            replicas=include_specific.replication_factor,
            config={"cleanup.policy": include_specific.cleanup_policy},
        )
        topics.append(include_specific)

        wait_until(
            lambda: self._topics_are_present(
                self.get_source_cluster_rpk(), topics, check_for_validity=True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Not all topics created on source cluster",
        )

        shadow_link_req = self.create_default_link_request(
            "test-link", mirror_all_topics=False, mirror_all_groups=False
        )

        shadow_link_req.shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name=include_topic_prefix,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_EXCLUDE,
                    name=exclude_topic_prefix,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name=include_topic_specific,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_EXCLUDE,
                    name=exclude_topic_specific,
                ),
            ]
        )

        self.logger.info(f"Creating shadow link with request: {shadow_link_req}")
        created_link = self.create_link_with_request(req=shadow_link_req)
        self.validate_created_link(created_link)

        self.logger.info("Verifying topics synced to target cluster")
        expected_topics = [
            t
            for t in topics
            if t.name.startswith(include_topic_prefix)
            and not t.name.startswith(exclude_topic_prefix)
            and not t.name == exclude_topic_specific
        ]
        expected_topics.append(include_specific)

        for t in expected_topics:
            t.replication_factor = 3

        self.logger.info(
            f"expected_topics: {[{'name': t.name, 'partition_count': t.partition_count, 'replication_factor': t.replication_factor} for t in expected_topics]}"
        )
        unexpected_topics = [
            t.name
            for t in topics
            if t.name.startswith(exclude_topic_prefix)
            or t.name == exclude_topic_specific
        ]
        self.logger.info(f"unexpected_topics: {unexpected_topics}")

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), expected_topics, True, unexpected_topics
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Not all topics created on target cluster",
        )

        mirror_topics = self.list_shadow_topics(shadow_link_name="test-link")
        assert len(mirror_topics) == len(expected_topics), (
            f"Expected {len(expected_topics)} topics in mirror but found {len(mirror_topics)}: {mirror_topics}"
        )

    @cluster(num_nodes=6)
    def test_topic_partition_count_sync(self):
        topic_name = "test-topic"

        topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=1)

        self.get_source_cluster_rpk().create_topic(
            topic=topic.name,
            partitions=topic.partition_count,
            replicas=topic.replication_factor,
        )

        created_link = self.create_link("test-link")
        self.validate_created_link(created_link)

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic not created on target cluster",
        )

        self.logger.info("Increasing partition count on source topic")

        self.get_source_cluster_rpk().add_partitions(topic_name, 3)

        topic.partition_count = 6

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic partition count not updated",
        )

    @cluster(num_nodes=6)
    def test_topic_properties_sync(self):
        topic_name = "test-topic"

        topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=1)

        self.get_source_cluster_rpk().create_topic(
            topic=topic.name,
            partitions=topic.partition_count,
            replicas=topic.replication_factor,
        )

        shadow_link_req = self.create_default_link_request("test-link")

        shadow_link_req.shadow_link.configurations.topic_metadata_sync_options.synced_shadow_topic_properties.append(
            "replication.factor"
        )

        created_link = self.create_link_with_request(req=shadow_link_req)
        self.validate_created_link(created_link)

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic not created on target cluster",
        )

        self.get_source_cluster_rpk().alter_topic_config(
            topic_name, "replication.factor", 3
        )

        topic.replication_factor = 3

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Replication factor never updated",
        )


class ClusterLinkingTopicSyncingTestNoSecurity(ClusterLinkingTopicSyncingTestBase):
    """
    Runs the base test with no security settings
    """

    pass


class ClusterLinkingTopicSyncingWithScram(ClusterLinkingTopicSyncingTestBase):
    """
    Run the same battery of tests as ClusterLinkingTopicSyncingTestBase but with SCRAM enabled
    """

    def __init__(self, test_context, *args, **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        secondary_args: SecondaryClusterArgs = SecondaryClusterArgs(security=security)
        self.cluster_link_user = "cluster-link-user"
        self.cluster_link_password = "cluster-link-password"
        self.cluster_link_mechanism = shadow_link_pb2.SCRAM_MECHANISM_SCRAM_SHA_256

        super().__init__(
            test_context=test_context,
            secondary_cluster_args=secondary_args,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        now = time.time()
        assert (
            shadow_link.configurations.client_options.authentication_configuration.WhichOneof(
                "authentication"
            )
            == "scram_configuration"
        ), (
            f"Expected 'scram_configuration' but got {shadow_link.configurations.client_options.authentication_configuration.WhichOneof('authentication')}"
        )

        scram_config = shadow_link.configurations.client_options.authentication_configuration.scram_configuration
        assert scram_config.password_set, "Password not set in scram configuration"
        assert scram_config.password == "", "Password should not be set"
        assert scram_config.username == self.cluster_link_user, (
            f"Username does not match: {scram_config.username} != {self.cluster_link_user}"
        )
        assert scram_config.scram_mechanism == self.cluster_link_mechanism, (
            f"Mechanism does not match: {scram_config.scram_mechanism} != {self.cluster_link_mechanism}"
        )
        assert (
            scram_config.password_set_at != google.protobuf.timestamp_pb2.Timestamp()
        ), "Password set time not set"

        assert now - 5 <= scram_config.password_set_at.seconds <= now + 5, (
            f"Password set time not recent: {scram_config.password_set_at.seconds} vs {now}"
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug(
            f"Adding credentials for user {self.cluster_link_user} to link"
        )
        shadow_link.configurations.client_options.authentication_configuration.scram_configuration.CopyFrom(
            shadow_link_pb2.ScramConfig(
                username=self.cluster_link_user,
                password=self.cluster_link_password,
                scram_mechanism=self.cluster_link_mechanism,
            )
        )
        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.mechanism,
        )

    def setUp(self):
        super().setUp()
        self.get_source_cluster_rpk().sasl_create_user(
            self.cluster_link_user, self.cluster_link_password
        )
        self.source_cluster.service.set_cluster_config(
            {
                "superusers": [
                    self.redpanda.SUPERUSER_CREDENTIALS.username,
                    self.cluster_link_user,
                ]
            }
        )

    @cluster(num_nodes=6)
    def test_set_same_password(self):
        shadow_link_req = self.create_default_link_request("test-link")
        created_link = self.create_link_with_request(req=shadow_link_req)

        orig_timestamp = created_link.configurations.client_options.authentication_configuration.scram_configuration.password_set_at
        self.logger.debug(f"Original password set at: {orig_timestamp}")

        # Update with the exact same password
        shadow_link_update = shadow_link_req.shadow_link

        update_mask: google.protobuf.field_mask_pb2.FieldMask = (
            google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.client_options"]
            )
        )

        updated_link = self.update_link(
            shadow_link=shadow_link_update, update_mask=update_mask
        )

        new_timestamp = updated_link.configurations.client_options.authentication_configuration.scram_configuration.password_set_at

        self.logger.debug(f"New timestamp: {new_timestamp}")

        assert new_timestamp == orig_timestamp, (
            f"Password set timestamp should not have changed: {new_timestamp} != {orig_timestamp}"
        )


class ClusterLinkingTopicSyncingWithPlain(ClusterLinkingTopicSyncingTestBase):
    """
    Run the same battery of tests with PLAIN
    """

    def __init__(self, test_context, *args, **kwargs):
        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = ["SCRAM", "PLAIN"]
        secondary_args: SecondaryClusterArgs = SecondaryClusterArgs(security=security)
        self.cluster_link_user = "cluster-link-user"
        self.cluster_link_password = "cluster-link-password"

        super().__init__(
            test_context=test_context,
            secondary_cluster_args=secondary_args,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        now = time.time()
        assert (
            shadow_link.configurations.client_options.authentication_configuration.WhichOneof(
                "authentication"
            )
            == "plain_configuration"
        ), (
            f"Expected 'plain_configuration' but got {shadow_link.configurations.client_options.authentication_configuration.WhichOneof('authentication')}"
        )

        plain_config = shadow_link.configurations.client_options.authentication_configuration.plain_configuration
        assert plain_config.password_set, "Password not set in plain configuration"
        assert plain_config.password == "", "Password should not be set"
        assert plain_config.username == self.cluster_link_user, (
            f"Username does not match: {plain_config.username} != {self.cluster_link_user}"
        )
        assert (
            plain_config.password_set_at != google.protobuf.timestamp_pb2.Timestamp()
        ), "Password set time not set"

        assert now - 5 <= plain_config.password_set_at.seconds <= now + 5, (
            f"Password set time not recent: {plain_config.password_set_at.seconds} vs {now}"
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug(
            f"Adding PLAIN credentials for user {self.cluster_link_user} to link"
        )

        shadow_link.configurations.client_options.authentication_configuration.plain_configuration.CopyFrom(
            shadow_link_pb2.PlainConfig(
                username=self.cluster_link_user, password=self.cluster_link_password
            )
        )
        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.mechanism,
        )

    def setUp(self):
        super().setUp()
        self.get_source_cluster_rpk().sasl_create_user(
            self.cluster_link_user, self.cluster_link_password
        )
        self.source_cluster.service.set_cluster_config(
            {
                "superusers": [
                    self.redpanda.SUPERUSER_CREDENTIALS.username,
                    self.cluster_link_user,
                ]
            }
        )


class ClusterLinkingTopicSyncingWithTlsFiles(ClusterLinkingTopicSyncingTestBase):
    """
    Runs the base tests with TLS enabled on both endpoints
    """

    def __init__(self, test_context, *args, **kwargs):
        self.test_context = test_context
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.security.require_client_auth = False

        super().__init__(
            test_context=self.test_context,
            secondary_cluster_args=SecondaryClusterArgs(security=self.security),
            security=self.security,
            *args,
            **kwargs,
        )

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        assert (
            shadow_link.configurations.client_options.tls_settings.WhichOneof(
                "tls_settings"
            )
            == "tls_file_settings"
        ), (
            f"Expected 'tls_file_settings' but got {shadow_link.configurations.client_options.tls_settings.WhichOneof('tls_settings')}"
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug("Adding TLS files to link")

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

        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service, tls_cert=self.tls.create_cert("source-rpk")
        )

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service, tls_cert=self.tls.create_cert("target-rpk")
        )


class ClusterLinkingTopicSyncingWithTlsValues(ClusterLinkingTopicSyncingTestBase):
    """
    Runs the base tests with TLS enabled on both endpoints and using TLS values rather than files
    """

    def __init__(self, test_context, *args, **kwargs):
        self.test_context = test_context
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.security.require_client_auth = False
        self.client_cert: Certificate | None = None

        super().__init__(
            test_context=self.test_context,
            secondary_cluster_args=SecondaryClusterArgs(security=self.security),
            security=self.security,
            *args,
            **kwargs,
        )

        self._ca_value: str = ""
        self._key_value: str = ""
        self._cert_value: str = ""

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        assert (
            shadow_link.configurations.client_options.tls_settings.WhichOneof(
                "tls_settings"
            )
            == "tls_pem_settings"
        ), (
            f"Expected 'tls_pem_settings' but got {shadow_link.configurations.client_options.tls_settings.WhichOneof('tls_settings')}"
        )
        pem_settings = (
            shadow_link.configurations.client_options.tls_settings.tls_pem_settings
        )
        assert pem_settings.ca == self._ca_value, (
            f"CA value does not match: {pem_settings.ca} != {self._ca_value}"
        )
        assert pem_settings.cert == self._cert_value, (
            f"Cert value does not match: {pem_settings.cert} != {self._cert_value}"
        )
        assert pem_settings.key == "", (
            f"Key value should not be returned: {pem_settings.key}"
        )
        key_hash = hashlib.sha256(self._key_value.encode()).digest()
        key_fingerprint = base64.b64encode(key_hash).decode()
        assert pem_settings.key_fingerprint == key_fingerprint, (
            f"Key fingerprint does not match: {pem_settings.key_fingerprint} != {key_fingerprint}"
        )

    def setUp(self):
        super().setUp()
        self.client_cert = self.tls.create_cert("cluster-link-client")

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug("Adding TLS values to link")

        assert self.client_cert is not None

        ca_content = open(self.client_cert.ca.crt, "r").read()
        self.logger.debug(f"ca: {ca_content}")
        self._ca_value = ca_content
        cert_content = open(self.client_cert.crt, "r").read()
        self.logger.debug(f"cert: {cert_content}")
        self._cert_value = cert_content
        key_content = open(self.client_cert.key, "r").read()
        self.logger.debug(f"key: {key_content}")
        self._key_value = key_content

        shadow_link.configurations.client_options.tls_settings.CopyFrom(
            tls_pb2.TLSSettings(
                enabled=True,
                tls_pem_settings=tls_pb2.TLSPEMSettings(
                    ca=ca_content, key=key_content, cert=cert_content
                ),
            )
        )

        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service, tls_cert=self.tls.create_cert("source-rpk")
        )

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service, tls_cert=self.tls.create_cert("target-rpk")
        )


class ClusterLinkingTopicSyncingWithMtls(ClusterLinkingTopicSyncingTestBase):
    def __init__(self, test_context, *args, **kwargs):
        self.test_context = test_context
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.security.require_client_auth = True
        self.security.endpoint_authn_method = "mtls_identity"
        self.security.kafka_enable_authorization = True
        self.extra_rp_conf = {}

        self.extra_rp_conf["kafka_mtls_principal_mapping_rules"] = [
            self.security.principal_mapping_rules
        ]

        self.current_superusers = ["admin", "source-rpk", "target-rpk"]

        self.extra_rp_conf["superusers"] = self.current_superusers

        super().__init__(
            test_context=self.test_context,
            secondary_cluster_args=SecondaryClusterArgs(
                security=self.security, extra_rp_conf=self.extra_rp_conf
            ),
            security=self.security,
            extra_rp_conf=self.extra_rp_conf,
            *args,
            **kwargs,
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        self.logger.debug("Adding TLS files to link")

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

        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service,
            tls_cert=self.tls.create_cert("source-rpk", common_name="source-rpk"),
        )

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service,
            tls_cert=self.tls.create_cert("target-rpk", common_name="target-rpk"),
        )

    def setUp(self):
        super().setUp()
        names = [n.name for n in self.redpanda.nodes]
        self.logger.info(f"Cluster node names: {names}")
        superusers = set(self.current_superusers + names)
        self.get_source_cluster_rpk().cluster_config_set(
            key="superusers", value=json.dumps(list(superusers))
        )


class ClusterLinkingSchemaRegistry(ShadowLinkTestBase):
    """
    These tests verify the behavior of syncing schema registry
    """

    simple_proto_def = """
syntax = "proto3";

message Simple {
  string id = 1;
}"""

    simple_a_proto_def = """
syntax = "proto3";

message AType {
  float f = 1;
}"""

    simple_b_proto_def = """
syntax = "proto3";

message BType {
  double d = 1;
}"""

    simple_c_proto_def = """
syntax = "proto3";

message CType {
  string id = 1;
}"""

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context=test_context,
            schema_registry_config=SchemaRegistryConfig(),
            secondary_cluster_args=SecondaryClusterArgs(
                schema_registry_config=SchemaRegistryConfig()
            ),
            log_config=LoggingConfig(
                "info",
                logger_levels={
                    "shadow_link": "trace",
                    "kafka/client": "trace",
                    "kafka": "trace",
                    "schemaregistry": "trace",
                    "schemaregistry/requests": "trace",
                    "shadow_link_service": "trace",
                },
            ),
        )

    def source_sr_client(self) -> SchemaRegistryRedpandaClient:
        return SchemaRegistryRedpandaClient(self.source_cluster_service)

    def target_sr_client(self) -> SchemaRegistryRedpandaClient:
        return SchemaRegistryRedpandaClient(self.target_cluster_service)

    def post_schema_to_subject(
        self, sr_client: SchemaRegistryRedpandaClient, subject: str, schema: str
    ) -> int:
        result_raw = sr_client.post_subjects_subject_versions(
            subject=subject,
            data=json.dumps({"schema": schema, "schemaType": "PROTOBUF"}),
        )
        self.logger.debug(f"post_schema_to_subject result: {result_raw}")
        assert result_raw.status_code == 200, (
            f"Failed to post schema to subject {subject}: {result_raw.text}"
        )
        result = result_raw.json()
        assert "id" in result, f"No 'id' in response: {result}"
        return result["id"]

    def get_subjects(self, sr_client: SchemaRegistryRedpandaClient) -> list[str]:
        result_raw = sr_client.get_subjects()
        self.logger.debug(f"get_subjects result: {result_raw}")
        assert result_raw.status_code == 200, (
            f"Failed to get subjects: {result_raw.text}"
        )
        result = result_raw.json()
        assert isinstance(result, list), f"Expected list but got {result}"
        return result

    @cluster(num_nodes=6)
    def test_schema_registry_basic(self):
        """
        This test will verify that the _schemas topic is not created
        until the link has been updated to enable mirroring schema linking
        and then will verify that schemas are replicated from the source to
        the shadow cluster
        """
        topics = [t for t in self.target_cluster_rpk.list_topics()]
        assert "_schemas" not in topics, (
            f"_schemas found in target cluster before link creation: {topics}"
        )

        # Populate source cluster schema registry
        source_sr_client = self.source_sr_client()

        first_id = self.post_schema_to_subject(
            source_sr_client, "first", self.simple_proto_def
        )
        self.logger.debug(f"First id: {first_id}")

        second_id = self.post_schema_to_subject(
            source_sr_client, "second", self.simple_a_proto_def
        )
        self.logger.debug(f"Second id: {second_id}")

        self.logger.info("Creating shadow link")
        created_link = self.create_link("test-link")

        self.logger.info(
            "Waiting 5 seconds and then verifying that _schemas topic is not on target cluster"
        )

        def schemas_in_target() -> bool:
            topics = [t for t in self.target_cluster_rpk.list_topics()]
            self.logger.debug(f"Topics in target cluster: {topics}")
            return "_schemas" in topics

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(schemas_in_target, timeout_sec=5, backoff_sec=1)

        self.logger.info(
            "Verify that the schemas topic gets created when we attempt to access the target schema registry"
        )
        target_sr_client = self.target_sr_client()
        subjects = self.get_subjects(target_sr_client)
        assert len(subjects) == 0, f"Expected no subjects but got {subjects}"
        assert schemas_in_target(), "_schemas topic not found in target cluster"

        self.logger.info("Enabling schema registry mirroring on the link")
        created_link.configurations.schema_registry_sync_options.shadow_schema_registry_topic.CopyFrom(
            shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryTopic()
        )
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.schema_registry_sync_options"]
        )
        updated_link = self.update_link(created_link, update_mask)
        assert updated_link.configurations.schema_registry_sync_options.shadow_schema_registry_topic, (
            "shadow_entire_schema_registry not set after update"
        )

        def subjects_match():
            source_subjects = self.get_subjects(source_sr_client)
            target_subjects = self.get_subjects(target_sr_client)
            self.logger.debug(
                f"source_subjects: {source_subjects}, target_subjects: {target_subjects}"
            )
            return set(source_subjects) == set(target_subjects)

        wait_until(
            subjects_match,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Subjects do not match",
        )

        # Verify we can query the shadow topic
        schemas_topic = self.get_shadow_topic(
            shadow_link_name="test-link", shadow_topic_name="_schemas"
        )
        assert schemas_topic.source_topic_name == "_schemas", (
            f"Expected topic name '_schemas'. Got '{schemas_topic.source_topic_name}'"
        )

        # Verify posting on target schema registry - read is allowed
        second_target_id = self.post_schema_to_subject(
            target_sr_client, "second", self.simple_a_proto_def
        )
        assert second_target_id == second_id, (
            f"Expected id {second_id}. Got {second_target_id}"
        )

        # Verify posting on target schema registry - write is disabled
        result_raw = target_sr_client.post_subjects_subject_versions(
            subject="third",
            data=json.dumps(
                {"schema": self.simple_b_proto_def, "schemaType": "PROTOBUF"}
            ),
        )
        assert result_raw.status_code == 412, (
            f"Expected error code '412'. Got {result_raw.status_code}"
        )

        # Verify that replication keeps up with source SR
        third_id = self.post_schema_to_subject(
            source_sr_client, "third", self.simple_b_proto_def
        )
        self.logger.debug(f"Third id: {third_id}")

        wait_until(
            lambda: "third" in self.get_subjects(source_sr_client),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Failed to write to source SR",
        )

        wait_until(
            subjects_match,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Subjects do not match",
        )

        # Now fail over the Schemas topic and verify that we can now write to it
        self.logger.info("Failing over the _schemas topic")
        self.failover_link_topic(link_name="test-link", topic="_schemas")

        def topic_has_failed_over(link_name: str, topic: str) -> bool:
            shadow_topic = self.get_shadow_topic(
                shadow_link_name=link_name, shadow_topic_name=topic
            )
            self.logger.debug(f"shadow_topic: {shadow_topic}")
            return (
                shadow_topic.status.state
                == shadow_link_pb2.SHADOW_TOPIC_STATE_FAILED_OVER
            )

        wait_until(
            lambda: topic_has_failed_over("test-link", "_schemas"),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="_schemas topic did not failover",
        )

        self.post_schema_to_subject(target_sr_client, "fourth", self.simple_c_proto_def)

    @cluster(
        num_nodes=6,
        log_allow_list=[
            re.compile(".*Schema registry failed to initialize.*"),
            re.compile(
                ".*Shadow Linking actively mirroring schema registry topic.  Topic will not be created.*"
            ),
        ],
    )
    def test_schema_registry_no_create(self):
        """
        This test will verify that the _schemas topic is not created when shadowing is enabled
        but no schemas topic is created on the source
        """
        create_link = self.create_default_link_request("test-link")
        create_link.shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_topic.CopyFrom(
            shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryTopic()
        )

        self.create_link_with_request(req=create_link)

        def schemas_in_target() -> bool:
            topics = [t for t in self.target_cluster_rpk.list_topics()]
            self.logger.debug(f"Topics in target cluster: {topics}")
            return "_schemas" in topics

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(schemas_in_target, timeout_sec=5, backoff_sec=1)

        self.logger.info("Verify that the schemas topic is not created on the source")
        result = self.target_sr_client().get_subjects()
        assert result.status_code == 500, f"Expected 500 but got {result.status_code}"

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(schemas_in_target, timeout_sec=5, backoff_sec=1)

        # Now create the topic and wait for subjects to show up

        # Populate source cluster schema registry
        source_sr_client = self.source_sr_client()

        first_id = self.post_schema_to_subject(
            source_sr_client, "first", self.simple_proto_def
        )
        self.logger.debug(f"First id: {first_id}")

        second_id = self.post_schema_to_subject(
            source_sr_client, "second", self.simple_a_proto_def
        )
        self.logger.debug(f"Second id: {second_id}")

        wait_until(
            schemas_in_target,
            timeout_sec=5,
            backoff_sec=1,
            err_msg="_schemas topic not created on target cluster",
        )

        target_sr_client = self.target_sr_client()

        def subjects_match():
            source_subjects = self.get_subjects(source_sr_client)
            target_subjects = self.get_subjects(target_sr_client)
            self.logger.debug(
                f"source_subjects: {source_subjects}, target_subjects: {target_subjects}"
            )
            return set(source_subjects) == set(target_subjects)

        wait_until(
            subjects_match,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Subjects do not match",
        )


class ShadowLinkingValidateAclsBase(ShadowLinkTestBase):
    """
    Base test class that will verify that the Shadow Link properly syncs ACLs and that
    it handles ACLs correctly
    """

    SHADOW_LINK_USER = "shadow-link-user"
    SHADOW_LINK_PASSWORD = "shadow-link-password"

    TEST_CLIENT_USER = "test-client-user"
    TEST_CLIENT_PASSWORD = "test-client-password"

    def __init__(self, test_context, *args, **kwargs):
        self.default_topic_replication = 3
        extra_rp_conf = {"default_topic_replications": self.default_topic_replication}

        if "extra_rp_conf" in kwargs:
            extra_rp_conf.update(kwargs["extra_rp_conf"])
            kwargs.pop("extra_rp_conf")

        super().__init__(
            test_context,
            extra_rp_conf=extra_rp_conf,
            *args,
            **kwargs,
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        raise NotImplementedError("Must be implemented in subclass")

    def create_default_link_request(
        self, link_name: str, *args, **kwargs
    ) -> shadow_link_pb2.CreateShadowLinkRequest:
        req = super().create_default_link_request(link_name=link_name, *args, **kwargs)
        req.shadow_link.CopyFrom(self.add_credentials_to_link(req.shadow_link))
        return req

    def create_link(
        self, link_name: str, *args, **kwargs
    ) -> shadow_link_pb2.ShadowLink:
        req = self.create_default_link_request(link_name=link_name, *args, **kwargs)
        return self.create_link_with_request(req=req)

    def get_source_cluster_rpk(self) -> RpkTool:
        raise NotImplementedError("Must be implemented in subclass")

    def get_target_cluster_rpk(self) -> RpkTool:
        raise NotImplementedError("Must be implemented in subclass")

    def get_target_cluster_superuser_rpk(self) -> RpkTool:
        raise NotImplementedError("Must be implemented in subclass")

    @cluster(num_nodes=6)
    def test_topics_with_proper_acls(self):
        """
        This test verifies that topics can be synced from the target
        cluster to the source cluster when proper ACLs are in place.  The Shadow Link
        principal must have READ, DESCRIBE, and DESCRIBE_CONFIG access to the topic.
        Additionally this test will verify that ACLs are only applied on the target cluster
        once the proper ACLs are assigned to the shadow link user
        """
        topic_name = "test-topic"

        topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=3)

        self.get_source_cluster_rpk().create_topic(
            topic=topic.name,
            partitions=topic.partition_count,
            replicas=topic.replication_factor,
        )

        source_rpk = self.get_source_cluster_rpk()
        target_rpk = self.get_target_cluster_rpk()
        target_superuser_rpk = self.get_target_cluster_superuser_rpk()

        self.create_link("test-link", mirror_all_acls=False)

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(
                lambda: self.topic_exists_in_target(
                    topic_name, rpk=target_superuser_rpk
                ),
                timeout_sec=5,
                backoff_sec=1,
            )

        # The following adds ACLs for the shadow link user one at a time
        describe_acl = RPKACLInput()
        describe_acl.allow_principal = [f"User:{self.SHADOW_LINK_USER}"]
        describe_acl.topic = [topic_name]
        describe_acl.operation = ["DESCRIBE"]
        source_rpk.acl_create(describe_acl)

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(
                lambda: self.topic_exists_in_target(
                    topic_name, rpk=target_superuser_rpk
                ),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Now add read
        read_acl = RPKACLInput()
        read_acl.allow_principal = [f"User:{self.SHADOW_LINK_USER}"]
        read_acl.topic = [topic_name]
        read_acl.operation = ["READ"]
        source_rpk.acl_create(read_acl)

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(
                lambda: self.topic_exists_in_target(
                    topic_name, rpk=target_superuser_rpk
                ),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Now finally DESCRIBE_CONFIGS
        describe_configs_acl = RPKACLInput()
        describe_configs_acl.allow_principal = [f"User:{self.SHADOW_LINK_USER}"]
        describe_configs_acl.topic = [topic_name]
        describe_configs_acl.operation = ["DESCRIBE_CONFIGS"]
        source_rpk.acl_create(describe_configs_acl)

        wait_until(
            lambda: self.topic_exists_in_target(topic_name, rpk=target_superuser_rpk),
            timeout_sec=5,
            backoff_sec=1,
        )

        # Now verify that the ACLs have not yet been brought over
        acls: Any = target_superuser_rpk.acl_list(format="json")
        assert len(acls["matches"]) == 0, (
            f"Expected no ACLs but found: {acls['matches']}"
        )

        # And verify we cannot see that topic
        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(
                lambda: self.topic_exists_in_target(topic_name, rpk=target_rpk),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Update the ACL mirroring settings to mirror all ACLs for the shadow link user
        update_link = self.get_link("test-link")

        resource_filter = shadow_link_pb2.ACLResourceFilter(
            resource_type=acl_pb2.ACL_RESOURCE_ANY, pattern_type=acl_pb2.ACL_PATTERN_ANY
        )
        access_filter = shadow_link_pb2.ACLAccessFilter(
            principal=f"User:{self.TEST_CLIENT_USER}",
            operation=acl_pb2.ACL_OPERATION_ANY,
            permission_type=acl_pb2.ACL_PERMISSION_TYPE_ANY,
        )
        acl_filter = shadow_link_pb2.ACLFilter(
            resource_filter=resource_filter, access_filter=access_filter
        )
        acl_filters: list[shadow_link_pb2.ACLFilter] = [acl_filter]

        update_link.configurations.security_sync_options.acl_filters.extend(acl_filters)

        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.security_sync_options.acl_filters"]
        )
        self.update_link(update_link, update_mask)

        def acls_present_in_target() -> bool:
            acls: Any = target_superuser_rpk.acl_list(format="json")
            matches: list[dict[str, str]] = acls["matches"]
            self.logger.debug(f"Matches: {matches}")
            if len(matches) != 1:
                return False
            return all(
                acl["principal"] == f"User:{self.TEST_CLIENT_USER}"
                and acl["operation"] == "ALL"
                and acl["resource_type"] == "TOPIC"
                and acl["resource_name"] == topic_name
                for acl in matches
            )

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(
                lambda: acls_present_in_target(),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Update source cluster to give shadow link permission to view ACLs (DESCRIBE on CLUSTER)
        source_rpk.acl_create_allow_cluster(
            username=self.SHADOW_LINK_USER, op="describe"
        )

        # We still should not see any ACLs present as we're filtering for the TEST_CLIENT_USER
        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(
                lambda: acls_present_in_target(),
                timeout_sec=5,
                backoff_sec=1,
            )

        # And verify we cannot see that topic
        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(
                lambda: self.topic_exists_in_target(topic_name, rpk=target_rpk),
                timeout_sec=5,
                backoff_sec=1,
            )

        # Now finally add the ACLs for the TEST_CLIENT_USER
        full_acl = RPKACLInput()
        full_acl.allow_principal = [f"User:{self.TEST_CLIENT_USER}"]
        full_acl.topic = [topic_name]
        full_acl.operation = ["ALL"]
        source_rpk.acl_create(full_acl)

        wait_until(
            lambda: acls_present_in_target(),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="ACLs not present in target cluster",
        )

        wait_until(
            lambda: self.topic_exists_in_target(topic_name, rpk=target_rpk),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic not visible to test client in target cluster",
        )

        # Verify we cannot write to the topic
        with expect_exception(RpkException, lambda e: "POLICY_VIOLATION" in str(e)):
            target_rpk.produce(topic_name, "key", "msg")

        # Now fail over the topic
        self.failover_link_topic(link_name="test-link", topic=topic_name)

        # Now wait for the state to be failed over
        def topic_failed_over() -> bool:
            shadow_topic = self.get_shadow_topic("test-link", topic_name)
            return (
                shadow_topic.status.state
                == shadow_link_pb2.SHADOW_TOPIC_STATE_FAILED_OVER
            )

        wait_until(
            lambda: topic_failed_over,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic did not fail over",
        )

        # Propogation of the state change may take a few moments, so retry until success
        wait_until(
            lambda: target_rpk.produce(topic_name, "key", "msg") is not None,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Unable to produce to failed over topic",
            retry_on_exc=True,
        )


class ShadowLinkingValidateAclsScram(ShadowLinkingValidateAclsBase):
    """
    Runs the base test with SCRAM authentication
    """

    def __init__(self, test_context, *args, **kwargs):
        self.security = SecurityConfig()
        self.security.enable_sasl = True
        self.security.endpoint_authn_method = "sasl"
        secondary_args: SecondaryClusterArgs = SecondaryClusterArgs(
            security=self.security
        )

        self.cluster_link_user = self.SHADOW_LINK_USER
        self.cluster_link_password = self.SHADOW_LINK_PASSWORD
        self.cluster_link_mechanism = shadow_link_pb2.SCRAM_MECHANISM_SCRAM_SHA_256

        super().__init__(
            test_context=test_context,
            secondary_cluster_args=secondary_args,
            security=self.security,
            *args,
            **kwargs,
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
        shadow_link.configurations.client_options.authentication_configuration.scram_configuration.CopyFrom(
            shadow_link_pb2.ScramConfig(
                username=self.SHADOW_LINK_USER,
                password=self.SHADOW_LINK_PASSWORD,
                scram_mechanism=shadow_link_pb2.SCRAM_MECHANISM_SCRAM_SHA_256,
            )
        )

        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.mechanism,
        )

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service,
            username=self.TEST_CLIENT_USER,
            password=self.TEST_CLIENT_PASSWORD,
            sasl_mechanism="SCRAM-SHA-256",
        )

    def get_target_cluster_superuser_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.mechanism,
        )

    def setUp(self):
        super().setUp()
        self.get_source_cluster_rpk().sasl_create_user(
            self.SHADOW_LINK_USER, self.SHADOW_LINK_PASSWORD
        )
        self.get_target_cluster_superuser_rpk().sasl_create_user(
            self.TEST_CLIENT_USER, self.TEST_CLIENT_PASSWORD
        )


class ShadowLinkTlsProvider(TLSProvider):
    def __init__(self, tls: TLSCertManager, shadow_link_user: str):
        self.tls: TLSCertManager = tls
        self.shadow_link_user = shadow_link_user

    @property
    def ca(self) -> CertificateAuthority:
        return self.tls.ca

    def create_broker_cert(self, service: Service, node: ClusterNode) -> Certificate:
        assert node in service.nodes
        return self.tls.create_cert(node.name, common_name=self.shadow_link_user)

    def create_service_client_cert(self, service: Service, name: str) -> Certificate:
        return self.tls.create_cert(socket.gethostname(), name=name, common_name=name)


class ShadowLinkingValidateAclsMTls(ShadowLinkingValidateAclsBase):
    """
    Runs the base test with mTLS authentication
    """

    def __init__(self, test_context, *args, **kwargs):
        self.test_context = test_context
        self.tls = TLSCertManager(self.logger)
        self.source_security = SecurityConfig()
        self.source_security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.source_security.require_client_auth = True
        self.source_security.endpoint_authn_method = "mtls_identity"
        self.source_security.kafka_enable_authorization = True

        self.target_security = SecurityConfig()
        self.target_security.tls_provider = ShadowLinkTlsProvider(
            self.tls, shadow_link_user=self.SHADOW_LINK_USER
        )
        self.target_security.require_client_auth = True
        self.target_security.endpoint_authn_method = "mtls_identity"
        self.target_security.kafka_enable_authorization = True

        self.extra_rp_conf = {}

        self.extra_rp_conf["kafka_mtls_principal_mapping_rules"] = [
            self.source_security.principal_mapping_rules
        ]

        secondary_args: SecondaryClusterArgs = SecondaryClusterArgs(
            security=self.source_security, extra_rp_conf=self.extra_rp_conf
        )

        super().__init__(
            test_context=test_context,
            secondary_cluster_args=secondary_args,
            security=self.target_security,
            extra_rp_conf=self.extra_rp_conf,
            *args,
            **kwargs,
        )

    def add_credentials_to_link(
        self, shadow_link: shadow_link_pb2.ShadowLink
    ) -> shadow_link_pb2.ShadowLink:
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

        return shadow_link

    def get_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.source_cluster.service,
            tls_cert=self.tls.create_cert(
                "source-rpk", common_name=self.redpanda.SUPERUSER_CREDENTIALS.username
            ),
        )

    def get_target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service,
            tls_cert=self.tls.create_cert(
                "target-rpk", common_name=self.TEST_CLIENT_USER
            ),
        )

    def get_target_cluster_superuser_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service,
            tls_cert=self.tls.create_cert(
                "target-superuser-rpk",
                common_name=self.redpanda.SUPERUSER_CREDENTIALS.username,
            ),
        )


class ClusterLinkingStorageModeSync(ClusterLinkingTopicSyncingTestBase):
    """
    Tests for syncing redpanda.storage.mode via cluster linking.
    """

    @cluster(num_nodes=6)
    @matrix(storage_mode=["unset", "local"])
    def test_storage_mode_sync(self, storage_mode):
        """
        Verifies that redpanda.storage.mode is synced from source to target
        when creating mirror topics via cluster linking.
        """
        topic_name = "test-storage-mode-topic"

        topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=1)

        self.get_source_cluster_rpk().create_topic(
            topic=topic.name,
            partitions=topic.partition_count,
            replicas=topic.replication_factor,
            config={"redpanda.storage.mode": storage_mode},
        )

        # Verify source topic has the expected storage mode
        source_configs = self.get_source_cluster_rpk().describe_topic_configs(
            topic.name
        )
        assert source_configs["redpanda.storage.mode"][0] == storage_mode, (
            f"Source topic storage mode: expected {storage_mode}, "
            f"got {source_configs['redpanda.storage.mode'][0]}"
        )

        created_link = self.create_link("test-link")
        self.validate_created_link(created_link)

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic not created on target cluster",
        )

        # Verify target topic has the same storage mode
        target_configs = self.get_target_cluster_rpk().describe_topic_configs(
            topic.name
        )
        assert target_configs["redpanda.storage.mode"][0] == storage_mode, (
            f"Target topic storage mode: expected {storage_mode}, "
            f"got {target_configs['redpanda.storage.mode'][0]}"
        )

    @cluster(num_nodes=6)
    def test_storage_mode_change_sync(self):
        """
        Verifies that changes to redpanda.storage.mode on the source topic
        are synced to the mirror topic via cluster linking.
        """
        topic_name = "test-storage-mode-change"

        topic = TopicSpec(name=topic_name, partition_count=3, replication_factor=1)

        self.get_source_cluster_rpk().create_topic(
            topic=topic.name,
            partitions=topic.partition_count,
            replicas=topic.replication_factor,
            config={"redpanda.storage.mode": "unset"},
        )

        created_link = self.create_link("test-link")
        self.validate_created_link(created_link)

        wait_until(
            lambda: self._topics_are_present(
                self.get_target_cluster_rpk(), [topic], True
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Topic not created on target cluster",
        )

        # Verify initial storage mode
        target_configs = self.get_target_cluster_rpk().describe_topic_configs(
            topic.name
        )
        assert target_configs["redpanda.storage.mode"][0] == "unset", (
            f"Expected unset, got {target_configs['redpanda.storage.mode'][0]}"
        )

        # Change storage mode on source from unset to local
        self.get_source_cluster_rpk().alter_topic_config(
            topic_name, "redpanda.storage.mode", "local"
        )

        # Wait for the change to sync
        def storage_mode_synced():
            configs = self.get_target_cluster_rpk().describe_topic_configs(topic.name)
            return configs["redpanda.storage.mode"][0] == "local"

        wait_until(
            storage_mode_synced,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Storage mode change not synced to target cluster",
        )


class ClusterLinkingCloudTopicSync(ShadowLinkTestBase):
    """
    Tests that cloud topics (redpanda.storage.mode=cloud) are
    mirrored to the target cluster via cluster linking.
    """

    def __init__(self, test_context, *args: Any, **kwargs: Any):
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
                log_config=LoggingConfig(
                    "info",
                    logger_levels={
                        "cluster": "trace",
                        "shadow_link": "trace",
                        "kafka/client": "trace",
                        "kafka": "trace",
                    },
                ),
            ),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=6)
    def test_cloud_topic_mirrored(self):
        """
        A source topic with redpanda.storage.mode=cloud should be
        mirrored to the target cluster via cluster linking, just like
        a normal topic.
        """
        cloud_topic = TopicSpec(
            name="cloud-topic", partition_count=3, replication_factor=1
        )
        normal_topic = TopicSpec(
            name="normal-topic", partition_count=3, replication_factor=1
        )

        source_rpk = RpkTool(self.source_cluster.service)

        # Create a cloud topic and a normal topic on the source
        source_rpk.create_topic(
            topic=cloud_topic.name,
            partitions=cloud_topic.partition_count,
            replicas=cloud_topic.replication_factor,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )
        source_rpk.create_topic(
            topic=normal_topic.name,
            partitions=normal_topic.partition_count,
            replicas=normal_topic.replication_factor,
        )

        # Verify the source cloud topic actually has cloud storage mode
        source_configs = source_rpk.describe_topic_configs(cloud_topic.name)
        assert (
            source_configs[TopicSpec.PROPERTY_STORAGE_MODE][0]
            == TopicSpec.STORAGE_MODE_CLOUD
        ), (
            f"Source topic storage mode: {source_configs[TopicSpec.PROPERTY_STORAGE_MODE]}"
        )

        self.create_link("test-link")

        # Both topics should appear on the target
        target_rpk = RpkTool(self.target_cluster.service)
        wait_until(
            lambda: normal_topic.name in target_rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Normal topic {normal_topic.name} not found in target cluster",
        )
        wait_until(
            lambda: cloud_topic.name in target_rpk.list_topics(),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Cloud topic {cloud_topic.name} not found in target cluster",
        )
