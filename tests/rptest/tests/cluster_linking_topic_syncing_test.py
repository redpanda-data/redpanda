# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from rptest.clients.admin.proto.redpanda.core.admin.v2 import shadow_link_pb2
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.multi_cluster_services import SecondaryClusterArgs
from rptest.services.redpanda import SecurityConfig, TLSProvider
from rptest.services.tls import CertificateAuthority, Certificate, TLSCertManager
from rptest.tests.cluster_linking_test_base import ShadowLinkTestBase
from rptest.util import wait_until

import socket


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
            t.replication_factor = self.default_topic_replication

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

        shadow_link_req.shadow_link.configurations.topic_metadata_sync_options.shadowed_topic_properties.append(
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


class ClusterLinkingTLSProvider(TLSProvider):
    def __init__(self, tls: TLSCertManager):
        self.tls: TLSCertManager = tls

    @property
    def ca(self) -> CertificateAuthority:
        return self.tls.ca

    def create_broker_cert(self, service: Service, node: ClusterNode) -> Certificate:
        assert node in service.nodes
        return self.tls.create_cert(node.name)

    def create_service_client_cert(self, service: Service, name: str) -> Certificate:
        return self.tls.create_cert(socket.gethostname(), name=name, common_name=name)


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
            shadow_link_pb2.TLSSettings(
                enabled=True,
                tls_file_settings=shadow_link_pb2.TLSFileSettings(
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

    def validate_created_link(self, shadow_link: shadow_link_pb2.ShadowLink) -> None:
        assert (
            shadow_link.configurations.client_options.tls_settings.WhichOneof(
                "tls_settings"
            )
            == "tls_pem_settings"
        ), (
            f"Expected 'tls_pem_settings' but got {shadow_link.configurations.client_options.tls_settings.WhichOneof('tls_settings')}"
        )

        # TODO: Add key fingerprint

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
        cert_content = open(self.client_cert.crt, "r").read()
        self.logger.debug(f"cert: {cert_content}")
        key_content = open(self.client_cert.key, "r").read()
        self.logger.debug(f"key: {key_content}")

        shadow_link.configurations.client_options.tls_settings.CopyFrom(
            shadow_link_pb2.TLSSettings(
                enabled=True,
                tls_pem_settings=shadow_link_pb2.TLSPEMSettings(
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
