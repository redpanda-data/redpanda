# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import hashlib
import json
import random

import requests
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import security_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.http_server import HttpServer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SchemaRegistryConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.rpenv import sample_license


class MetricsReporterServer:
    def __init__(self, test_ctx):
        self.http = HttpServer(test_ctx)

    def start(self):
        self.http.start()

    def stop(self):
        self.http.stop()

    def rp_conf(self):
        return {
            # report every two seconds
            "metrics_reporter_tick_interval": 2000,
            "metrics_reporter_report_interval": 1000,
            "enable_metrics_reporter": True,
            "metrics_reporter_url": f"{self.http.url}/metrics",
        }

    def clear_requests(self):
        self.http.requests.clear()

    def requests(self):
        return self.http.requests

    def reports(self):
        return [
            json.loads(r["body"]) for r in self.requests() if r["path"] == "/metrics"
        ]


class MetricsReporterTest(RedpandaTest):
    def __init__(self, test_ctx, num_brokers):
        self._ctx = test_ctx
        self.metrics = MetricsReporterServer(self._ctx)
        super(MetricsReporterTest, self).__init__(
            test_context=test_ctx,
            num_brokers=num_brokers,
            extra_rp_conf={
                "health_monitor_max_metadata_age": 1000,
                "retention_bytes": 20000,
                **self.metrics.rp_conf(),
            },
        )
        self.redpanda.set_environment({"REDPANDA_ENVIRONMENT": "test"})

    def setUp(self):
        # Start HTTP server before redpanda to avoid connection errors
        self.metrics.start()
        self.redpanda.start()

    def _test_redpanda_metrics_reporting(self):
        """
        Test that redpanda nodes send well formed messages to the metrics endpoint
        """

        # Load and put a license at start. This is to check the SHA-256 checksum
        admin = Admin(self.redpanda)
        license = sample_license()
        if license is None:
            self.logger.info("Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        assert admin.put_license(license).status_code == 200, "PUT License failed"

        # blow away the metrics state so we can test the has_license flag later
        self.metrics.clear_requests()

        total_topics = 5
        total_partitions = 0
        for _ in range(0, total_topics):
            partitions = random.randint(1, 8)
            total_partitions += partitions
            self.client().create_topic(
                [
                    TopicSpec(
                        partition_count=partitions,
                        replication_factor=len(self.redpanda.nodes),
                    )
                ]
            )

        # create topics
        self.redpanda.logger.info(
            f"created {total_topics} topics with {total_partitions} partitions"
        )

        def _state_up_to_date():
            if self.metrics.requests():
                r = self.metrics.reports()[-1]
                self.logger.info(f"Latest request: {r}")
                return r["topic_count"] == total_topics
            else:
                self.logger.info("No requests yet")
            return False

        wait_until(_state_up_to_date, 20, backoff_sec=1)
        self.metrics.stop()
        metadata = self.metrics.reports()
        for m in metadata:
            self.redpanda.logger.info(m)

        def assert_fields_are_the_same(metadata, field):
            def maybe_sort(value):
                if not isinstance(value, list):
                    return value
                return sorted(value)

            assert all(
                maybe_sort(m[field]) == maybe_sort(metadata[0][field]) for m in metadata
            )

        features = admin.get_features()

        # cluster uuid and create timestamp should stay the same across requests
        assert_fields_are_the_same(metadata, "cluster_uuid")
        assert_fields_are_the_same(metadata, "cluster_created_ts")
        # Configuration should be the same across requests
        assert_fields_are_the_same(metadata, "has_kafka_gssapi")
        assert_fields_are_the_same(metadata, "has_oidc")
        # cluster config should be the same
        assert_fields_are_the_same(metadata, "config")
        # No transforms are deployed
        assert_fields_are_the_same(metadata, "data_transforms_count")
        # license violation status should not change across requests
        assert_fields_are_the_same(metadata, "has_valid_license")
        assert_fields_are_the_same(metadata, "has_enterprise_features")
        assert_fields_are_the_same(metadata, "enterprise_features")
        assert_fields_are_the_same(metadata, "hostname")
        assert_fields_are_the_same(metadata, "domainname")
        assert_fields_are_the_same(metadata, "fqdns")
        # get the last report
        last = metadata.pop()
        assert last["topic_count"] == total_topics
        assert last["local_topic_count"] == total_topics
        assert last["cloud_topic_count"] == 0
        assert last["partition_count"] == total_partitions
        assert last["has_kafka_gssapi"] is False
        assert last["has_oidc"] is False
        assert last["data_transforms_count"] == 0
        assert last["active_logical_version"] == features["cluster_version"]
        assert last["original_logical_version"] == features["original_cluster_version"]
        assert last["has_valid_license"]
        # NOTE: value will vary depending on FIPS mode. we're confident that
        # the source of the value is sound, so assert on presence instead.
        assert "has_enterprise_features" in last
        assert "enterprise_features" in last
        assert type(last["enterprise_features"]) is list
        assert "hostname" in last
        assert "domainname" in last
        assert "fqdns" in last
        nodes_meta = last["nodes"]

        assert len(last["nodes"]) == len(self.redpanda.nodes)

        assert all("node_id" in n for n in nodes_meta)
        assert all("cpu_count" in n for n in nodes_meta)
        assert all("version" in n for n in nodes_meta)
        assert all("logical_version" in n for n in nodes_meta)
        assert all("uptime_ms" in n for n in nodes_meta)
        assert all("is_alive" in n for n in nodes_meta)
        assert all("disks" in n for n in nodes_meta)
        assert all("kafka_advertised_listeners" in n for n in nodes_meta)

        # Check cluster UUID and creation time survive a restart
        for n in self.redpanda.nodes:
            self.redpanda.stop_node(n)

        pre_restart_requests = len(self.metrics.requests())

        self.metrics.start()
        for n in self.redpanda.nodes:
            self.redpanda.start_node(n)

        wait_until(
            lambda: len(self.metrics.requests()) > pre_restart_requests,
            timeout_sec=20,
            backoff_sec=1,
        )
        self.redpanda.logger.info("Checking metadata after restart")
        assert_fields_are_the_same(metadata, "cluster_uuid")
        assert_fields_are_the_same(metadata, "cluster_created_ts")

        # Check config values
        assert last["config"]["retention_bytes"] == "[value]"
        assert last["config"]["enable_metrics_reporter"] == True
        assert last["config"]["auto_create_topics_enabled"] == False
        assert "metrics_reporter_tick_interval" not in last["config"]
        assert last["config"]["log_message_timestamp_type"] == "CreateTime"
        assert last["redpanda_environment"] == "test"

        raw_id_hash = hashlib.sha256(license.encode()).hexdigest()
        last_post_restart = metadata.pop()
        assert last_post_restart["id_hash"] == last["id_hash"]
        assert last_post_restart["id_hash"] == raw_id_hash


class MultiNodeMetricsReporterTest(MetricsReporterTest):
    """
    Metrics reporting on a typical 3 node cluster
    """

    def __init__(self, test_ctx):
        super().__init__(test_ctx, 3)

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_redpanda_metrics_reporting(self):
        self._test_redpanda_metrics_reporting()


class SingleNodeMetricsReporterTest(MetricsReporterTest):
    """
    Metrics reporting on a single node cluster: verify that our cluster_uuid
    generation works properly with fewer raft_configuration batches in the
    controller log than a multi-node system has.
    """

    def __init__(self, test_ctx):
        super().__init__(test_ctx, 1)

    @cluster(num_nodes=2, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_redpanda_metrics_reporting(self):
        self._test_redpanda_metrics_reporting()


class UniqueGroupCountMetricsTest(RedpandaTest):
    """
    Test that unique_group_count is correctly reported in metrics.
    Groups can come from two sources:
    - Role members with type 'Group'
    - ACL principals with 'Group:' prefix
    """

    def __init__(self, test_ctx):
        self._ctx = test_ctx
        self.metrics = MetricsReporterServer(self._ctx)
        super(UniqueGroupCountMetricsTest, self).__init__(
            test_context=test_ctx,
            num_brokers=1,
            extra_rp_conf={
                "health_monitor_max_metadata_age": 1000,
                **self.metrics.rp_conf(),
            },
        )

    def setUp(self):
        self.metrics.start()
        self.redpanda.start()

    def _make_group_member(self, group_name: str) -> security_pb2.RoleMember:
        """Create a RoleMember with a group."""
        return security_pb2.RoleMember(group=security_pb2.RoleGroup(name=group_name))

    @cluster(num_nodes=2)
    def test_unique_group_count(self):
        """
        Test that unique_group_count correctly counts groups from roles and ACLs.
        """
        superuser = self.redpanda.SUPERUSER_CREDENTIALS
        admin_v2 = AdminV2(self.redpanda, auth=(superuser.username, superuser.password))
        rpk = RpkTool(self.redpanda)

        # Initially, unique_group_count should be 0
        def _get_unique_group_count():
            if self.metrics.requests():
                r = self.metrics.reports()[-1]
                self.logger.info(f"Latest request: {r}")
                return r.get("unique_group_count", -1)
            return -1

        def _wait_for_group_count(expected):
            def check():
                count = _get_unique_group_count()
                self.logger.info(
                    f"Current unique_group_count: {count}, expected: {expected}"
                )
                return count == expected

            wait_until(check, timeout_sec=20, backoff_sec=1)

        # Wait for initial report with 0 groups
        _wait_for_group_count(0)

        # Add a role with a group member
        self.logger.info("Creating role with group member")
        admin_v2.security().create_role(
            security_pb2.CreateRoleRequest(
                role=security_pb2.Role(
                    name="test_role_1",
                    members=[self._make_group_member("group1")],
                )
            )
        )

        # Should now have 1 unique group
        _wait_for_group_count(1)

        # Add another role with the same group (should still be 1 unique)
        self.logger.info("Creating another role with same group member")
        admin_v2.security().create_role(
            security_pb2.CreateRoleRequest(
                role=security_pb2.Role(
                    name="test_role_2",
                    members=[self._make_group_member("group1")],
                )
            )
        )

        # Still 1 unique group (same group name)
        _wait_for_group_count(1)

        # Add a role with a different group
        self.logger.info("Creating role with different group member")
        admin_v2.security().create_role(
            security_pb2.CreateRoleRequest(
                role=security_pb2.Role(
                    name="test_role_3",
                    members=[self._make_group_member("group2")],
                )
            )
        )

        # Now 2 unique groups
        _wait_for_group_count(2)

        # Add an ACL with Group: principal
        self.logger.info("Creating ACL with Group: principal")
        rpk.acl_create_allow_cluster(
            username="group3", op="describe", principal_type="Group"
        )

        # Now 3 unique groups
        _wait_for_group_count(3)

        # Add another ACL with same group (should still be 3)
        self.logger.info("Creating another ACL with same Group: principal")
        rpk.acl_create_allow_cluster(
            username="group3", op="alter", principal_type="Group"
        )

        # Still 3 unique groups
        _wait_for_group_count(3)

        # Add an ACL with a group that's also in a role (group1)
        self.logger.info("Creating ACL with Group: principal that matches role group")
        rpk.acl_create_allow_cluster(
            username="group1", op="describe", principal_type="Group"
        )

        # Still 3 unique groups (group1 already counted from roles)
        _wait_for_group_count(3)

        # Add a new unique group via ACL
        self.logger.info("Creating ACL with new Group: principal")
        rpk.acl_create_allow_cluster(
            username="group4", op="describe", principal_type="Group"
        )

        # Now 4 unique groups
        _wait_for_group_count(4)

        self.metrics.stop()


class SchemaRegistryContextMetricsTest(RedpandaTest):
    # Simple Avro schema for testing
    SCHEMA_DEF = (
        '{"type":"record","name":"test","fields":[{"name":"f1","type":"string"}]}'
    )

    def __init__(self, test_ctx):
        self._ctx = test_ctx
        self.metrics = MetricsReporterServer(self._ctx)
        super(SchemaRegistryContextMetricsTest, self).__init__(
            test_context=test_ctx,
            num_brokers=1,
            extra_rp_conf={
                "health_monitor_max_metadata_age": 1000,
                "schema_registry_enable_qualified_subjects": True,
                **self.metrics.rp_conf(),
            },
            schema_registry_config=SchemaRegistryConfig(),
        )

    def setUp(self):
        self.metrics.start()
        self.redpanda.start()

    def tearDown(self):
        self.metrics.stop()
        super().tearDown()

    def _get_sr_base_uri(self):
        return f"http://{self.redpanda.nodes[0].account.hostname}:8081"

    def _register_schema(self, subject: str):
        """Register a schema to a subject (may be context-qualified)."""
        uri = f"{self._get_sr_base_uri()}/subjects/{subject}/versions"
        headers = {
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Accept": "application/vnd.schemaregistry.v1+json",
        }
        data = json.dumps({"schema": self.SCHEMA_DEF})
        resp = requests.post(uri, headers=headers, data=data)
        assert resp.status_code == 200, f"Failed to register schema: {resp.text}"
        return resp.json()

    @cluster(num_nodes=2)
    def test_schema_registry_context_count(self):
        """
        Test that schema_registry.context_count correctly counts non-default contexts.
        """

        def _get_context_count() -> int | None:
            if self.metrics.requests():
                r = self.metrics.reports()[-1]
                self.logger.info(f"Latest request: {r}")
                sr = r.get("schema_registry")
                if sr is None:
                    return None
                return sr.get("context_count", None)
            return None

        def _wait_for_context_count(expected: int):
            def check():
                count = _get_context_count()
                self.logger.info(
                    f"Current context_count: {count}, expected: {expected}"
                )
                return count == expected

            wait_until(check, timeout_sec=30, backoff_sec=1)

        # Initially, context_count should be 0 (only default context exists)
        _wait_for_context_count(0)

        # Register a schema in the default context (no prefix)
        self.logger.info("Registering schema in default context")
        self._register_schema("default-subject")

        # Still 0 non-default contexts
        _wait_for_context_count(0)

        # Register a schema in a non-default context using qualified subject
        self.logger.info("Creating first non-default context")
        self._register_schema(":.ctx1:subject1")

        # Now 1 non-default context
        _wait_for_context_count(1)

        # Register another schema in the same context
        self.logger.info("Registering another schema in same context")
        self._register_schema(":.ctx1:subject2")

        # Still 1 non-default context (same context)
        _wait_for_context_count(1)

        # Create a second non-default context
        self.logger.info("Creating second non-default context")
        self._register_schema(":.ctx2:subject1")

        # Now 2 non-default contexts
        _wait_for_context_count(2)
