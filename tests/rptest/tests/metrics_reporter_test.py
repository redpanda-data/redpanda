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

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.utils.rpenv import sample_license
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.http_server import HttpServer


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
        return [json.loads(r['body']) for r in self.requests()]


class MetricsReporterTest(RedpandaTest):
    def __init__(self, test_ctx, num_brokers):
        self._ctx = test_ctx
        self.metrics = MetricsReporterServer(self._ctx)
        super(MetricsReporterTest,
              self).__init__(test_context=test_ctx,
                             num_brokers=num_brokers,
                             extra_rp_conf={
                                 "health_monitor_max_metadata_age": 1000,
                                 "retention_bytes": 20000,
                                 **self.metrics.rp_conf(),
                             })
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
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        assert admin.put_license(
            license).status_code == 200, "PUT License failed"

        # blow away the metrics state so we can test the has_license flag later
        self.metrics.clear_requests()

        total_topics = 5
        total_partitions = 0
        for _ in range(0, total_topics):
            partitions = random.randint(1, 8)
            total_partitions += partitions
            self.client().create_topic([
                TopicSpec(partition_count=partitions,
                          replication_factor=len(self.redpanda.nodes))
            ])

        # create topics
        self.redpanda.logger.info(
            f"created {total_topics} topics with {total_partitions} partitions"
        )

        def _state_up_to_date():
            if self.metrics.requests():
                r = self.metrics.reports()[-1]
                self.logger.info(f"Latest request: {r}")
                return r['topic_count'] == total_topics
            else:
                self.logger.info("No requests yet")
            return False

        wait_until(_state_up_to_date, 20, backoff_sec=1)
        self.metrics.stop()
        metadata = self.metrics.reports()
        for m in metadata:
            self.redpanda.logger.info(m)

        def assert_fields_are_the_same(metadata, field):
            assert all(m[field] == metadata[0][field] for m in metadata)

        features = admin.get_features()

        # cluster uuid and create timestamp should stay the same across requests
        assert_fields_are_the_same(metadata, 'cluster_uuid')
        assert_fields_are_the_same(metadata, 'cluster_created_ts')
        # Configuration should be the same across requests
        assert_fields_are_the_same(metadata, 'has_kafka_gssapi')
        assert_fields_are_the_same(metadata, 'has_oidc')
        # cluster config should be the same
        assert_fields_are_the_same(metadata, 'config')
        # No transforms are deployed
        assert_fields_are_the_same(metadata, 'data_transforms_count')
        # license violation status should not change across requests
        assert_fields_are_the_same(metadata, 'has_valid_license')
        assert_fields_are_the_same(metadata, 'has_enterprise_features')
        # get the last report
        last = metadata.pop()
        assert last['topic_count'] == total_topics
        assert last['partition_count'] == total_partitions
        assert last['has_kafka_gssapi'] is False
        assert last['has_oidc'] is False
        assert last['data_transforms_count'] == 0
        assert last['active_logical_version'] == features['cluster_version']
        assert last['original_logical_version'] == features[
            'original_cluster_version']
        assert last['has_valid_license']
        # NOTE: value will vary depending on FIPS mode. we're confident that
        # the source of the value is sound, so assert on presence instead.
        assert 'has_enterprise_features' in last
        nodes_meta = last['nodes']

        assert len(last['nodes']) == len(self.redpanda.nodes)

        assert all('node_id' in n for n in nodes_meta)
        assert all('cpu_count' in n for n in nodes_meta)
        assert all('version' in n for n in nodes_meta)
        assert all('logical_version' in n for n in nodes_meta)
        assert all('uptime_ms' in n for n in nodes_meta)
        assert all('is_alive' in n for n in nodes_meta)
        assert all('disks' in n for n in nodes_meta)

        # Check cluster UUID and creation time survive a restart
        for n in self.redpanda.nodes:
            self.redpanda.stop_node(n)

        pre_restart_requests = len(self.metrics.requests())

        self.metrics.start()
        for n in self.redpanda.nodes:
            self.redpanda.start_node(n)

        wait_until(lambda: len(self.metrics.requests()) > pre_restart_requests,
                   timeout_sec=20,
                   backoff_sec=1)
        self.redpanda.logger.info("Checking metadata after restart")
        assert_fields_are_the_same(metadata, 'cluster_uuid')
        assert_fields_are_the_same(metadata, 'cluster_created_ts')

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
