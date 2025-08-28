# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import sys

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import inject_remote_script


class TopicIdMigrationTest(RedpandaTest):
    LEADER_BALANCER_PERIOD_MS = 60 * 1_000  # 60s
    """
    Test that verifies at scale the Topic ID migration action
    """

    def __init__(self, test_context):
        super(TopicIdMigrationTest, self).__init__(
            test_context=test_context,
            num_brokers=5,
            extra_rp_conf={
                "leader_balancer_idle_timeout": self.LEADER_BALANCER_PERIOD_MS,
                "leader_balancer_mute_timeout": self.LEADER_BALANCER_PERIOD_MS,
                # Increase connections limit to well above what this test reaches
                "kafka_connections_max": 100_000,
                "kafka_connections_max_per_ip": 100_000,
                # We don't scrub tiered storage in this test because it is slow
                # (on purpose) and takes unreasonable amount of time for a CI
                # job. We should figure out how to make it faster for this
                # use-case.
                "cloud_storage_enable_scrubbing": False,
                # Minimis disk usage.
                "log_segment_size": 1048576,
                # Squeeze lots of partition replicas on to the cluster
                "topic_partitions_per_shard": 131072,
                "topic_memory_per_partition": 10 * 1024,
            },
        )
        self.installer: RedpandaInstaller = self.redpanda._installer
        self.admin = Admin(self.redpanda)

    def setUp(self):
        self.upgrade_version = self.installer.latest_for_line((25, 2))[0]
        # Use head until 25.2 is released
        self.upgrade_version = (
            RedpandaInstaller.HEAD
            if self.upgrade_version[1] != 2
            else self.upgrade_version
        )
        self.logger.debug(f"Using upgrade version: {self.upgrade_version}")
        # For some reason when I select (25,1,1) it uses head which isn't helpful
        self.installer.install(self.redpanda.nodes, (25, 1, 2))
        super(TopicIdMigrationTest, self).setUp()

    def _try_parse_json(self, node, jsondata):
        try:
            return json.loads(jsondata)
        except ValueError:
            self.logger.debug(
                f"{str(node.account)}: Could not parse as json: {str(jsondata)}"
            )
            return {}

    def _create_topics(self, brokers: str, node: ClusterNode, topic_count: int):
        remote_script_path = inject_remote_script(node, "topic_operations.py")
        cmd = f"python3 {remote_script_path} "
        cmd += f"--brokers '{brokers}' "
        cmd += "--batch-size 2048 "
        cmd += "create "
        cmd += f"--topic-count {topic_count} "
        cmd += "--kafka-batching "
        cmd += "--skip-randomize-names"
        hostname = node.account.hostname
        self.logger.info(f'Starting topic creation script on "{hostname}"')
        self.logger.debug(f"...cmd: {cmd}")

        data = {}
        for line in node.account.ssh_capture(cmd):
            self.logger.debug(f"received {sys.getsizeof(line)}B from '{hostname}'.")
            data = self._try_parse_json(node, line.strip())
            if "error" in data:
                self.logger.warning(
                    f"Node '{hostname}' reported error:\n{data['error']}"
                )
                raise RuntimeError(data["error"])

        topic_details = data.get("topics", [])
        current_count = len(topic_details)
        self.logger.info(f"Created {current_count} topics")
        assert len(topic_details) == topic_count, (
            f"Topic count not reached: {current_count}/{topic_count}"
        )
        return topic_details

    @cluster(num_nodes=6)
    @matrix(num_topics=[10, 100, 1000, 10000])
    def test_topic_id_migration(self, num_topics: int):
        self.redpanda.set_expected_controller_records(200 + num_topics * 2)

        brokers = ",".join(self.redpanda.brokers_list())
        node = self.cluster.alloc(ClusterSpec.simple_linux(1))[0]
        deets = self._create_topics(brokers=brokers, node=node, topic_count=num_topics)
        self.logger.info(f'Topic deets: "{deets}"')
        self.cluster.free_single(node)

        self.logger.info(f"Upgrading redpanda to {self.upgrade_version}")
        self.installer.install(self.redpanda.nodes, self.upgrade_version)

        self.redpanda.restart_nodes(self.redpanda.nodes)

        def wait_for_topic_id_to_be_active():
            features = self.admin.get_features()["features"]
            self.logger.debug(f"Features: {features}")
            for f in features:
                if f["name"] == "topic_ids":
                    if f["state"] == "active":
                        return True
            return False

        wait_until(
            wait_for_topic_id_to_be_active,
            timeout_sec=60,
            backoff_sec=5,
            err_msg="Topic ID migration feature did not become active",
            retry_on_exc=True,
        )

        assert self.redpanda.search_log_any(
            "Successfully assigned a UUID to all existing topics"
        )
