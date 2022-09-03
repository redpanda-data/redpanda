# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.services.redpanda import RedpandaService

import os
import yaml
import tempfile


class RpkRedpandaStartTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkRedpandaStartTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start it
        pass

    @cluster(num_nodes=1)
    def test_simple_start(self):
        """
        Validate simple start using rpk, no additional flags.
        """
        node = self.redpanda.nodes[0]
        self.redpanda.start_node_with_rpk(node)

        # By default we start with developer_mode: true.
        assert self.redpanda.search_log_any(
            "WARNING: This is a setup for development purposes only")

    @cluster(num_nodes=1)
    def test_container_mode(self):
        """
        Verify that when using flag --mode dev-container we succesfully
        set some cluster properties. We verify this using rpk instead
        of using admin endpoint directly.
        """
        node = self.redpanda.nodes[0]
        self.redpanda.start_node_with_rpk(node, "--mode dev-container")

        expected_cluster_properties = {
            "auto_create_topics_enabled": "true",
            "group_topic_partitions": "3",
            # RPK returns scientific notation for large int values.
            # https://github.com/redpanda-data/redpanda/issues/6070
            "storage_min_free_bytes": "1.048576e+07",
            "topic_partitions_per_shard": "1000"
        }

        for p in expected_cluster_properties:
            cli_readback = self.rpk.cluster_config_get(p)
            if cli_readback != expected_cluster_properties[p]:
                self.logger.error(
                    f"Unexpected value for {p}, expected {expected_cluster_properties[p]}, got {cli_readback}"
                )
            assert cli_readback == expected_cluster_properties[p]

        assert self.redpanda.search_log_any(
            "WARNING: This is a setup for development purposes only")

    @cluster(num_nodes=1)
    def test_production_mode(self):
        """
        Test will set production mode, start redpanda, and verify
        that we are executing the checks.
        """
        node = self.redpanda.nodes[0]
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        self.redpanda.clean_node(node)
        rpk = RpkRemoteTool(self.redpanda, node)
        rpk.mode_set("production")

        self.redpanda.start_node_with_rpk(node)

        # First we check that we don't modify redpanda.developer_mode
        # on the first start.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.NODE_CONFIG_FILE, d)
            with open(os.path.join(d, 'redpanda.yaml')) as f:
                actual_config = yaml.full_load(f.read())
                assert 'developer_mode' not in actual_config['redpanda']

        # This is production, just checking that we are not setting
        # anything that makes rpk think that is a dev environment.
        assert not self.redpanda.search_log_any(
            "WARNING: This is a setup for development purposes only")

        # We execute checks when starting redpanda if production mode is enabled
        assert self.redpanda.search_log_any("System check - PASSED")

    @cluster(num_nodes=1)
    def test_seastar_flag(self):
        """
        Reproduce: https://github.com/redpanda-data/redpanda/issues/4778
        Verify that rpk doesn't transforms additional arguments. 
        """
        node = self.redpanda.nodes[0]
        node.account.mkdirs(os.path.dirname(RedpandaService.NODE_CONFIG_FILE))

        self.redpanda.start_node_with_rpk(node, "--abort-on-seastar-bad-alloc")

        # This was the original issue:
        assert not self.redpanda.search_log_any(
            f"\-\-abort-on-seastar-bad-alloc=true")
