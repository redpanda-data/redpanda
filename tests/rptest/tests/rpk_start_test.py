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

    @cluster(num_nodes=3)
    def test_simple_start_three_with_seeds(self):
        """
        Validate simple start using rpk with multiple nodes. Node IDs should be
        assigned automatically by Redpanda, configuring seeds-driven cluster
        formation.
        """
        node_ids = set()
        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.redpanda.nodes])

        def start_with_rpk(node):
            seeds_arg = f"--seeds={seeds_str}"
            bootstrap_arg = "--set redpanda.empty_seed_starts_cluster=false"
            args = f"{seeds_arg} {bootstrap_arg} --rpc-addr={node.account.hostname}"
            self.redpanda.start_node_with_rpk(node, args)

        # Seed nodes need to be started in parallel because they need to be
        # able to form a quorum before they become ready.
        self.redpanda._for_nodes(self.redpanda.nodes,
                                 start_with_rpk,
                                 parallel=True)
        for node in self.redpanda.nodes:
            node_ids.add(self.redpanda.node_id(node))
        assert len(node_ids) == 3, f"Node IDs: {node_ids}"

    @cluster(num_nodes=3)
    def test_bootstrap_then_start(self):
        """
        Validate bootstrap and start using rpk with multiple nodes. Node IDs
        should be assigned automatically by Redpanda, configuring seeds-driven
        cluster formation.
        """
        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.redpanda.nodes])

        def config_bootstrap_with_rpk(node):
            self.redpanda.clean_node(node)
            node.account.mkdirs(
                os.path.dirname(RedpandaService.NODE_CONFIG_FILE))
            seeds_arg = f"--ips={seeds_str}"
            rpk = f"{self.rpk._rpk_binary()} --config {RedpandaService.NODE_CONFIG_FILE}"
            # Dockerized test runs don't play well with the bootstrap command's
            # `--self` config, which expects an IP. Instead, manually set the
            # RPC server address to something usable with a Dockerized network.
            node.account.ssh(f"{rpk} redpanda config bootstrap {seeds_arg} && " \
                    f"{rpk} redpanda config set redpanda.empty_seed_starts_cluster false && " \
                    f"{rpk} redpanda config set redpanda.rpc_server " \
                    f"'{{\"address\":\"{node.account.hostname}\",\"port\":33145}}'")

        self.redpanda._for_nodes(self.redpanda.nodes,
                                 config_bootstrap_with_rpk,
                                 parallel=True)

        # Run a start with no arguments, as is done when Redpanda is run by a
        # systemd service.
        self.redpanda._for_nodes(
            self.redpanda.nodes,
            lambda n: self.redpanda.start_node_with_rpk(n, clean_node=False),
            parallel=True)
        node_ids = set()
        for node in self.redpanda.nodes:
            node_ids.add(self.redpanda.node_id(node))
        assert len(node_ids) == 3, f"Node IDs: {node_ids}"

    @cluster(num_nodes=3)
    def test_simple_start_three_with_root(self):
        """
        Validate simple start using rpk with multiple nodes. Node IDs should be
        assigned automatically by Redpanda, configuring a single root node as
        the cluster founder.
        """
        node_ids = set()
        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.redpanda.nodes])
        for node in self.redpanda.nodes:
            seeds_arg = f"--seeds={seeds_str}"
            if self.redpanda.idx(node) == 1:
                seeds_arg = ""
            args = f"{seeds_arg} --rpc-addr={node.account.hostname}"
            self.redpanda.start_node_with_rpk(node, args)
            node_ids.add(self.redpanda.node_id(node))
        assert len(node_ids) == 3, f"Node IDs: {node_ids}"

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
            "storage_min_free_bytes": "10485760",
            "topic_partitions_per_shard": "1000",
            "fetch_reads_debounce_timeout": "10"
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

        # First we clean the node and then we write via
        # rpk redpanda mode set prod.
        self.redpanda.clean_node(node)
        rpk = RpkRemoteTool(self.redpanda, node)
        rpk.mode_set("production")

        # Avoid cleaning, that will delete the config files and we
        # already cleaned the node above.
        self.redpanda.start_node_with_rpk(node, clean_node=False)

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
