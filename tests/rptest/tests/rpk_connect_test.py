# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception
from ducktape.utils.util import wait_until
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.clients.rpk import RpkException


class RpkConnectTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkConnectTest, self).__init__(test_context=ctx)
        self._ctx = ctx

    def cleanup_connect(self, node):
        rpk_remote = RpkRemoteTool(self.redpanda, node)
        rpk_remote.uninstall_connect()
        assert "connect" not in rpk_remote.plugin_list()

    def wait_until_installed(self, node):
        def installed():
            rpk_remote = RpkRemoteTool(self.redpanda, node)
            plugin_list = rpk_remote.plugin_list()
            return "connect" in plugin_list

        wait_until(
            installed,
            timeout_sec=120,
            backoff_sec=2,
            err_msg=
            f"could not find 'redpanda-connect' in plugin list after installing"
        )

    @cluster(num_nodes=1)
    def test_manual_latest_install(self):
        node = self.redpanda.get_node(0)
        self.cleanup_connect(node)

        rpk_remote = RpkRemoteTool(self.redpanda, node)
        rpk_remote.install_connect()
        self.wait_until_installed(node)

    @cluster(num_nodes=1)
    def test_upgrade(self):
        node = self.redpanda.get_node(0)
        self.cleanup_connect(node)

        rpk_remote = RpkRemoteTool(self.redpanda, node)
        initial_version = "4.35.1"
        rpk_remote.install_connect(initial_version)
        self.wait_until_installed(node)
        assert rpk_remote.connect_version() == initial_version

        rpk_remote.upgrade_connect()
        self.wait_until_installed(node)
        # We assume here that is updated to latest.
        assert rpk_remote.connect_version() != initial_version

    @cluster(num_nodes=1)
    def test_automatic_install(self):
        node = self.redpanda.get_node(0)
        self.cleanup_connect(node)
        rpk_remote = RpkRemoteTool(self.redpanda, node)

        # rpk will not install if the user runs just --help, instead
        # it will display the help text and exit 0.
        assert "Usage:" in rpk_remote.run_connect_arbitrary(["--help"])

        # rpk shouldn't install either if the user runs --version without
        # connect being installed first.
        with expect_exception(
                RpkException,
                lambda e: "rpk connect is not installed" in str(e)):
            rpk_remote.connect_version()

        # Now, rpk should download and install on-the-fly Connect when
        # executing a subcommand.
        assert "connect" not in rpk_remote.plugin_list()
        rpk_remote.run_connect_arbitrary(["run", "--help"])
        self.wait_until_installed(node)

        # And executing --version now that is installed should return
        # something and exit 0.
        assert rpk_remote.connect_version() != ""
