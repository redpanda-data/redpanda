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
from rptest.clients.rpk import RpkTool, RpkException


class RpkConnectTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkConnectTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    def cleanup_connect(self):
        self._rpk.uninstall_connect()
        assert "connect" not in self._rpk.plugin_list()

    @cluster(num_nodes=1)
    def test_manual_latest_install(self):
        self.cleanup_connect()
        self._rpk.install_connect()
        assert "connect" in self._rpk.plugin_list()

    @cluster(num_nodes=1)
    def test_upgrade(self):
        self.cleanup_connect()

        initial_version = "4.35.1"
        self._rpk.install_connect(initial_version)
        assert "connect" in self._rpk.plugin_list()
        assert self._rpk.connect_version() == initial_version

        self._rpk.upgrade_connect()
        # We assume here that is updated to latest.
        assert self._rpk.connect_version() != initial_version

    @cluster(num_nodes=1)
    def test_automatic_install(self):
        self.cleanup_connect()

        # rpk will not install if the user runs just --help, instead
        # it will display the help text and exit 0.
        assert "Usage:" in self._rpk.run_connect_arbitrary(["--help"])

        # rpk shouldn't install either if the user runs --version without
        # connect being installed first.
        with expect_exception(
                RpkException,
                lambda e: "rpk connect is not installed" in str(e)):
            self._rpk.connect_version()

        # Now, rpk should download and install on-the-fly Connect when
        # executing a subcommand.
        assert "connect" not in self._rpk.plugin_list()
        self._rpk.run_connect_arbitrary(["run", "--help"])
        assert "connect" in self._rpk.plugin_list()

        # And executing --version now that is installed should return
        # something and exit 0.
        assert self._rpk.connect_version() != ""
