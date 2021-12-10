# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re
import zipfile
import json

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk_remote import RpkRemoteTool


class RpkClusterTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkClusterTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_cluster_info(self):
        def condition():
            brokers = self._rpk.cluster_info()

            if len(brokers) != len(self.redpanda.nodes):
                return False

            advertised_addrs = self.redpanda.brokers()

            ok = True
            for b in brokers:
                ok = ok and \
                    b.address in advertised_addrs

            return ok

        wait_until(condition,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="No brokers found or output doesn't match")

    @cluster(num_nodes=3)
    def test_debug_bundle(self):
        # The main RpkTool helper runs rpk on the test runner machine -- debug
        # commands are run on redpanda nodes.

        working_dir = "/tmp"
        node = self.redpanda.nodes[0]

        rpk_remote = RpkRemoteTool(self.redpanda, node)
        output = rpk_remote.debug_bundle(working_dir)
        lines = output.split("\n")

        # On error, rpk bundle returns 0 but writes error description to stdout
        output_file = None
        error_lines = []
        any_errs = False
        for l in lines:
            self.logger.info(l)
            if l.strip().startswith("* "):
                error_lines.append(l)
            elif 'errors occurred' in l:
                any_errs = True
            else:
                m = re.match("^Debug bundle saved to '(.+)'$", l)
                if m:
                    output_file = m.group(1)

        # Avoid false passes if our error line scraping gets broken
        # by a format change.
        if any_errs:
            assert error_lines

        filtered_errors = []
        for l in error_lines:
            if "dmidecode" in l:
                # dmidecode doesn't work in ducktape containers, ignore
                # errors about it.
                continue
            if re.match('.* error querying .*\.ntp\..* i\/o timeout', l):
                self.logger.error(f"Non-fatal transitory NTP error: {l}")
            else:
                self.logger.error(f"Bad output line: {l}")
                filtered_errors.append(l)

        assert not filtered_errors
        assert output_file is not None

        output_path = os.path.join(working_dir, output_file)
        node.account.copy_from(output_path, working_dir)

        zf = zipfile.ZipFile(output_path)
        files = zf.namelist()
        assert 'redpanda.yaml' in files
        assert 'redpanda.log' in files
        assert 'prometheus-metrics.txt' in files

    @cluster(num_nodes=3)
    def test_get_config(self):
        rpk_bin = self.redpanda.find_binary('rpk')
        node = self.redpanda.nodes[0]

        config_output = self._rpk.admin_config_print(node)

        # Check the output is valid json
        parsed = json.loads("".join(config_output))

        # Check the output contains at least one known config property
        assert 'enable_transactions' in parsed
