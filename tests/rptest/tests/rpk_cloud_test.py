# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile
import yaml

from rptest.services.cluster import cluster

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService
from rptest.clients.rpk import RpkTool


def get_ci_env_var(env_var):
    out = os.environ.get(env_var, None)
    if out is None:
        is_ci = os.environ.get("CI", "false")
        if is_ci == "true":
            raise RuntimeError(
                f"Expected {env_var} variable to be set in this environment")

    return out


def read_rpk_cfg(node):
    with tempfile.TemporaryDirectory() as d:
        node.account.copy_from(RedpandaService.RPK_CONFIG_FILE, d)
        with open(os.path.join(d, 'rpk.yaml')) as f:
            return yaml.full_load(f.read())


class RpkCloudTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkCloudTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=1)
    def test_cloud_login_logout_cc(self):
        """
        Test login to cloud via rpk using client
        credentials, make sure we store the token and
        then delete it when we logout.
        """
        id = get_ci_env_var("RPK_TEST_CLIENT_ID")
        secret = get_ci_env_var("RPK_TEST_CLIENT_SECRET")
        if id is None or secret is None:
            self.logger.warn(
                "Skipping test, client credentials env vars not found")
            return

        output = self._rpk.cloud_login_cc(id, secret)
        assert "Successfully logged in" in output

        # Check for a token present in file.
        node = self.redpanda.get_node(0)
        rpk_yaml = read_rpk_cfg(node)
        assert rpk_yaml["cloud_auth"][0]["auth_token"] is not None

        # Check that the token is not there anymore.
        output = self._rpk.cloud_logout()
        assert "You are now logged out" in output
        rpk_yaml = read_rpk_cfg(node)
        assert rpk_yaml["cloud_auth"][0]["auth_token"] is None
