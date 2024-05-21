# Copyright 2023 Redpanda Data, Inc.
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
from ducktape.mark import ok_to_fail_fips


class RpkPluginTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkPluginTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @ok_to_fail_fips
    @cluster(num_nodes=1)
    def test_managed_byoc(self):
        """
        Test that rpk is able to recognize the plugin when installed
        in the default folder, and assert that we are passing the
        required flags to the plugin itself.
        """
        assert ".rpk.managed-byoc" in self._rpk.plugin_list()

        def find_flag(flags, name, value):
            for flag in flags:
                if flag["name"] == name and flag["value"] == value:
                    return True
            return False

        test_id = "test_id"
        test_token = "test_token"

        # First we validate only passing the redpanda_id and token.
        out = self._rpk.cloud_byoc_aws_apply(redpanda_id=test_id,
                                             token=test_token)
        assert len(out["args"]) == 0
        assert find_flag(out["flags"], "--redpanda-id", test_id)
        assert find_flag(out["flags"], "--cloud-api-token", test_token)

        # Now we validate that we strip rpk flags (-X and --verbose).
        out = self._rpk.cloud_byoc_aws_apply(
            redpanda_id=test_id,
            token=test_token,
            extra_flags=["-X", "brokers=127.0.0.1:9092", "--verbose"])

        assert len(out["args"]) == 0

        # Included:
        assert find_flag(out["flags"], "--redpanda-id", test_id)
        assert find_flag(out["flags"], "--cloud-api-token", test_token)
        # Stripped:
        assert not find_flag(out["flags"], "-X", "brokers=127.0.0.1:9092")
        assert not find_flag(out["flags"], "--verbose", "")

        # Now we validate that we pass other byoc-only flags.
        region = "us-east-2"
        out = self._rpk.cloud_byoc_aws_apply(redpanda_id=test_id,
                                             token=test_token,
                                             extra_flags=[
                                                 "-X",
                                                 "brokers=127.0.0.1:9092",
                                                 "--verbose", "--region",
                                                 region
                                             ])

        assert len(out["args"]) == 0

        # Included:
        assert find_flag(out["flags"], "--redpanda-id", test_id)
        assert find_flag(out["flags"], "--cloud-api-token", test_token)
        assert find_flag(out["flags"], "--region", region)

        # Stripped:
        assert not find_flag(out["flags"], "-X", "brokers=127.0.0.1:9092")
        assert not find_flag(out["flags"], "--verbose", "")
