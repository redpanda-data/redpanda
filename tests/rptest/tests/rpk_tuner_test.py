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
from rptest.clients.rpk_remote import RpkRemoteTool


class RpkTunerTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkTunerTest, self).__init__(test_context=ctx)
        self._ctx = ctx

    @cluster(num_nodes=1)
    def test_tune_prod_all(self):
        """
        Test will set production mode and execute rpk redpanda tune all,
        we expect the command to exit with 1 if an error happens.
        """
        node = self.redpanda.nodes[0]
        rpk = RpkRemoteTool(self.redpanda, node)
        rpk.mode_set("prod")

        rpk.tune("all")

    @cluster(num_nodes=1)
    def test_tune_fstrim(self):
        """
        Validate fstrim tuner execution,
        fstrim was disabled in production mode https://github.com/redpanda-data/redpanda/issues/3068 
        """
        node = self.redpanda.nodes[0]
        rpk = RpkRemoteTool(self.redpanda, node)
        rpk.config_set('rpk.tune_fstrim', 'true')

        rpk.tune("fstrim")

    @cluster(num_nodes=1)
    def test_tune_transparent_hugepages(self):
        """
        Validate transparent hugepage tuner execution.
        THP tuner is disabled in production mode
        """
        node = self.redpanda.nodes[0]
        rpk = RpkRemoteTool(self.redpanda, node)
        rpk.config_set('rpk.tune_transparent_hugepages', 'true')

        rpk.tune("transparent_hugepages")
