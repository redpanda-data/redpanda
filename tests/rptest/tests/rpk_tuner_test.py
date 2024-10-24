# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os

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

    @cluster(num_nodes=1)
    def test_tune_list(self):
        """
        Forward compatible test, the purpose is to check if available
        tuners match our current setup, if a new tuner gets added we
        will catch it here.
        """
        node = self.redpanda.nodes[0]
        rpk = RpkRemoteTool(self.redpanda, node)
        # Set all tuners:
        rpk.mode_set("prod")
        rpk.config_set('rpk.tune_fstrim', 'true')
        rpk.config_set('rpk.tune_transparent_hugepages', 'true')
        rpk.config_set('rpk.tune_coredump', 'true')

        expected = '''TUNER                  ENABLED  SUPPORTED  UNSUPPORTED-REASON
aio_events             true     true       
ballast_file           true     true       
clocksource            true     true       
coredump               true     true       
cpu                    true     true       
disk_irq               true     true       
disk_nomerges          true     true       
disk_scheduler         true     true       
disk_write_cache       true     false      Disk write cache tuner is only supported in GCP
fstrim                 true     true       
net                    true     true       
swappiness             true     true       
transparent_hugepages  true     true       
'''

        uname = str(node.account.ssh_output("uname -m"))
        # either x86-64 or i386.
        is_not_x86 = "86" not in uname

        r = str(node.account.ssh_output("dmidecode -s system-product-name"))
        isGCP = "Google Compute Engine" in r

        # Clocksource is only available for x86 architectures.
        expected = expected.replace(
            "clocksource            true     true       ",
            "clocksource            true     false      Clocksource setting not available for this architecture"
        ) if is_not_x86 else expected

        expected = expected.replace(
            "disk_write_cache       true     false      Disk write cache tuner is only supported in GCP",
            "disk_write_cache       true     true       "
        ) if isGCP else expected

        output = rpk.tune("list")
        if output != expected:
            self.logger.debug(f"expected:\n{expected}\ngot:\n{output}")

        assert output == expected
