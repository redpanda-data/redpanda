# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest


class HostMetricsTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args, num_brokers=1, extra_rp_conf={"enable_host_metrics": True}, **kwargs
        )

    @cluster(num_nodes=1)
    def test_basic_hoststats(self):
        """
        Test that we export the host stats

        """

        metrics = list(self.redpanda.metrics(self.redpanda.nodes[0]))

        disk_count = 0
        netstat_count = 0
        snmp_count = 0
        for family in metrics:
            if family.name == "vectorized_host_diskstats_reads":
                for sample in family.samples:
                    disk_count += int(sample.value)
            if family.name == "vectorized_host_netstat_bytes_received":
                for sample in family.samples:
                    netstat_count += int(sample.value)
            if family.name == "vectorized_host_snmp_packets_received":
                for sample in family.samples:
                    snmp_count += int(sample.value)

        assert disk_count > 0, "Expected some disk reads"
        assert netstat_count > 0, "Expected some received bytes"
        assert snmp_count > 0, "Expected some received packets"
