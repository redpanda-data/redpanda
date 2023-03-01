# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool, RpkException
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import ResourceSettings
from rptest.services.cluster import cluster
from rptest.utils.mode_checks import skip_debug_mode


class ResourceLimitsTest(RedpandaTest):
    """
    Tests the limits that are imposed on partition counts during topic
    creation, to prevent users from overwhelming available system
    resources with too many partitions.
    """
    def setUp(self):
        # Override parent setUp so that we don't start redpanda until each test,
        # enabling each test case to customize its ResourceSettings
        pass

    @cluster(num_nodes=3)
    def test_memory_limited(self):
        """
        Check enforcement of the RAM-per-partition threshold
        """
        self.redpanda.set_resource_settings(
            ResourceSettings(memory_mb=1024, num_cpus=1))
        self.redpanda.set_extra_rp_conf({
            # Use a larger than default memory per partition, so that a 1GB system can be
            # tested without creating 1000 partitions (which overwhelms debug redpanda
            # builds because they're much slower than the real product)
            'topic_memory_per_partition':
            10 * 1024 * 1024,
        })

        self.redpanda.start()

        rpk = RpkTool(self.redpanda)

        # Three nodes, each with 1GB memory, replicas=3, should
        # result in an effective limit of 1024 with the default
        # threshold of 1MB per topic.
        try:
            rpk.create_topic("toobig", partitions=110, replicas=3)
        except RpkException as e:
            assert 'INVALID_PARTITIONS' in e.msg
        else:
            assert False

        # Should succeed
        rpk.create_topic("okay", partitions=55, replicas=3)

        # Trying to grow the partition count in violation of the limit should fail
        try:
            rpk.add_topic_partitions("okay", 55)
        except RpkException as e:
            assert 'INVALID_PARTITIONS' in e.msg
        else:
            assert False

        # Growing the partition count within the limit should succeed
        rpk.add_topic_partitions("okay", 10)

    @skip_debug_mode
    @cluster(num_nodes=3)
    def test_cpu_limited(self):
        """
        Check enforcement of the partitions-per-core
        """
        self.redpanda.set_resource_settings(ResourceSettings(num_cpus=1))

        self.redpanda.set_extra_rp_conf({
            # Disable memory limit: on a test node the physical memory can easily
            # be the limiting factor
            'topic_memory_per_partition': None,
            # Disable FD enforcement: tests running on workstations may have low ulimits
            'topic_fds_per_partition': None
        })

        self.redpanda.start()

        rpk = RpkTool(self.redpanda)

        # Three nodes, each with 1 core, 1000 partition-replicas
        # per core, so with replicas=3, 1000 partitions should be the limit
        try:
            rpk.create_topic("toobig", partitions=1500, replicas=3)
        except RpkException as e:
            assert 'INVALID_PARTITIONS' in e.msg
        else:
            assert False

        # This is not exactly 1000 because of system partitions consuming
        # some of hte allowance.
        rpk.create_topic("okay", partitions=900, replicas=3)

    @cluster(num_nodes=3)
    def test_fd_limited(self):
        self.redpanda.set_resource_settings(ResourceSettings(nfiles=1000))
        self.redpanda.set_extra_rp_conf({
            # Disable memory limit: on a test node the physical memory can easily
            # be the limiting factor
            'topic_memory_per_partition': None,
        })
        self.redpanda.start()

        rpk = RpkTool(self.redpanda)

        # Default 5 fds per partition, we set ulimit down to 1000, so 100 should be the limit
        try:
            rpk.create_topic("toobig", partitions=220, replicas=3)
        except RpkException as e:
            assert 'INVALID_PARTITIONS' in e.msg
        else:
            assert False

        # Should succeed
        rpk.create_topic("okay", partitions=90, replicas=3)
