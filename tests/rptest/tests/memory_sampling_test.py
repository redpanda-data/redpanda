# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode

BOOTSTRAP_CONFIG = {
    'memory_enable_memory_sampling': True,
}


class MemorySamplingTestTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        rp_conf = BOOTSTRAP_CONFIG.copy()

        super(MemorySamplingTestTest, self).__init__(*args,
                                                     extra_rp_conf=rp_conf,
                                                     **kwargs)

        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    @skip_debug_mode  # not using seastar allocator in debug
    def test_get_all_stacks(self):
        """
        Verify that the sampled_memory_profile GET endpoint serves valid json
        with some profile data.

        """
        admin = Admin(self.redpanda)
        profile = admin.get_sampled_memory_profile()

        num_shards = self.redpanda.shards()[1] + 1
        assert len(profile) == num_shards
        assert 'shard' in profile[0]
        assert 'allocation_sites' in profile[0]
        assert len(profile[0]['allocation_sites']) > 0
        assert 'size' in profile[0]['allocation_sites'][0]
        assert 'count' in profile[0]['allocation_sites'][0]
        assert 'backtrace' in profile[0]['allocation_sites'][0]

    @cluster(num_nodes=1)
    @skip_debug_mode  # not using seastar allocator in debug
    def test_get_per_shard_stacks(self):
        """
        Verify that the sampled_memory_profile GET endpoint serves valid json
        with some profile data with a shard parameter

        """
        admin = Admin(self.redpanda)
        profile = admin.get_sampled_memory_profile(shard=1)

        assert len(profile) == 1
        assert 'shard' in profile[0]
        assert 'allocation_sites' in profile[0]
        assert len(profile[0]['allocation_sites']) > 0
        assert 'size' in profile[0]['allocation_sites'][0]
        assert 'count' in profile[0]['allocation_sites'][0]
        assert 'backtrace' in profile[0]['allocation_sites'][0]
