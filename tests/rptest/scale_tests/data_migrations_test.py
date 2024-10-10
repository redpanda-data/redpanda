# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.mark import parametrize
from rptest.tests.data_migrations import DataMigrationsTest

MIB_PER_PARTITION = 1
PARTITIONS_PER_SHARD = 10000
STOP_TIMEOUT = 5 * 60


class DataMigrationsScaleTest(DataMigrationsTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         **kwargs,
                         transition_timeout=600,
                         extra_rp_conf={
                             'topic_partitions_per_shard':
                             PARTITIONS_PER_SHARD,
                             'topic_memory_per_partition':
                             MIB_PER_PARTITION * 1024 * 1024,
                         })

    @cluster(num_nodes=3,
             log_allow_list=DataMigrationsTest.MIGRATION_LOG_ALLOW_LIST)
    @parametrize(topics_count=1000, partitions_count=3)
    @parametrize(topics_count=3, partitions_count=1000)
    def test_migrate(self, topics_count, partitions_count):
        self.do_test_creating_and_listing_migrations(topics_count,
                                                     partitions_count)
        self.redpanda.stop(timeout=STOP_TIMEOUT)
