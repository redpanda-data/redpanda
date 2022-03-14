# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools


class CompactionRecoveryTest(RedpandaTest):
    """
    Verify that segment indices are recovered on startup.

    The basic strategy is:

       1. Create some segments
       2. Shutdown the nodes
       3. Delete all the segment indexes
       4. Restart nodes
       5. Verify that the indexes are recreated

    Having compaction run complicates this in two ways. First, because
    compaction removes/creates indices and staging files grabbing a consistent
    view can be a challenge. For this, giving the system time to reach a
    quiescent state is sufficient.

    The main complication is adjacent segment compaction in which the segments
    are combined. This isn't related to recovery, but the same process that
    joins segments also recovers the indexes, and the joining process can take a
    long time (especially on debug builds) and creates a lot of file system
    churn.

    The solution to this second problem is to choose a upper bound of 1 byte for
    the combined segment size so that segment joining is effectively disabled.
    """
    topics = (TopicSpec(partition_count=1,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_COMPACT), )

    def __init__(self, test_context):
        extra_rp_conf = dict(compacted_log_segment_size=1048576, )

        super(CompactionRecoveryTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_index_recovery(self, log_allow_list=RESTART_LOG_ALLOW_LIST):
        partitions = self.produce_until_segments(3)

        for p in partitions:
            self.redpanda.stop_node(p.node)

        for p in partitions:
            p.delete_indices(allow_fail=False)

        extra_rp_conf = dict(compacted_log_segment_size=1048576,
                             log_compaction_interval_ms=1000,
                             max_compacted_log_segment_size=1,
                             compaction_ctrl_min_shares=1000,
                             compaction_ctrl_max_shares=1000)

        for p in partitions:
            self.redpanda.start_node(p.node)

        self.redpanda.set_cluster_config(extra_rp_conf, True)

        wait_until(lambda: all(map(lambda p: p.recovered(), partitions)),
                   timeout_sec=90,
                   backoff_sec=2,
                   err_msg="Timeout waiting for partitions to recover.")

    def produce_until_segments(self, count):
        partitions = []
        kafka_tools = KafkaCliTools(self.redpanda)

        def check_partitions():
            kafka_tools.produce(self.topic, 1024, 1024)
            storage = self.redpanda.storage()
            partitions[:] = storage.partitions("kafka", self.topic)
            return partitions and all(
                map(lambda p: len(p.segments) > count and p.recovered(),
                    partitions))

        wait_until(lambda: check_partitions(),
                   timeout_sec=90,
                   backoff_sec=2,
                   err_msg="Segments not found")

        return partitions
