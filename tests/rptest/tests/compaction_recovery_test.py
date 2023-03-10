# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import produce_until_segments

import os.path


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

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_index_recovery(self):
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


class CompactionRecoveryUpgradeTest(RedpandaTest):
    OLD_VERSION = (22, 2, 8)
    SEGMENT_SIZE = 1048576

    topics = (TopicSpec(partition_count=1,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_COMPACT), )

    def __init__(self, *args, **kwargs):
        super(CompactionRecoveryUpgradeTest, self).__init__(
            *args,
            extra_rp_conf=dict(compacted_log_segment_size=self.SEGMENT_SIZE,
                               log_compaction_interval_ms=1000,
                               max_compacted_log_segment_size=1,
                               compaction_ctrl_min_shares=1000,
                               compaction_ctrl_max_shares=1000),
            **kwargs)

    def setUp(self):
        self.installer = self.redpanda._installer
        self.installer.install(self.redpanda.nodes, self.OLD_VERSION)
        super(CompactionRecoveryUpgradeTest, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_index_recovery_after_upgrade(self):
        """
        Test compatibility between v1 and v2 compaction index footers.
        * Redpanda version with v2 footers should be able to recover
          v1 footers without rebuilding them.
        * Old redpanda version with v1 footers is not able to recover
          v2 footers, but at least it should be able to rebuild them.
        """

        # The version we will update+rollback to, before eventually upgrading
        # all the way to HEAD
        next_version = self.installer.latest_for_line(release_line=(23, 1))[0]

        def get_storage_partition(node):
            storage = self.redpanda.node_storage(node, sizes=True)
            partitions = storage.partitions('kafka', self.topic)
            assert len(partitions) == 1
            return partitions[0]

        def produce_and_wait_for_compaction(node, count):
            initial_count = len(get_storage_partition(node).segments)

            # count + 1 in case we haven't had a segment open
            produce_until_segments(self.redpanda,
                                   self.topic,
                                   partition_idx=0,
                                   count=initial_count + count + 1,
                                   record_size=1024,
                                   batch_size=2048)

            def no_big_closed_segments():
                partition = get_storage_partition(node)
                return not any(
                    seg.base_index and seg.size > (self.SEGMENT_SIZE / 2)
                    for seg in partition.segments.values())

            wait_until(no_big_closed_segments, timeout_sec=30, backoff_sec=2)

        def get_closed_segment2mtime(node):
            partition = get_storage_partition(node)

            seg2mtime = dict()
            for sname, seg in partition.segments.items():
                if seg.base_index:
                    path = os.path.join(partition.path, seg.data_file)
                    seg2mtime[path] = partition.get_mtime(seg.data_file)

            for k, v in sorted(seg2mtime.items()):
                self.logger.debug(f"mtime for closed segment {k}: {v}")

            return seg2mtime

        # Restart only 1 node so that cluster feature version doesn't increase
        # and we can go back to the previous redpanda version.
        to_restart = self.redpanda.nodes[0]
        self.logger.info(f"will test node {to_restart.account.hostname}")

        produce_and_wait_for_compaction(to_restart, 2)
        seg2mtime_1 = get_closed_segment2mtime(to_restart)
        assert len(seg2mtime_1) >= 2

        self.installer.install([to_restart], next_version)
        self.redpanda.restart_nodes([to_restart])
        self.redpanda.wait_for_membership(first_start=False)

        # After restart we produce and wait for the new segments to be compacted.
        # Because redpanda compacts segments from the beginning, this means that
        # all earlier segments have been either recovered or rebuilt after restart.
        produce_and_wait_for_compaction(to_restart, 2)
        seg2mtime_2 = get_closed_segment2mtime(to_restart)
        assert len(seg2mtime_2) >= len(seg2mtime_1) + 2

        for index in seg2mtime_1.keys():
            # v1 compacted segments should be left intact
            assert index in seg2mtime_2
            assert seg2mtime_1[index] == seg2mtime_2[index]

        self.installer.install([to_restart], self.OLD_VERSION)
        self.redpanda.restart_nodes([to_restart])
        self.redpanda.wait_for_membership(first_start=False)

        produce_and_wait_for_compaction(to_restart, 2)
        seg2mtime_3 = get_closed_segment2mtime(to_restart)
        assert len(seg2mtime_3) >= len(seg2mtime_2) + 2
        for index in seg2mtime_2.keys():
            assert index in seg2mtime_3
            if index in seg2mtime_1:
                # v1-compacted segments should not be rewritten
                assert seg2mtime_3[index] == seg2mtime_2[index]
            else:
                # old version should rebuild v2 indices and re-compact segments
                assert seg2mtime_3[index] > seg2mtime_2[index]

        # Now that we are back at the old version, proceed to upgrade all the way to HEAD
        remaining_versions = self.load_version_range(self.OLD_VERSION)[1:]
        for version in self.upgrade_through_versions(remaining_versions,
                                                     already_running=True):
            self.logger.info(f"Updated to {version}")

        produce_and_wait_for_compaction(self.redpanda.nodes[0], 2)
