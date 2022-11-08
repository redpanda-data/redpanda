# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.upgrade_with_workload import MixedVersionWorkloadRunner
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.admin import Admin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST

LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [
    # raft - [follower: {id: {1}, revision: {9}}] [group_id:1, {kafka/topic-xyeyqcbyxi/0}] - recovery_stm.cc:422 - recovery append entries error: rpc::errc::exponential_backoff
    "raft - .*recovery append entries error"
]


class PrefixTruncateRecoveryTestBase(RedpandaTest):
    """
    The purpose of this test is to exercise recovery of partitions which have
    had data reclaimed based on retention policy. The testing strategy is:

       1. Stop 1 out 3 nodes
       2. Produce until retention policy reclaims data
       3. Restart the stopped node
       4. Verify that the stopped node recovers

    Leadership balancing is disabled in this test because the final verification
    step tries to force leadership so that verification may query metadata from
    specific nodes where the kafka protocol only returns state from leaders.
    """
    topics = (TopicSpec(cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_segment_size=1048576,
            retention_bytes=3145728,
            log_compaction_interval_ms=1000,
            enable_leader_balancer=False,
        )

        super(PrefixTruncateRecoveryTestBase,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.kafka_cat = KafkaCat(self.redpanda)

    def setUp(self):
        super(PrefixTruncateRecoveryTestBase, self).setUp()

    def fully_replicated(self, nodes):
        """
        Test that for each specified node that there are no reported under
        replicated partitions corresponding to the test topic.
        """
        metric = self.redpanda.metrics_sample("under_replicated_replicas",
                                              nodes)
        metric = metric.label_filter(dict(namespace="kafka", topic=self.topic))
        assert len(metric.samples) == len(nodes)
        return all(map(lambda s: s.value == 0, metric.samples))

    def get_segments_deleted(self, nodes):
        """
        Return the values of the log segments removed metric.
        """
        metric = self.redpanda.metrics_sample("log_segments_removed", nodes)
        metric = metric.label_filter(dict(namespace="kafka", topic=self.topic))
        assert len(metric.samples) == len(nodes)
        return [s.value for s in metric.samples]

    def produce_until_reclaim(self, living_nodes, initial_deleted, acks):
        """
        Produce data until we observe that segments have been deleted. The
        initial_deleted parameter is the max number of segments deleted across
        nodes, and we wait for all partitions to report at least initial + 3
        deletions so that all nodes have experienced some deletion.
        """
        deleted = self.get_segments_deleted(living_nodes)
        if all(map(lambda d: d >= initial_deleted + 2, deleted)):
            return True
        self.kafka_tools.produce(self.topic, 1024, 1024, acks=acks)
        return False

    def verify_offsets(self):
        """
        Test that the ending offset for the partition as seen on each
        node are identical. Since we can only query this from the leader, we
        disable auto leadership balancing, and manually transfer leadership
        before querying.

        Note that because each node applies retention policy independently to a
        prefix of the log we can't reliably compare the starting offsets.
        """
        admin = Admin(self.redpanda)
        offsets = []
        for node in self.redpanda.nodes:
            admin.transfer_leadership_to(namespace="kafka",
                                         topic=self.topic,
                                         partition=0,
                                         target_id=self.redpanda.idx(node))
            # % ERROR: offsets_for_times failed: Local: Unknown partition
            # may occur here presumably because there is an interaction
            # with leadership transfer. the built-in retries in list_offsets
            # appear to deal with this gracefully and we still pass.
            offsets.append(self.kafka_cat.list_offsets(self.topic, 0))
        assert all(map(lambda o: o[1] == offsets[0][1], offsets))

    def run_recovery(self, acks, dst_node):
        """
        Runs the workload and recovery such that 'dst_node' is the receiver of
        the recovery snapshot.
        """
        # Stop this unfortunate node.
        self.redpanda.stop_node(dst_node)
        living_nodes = [n for n in self.redpanda.nodes if n != dst_node]

        # Produce until the topic reclaims some segments.
        deleted = max(self.get_segments_deleted(living_nodes))
        wait_until(
            lambda: self.produce_until_reclaim(living_nodes, deleted, acks),
            timeout_sec=90,
            backoff_sec=1)

        # We should now observe the under-replicated state.
        wait_until(lambda: not self.fully_replicated(living_nodes),
                   timeout_sec=90,
                   backoff_sec=1)

        # Continue the node and wait until it's fully replicated.
        self.redpanda.start_node(dst_node)
        wait_until(lambda: self.fully_replicated(self.redpanda.nodes),
                   timeout_sec=90,
                   backoff_sec=1)
        self.verify_offsets()


class PrefixTruncateRecoveryTest(PrefixTruncateRecoveryTestBase):
    @cluster(num_nodes=3, log_allow_list=LOG_ALLOW_LIST)
    @matrix(acks=[-1, 1], start_empty=[True, False])
    def test_prefix_truncate_recovery(self, acks, start_empty):
        # cover boundary conditions of partition being empty/non-empty
        if not start_empty:
            self.kafka_tools.produce(self.topic, 2048, 1024, acks=acks)
            wait_until(lambda: self.fully_replicated(self.redpanda.nodes),
                       timeout_sec=90,
                       backoff_sec=5)
        self.run_recovery(acks=acks, dst_node=self.redpanda.nodes[0])


class PrefixTruncateRecoveryUpgradeTest(PrefixTruncateRecoveryTestBase):
    """
    Exercises the work required to install a snapshot works through the stages
    of an upgrade.
    """
    def __init__(self, test_context):
        super(PrefixTruncateRecoveryUpgradeTest, self).__init__(test_context)
        self.initial_version = MixedVersionWorkloadRunner.PRE_SERDE_VERSION

    def setUp(self):
        self.redpanda._installer.install(self.redpanda.nodes,
                                         self.initial_version)
        super(PrefixTruncateRecoveryUpgradeTest, self).setUp()

    @cluster(num_nodes=3,
             log_allow_list=LOG_ALLOW_LIST +
             MixedVersionWorkloadRunner.ALLOWED_LOGS)
    def test_recover_during_upgrade(self):
        def _run_recovery(src_node, dst_node):
            # Force leadership to 'src_node' so we can deterministically check
            # mixed-version traffic.
            # Note it's possible to see surprising error codes on older
            # versions following a node restart (e.g. 500 insteaad of 503/504);
            # just retry until leadership is moved.
            wait_until(lambda: self.redpanda._admin.transfer_leadership_to(
                namespace="kafka",
                topic=self.topic,
                partition=0,
                target_id=self.redpanda.idx(src_node)),
                       timeout_sec=30,
                       backoff_sec=2,
                       retry_on_exc=True)
            # Speed the test up a bit by using acks=1.
            self.run_recovery(acks=1, dst_node=dst_node)

        MixedVersionWorkloadRunner.upgrade_with_workload(
            self.redpanda, self.initial_version, _run_recovery)
