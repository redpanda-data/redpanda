# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.admin import Admin


class PrefixTruncateRecoveryTest(RedpandaTest):
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

        super(PrefixTruncateRecoveryTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.kafka_cat = KafkaCat(self.redpanda)

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

    def produce_until_reclaim(self, initial_deleted, acks):
        """
        Produce data until we observe that segments have been deleted. The
        initial_deleted parameter is the max number of segments deleted across
        nodes, and we wait for all partitions to report at least initial + 3
        deletions so that all nodes have experienced some deletion.
        """
        deleted = self.get_segments_deleted(self.redpanda.nodes[1:])
        if all(map(lambda d: d >= initial_deleted + 2, deleted)):
            return True
        self.kafka_tools.produce(self.topic, 1024, 1024, acks=acks)
        return False

    @cluster(num_nodes=3)
    @matrix(acks=[-1, 1], start_empty=[True, False])
    def test_prefix_truncate_recovery(self, acks, start_empty):
        # cover boundary conditions of partition being empty/non-empty
        if not start_empty:
            self.kafka_tools.produce(self.topic, 2048, 1024, acks=acks)
            wait_until(lambda: self.fully_replicated(self.redpanda.nodes),
                       timeout_sec=90,
                       backoff_sec=5)

        # stop this unfortunate node
        stopped_node = self.redpanda.nodes[0]
        self.redpanda.stop_node(stopped_node)

        # produce data into the topic until segments are reclaimed
        # by the configured retention policy
        deleted = max(self.get_segments_deleted(self.redpanda.nodes[1:]))
        wait_until(lambda: self.produce_until_reclaim(deleted, acks),
                   timeout_sec=90,
                   backoff_sec=5)

        # we should now observe an under replicated state
        wait_until(lambda: not self.fully_replicated(self.redpanda.nodes[1:]),
                   timeout_sec=90,
                   backoff_sec=5)

        # finally restart the node and wait until fully replicated
        self.redpanda.start_node(stopped_node)
        wait_until(lambda: self.fully_replicated(self.redpanda.nodes),
                   timeout_sec=90,
                   backoff_sec=5)

        self.verify_offsets()

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
                                         target=node)
            # % ERROR: offsets_for_times failed: Local: Unknown partition
            # may occur here presumably because there is an interaction
            # with leadership transfer. the built-in retries in list_offsets
            # appear to deal with this gracefully and we still pass.
            offsets.append(self.kafka_cat.list_offsets(self.topic, 0))
        assert all(map(lambda o: o[1] == offsets[0][1], offsets))
