# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools


class CompactionTermRollRecoveryTest(RedpandaTest):
    topics = (TopicSpec(cleanup_policy=TopicSpec.CLEANUP_COMPACT,
                        partition_count=1), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=5000,
            compacted_log_segment_size=1048576,
        )

        super(CompactionTermRollRecoveryTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_compact_term_rolled_recovery(self):
        """
        Tests recovery of a partition replica when the data being recovered are
        coming from compacted segments. The particular bug that prompted this
        test to be created was related to ghost batches which use a fixed 0
        term, but were incorrectly being used to set the term rather than simply
        representing gaps in the log offsets.

        TODO: this test should be generalized so we can test this basic recovery
        scenario with various topic configurations.
        """
        # Operate on the topics single partition. This is relevant
        # because we use a metric that is aggregated by partition
        # to retrieve the number of compacted segments, which means
        # that it cannot differentiate between multiple partitions.
        partition = self.redpanda.partitions(self.topic)[0]

        # stop a replica in order to test its recovery
        all_replicas = list(partition.replicas)
        needs_recovery, *others = all_replicas
        self.redpanda.stop_node(needs_recovery)

        # produce until segments have been compacted
        self._produce_until_compaction(others, self.topic)

        # restart all replicas: rolls term, starts recovery
        self.redpanda.restart_nodes(all_replicas)

        # ensure that the first stopped node recovered ok
        self._wait_until_recovered(all_replicas, self.topic, partition.index)

    def _produce_until_compaction(self, nodes, topic):
        """
        Produce into the topic until some new segments have been compacted.
        """
        num_segs = 3

        target = list(
            map(lambda cnt: cnt + num_segs,
                self._compacted_segments(nodes, topic)))

        kafka_tools = KafkaCliTools(self.redpanda)

        def done():
            kafka_tools.produce(self.topic, 1024, 1024)
            curr = self._compacted_segments(nodes, topic)
            return all(map(lambda cnt: cnt[0] > cnt[1], zip(curr, target)))

        wait_until(done,
                   timeout_sec=60,
                   backoff_sec=2,
                   err_msg="Compacted segments were not created")

    def _compacted_segments(self, nodes, topic):
        """
        Fetch the number of compacted segments.

        TODO: we may want to consider starting up prometheus on a ducktape node
        so that we can use a query language to lookup metrics instead of parsing
        exported node metrics in their raw form.
        """
        def fetch(node):
            count = 0
            metrics = self.redpanda.metrics(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_storage_log_compacted_segment_total" and \
                            sample.labels["namespace"] == "kafka" and \
                            sample.labels["topic"] == topic:
                        count += int(sample.value)
            self.logger.debug(count)
            return count

        return map(fetch, nodes)

    def _wait_until_recovered(self, nodes, topic, partition):
        """
        Wait until all nodes report the same LSO > 0
        """
        def fetch_lso(node):
            last_stable_offset = None
            try:
                metrics = self.redpanda.metrics(node)
                for family in metrics:
                    for sample in family.samples:
                        if sample.name == "vectorized_cluster_partition_last_stable_offset" and \
                                sample.labels["namespace"] == "kafka" and \
                                sample.labels["topic"] == topic and \
                                int(sample.labels["partition"]) == partition:
                            last_stable_offset = int(sample.value)
            except Exception as e:
                self.logger.debug(e)
            finally:
                return last_stable_offset

        def identical_lso():
            offsets = list(map(fetch_lso, nodes))
            self.logger.debug(f"Found replica LSOs {offsets}")
            return all(map(lambda o: o and o > 0 and o == offsets[0], offsets))

        wait_until(identical_lso,
                   timeout_sec=60,
                   backoff_sec=1,
                   err_msg="Replicas did not converge to the same LSO")
