# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin

from rptest.tests.end_to_end import EndToEndTest
from rptest.services.cluster import cluster
from rptest.util import segments_count

from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.utils.mode_checks import cleanup_on_early_exit


class CompactionEndToEndTest(EndToEndTest):
    def setUp(self):
        super(CompactionEndToEndTest, self).setUp()

        self.throughput = 100000 if not self.debug_mode else 10000
        self.segment_size = 5 * 1024 * 1024 if not self.debug_mode else 1024 * 1024
        # we use small partition count to make the segment count grow faster
        self.partition_count = 2

        # Can be removed once transactions are enabled by default.
        enable_transactions = {
            "enable_idempotence": True,
            "enable_transactions": True,
            "transaction_coordinator_replication": 3,
            "id_allocator_replication": 3,
            "enable_leader_balancer": False
        }

        # setup redpanda to speed up compaction
        self.start_redpanda(num_nodes=3, extra_rp_conf=enable_transactions)

    def start_workload(self, key_set_cardinality, initial_cleanup_policy,
                       tx_enabled, tx_inject_aborts):
        # create topic with deletion policy
        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=3,
                         segment_bytes=self.segment_size,
                         cleanup_policy=initial_cleanup_policy)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(num_nodes=1,
                            throughput=self.throughput,
                            repeating_keys=key_set_cardinality,
                            transactional=tx_enabled,
                            tx_inject_aborts=tx_inject_aborts)

    def topic_segments(self):
        storage = self.redpanda.node_storage(self.redpanda.nodes[0])
        topic_partitions = storage.partitions("kafka", self.topic)

        return [len(p.segments) for p in topic_partitions]

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(key_set_cardinality=[10],
            initial_cleanup_policy=[
                TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_DELETE
            ],
            transactions=[True, False],
            tx_inject_aborts=[True, False])
    def test_basic_compaction(self, key_set_cardinality,
                              initial_cleanup_policy, transactions,
                              tx_inject_aborts):
        '''
        Basic end to end compaction logic test. The test verifies if last value 
        consumed for each key matches the last produced value for the same key. 
        The test waits for the compaction to merge some adjacent segments.

        The test is parametrized with initial cleanup policy parameter. 
        This way the test validates both recovery and inflight generation 
        of compaction indices.

        Note: This test has many parameter combinations and is prone to long
        running times and/or substantial disk usage. Keep this in mind before
        adding further parameters.
        '''

        if not transactions and tx_inject_aborts:
            # Not a valid combination
            cleanup_on_early_exit(self)
            return

        # skip debug mode tests for keys with high cardinality
        if key_set_cardinality > 10 and self.debug_mode:
            cleanup_on_early_exit(self)
            return

        self.start_workload(key_set_cardinality, initial_cleanup_policy,
                            transactions, tx_inject_aborts)
        rpk = RpkTool(self.redpanda)
        cfgs = rpk.describe_topic_configs(self.topic)
        self.logger.debug(f"Initial topic {self.topic} configuration: {cfgs}")

        # make sure that segments will not be compacted when we populate
        # topic partitions with data
        rpk.cluster_config_set("log_compaction_interval_ms", str(3600 * 1000))

        def segment_number_matches(predicate):
            segments_per_partition = self.topic_segments()
            self.logger.debug(
                f"Topic {self.topic} segments per partition: {segments_per_partition}"
            )

            return all([predicate(n) for n in segments_per_partition])

        # wait for multiple segments to appear in topic partitions
        wait_until(lambda: segment_number_matches(lambda s: s >= 5),
                   timeout_sec=180,
                   backoff_sec=2)

        # enable compaction, if topic isn't compacted
        if initial_cleanup_policy == TopicSpec.CLEANUP_DELETE:
            rpk.alter_topic_config(self.topic,
                                   set_key="cleanup.policy",
                                   set_value=TopicSpec.CLEANUP_COMPACT)
        # stop producer
        self.logger.info("Stopping producer after writing up to offsets %s" %\
                         str(self.producer.last_acked_offsets))
        self.producer.stop()
        current_segments_per_partition = self.topic_segments()
        self.logger.info(
            f"Stopped producer, segments per partition: {current_segments_per_partition}"
        )
        # make compaction frequent
        rpk.cluster_config_set("log_compaction_interval_ms", str(3000))

        # wait for compaction to merge some adjacent segments
        wait_until(lambda: segment_number_matches(lambda s: s < 5),
                   timeout_sec=180,
                   backoff_sec=2)

        # enable consumer and validate consumed records
        self.start_consumer(num_nodes=1, verify_offsets=False)

        self.run_validation(enable_compaction=True)
