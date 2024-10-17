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

from ducktape.cluster.remoteaccount import RemoteCommandError
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.default import DefaultClient
from rptest.services.cluster import cluster
from rptest.utils.mode_checks import skip_debug_mode
from time import sleep
from rptest.services.redpanda import RedpandaService

from rptest.transactions.verifiers.compacted_verifier import CompactedVerifier, Workload
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.services.metrics_check import MetricCheck


class CompactionE2EIdempotencyTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {}

        self.segment_size = 5 * 1024 * 1024
        self.partition_count = 3

        super(CompactionE2EIdempotencyTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    def topic_segments(self) -> list[list[int]]:
        partitions = []
        for node in self.redpanda.nodes:
            storage = self.redpanda.node_storage(node)
            topic_partitions = storage.partitions("kafka", self.topic)
            partitions.append([len(p.segments) for p in topic_partitions])
        return partitions

    @skip_debug_mode
    @cluster(num_nodes=4)
    @matrix(
        initial_cleanup_policy=[
            TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_DELETE
        ],
        workload=[Workload.IDEMPOTENCY, Workload.TX, Workload.TX_UNIQUE_KEYS])
    def test_basic_compaction(self, initial_cleanup_policy, workload):
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

        # tx workloads are more heavy, less parallel and contains aborted
        # cases which don't report progress so choosing a smaller target
        expect_progress = 1000 if workload == Workload.IDEMPOTENCY else 100

        # creating the topic manually instead of relying on topics
        # because the topic depends on the test parameter
        client = DefaultClient(self.redpanda)
        self.topics = [
            TopicSpec(partition_count=self.partition_count,
                      replication_factor=3,
                      segment_bytes=self.segment_size,
                      cleanup_policy=initial_cleanup_policy)
        ]
        client.create_topic(self.topics[0])

        # keep track of vectorized_storage_log_compaction_removed_bytes, check during the test that the value increases
        metric = 'vectorized_storage_log_compaction_removed_bytes_total'

        m = MetricCheck(self.logger,
                        self.redpanda,
                        self.redpanda.partitions(
                            self.topics[0].name)[0].leader, [metric],
                        labels={
                            'namespace': 'kafka',
                            'topic': self.topics[0].name,
                            'partition': '0',
                        },
                        reduce=sum)

        rw_verifier = CompactedVerifier(self.test_context, self.redpanda,
                                        workload)
        rw_verifier.start()

        rw_verifier.remote_start_producer(self.redpanda.brokers(), self.topic,
                                          self.partition_count)
        self.logger.info(
            f"Waiting for {expect_progress} writes to ensure progress")
        rw_verifier.ensure_progress(expect_progress, 30)
        self.logger.info(f"The test made progress")

        rpk = RpkTool(self.redpanda)
        cfgs = rpk.describe_topic_configs(self.topic)
        self.logger.debug(f"Initial topic {self.topic} configuration: {cfgs}")

        # @mmaslankaprv why do we need to
        # make sure that segments will not be compacted when we populate
        # topic partitions with data
        self.logger.info(
            f"setting log_compaction_interval_ms to {3600 * 1000}")
        rpk.cluster_config_set("log_compaction_interval_ms", str(3600 * 1000))

        def segment_number_matches(predicate):
            segments_per_partition_per_node = self.topic_segments()
            self.logger.debug(
                f"Topic {self.topic} {segments_per_partition_per_node=}")

            for node in segments_per_partition_per_node:
                for partition in node:
                    if not predicate(partition):
                        return False
            return True

        timeout_sec = 300
        self.logger.info(
            f"wait for multiple segments to appear in topic partitions")
        # wait for multiple segments to appear in topic partitions
        wait_until(lambda: segment_number_matches(lambda s: s >= 5),
                   timeout_sec=timeout_sec,
                   backoff_sec=2)

        # enable compaction, if topic isn't compacted
        if initial_cleanup_policy == TopicSpec.CLEANUP_DELETE:
            # check: expect that no compaction is reported up until now
            m.expect([(metric, lambda old, new: old == new)])
            rpk.alter_topic_config(self.topic,
                                   set_key="cleanup.policy",
                                   set_value=TopicSpec.CLEANUP_COMPACT)

        self.logger.info(
            f"Waiting for {expect_progress} writes to ensure progress")
        rw_verifier.ensure_progress(expect_progress, 30)
        self.logger.info(f"The test made progress, stopping producer")
        rw_verifier.remote_stop_producer()
        rw_verifier.remote_wait_producer()
        self.logger.info(f"Producer is stopped")

        current_segments_per_partition = self.topic_segments()
        self.logger.info(
            f"Stopped producer, segments per partition: {current_segments_per_partition}"
        )
        # make compaction frequent
        self.logger.info(f"setting log_compaction_interval_ms to {3600}")
        rpk.cluster_config_set("log_compaction_interval_ms", str(3000))
        self.logger.info(f"waiting for compaction to happen")

        # it looks like we're guessing that the number of compacted
        # segments is less than 5, a place or a potential timeout when
        # we guessed wrong
        wait_until(lambda: segment_number_matches(lambda s: s < 5),
                   timeout_sec=timeout_sec,
                   backoff_sec=2)

        self.logger.info(f"enable consumer and validate consumed records")
        rw_verifier.remote_start_consumer()
        rw_verifier.remote_wait_consumer()
        rw_verifier.stop()
        rw_verifier.wait(timeout_sec=10)

        # expect some level of compaction
        m.expect([(metric, lambda old, new: old < new)])


class CompactionWithRecoveryTest(RedpandaTest, PartitionMovementMixin):
    def __init__(self, test_context):

        self.segment_size = 1 * 1024 * 1024
        self.num_moves = 50
        # keep read size low to ensure reads fall within a transaction
        extra_rp_conf = {
            "raft_recovery_default_read_size": 1024,
            "log_compaction_interval_ms": 2000
        }
        super(CompactionWithRecoveryTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    @skip_debug_mode
    @cluster(num_nodes=4)
    def test_tx_compaction_with_recovery(self):
        """Ensures correctness of tx + compaction with partition moves"""

        client = DefaultClient(self.redpanda)
        topic = TopicSpec(partition_count=1,
                          replication_factor=1,
                          segment_bytes=self.segment_size,
                          cleanup_policy=TopicSpec.CLEANUP_COMPACT)

        self.topics = [topic]
        client.create_topic(topic)

        metric = 'vectorized_storage_log_compacted_segment_total'
        m = MetricCheck(self.logger,
                        self.redpanda,
                        self.redpanda.partitions(self.topic)[0].leader,
                        [metric],
                        labels={
                            'namespace': 'kafka',
                            'topic': self.topic,
                            'partition': '0',
                        },
                        reduce=sum)

        workload = CompactedVerifier(self.test_context, self.redpanda,
                                     Workload.TX)
        workload.start()

        workload.remote_start_producer(self.redpanda.brokers(),
                                       self.topic,
                                       1,
                                       abort_probability=1)

        # Wait for first round of compaction to kick in
        wait_until(
            lambda: m.evaluate([(metric, lambda _, metric: metric > 0)]),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Timeed out waiting for first round of compaction to finish"
        )

        moves_done = 0
        while moves_done < self.num_moves:
            self._do_move_and_verify(topic=topic.name,
                                     partition=0,
                                     timeout_sec=300)
            moves_done += 1
            sleep(1)

        workload.remote_stop_producer()
        workload.remote_wait_producer()
        workload.remote_start_consumer()
        workload.remote_wait_consumer(timeout_sec=60)


class CompactionE2ERebootTest(RedpandaTest):
    def __init__(self, test_context):
        self.segment_size = 5 * 1024 * 1024
        super(CompactionE2ERebootTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf={},
                             log_level="trace")

    @skip_debug_mode
    @cluster(num_nodes=4)
    def test_write_reboot_read(self):
        initial_cleanup_policy = TopicSpec.CLEANUP_COMPACT
        workload = Workload.TX_UNIQUE_KEYS

        # tx workloads are more heavy, less parallel and contains aborted
        # cases which don't report progress so choosing a smaller target
        expect_progress = 1000 if workload == Workload.IDEMPOTENCY else 100

        # creating the topic manually instead of relying on topics
        # because the topic depends on the test parameter
        client = DefaultClient(self.redpanda)
        self.topics = [
            TopicSpec(partition_count=1,
                      replication_factor=3,
                      segment_bytes=self.segment_size,
                      cleanup_policy=initial_cleanup_policy)
        ]
        client.create_topic(self.topics[0])

        rw_verifier = CompactedVerifier(self.test_context, self.redpanda,
                                        workload)
        rw_verifier.start()

        rw_verifier.remote_start_producer(self.redpanda.brokers(), self.topic,
                                          1)
        self.logger.info(
            f"Waiting for {expect_progress} writes to ensure progress")
        rw_verifier.ensure_progress(expect_progress, 30)
        self.logger.info(f"The test made progress, stopping producer")
        rw_verifier.remote_stop_producer()
        rw_verifier.remote_wait_producer()
        self.logger.info(f"Producer is stopped")

        self.logger.info(f"Rebooting redpanda cluster")
        assert len(self.redpanda.started_nodes(
        )) == 3, f"only {len(self.redpanda.started_nodes())} nodes are running"
        nodes = []
        for node in list(self.redpanda.started_nodes()):
            nodes.append(node)
            self.logger.info(f"Stoping {node.account.hostname}")
            self.redpanda.stop_node(node)
        for node in nodes:
            self.logger.info(f"Starting {node.account.hostname}")
            self.redpanda.start_node(node)

        def compaction_is_triggered(node):
            try:
                for line in node.account.ssh_capture(
                        f"grep 'rebuilt index:' {RedpandaService.STDOUT_STDERR_CAPTURE}",
                        timeout_sec=5):
                    self.logger.info(
                        f"Compaction was triggered on {node.account.hostname}: {line}"
                    )
                    return True
            except RemoteCommandError:
                pass
            return False

        self.logger.info(f"Waiting until compaction is triggered")
        for node in list(self.redpanda.started_nodes()):
            wait_until(
                lambda: compaction_is_triggered(node),
                timeout_sec=60,
                backoff_sec=2,
                err_msg=
                f"Compaction wasn't triggered on {node.account.hostname} in 60s"
            )

        self.logger.info(f"Start consumer and validate consumed records")
        rw_verifier.remote_start_consumer()
        rw_verifier.remote_wait_consumer()
        rw_verifier.stop()
        rw_verifier.wait(timeout_sec=10)
