# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import time
import threading

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierConsumerGroupConsumer, KgoVerifierSeqConsumer
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.tests.prealloc_nodes import PreallocNodesTest


class LogCompactionTest(PreallocNodesTest, PartitionMovementMixin):
    def __init__(self, test_context):
        self.test_context = test_context
        # Run with small segments, a low retention value and a very frequent compaction interval.
        self.extra_rp_conf = {
            'log_compaction_interval_ms': 4000,
            'log_segment_size': 2 * 1024**2,  # 2 MiB
            'retention_bytes': 25 * 1024**2,  # 25 MiB
            'compacted_log_segment_size': 1024**2  # 1 MiB
        }
        super().__init__(test_context=test_context,
                         num_brokers=3,
                         node_prealloc_count=1,
                         extra_rp_conf=self.extra_rp_conf)

    def topic_setup(self, cleanup_policy, key_set_cardinality):
        """
        Sets variables and creates topic.
        """
        self.msg_size = 1024  # 1 KiB
        self.rate_limit = 50 * 1024**2  # 50 MiBps
        self.total_data = 100 * 1024**2  # 100 MiB
        self.msg_count = int(self.total_data / self.msg_size)
        self.tombstone_probability = 0.4
        self.partition_count = 10
        self.cleanup_policy = cleanup_policy
        self.key_set_cardinality = key_set_cardinality

        # A value below log_compaction_interval_ms (therefore, tombstones that would be compacted away during deduplication will be visibly removed instead)
        self.delete_retention_ms = 3000
        self.topic_spec = TopicSpec(
            name="tapioca",
            delete_retention_ms=self.delete_retention_ms,
            partition_count=self.partition_count,
            cleanup_policy=self.cleanup_policy)
        self.client().create_topic(self.topic_spec)

    def get_removed_tombstones(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_tombstones_removed_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name)

    def get_cleanly_compacted_segments(self):
        return self.redpanda.metric_sum(
            metric_name=
            "vectorized_storage_log_cleanly_compacted_segment_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name)

    def get_segments_marked_tombstone_free(self):
        return self.redpanda.metric_sum(
            metric_name=
            "vectorized_storage_log_segments_marked_tombstone_free_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name)

    def get_complete_sliding_window_rounds(self):
        return self.redpanda.metric_sum(
            metric_name=
            "vectorized_storage_log_complete_sliding_window_rounds_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name)

    def produce_and_consume(self):
        """
        Creates producer and consumer. Asserts that tombstones are seen
        in the consumer log.
        """

        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.topic_spec.name,
            debug_logs=True,
            trace_logs=True,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            rate_limit_bps=self.rate_limit,
            key_set_cardinality=self.key_set_cardinality,
            tolerate_data_loss=False,
            tombstone_probability=self.tombstone_probability,
            validate_latest_values=True,
            custom_node=self.preallocated_nodes)

        # Produce and wait
        producer.start()
        producer.wait_for_latest_value_map()
        producer.wait(timeout_sec=180)

        assert producer.produce_status.tombstones_produced > 0
        assert producer.produce_status.bad_offsets == 0

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic_spec.name,
                                          self.msg_size,
                                          debug_logs=True,
                                          trace_logs=True,
                                          compacted=True,
                                          loop=False,
                                          nodes=self.preallocated_nodes)

        # Consume and wait. clean=False to not accidentally remove latest value map.
        consumer.start(clean=False)
        consumer.wait(timeout_sec=180)

        # Clean up
        producer.stop()
        consumer.stop()

        assert consumer.consumer_status.validator.tombstones_consumed > 0
        assert consumer.consumer_status.validator.invalid_reads == 0

    def validate_log(self):
        """
        After several rounds of compaction, restart the brokers,
        create a consumer, and assert that no tombstones are consumed.
        Latest key-value pairs in the log are verified in KgoVerifier.
        """

        # Restart each redpanda broker to force roll segments
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Sleep until the log has been fully compacted.
        self.prev_sliding_window_rounds = -1
        self.prev_tombstones_removed = -1

        def compaction_has_completed():
            # In order to be confident that compaction has settled,
            # we check that the number of compaction rounds that
            # have occured as well as the number of tombstones records
            # removed have stabilized over some period longer than
            # log_compaction_interval_ms (and expected time for compaction to complete).
            new_sliding_window_rounds = self.get_complete_sliding_window_rounds(
            )
            new_tombstones_removed = self.get_removed_tombstones()
            res = self.prev_sliding_window_rounds == new_sliding_window_rounds and self.prev_tombstones_removed == new_tombstones_removed
            self.prev_sliding_window_rounds = new_sliding_window_rounds
            self.prev_tombstones_removed = new_tombstones_removed
            return res

        wait_until(
            compaction_has_completed,
            timeout_sec=120,
            backoff_sec=self.extra_rp_conf['log_compaction_interval_ms'] /
            1000 * 4,
            err_msg="Compaction did not stabilize.")

        assert self.get_complete_sliding_window_rounds() > 0
        assert self.get_cleanly_compacted_segments() > 0
        assert self.get_segments_marked_tombstone_free() > 0

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic_spec.name,
                                          self.msg_size,
                                          debug_logs=True,
                                          trace_logs=True,
                                          compacted=True,
                                          loop=False,
                                          validate_latest_values=True,
                                          nodes=self.preallocated_nodes)

        # Consume and wait. clean=False to not accidentally remove latest value map.
        consumer.start(clean=False)
        consumer.wait(timeout_sec=180)

        consumer.stop()

        # Expect to see 0 tombstones consumed
        assert consumer.consumer_status.validator.tombstones_consumed == 0
        assert consumer.consumer_status.validator.invalid_reads == 0

    @skip_debug_mode
    @cluster(num_nodes=4)
    @matrix(
        cleanup_policy=[
            TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_COMPACT_DELETE
        ],
        key_set_cardinality=[100, 1000],
    )
    def compaction_stress_test(self, cleanup_policy, key_set_cardinality):
        """
        Uses partition movement and frequent compaction/garbage collecting to
        validate tombstone removal and general compaction behavior.
        """
        self.topic_setup(cleanup_policy, key_set_cardinality)

        class PartitionMoveExceptionReporter:
            exc = None

        def background_test_loop(reporter,
                                 fn,
                                 iterations=10,
                                 sleep_sec=1,
                                 allowable_retries=3):
            try:
                while iterations > 0:
                    try:
                        fn()
                    except Exception as e:
                        if allowable_retries == 0:
                            raise e
                    time.sleep(sleep_sec)
                    iterations -= 1
                    allowable_retries -= 1
            except Exception as e:
                reporter.exc = e

        def issue_partition_move():
            try:
                self._dispatch_random_partition_move(self.topic_spec.name, 0)
                self._wait_for_move_in_progress(self.topic_spec.name,
                                                0,
                                                timeout=5)
            except Exception as e:
                reporter.exc = e

        partition_move_thread = threading.Thread(
            target=background_test_loop,
            args=(PartitionMoveExceptionReporter, issue_partition_move),
            kwargs={
                'iterations': 5,
                'sleep_sec': 1
            })

        # Start partition movement thread
        partition_move_thread.start()

        self.produce_and_consume()

        self.validate_log()

        # Clean up partition movement thread
        partition_move_thread.join()

        if PartitionMoveExceptionReporter.exc is not None:
            raise PartitionMoveExceptionReporter.exc
