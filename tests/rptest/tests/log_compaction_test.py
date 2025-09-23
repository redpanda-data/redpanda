# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import threading
import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode


class LogCompactionTestBase:
    def topic_setup(
        self,
        cleanup_policy,
        replication_factor,
        key_set_cardinality,
        partition_count=10,
        tombstone_probability=0.4,
        min_cleanable_dirty_ratio=0.0,
    ):
        """
        Sets variables and creates topic.
        """

        self.msg_size = 1024  # 1 KiB
        self.rate_limit = 50 * 1024**2  # 50 MiBps
        self.total_data = 100 * 1024**2  # 100 MiB
        self.msg_count = int(self.total_data / self.msg_size)
        self.cleanup_policy = cleanup_policy
        self.replication_factor = replication_factor
        self.key_set_cardinality = key_set_cardinality
        self.partition_count = partition_count
        self.tombstone_probability = tombstone_probability
        self.min_cleanable_dirty_ratio = min_cleanable_dirty_ratio

        # A value below log_compaction_interval_ms (therefore, tombstones that would be compacted away during deduplication will be visibly removed instead)
        self.delete_retention_ms = 3000
        self.topic_spec = TopicSpec(
            delete_retention_ms=self.delete_retention_ms,
            replication_factor=self.replication_factor,
            partition_count=self.partition_count,
            cleanup_policy=self.cleanup_policy,
            min_cleanable_dirty_ratio=self.min_cleanable_dirty_ratio,
        )
        self.client().create_topic(self.topic_spec)

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
            custom_node=self.preallocated_nodes,
        )

        # Produce and wait
        producer.start()

        def seen_dirty_ratio_above_zero():
            return self.get_dirty_ratio() > 0.0

        wait_until(
            seen_dirty_ratio_above_zero,
            timeout_sec=10,
            backoff_sec=0.001,
            err_msg="Did not see a non-zero dirty ratio.",
        )

        producer.wait_for_latest_value_map()
        producer.wait(timeout_sec=180)

        assert producer.produce_status.tombstones_produced > 0
        assert producer.produce_status.bad_offsets == 0

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_spec.name,
            self.msg_size,
            debug_logs=True,
            trace_logs=True,
            compacted=True,
            loop=False,
            nodes=self.preallocated_nodes,
        )

        # Consume and wait. clean=False to not accidentally remove latest value map.
        consumer.start(clean=False)
        consumer.wait(timeout_sec=180)

        # Clean up
        producer.stop()
        consumer.stop()

        assert consumer.consumer_status.validator.tombstones_consumed > 0
        assert consumer.consumer_status.validator.invalid_reads == 0

    def get_removed_tombstones(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_tombstones_removed_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )

    def get_cleanly_compacted_segments(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_cleanly_compacted_segment_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )

    def get_segments_marked_tombstone_free(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_segments_marked_tombstone_free_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )

    def get_complete_sliding_window_rounds(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_complete_sliding_window_rounds_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )

    def get_chunked_compaction_runs(self):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_chunked_compaction_runs_total",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
        )

    def get_dirty_segment_bytes(self, nodes=None):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_dirty_segment_bytes",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
            nodes=nodes,
        )

    def get_closed_segment_bytes(self, nodes=None):
        return self.redpanda.metric_sum(
            metric_name="vectorized_storage_log_closed_segment_bytes",
            metrics_endpoint=MetricsEndpoint.METRICS,
            topic=self.topic_spec.name,
            nodes=nodes,
        )

    def get_dirty_ratio(self, nodes=None):
        dirty_segment_bytes = self.get_dirty_segment_bytes(nodes=nodes)
        closed_segment_bytes = self.get_closed_segment_bytes(nodes=nodes)
        return (
            0.0
            if closed_segment_bytes == 0
            else float(dirty_segment_bytes) / float(closed_segment_bytes)
        )


class LogCompactionTest(
    LogCompactionTestBase, PreallocNodesTest, PartitionMovementMixin
):
    def __init__(self, test_context):
        self.test_context = test_context
        # Run with small segments, a low retention value and a very frequent compaction interval.
        key_map_memory_kb = self.test_context.injected_args[
            "storage_compaction_key_map_memory_kb"
        ]
        key_set_cardinality = self.test_context.injected_args["key_set_cardinality"]
        self.extra_rp_conf = {
            "log_compaction_interval_ms": 4000,
            "log_segment_size": 2 * 1024**2,  # 2 MiB
            "retention_bytes": 25 * 1024**2,  # 25 MiB
            "compacted_log_segment_size": 1024**2,  # 1 MiB
            "storage_compaction_key_map_memory": key_map_memory_kb * 1024,
            "min_cleanable_dirty_ratio": 0.0,
        }

        # This environment variable is required to get around the map memory bounds
        # of > 16MiB.
        environment = {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}

        # Assume that all of the key set will comfortably fit in one segment.
        # If test parameters are changed, this may have to be re-estimated.
        keys_per_segment = key_set_cardinality

        # hash_key_offset_map::entry is exactly 40 bytes-
        # a 32 byte digest, and an 8 byte offset.
        # See key_offset_map.h.
        entry_size = 40
        indexed_key_estimation = key_map_memory_kb * 1024 // entry_size
        self.needs_chunked_compaction = indexed_key_estimation < keys_per_segment
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            node_prealloc_count=1,
            extra_rp_conf=self.extra_rp_conf,
            environment=environment,
        )

    def validate_log(self, cleanup_policy):
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
        self.prev_chunked_compaction_runs = -1

        def compaction_has_completed():
            # In order to be confident that compaction has settled,
            # we check that the number of compaction rounds that
            # have occured as well as the number of tombstones records
            # removed have stabilized over some period longer than
            # log_compaction_interval_ms (and expected time for compaction to complete).
            new_sliding_window_rounds = self.get_complete_sliding_window_rounds()
            new_tombstones_removed = self.get_removed_tombstones()
            new_chunked_compaction_runs = self.get_chunked_compaction_runs()
            res = (
                self.prev_sliding_window_rounds == new_sliding_window_rounds
                and self.prev_tombstones_removed == new_tombstones_removed
                and self.prev_chunked_compaction_runs == new_chunked_compaction_runs
            )
            self.prev_sliding_window_rounds = new_sliding_window_rounds
            self.prev_tombstones_removed = new_tombstones_removed
            self.prev_chunked_compaction_runs = new_chunked_compaction_runs
            return res

        wait_until(
            compaction_has_completed,
            timeout_sec=120,
            backoff_sec=self.extra_rp_conf["log_compaction_interval_ms"] / 1000 * 4,
            err_msg="Compaction did not stabilize.",
        )

        assert self.get_complete_sliding_window_rounds() > 0
        assert self.get_cleanly_compacted_segments() > 0
        assert self.get_segments_marked_tombstone_free() > 0

        if self.needs_chunked_compaction:
            assert self.get_chunked_compaction_runs() > 0
        else:
            assert self.get_chunked_compaction_runs() == 0

        def log_is_fully_clean():
            # There should be no dirty segments left
            return self.get_dirty_segment_bytes() == 0

        wait_until(
            log_is_fully_clean,
            timeout_sec=120,
            backoff_sec=self.extra_rp_conf["log_compaction_interval_ms"] / 1000,
            err_msg="Did not see a fully clean log.",
        )

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_spec.name,
            self.msg_size,
            debug_logs=True,
            trace_logs=True,
            compacted=True,
            loop=False,
            validate_latest_values=True,
            nodes=self.preallocated_nodes,
        )

        # Consume and wait. clean=False to not accidentally remove latest value map.
        consumer.start(clean=False)
        consumer.wait(timeout_sec=180)

        consumer.stop()

        # Expect to see 0 tombstones consumed
        assert consumer.consumer_status.validator.tombstones_consumed == 0
        assert consumer.consumer_status.validator.invalid_reads == 0

    def wait_for_log_truncation(self):
        # Set log_retention_ms to an arbitrarily tiny value and wait for log truncation.
        # This is done by watching the number of bytes in closed segments,
        # which will decrease as segments are removed.
        self.client().alter_topic_config(
            self.topic_spec.name, TopicSpec.PROPERTY_RETENTION_TIME, 1000
        )

        def all_segments_removed():
            closed_segment_bytes = self.get_closed_segment_bytes()
            dirty_segment_bytes = self.get_dirty_segment_bytes()

            return dirty_segment_bytes == 0 and closed_segment_bytes == 0

        wait_until(
            all_segments_removed,
            timeout_sec=120,
            backoff_sec=1,
            err_msg="Closed segment bytes did not reach zero.",
        )

    @skip_debug_mode
    @cluster(num_nodes=4)
    @matrix(
        cleanup_policy=[TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_COMPACT_DELETE],
        key_set_cardinality=[100, 1000],
        storage_compaction_key_map_memory_kb=[3, 10, 128 * 1024],
    )
    def compaction_stress_test(
        self, cleanup_policy, key_set_cardinality, storage_compaction_key_map_memory_kb
    ):
        """
        Uses partition movement and frequent compaction/garbage collecting to
        validate tombstone removal and general compaction behavior.
        """
        self.topic_setup(
            cleanup_policy=cleanup_policy,
            replication_factor=3,
            key_set_cardinality=key_set_cardinality,
        )

        class PartitionMoveExceptionReporter:
            exc = None

        def background_test_loop(
            reporter, fn, iterations=10, sleep_sec=1, allowable_retries=3
        ):
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
            self._dispatch_random_partition_move(self.topic_spec.name, 0)
            self._wait_for_move_in_progress(self.topic_spec.name, 0, timeout=5)

        partition_move_thread = threading.Thread(
            target=background_test_loop,
            args=(PartitionMoveExceptionReporter, issue_partition_move),
            kwargs={"iterations": 5, "sleep_sec": 1},
        )

        # Start partition movement thread
        partition_move_thread.start()

        self.produce_and_consume()

        self.validate_log(cleanup_policy)

        if cleanup_policy == TopicSpec.CLEANUP_COMPACT_DELETE:
            self.wait_for_log_truncation()

        # Clean up partition movement thread
        partition_move_thread.join()

        if PartitionMoveExceptionReporter.exc is not None:
            raise PartitionMoveExceptionReporter.exc


class LogCompactionSchedulingTest(LogCompactionTestBase, PreallocNodesTest):
    def __init__(self, test_context):
        self.test_context = test_context
        # Run with small segments and a very frequent compaction interval.
        self.extra_rp_conf = {
            "log_compaction_interval_ms": 4000,
            "log_segment_size": 2 * 1024**2,  # 2 MiB
            "compacted_log_segment_size": 1024**2,  # 1 MiB
        }

        super().__init__(
            test_context=test_context,
            num_brokers=3,
            node_prealloc_count=1,
            extra_rp_conf=self.extra_rp_conf,
        )

        self._rpk_client = RpkTool(self.redpanda)

    def set_min_cleanable_dirty_ratio(self, dirty_ratio):
        self.min_cleanable_dirty_ratio = dirty_ratio
        self._rpk_client.alter_topic_config(
            self.topic_spec.name,
            TopicSpec.PROPERTY_MIN_CLEANABLE_DIRTY_RATIO,
            dirty_ratio,
        )

    def consume_and_validate_log(self):
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_spec.name,
            self.msg_size,
            debug_logs=True,
            trace_logs=True,
            compacted=True,
            loop=False,
            validate_latest_values=True,
            nodes=self.preallocated_nodes,
        )

        # Consume and wait. clean=False to not accidentally remove latest value map.
        consumer.start(clean=False)
        consumer.wait(timeout_sec=180)

        consumer.stop()

    @skip_debug_mode
    @cluster(num_nodes=4)
    @matrix(key_set_cardinality=[100, 1000])
    def dirty_ratio_scheduling_test(self, key_set_cardinality):
        """
        Tests that the dirty ratio of a log controls scheduling of compaction rounds
        and that dirty/closed bytes are also accurately tracked.
        """

        # Create a topic with `compact` policy, and a min.cleanable.dirty.ratio of 1.0.
        self.topic_setup(
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            replication_factor=3,
            key_set_cardinality=key_set_cardinality,
            partition_count=10,
            min_cleanable_dirty_ratio=1.0,
        )

        self.produce_and_consume()

        # At this point, the min.cleanable.dirty.ratio is 1.0
        self.prev_sliding_window_rounds = -1

        def compaction_has_completed():
            new_sliding_window_rounds = self.get_complete_sliding_window_rounds()

            res = self.prev_sliding_window_rounds == new_sliding_window_rounds
            self.prev_sliding_window_rounds = new_sliding_window_rounds
            return res

        wait_until(
            compaction_has_completed,
            timeout_sec=120,
            backoff_sec=self.extra_rp_conf["log_compaction_interval_ms"] / 1000 * 4,
            err_msg="Compaction did not stabilize.",
        )

        # We may race with a segment roll which won't be compacted (due to high min.cleanable.dirty.ratio),
        # so we cannot assert dirty_segment_bytes == 0 here.

        # Restart each redpanda broker to roll segments
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Check the dirty ratio after the segments were rolled and added to the dirty/closed bytes
        def seen_dirty_ratio_above_zero():
            return all(
                [self.get_dirty_ratio([node]) > 0.0 for node in self.redpanda.nodes]
            )

        wait_until(
            seen_dirty_ratio_above_zero,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Did not see a non-zero dirty ratio across all brokers.",
        )

        # Sleep for a period of time. We want to assert that no compaction rounds have
        # occured for our topic, which still has a min.cleanable.dirty.ratio of 1.0, but
        # a large number of closed, clean segments with only a small number of dirty segments.
        time.sleep(self.extra_rp_conf["log_compaction_interval_ms"] * 3 / 1000)

        complete_sliding_window_rounds = self.get_complete_sliding_window_rounds()
        assert complete_sliding_window_rounds == 0, (
            f"Expected complete sliding window rounds == 0 for a topic with min.cleanable.dirty.ratio == 1.0, got {complete_sliding_window_rounds}."
        )

        # Set the min.cleanable.dirty.ratio for our topic to 0.0. Expect to
        # see the rolled segment compacted along with the rest of the log
        self.set_min_cleanable_dirty_ratio(0.0)

        wait_until(
            compaction_has_completed,
            timeout_sec=120,
            backoff_sec=self.extra_rp_conf["log_compaction_interval_ms"] / 1000 * 4,
            err_msg="Compaction did not stabilize.",
        )

        def no_dirty_bytes():
            return all(
                [
                    self.get_dirty_segment_bytes([node]) == 0
                    and self.get_closed_segment_bytes([node]) > 0
                    for node in self.redpanda.nodes
                ]
            )

        # All dirty bytes should have eventually be cleaned
        # up by unconditional compaction
        wait_until(
            no_dirty_bytes,
            timeout_sec=120,
            backoff_sec=1,
            err_msg=f"Did not see dirty_segment_bytes == 0 and closed_segment_bytes > 0 across all brokers.",
        )

        # Perform validation with KgoVerifierSeqConsumer
        self.consume_and_validate_log()


class LogCompactionEnableSlidingWindow(RedpandaTest):
    """
    Test that enabling log_compaction_use_sliding_window when Redpanda is started with it
    disabled does not result in any broker crashes.
    """

    def __init__(self, test_context):
        self.test_context = test_context

        # Start with sliding window compaction set per the starting_config argument.
        use_sliding_window_starting_config = self.test_context.injected_args[
            "starting_config"
        ]
        self.extra_rp_conf = {
            "log_compaction_use_sliding_window": use_sliding_window_starting_config,
            "log_compaction_interval_ms": 100,
            "log_segment_size": 2 * 1024**2,  # 2 MiB
            "compacted_log_segment_size": 1024**2,  # 1 MiB
        }

        super().__init__(
            test_context=test_context, num_brokers=1, extra_rp_conf=self.extra_rp_conf
        )

        self._rpk_client = RpkTool(self.redpanda)

    @cluster(num_nodes=2)
    @matrix(starting_config=[False, True])
    def test_toggle_sliding_window(self, starting_config):
        next_sliding_window_config = not starting_config
        # If Redpanda was started with log_compaction_use_sliding_window=false and
        # we set it to true, the memory reservation for compaction will be left at 0.
        self._rpk_client.cluster_config_set(
            "log_compaction_use_sliding_window", next_sliding_window_config
        )

        topic_spec = TopicSpec(
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            min_cleanable_dirty_ratio=0.0,
            replication_factor=1,
        )
        self.client().create_topic(topic_spec)

        # The number of times log_compaction_use_sliding_window will be flipped on/off.
        self.prev_num_compacted_segments = 0
        num_rounds = 5
        for i in range(0, num_rounds):
            # Produce a small amount of segments and wait
            producer = KgoVerifierProducer(
                context=self.test_context,
                redpanda=self.redpanda,
                topic=topic_spec.name,
                msg_size=1024,
                msg_count=10000,
            )

            producer.start()
            producer.wait(timeout_sec=180)

            def seen_compacted_segments():
                num_compacted_segments = self.redpanda.metric_sum(
                    metric_name="vectorized_storage_log_compacted_segment_total",
                    metrics_endpoint=MetricsEndpoint.METRICS,
                    topic=topic_spec.name,
                    expect_metric=True,
                )
                ret = num_compacted_segments > self.prev_num_compacted_segments
                self.prev_num_compacted_segments = num_compacted_segments
                return ret

            wait_until(
                seen_compacted_segments,
                timeout_sec=60,
                backoff_sec=1,
                err_msg=f"Did not see any compacted segments.",
            )

            producer.free()

            next_sliding_window_config = not next_sliding_window_config
            self._rpk_client.cluster_config_set(
                "log_compaction_use_sliding_window", next_sliding_window_config
            )
