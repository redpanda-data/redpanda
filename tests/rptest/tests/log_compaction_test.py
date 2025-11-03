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
from dataclasses import dataclass

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda_installer import (
    RedpandaVersionLine,
)
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.util import wait_until_result


class LogCompactionTestBase(PartitionMovementMixin):
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

    def start_partition_movement(self):
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

        self.partition_move_thread = threading.Thread(
            target=background_test_loop,
            args=(PartitionMoveExceptionReporter, issue_partition_move),
            kwargs={"iterations": 5, "sleep_sec": 1},
        )
        self.partition_movement_exception = PartitionMoveExceptionReporter.exc

        # Start partition movement thread
        self.partition_move_thread.start()

    def stop_partition_movement(self):
        # Clean up partition movement thread
        self.partition_move_thread.join()

        if self.partition_movement_exception is not None:
            raise self.partition_movement_exception

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

    def wait_for_sliding_window_compaction(self):
        self.prev_sliding_window_rounds = None

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


class LogCompactionTest(LogCompactionTestBase, PreallocNodesTest):
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

        self.start_partition_movement()
        self.produce_and_consume()

        self.validate_log(cleanup_policy)

        if cleanup_policy == TopicSpec.CLEANUP_COMPACT_DELETE:
            self.wait_for_log_truncation()

        self.stop_partition_movement()


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
        self.wait_for_sliding_window_compaction()

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

        self.wait_for_sliding_window_compaction()

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
            err_msg="Did not see dirty_segment_bytes == 0 and closed_segment_bytes > 0 across all brokers.",
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
                err_msg="Did not see any compacted segments.",
            )

            producer.free()

            next_sliding_window_config = not next_sliding_window_config
            self._rpk_client.cluster_config_set(
                "log_compaction_use_sliding_window", next_sliding_window_config
            )


class LogCompactionTxRemovalTestBase(LogCompactionTestBase, PreallocNodesTest):
    @dataclass
    class TestCase:
        msg_size: int
        msg_count: int
        abort_rate: float
        msgs_per_transaction: int

    test_cases = {
        "Mixed aborts and commits": TestCase(
            msg_size=1024,
            msg_count=10000,
            msgs_per_transaction=10,
            abort_rate=0.5,
        ),
        "All aborts": TestCase(
            msg_size=1024,
            msg_count=10000,
            msgs_per_transaction=10,
            abort_rate=1.0,
        ),
        "All commits": TestCase(
            msg_size=1024,
            msg_count=10000,
            msgs_per_transaction=10,
            abort_rate=0.0,
        ),
        "Multi-segment spanning transactions": TestCase(
            msg_size=10240,
            msg_count=1000,
            msgs_per_transaction=100,
            abort_rate=0.5,
        ),
    }

    def __init__(self, test_context):
        self.test_context = test_context
        # Run with small segments and a very frequent compaction interval.
        self.extra_rp_conf = {
            "log_compaction_interval_ms": 1000,
            "log_segment_size": 2 * 1024**2,  # 2 MiB
            "compacted_log_segment_size": 1024**2,  # 1 MiB
            # Trigger tombstone removal quickly
            "storage_target_replay_bytes": 100,
            "log_segment_ms": 60,
        }
        self.transaction_timeout_ms = 2000

        super().__init__(
            test_context=test_context,
            num_brokers=3,
            node_prealloc_count=1,
            extra_rp_conf=self.extra_rp_conf,
        )

        self._rpk_client = RpkTool(self.redpanda)

    def produce(self, test_case):
        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.topic_spec.name,
            msg_size=test_case.msg_size,
            msg_count=test_case.msg_count,
            use_transactions=True,
            transaction_timeout_ms=self.transaction_timeout_ms,
            transaction_abort_rate=test_case.abort_rate,
            msgs_per_transaction=test_case.msgs_per_transaction,
            custom_node=self.preallocated_nodes,
            tolerate_failed_produce=True,
            tolerate_data_loss=True,
            wait_for_acks=False,
        )

        producer.start()
        producer.wait(timeout_sec=180)
        producer.stop()

    def all_tx_batches_removed(self):
        viewer = OfflineLogViewer(self.redpanda)
        node_results = []
        for node in self.redpanda.nodes:
            num_control_batches = 0
            num_fence_batches = 0
            partitions = viewer.read_kafka_records(node, self.topic_spec.name)
            partition_results = []
            for partition in partitions:
                for record_or_batch in partition:
                    if "expanded_attrs" not in record_or_batch:
                        continue
                    if record_or_batch["expanded_attrs"]["control_batch"]:
                        num_control_batches += 1
                    if record_or_batch["type_name"] == "tx_fence":
                        num_fence_batches += 1

                partition_results.append((num_control_batches, num_fence_batches))
            self.redpanda.logger.debug(
                f"Node {node.name} compaction results {partition_results}"
            )
            node_results.append(
                all(
                    [
                        num_control_batches == 0 and num_fence_batches == 0
                        for num_control_batches, num_fence_batches in partition_results
                    ]
                )
            )
        return all(node_results)

    def wait_until_stms_caught_up(self):
        # grab the debug partition dump for each partition ensure
        # committed index matches last applied offset
        def stms_caught_up(partition_id: int):
            state = self.redpanda._admin.get_partition_state(
                namespace="kafka", topic=self.topic_spec.name, partition=partition_id
            )
            raft_states = [r["raft_state"] for r in state["replicas"]]
            commit_indexes = []
            commit_and_applied = []
            for s in raft_states:
                commit_index = s["commit_index"]
                commit_indexes.append(commit_index)
                last_applied = -1
                for stm in s["stms"]:
                    if stm["name"] == "tx.snapshot":
                        last_applied = stm["last_applied_offset"]
                commit_and_applied.append(commit_index == last_applied)
            synced = all(commit_and_applied) and len(set(commit_indexes)) == 1
            # Returns whether all STMs are caught up, and the commit index
            # at which they are caught up.
            return (synced, commit_indexes[0])

        def all_partition_stms_caught_up():
            stm_results = [
                stms_caught_up(partition_id)
                for partition_id in range(self.partition_count)
            ]
            synced, commit_indexes = map(list, zip(*stm_results))
            return all(synced), commit_indexes

        return wait_until_result(
            all_partition_stms_caught_up,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="STMs did not catch up for all partitions.",
            retry_on_exc=True,
        )

    def wait_for_local_snaphot_catchup(self, offsets: list[int]):
        def stms_snaphotted(partition_id: int, target_offset: int):
            state = self.redpanda._admin.get_partition_state(
                namespace="kafka", topic=self.topic_spec.name, partition=partition_id
            )
            raft_states = [r["raft_state"] for r in state["replicas"]]
            snapshotted = []
            for s in raft_states:
                for stm in s["stms"]:
                    if stm["name"] == "tx.snapshot":
                        last_snapshot = stm["last_local_snapshot_offset"]
                        snapshotted.append(last_snapshot >= target_offset)
            return all(snapshotted) and len(snapshotted) == len(raft_states)

        def all_partition_stms_snaphotted():
            return all(
                [
                    stms_snaphotted(partition_id, offsets[partition_id])
                    for partition_id in range(self.partition_count)
                ]
            )

        deadline = time.time() + 120
        while True:
            if time.time() > deadline:
                raise TimeoutError(
                    "Not all STMs have local snapshots at target offsets."
                )
            try:
                if all_partition_stms_snaphotted():
                    return
                # Produce some garbage to force snapshots
                KgoVerifierProducer.oneshot(
                    context=self.test_context,
                    redpanda=self.redpanda,
                    topic=self.topic_spec.name,
                    msg_size=1024,
                    msg_count=10000,
                    custom_node=self.preallocated_nodes,
                )
            except Exception as e:
                self.redpanda.logger.warning(
                    f"Exception while checking STM snapshots, retrying... {e}"
                )
            time.sleep(5)

    def do_test_tx_control_batch_removal(self, test_case_name, test_case):
        self.logger.info(
            f"Running test case {test_case_name} with topic {self.topic_spec.name}"
        )

        self.start_partition_movement()
        self.produce(test_case)

        # Restart the redpanda broker to roll segments
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Sleep in order to allow any open transaction to be closed.
        time.sleep(2 * self.transaction_timeout_ms / 1000)

        self.wait_for_sliding_window_compaction()

        self.stop_partition_movement()

        commit_idxs = self.wait_until_stms_caught_up()
        self.logger.debug(
            f"STMs caught up at commit indexes {commit_idxs}, waiting for local snapshots to catchup"
        )
        self.wait_for_local_snaphot_catchup(commit_idxs)
        # Check that no tx batches are seen after compaction settles
        self.redpanda.wait_until(
            self.all_tx_batches_removed,
            timeout_sec=240,
            backoff_sec=5,
            err_msg="Transactional batches were not removed after compaction.",
            retry_on_exc=True,
        )


class LogCompactionTxRemovalTest(LogCompactionTxRemovalTestBase):
    def __init__(self, test_context):
        super().__init__(test_context)

    @cluster(num_nodes=4)
    def test_tx_control_batch_removal(self):
        failed_test_cases = []
        for name, test_case in LogCompactionTxRemovalTestBase.test_cases.items():
            self.topic_setup(
                cleanup_policy=TopicSpec.CLEANUP_COMPACT,
                replication_factor=3,
                key_set_cardinality=100,
                partition_count=1,
            )

            try:
                self.do_test_tx_control_batch_removal(name, test_case)
            except Exception as e:
                self.logger.info(f"Test case {name} failed with exception {e}")

                failed_test_cases.append(e)
        assert len(failed_test_cases) == 0, (
            f"Expected 0 failed test cases, got {len(failed_test_cases)}"
        )


class LogCompactionTxRemovalUpgradeTest(LogCompactionTxRemovalTestBase):
    def __init__(self, test_context):
        super().__init__(test_context)
        # Version before `may_have_transactional_batches` was added.
        self.initial_version: RedpandaVersionLine = (25, 1)
        # Version before tx removal was added to compaction.
        self.may_have_tx_batch_version: RedpandaVersionLine = (25, 2)

    def setUp(self):
        self.redpanda._installer.install(self.redpanda.nodes, self.initial_version)
        self.redpanda.start()

    def upgrade_to_version(self, version):
        self.redpanda._installer.install(self.redpanda.nodes, version)
        self.redpanda.restart_nodes(self.redpanda.nodes)

    @cluster(num_nodes=4)
    @matrix(test_case_name=list(LogCompactionTxRemovalTestBase.test_cases.keys()))
    def test_tx_control_batch_removal_with_upgrade(self, test_case_name):
        test_case = LogCompactionTxRemovalTestBase.test_cases[test_case_name]

        self.topic_setup(
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            replication_factor=3,
            key_set_cardinality=100,
            partition_count=1,
        )

        # Produce some transactional data
        self.produce(test_case)

        # Upgrade to `may_have_transactional_batch` version.
        self.upgrade_to_version(self.may_have_tx_batch_version)

        # Produce more transactional data.
        self.produce(test_case)

        # Upgrade to `HEAD`.
        for version in self.redpanda._installer.upgrade_path_to_head(
            self.may_have_tx_batch_version
        ):
            self.upgrade_to_version(version)

        # Perform rest of test
        self.do_test_tx_control_batch_removal(test_case_name, test_case)
