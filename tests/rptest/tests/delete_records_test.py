# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random
import signal
import string
import threading
import confluent_kafka as ck
import ducktape

from ducktape.mark import parametrize
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.util import produce_until_segments
from rptest.util import expect_exception


class DeleteRecordsTest(RedpandaTest):
    """
    The purpose of this test is to exercise the delete-records API which
    prefix truncates the log at a user defined offset.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        extra_rp_conf = dict(
            enable_leader_balancer=False,
            log_compaction_interval_ms=5000,
            log_segment_size=1048576,
        )
        super(DeleteRecordsTest, self).__init__(test_context=test_context,
                                                num_brokers=3,
                                                extra_rp_conf=extra_rp_conf)
        self._ctx = test_context

        self.rpk = RpkTool(self.redpanda)

    def get_topic_info(self):
        topics_info = list(self.rpk.describe_topic(self.topic))
        self.logger.info(topics_info)
        assert len(topics_info) == 1
        return topics_info[0]

    def wait_until_records(self, offset, timeout_sec=30, backoff_sec=1):
        def expect_high_watermark():
            topic_info = self.get_topic_info()
            return topic_info.high_watermark == offset

        wait_until(expect_high_watermark,
                   timeout_sec=timeout_sec,
                   backoff_sec=backoff_sec)

    def delete_records(self, topic, partition, truncate_offset):
        """
        Makes delete records call with 1 topic partition in the request body.

        Asserts that the response contains 1 element, with matching topic &
        partition contents.
        """
        response = self.rpk.trim_prefix(topic,
                                        truncate_offset,
                                        partitions=[partition])
        assert len(response) == 1
        assert response[0].topic == topic
        assert response[0].partition == partition
        assert response[0].error_msg == "", f"Err msg: {response[0].error}"
        return response[0].new_start_offset

    def assert_start_partition_boundaries(self, truncate_offset):
        def check_bound_start(offset):
            try:
                r = self.rpk.consume(self.topic,
                                     n=1,
                                     offset=f'{offset}-{offset+1}',
                                     quiet=True,
                                     timeout=10)
                return r.count('_') == 1
            except Exception as _:
                return False

        assert check_bound_start(
            truncate_offset
        ), f"new log start: {truncate_offset} not consumable"
        assert not check_bound_start(
            truncate_offset -
            1), f"before log start: {truncate_offset - 1} is consumable"

    def assert_new_partition_boundaries(self, truncate_offset, high_watermark):
        """
        Returns true if the partition contains records at the expected boundaries,
        ensuring the truncation worked at the exact requested point and that the
        number of remaining records is as expected.
        """
        def check_bound_end(offset):
            try:
                # Not timing out means data was available to read
                _ = self.rpk.consume(self.topic,
                                     n=1,
                                     offset=offset,
                                     timeout=10)
            except Exception as _:
                return False
            return True

        assert truncate_offset <= high_watermark, f"Test malformed"

        if truncate_offset == high_watermark:
            # Assert no data at all can be read
            assert not check_bound_end(truncate_offset)
            return

        # truncate_offset is inclusive start of log
        # high_watermark is exclusive end of log
        # Readable offsets: [truncate_offset, high_watermark)
        self.assert_start_partition_boundaries(truncate_offset)
        assert check_bound_end(
            high_watermark -
            1), f"log end: {high_watermark - 1} not consumable"
        assert not check_bound_end(
            high_watermark), f"high watermark: {high_watermark} is consumable"

    @cluster(num_nodes=3)
    def test_delete_records_topic_start_delta(self):
        """
        Test that delete-records moves forward the segment offset delta

        Perform this by creating only 1 segment and moving forward the start
        offset within that segment, performing verifications at each step
        """
        num_records = 10240
        records_size = 512
        truncate_offset_start = 100

        # Produce some data, wait for it all to arrive
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.produce(self.topic, num_records, records_size)
        self.wait_until_records(num_records, timeout_sec=10, backoff_sec=1)

        # Call delete-records in a loop incrementing new point each time
        for truncate_offset in range(truncate_offset_start,
                                     truncate_offset_start + 5):
            # Perform truncation
            low_watermark = self.delete_records(self.topic, 0, truncate_offset)
            assert low_watermark == truncate_offset, f"Expected low watermark: {truncate_offset} observed: {low_watermark}"

            # Assert correctness of start and end offsets in topic metadata
            topic_info = self.get_topic_info()
            assert topic_info.id == 0, f"Partition id: {topic_info.id}"
            assert topic_info.start_offset == truncate_offset, f"Start offset: {topic_info.start_offset}"
            assert topic_info.high_watermark == num_records, f"High watermark: {topic_info.high_watermark}"

            # ... and in actual fetch requests
            self.assert_new_partition_boundaries(truncate_offset,
                                                 topic_info.high_watermark)

    # Disable checks for storage usage inconsistencies as orphaned log segments left
    # post node crash have been observed in this test. This is not something
    # specific to delete-records but will happen in all cases where segment deletion
    # occurs at the moment a failure is injected.
    @cluster(num_nodes=3, check_for_storage_usage_inconsistencies=False)
    @parametrize(truncate_point="at_segment_boundary")
    @parametrize(truncate_point="within_segment")
    @parametrize(truncate_point="one_below_high_watermark")
    @parametrize(truncate_point="at_high_watermark")
    def test_delete_records_segment_deletion(self, truncate_point: str):
        """
        Test that prefix truncation actually deletes data

        In the case the desired truncation point passes multiple segments it
        can be asserted that all of those segments will be deleted once the
        log_eviction_stm processes the deletion event
        """
        segments_to_write = 10

        # Produce 10 segments, then issue a truncation, assert corect number
        # of segments are deleted
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=segments_to_write,
            acks=-1,
        )

        # Grab a snapshot of the segments to determine the final segment
        # indicies of all segments. This is to test corner cases where the
        # user happens to pass a segment index as a truncation index.
        snapshot = self.redpanda.storage().segments_by_node(
            "kafka", self.topic, 0)
        assert len(snapshot) > 0, "empty snapshot"
        self.redpanda.logger.info(f"Snapshot: {snapshot}")

        def get_segment_boundaries(node):
            def to_final_indicies(seg):
                if seg.data_file is not None:
                    return int(seg.data_file.split('-')[0])
                else:
                    # A segment with no data file indicates that an index or
                    # compaction index was left behind for a deleted segment, or
                    # deletion is currently in flight.
                    return 0

            return sorted([to_final_indicies(seg) for seg in node])

        # Tests for 3 different types of scenarios
        # 1. User wants to truncate all data - high_watermark
        # 2. User wants to truncate at a segment boundary - at_segment_boundary
        # 3. User wants to trunate between a boundary - within_segment
        def obtain_test_parameters(snapshot):
            node = snapshot[list(snapshot.keys())[-1]]
            segment_boundaries = get_segment_boundaries(node)
            self.redpanda.logger.info(
                f"Segment boundaries: {segment_boundaries}")
            truncate_offset = None
            high_watermark = int(self.get_topic_info().high_watermark)
            if truncate_point == "one_below_high_watermark":
                truncate_offset = high_watermark - 1  # Leave 1 record
            elif truncate_point == "at_high_watermark":
                truncate_offset = high_watermark  # Delete all data
            elif truncate_point == "within_segment":
                random_seg = random.randint(0, len(segment_boundaries) - 2)
                truncate_offset = random.randint(
                    segment_boundaries[random_seg] + 1,
                    segment_boundaries[random_seg + 1] - 1)
            elif truncate_point == "at_segment_boundary":
                random_seg = random.randint(1, len(segment_boundaries) - 1)
                truncate_offset = segment_boundaries[random_seg] - 1
            else:
                assert False, "unknown test parameter encountered"

            self.redpanda.logger.info(f"Truncate offset: {truncate_offset}")
            return (truncate_offset, high_watermark)

        (truncate_offset, high_watermark) = obtain_test_parameters(snapshot)

        # Make delete-records call, assert response looks ok
        try:
            low_watermark = self.delete_records(self.topic, 0, truncate_offset)
            assert low_watermark == truncate_offset, f"Expected low watermark: {truncate_offset} observed: {low_watermark}"
        except Exception as e:
            topic_info = self.get_topic_info()
            self.redpanda.logger.info(
                f"Start offset: {topic_info.start_offset}")
            raise e

        # Restart one node while the effect is propogating
        stopped_node = random.choice(self.redpanda.nodes)
        self.redpanda.signal_redpanda(stopped_node, signal=signal.SIGKILL)
        self.redpanda.start_node(stopped_node)

        # Assert start offset is correct and there aren't any off-by-one errors
        self.assert_new_partition_boundaries(low_watermark, high_watermark)

        def all_segments_removed(segments):
            num_below_watermark = len(
                [seg for seg in segments if seg < low_watermark])
            return num_below_watermark <= 1

        try:
            self.redpanda.logger.info(
                f"Waiting until all segments below low watermark {low_watermark} are deleted"
            )

            def are_all_local_segments_removed():
                snapshot = self.redpanda.storage(
                    all_nodes=True).segments_by_node("kafka", self.topic, 0)
                return all([
                    all_segments_removed(get_segment_boundaries(node))
                    for _, node in snapshot.items()
                ])

            wait_until(
                are_all_local_segments_removed,
                timeout_sec=30,
                backoff_sec=5,
                err_msg=
                f"Failed while waiting on all segments below lwm: {low_watermark} to be removed"
            )
        except ducktape.errors.TimeoutError as e:
            self.redpanda.logger.info(
                f"Timed out waiting for segments, ensure no orphaned segments exist nodes that didn't crash"
            )
            snapshot = self.redpanda.storage(all_nodes=True).segments_by_node(
                "kafka", self.topic, 0)
            for _, node in snapshot.items():
                if node != stopped_node:
                    assert all_segments_removed(get_segment_boundaries(node))

    @cluster(num_nodes=3)
    def test_delete_records_bounds_checking(self):
        """
        Ensure that the delete-records handler returns the appropriate error
        code when a user attempts to truncate at an offset that is not within
        the boundaries of the partition.
        """
        num_records = 10240
        records_size = 512
        out_of_range_prefix = "OFFSET_OUT_OF_RANGE"

        # Produce some data, wait for it all to arrive
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.produce(self.topic, num_records, records_size)
        self.wait_until_records(num_records, timeout_sec=10, backoff_sec=1)

        def bad_truncation(truncate_offset):
            with expect_exception(RpkException,
                                  lambda e: out_of_range_prefix in str(e)):
                self.rpk.trim_prefix(self.topic, truncate_offset, [0])

        # Try truncating past the end of the log
        bad_truncation(num_records + 1)

        # Truncate to attempt to truncate before new beginning
        truncate_offset = 125
        low_watermark = self.delete_records(self.topic, 0, truncate_offset)
        assert low_watermark == truncate_offset

        # Try to truncate before and at the low_watermark
        bad_truncation(0)
        bad_truncation(low_watermark)

        # Try to truncate at a specific edge case where the start and end
        # are 1 offset away from eachother
        truncate_offset = num_records - 2
        for t_ofs in range(truncate_offset, num_records + 1):
            low_watermark = self.delete_records(self.topic, 0, t_ofs)
            assert low_watermark == t_ofs

        # Assert that nothing is readable
        bad_truncation(truncate_offset)
        bad_truncation(num_records)
        bad_truncation(num_records + 1)

    @cluster(num_nodes=3)
    def test_delete_records_empty_or_missing_topic_or_partition(self):
        missing_topic = "foo_bar"
        out_of_range_prefix = "OFFSET_OUT_OF_RANGE"
        unknown_topic_or_partition = "UNKNOWN_TOPIC_OR_PARTITION"

        # Assert failure condition on unknown topic
        with expect_exception(RpkException,
                              lambda e: unknown_topic_or_partition in str(e)):
            self.rpk.trim_prefix(missing_topic, 0, [0])

        # Assert failure condition on unknown partition index
        with expect_exception(RpkException,
                              lambda e: unknown_topic_or_partition in str(e)):
            missing_idx = 15
            self.rpk.trim_prefix(self.topic, 0, [missing_idx])

        # Assert out of range occurs on an empty topic
        with expect_exception(RpkException,
                              lambda e: out_of_range_prefix in str(e)):
            self.rpk.trim_prefix(self.topic, 0, [0])

        # Assert correct behavior on a topic with 1 record
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.produce(self.topic, 1, 512)
        self.wait_until_records(1, timeout_sec=5, backoff_sec=1)
        with expect_exception(RpkException,
                              lambda e: out_of_range_prefix in str(e)):
            self.rpk.trim_prefix(self.topic, 0, [0])

        # ... truncating at high watermark 1 should delete all data
        low_watermark = self.delete_records(self.topic, 0, 1)
        assert low_watermark == 1
        topic_info = self.get_topic_info()
        assert topic_info.high_watermark == 1

    @cluster(num_nodes=3)
    def test_delete_records_with_transactions(self):
        """
        Tests that the log_eviction_stm is respecting the max_collectible_offset
        """
        payload = ''.join(
            random.choice(string.ascii_letters) for _ in range(512))
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
        })
        producer.init_transactions()

        def delete_records_within_transaction(reporter):
            try:
                high_watermark = int(self.get_topic_info().high_watermark)
                response = self.rpk.trim_prefix(self.topic, high_watermark,
                                                [0])
                assert len(response) == 1
                # Even though the on disk data may be late to evict, the start offset
                # should have been immediately updated
                assert response[0].new_start_offset == high_watermark
                assert response[0].error_msg == ""
            except Exception as e:
                reporter.exc = e

        # Write 20 records and leave the transaction open
        producer.begin_transaction()
        for idx in range(0, 20):
            producer.produce(self.topic, '0', payload, 0)
            producer.flush()

        # The eviction_stm will be re-queuing the event until the max collectible
        # offset is moved ahead of the eviction offset, or until a timeout occurs
        class ThreadReporter:
            exc = None

        eviction_thread = threading.Thread(
            target=delete_records_within_transaction, args=(ThreadReporter, ))
        eviction_thread.start()

        # Committing the transaction will allow the eviction_stm to move forward
        # and process the event.
        time.sleep(1)
        producer.commit_transaction()
        eviction_thread.join()
        # Rethrow exception encountered in thread if observed
        if ThreadReporter.exc is not None:
            raise ThreadReporter.exc

    @cluster(num_nodes=5)
    def test_delete_records_concurrent_truncations(self):
        """
        This tests verifies that issuing DeleteRecords requests with concurrent
        producers and consumers has no effect on correctness
        """

        # Should complete producing within 20s
        producer = KgoVerifierProducer(self._ctx,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=512,
                                       msg_count=20000,
                                       rate_limit_bps=500000)  # 0.5/mbs
        consumer = KgoVerifierConsumerGroupConsumer(self._ctx,
                                                    self.redpanda,
                                                    self.topic,
                                                    512,
                                                    1,
                                                    max_msgs=20000)

        def periodic_delete_records():
            topic_info = self.get_topic_info()
            start_offset = topic_info.start_offset + 1
            high_watermark = topic_info.high_watermark - 1
            if high_watermark - start_offset <= 0:
                self.redpanda.logger.info("Waiting on log to populate")
                return
            truncate_point = random.randint(start_offset, high_watermark)
            self.redpanda.logger.info(
                f"Issuing delete_records request at offset: {truncate_point}")
            response = self.rpk.trim_prefix(self.topic, truncate_point, [0])
            assert len(response) == 1
            assert response[0].new_start_offset == truncate_point
            assert response[0].error_msg == ""
            # Cannot assert end boundaries as there is a concurrent producer
            # moving the hwm forward
            self.assert_start_partition_boundaries(truncate_point)

        def periodic_delete_records_loop(reporter,
                                         iterations,
                                         sleep_sec,
                                         allowable_retrys=3):
            """
            Periodicially issue delete records requests within a loop. allowable_reties
            exists so that the test doesn't automatically fail when a leadership change occurs.
            The implementation guaratntees that the special prefix_truncation batch is
            replicated before the client is acked, it however does not guarantee that the effect
            has occured. Have the test retry on some failures to account for this.
            """
            try:
                try:
                    while iterations > 0:
                        periodic_delete_records()
                        iterations -= 1
                        time.sleep(sleep_sec)
                except AssertionError as e:
                    if allowable_retrys == 0:
                        raise e
                    allowable_retrys = allowable_retrys - 1
            except Exception as e:
                reporter.exc = e

        class ThreadReporter:
            exc = None

        n_attempts = 10
        thread_sleep = 1  # 10s of uptime, less if exception thrown
        delete_records_thread = threading.Thread(
            target=periodic_delete_records_loop,
            args=(
                ThreadReporter,
                n_attempts,
                thread_sleep,
            ))

        # Start up producer/consumer and thread that periodically issues delete records requests
        delete_records_thread.start()
        producer.start()
        consumer.start()

        # Shut down all threads started above
        self.redpanda.logger.info("Joining on delete-records thread")
        delete_records_thread.join()
        self.redpanda.logger.info("Joining on producer thread")
        producer.wait()
        self.redpanda.logger.info("Calling consumer::stop")
        consumer.stop()
        self.redpanda.logger.info("Joining on consumer thread")
        consumer.wait()
        if ThreadReporter.exc is not None:
            raise ThreadReporter.exc

        status = consumer.consumer_status
        assert status.validator.invalid_reads == 0
        assert status.validator.out_of_scope_invalid_reads == 0
