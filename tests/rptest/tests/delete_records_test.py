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

from ducktape.mark import parametrize, matrix
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kcl import KCL
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.util import produce_until_segments
from rptest.services.redpanda import SISettings
from rptest.utils.si_utils import BucketView, NTP


class DeleteRecordsTest(RedpandaTest):
    """
    The purpose of this test is to exercise the delete-records API which
    prefix truncates the log at a user defined offset.
    """

    log_segment_size = 1048576

    topics = [
        TopicSpec(name="test-topic-1",
                  partition_count=1,
                  replication_factor=3,
                  retention_bytes=-1,
                  cleanup_policy=TopicSpec.CLEANUP_DELETE)
    ]

    def __init__(self, test_context):
        extra_rp_conf = dict(enable_leader_balancer=False,
                             log_compaction_interval_ms=5000,
                             log_segment_size=self.log_segment_size)
        super(DeleteRecordsTest, self).__init__(test_context=test_context,
                                                num_brokers=3,
                                                extra_rp_conf=extra_rp_conf)
        self._ctx = test_context

        self.kcl = KCL(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        # Defer cluster startup so we can tweak configs based on the type of
        # test run.
        pass

    def _start(self, cloud_storage_enabled, start_with_data=True):
        # Tests will invoke this method to start redpanda with the correctly
        # applied test settings
        if cloud_storage_enabled:
            si_settings = SISettings(
                self._ctx,
                log_segment_size=self.log_segment_size,
                cloud_storage_housekeeping_interval_ms=2000,
                fast_uploads=True)
            extra_rp_conf = dict(
                log_compaction_interval_ms=2000,
                compacted_log_segment_size=self.log_segment_size)
            self.redpanda.set_extra_rp_conf(extra_rp_conf)
            self.redpanda.set_si_settings(si_settings)

        self.redpanda.start()
        self._create_initial_topics()

        if start_with_data is True:
            produce_until_segments(
                self.redpanda,
                topic=self.topic,
                partition_idx=0,
                count=10,
                acks=-1,
            )

        local_snapshot = self.redpanda.storage().segments_by_node(
            "kafka", self.topic, 0)
        assert len(local_snapshot) > 0, "empty snapshot"
        self.redpanda.logger.info(f"Snapshot: {local_snapshot}")

        # aggressive_local_retention = random.choice([True, False])
        aggressive_local_retention = True
        if cloud_storage_enabled and aggressive_local_retention:
            # Will evict local segments every 2 segments, the rational here is to
            # stress the TS path of delete-records where the data only exists in cloud
            # and not locally
            self.local_retention = self.log_segment_size * 2
            self.redpanda.logger.info(
                f"Turning on aggressive local retention, log_segment_size: {self.log_segment_size} retention.local.target.bytes: {self.local_retention}"
            )
            self.rpk.alter_topic_config(
                self.topic, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                self.local_retention)

        return local_snapshot

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
        response = self.kcl.delete_records(
            {topic: {
                partition: truncate_offset
            }})
        assert len(response) == 1
        assert response[0].topic == topic
        assert response[0].partition == partition
        assert response[0].error == 'OK', f"Err msg: {response[0].error}"
        return response[0].new_low_watermark

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
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_topic_start_delta(self, cloud_storage_enabled):
        """
        Test that delete-records moves forward the segment offset delta

        Perform this by creating only 1 segment and moving forward the start
        offset within that segment, performing verifications at each step
        """
        num_records = 10240
        records_size = 512
        truncate_offset_start = 100

        self._start(cloud_storage_enabled, start_with_data=False)

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
    @matrix(cloud_storage_enabled=[True, False],
            truncate_point=[
                "at_segment_boundary", "within_segment",
                "one_below_high_watermark", "at_high_watermark"
            ])
    def test_delete_records_segment_deletion(self, cloud_storage_enabled: bool,
                                             truncate_point: str):
        """
        Test that prefix truncation actually deletes data

        In the case the desired truncation point passes multiple segments it
        can be asserted that all of those segments will be deleted once the
        log_eviction_stm processes the deletion event
        """
        local_snapshot = self._start(cloud_storage_enabled)

        def get_segment_boundaries_via_local_storage(node):
            boundaries = sorted(
                [int(seg.data_file.split('-')[0]) for seg in node])
            self.redpanda.logger.debug(f"Local boundaries: {boundaries}")
            return boundaries

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

        def get_segment_boundaries_via_cloud_manifest(manifest):
            boundaries = sorted([
                int(idx.split('-')[0]) for idx in manifest['segments'].keys()
            ])
            self.redpanda.logger.debug(f"Cloud boundaries: {boundaries}")
            return boundaries

        # Tests for 3 different types of scenarios
        # 1. User wants to truncate all data - high_watermark
        # 2. User wants to truncate at a segment boundary - at_segment_boundary
        # 3. User wants to trunate between a boundary - within_segment
        def obtain_test_parameters(local_snapshot):
            node = local_snapshot[list(local_snapshot.keys())[-1]]
            segment_boundaries = get_segment_boundaries_via_local_storage(node)
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

        (truncate_offset,
         high_watermark) = obtain_test_parameters(local_snapshot)

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
                local_snapshot = self.redpanda.storage(
                    all_nodes=True).segments_by_node("kafka", self.topic, 0)
                return all([
                    all_segments_removed(
                        get_segment_boundaries_via_local_storage(node))
                    for _, node in local_snapshot.items()
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
                    assert all_segments_removed(
                        get_segment_boundaries_via_local_storage(node))

        if cloud_storage_enabled:

            def are_all_cloud_segments_removed():
                bv = BucketView(self.redpanda)
                cloud_manifest = bv.get_partition_manifest(
                    NTP("kafka", self.topic, 0))
                return all_segments_removed(
                    get_segment_boundaries_via_cloud_manifest(cloud_manifest))

            wait_until(
                are_all_cloud_segments_removed,
                timeout_sec=30,
                backoff_sec=5,
                err_msg=
                f"Failed while waiting on all cloud segments below lwm: {low_watermark} to be removed"
            )

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_bounds_checking(self, cloud_storage_enabled):
        """
        Ensure that the delete-records handler returns the appropriate error
        code when a user attempts to truncate at an offset that is not within
        the boundaries of the partition.
        """
        out_of_range_prefix = "OFFSET_OUT_OF_RANGE"

        self._start(cloud_storage_enabled)

        num_records = self.get_topic_info().high_watermark

        def check_bad_response(response):
            assert len(response) == 1
            assert response[0].topic == self.topic
            assert response[0].partition == 0
            assert response[0].new_low_watermark == -1
            return response[0].error

        def bad_truncation(truncate_offset):
            response = self.kcl.delete_records(
                {self.topic: {
                    0: truncate_offset
                }})
            assert check_bad_response(response).startswith(
                out_of_range_prefix
            ), f"Unexpected error msg: {response[0].error}"

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
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_empty_or_missing_topic_or_partition(
            self, cloud_storage_enabled):
        missing_topic = "foo_bar"
        out_of_range_prefix = "OFFSET_OUT_OF_RANGE"
        unknown_topic_or_partition = "UNKNOWN_TOPIC_OR_PARTITION"
        missing_partition = "partition was not returned"

        self._start(cloud_storage_enabled, start_with_data=False)

        # Assert failure condition on unknown topic
        response = self.kcl.delete_records({missing_topic: {0: 0}})
        assert len(response) == 1
        assert response[0].new_low_watermark == -1
        assert response[0].error.find(unknown_topic_or_partition) != -1

        # Assert failure condition on unknown partition index
        missing_idx = 15
        response = self.kcl.delete_records({self.topic: {missing_idx: 0}})
        assert len(response) == 1
        assert response[0].new_low_watermark == -1
        assert response[0].error.find(missing_partition) != -1

        # Assert out of range occurs on an empty topic
        attempts = 5
        response = []
        while attempts > 0:
            response = self.kcl.delete_records({self.topic: {0: 0}})
            assert len(response) == 1
            assert response[0].topic == self.topic
            assert response[0].partition == 0
            assert response[0].new_low_watermark == -1
            if not response[0].error.startswith("NOT_LEADER"):
                break
            time.sleep(1)
            attempts -= 1
        assert response[0].error.startswith(out_of_range_prefix)

        # Assert correct behavior on a topic with 1 record
        self.rpk.produce(self.topic, "k", "v", partition=0)
        self.wait_until_records(1, timeout_sec=5, backoff_sec=1)
        response = self.kcl.delete_records({self.topic: {0: 0}})
        assert len(response) == 1
        assert response[0].new_low_watermark == -1
        assert response[0].error.startswith(out_of_range_prefix)

        # ... truncating at high watermark 1 should delete all data
        low_watermark = self.delete_records(self.topic, 0, 1)
        assert low_watermark == 1
        topic_info = self.get_topic_info()
        assert topic_info.high_watermark == 1

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_with_transactions(self, cloud_storage_enabled):
        """
        Tests that the log_eviction_stm is respecting the max_collectible_offset
        """
        self._start(cloud_storage_enabled)

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
                response = self.kcl.delete_records(
                    {self.topic: {
                        0: high_watermark
                    }}, timeout_ms=5000)
                assert len(response) == 1
                # Even though the on disk data may be late to evict, the start offset
                # should have been immediately updated
                assert response[0].new_low_watermark == high_watermark
                assert response[0].error == 'OK'
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
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_concurrent_truncations(self,
                                                   cloud_storage_enabled):
        """
        This tests verifies that issuing DeleteRecords requests with concurrent
        producers and consumers has no effect on correctness
        """
        self._start(cloud_storage_enabled)

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
            response = self.kcl.delete_records(
                {self.topic: {
                    0: truncate_point
                }}, timeout_ms=5000)
            assert len(response) == 1
            assert response[0].new_low_watermark == truncate_point
            assert response[0].error == 'OK'
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
