# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures
import datetime
import re
import threading
from logging import Logger
import time
from typing import Callable

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService, SISettings, make_redpanda_service
from rptest.services.metrics_check import MetricCheck
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cat import KafkaCat
from rptest.util import (segments_count, wait_until,
                         wait_for_local_storage_truncate, wait_until_result)

from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.utils.si_utils import BucketView, NTP

from ducktape.mark import parametrize

from rptest.services.kafka import KafkaServiceAdapter
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from ducktape.mark.resource import cluster as ducktape_cluster
from kafkatest.version import V_3_0_0
from ducktape.tests.test import Test
from rptest.clients.default import DefaultClient


class BaseTimeQuery:

    log_segment_size: int
    client: Callable[[], DefaultClient]
    test_context: dict
    logger: Logger

    def _create_and_produce(self, cluster, cloud_storage, local_retention,
                            base_ts, record_size, msg_count):
        topic = TopicSpec(name="tqtopic",
                          partition_count=1,
                          replication_factor=3)
        self.client().create_topic(topic)

        if cloud_storage:
            for k, v in {
                    'redpanda.remote.read': 'true',
                    'redpanda.remote.write': 'true',
                    'retention.local.target.bytes': local_retention
            }.items():
                self.client().alter_topic_config(topic.name, k, v)
            desc = self.client().describe_topic_configs(topic.name)
            assert desc['redpanda.remote.read'] == 'true'
            assert desc['redpanda.remote.write'] == 'true'

        # Configure topic to trust client-side timestamps, so that
        # we can generate fake ones for the test
        self.client().alter_topic_config(topic.name, 'message.timestamp.type',
                                         "CreateTime")

        # Disable time based retention because we will use synthetic timestamps
        # that may well fall outside of the default 1 week relative to walltime
        self.client().alter_topic_config(topic.name, 'retention.ms', "-1")

        # Use small segments
        self.client().alter_topic_config(topic.name, 'segment.bytes',
                                         self.log_segment_size)

        # Produce a run of messages with CreateTime-style timestamps, each
        # record having a timestamp 1ms greater than the last.
        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=cluster,
            topic=topic.name,
            msg_size=record_size,
            msg_count=msg_count,

            # A respectable number of messages per batch so that we are covering
            # the case of looking up timestamps that fall in the middle of a batch,
            # but small enough that we are getting plenty of distinct batches.
            batch_max_bytes=record_size * 10,

            # A totally arbitrary artificial timestamp base in milliseconds
            fake_timestamp_ms=base_ts)
        producer.start()
        producer.wait()

        # We know timestamps, they are generated linearly from the
        # base we provided to kgo-verifier
        timestamps = dict((i, base_ts + i) for i in range(0, msg_count))
        return topic, timestamps

    def _test_timequery(self, cluster, cloud_storage: bool, batch_cache: bool):
        total_segments = 12
        record_size = 1024
        base_ts = 1664453149000
        msg_count = (self.log_segment_size * total_segments) // record_size
        local_retention = self.log_segment_size * 4
        kcat = KafkaCat(cluster)

        # Test the base case with an empty topic.
        empty_topic = TopicSpec(name="tq_empty_topic",
                                partition_count=1,
                                replication_factor=3)
        self.client().create_topic(empty_topic)
        offset = kcat.query_offset(empty_topic.name, 0, base_ts)
        self.logger.info(f"Time query returned offset {offset}")
        assert offset == -1, f"Expected -1, got {offset}"

        # Create a topic and produce a run of messages we will query.
        topic, timestamps = self._create_and_produce(cluster, cloud_storage,
                                                     local_retention, base_ts,
                                                     record_size, msg_count)
        for k, v in timestamps.items():
            self.logger.debug(f"  Offset {k} -> Timestamp {v}")

        # Confirm messages written
        rpk = RpkTool(cluster)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        if cloud_storage:
            # If using cloud storage, we must wait for some segments
            # to fall out of local storage, to ensure we are really
            # hitting the cloud storage read path when querying.
            wait_for_local_storage_truncate(redpanda=cluster,
                                            topic=topic.name,
                                            target_bytes=local_retention)

        # Identify partition leader for use in our metrics checks
        leader_node = cluster.get_node(
            next(rpk.describe_topic(topic.name)).leader)

        # For when using cloud storage, we expect offsets ahead
        # of this to still hit raft for their timequeries.
        is_redpanda = isinstance(cluster, RedpandaService)
        if is_redpanda:
            admin = Admin(self.redpanda)
            status = admin.get_partition_cloud_storage_status(topic.name, 0)
            local_start_offset = status["local_log_start_offset"]

        # Class defining expectations of timequery results to be checked
        class ex:
            def __init__(self, offset, ts=None, expect_read=True):
                if ts is None:
                    ts = timestamps[offset]
                self.ts = ts
                self.offset = offset
                self.expect_read = expect_read

        # We will do approx. 10 timequeries within each segment.
        step = msg_count // total_segments // 10

        expectations = []
        for o in range(0, msg_count, step):
            expect_read = o < msg_count
            expectations.append(ex(o, timestamps[o], expect_read))

        # Add edge cases
        expectations += [
            ex(0, timestamps[0] - 1000),  # Before the start of the log
            ex(-1, timestamps[msg_count - 1] + 1000,
               False)  # After last message
        ]

        # Remember which offsets we already hit, so that we can
        # make a good guess at whether subsequent hits on the same
        # offset should cause cloud downloads.
        hit_offsets = set()

        cloud_metrics = None
        local_metrics = None

        def diff_bytes(old, new):
            # Each timequery will download a maximum of two chunks, but
            # we make the check extra generous to account for the index
            # download.
            return new - old <= self.chunk_size * 5

        def diff_chunks(old, new):
            # The sampling step for a segment's remote index is 64 KiB and the chunk
            # size in this the is 128 KiB. Therefore, a timequery should never require
            # more than two chunks. If the samples were perfectly aligned with the chunks,
            # we'd only need one chunk, but that's not always the case.
            return new - old <= 2

        for e in expectations:
            ts = e.ts
            o = e.offset

            if is_redpanda and cloud_storage:
                cloud_metrics = MetricCheck(
                    self.logger,
                    cluster,
                    leader_node,
                    re.compile("vectorized_cloud_storage_.*"),
                    reduce=sum)

            if is_redpanda and not cloud_storage:
                local_metrics = MetricCheck(
                    self.logger,
                    cluster,
                    leader_node,
                    re.compile("vectorized_storage_.*"),
                    reduce=sum)

            self.logger.info(
                f"Attempting time lookup ts={ts} (should be o={o})")
            offset = kcat.query_offset(topic.name, 0, ts)
            self.logger.info(f"Time query returned offset {offset}")
            assert offset == o

            if is_redpanda and cloud_storage and o < local_start_offset and o not in hit_offsets and e.expect_read:
                # The number of bytes downloaded to query this offset is less than log segment size. We cannot rely
                # on the number of segments downloaded, as chunked read registers each chunk as one segment, and a
                # number of chunks may be downloaded to query an offset in a segment. The sum of bytes downloaded
                # through chunks must be less than the segment size.
                cloud_metrics.expect([
                    ("vectorized_cloud_storage_bytes_received_total",
                     diff_bytes)
                ])

                cloud_metrics.expect([(
                    "vectorized_cloud_storage_read_path_chunks_hydrated_total",
                    diff_chunks)])

            if is_redpanda and not cloud_storage and not batch_cache and e.expect_read:
                # Expect to read at most one segment from disk: this validates that
                # we are correctly looking up the right segment before seeking to
                # the exact offset, and not e.g. reading from the start of the log.
                local_metrics.expect([
                    ("vectorized_storage_log_read_bytes_total", diff_bytes)
                ])

            hit_offsets.add(o)

        # TODO: do some double-restarts to generate segments with
        # no user data, to check that their timestamps don't
        # break out indexing.

    def _test_timequery_below_start_offset(self, cluster):
        """
        Run a timequery for an offset that falls below the start offset
        of the local log and ensure that -1 (i.e. not found) is returned.
        """
        total_segments = 3
        local_retain_segments = 1
        record_size = 1024
        base_ts = 1664453149000
        msg_count = (self.log_segment_size * total_segments) // record_size

        topic, timestamps = self._create_and_produce(cluster, False,
                                                     local_retain_segments,
                                                     base_ts, record_size,
                                                     msg_count)

        self.client().alter_topic_config(
            topic.name, 'retention.bytes',
            self.log_segment_size * local_retain_segments)

        rpk = RpkTool(cluster)

        def start_offset():
            return next(rpk.describe_topic(topic.name)).start_offset

        wait_until(lambda: start_offset() > 0,
                   timeout_sec=120,
                   backoff_sec=5,
                   err_msg="Start offset did not advance")

        kcat = KafkaCat(cluster)
        lwm_before = start_offset()
        offset = kcat.query_offset(topic.name, 0, base_ts)
        lwm_after = start_offset()

        # When querying before the start of the log, we should get
        # the offset of the start of the log.
        assert offset >= 0
        # The LWM can move during background housekeeping while we
        # are doing timequery, so success condition is that if falls
        # within a range.
        assert offset >= lwm_before
        assert offset <= lwm_after


class TimeQueryTest(RedpandaTest, BaseTimeQuery):
    # We use small segments to enable quickly exercising the
    # lookup of the proper segment for a time index, as well
    # as the lookup of the offset within that segment.
    log_segment_size = 1024 * 1024
    chunk_size = 1024 * 128

    def setUp(self):
        # Don't start up redpanda yet, because we will need the
        # test parameter to set cluster configs before starting.
        pass

    def set_up_cluster(self, cloud_storage: bool, batch_cache: bool,
                       spillover: bool):
        self.redpanda.set_extra_rp_conf({
            # Testing with batch cache disabled is important, because otherwise
            # we won't touch the path in skipping_consumer that applies
            # timestamp bounds
            'disable_batch_cache':
            not batch_cache,

            # Our time bounds on segment removal depend on the leadership
            # staying in one place.
            'enable_leader_balancer':
            False,
            'log_segment_size_min':
            32 * 1024,
            'cloud_storage_cache_chunk_size':
            self.chunk_size,

            # Avoid configuring spillover so tests can do it themselves.
            'cloud_storage_spillover_manifest_size':
            None,

            # Disable time-based retention so that we can use synthetic
            # timestamps that may fall outside of the default retention window.
            'log_retention_ms':
            -1
        })

        if cloud_storage:
            si_settings = SISettings(
                self.test_context,
                cloud_storage_max_connections=5,
                log_segment_size=self.log_segment_size,

                # Off by default: test is parametrized to turn
                # on if SI is wanted.
                cloud_storage_enable_remote_read=True,
                cloud_storage_enable_remote_write=True,
            )
            if spillover:
                # Enable spillover with a low limit so that we can test
                # timequery fetching from spillover manifest.
                si_settings.cloud_storage_spillover_manifest_max_segments = 2
                si_settings.cloud_storage_housekeeping_interval_ms = 1000
            self.redpanda.set_si_settings(si_settings)
        else:
            self.redpanda.add_extra_rp_conf(
                {'log_segment_size': self.log_segment_size})

        self.redpanda.start()

    def _do_test_timequery(self, cloud_storage: bool, batch_cache: bool,
                           spillover: bool):
        self.set_up_cluster(cloud_storage, batch_cache, spillover)
        self._test_timequery(cluster=self.redpanda,
                             cloud_storage=cloud_storage,
                             batch_cache=batch_cache)
        if spillover:
            # Check that we are actually using the spillover manifest
            def check():
                try:
                    bucket = BucketView(self.redpanda)
                    res = bucket.get_spillover_metadata(
                        ntp=NTP(ns="kafka", topic="tqtopic", partition=0))
                    return res is not None and len(res) > 0
                except:
                    return False

            wait_until(check,
                       timeout_sec=120,
                       backoff_sec=5,
                       err_msg="Spillover use is not detected")

    @cluster(num_nodes=4)
    @parametrize(cloud_storage=True, batch_cache=False, spillover=False)
    @parametrize(cloud_storage=True, batch_cache=False, spillover=True)
    @parametrize(cloud_storage=False, batch_cache=True, spillover=False)
    @parametrize(cloud_storage=False, batch_cache=False, spillover=False)
    def test_timequery(self, cloud_storage: bool, batch_cache: bool,
                       spillover: bool):
        self._do_test_timequery(cloud_storage, batch_cache, spillover)

    @cluster(num_nodes=4)
    @parametrize(spillover=False)
    @parametrize(spillover=True)
    def test_timequery_below_start_offset(self, spillover: bool):
        self.set_up_cluster(cloud_storage=False,
                            batch_cache=False,
                            spillover=spillover)
        self._test_timequery_below_start_offset(cluster=self.redpanda)

    @cluster(num_nodes=4)
    def test_timequery_with_local_gc(self):
        # Reduce the segment size so we generate more segments and are more
        # likely to race timequeries with GC.
        self.log_segment_size = int(self.log_segment_size / 32)
        total_segments = 32 * 12
        self.set_up_cluster(cloud_storage=True,
                            batch_cache=False,
                            spillover=False)
        local_retention = self.log_segment_size * 4
        record_size = 1024
        base_ts = 1664453149000
        msg_count = (self.log_segment_size * total_segments) // record_size

        topic, timestamps = self._create_and_produce(self.redpanda, True,
                                                     local_retention, base_ts,
                                                     record_size, msg_count)

        # While waiting for local GC to occur, run several concurrent
        # timequeries all across the keyspace at once.
        num_threads = 4
        num_offsets_per_thread = int(msg_count / num_threads)
        errors = [0 for _ in range(num_threads)]
        should_stop = threading.Event()

        failed_offsets = set()

        def check_offset(kcat, o):
            expected_offset = o
            ts = timestamps[o]
            offset = kcat.query_offset(topic.name, 0, ts)
            if expected_offset != offset:
                self.logger.exception(
                    f"Timestamp {ts} returned {offset} instead of {expected_offset}"
                )
                return True
            else:
                return False

        def query_slices(tid):
            kcat = KafkaCat(self.redpanda)
            while not should_stop.is_set():
                start_idx = tid * num_offsets_per_thread
                end_idx = start_idx + num_offsets_per_thread
                # Step 100 offsets at a time so we only end up with a few dozen
                # queries per thread at a time.
                for idx in range(start_idx, end_idx, 100):
                    if check_offset(kcat, idx):
                        failed_offsets.add(idx)
                        errors[tid] += 1

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=num_threads) as executor:
            try:
                # Evaluate the futures with list().
                executor.map(query_slices, range(num_threads))
                wait_for_local_storage_truncate(redpanda=self.redpanda,
                                                topic=topic.name,
                                                target_bytes=local_retention)
            finally:
                should_stop.set()

        # Re-issue queries the failed offsets: this tells us if the error
        # was transient, and if it happens again it gives us a cleaner
        # log to analyze compared with the concurrent operations above.
        # A transient failure is still a failure, but it's interesting
        # to know that it was transient when investigating the bug.
        if failed_offsets:
            self.logger.info("Re-issuing queries on failed offsets...")
            kcat = KafkaCat(self.redpanda)
            for o in failed_offsets:
                if check_offset(kcat, o):
                    self.logger.info(f"Reproducible failure at {o}")
                else:
                    self.logger.info(f"Query at {o} succeeded on retry")

        assert not any([e > 0 for e in errors])

    @cluster(num_nodes=4)
    @parametrize(cloud_storage=True, spillover=False)
    @parametrize(cloud_storage=True, spillover=True)
    @parametrize(cloud_storage=False, spillover=False)
    def test_timequery_with_trim_prefix(self, cloud_storage: bool,
                                        spillover: bool):
        self.set_up_cluster(cloud_storage=cloud_storage,
                            batch_cache=False,
                            spillover=spillover)
        total_segments = 12
        record_size = 1024
        base_ts = 1664453149000
        msg_count = (self.log_segment_size * total_segments) // record_size
        local_retention = self.log_segment_size * 4
        topic, timestamps = self._create_and_produce(self.redpanda, True,
                                                     local_retention, base_ts,
                                                     record_size, msg_count)

        # Confirm messages written
        rpk = RpkTool(self.redpanda)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        if cloud_storage:
            # If using cloud storage, we must wait for some segments
            # to fall out of local storage, to ensure we are really
            # hitting the cloud storage read path when querying.
            wait_for_local_storage_truncate(redpanda=self.redpanda,
                                            topic=topic.name,
                                            target_bytes=local_retention)

        num_batches_per_segment = self.log_segment_size // record_size
        new_lwm = int(num_batches_per_segment * 2.5)
        trim_response = rpk.trim_prefix(topic.name,
                                        offset=new_lwm,
                                        partitions=[0])
        assert len(trim_response) == 1
        assert new_lwm == trim_response[0].new_start_offset

        # Double check that the start offset has advanced.
        p = next(rpk.describe_topic(topic.name))
        assert new_lwm == p.start_offset, f"Expected {new_lwm}, got {p.start_offset}"

        # Query below valid timestamps the offset of the first message.
        kcat = KafkaCat(self.redpanda)
        offset = kcat.query_offset(topic.name, 0, timestamps[0] - 1000)
        assert offset == new_lwm, f"Expected {new_lwm}, got {offset}"

        # Leave just the last message in the log.
        trim_response = rpk.trim_prefix(topic.name,
                                        offset=p.high_watermark - 1,
                                        partitions=[0])

        # Query below valid timestamps the offset of the only message left.
        # This is an edge-case where tiered storage, if in use, becomes
        # completely irrelevant.
        kcat = KafkaCat(self.redpanda)
        offset = kcat.query_offset(topic.name, 0, timestamps[0] - 1000)
        assert offset == msg_count - 1, f"Expected {msg_count - 1}, got {offset}"

        # Trim everything, leaving an empty log.
        rpk.trim_prefix(topic.name, offset=p.high_watermark, partitions=[0])
        kcat = KafkaCat(self.redpanda)
        offset = kcat.query_offset(topic.name, 0, timestamps[0] - 1000)
        assert offset == -1, f"Expected -1, got {offset}"

    @cluster(
        num_nodes=4,
        log_allow_list=["Failed to upload spillover manifest {timed_out}"])
    def test_timequery_with_spillover_gc_delayed(self):
        self.set_up_cluster(cloud_storage=True,
                            batch_cache=False,
                            spillover=True)
        total_segments = 16
        record_size = 1024
        base_ts = 1664453149000
        msg_count = (self.log_segment_size * total_segments) // record_size
        local_retention = self.log_segment_size * 4
        topic_retention = self.log_segment_size * 8
        topic, timestamps = self._create_and_produce(self.redpanda, True,
                                                     local_retention, base_ts,
                                                     record_size, msg_count)

        # Confirm messages written
        rpk = RpkTool(self.redpanda)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        # If using cloud storage, we must wait for some segments
        # to fall out of local storage, to ensure we are really
        # hitting the cloud storage read path when querying.
        wait_for_local_storage_truncate(redpanda=self.redpanda,
                                        topic=topic.name,
                                        target_bytes=local_retention)

        # Set timeout to 0 to prevent the cloud storage housekeeping from
        # running, triggering gc, and advancing clean offset.
        self.redpanda.set_cluster_config(
            {"cloud_storage_manifest_upload_timeout_ms": 0})
        # Disable internal scrubbing as it won't be able to make progress.
        self.si_settings.skip_end_of_test_scrubbing = True

        self.client().alter_topic_config(topic.name, 'retention.bytes',
                                         topic_retention)
        self.logger.info("Waiting for start offset to advance...")
        start_offset = wait_until_result(
            lambda: next(rpk.describe_topic(topic.name)).start_offset > 0,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Start offset did not advance")

        start_offset = next(rpk.describe_topic(topic.name)).start_offset

        # Query below valid timestamps the offset of the first message.
        kcat = KafkaCat(self.redpanda)

        test_cases = [
            (timestamps[0] - 1000, start_offset, "before start of log"),
            (timestamps[0], start_offset,
             "first message but out of retention now"),
            (timestamps[start_offset - 1], start_offset,
             "before new HWM, out of retention"),
            (timestamps[start_offset], start_offset, "new HWM"),
            (timestamps[start_offset + 10], start_offset + 10,
             "few messages after new HWM"),
            (timestamps[msg_count - 1] + 1000, -1, "after last message"),
        ]

        # Basic time query cases.
        for ts, expected_offset, desc in test_cases:
            self.logger.info(f"Querying ts={ts} ({desc})")
            offset = kcat.query_offset(topic.name, 0, ts)
            self.logger.info(f"Time query returned offset {offset}")
            assert offset == expected_offset, f"Expected {expected_offset}, got {offset}"

        # Now check every single one of them to make sure there are no
        # off-by-one errors, iterators aren't getting stuck on segment and
        # spillover boundaries, etc. The segment boundaries are not exact
        # due to internal messages, segment roll logic, etc. but the tolerance
        # should cover that.
        boundary_ranges = []
        for i in range(1, total_segments):
            boundary_ranges.append(
                (int(i * self.log_segment_size / record_size - 100),
                 int(i * self.log_segment_size / record_size + 100)))

        for r in boundary_ranges:
            self.logger.debug(f"Checking range {r}")
            for o in range(int(r[0]), int(r[1])):
                ts = timestamps[o]
                self.logger.debug(f"  Querying ts={ts}")
                offset = kcat.query_offset(topic.name, 0, ts)
                if o < start_offset:
                    assert offset == start_offset, f"Expected {start_offset}, got {offset}"
                else:
                    assert offset == o, f"Expected {o}, got {offset}"

    @cluster(num_nodes=4)
    def test_timequery_empty_local_log(self):
        self.set_up_cluster(cloud_storage=True,
                            batch_cache=False,
                            spillover=False)

        total_segments = 3
        record_size = 1024
        base_ts = 1664453149000
        msg_count = (self.log_segment_size * total_segments) // record_size
        local_retention = 1  # Any value works for this test.
        topic, timestamps = self._create_and_produce(self.redpanda, True,
                                                     local_retention, base_ts,
                                                     record_size, msg_count)

        # Confirm messages written
        rpk = RpkTool(self.redpanda)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        # Restart the cluster to force segment roll. The newly created segment
        # will have no user data which is what we want to test.
        self.redpanda.restart_nodes(self.redpanda.nodes)
        wait_until(lambda: len(list(rpk.describe_topic(topic.name))) > 0,
                   30,
                   backoff_sec=2)

        wait_until(
            lambda: next(segments_count(self.redpanda, topic.name, 0)) == 1,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Expected only one segment to be present")

        kcat = KafkaCat(self.redpanda)

        # Query below valid timestamps the offset of the first message.
        offset = kcat.query_offset(topic.name, 0, timestamps[0] - 1000)
        assert offset == 0, f"Expected 0, got {offset}"

        # Query with a timestamp in-between cloud log and the configuration
        # batch present in the local log.
        offset = kcat.query_offset(topic.name, 0,
                                   timestamps[msg_count - 1] + 1000)
        assert offset == -1, f"Expected -1, got {offset}"

        # Query with a timestamp in the future.
        offset = kcat.query_offset(
            topic.name, 0,
            int(time.time() + datetime.timedelta(days=1).total_seconds()) *
            1000)
        assert offset == -1, f"Expected -1, got {offset}"


class TimeQueryKafkaTest(Test, BaseTimeQuery):
    """
    Time queries are one of the less clearly defined aspects of the
    Kafka protocol, so we run our test procedure against Apache Kafka
    to establish a baseline behavior to ensure our compatibility.
    """
    log_segment_size = 1024 * 1024

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.zk = ZookeeperService(self.test_context,
                                   num_nodes=1,
                                   version=V_3_0_0)

        self.kafka = KafkaServiceAdapter(
            self.test_context,
            KafkaService(self.test_context,
                         num_nodes=3,
                         zk=self.zk,
                         version=V_3_0_0))

        self._client = DefaultClient(self.kafka)

    def client(self):
        return self._client

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        # ducktape handle service teardown automatically, but it is hard
        # to tell what went wrong if one of the services hangs.  Do it
        # explicitly here with some logging, to enable debugging issues
        # like https://github.com/redpanda-data/redpanda/issues/4270

        self.logger.info("Stopping Kafka...")
        self.kafka.stop()

        self.logger.info("Stopping zookeeper...")
        self.zk.stop()

    @ducktape_cluster(num_nodes=5)
    def test_timequery(self):
        self._test_timequery(cluster=self.kafka,
                             cloud_storage=False,
                             batch_cache=True)

    @ducktape_cluster(num_nodes=5)
    def test_timequery_below_start_offset(self):
        self._test_timequery_below_start_offset(cluster=self.kafka)


class TestReadReplicaTimeQuery(RedpandaTest):
    """Test time queries with read-replica topic."""

    log_segment_size = 1024 * 1024
    topic_name = "panda-topic"

    def __init__(self, test_context):
        super(TestReadReplicaTimeQuery, self).__init__(
            test_context=test_context,
            si_settings=SISettings(
                test_context,
                log_segment_size=TestReadReplicaTimeQuery.log_segment_size,
                cloud_storage_segment_max_upload_interval_sec=5))

        self.rr_settings = SISettings(
            test_context,
            bypass_bucket_creation=True,
            cloud_storage_readreplica_manifest_sync_timeout_ms=500)

        self.rr_cluster = None

    def start_read_replica_cluster(self, num_brokers) -> None:
        # NOTE: the RRR cluster won't have a bucket, so don't upload.
        extra_rp_conf = dict(enable_cluster_metadata_upload_loop=False)
        self.rr_cluster = make_redpanda_service(self.test_context,
                                                num_brokers=num_brokers,
                                                si_settings=self.rr_settings,
                                                extra_rp_conf=extra_rp_conf)
        self.rr_cluster.start(start_si=False)

    def create_read_replica_topic(self) -> None:
        try:
            rpk_rr_cluster = RpkTool(self.rr_cluster)
            conf = {
                'redpanda.remote.readreplica':
                self.si_settings.cloud_storage_bucket,
            }
            rpk_rr_cluster.create_topic(self.topic_name, config=conf)
        except:
            self.logger.warn(f"Failed to create a read-replica topic")
            return False
        return True

    def setup_clusters(self,
                       base_ts,
                       num_messages=0,
                       partition_count=1) -> None:

        spec = TopicSpec(name=self.topic_name,
                         partition_count=partition_count,
                         redpanda_remote_write=True,
                         replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)

        producer = KgoVerifierProducer(context=self.test_context,
                                       redpanda=self.redpanda,
                                       topic=self.topic_name,
                                       msg_size=1024,
                                       msg_count=num_messages,
                                       batch_max_bytes=10240,
                                       fake_timestamp_ms=base_ts)

        producer.start()
        producer.wait()

        self.start_read_replica_cluster(num_brokers=3)

        # wait until the read replica topic creation succeeds
        wait_until(self.create_read_replica_topic,
                   timeout_sec=300,
                   backoff_sec=5,
                   err_msg="Read replica topic is not created")

    def query_timestamp(self, ts, kcat_src, kcat_rr):
        self.logger.info(f"Querying ts={ts}")
        offset_src = kcat_src.query_offset(self.topic_name, 0, ts)
        offset_rr = kcat_rr.query_offset(self.topic_name, 0, ts)
        self.logger.info(
            f"Time query {ts} expected offset {offset_src}, read-replica {offset_rr}"
        )
        matches = offset_src == offset_rr
        if not matches:
            try:
                record = kcat_src.consume_one(self.topic_name, 0, offset_src)
                self.logger.info(
                    f"src cluster record at {offset_src}: {record}")
            except:
                pass
            try:
                record = kcat_rr.consume_one(self.topic_name, 0, offset_rr)
                self.logger.info(f"rr cluster record at {offset_rr}: {record}")
            except:
                pass
        assert matches, f"Expected offset {offset_src}, got {offset_rr}"

    @ducktape_cluster(num_nodes=7)
    def test_timequery(self):
        base_ts = 1664453149000
        num_messages = 1000
        self.setup_clusters(base_ts, num_messages, 3)

        def read_replica_ready():
            orig = RpkTool(self.redpanda).describe_topic(self.topic_name)
            repl = RpkTool(self.rr_cluster).describe_topic(self.topic_name)
            for o, r in zip(orig, repl):
                if o.high_watermark > r.high_watermark:
                    return False
            return True

        wait_until(read_replica_ready,
                   timeout_sec=200,
                   backoff_sec=3,
                   err_msg="Read replica is not ready")
        kcat1 = KafkaCat(self.redpanda)
        kcat2 = KafkaCat(self.rr_cluster)
        for ix in range(0, num_messages, 20):
            self.query_timestamp(base_ts + ix, kcat1, kcat2)
