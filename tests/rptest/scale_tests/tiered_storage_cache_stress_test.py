# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import sys
from dataclasses import dataclass

from rptest.services.metrics_check import MetricCheck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer, KgoVerifierRandomConsumer
from rptest.services.redpanda import SISettings, MetricsEndpoint, ResourceSettings
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.utils.si_utils import quiesce_uploads
from typing import Optional
from ducktape.mark import matrix
import time
from enum import Enum

S3_ERROR_LOGS = ['unexpected REST API error "Internal Server Error" detected']


class LimitMode(str, Enum):
    bytes = 'bytes'
    objects = 'objects'
    both = 'both'


@dataclass
class CacheExpectations:
    tracker_size: int = 0
    min_mem_trims: int = 0
    max_mem_trims: int = sys.maxsize
    min_fast_trims: int = 0
    max_fast_trims: int = sys.maxsize
    min_exhaustive_trims: int = 0
    max_exhaustive_trims: int = sys.maxsize
    num_syncs: int = 0


class TieredStorageCacheStressTest(RedpandaTest):
    segment_upload_interval = 30
    manifest_upload_interval = 10

    # Anywhere we calculate an expected runtime based on an expected bandwidth,
    # use this grace factor to make timeouts reasonably permissive.
    runtime_grace_factor = 4.0

    def __init__(self, test_context, *args, **kwargs):

        super().__init__(test_context, *args, **kwargs)

    def setUp(self):
        # Use interval uploads so that tests can do a "wait til everything uploaded"
        # check if they want to.
        extra_rp_conf = {
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval,
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval,
            'disable_public_metrics': False,
            'cloud_storage_cache_check_interval': 500,
        }
        self.redpanda.set_extra_rp_conf(extra_rp_conf)

        # We will run with artificially constrained memory, to minimize use of
        # the batch cache and ensure that tiered storage reads are not using
        # egregious amounts of memory.  The memory is within the official system
        # requirements (2GB per core).  Use a small core count so that if we're
        # running on a system with plenty of cores, we don't spread out the
        # partitions such that each core has lots of slack memory.
        self.redpanda.set_resource_settings(
            ResourceSettings(memory_mb=4096, num_cpus=2))

        # defer redpanda startup to the test
        pass

    def _validate_node_storage(self, node, limit_mode: LimitMode,
                               size_limit: Optional[int],
                               max_objects: Optional[int]):
        admin = Admin(self.redpanda)

        self.logger.info(
            f"Validating node {node.name} cache vs limits {size_limit}/{max_objects}"
        )

        # Read logical space usage according to Redpanda
        usage = admin.get_local_storage_usage(node)
        self.logger.info(f"Checking cache usage on {node.name}: {usage}")

        # Read physical space usage according to the operating system
        node_storage = self.redpanda.node_storage(node)
        self.logger.info(
            f"Checked physical cache usage on {node.name}: {node_storage.cache}"
        )

        # Read HWM stats from Redpanda, in case we transiently violated cache size
        metrics = self.redpanda.metrics(
            node, metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        hwm_size = None
        hwm_objects = None
        for family in metrics:
            for sample in family.samples:
                if sample.name == "redpanda_cloud_storage_cache_space_hwm_size_bytes":
                    hwm_size = int(sample.value)
                elif sample.name == "redpanda_cloud_storage_cache_space_hwm_files":
                    hwm_objects = int(sample.value)
        assert hwm_size is not None, "Cache HWM metric not found"

        any_cache_usage = (usage['cloud_storage_cache_bytes'] > 0
                           and usage['cloud_storage_cache_objects'] > 0)

        self.logger.info(
            f"Admin API cache size[{node.name}]: {usage['cloud_storage_cache_bytes']}/{usage['cloud_storage_cache_objects']}"
        )
        self.logger.info(
            f"Physical cache usage[{node.name}]: {node_storage.cache.bytes}/{node_storage.cache.objects}"
        )
        self.logger.info(f"Cache HWM [{node.name}]: {hwm_size}/{hwm_objects}")

        if limit_mode == LimitMode.bytes or limit_mode == LimitMode.both:
            assert size_limit is not None
            assert usage['cloud_storage_cache_bytes'] <= size_limit
            assert node_storage.cache.bytes <= size_limit
            assert hwm_size <= size_limit
        elif limit_mode == LimitMode.objects or limit_mode == LimitMode.both:
            assert max_objects is not None
            assert usage['cloud_storage_cache_objects'] <= max_objects
            assert node_storage.cache.objects <= max_objects
            assert hwm_objects <= max_objects
        else:
            raise NotImplementedError(limit_mode)

        return any_cache_usage

    def _create_topic(self, topic_name: str, partition_count: int,
                      local_retention: int):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic_name,
                         partitions=partition_count,
                         replicas=3,
                         config={
                             'retention.local.target.bytes': local_retention,
                         })

    def _produce_and_quiesce(self, topic_name: str, msg_size: int,
                             data_size: int, expect_bandwidth: float):
        expect_runtime = max(60.0, (data_size / expect_bandwidth) * 2)

        t1 = time.time()
        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    topic_name,
                                    msg_size=msg_size,
                                    msg_count=data_size // msg_size,
                                    batch_max_bytes=msg_size * 8,
                                    timeout_sec=expect_runtime *
                                    self.runtime_grace_factor)
        produce_duration = time.time() - t1
        self.logger.info(
            f"Produced {data_size} bytes in {produce_duration} seconds, {(data_size/produce_duration)/1000000.0:.2f}MB/s"
        )

        quiesce_uploads(
            self.redpanda, [topic_name],
            self.manifest_upload_interval + self.segment_upload_interval + 30)

    @cluster(num_nodes=4, log_allow_list=S3_ERROR_LOGS)
    @matrix(limit_mode=[LimitMode.bytes, LimitMode.objects, LimitMode.both],
            log_segment_size=[1024 * 1024, 128 * 1024 * 1024])
    def streaming_cache_test(self, limit_mode, log_segment_size):
        """
        Validate that reading data much  larger than the cache proceeds safely
        and promptly: this exercises the mode where we are reading some data,
        trimming cache, reading, trimming etc.
        """

        chunk_size = min(16 * 1024 * 1024, log_segment_size)

        if self.redpanda.dedicated_nodes:
            partition_count = 128

            # Lowball expectation of bandwidth that should work reliably on
            # small instance types
            expect_bandwidth = int(100E6)

            # Make the cache at least this big (avoid using very small caches when
            # the segment size is small)
            at_least_bytes = 20 * 1024 * 1024 * 1024
        else:
            # In general, this test should always be run on dedicated nodes: mini-me mode
            # for developers on fast workstations hacking on the test.
            partition_count = 16
            expect_bandwidth = int(20E6)
            at_least_bytes = 1 * 1024 * 1024 * 1024

        topic_name = 'streaming-read'

        msg_size = 16384

        # Cache trim interval is 5 seconds.  Cache trim low watermark is 80%.
        # Effective streaming bandwidth is 20% of cache size every trim period
        size_limit = max(
            SISettings.cache_size_for_throughput(expect_bandwidth),
            partition_count * chunk_size)
        size_limit = max(size_limit, at_least_bytes)
        # One index per segment, one tx file per segment, one object per chunk bytes
        max_objects = (size_limit //
                       log_segment_size) * 2 + size_limit // chunk_size

        if partition_count >= len(self.redpanda.nodes):
            # If we have multiple partitions then we are spreading load across multiple
            # nodes, and each node should need a proportionally smaller cache
            size_limit = size_limit // len(self.redpanda.nodes)

        si_settings = SISettings(test_context=self.test_context,
                                 log_segment_size=log_segment_size)

        if limit_mode == LimitMode.bytes:
            si_settings.cloud_storage_cache_size = int(size_limit)
            si_settings.cloud_storage_cache_max_objects = 2**32 - 1
        elif limit_mode == LimitMode.objects:
            si_settings.cloud_storage_cache_size = 2**64 - 1
            si_settings.cloud_storage_cache_max_objects = int(max_objects)
        elif limit_mode == LimitMode.both:
            # This is the normal way of configuring Redpanda, with
            # safety limits on both data and metadata
            si_settings.cloud_storage_cache_size = int(size_limit)
            si_settings.cloud_storage_cache_max_objects = int(max_objects)
        else:
            raise NotImplementedError(limit_mode)

        # Write 5x more data than fits in the cache, so that during a consume
        # cycle we are having to drop+rehydrate it all.
        data_size = size_limit * 5

        # Write 3x more than one segment size, so that we will be reading from
        # remote data instead of local data (local retention set to one segment)
        data_size = max(data_size, log_segment_size * 3 * partition_count)

        msg_count = data_size // msg_size

        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start()

        # Minimal local retention, to send traffic to remote
        # storage.
        self._create_topic(topic_name, partition_count, log_segment_size)

        self.logger.info(
            f"Writing {data_size} bytes, will be read using {size_limit}/{max_objects} cache"
        )

        # Sanity check test parameters against the nodes we are running on
        disk_space_required = size_limit + data_size
        assert self.redpanda.get_node_disk_free(
        ) >= disk_space_required, f"Need at least {disk_space_required} bytes space"

        # Write out the data.  We will write + read in separate phases in order
        # that this test cleanly exercises the read path.
        self._produce_and_quiesce(topic_name, msg_size, data_size,
                                  expect_bandwidth)

        # Read all the data, validate that we read complete and achieve
        # the streaming bandwidth that we expect
        t1 = time.time()
        expect_duration = data_size // expect_bandwidth
        self.logger.info(
            f"Consuming, expected duration {expect_duration:.2f}s")
        consumer = KgoVerifierSeqConsumer.oneshot(self.test_context,
                                                  self.redpanda,
                                                  topic_name,
                                                  loop=False,
                                                  timeout_sec=expect_duration *
                                                  2)
        assert consumer.consumer_status.validator.valid_reads == msg_count
        assert consumer.consumer_status.validator.invalid_reads == 0
        assert consumer.consumer_status.validator.out_of_scope_invalid_reads == 0
        consume_duration = time.time() - t1
        consume_rate = data_size / consume_duration
        self.logger.info(
            f"Consumed {data_size} bytes in {consume_duration} seconds, {consume_rate/1000000.0:.2f}MB/s"
        )

        # If we are not keeping up, it indicates an issue with trimming logic, such as
        # backing off too much or not trimming enough each time: there is a generous
        # 2x margin to make the test robust: if this _still_ fails, something is up.
        assert consume_rate > expect_bandwidth / 2

        # Validate that the cache end state is within configured limit
        nodes_cache_used = self.redpanda.for_nodes(
            self.redpanda.nodes, lambda n: self._validate_node_storage(
                n, limit_mode, size_limit, max_objects))

        # At least one node should have _something_ in its cache, or something is wrong with our test
        assert any(nodes_cache_used) is True

    @cluster(num_nodes=4, log_allow_list=S3_ERROR_LOGS)
    def tiny_cache_test(self):
        """
        Verify stability and eventual progress when running with an absurdly small cache.

        This is an unrealistic situation, aimed at shaking out edge cases in the cache
        trimming logic that might only occur when the cache size is <= the order of
        magnitude as data in flight.

        When the cache is under this much pressure, usual ordering of put/eviction
        of chunks vs. indices is violated, so we are exercising that trimming works
        for indices even if they outlive their associated chunks, etc.
        """

        topic_name = 'tiny-cache'
        msg_size = 16384
        partition_count = 1

        consume_parallel = 20
        consume_messages_per_worker = 5

        # Lowball expectation of bandwidth that should work reliably on small instance types
        expect_bandwidth = 100E6

        segment_size = 32 * 1024 * 1024
        chunk_size = min(16 * 1024 * 1024, segment_size)

        # Cache only large enough for each worker to have one chunk at a time
        cache_size = chunk_size * consume_parallel

        # We will write data far larger than the cache.
        data_size = segment_size * 100

        si_settings = SISettings(test_context=self.test_context,
                                 log_segment_size=segment_size,
                                 cloud_storage_cache_size=cache_size)
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start()

        # Inject data
        # Minimal local retention, to send traffic to remote storage.
        self._create_topic(topic_name, partition_count, segment_size)
        self._produce_and_quiesce(topic_name, msg_size, data_size,
                                  expect_bandwidth)

        # Use a random consumer to pathologically overload the ability of the cache to
        # handle incoming reads.
        # This should be slow, but:
        #  - Eventually complete
        #  - Do not overshoot cache size limits

        # Runtime will be as long as it takes to cycle each read chunk through
        # the cache, multiplied by the trim throttle interval (5s)
        expect_consume_time_s = self.runtime_grace_factor * (
            (consume_parallel * consume_messages_per_worker * 5) /
            (cache_size / chunk_size))

        t1 = time.time()
        KgoVerifierRandomConsumer.oneshot(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size=msg_size,
            rand_read_msgs=consume_messages_per_worker,
            parallel=consume_parallel,
            timeout_sec=expect_consume_time_s)
        random_consume_duration = time.time() - t1
        self.logger.info(
            f"Random consume finished  in {random_consume_duration} seconds")

        for node in self.redpanda.nodes:
            self._validate_node_storage(node,
                                        LimitMode.bytes,
                                        cache_size,
                                        max_objects=None)

    def run_test_with_cache_prefilled(self, cache_prefill_command: str,
                                      prefill_count: int,
                                      expectations: CacheExpectations):
        segment_size = 128 * 1024 * 1024
        msg_size = 16384
        data_size = segment_size * 10
        topic_name = "exhaustive-trim"
        expect_bandwidth = 100 * 1024 * 1024

        # Clean manually, so that later we can start(clean_nodes=False) to preserve
        # our injected garbage files.
        for n in self.redpanda.nodes:
            self.redpanda.clean_node(n)

        # Pre-populate caches with files.
        for node in self.redpanda.nodes:
            node.account.ssh(cache_prefill_command.format(prefill_count))

        cache_object_limit = prefill_count // 2

        # Set cache size to 50 objects
        si_settings = SISettings(
            test_context=self.test_context,
            log_segment_size=segment_size,
            cloud_storage_cache_max_objects=cache_object_limit,
            cloud_storage_cache_size=SISettings.cache_size_for_throughput(
                expect_bandwidth))

        # Bring up redpanda

        self.redpanda.set_si_settings(si_settings)

        self.redpanda.start(clean_nodes=False)

        # Cache startup should have registered the garbage objects in stats
        admin = Admin(self.redpanda)
        for node in self.redpanda.nodes:
            usage = admin.get_local_storage_usage(node)
            assert usage[
                'cloud_storage_cache_objects'] >= prefill_count, \
                (f"Node {node.name} has unexpectedly few objects "
                 f"{usage['cloud_storage_cache_objects']} < {prefill_count}")

        # Inject data
        self._create_topic(topic_name, 1, segment_size)

        trim_metrics = [
            'redpanda_cloud_storage_cache_trim_in_mem_trims_total',
            'redpanda_cloud_storage_cache_trim_fast_trims_total',
            'redpanda_cloud_storage_cache_trim_exhaustive_trims_total',
            'redpanda_cloud_storage_cache_space_tracker_syncs_total',
            'redpanda_cloud_storage_cache_space_tracker_size'
        ]
        m = MetricCheck(self.redpanda.logger,
                        self.redpanda,
                        self.redpanda.partitions(topic_name)[0].leader,
                        trim_metrics,
                        metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)

        m.expect([('redpanda_cloud_storage_cache_space_tracker_syncs_total',
                   lambda a, b: a == expectations.num_syncs == b)])

        m.expect([('redpanda_cloud_storage_cache_space_tracker_size',
                   lambda a, b: a == expectations.tracker_size == b)])

        self._produce_and_quiesce(topic_name, msg_size, data_size,
                                  expect_bandwidth)

        # Try to do some reads
        KgoVerifierRandomConsumer.oneshot(self.test_context,
                                          self.redpanda,
                                          topic_name,
                                          msg_size=msg_size,
                                          rand_read_msgs=4,
                                          parallel=4,
                                          timeout_sec=30)

        admin = Admin(self.redpanda)
        leader_node = self.redpanda.get_node_by_id(
            admin.get_partition_leader(namespace="kafka",
                                       topic=topic_name,
                                       partition=0))

        # Assert that an exhaustive trim happened, such that the garbage files
        # reduce in number to the point that the read is able to proceed
        usage = admin.get_local_storage_usage(leader_node)
        assert usage[
            'cloud_storage_cache_objects'] <= cache_object_limit, \
            (f"Node {leader_node.name} has unexpectedly many objects "
             f"{usage['cloud_storage_cache_objects']} > {cache_object_limit}")

        m.expect([('redpanda_cloud_storage_cache_trim_in_mem_trims_total',
                   lambda a, b: a == 0 and expectations.max_mem_trims >= b >=
                   expectations.min_mem_trims)])
        m.expect([('redpanda_cloud_storage_cache_trim_fast_trims_total',
                   lambda a, b: a == 0 and expectations.max_fast_trims >= b >=
                   expectations.min_fast_trims)])
        m.expect([('redpanda_cloud_storage_cache_trim_exhaustive_trims_total',
                   lambda a, b: a == 0 and expectations.max_exhaustive_trims >=
                   b >= expectations.min_exhaustive_trims)])

    @cluster(num_nodes=4, log_allow_list=S3_ERROR_LOGS)
    def garbage_objects_test(self):
        """
        Verify that if there are a large number of small files which do not pair
        with data chunks, we still trim them when cache space is low.

        This test is a reproducer for issues where the cache needs trimming but there
        are no data objects present to cue the fast trim process to delete indices etc,
        and we must fall back to exhaustive trim, such as:
        https://github.com/redpanda-data/redpanda/issues/11835
        """

        prefill_count = 100
        expectations = CacheExpectations(
            num_syncs=1,
            # The tracker will add all non-index/tx/tmp files during startup sync
            tracker_size=prefill_count,
            # Tracker based trim should be enough to acquire space
            min_mem_trims=1,
            min_fast_trims=0,
            max_fast_trims=0,
            min_exhaustive_trims=0,
            max_exhaustive_trims=0)
        self.run_test_with_cache_prefilled(
            f"mkdir -p {self.redpanda.cache_dir} ; "
            "for n in `seq 1 {}`; do "
            f"dd if=/dev/urandom bs=1k count=4 of={self.redpanda.cache_dir}/garbage_$n.bin ; done",
            prefill_count, expectations)

    @cluster(num_nodes=4, log_allow_list=S3_ERROR_LOGS)
    def test_indices_dominate_cache(self):
        """
        Ensures that if the cache is filled with index and tx objects alone,
        trimming still works.
        """

        prefill_count = 100
        expectations = CacheExpectations(
            num_syncs=1,
            # Neither index nor tx files will be added to tracker during startup sync
            tracker_size=0,
            # Since orphan index and tx files are only cleaned up in exhaustive trim,
            # all three trims have to run to acquire space
            min_mem_trims=1,
            min_fast_trims=1,
            min_exhaustive_trims=1)
        self.run_test_with_cache_prefilled(
            f"mkdir -pv {self.redpanda.cache_dir}; "
            "for n in `seq 1 {}`; do "
            f"touch {self.redpanda.cache_dir}/garbage_$n.index && "
            f"touch {self.redpanda.cache_dir}/garbage_$n.tx; "
            "done", prefill_count, expectations)
