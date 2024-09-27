# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures
import threading
import time

from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings, MetricsEndpoint, ResourceSettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.si_utils import quiesce_uploads


class TieredStorageReaderStressTest(RedpandaTest):
    segment_upload_interval = 10
    manifest_upload_interval = 1
    expect_throughput = 50 * 1024 * 1024
    runtime_grace_factor = 2.0

    # Make the cache big enough to definitely accomodate disk-saturating throughput,  to avoid
    # artificially limiting the rate at which we create segment readers and fill their read
    # buffers: we want maximum memory stress, not readers waiting on cache space.
    cache_max_throughput = 10 * expect_throughput
    cache_size = SISettings.cache_size_for_throughput(cache_max_throughput)

    # This is the same as the default at time of writing (v23.2)
    readers_per_shard = 1000

    # To help reduce runtime by requiring less data to get a given segment count
    segment_size = 16 * 1024 * 1024
    chunk_size = 1 * 1024 * 1024

    def __init__(self, test_context, *args, **kwargs):
        si_settings = SISettings(test_context=test_context,
                                 log_segment_size=self.segment_size,
                                 cloud_storage_cache_size=self.cache_size)
        extra_rp_conf = {
            "cloud_storage_cache_chunk_size": self.chunk_size,
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval,
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval,
            'cloud_storage_max_segment_readers_per_shard':
            self.readers_per_shard
        }
        super().__init__(test_context,
                         *args,
                         si_settings=si_settings,
                         extra_rp_conf=extra_rp_conf,
                         **kwargs)

        # Artificially limit CPUs, so that we concentrate readers on a small, deterministic
        # number of cores, and have relatively limited memory.  The memory is within the official system
        # requirements (2GB per core).
        self.redpanda.set_resource_settings(
            ResourceSettings(memory_mb=4096, num_cpus=2))

    def _produce_and_quiesce(self, topic_name: str, msg_size: int,
                             data_size: int, expect_bandwidth: float,
                             **kwargs):
        expect_runtime = max(60.0, (data_size / expect_bandwidth) * 2)

        t1 = time.time()
        timeout = expect_runtime * self.runtime_grace_factor

        self.logger.info(f"Producing {data_size} bytes, timeout = {timeout}")
        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    topic_name,
                                    msg_size=msg_size,
                                    msg_count=data_size // msg_size,
                                    batch_max_bytes=msg_size * 8,
                                    timeout_sec=timeout,
                                    **kwargs)
        produce_duration = time.time() - t1
        self.logger.info(
            f"Produced {data_size} bytes in {produce_duration} seconds, {(data_size / produce_duration) / 1000000.0:.2f}MB/s"
        )

        quiesce_uploads(
            self.redpanda, [topic_name],
            self.manifest_upload_interval + self.segment_upload_interval + 30)

    def _create_topic(self, topic_name: str, partition_count: int,
                      local_retention: int):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                'retention.local.target.bytes': local_retention,
                # This test uses synthetic timestamps in the past, so
                # disable time based retention to avoid it deleting our
                # "old" data immediately.
                'retention.ms': -1
            })

    def _get_stats(self):
        """The stats we care about for reader stress, especially
        reader count and kafka connection count"""

        results = {}
        for node in self.redpanda.nodes:
            metrics = self.redpanda.metrics(
                node, metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
            segment_reader_count = 0
            partition_reader_count = 0
            partition_reader_delay_count = 0
            segment_reader_delay_count = 0
            materialize_segment_delay_count = 0
            connection_count = 0
            hydrations_count = 0
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "redpanda_cloud_storage_readers":
                        segment_reader_count += int(sample.value)
                    if sample.name == "redpanda_cloud_storage_partition_readers":
                        partition_reader_count += int(sample.value)
                    if sample.name == "redpanda_cloud_storage_partition_readers_delayed_total":
                        partition_reader_delay_count += int(sample.value)
                    if sample.name == "redpanda_cloud_storage_segment_readers_delayed_total":
                        segment_reader_delay_count += int(sample.value)
                    if sample.name == "redpanda_cloud_storage_segment_materializations_delayed_total":
                        materialize_segment_delay_count += int(sample.value)
                    elif sample.name == "redpanda_rpc_active_connections":
                        if sample.labels["redpanda_server"] == "kafka":
                            connection_count += int(sample.value)
            for family in self.redpanda.metrics(
                    node, metrics_endpoint=MetricsEndpoint.METRICS):
                for sample in family.samples:
                    if sample.name == "vectorized_cloud_storage_read_path_hydrations_in_progress_total":
                        hydrations_count += int(sample.value)

            stats = {
                "segment_readers": segment_reader_count,
                "segment_readers_delayed": segment_reader_delay_count,
                "partition_readers": partition_reader_count,
                "partition_readers_delayed": partition_reader_delay_count,
                "materialize_segments_delayed":
                materialize_segment_delay_count,
                "connections": connection_count,
                "hydrations_count": hydrations_count
            }
            self.logger.debug(f"stats[{node.name}] = {stats}")
            results[node] = stats

        return results

    @cluster(num_nodes=4)
    def reader_stress_test(self):
        """
        Validate that when Kafka RPCs request far more concurrent readers than we can provide:
        - we do not leak kafka client connections from long-waiting readers whose
          originating kafka rpc has long since timed out on the client side
        - reader limits are respected to within a reasonable bound
        - the system stays up.
        - that more reasonable reads are still serviced eventually, after we stop hammering
          the cluster with unreasonable reads.

        The easiest way to bury a redpanda cluster in tiered storage reads is to issue
        N time queries across M partitions, where each time query happens to target a
        different data chunk.
        """

        # Use interval uploads so that tests can do a "wait til everything uploaded"
        # check if they want to.
        topic_name = "reader-stress"

        concurrent_timequeries = 32
        total_timequeries = concurrent_timequeries * 16
        partition_count = 128
        msg_size = 16384
        data_size = max(
            # Write enough segments that most of the data falls into remote storage
            self.segment_size * partition_count * 4,
            # Write enough data that it won't all remain in cache, so that under stress
            # we are continually promoting/trimming data for maximum disk and memory stress
            self.cache_size * 4)
        msg_count = data_size // msg_size

        # We will use synthetic timestamps, to keep the test somewhat deterministic when we
        # select timequery points evenly spaced through the range
        base_fake_ts = 1688562373356
        max_fake_ts = base_fake_ts + msg_count - 1

        peak_readers = partition_count * concurrent_timequeries
        peak_readers_per_shard = peak_readers // (len(
            (self.redpanda.nodes) * self.redpanda.get_node_cpu_count()))

        self._create_topic(topic_name, partition_count, self.segment_size)
        self._produce_and_quiesce(topic_name,
                                  msg_size,
                                  data_size,
                                  self.expect_throughput,
                                  fake_timestamp_ms=base_fake_ts)

        rpk = RpkTool(self.redpanda)

        client_timeout = 5

        def tq_at(ts):
            self.logger.debug(f"starting timequery: {ts}")
            out = rpk.consume(topic_name,
                              n=1,
                              offset=f"@{ts}",
                              format="%p,%o",
                              timeout=5)
            self.logger.debug(f"completed timequery: {ts} -> {out}")

        stats_watcher_stop = threading.Event()
        stats_hwms = {}

        def stats_watcher():
            while not stats_watcher_stop.is_set():
                stats = self._get_stats()
                for node, node_stats in stats.items():
                    hwms = stats_hwms.get(node, node_stats)
                    for k, v in node_stats.items():
                        if v > hwms[k]:
                            hwms[k] = v
                    stats_hwms[node] = hwms

                stats_watcher_stop.wait(0.5)

        # We are running clients locally on the test runner machine
        self.logger.info(
            f"Spawning timequery {total_timequeries} client tasks with {concurrent_timequeries}-way concurrency"
        )
        any_failed = False

        # How long we expect it to  take for all tasks to execute
        task_wait_timeout = (total_timequeries // concurrent_timequeries
                             ) * client_timeout * self.runtime_grace_factor
        task_initial_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=concurrent_timequeries + 1) as executor:
            watcher_job = executor.submit(stats_watcher)
            jobs = []
            step = msg_count // concurrent_timequeries

            # k is the counter through rounds of concurrent timequery tasks
            k_iterations = total_timequeries // concurrent_timequeries

            for k in range(0, k_iterations):
                for i in range(0, concurrent_timequeries):
                    ts = base_fake_ts + step * i + k * (step // k_iterations)
                    jobs.append((k, ts, executor.submit(tq_at, ts)))

            for (k, ts, j) in jobs:
                try:
                    # Throw and fail the test if our overall runtime for all tasks
                    # has expired.
                    timeout = task_wait_timeout - (time.time() -
                                                   task_initial_time)
                    j.result(timeout=timeout)
                except RpkException as e:
                    # We expect this: timequeries will time out
                    self.logger.info(
                        f"timequery task {k}-@{ts} on overloaded cluster failed: {e}"
                    )
                    any_failed = True
                else:
                    self.logger.info(f"timequery task {k}-@{ts} succeeded")

            stats_watcher_stop.set()
            watcher_job.result()

        self.logger.info("Finished timequery client tasks")

        assert any_failed, "Expected some timequeries to fail due to overload"

        for node, hwms in stats_hwms.items():
            self.logger.info(f"hwms[{node.name}]: {hwms}")

            assert hwms['connections'] <= total_timequeries
            assert hwms[
                'partition_readers'] <= self.readers_per_shard * self.redpanda.get_node_cpu_count(
                )
            assert hwms[
                'segment_readers'] <= self.readers_per_shard * self.redpanda.get_node_cpu_count(
                )

        # TODO: assert on reader HWM once we enforce it more strongly

        stats = self._get_stats()
        for node, stats in stats.items():
            # The stats indicate saturation with tiered storage reads.
            assert stats['partition_readers_delayed'] or stats[
                'segment_readers_delayed']

        def is_metric_zero(fn, label):
            for node, stats in self._get_stats().items():
                if metric := fn(stats):
                    self.logger.debug(
                        f"Node {node.name} still has {metric} {label}")
                    return False
            return True

        """
        Active chunk hydrations block remote segment readers during reader creation,
        which in turn keep kafka connections open. A connection can only be closed
        once the download finishes, so we wait for active downloads to finish first.
        """
        self.logger.info("Waiting for active hydrations to finish")
        self.redpanda.wait_until(
            lambda: is_metric_zero(lambda s: s['hydrations_count'],
                                   'active hydrations'),
            timeout_sec=15,
            backoff_sec=1,
            err_msg="Waiting for active hydrations to finish")

        # Once downloads are done, connections should be closed very shortly after.
        self.logger.info("Waiting for all Kafka connections to close")
        self.redpanda.wait_until(
            lambda: is_metric_zero(lambda s: s["connections"],
                                   "kafka connections"),
            timeout_sec=3,
            backoff_sec=1,
            err_msg="Waiting for Kafka connections to close")

        # No new downloads should be started once all reader activity is done
        for _ in range(10):
            time.sleep(0.5)
            assert is_metric_zero(
                lambda s: s['hydrations_count'], 'active hydrations'
            ), 'found an active hydration where none expected'
