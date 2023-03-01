# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import signal

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.clients.rpk import RpkTool
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from ducktape.cluster.cluster_spec import ClusterSpec


class StorageResourceTest(RedpandaTest):
    """
    Validate that the storage system is using the expected
    resources, in terms of how many files are created and how
    many concurrent file handles are held.
    """

    SEGMENT_SIZE = 2**20

    topics = (
        TopicSpec(
            partition_count=1,
            replication_factor=1,
            # Enable compaction so that we can test the handling of compaction_index
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            segment_bytes=SEGMENT_SIZE), )

    def _get_segments(self):
        storage = self.redpanda.storage()
        partitions = storage.partitions("kafka", self.topic)
        p = list(partitions)[0]
        segments = list(p.segments.values())

        # Sort by offset, so that earliest-written segments are first
        segments = sorted(segments, key=lambda s: int(s.name.split("-")[0]))

        for s in segments:
            self.logger.info(f"Got segment: {s}")
        return segments

    def _topic_fd_count(self):
        """
        How many file descriptors is redpanda consuming for our topic?
        """
        for f in self.redpanda.lsof_node(self.redpanda.nodes[0],
                                         filter=self.topic):
            self.logger.info(f"Open file: {f}")

        return sum(1 for _ in self.redpanda.lsof_node(self.redpanda.nodes[0],
                                                      filter=self.topic))

    def _write(self, msg_size, msg_count):
        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size,
                               msg_count=msg_count,
                               acks=-1)
        producer.start()
        producer.wait()
        producer.free()

    @cluster(num_nodes=2)
    def test_files_and_fds(self):
        """
        Verify that the indices for a segment are not written out
        until they are ready (avoid wastefully opening a file
        handle prematurely)
        """

        msg_size = int(self.SEGMENT_SIZE / 8)

        # Write enough data to half fill the first segment
        self._write(msg_size, 4)

        segments = self._get_segments()
        assert len(segments) == 1
        live_seg = segments[-1]
        assert live_seg.base_index is None

        assert self._topic_fd_count() == 1
        assert live_seg.compaction_index is None

        # Produce twice to get two batches (one batch would just result in overshooting
        # the segment size limit)
        self._write(msg_size, 8)
        self._write(msg_size, 8)

        segments = self._get_segments()
        assert len(segments) >= 2
        for sealed_seg in segments[0:-1]:
            assert sealed_seg.base_index is not None
            assert sealed_seg.compaction_index is not None
        live_seg = segments[-1]
        assert live_seg.base_index is None

        # Even though there are two segments, only the latest one should
        # be open.
        assert self._topic_fd_count() == 1
        assert live_seg.compaction_index is None


class StorageResourceRestartTest(RedpandaTest):
    PARTITION_COUNT = 64
    TARGET_REPLAY_BYTES = 1024 * 1024 * 1024

    topics = (TopicSpec(
        partition_count=PARTITION_COUNT,
        replication_factor=1,
    ), )

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            extra_rp_conf={
                # Decrease from the default 10GB so that we can run the test
                # without having to write >10GB of data on slow docker environments
                'storage_target_replay_bytes': self.TARGET_REPLAY_BYTES
            },
            **kwargs)

    def _await_replay(self):
        """
        Block until all partitions are available
        """
        rpk = RpkTool(self.redpanda)

        def ready():
            partitions = list(rpk.describe_topic(self.topic))
            if len(partitions) < self.PARTITION_COUNT:
                return False
            else:
                return all(p.leader >= 0 for p in partitions)

        wait_until(ready, timeout_sec=30, backoff_sec=5)

    def _write(self, msg_size, msg_count):
        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size,
                               msg_count=msg_count,
                               acks=-1)
        producer.start()
        producer.wait()
        producer.free()

    def _read_all(self, msg_count):
        consumer = RpkConsumer(self.test_context,
                               self.redpanda,
                               self.topic,
                               offset='oldest',
                               save_msgs=False,
                               num_msgs=msg_count)
        consumer.start()
        consumer.wait()
        consumer.free()

    def _read_tips(self):
        rpk = RpkTool(self.redpanda)
        partition_meta = list(rpk.describe_topic(self.topic))
        assert len(partition_meta) == self.PARTITION_COUNT
        for p in partition_meta:
            rpk.consume(self.topic,
                        partition=p.id,
                        n=1,
                        offset=p.high_watermark - 1,
                        quiet=True)

    def _sum_metrics(self, metric_name):
        samples = self.redpanda.metrics_sample(metric_name).samples
        self.logger.info(f"{metric_name} samples:")
        for s in samples:
            self.logger.info(f"{s.value} {s.labels}")
        return sum(s.value for s in samples)

    def _get_partition_segments(self, partition_idx):
        storage = self.redpanda.storage(all_nodes=True)
        partitions = list(storage.partitions("kafka", self.topic))
        p = partitions[partition_idx]
        segments = list(p.segments.values())

        # Sort by offset, so that earliest-written segments are first
        segments = sorted(segments, key=lambda s: int(s.name.split("-")[0]))

        for s in segments:
            self.logger.info(f"Got segment: {s}")
        return segments

    @cluster(num_nodes=2)
    @parametrize(clean_shutdown=False)
    @parametrize(clean_shutdown=True)
    def test_recovery_reads(self, clean_shutdown):
        """
        Verify the amount of disk IO that occurs on both clean and
        unclean restarts.
        """

        if self.debug_mode:
            self.logger.info("Skipping in debug mode")
            # Satisfy checks for test using all its nodes
            nodes = self.test_context.cluster.alloc(
                ClusterSpec.simple_linux(1))
            self.test_context.cluster.free_single(nodes[0])
            return

        # Enough writes to exceed threshold for checkpointing
        # a single partition.  The equivalent for testing with
        # large numbers of partitions is ManyPartitionsTest, which
        # we do not run here because docker test environments can't
        # handle it.
        msg_size = 128 * 1024
        msg_count = 8 * self.PARTITION_COUNT * 32

        # We should be playing in enough traffic to trip checkpoints/snapshots
        # for meeting the target replay threshold.
        assert msg_count * msg_size > self.TARGET_REPLAY_BYTES

        # This value is hardcoded in Redpanda, the threshold for checkpointing
        # offset translator and/or state machines on each partition.
        per_partition_checkpoint_threshold = 64 * 1024 * 1024

        # We should not be playing in enough data per-partition to trip the
        # naive per-partition checkpoint limits
        assert (msg_count * msg_size /
                self.PARTITION_COUNT) < per_partition_checkpoint_threshold

        self._write(msg_size, msg_count)
        total_bytes = msg_size * msg_count

        # Use low level seastar metrics, because storage log metrics do
        # not reflect I/O during replay
        write_bytes_metric = "vectorized_io_queue_total_write_bytes_total"
        read_bytes_metric = "vectorized_io_queue_total_read_bytes_total"

        assert self._sum_metrics(write_bytes_metric) >= total_bytes

        # Stop node
        # =========
        if clean_shutdown:
            # Clean shutdown: stop with SIGTERM
            self.redpanda.stop_node(self.redpanda.nodes[0])
        else:
            # Unclean shutdown: stop with SIGKILL
            self.redpanda.signal_redpanda(self.redpanda.nodes[0],
                                          signal=signal.SIGKILL)

        # Inspect disk
        # ============
        # Just look at one partition as an example
        segments = self._get_partition_segments(0)

        assert len(segments) == 1
        if clean_shutdown:
            assert segments[0].base_index is not None
        else:
            assert segments[0].base_index is None

        # Start node
        # ==========
        self.redpanda.start_node(self.redpanda.nodes[0])
        self._await_replay()

        if clean_shutdown:
            # Clean shutdown
            # ==============
            assert self._sum_metrics(read_bytes_metric) < total_bytes

            # Reading the tip of the log should not prompt reading all history
            self._read_tips()
            assert self._sum_metrics(read_bytes_metric) < total_bytes

            # Validate metrics work as expected by explicitly reading all data
            self._read_all(msg_count)
            assert self._sum_metrics(read_bytes_metric) >= total_bytes
        else:
            # Unclean shutdown
            # ================
            # We expect the entire latest segment for all partitions to
            # be read during replay after unsafe shutdown, to rebuild the index.
            assert self._sum_metrics(read_bytes_metric) >= total_bytes

            # Check that reads still work, although we aren't interested in
            # the byte count: we have already read >= the total size of data
            # on disk.
            self._read_tips()
            self._read_all(msg_count)
