# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer


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
