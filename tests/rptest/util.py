# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


from ducktape.utils.util import wait_until


class Scale:
    KEY = "scale"
    LOCAL = "local"
    CI = "ci"
    RELEASE = "release"
    DEFAULT = LOCAL
    SCALES = (LOCAL, CI, RELEASE)

    def __init__(self, context):
        self._scale = context.globals.get(Scale.KEY,
                                          Scale.DEFAULT).strip().lower()

        if self._scale not in Scale.SCALES:
            raise RuntimeError(
                f"Invalid scale {self._scale}. Available: {Scale.SCALES}")

    def __str__(self):
        return self._scale

    @property
    def local(self):
        return self._scale == Scale.LOCAL

    @property
    def ci(self):
        return self._scale == Scale.CI

    @property
    def release(self):
        return self._scale == Scale.RELEASE


def _segments_count(storage, topic, partition_idx):
    topic_partitions = storage.partitions("kafka", topic)

    return map(
        lambda p: len(p.segments),
        filter(lambda p: p.num == partition_idx, topic_partitions),
    )


def produce_until_segments(storage, kafka_tools, topic, partition_idx, count, acks=-1):
    """
    Produce into the topic until given number of segments will appear
    """

    def done():
        kafka_tools.produce(topic, 10000, 1024, acks=acks)
        topic_partitions = _segments_count(
            storage=storage,
            topic=topic,
            partition_idx=partition_idx,
        )
        partitions = []
        for p in topic_partitions:
            partitions.append(p >= count)
        return all(partitions)

    wait_until(
        done, timeout_sec=120, backoff_sec=2, err_msg="Segments were not created"
    )


def wait_for_segments_removal(storage, topic, partition_idx, count):
    """
    Wait until only given number of segments will left in a partitions
    """

    def done():
        topic_partitions = _segments_count(storage, topic, partition_idx)
        partitions = []
        for p in topic_partitions:
            partitions.append(p <= count)
        return all(partitions)

    wait_until(
        done, timeout_sec=120, backoff_sec=5, err_msg="Segments were not removed"
    )
