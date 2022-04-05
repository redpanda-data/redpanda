# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.util import (
    produce_until_segments,
    wait_for_segments_removal,
)


class OffsetForLeaderEpochArchivalTest(RedpandaTest):
    """
    Check offset for leader epoch handling
    """
    segment_size = 1 * (2 << 20)

    def _produce(self, topic, msg_cnt):
        rpk = RpkTool(self.redpanda)
        for i in range(0, msg_cnt):
            rpk.produce(topic, f"k-{i}", f"v-{i}")

    def __init__(self, test_context):
        super(OffsetForLeaderEpochArchivalTest, self).__init__(
            test_context=test_context,
            extra_rp_conf={
                'enable_leader_balancer': False,
                "log_compaction_interval_ms": 1000
            },
            si_settings=SISettings(
                log_segment_size=OffsetForLeaderEpochArchivalTest.segment_size,
                cloud_storage_cache_size=5 *
                OffsetForLeaderEpochArchivalTest.segment_size))

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_querying_remote_partitions(self):
        topic = TopicSpec(redpanda_remote_read=True,
                          redpanda_remote_write=True)
        epoch_offsets = {}
        rpk = RpkTool(self.redpanda)
        self.client().create_topic(topic)
        rpk.alter_topic_config(topic.name, "redpanda.remote.read", 'true')
        rpk.alter_topic_config(topic.name, "redpanda.remote.write", 'true')

        def wait_for_topic():
            wait_until(lambda: len(list(rpk.describe_topic(topic.name))) > 0,
                       30,
                       backoff_sec=2)

        # restart whole cluster 6 times to trigger term rolls
        for i in range(0, 6):
            wait_for_topic()
            produce_until_segments(
                redpanda=self.redpanda,
                topic=topic.name,
                partition_idx=0,
                count=2 * i,
            )
            res = list(rpk.describe_topic(topic.name))
            epoch_offsets[res[0].leader_epoch] = res[0].high_watermark
            self.redpanda.restart_nodes(self.redpanda.nodes)

        self.logger.info(f"ledear epoch high watermarks: {epoch_offsets}")

        wait_for_topic()

        rpk.alter_topic_config(topic.name, TopicSpec.PROPERTY_RETENTION_BYTES,
                               OffsetForLeaderEpochArchivalTest.segment_size)
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=topic.name,
                                  partition_idx=0,
                                  count=7)
        kcl = KCL(self.redpanda)

        for epoch, offset in epoch_offsets.items():
            self.logger.info(f"querying partition epoch {epoch} end offsets")
            epoch_end_offset = kcl.offset_for_leader_epoch(
                topics=topic.name, leader_epoch=epoch)[0].epoch_end_offset
            self.logger.info(
                f"epoch {epoch} end_offset: {epoch_end_offset}, expected offset: {offset}"
            )
            assert epoch_end_offset == offset
