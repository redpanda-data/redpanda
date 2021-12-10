# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from ducktape.mark import parametrize

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.util import (
    Scale,
    produce_until_segments,
    wait_for_segments_removal,
)


class FetchAfterDeleteTest(RedpandaTest):

    topics = (TopicSpec(partition_count=1,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        self.segment_size = 1048576
        super(FetchAfterDeleteTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf={
                                 "log_compaction_interval_ms": 5000,
                                 "log_segment_size": self.segment_size,
                                 "enable_leader_balancer": False,
                             })

    def setUp(self):
        # Override parent's setUp so that we can start redpanda later
        pass

    @cluster(num_nodes=3)
    @parametrize(transactions_enabled=True)
    @parametrize(transactions_enabled=False)
    def test_fetch_after_committed_offset_was_removed(self,
                                                      transactions_enabled):
        """
        Test fetching when consumer offset was deleted by retention
        """

        self.redpanda._extra_rp_conf[
            "enable_transactions"] = transactions_enabled
        self.redpanda._extra_rp_conf[
            "enable_idempotence"] = transactions_enabled
        self.redpanda.start()

        kafka_tools = KafkaCliTools(self.redpanda)

        # produce until segments have been compacted
        produce_until_segments(
            self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )
        consumer_group = 'test'
        rpk = RpkTool(self.redpanda)

        def consume(n=1):

            out = rpk.consume(self.topic, group=consumer_group, n=n)
            split = out.split('}')
            split = filter(lambda s: "{" in s, split)

            return map(lambda s: json.loads(s + "}"), split)

        #consume from the beggining
        msgs = consume(10)
        last = list(msgs).pop()
        offset = last['offset']

        # change retention time
        kafka_tools.alter_topic_config(
            self.topic, {
                TopicSpec.PROPERTY_RETENTION_BYTES: 2 * self.segment_size,
            })

        wait_for_segments_removal(self.redpanda,
                                  self.topic,
                                  partition_idx=0,
                                  count=5)

        partitions = list(rpk.describe_topic(self.topic))
        p = partitions[0]
        assert p.start_offset > offset
        # consume from the offset that doesn't exists,
        # the one that was committed previously was already removed
        out = list(consume(1))
        assert out[0]['offset'] == p.start_offset
