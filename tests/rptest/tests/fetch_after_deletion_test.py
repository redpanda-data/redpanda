# Copyright 2020 Redpanda Data, Inc.
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
    produce_until_segments,
    wait_for_local_storage_truncate,
)


class FetchAfterDeleteTest(RedpandaTest):
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

        topic = TopicSpec(partition_count=1,
                          replication_factor=3,
                          cleanup_policy=TopicSpec.CLEANUP_DELETE)
        self.client().create_topic(topic)

        kafka_tools = KafkaCliTools(self.redpanda)

        # produce until segments have been compacted
        produce_until_segments(
            self.redpanda,
            topic=topic.name,
            partition_idx=0,
            count=10,
        )
        consumer_group = 'test'
        rpk = RpkTool(self.redpanda)

        def consume(n=1):

            out = rpk.consume(topic.name, group=consumer_group, n=n)
            split = out.split('}')
            split = filter(lambda s: "{" in s, split)

            return map(lambda s: json.loads(s + "}"), split)

        #consume from the beggining
        msgs = consume(10)
        last = list(msgs).pop()
        offset = last['offset']

        # change retention time
        retention_bytes = 2 * self.segment_size
        kafka_tools.alter_topic_config(
            topic.name, {
                TopicSpec.PROPERTY_RETENTION_BYTES: retention_bytes,
            })

        wait_for_local_storage_truncate(self.redpanda,
                                        topic.name,
                                        target_bytes=retention_bytes)

        partitions = list(rpk.describe_topic(topic.name))
        p = partitions[0]
        assert p.start_offset > offset
        # consume from the offset that doesn't exists,
        # the one that was committed previously was already removed
        out = list(consume(1))
        assert out[0]['offset'] == p.start_offset
