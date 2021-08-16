# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


class CreatePartitionsTest(RedpandaTest):
    topics = (TopicSpec(partition_count=2, replication_factor=3), )

    @cluster(num_nodes=3)
    def test_create_partitions(self):
        topic = self.topics[0].name
        cli = KafkaCliTools(self.redpanda)
        # add 5 partitions to the topic
        cli.add_topic_partitions(topic, 5)

        res = cli.describe_topic(topic)
        # initially topic had 2 partitions, we added 5
        assert res.partition_count == 7
