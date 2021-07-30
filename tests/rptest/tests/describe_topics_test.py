# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


class DescribeTopicsTest(RedpandaTest):
    topics = (TopicSpec(partition_count=2, replication_factor=3), )

    @cluster(num_nodes=3)
    def test_describe_topics(self):
        tools = KafkaCliTools(self.redpanda)
        output = tools.describe_topics()
        assert "partition_count=2" in output
        assert "replication_factor=3" in output
