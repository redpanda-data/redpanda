# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkException, RpkTool
from rptest.services import redpanda
from rptest.services.admin import Admin
from rptest.services.cluster import cluster

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
import string

from rptest.util import wait_until


class LimitsTest(RedpandaTest):
    def _rand_string(sz):
        return ''.join(
            random.choice(string.ascii_letters + string.digits)
            for _ in range(sz))

    @cluster(num_nodes=3)
    def test_kafka_request_max_size(self):
        topic = TopicSpec(partition_count=1)
        # create topic
        DefaultClient(self.redpanda).create_topic(topic)
        rpk = RpkTool(self.redpanda)
        # produce 512KB message
        rpk.produce(topic=topic.name,
                    key="test",
                    msg=LimitsTest._rand_string(512 * 1024))
        # set request max bytes to 50KB
        new_limit = 50 * 1024
        rpk.cluster_config_set("kafka_request_max_bytes", str(new_limit))

        def produce_error():
            try:
                rpk.produce(topic=topic.name,
                            key="test",
                            msg=LimitsTest._rand_string(512 * 1024))
            except RpkException as e:
                return "timed out" in e.msg

            return False

        wait_until(produce_error, 30, 1)
