# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import string
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings

from rptest.tests.redpanda_test import RedpandaTest


class CreateTopicsTest(RedpandaTest):

    #TODO: add shadow indexing properties:
    #
    # 'redpanda.remote.write': lambda: random.choice(['true', 'false']),
    # 'redpanda.remote.read':    lambda: random.choice(['true', 'false'])
    _topic_properties = {
        'compression.type':
        lambda: random.choice(["producer", "zstd"]),
        'cleanup.policy':
        lambda: random.choice(["compact", "delete", "compact,delete"]),
        'message.timestamp.type':
        lambda: random.choice(["LogAppendTime", "CreateTime"]),
        'segment.bytes':
        lambda: random.randint(1024 * 1024, 1024 * 1024 * 1024),
        'retention.bytes':
        lambda: random.randint(1024 * 1024, 1024 * 1024 * 1024),
        'retention.ms':
        lambda: random.randint(-1, 10000000),
        'max.message.bytes':
        lambda: random.randint(1024 * 1024, 10 * 1024 * 1024),
    }

    def __init__(self, test_context):
        si_settings = SISettings(
            cloud_storage_reconciliation_interval_ms=50,
            cloud_storage_max_connections=5,
            cloud_storage_segment_max_upload_interval_sec=10,
            log_segment_size=100 * 1024 * 1024)

        super(CreateTopicsTest, self).__init__(test_context=test_context,
                                               num_brokers=3,
                                               si_settings=si_settings)

    def _topic_name(self):
        return "test-topic-" + "".join(
            random.choice(string.ascii_lowercase) for _ in range(16))

    @cluster(num_nodes=3)
    def test_create_topic_with_single_configuration_property(self):
        rpk = RpkTool(self.redpanda)

        for p, generator in CreateTopicsTest._topic_properties.items():
            name = self._topic_name()
            partitions = random.randint(1, 10)
            property_value = generator()
            rpk.create_topic(topic=name,
                             partitions=partitions,
                             replicas=3,
                             config={p: property_value})

            cfgs = rpk.describe_topic_configs(topic=name)
            assert str(cfgs[p][0]) == str(property_value)
