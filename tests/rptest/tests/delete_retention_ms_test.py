# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from collections import namedtuple
from ducktape.mark import matrix, defaults
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


class DeleteRetentionMsTest(RedpandaTest):
    def __init__(self, ctx):
        self.ctx = ctx
        super().__init__(ctx)

        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=1)
    def test_get_cluster_config(self):
        # Default value
        cluster_default = self.rpk.cluster_config_get('tombstone_retention_ms')
        assert cluster_default == 'null'

    @cluster(num_nodes=1)
    def test_set_cluster_config(self):
        self.rpk.cluster_config_set('tombstone_retention_ms', 1234567890)
        cluster_prop = self.rpk.cluster_config_get('tombstone_retention_ms')
        assert cluster_prop == "1234567890"

        topic_name = "tapioca"
        topic = TopicSpec(name=topic_name)
        self.rpk.create_topic(topic_name, partitions=1)
        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['delete.retention.ms'][0] == '1234567890'

    @cluster(num_nodes=1)
    def test_alter_topic_config(self):
        topic_name = "tapioca"
        topic = TopicSpec(name=topic_name)
        self.rpk.create_topic(topic_name, partitions=1)
        topic_desc = self.rpk.describe_topic_configs(topic_name)

        # Upon topic construction, this property is actually "disabled" by default.
        assert topic_desc['delete.retention.ms'][0] == '-1'
        assert topic_desc['delete.retention.ms'][1] == 'DEFAULT_CONFIG'

        # Set value
        self.rpk.alter_topic_config(topic_name, 'delete.retention.ms',
                                    1234567890)
        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['delete.retention.ms'][0] == '1234567890'
        assert topic_desc['delete.retention.ms'][1] == 'DYNAMIC_TOPIC_CONFIG'

        # Set cluster value
        self.rpk.cluster_config_set('tombstone_retention_ms', 100)

        # Assert topic property and source haven't changed.
        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['delete.retention.ms'][0] == '1234567890'
        assert topic_desc['delete.retention.ms'][1] == 'DYNAMIC_TOPIC_CONFIG'

        # Delete topic config
        self.rpk.delete_topic_config(topic_name, 'delete.retention.ms')
        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['delete.retention.ms'][0] == '100'
        assert topic_desc['delete.retention.ms'][1] == 'DEFAULT_CONFIG'

        # Disable topic value
        self.rpk.alter_topic_config(topic_name, 'delete.retention.ms', -1)
        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['delete.retention.ms'][0] == '-1'
        assert topic_desc['delete.retention.ms'][1] == 'DYNAMIC_TOPIC_CONFIG'

        # Set empty cluster value and delete topic config
        self.rpk.cluster_config_set('tombstone_retention_ms', "")
        self.rpk.delete_topic_config(topic_name, 'delete.retention.ms')
        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['delete.retention.ms'][0] == '-1'
        assert topic_desc['delete.retention.ms'][1] == 'DEFAULT_CONFIG'

        # Disable topic config
        self.rpk.delete_topic_config(topic_name, 'delete.retention.ms')
        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['delete.retention.ms'][0] == '-1'
        assert topic_desc['delete.retention.ms'][1] == 'DEFAULT_CONFIG'
