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

    @cluster(num_nodes=1)
    def test_create_topic_configurations_cluster_defaults(self):
        topic_name = "tapioca"
        TopicAndClusterConf = namedtuple('TopicAndClusterConf', [
            'delete_retention_ms', 'cloud_storage_remote_read',
            'cloud_storage_remote_write', 'valid'
        ])
        # We cannot enable delete.retention.ms if any cloud storage properties are also enabled.
        test_cases = [
            TopicAndClusterConf(delete_retention_ms=None,
                                cloud_storage_remote_read='false',
                                cloud_storage_remote_write='false',
                                valid=True),
            TopicAndClusterConf(delete_retention_ms=None,
                                cloud_storage_remote_read='true',
                                cloud_storage_remote_write='false',
                                valid=True),
            TopicAndClusterConf(delete_retention_ms=None,
                                cloud_storage_remote_read='false',
                                cloud_storage_remote_write='true',
                                valid=True),
            TopicAndClusterConf(delete_retention_ms=None,
                                cloud_storage_remote_read='true',
                                cloud_storage_remote_write='true',
                                valid=True),
            TopicAndClusterConf(delete_retention_ms=1000,
                                cloud_storage_remote_read='false',
                                cloud_storage_remote_write='false',
                                valid=True),
            TopicAndClusterConf(delete_retention_ms=1000,
                                cloud_storage_remote_read='true',
                                cloud_storage_remote_write='false',
                                valid=False),
            TopicAndClusterConf(delete_retention_ms=1000,
                                cloud_storage_remote_read='false',
                                cloud_storage_remote_write='true',
                                valid=False),
            TopicAndClusterConf(delete_retention_ms=1000,
                                cloud_storage_remote_read='true',
                                cloud_storage_remote_write='true',
                                valid=False),
        ]
        for test_case in test_cases:
            self.rpk.cluster_config_set('cloud_storage_enable_remote_read',
                                        test_case.cloud_storage_remote_read)
            self.rpk.cluster_config_set('cloud_storage_enable_remote_write',
                                        test_case.cloud_storage_remote_write)

            # Sanity check cluster defaults after setting them.
            assert self.rpk.cluster_config_get(
                'cloud_storage_enable_remote_read'
            ) == test_case.cloud_storage_remote_read

            assert self.rpk.cluster_config_get(
                'cloud_storage_enable_remote_write'
            ) == test_case.cloud_storage_remote_write

            config = {}
            if test_case.delete_retention_ms is not None:
                config = {'delete.retention.ms': test_case.delete_retention_ms}
                expected_delete_retention_ms = str(
                    test_case.delete_retention_ms)
            else:
                expected_delete_retention_ms = '-1'

            if test_case.valid:
                # Expect a successful topic creation.
                self.rpk.create_topic(topic_name, partitions=1, config=config)
                topic_desc = self.rpk.describe_topic_configs(topic_name)
                assert topic_desc['delete.retention.ms'][
                    0] == expected_delete_retention_ms
                # Delete the topic so that we can try again with the next test case.
                self.rpk.delete_topic(topic_name)
            else:
                # Expect a failure due to misconfiguration.
                with expect_exception(
                        RpkException, lambda e:
                        'Unsupported delete.retention.ms configuration, cannot be enabled at the same time as redpanda.remote.read or redpanda.remote.write.'
                        in str(e)):
                    self.rpk.create_topic(topic_name,
                                          partitions=1,
                                          config=config)

    @cluster(num_nodes=1)
    def test_create_topic_configurations(self):
        topic_name = "tapioca"
        TopicConf = namedtuple(
            'TopicConf',
            ['delete_retention_ms', 'remote_read', 'remote_write', 'valid'])
        # We cannot enable delete.retention.ms if any cloud storage properties are also enabled.
        test_cases = [
            TopicConf(delete_retention_ms=None,
                      remote_read='false',
                      remote_write='false',
                      valid=True),
            TopicConf(delete_retention_ms=None,
                      remote_read='true',
                      remote_write='false',
                      valid=True),
            TopicConf(delete_retention_ms=None,
                      remote_read='false',
                      remote_write='true',
                      valid=True),
            TopicConf(delete_retention_ms=None,
                      remote_read='true',
                      remote_write='true',
                      valid=True),
            TopicConf(delete_retention_ms=1000,
                      remote_read='false',
                      remote_write='false',
                      valid=True),
            TopicConf(delete_retention_ms=1000,
                      remote_read='true',
                      remote_write='false',
                      valid=False),
            TopicConf(delete_retention_ms=1000,
                      remote_read='false',
                      remote_write='true',
                      valid=False),
            TopicConf(delete_retention_ms=1000,
                      remote_read='true',
                      remote_write='true',
                      valid=False),
        ]
        for test_case in test_cases:
            config = {
                'redpanda.remote.read': test_case.remote_read,
                'redpanda.remote.write': test_case.remote_write
            }

            if test_case.delete_retention_ms is not None:
                config['delete.retention.ms'] = test_case.delete_retention_ms
                expected_delete_retention_ms = str(
                    test_case.delete_retention_ms)
            else:
                expected_delete_retention_ms = '-1'

            if test_case.valid:
                # Expect a successful topic creation.
                self.rpk.create_topic(topic_name, partitions=1, config=config)
                topic_desc = self.rpk.describe_topic_configs(topic_name)
                assert topic_desc['delete.retention.ms'][
                    0] == expected_delete_retention_ms
                # Delete the topic so that we can try again with the next test case.
                self.rpk.delete_topic(topic_name)
            else:
                # Expect a failure due to misconfiguration.
                with expect_exception(
                        RpkException, lambda e:
                        'Unsupported delete.retention.ms configuration, cannot be enabled at the same time as redpanda.remote.read or redpanda.remote.write.'
                        in str(e)):
                    self.rpk.create_topic(topic_name,
                                          partitions=1,
                                          config=config)
