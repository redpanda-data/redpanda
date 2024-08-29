import typing

from ducktape.mark import matrix, defaults
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kafka_cli_consumer import KafkaCliConsumer

from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception


class TombstoneRetentionMsTest(RedpandaTest):
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
        assert topic_desc['tombstone.retention.ms'][0] == '1234567890'

    @cluster(num_nodes=1)
    def test_alter_topic_config(self):
        topic_name = "tapioca"
        topic = TopicSpec(name=topic_name)
        self.rpk.create_topic(topic_name, partitions=1)
        topic_desc = self.rpk.describe_topic_configs(topic_name)

        # Default value
        assert topic_desc['tombstone.retention.ms'][0] == '-1'

        self.rpk.alter_topic_config(topic_name, 'tombstone.retention.ms',
                                    1234567890)

        topic_desc = self.rpk.describe_topic_configs(topic_name)
        assert topic_desc['tombstone.retention.ms'][0] == '1234567890'

    @cluster(num_nodes=1)
    @matrix(tombstone_retention_ms=[None, 1000],
            cloud_storage_remote_read=[False, True],
            cloud_storage_remote_write=[False, True])
    def test_create_topic_configurations(self, tombstone_retention_ms,
                                         cloud_storage_remote_read,
                                         cloud_storage_remote_write):
        topic_name = "tapioca"
        #We cannot enable tombstone_retention_ms if any cloud storage properties are also enabled.
        is_valid = False if any([
            cloud_storage_remote_read, cloud_storage_remote_write
        ]) and tombstone_retention_ms is not None else True
        self.rpk.cluster_config_set('cloud_storage_enable_remote_read',
                                    cloud_storage_remote_read)
        self.rpk.cluster_config_set('cloud_storage_enable_remote_write',
                                    cloud_storage_remote_write)

        topic = TopicSpec(name=topic_name)
        config = {}
        if tombstone_retention_ms is not None:
            config = {'tombstone.retention.ms': tombstone_retention_ms}
        else:
            tombstone_retention_ms = -1

        if is_valid:
            # Expect a successful topic creation.
            self.rpk.create_topic(topic_name, partitions=1, config=config)
            topic_desc = self.rpk.describe_topic_configs(topic_name)
            print(topic_desc['tombstone.retention.ms'])
            assert topic_desc['tombstone.retention.ms'][0] == str(
                tombstone_retention_ms)
            assert is_valid == True
        else:
            # Expect a failure due to misconfiguration.
            with expect_exception(
                    RpkException, lambda e:
                    'Unsupported tombstone_retention_ms configuration, cannot be enabled at the same time as repanda.remote.read or redpanda.remote.write.'
                    in str(e)):
                self.rpk.create_topic(topic_name, partitions=1, config=config)
