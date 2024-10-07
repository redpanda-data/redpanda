# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import string
import time
import subprocess
from rptest.services.admin import Admin
from rptest.clients.kcl import KCL, RawKCL
from rptest.utils.si_utils import BucketView, NT
from ducktape.utils.util import wait_until
from rptest.util import wait_until_result

from rptest.services.cluster import cluster
from ducktape.mark import parametrize, matrix
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool

from rptest.services.redpanda_installer import RedpandaVersionTriple
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.redpanda_installer import InstallOptions, RedpandaInstaller
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings


class AlterTopicConfiguration(RedpandaTest):
    """
    Change a partition's replica set.
    """
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        super(AlterTopicConfiguration,
              self).__init__(test_context=test_context, num_brokers=3)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    @cluster(num_nodes=3)
    @parametrize(property=TopicSpec.PROPERTY_CLEANUP_POLICY, value="compact")
    @parametrize(property=TopicSpec.PROPERTY_SEGMENT_SIZE,
                 value=10 * (2 << 20))
    @parametrize(property=TopicSpec.PROPERTY_RETENTION_BYTES,
                 value=200 * (2 << 20))
    @parametrize(property=TopicSpec.PROPERTY_RETENTION_TIME, value=360000)
    @parametrize(property=TopicSpec.PROPERTY_TIMESTAMP_TYPE,
                 value="LogAppendTime")
    def test_altering_topic_configuration(self, property, value):
        topic = self.topics[0].name
        self.client().alter_topic_configs(topic, {property: value})
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)

        # e.g. retention.ms is TopicSpec.retention_ms
        attr_name = property.replace(".", "_")
        assert getattr(spec, attr_name, None) == value

    @cluster(num_nodes=3)
    def test_alter_config_does_not_change_replication_factor(self):
        topic = self.topics[0].name
        # change default replication factor
        self.redpanda.set_cluster_config(
            {"default_topic_replications": str(5)})
        kcl = RawKCL(self.redpanda)
        kcl.raw_alter_topic_config(
            1, topic, {
                TopicSpec.PROPERTY_RETENTION_TIME: 360000,
                TopicSpec.PROPERTY_TIMESTAMP_TYPE: "LogAppendTime"
            })
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)

        assert spec.replication_factor == 3
        assert spec.retention_ms == 360000
        assert spec.message_timestamp_type == "LogAppendTime"

    @cluster(num_nodes=3)
    def test_altering_multiple_topic_configurations(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        self.client().alter_topic_configs(
            topic, {
                TopicSpec.PROPERTY_SEGMENT_SIZE: 1024 * 1024,
                TopicSpec.PROPERTY_RETENTION_TIME: 360000,
                TopicSpec.PROPERTY_TIMESTAMP_TYPE: "LogAppendTime"
            })
        spec = kafka_tools.describe_topic(topic)

        assert spec.segment_bytes == 1024 * 1024
        assert spec.retention_ms == 360000
        assert spec.message_timestamp_type == "LogAppendTime"

    def random_string(self, size):
        return ''.join(
            random.choice(string.ascii_uppercase + string.digits)
            for _ in range(size))

    @cluster(num_nodes=3)
    def test_set_config_from_describe(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        topic_config = kafka_tools.describe_topic_config(topic=topic)
        self.client().alter_topic_configs(topic, topic_config)

    @cluster(num_nodes=3)
    def test_configuration_properties_kafka_config_allowlist(self):
        rpk = RpkTool(self.redpanda)
        config = rpk.describe_topic_configs(self.topic)

        new_segment_bytes = int(config['segment.bytes'][0]) + 1
        self.client().alter_topic_configs(
            self.topic, {
                "unclean.leader.election.enable": True,
                TopicSpec.PROPERTY_SEGMENT_SIZE: new_segment_bytes
            })

        new_config = rpk.describe_topic_configs(self.topic)
        assert int(new_config['segment.bytes'][0]) == new_segment_bytes

    @cluster(num_nodes=3)
    def test_configuration_properties_name_validation(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)
        for _ in range(0, 5):
            key = self.random_string(5)
            try:
                self.client().alter_topic_configs(topic, {key: "123"})
            except Exception as inst:
                self.logger.info(
                    "alter failed as expected: expected exception %s", inst)
            else:
                raise RuntimeError("Alter should have failed but succeeded!")

        new_spec = kafka_tools.describe_topic(topic)
        # topic spec shouldn't change
        assert new_spec == spec

    @cluster(num_nodes=3)
    def test_segment_size_validation(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        initial_spec = kafka_tools.describe_topic(topic)
        self.redpanda.set_cluster_config({"log_segment_size_min": 1024})
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: 16})
        except subprocess.CalledProcessError as e:
            assert "is outside of allowed range" in e.output

        assert initial_spec.segment_bytes == kafka_tools.describe_topic(
            topic
        ).segment_bytes, "segment.bytes shouldn't be changed to invalid value"

        # change min segment bytes redpanda property
        self.redpanda.set_cluster_config({"log_segment_size_min": 1024 * 1024})
        # try setting value that is smaller than requested min
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: 1024 * 1024 - 1})
        except subprocess.CalledProcessError as e:
            assert "is outside of allowed range" in e.output

        assert initial_spec.segment_bytes == kafka_tools.describe_topic(
            topic
        ).segment_bytes, "segment.bytes shouldn't be changed to invalid value"
        valid_segment_size = 1024 * 1024 + 1
        self.client().alter_topic_configs(
            topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: valid_segment_size})
        assert kafka_tools.describe_topic(
            topic).segment_bytes == valid_segment_size

        self.redpanda.set_cluster_config(
            {"log_segment_size_max": 10 * 1024 * 1024})
        # try to set value greater than max allowed segment size
        try:
            self.client().alter_topic_configs(
                topic, {TopicSpec.PROPERTY_SEGMENT_SIZE: 20 * 1024 * 1024})
        except subprocess.CalledProcessError as e:
            assert "is outside of allowed range" in e.output

        # check that segment size didn't change
        assert kafka_tools.describe_topic(
            topic).segment_bytes == valid_segment_size

    @cluster(num_nodes=3)
    def test_shadow_indexing_config(self):
        topic = self.topics[0].name
        original_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"original_output={original_output}")
        assert original_output["redpanda.remote.read"] == "false"
        assert original_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read", "true")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read", "true")
        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "true")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.read", "true")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"


class ShadowIndexingGlobalConfig(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        self._extra_rp_conf = dict(cloud_storage_enable_remote_read=True,
                                   cloud_storage_enable_remote_write=True)
        si_settings = SISettings(test_context)

        super(ShadowIndexingGlobalConfig,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=self._extra_rp_conf,
                             si_settings=si_settings)

    @cluster(num_nodes=3)
    def test_overrides_set(self):
        topic = self.topics[0].name
        original_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"original_output={original_output}")
        assert original_output["redpanda.remote.read"] == "true"
        assert original_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

    @cluster(num_nodes=3)
    def test_overrides_remove(self):
        topic = self.topics[0].name
        original_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"original_output={original_output}")
        assert original_output["redpanda.remote.read"] == "true"
        assert original_output["redpanda.remote.write"] == "true"

        # disable shadow indexing for topic
        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

        # Assert cluster values are both True
        admin = Admin(self.redpanda)
        cluster_conf = admin.get_cluster_config()
        assert cluster_conf['cloud_storage_enable_remote_read'] == True
        assert cluster_conf['cloud_storage_enable_remote_write'] == True

        # delete topic configs (value from cluster configuration should be used)
        self.client().delete_topic_config(topic, "redpanda.remote.read")
        self.client().delete_topic_config(topic, "redpanda.remote.write")

        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "true"

        # Set cluster values to False
        self.redpanda.set_cluster_config({
            'cloud_storage_enable_remote_read':
            False,
            'cloud_storage_enable_remote_write':
            False
        })
        cluster_conf = admin.get_cluster_config()
        assert cluster_conf['cloud_storage_enable_remote_read'] == False
        assert cluster_conf['cloud_storage_enable_remote_write'] == False

        # delete topic configs (value from cluster configuration should be used)
        self.client().delete_topic_config(topic, "redpanda.remote.read")
        self.client().delete_topic_config(topic, "redpanda.remote.write")

        altered_output = self.client().describe_topic_configs(topic)
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

    @cluster(num_nodes=3)
    def test_topic_manifest_reupload(self):
        bucket_view = BucketView(self.redpanda)
        initial = wait_until_result(
            lambda: bucket_view.get_topic_manifest(
                NT(ns="kafka", topic=self.topic)),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Failed to fetch initial topic manifest",
            retry_on_exc=True)

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, "retention.bytes", "400")

        def check():
            bucket_view = BucketView(self.redpanda)
            manifest = bucket_view.get_topic_manifest(
                NT(ns="kafka", topic=self.topic))
            return manifest["retention_bytes"] == 400

        wait_until(check,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Topic manifest was not re-uploaded as expected",
                   retry_on_exc=True)


class AlterConfigMixedNodeTest(EndToEndTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, ctx):
        super(AlterConfigMixedNodeTest, self).__init__(test_context=ctx)

    @cluster(num_nodes=3)
    @matrix(incremental_update=[True, False])
    def test_alter_config_shadow_indexing_mixed_node(self, incremental_update):
        """Assert that the `AlterConfig` and `IncrementalAlterConfig` APIs still work as expected,
        most notably with `redpanda.remote.read` and `redpanda.remote.write`, which have seen some
        changed behavior in v24.3 and above versions of `redpanda`."""
        num_nodes = 3

        install_opts = InstallOptions(version=RedpandaVersionTriple((24, 1,
                                                                     1)))
        self.start_redpanda(
            num_nodes=num_nodes,
            si_settings=SISettings(test_context=self.test_context),
            install_opts=install_opts,
            license_required=True)

        rpk = RpkTool(self.redpanda)
        # KCL is used to direct AlterConfig and DescribeConfigs requests to specific brokers.
        kcl = KCL(self.redpanda)
        topic = self.topics[0].name

        rpk.create_topic(topic,
                         partitions=1,
                         replicas=3,
                         config={
                             'redpanda.remote.read': 'false',
                             'redpanda.remote.write': 'false'
                         })

        # Sanity check defaults
        desc = rpk.describe_topic_configs(topic)
        assert desc['redpanda.remote.read'][0] == 'false'
        assert desc['redpanda.remote.write'][0] == 'false'

        def check_consistent_properties_across_nodes(_):
            # Cannot assert on topic configs here, as we
            # know the deprecated code path is bug prone.
            # Still, check that view of topic properties
            # is consistent across the cluster.
            node_props = []
            for node in self.redpanda.nodes:
                desc = kcl.describe_topic(topic, node=node)
                props = set()
                for line in desc.split('\n'):
                    line = line.rstrip()
                    if 'redpanda.remote.read' in line:
                        props.add(line)
                    elif 'redpanda.remote.write' in line:
                        props.add(line)
                assert len(props) == 2
                node_props.append(props)
            return all(p == node_props[0] for p in node_props)

        def alter_and_check(func):
            # Make the changes to topic properties and assert they are
            # propagated across all nodes.
            props_list = [{
                'redpanda.remote.read': 'true',
                'redpanda.remote.write': 'true'
            }, {
                'redpanda.remote.read': 'false',
                'redpanda.remote.write': 'false'
            }, {
                'redpanda.remote.read': 'true',
                'redpanda.remote.write': 'false'
            }]
            for props in props_list:
                kcl.alter_topic_config(props,
                                       incremental_update,
                                       topic,
                                       node=self.redpanda.controller())
                wait_until(lambda: func(props) == True,
                           timeout_sec=10,
                           backoff_sec=1,
                           err_msg=f'Failed check {func.__name__}')

        def restart_node_and_await_stable_leader(node):
            self.redpanda.restart_nodes(node)
            self.redpanda.wait_for_membership(first_start=False)
            self.redpanda._admin.await_stable_leader(namespace='redpanda',
                                                     topic='controller',
                                                     partition=0)

            def wait_for_controller_id(n):
                health_report = self.redpanda._admin.get_cluster_health_overview(
                    n)
                return health_report['controller_id'] != -1

            for node in self.redpanda.nodes:
                wait_until(lambda: wait_for_controller_id(node) == True,
                           timeout_sec=15,
                           backoff_sec=1,
                           err_msg='Controller leadership did not stabilize.')

        def check_shadow_indexing_feature_state(nodes, state):
            nodes = nodes if isinstance(nodes, list) else [nodes]
            for node in nodes:
                assert self.redpanda.get_feature_state(
                    'shadow_indexing_split_topic_property_update',
                    node) == state

        # Perform alterations and consistency checks across cluster with all un-upgraded nodes
        alter_and_check(check_consistent_properties_across_nodes)

        # Install updated version of redpanda across all nodes.
        self.redpanda._installer.install(self.redpanda.nodes,
                                         RedpandaInstaller.HEAD)
        # Restart one node.
        restart_node_and_await_stable_leader(self.redpanda.nodes[0])

        # Assert that shadow indexing feature is unavailable for the upgraded node.
        check_shadow_indexing_feature_state(self.redpanda.nodes[0],
                                            'unavailable')

        # Perform alterations and consistency checks across cluster with only one upgraded node
        alter_and_check(check_consistent_properties_across_nodes)

        # Restart the second node.
        restart_node_and_await_stable_leader(self.redpanda.nodes[1])

        # Assert that shadow indexing feature is still unavailable for the upgraded nodes
        check_shadow_indexing_feature_state(
            [self.redpanda.nodes[0], self.redpanda.nodes[1]], 'unavailable')

        # Perform alterations and consistency checks across cluster with two upgraded nodes
        alter_and_check(check_consistent_properties_across_nodes)

        # Restart the last node.
        restart_node_and_await_stable_leader(self.redpanda.nodes[2])

        # Await the shadow indexing feature.
        self.redpanda.await_feature(
            'shadow_indexing_split_topic_property_update',
            'active',
            timeout_sec=15)

        # Assert that shadow indexing feature is now active.
        check_shadow_indexing_feature_state(self.redpanda.nodes, 'active')

        def check_remote_read_and_write_on_nodes(props):
            # We can assert on topic properties on the upgraded nodes, thanks
            # to the bug fixes in the updated code path.
            remote_read = props['redpanda.remote.read']
            remote_write = props['redpanda.remote.write']
            for node in self.redpanda.nodes:
                remote_read_valid = False
                remote_write_valid = False
                desc = kcl.describe_topic(topic, node=node)
                for line in desc.split('\n'):
                    line = line.rstrip()
                    if 'redpanda.remote.read' in line:
                        remote_read_valid = remote_read in line
                    if 'redpanda.remote.write' in line:
                        remote_write_valid = remote_write in line
                valid = remote_read_valid and remote_write_valid
                if not valid:
                    return False
            return True

        # Perform alterations and strong topic property checks across cluster with all upgraded nodes
        alter_and_check(check_remote_read_and_write_on_nodes)
