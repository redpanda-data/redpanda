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
import itertools
import json
from time import sleep
from rptest.clients.default import DefaultClient
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.clients.kcl import RawKCL, KclCreateTopicsRequestTopic
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda import ResourceSettings, SISettings, CloudStorageType, get_cloud_storage_type
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.services.rpk_producer import RpkProducer
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import wait_for_local_storage_truncate, expect_exception
from rptest.clients.kcl import KCL
from rptest.tests.cluster_config_test import wait_for_version_sync

from ducktape.utils.util import wait_until
from ducktape.mark import matrix, parametrize

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.offline_log_viewer import OfflineLogViewer


class Workload():
    ACKS_1 = 'ACKS_1'
    ACKS_ALL = 'ACKS_ALL'
    IDEMPOTENT = 'IDEMPOTENT'


class TopicRecreateTest(RedpandaTest):
    def __init__(self, test_context):
        super(TopicRecreateTest,
              self).__init__(test_context=test_context,
                             num_brokers=5,
                             resource_settings=ResourceSettings(num_cpus=1),
                             extra_rp_conf={
                                 "auto_create_topics_enabled": False,
                                 "max_compacted_log_segment_size":
                                 5 * (2 << 20)
                             })

    @cluster(num_nodes=6)
    @matrix(
        workload=[Workload.ACKS_1, Workload.ACKS_ALL, Workload.IDEMPOTENT],
        cleanup_policy=[TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_DELETE])
    def test_topic_recreation_while_producing(self, workload, cleanup_policy):
        '''
        Test that we are able to recreate topic multiple times
        '''
        self._client = DefaultClient(self.redpanda)

        # scaling parameters
        partition_count = 30
        producer_count = 10

        spec = TopicSpec(partition_count=partition_count, replication_factor=3)
        spec.cleanup_policy = cleanup_policy

        self.client().create_topic(spec)

        producer_properties = {}
        if workload == Workload.ACKS_1:
            producer_properties['acks'] = 1
        elif workload == Workload.ACKS_ALL:
            producer_properties['acks'] = -1
        elif workload == Workload.IDEMPOTENT:
            producer_properties['acks'] = -1
            producer_properties['enable.idempotence'] = True
        else:
            assert False

        swarm = ProducerSwarm(self.test_context,
                              self.redpanda,
                              spec.name,
                              producer_count,
                              10000000000,
                              log_level="ERROR",
                              properties=producer_properties)
        swarm.start()

        rpk = RpkTool(self.redpanda)

        def topic_is_healthy():
            if not swarm.is_alive(swarm.nodes[0]):
                swarm.stop()
                swarm.start()
            partitions = rpk.describe_topic(spec.name)
            hw_offsets = [p.high_watermark for p in partitions]
            offsets_present = [hw > 0 for hw in hw_offsets]
            self.logger.debug(f"High watermark offsets: {hw_offsets}")
            return len(offsets_present) == partition_count and all(
                offsets_present)

        for i in range(1, 20):
            rf = 3 if i % 2 == 0 else 1
            self.client().delete_topic(spec.name)
            spec.replication_factor = rf
            self.client().create_topic(spec)
            wait_until(topic_is_healthy,
                       30,
                       2,
                       err_msg=f"Topic {spec.name} health")
            sleep(5)

        swarm.stop()
        swarm.wait()


class TopicAutocreateTest(RedpandaTest):
    """
    Verify that autocreation works, and that the settings of an autocreated
    topic match those for a topic created by hand with rpk.
    """
    def __init__(self, test_context):
        super(TopicAutocreateTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            extra_rp_conf={'auto_create_topics_enabled': False},
            si_settings=SISettings(test_context))

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    def topic_autocreate_test(self):
        auto_topic = 'autocreated'
        manual_topic = "manuallycreated"

        # With autocreation disabled, producing to a nonexistent topic should not work.
        try:
            # Use rpk rather than kafka CLI because rpk errors out promptly
            self.rpk.produce(auto_topic, "foo", "bar")
        except Exception:
            # The write failed, and shouldn't have created a topic
            assert auto_topic not in self.kafka_tools.list_topics()
        else:
            assert False, "Producing to a nonexistent topic should fail"

        # Enable autocreation
        self.redpanda.set_cluster_config({'auto_create_topics_enabled': True})

        # Auto create topic
        assert auto_topic not in self.kafka_tools.list_topics()
        self.kafka_tools.produce(auto_topic, 1, 4096)
        assert auto_topic in self.kafka_tools.list_topics()
        auto_topic_spec = self.kafka_tools.describe_topic(auto_topic)
        assert auto_topic_spec.retention_ms is None
        assert auto_topic_spec.retention_bytes is None
        assert auto_topic_spec.cleanup_policy is not None

        # Create topic by hand, compare its properties to the autocreated one
        self.rpk.create_topic(manual_topic)
        manual_topic_spec = self.kafka_tools.describe_topic(auto_topic)
        assert manual_topic_spec.retention_ms == auto_topic_spec.retention_ms
        assert manual_topic_spec.retention_bytes == auto_topic_spec.retention_bytes
        assert manual_topic_spec.cleanup_policy == auto_topic_spec.cleanup_policy

        # Clear name and compare the rest of the attributes
        manual_topic_spec.name = auto_topic_spec.name = None
        assert manual_topic_spec == auto_topic_spec

        # compare topic configs as retrieved by rpk.
        # describe the topics and convert the resulting dict in a set, to compute the difference
        auto_topic_rpk_cfg = set(
            self.rpk.describe_topic_configs(auto_topic).items())
        manual_topic_rpk_cfg = set(
            self.rpk.describe_topic_configs(manual_topic).items())

        # retrieve the cloud storage mode and append it as an extra config, to check it. see issue/13492
        auto_topic_rpk_cfg.add(('cloud_storage_mode',
                                self.admin.get_partition_cloud_storage_status(
                                    auto_topic, 0)['cloud_storage_mode']))
        manual_topic_rpk_cfg.add(
            ('cloud_storage_mode',
             self.admin.get_partition_cloud_storage_status(
                 manual_topic, 0)['cloud_storage_mode']))

        self.logger.debug(f"{auto_topic=} config={auto_topic_rpk_cfg}")
        self.logger.debug(f"{manual_topic=}, config={manual_topic_rpk_cfg}")

        # remove elements that are equal. for the test to be a success,
        # auto and manual should be equal so at the end of the operations, the result should be empty
        cfg_intersection = auto_topic_rpk_cfg & manual_topic_rpk_cfg
        auto_topic_cfg_unique = auto_topic_rpk_cfg - cfg_intersection
        manual_topic_cfg_unique = manual_topic_rpk_cfg - cfg_intersection

        assert len(auto_topic_cfg_unique) == 0 and \
            len(manual_topic_cfg_unique) == 0, \
                  f"topics {auto_topic=} and {manual_topic=} have these different configs (should be empty) {auto_topic_cfg_unique=} {manual_topic_cfg_unique=}"


def topic_name():
    return "test-topic-" + "".join(
        random.choice(string.ascii_lowercase) for _ in range(16))


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
        'redpanda.remote.delete':
        lambda: "true" if random.randint(0, 1) else "false",
        'segment.ms':
        lambda: random.choice([-1, random.randint(10000, 10000000)]),
    }

    def __init__(self, test_context):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=5,
            cloud_storage_segment_max_upload_interval_sec=10,
            log_segment_size=100 * 1024 * 1024)

        super(CreateTopicsTest, self).__init__(test_context=test_context,
                                               num_brokers=3,
                                               si_settings=si_settings)

    @cluster(num_nodes=3)
    def test_create_topic_with_single_configuration_property(self):
        rpk = RpkTool(self.redpanda)

        for p, generator in CreateTopicsTest._topic_properties.items():
            name = topic_name()
            partitions = random.randint(1, 10)
            property_value = generator()
            rpk.create_topic(topic=name,
                             partitions=partitions,
                             replicas=3,
                             config={p: property_value})

            cfgs = rpk.describe_topic_configs(topic=name)
            assert str(cfgs[p][0]) == str(
                property_value), f"{cfgs[p][0]=} != {property_value=}"

    @cluster(num_nodes=3)
    def test_no_log_bloat_when_recreating_existing_topics(self):
        rpk = RpkTool(self.redpanda)
        topic = "test"
        rpk.create_topic(topic=topic)

        for _ in range(0, 10):
            try:
                rpk.create_topic(topic=topic)
                assert False, f"No exception receating existing topic: {topic}"
            except RpkException as e:
                if "TOPIC_ALREADY_EXISTS" not in e.msg:
                    raise e

        def create_topic_commands():
            cmds = []
            for node in self.redpanda.started_nodes():
                log_viewer = OfflineLogViewer(self.redpanda)
                records = log_viewer.read_controller(node=node)

                def is_create_topic_cmd(r):
                    return "type" in r.keys() and r["type"] == "topic_management_cmd" and\
                        r["data"]["type"] == 0

                create_topic_cmds = list(filter(is_create_topic_cmd, records))
                self.redpanda.logger.debug(
                    f"Node {node.account.hostname}, controller records: {records}"
                )
                cmds.append(len(create_topic_cmds) == 1)
            return all(cmds)

        self.redpanda.wait_until(
            create_topic_commands,
            timeout_sec=30,
            backoff_sec=3,
            err_msg="Timed out waiting for single create_topic command")

    @staticmethod
    def _modify_cluster_config(admin, redpanda, upsert):
        patch_result = admin.patch_cluster_config(upsert=upsert)
        wait_for_version_sync(admin, redpanda, patch_result['config_version'])

    @cluster(num_nodes=3)
    def test_create_with_min_rf(self):
        """
        Validate behavior of topic creation when setting
        minimum_topic_replications
        """
        admin = Admin(self.redpanda)
        # Set default RF to 3
        self._modify_cluster_config(admin, self.redpanda,
                                    {'default_topic_replications': 3})
        # Now can set minimum RF to 3
        self._modify_cluster_config(admin, self.redpanda,
                                    {'minimum_topic_replications': 3})
        rpk = RpkTool(self.redpanda)
        try:
            rpk.create_topic("should-fail", replicas=1)
            assert False, "Creation should have failed"
        except RpkException as e:
            assert "Replication factor must be greater than or equal to specified minimum value" in str(
                e), f'Unexpected return message: "{str(e)}"'

        rpk.create_topic("should-succeed", replicas=None)

    @cluster(num_nodes=3)
    def test_min_rf_log(self):
        """
        Validates that a log message appears when minimum_topic_replications
        is changed and current topic RF's that violate the minimum are logged.
        """
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic-1", replicas=1)
        rpk.create_topic("topic-3", replicas=3)

        admin = Admin(self.redpanda)
        self._modify_cluster_config(admin, self.redpanda,
                                    {'default_topic_replications': 3})
        self._modify_cluster_config(admin, self.redpanda,
                                    {'minimum_topic_replications': 3})

        assert self.redpanda.search_log_node(
            self.redpanda.nodes[0],
            "Topic {kafka/topic-1} has a replication factor less than specified minimum: 1 < 3"
        ), "Missing log message for topic-1"
        assert not self.redpanda.search_log_node(
            self.redpanda.nodes[0],
            "Topic {kafka-topic3} has a replication factor"
        ), "Invalid log message found for topic-3"

        # Restart nodes and verify we see the message at startup
        self.redpanda.restart_nodes(self.redpanda.nodes)

        num_found = self.redpanda.count_log_node(
            self.redpanda.nodes[0],
            "Topic {kafka/topic-1} has a replication factor less than specified minimum: 1 < 3"
        )
        assert num_found == 2, f'Expected to find 2 messages about topic-1, but found {num_found}'

        num_found = self.redpanda.count_log_node(
            self.redpanda.nodes[0],
            "Topic {kafka-topic3} has a replication factor")
        assert num_found == 0, f'Expected to find 0 messages about topic-3, but found {num_found}'


class CreateTopicsResponseTest(RedpandaTest):
    SUCCESS_EC = 0
    TOPIC_EXISTS_EC = 36

    DEFAULT_CLEANUP_POLICY = 'delete'
    DEFAULT_CONFIG_SOURCE = 5

    CONFIG_SOURCE_MAPPING = {
        1: 'DYNAMIC_TOPIC_CONFIG',
        5: 'DEFAULT_CONFIG',
    }

    def __init__(self, test_context):
        super(CreateTopicsResponseTest,
              self).__init__(test_context=test_context)
        self.kcl_client = RawKCL(self.redpanda)
        self.admin = Admin(self.redpanda)

    # we don't really care about the name aside from its not being random
    # so just construct it from the partition count and replication factor

    def create_topics(self, p_cnt, r_fac, n=1, validate_only=False):
        topics = []
        for i in range(0, n):
            topics.append({
                'name': f"foo-{p_cnt}-{r_fac}-{i}",
                'partition_count': p_cnt,
                'replication_factor': r_fac
            })

        return self.kcl_client.create_topics(6,
                                             topics=topics,
                                             validate_only=validate_only)

    def create_topic(self, name):
        topics = [{
            'name': f"{name}",
            'partition_count': 1,
            'replication_factor': 1
        }]
        return self.kcl_client.create_topics(6,
                                             topics=topics,
                                             validate_only=False)

    def get_np(self, tp):
        return tp['NumPartitions']

    def get_rf(self, tp):
        return tp['ReplicationFactor']

    def get_ec(self, tp):
        return tp['ErrorCode']

    def get_configs(self, tp):
        return tp['Configs']

    def get_config_by_name(self, tp, name):
        cfgs = self.get_configs(tp)
        return next((cfg for cfg in cfgs if cfg['Name'] == name), None)

    def check_topic_resp(self, topic, expected_np, expected_rf, expected_ec):
        np = self.get_np(topic)
        assert np == expected_np, f"Expected partition count {expected_np}, got {np}"
        rf = self.get_rf(topic)
        assert rf == expected_rf, f"Expected partition count {expected_rf}, got {rf}"
        ec = self.get_ec(topic)
        assert ec == expected_ec, f"Expected partition count {expected_ec}, got {ec}"

    @cluster(num_nodes=3)
    @matrix(
        partition_count=[3, -1],
        replication_factor=[3, -1],
    )
    def test_create_topic_responses(self, partition_count, replication_factor):
        """
        Validates that create_topic responses are populated with real values when
        default placeholders are supplied in the request
        """

        cfg = self.admin.get_cluster_config()
        expected_np = partition_count if partition_count > 0 else cfg[
            'default_topic_partitions']
        expected_rf = replication_factor if replication_factor > 0 else cfg[
            'default_topic_replications']

        topics = self.create_topics(partition_count, replication_factor, 3)
        for topic in topics:
            self.check_topic_resp(topic, expected_np, expected_rf,
                                  self.SUCCESS_EC)

        topics = self.create_topics(partition_count, replication_factor, 3)
        for topic in topics:
            self.check_topic_resp(topic, expected_np, expected_rf,
                                  self.TOPIC_EXISTS_EC)

    @cluster(num_nodes=3)
    def test_create_topic_response_configs(self):
        """
        Validates that configs returned in create_topics responses are
          a. qualified with an appropriate "source"
          b. serialized correctly
        """

        topic_name = 'test-create-topic-response'
        create_topics_response = self.create_topic(topic_name)
        topic_response = create_topics_response[0]

        res = self.kcl_client.describe_topic(topic_name)
        describe_configs = [line.split() for line in res.strip().split('\n')]

        for (key, value, source) in describe_configs:
            topic_config = self.get_config_by_name(topic_response, key)

            assert topic_config, f"Config '{key}' returned by DescribeConfigs is missing from configs response in CreateTopic"
            assert topic_config[
                'Value'] == value, f"config value mismatch for {key} across CreateTopic and DescribeConfigs: {topic_config['Value']} != {value}"

            assert self.CONFIG_SOURCE_MAPPING[topic_config[
                'Source']] == source, f"config source mismatch for {key} across CreateTopic and DescribeConfigs: {self.CONFIG_SOURCE_MAPPING[topic_config['Source']]} != {source}"

    @cluster(num_nodes=3)
    def test_create_topic_validate_only(self):
        """
        Validates that create topics calls with validate only flag return
        the correct error code depending on whether or not the topic already
        exists.
        """

        topic = self.create_topics(1, 1, validate_only=True)[0]
        self.check_topic_resp(topic, 1, 1, self.SUCCESS_EC)

        topic = self.create_topics(1, 1)[0]
        self.check_topic_resp(topic, 1, 1, self.SUCCESS_EC)

        topic = self.create_topics(1, 1, validate_only=True)[0]
        self.check_topic_resp(topic, -1, -1, self.TOPIC_EXISTS_EC)


class CreateSITopicsTest(RedpandaTest):
    def __init__(self, test_context):
        super(CreateSITopicsTest,
              self).__init__(test_context=test_context,
                             num_brokers=1,
                             si_settings=SISettings(test_context))

    def _to_bool(self, x: str) -> bool:
        return True if x == "true" else False

    def _from_bool(self, x: bool) -> str:
        return "true" if x else "false"

    @cluster(num_nodes=1)
    def test_shadow_indexing_mode(self):
        rpk = RpkTool(self.redpanda)

        cluster_remote_read = [True, False]
        cluster_remote_write = [True, False]
        topic_remote_read = [True, False, None]
        topic_remote_write = [True, False, None]

        cases = list(
            itertools.product(cluster_remote_read, cluster_remote_write,
                              topic_remote_read, topic_remote_write))

        for c_read, c_write, t_read, t_write in cases:
            self.logger.info(
                f"Test case: cloud_storage_enable_remote_read={c_read}, "
                f"cloud_storage_enable_remote_write={c_write}, "
                f"redpanda.remote.read={t_read}, "
                f"redpanda.remote.write={t_write}")

            expected_read = t_read if t_read is not None \
                            else c_read
            expected_write = t_write if t_write is not None \
                             else c_write

            self.redpanda.set_cluster_config(
                {
                    "cloud_storage_enable_remote_read": c_read,
                    "cloud_storage_enable_remote_write": c_write
                },
                expect_restart=True)

            config = {}
            if t_read is not None:
                config["redpanda.remote.read"] = self._from_bool(t_read)
            if t_write is not None:
                config["redpanda.remote.write"] = self._from_bool(t_write)

            topic = topic_name()
            rpk.create_topic(topic=topic,
                             partitions=1,
                             replicas=1,
                             config=config)

            ret = rpk.describe_topic_configs(topic=topic)

            read = self._to_bool(ret["redpanda.remote.read"][0])
            write = self._to_bool(ret["redpanda.remote.write"][0])
            assert read == expected_read, f"{read} != {expected_read}"
            assert write == expected_write, f"{write} != {expected_write}"

    @cluster(num_nodes=1)
    def test_shadow_indexing_mode_persistence(self):
        rpk = RpkTool(self.redpanda)
        self.redpanda.set_cluster_config(
            {
                "cloud_storage_enable_remote_read": True,
                "cloud_storage_enable_remote_write": True
            },
            expect_restart=True)

        default_si_topic = topic_name()
        explicit_si_topic = topic_name()
        rpk.create_topic(topic=default_si_topic, partitions=1, replicas=1)
        rpk.create_topic(topic=explicit_si_topic,
                         partitions=1,
                         replicas=1,
                         config={"redpanda.remote.write": "false"})

        # Changing the defaults after creating a topic should not change
        # the configuration of the already-created topic
        self.redpanda.set_cluster_config(
            {
                "cloud_storage_enable_remote_read": False,
                "cloud_storage_enable_remote_write": True
            },
            expect_restart=True)

        default_si_configs = rpk.describe_topic_configs(topic=default_si_topic)
        explicit_si_configs = rpk.describe_topic_configs(
            topic=explicit_si_topic)

        # This topic has topic-level properties set from the cluster defaults
        # and the values should _not_ have been changed by the intervening
        # change to those defaults.  Properties which still match the current
        # default will be reported as DEFAULT, even though they are sticky,
        # per issue https://github.com/redpanda-data/redpanda/issues/7451
        assert default_si_configs["redpanda.remote.read"] == (
            "true", "DYNAMIC_TOPIC_CONFIG")
        assert default_si_configs["redpanda.remote.write"] == (
            "true", "DEFAULT_CONFIG")

        # This topic was created with explicit properties that differed
        # from the defaults.  Both properties differ from the present
        # defaults so will be reported as DYNAMIC
        assert explicit_si_configs["redpanda.remote.read"] == (
            "true", "DYNAMIC_TOPIC_CONFIG")
        assert explicit_si_configs["redpanda.remote.write"] == (
            "false", "DYNAMIC_TOPIC_CONFIG")

    @cluster(num_nodes=1)
    def topic_alter_config_test(self):
        """
        Intentionally use the legacy (deprecated in Kafka 2.3.0) AlterConfig
        admin RPC, to check it works with our custom topic properties
        """
        rpk = RpkTool(self.redpanda)
        topic = topic_name()
        rpk.create_topic(topic=topic, partitions=1, replicas=1)

        # Older KCL has support for the legacy AlterConfig RPC: latest rpk and kafka CLI do not.
        kcl = KCL(self.redpanda)

        examples = {
            'redpanda.remote.delete': 'true',
            'redpanda.remote.write': 'true',
            'redpanda.remote.read': 'true',
            'retention.local.target.bytes': '123456',
            'retention.local.target.ms': '123456',
            'initial.retention.local.target.bytes': '123456',
            'initial.retention.local.target.ms': '123456'
        }

        for k, v in examples.items():
            kcl.alter_topic_config({k: v}, incremental=False, topic=topic)

        # 'cleanup.policy' is defaulted to 'delete' upon topic creation.
        # AlterConfigs handling should preserve this default, unless explicitly
        # overriden.
        topic_config = rpk.describe_topic_configs(topic)
        value, src = topic_config["cleanup.policy"]
        assert value == "delete" and src == "DEFAULT_CONFIG"

        kcl.alter_topic_config({"cleanup.policy": "compact"},
                               incremental=False,
                               topic=topic)
        topic_config = rpk.describe_topic_configs(topic)
        value, src = topic_config["cleanup.policy"]
        assert value == "compact" and src == "DYNAMIC_TOPIC_CONFIG"

        # As a control, confirm that if we did pass an invalid property, we would have got an error
        with expect_exception(RuntimeError, lambda e: "invalid" in str(e)):
            kcl.alter_topic_config({"redpanda.invalid.property": 'true'},
                                   incremental=False,
                                   topic=topic)


# When quickly recreating topics after deleting them, redpanda's topic
# dir creation can trip up over the topic dir deletion.  This is not
# harmful because creation is retried, but it does generate a log error.
# See https://github.com/redpanda-data/redpanda/issues/5768
RECREATE_LOG_ALLOW_LIST = ["mkdir failed: No such file or directory"]


class RecreateTopicMetadataTest(RedpandaTest):
    def __init__(self, test_context):

        super(RecreateTopicMetadataTest, self).__init__(
            test_context=test_context,
            num_brokers=5,
            extra_rp_conf={
                # Test does explicit leadership movements
                # that the balancer would interfere with.
                'enable_leader_balancer': False
            })

    @cluster(num_nodes=6, log_allow_list=RECREATE_LOG_ALLOW_LIST)
    @parametrize(replication_factor=3)
    @parametrize(replication_factor=5)
    def test_recreated_topic_metadata_are_valid(self, replication_factor):
        """
        Test recreated topic metadata are valid across all the nodes
        """

        topic = 'tp-test'
        partition_count = 5
        rpk = RpkTool(self.redpanda)
        kcat = KafkaCat(self.redpanda)
        admin = Admin(self.redpanda)
        # create topic with replication factor of 3
        rpk.create_topic(topic='tp-test',
                         partitions=partition_count,
                         replicas=replication_factor)

        # produce some data to the topic

        def wait_for_leader(partition, expected_leader):
            leader, _ = kcat.get_partition_leader(topic, partition)
            return leader == expected_leader

        def transfer_all_leaders():
            partitions = rpk.describe_topic(topic)
            for p in partitions:
                replicas = set(p.replicas)
                replicas.remove(p.leader)
                target = random.choice(list(replicas))
                admin.partition_transfer_leadership("kafka", topic, p.id,
                                                    target)
                wait_until(lambda: wait_for_leader(p.id, target),
                           timeout_sec=30,
                           backoff_sec=1)
            msg_cnt = 100
            producer = RpkProducer(self.test_context,
                                   self.redpanda,
                                   topic,
                                   16384,
                                   msg_cnt,
                                   acks=-1)

            producer.start()
            producer.wait()
            producer.free()

        # transfer leadership to grow the term
        for i in range(0, 10):
            transfer_all_leaders()

        # recreate the topic
        rpk.delete_topic(topic)
        rpk.create_topic(topic='tp-test',
                         partitions=partition_count,
                         replicas=replication_factor)

        def metadata_consistent():
            # validate leadership information on each node
            for p in range(0, partition_count):
                leaders = set()
                for n in self.redpanda.nodes:
                    admin_partition = admin.get_partitions(topic=topic,
                                                           partition=p,
                                                           namespace="kafka",
                                                           node=n)
                    self.logger.info(
                        f"node: {n.account.hostname} partition: {admin_partition}"
                    )
                    leaders.add(admin_partition['leader_id'])

                self.logger.info(f"{topic}/{p} leaders: {leaders}")
                if len(leaders) != 1:
                    return False
            return True

        wait_until(metadata_consistent, 45, backoff_sec=2)


class CreateTopicUpgradeTest(RedpandaTest):
    def __init__(self, test_context):
        si_settings = SISettings(test_context,
                                 cloud_storage_enable_remote_write=False,
                                 cloud_storage_enable_remote_read=False)
        self._s3_bucket = si_settings.cloud_storage_bucket

        super(CreateTopicUpgradeTest, self).__init__(test_context=test_context,
                                                     num_brokers=3,
                                                     si_settings=si_settings)
        self.installer = self.redpanda._installer
        self.rpk = RpkTool(self.redpanda)

    # This test starts the Redpanda service inline (see 'install_and_start') at the beginning
    # of the test body. By default, in the Azure CDT env, the service startup
    # logic attempts to set the azure specific cluster configs.
    # However, these did not exist prior to v23.1 and the test would fail
    # before it can be skipped.
    def setUp(self):
        pass

    def install_and_start(self):
        self.installer.install(
            self.redpanda.nodes,
            (22, 2),
        )
        self.redpanda.start()

    def _populate_tiered_storage_topic(self, topic_name, local_retention):
        # Write 3x the local retention, then wait for local storage to be
        # trimmed back accordingly.
        bytes = local_retention * 3
        msg_size = 131072
        msg_count = bytes // msg_size
        for _ in range(0, max(msg_count, 4)):
            self.rpk.produce(topic_name, "key", "b" * msg_size)

        wait_for_local_storage_truncate(self.redpanda,
                                        topic=topic_name,
                                        target_bytes=local_retention)

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_cloud_storage_sticky_enablement_v22_2_to_v22_3(
            self, cloud_storage_type):
        """
        In Redpanda 22.3, the cluster defaults for cloud storage change
        from being applied at runtime to being sticky at creation time,
        or at upgrade time.
        """
        self.install_and_start()

        # Switch on tiered storage using cluster properties, not topic properties
        self.redpanda.set_cluster_config(
            {
                'cloud_storage_enable_remote_read': True,
                'cloud_storage_enable_remote_write': True
            },
            # This shouldn't require a restart, but it does in Redpanda < 22.3
            True)

        topic = "test-topic"
        self.rpk.create_topic(topic)
        described = self.rpk.describe_topic_configs(topic)
        assert described['redpanda.remote.write'] == ('true', 'DEFAULT_CONFIG')
        assert described['redpanda.remote.read'] == ('true', 'DEFAULT_CONFIG')

        # Upgrade to Redpanda latest
        self.installer.install(self.redpanda.nodes, (22, 3))
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for properties migration to run
        self.redpanda.await_feature("cloud_retention",
                                    active=True,
                                    timeout_sec=30)

        self.logger.info(
            f"Config status after upgrade: {self.redpanda._admin.get_cluster_config_status()}"
        )

        # Properties should still be set, but migrated to topic-level
        described = self.rpk.describe_topic_configs(topic)
        assert described['redpanda.remote.write'] == ('true', 'DEFAULT_CONFIG')
        assert described['redpanda.remote.read'] == ('true', 'DEFAULT_CONFIG')

        # A new topic picks up these properties too
        self.rpk.create_topic("created-after-enabled")
        described = self.rpk.describe_topic_configs("created-after-enabled")
        assert described['redpanda.remote.write'] == ('true', 'DEFAULT_CONFIG')
        assert described['redpanda.remote.read'] == ('true', 'DEFAULT_CONFIG')

        # Switching off cluster defaults shoudln't affect existing topic
        self.redpanda.set_cluster_config(
            {
                'cloud_storage_enable_remote_read': False,
                'cloud_storage_enable_remote_write': False
            }, False)
        described = self.rpk.describe_topic_configs(topic)
        assert described['redpanda.remote.write'] == ('true',
                                                      'DYNAMIC_TOPIC_CONFIG')
        assert described['redpanda.remote.read'] == ('true',
                                                     'DYNAMIC_TOPIC_CONFIG')

        # A newly created topic should have tiered storage switched off
        self.rpk.create_topic("created-after-disabled")
        described = self.rpk.describe_topic_configs("created-after-disabled")
        assert described['redpanda.remote.write'] == ('false',
                                                      'DEFAULT_CONFIG')
        assert described['redpanda.remote.read'] == ('false', 'DEFAULT_CONFIG')

        # Nothing in this test guarantees that partition manifests will be uploaded.
        self.redpanda.si_settings.set_expected_damage({"ntpr_no_manifest"})

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_retention_config_on_upgrade_from_v22_2_to_v22_3(
            self, cloud_storage_type):
        self.install_and_start()

        self.rpk.create_topic("test-topic-with-retention",
                              config={"retention.bytes": 10000})

        local_retention = 10000
        self.rpk.create_topic(
            "test-si-topic-with-retention",
            config={
                "retention.bytes": str(local_retention),
                "redpanda.remote.write": "true",
                "redpanda.remote.read": "true",
                # Small segment.bytes so that we can readily
                # get data into S3 by writing small amounts.
                "segment.bytes": 1000000
            })

        # Write a few megabytes of data, enough to spill to S3.  This will be used
        # later for checking that deletion behavior is correct on legacy topics.
        self._populate_tiered_storage_topic("test-si-topic-with-retention",
                                            local_retention)

        # This topic is like "test-si-topic-with-retention", but instead
        # of settings its properties at creation time, set them via
        # alter messages: these follow a different code path in
        # kafka and controller, and are serialized using different structures
        # than the structures used in topic creation.
        self.rpk.create_topic("test-si-topic-with-retention-altered")
        self.rpk.alter_topic_config("test-si-topic-with-retention-altered",
                                    "retention.bytes", 10000)
        self.rpk.alter_topic_config("test-si-topic-with-retention-altered",
                                    "redpanda.remote.write", "true")

        # Check our alter operations applied properly
        # (see https://github.com/redpanda-data/redpanda/issues/6772)
        after_alter = self.rpk.describe_topic_configs(
            "test-si-topic-with-retention-altered")
        assert after_alter['redpanda.remote.write'][0] == 'true'
        assert after_alter['retention.bytes'][0] == '10000'

        # TODO: this test currently exercises 22.2 (serde) encoding to
        # 22.3 (newer serde) encoding.  It should be extended to also
        # cover the case of creating topics in version 22.1, upgrading
        # to 22.2, and then to 22.3, to check the ADL encoding of messages
        # in the controller log.

        self.installer.install(
            self.redpanda.nodes,
            (22, 3),
        )

        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for any migration steps to complete
        self.redpanda.await_feature("cloud_retention",
                                    active=True,
                                    timeout_sec=30)

        non_si_configs = self.rpk.describe_topic_configs(
            "test-topic-with-retention")
        si_configs = self.rpk.describe_topic_configs(
            "test-si-topic-with-retention")
        si_altered_configs = self.rpk.describe_topic_configs(
            "test-si-topic-with-retention-altered")

        # "test-topic-with-retention" did not have remote write enabled
        # at the time of the upgrade, so retention.* configs are preserved
        # and retention.local.target.* configs are set to their default values,
        # but ignored
        self.logger.debug(f"Checking config {non_si_configs}")
        assert non_si_configs["retention.bytes"] == ("10000",
                                                     "DYNAMIC_TOPIC_CONFIG")
        assert non_si_configs["retention.ms"][1] == "DEFAULT_CONFIG"

        assert non_si_configs["retention.local.target.bytes"] == (
            "-1", "DEFAULT_CONFIG")
        assert non_si_configs["retention.local.target.ms"][
            1] == "DEFAULT_CONFIG"

        # 'test-si-topic-with-retention' was enabled from remote write
        # at the time of the upgrade, so retention.local.target.* configs
        # should be initialised from retention.* and retention.* configs should
        # be disabled.
        for conf in (si_configs, si_altered_configs):
            self.logger.debug(f"Checking config: {conf}")
            assert conf['redpanda.remote.write'][0] == 'true'
            assert conf["retention.bytes"][0] == "-1"
            assert conf["retention.ms"][0] == "-1"

            assert conf["retention.local.target.bytes"] == (
                "10000", "DYNAMIC_TOPIC_CONFIG")
            assert conf["retention.local.target.ms"][1] == "DEFAULT_CONFIG"
            assert conf["redpanda.remote.delete"][0] == "false"

        # After upgrade, newly created topics should have remote.delete
        # enabled by default, and interpret assignments to retention properties
        # literally (no mapping of retention -> retention.local)
        for (new_topic_name,
             enable_si) in [("test-topic-post-upgrade-nosi", False),
                            ("test-topic-post-upgrade-si", True)]:

            segment_bytes = 1000000
            local_retention = segment_bytes * 2
            retention_bytes = segment_bytes * 10
            create_config = {
                "retention.bytes": retention_bytes,
                "retention.local.target.bytes": local_retention,
                "segment.bytes": segment_bytes
            }
            if enable_si:
                create_config['redpanda.remote.write'] = 'true'
                create_config['redpanda.remote.read'] = 'true'

            self.rpk.create_topic(new_topic_name, config=create_config)
            new_config = self.rpk.describe_topic_configs(new_topic_name)
            assert new_config["redpanda.remote.delete"][0] == "true"
            assert new_config["retention.bytes"] == (str(retention_bytes),
                                                     "DYNAMIC_TOPIC_CONFIG")
            assert new_config["retention.ms"][1] == "DEFAULT_CONFIG"
            assert new_config["retention.local.target.ms"][
                1] == "DEFAULT_CONFIG"
            assert new_config["retention.local.target.bytes"] == (
                str(local_retention), "DYNAMIC_TOPIC_CONFIG")
            if enable_si:
                assert new_config['redpanda.remote.write'][0] == "true"
                assert new_config['redpanda.remote.read'][0] == "true"

            # The remote.delete property is applied irrespective of whether
            # the topic is initially tiered storage enabled.
            assert new_config['redpanda.remote.delete'][0] == "true"

            if enable_si:
                self._populate_tiered_storage_topic(new_topic_name,
                                                    local_retention)

        # A newly created tiered storage topic should have its data deleted
        # in S3 when the topic is deleted
        self._delete_tiered_storage_topic("test-topic-post-upgrade-si", True)

        # Ensure that the `redpanda.remote.delete==false` configuration is
        # really taking effect, by deleting a legacy topic and ensuring data is
        # left behind in S3
        self._delete_tiered_storage_topic("test-si-topic-with-retention",
                                          False)

    def _delete_tiered_storage_topic(self, topic_name: str,
                                     expect_s3_deletion: bool):
        self.logger.debug(f"Deleting {topic_name} and checking S3 result")

        before_objects = set(
            o.key
            for o in self.cloud_storage_client.list_objects(self._s3_bucket)
            if topic_name in o.key)

        # Test is meaningless if there were no objects to start with
        assert len(before_objects) > 0

        self.rpk.delete_topic(topic_name)

        def is_empty():
            return sum(1 for o in self.cloud_storage_client.list_objects(
                self._s3_bucket) if topic_name in o.key) == 0

        if expect_s3_deletion:
            wait_until(is_empty, timeout_sec=10, backoff_sec=1)
        else:
            # When we expect objects to remain, require that they remain for
            # at least some time, to avoid false-passing if they were deleted with
            # some small delay.
            sleep(10)

        after_objects = set(
            o.key
            for o in self.cloud_storage_client.list_objects(self._s3_bucket)
            if topic_name in o.key)
        deleted_objects = before_objects - after_objects
        if expect_s3_deletion:
            assert deleted_objects
        else:
            self.logger.debug(
                f"deleted objects (should be empty): {deleted_objects}")
            assert len(deleted_objects) == 0

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_retention_upgrade_with_cluster_remote_write(
            self, cloud_storage_type):
        """
        Validate how the cluster-wide cloud_storage_enable_remote_write
        is handled on upgrades from <=22.2
        """
        self.install_and_start()

        self.redpanda.set_cluster_config(
            {"cloud_storage_enable_remote_write": "true"}, expect_restart=True)

        self.rpk.create_topic("test-topic-with-remote-write",
                              config={"retention.bytes": 10000})

        self.rpk.create_topic("test-topic-without-remote-write",
                              config={
                                  "retention.bytes": 10000,
                                  "redpanda.remote.write": "false"
                              })

        self.installer.install(
            self.redpanda.nodes,
            (22, 3),
        )

        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for any migration steps to complete
        self.redpanda.await_feature('cloud_retention',
                                    active=True,
                                    timeout_sec=30)

        # Because legacy Redpanda treated cloud_storage_enable_remote_write as
        # an override to the topic property (even if the topic remote.write was explicitly
        # set to false), our two topics should end up with identical config: both
        # with SI enabled and with their retention settings transposed.
        for topic in [
                "test-topic-with-remote-write",
                "test-topic-without-remote-write"
        ]:
            config = self.rpk.describe_topic_configs(topic)
            assert config["retention.ms"] == ("-1", "DYNAMIC_TOPIC_CONFIG")
            assert config["retention.bytes"] == ("-1", "DYNAMIC_TOPIC_CONFIG")
            assert config["retention.local.target.bytes"] == (
                "10000", "DYNAMIC_TOPIC_CONFIG")
            assert config["retention.local.target.ms"][1] == "DEFAULT_CONFIG"

        # The ntp manifest may not have been uploaded yet, but that's fine
        # since this test focuses on the topic config change.
        self.redpanda.si_settings.set_expected_damage({"ntpr_no_manifest"})
