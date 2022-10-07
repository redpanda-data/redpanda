# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
import random
import string
import itertools
from time import sleep
from rptest.clients.default import DefaultClient
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cat import KafkaCat
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda import ResourceSettings, SISettings
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.services.rpk_producer import RpkProducer
from rptest.clients.kafka_cli_tools import KafkaCliTools

from ducktape.utils.util import wait_until
from ducktape.mark import matrix, parametrize, ok_to_fail

from rptest.tests.redpanda_test import RedpandaTest


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
            partitions = rpk.describe_topic(spec.name)
            offsets_present = [p.high_watermark > 0 for p in partitions]
            return len(offsets_present) == partition_count and all(
                offsets_present)

        for i in range(1, 20):
            rf = 3 if i % 2 == 0 else 1
            self.client().delete_topic(spec.name)
            spec.replication_factor = rf
            self.client().create_topic(spec)
            wait_until(topic_is_healthy, 30, 2)
            sleep(5)

        swarm.stop_all()
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
            extra_rp_conf={'auto_create_topics_enabled': False})

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

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

        # Create topic by hand, compare its properties to the autocreated one
        self.rpk.create_topic(manual_topic)
        manual_topic_spec = self.kafka_tools.describe_topic(auto_topic)
        assert manual_topic_spec.retention_ms == auto_topic_spec.retention_ms
        assert manual_topic_spec.retention_bytes == auto_topic_spec.retention_bytes

        # Clear name and compare the rest of the attributes
        manual_topic_spec.name = auto_topic_spec.name = None
        assert manual_topic_spec == auto_topic_spec


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
            assert str(cfgs[p][0]) == str(property_value)


class CreateSITopicsTest(RedpandaTest):
    def __init__(self, test_context):
        super(CreateSITopicsTest, self).__init__(test_context=test_context,
                                                 num_brokers=1,
                                                 si_settings=SISettings())

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

        self.redpanda.set_cluster_config(
            {
                "cloud_storage_enable_remote_read": False,
                "cloud_storage_enable_remote_write": True
            },
            expect_restart=True)

        default_si_configs = rpk.describe_topic_configs(topic=default_si_topic)
        explicit_si_configs = rpk.describe_topic_configs(
            topic=explicit_si_topic)

        # Since this topic did not specifiy an explicit remote read/write
        # policy, the values are bound to the cluster level configs.
        assert default_si_configs["redpanda.remote.read"] == ("false",
                                                              "DEFAULT_CONFIG")
        assert default_si_configs["redpanda.remote.write"] == (
            "true", "DEFAULT_CONFIG")

        # Since this topic specified an explicit remote read/write policy,
        # the values are not overriden by the cluster config change.
        assert explicit_si_configs["redpanda.remote.read"] == (
            "true", "DYNAMIC_TOPIC_CONFIG")
        assert explicit_si_configs["redpanda.remote.write"] == (
            "false", "DYNAMIC_TOPIC_CONFIG")


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
        si_settings = SISettings(cloud_storage_enable_remote_write=False,
                                 cloud_storage_enable_remote_read=False)

        super(CreateTopicUpgradeTest, self).__init__(test_context=test_context,
                                                     num_brokers=3,
                                                     si_settings=si_settings)
        self.installer = self.redpanda._installer
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        self.installer.install(
            self.redpanda.nodes,
            (22, 2, 4),
        )
        self.redpanda.start()

    @cluster(num_nodes=3)
    def test_retention_config_on_upgrade_from_v22_2_to_v22_3(self):
        self.rpk.create_topic("test-topic-with-retention",
                              config={"retention.bytes": 10000})

        self.rpk.create_topic("test-si-topic-with-retention",
                              config={
                                  "retention.bytes": 10000,
                                  "redpanda.remote.write": "true"
                              })

        self.installer.install(
            self.redpanda.nodes,
            RedpandaInstaller.HEAD,
        )

        self.redpanda.restart_nodes(self.redpanda.nodes)

        non_si_configs = self.rpk.describe_topic_configs(
            "test-topic-with-retention")
        si_configs = self.rpk.describe_topic_configs(
            "test-si-topic-with-retention")

        self.logger.debug(f"test-si-topic-with-retention conig: {si_configs}")
        self.logger.debug(f"test-topic-with-retention: {non_si_configs}")

        # "test-topic-with-retention" did not have remote write enabled
        # at the time of the upgrade, so retention.* configs are preserved
        # and retention.local-target.* configs are set to their default values,
        # but ignored
        assert non_si_configs["retention.bytes"] == ("10000",
                                                     "DYNAMIC_TOPIC_CONFIG")
        assert non_si_configs["retention.ms"][1] == "DEFAULT_CONFIG"

        assert non_si_configs["retention.local-target.bytes"] == (
            "-1", "DEFAULT_CONFIG")
        assert non_si_configs["retention.local-target.ms"][
            1] == "DEFAULT_CONFIG"

        # 'test-si-topic-with-retention' was enabled from remote write
        # at the time of the upgrade, so retention.local-target.* configs
        # should be initialised from retention.* and retention.* configs should
        # be disabled.
        assert si_configs["retention.bytes"] == ("-1", "DYNAMIC_TOPIC_CONFIG")
        assert si_configs["retention.ms"] == ("-1", "DYNAMIC_TOPIC_CONFIG")

        assert si_configs["retention.local-target.bytes"] == (
            "10000", "DYNAMIC_TOPIC_CONFIG")
        assert si_configs["retention.local-target.ms"][1] == "DEFAULT_CONFIG"

    # Previous version of Redpanda have a bug due to which the topic
    # level overrides are not applied for remote read/write. This causes
    # the test to fail. Remove ok_to_fail once #6663 is merged and
    # backported. TODO(vlad)
    @ok_to_fail
    @cluster(num_nodes=3)
    def test_retention_upgrade_with_cluster_remote_write(self):
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
            RedpandaInstaller.HEAD,
        )

        self.redpanda.restart_nodes(self.redpanda.nodes)

        without_remote_write_config = self.rpk.describe_topic_configs(
            "test-topic-without-remote-write")
        with_remote_write_config = self.rpk.describe_topic_configs(
            "test-topic-with-remote-write")

        self.logger.debug(
            f"test-topic-with-remote-write config: {with_remote_write_config}")
        self.logger.debug(
            f"test-topic-without-remote-write config: {without_remote_write_config}"
        )

        # 'test-topic-without-remote-write' overwrites the global cluster config,
        # so retention.* configs are preserved and retention.local-target.* configs
        # are set to their default values, but ignored
        assert without_remote_write_config["retention.bytes"] == (
            "10000", "DYNAMIC_TOPIC_CONFIG")
        assert without_remote_write_config["retention.ms"][
            1] == "DEFAULT_CONFIG"

        assert without_remote_write_config["retention.local-target.bytes"] == (
            "-1", "DEFAULT_CONFIG")
        assert with_remote_write_config["retention.local-target.ms"][
            1] == "DEFAULT_CONFIG"

        # "test-topic-with-remote-write-config" does not overwrite the
        # cluster level remote write enablement, so retention.local-target.* configs
        # should be initialised from retention.* and retention.* configs should
        # be disabled.
        assert with_remote_write_config["retention.bytes"] == (
            "-1", "DYNAMIC_TOPIC_CONFIG")
        assert with_remote_write_config["retention.ms"] == (
            "-1", "DYNAMIC_TOPIC_CONFIG")

        assert with_remote_write_config["retention.local-target.bytes"] == (
            "10000", "DYNAMIC_TOPIC_CONFIG")
        assert with_remote_write_config["retention.local-target.ms"][
            1] == "DEFAULT_CONFIG"
