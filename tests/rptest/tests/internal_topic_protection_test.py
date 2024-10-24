# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import time

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import SchemaRegistryConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kafka_cat import KafkaCat
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.util import expect_exception, wait_until_result
from rptest.utils.schema_registry_utils import get_subjects

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until


class InternalTopicProtectionTest(RedpandaTest):
    """
    Verify that the `kafka_nodelete_topics` and `kafka_noproduce_topics`
    configuration properties function as intended.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, extra_rp_conf={}, **kwargs)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.kafka_cat = KafkaCat(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    @parametrize(protect_config="kafka_nodelete_topics")
    @parametrize(protect_config="kafka_noproduce_topics")
    def kafka_protections_disable_config_test(
            self,
            protect_config,
            config="retention.ms",
            original_val=str(4 * 60 * 60 * 60),  # 4 hrs
            new_val=str(5 * 60 * 60 * 60)  # 5 hrs
    ):

        test_topic = "test_topic"
        self.rpk.create_topic(test_topic, 3, config={config: original_val})

        # Protect topic
        self.redpanda.set_cluster_config({protect_config: [test_topic]})

        # Ensure config of protected topic can't be changed.
        with expect_exception(
                RpkException,
                lambda e: "TOPIC_AUTHORIZATION_FAILED" in str(e)):
            self.rpk.alter_topic_config(test_topic, config, new_val)

        # Allow time for a potential change to be propagated.
        time.sleep(10)
        config_val, _ = self.rpk.describe_topic_configs(test_topic)[config]

        assert config_val == original_val, "Topic config was changed even with protection"

        # Ensure config of protected topic can't be deleted.
        self.rpk.delete_topic_config(test_topic, config)

        # Allow time for a potential change to be propagated.
        time.sleep(10)
        config_val, _ = self.rpk.describe_topic_configs(test_topic)[config]

        assert config_val == original_val, "Topic config was deleted even with protection"

        # Remove topic from protection list and ensure config can be changed
        self.redpanda.set_cluster_config({protect_config: []})
        self.rpk.alter_topic_config(test_topic, config, new_val)

        # Allow time for the change to be propagated.
        time.sleep(10)
        config_val, _ = self.rpk.describe_topic_configs(test_topic)[config]

        assert config_val == new_val, "Topic config wasn't changed"

    @cluster(num_nodes=3)
    @parametrize(client_type="rpk")
    @parametrize(client_type="kafka_tools")
    def kafka_noproduce_topics_test(self, client_type):
        def get_hw(topic, partition_id):
            partition = []

            def has_partition():
                nonlocal partition
                partition = [
                    p for p in self.rpk.describe_topic(topic)
                    if p.id == partition_id
                ]
                return len(partition) == 1

            wait_until(has_partition, timeout_sec=30, backoff_sec=3)

            return partition[0].high_watermark

        if client_type == "rpk":
            produce_fn = lambda topic, msg: self.rpk.produce(
                topic, "key", msg, timeout=30)
            failure_exception_type = RpkException

        elif client_type == "kafka_tools":
            produce_fn = lambda topic, msg: self.kafka_cat.produce_one(
                topic, msg)
            failure_exception_type = subprocess.CalledProcessError

        else:
            assert False, "Unknown client type"

        test_topic = "noproduce_topic"
        self.kafka_tools.create_topic(TopicSpec(name=test_topic))
        partition_id = 0

        wait_until(lambda: test_topic in self.rpk.list_topics(),
                   timeout_sec=90,
                   backoff_sec=3)

        # Ensure topic can't be produced to via the Kafka API when
        # it's in the kafka_noproduce_topics list.
        self.redpanda.set_cluster_config(
            {"kafka_noproduce_topics": [test_topic]})

        pre_produce_hw = get_hw(test_topic, partition_id)
        try:
            produce_fn(test_topic, "test_msg")
        except Exception as e:
            assert type(e) == failure_exception_type
        else:
            assert False, "Call to delete topic returned sucess"
        post_produce_hw = get_hw(test_topic, partition_id)

        assert pre_produce_hw == post_produce_hw, "was able to produce to topic"

        # Check that a topic can be removed from the kafka_noproduce_topics
        # list then produced to.
        self.redpanda.set_cluster_config({"kafka_noproduce_topics": []})

        pre_produce_hw = get_hw(test_topic, partition_id)
        produce_fn(test_topic, "test_msg")
        post_produce_hw = get_hw(test_topic, partition_id)

        assert pre_produce_hw < post_produce_hw, "wasn't able to produce to topic"

    @cluster(num_nodes=3)
    @parametrize(client_type="rpk")
    @parametrize(client_type="kafka_tools")
    def kafka_nodelete_topics_test(self, client_type):
        if client_type == "rpk":
            client = self.rpk
        elif client_type == "kafka_tools":
            client = self.kafka_tools
        else:
            assert False, "Unknown client type"

        test_topic = "nodelete_topic"
        self.kafka_tools.create_topic(
            TopicSpec(name=test_topic, partition_count=3))

        wait_until(lambda: test_topic in client.list_topics(),
                   timeout_sec=90,
                   backoff_sec=3)

        # Ensure topic can't be deleted via the Kafka API when it's
        # in the nodelete list.
        self.redpanda.set_cluster_config(
            {"kafka_nodelete_topics": [test_topic]})
        try:
            client.delete_topic(test_topic)
            assert False, "Call to delete topic must fail"
        except Exception:
            self.redpanda.logger.info(
                f"we were expecting delete_topic to fail", exc_info=True)
            pass

        # allow time for any erronous deletion to be propagated
        time.sleep(10)
        assert test_topic in client.list_topics()

        # Check that topics in the nodelete list can be removed then
        # deleted.
        self.redpanda.set_cluster_config({"kafka_nodelete_topics": []})
        client.delete_topic(test_topic)

        wait_until(lambda: test_topic not in client.list_topics(),
                   timeout_sec=90,
                   backoff_sec=3)


class InternalTopicProtectionLargeClusterTest(RedpandaTest):
    """
    Verifies that constraints against minimum RF do not apply against
    internally created topics
    """
    def __init__(self, *args, **kwargs):
        kwargs['num_brokers'] = 5
        kwargs['schema_registry_config'] = SchemaRegistryConfig()
        super().__init__(*args, extra_rp_conf={}, **kwargs)

        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _modify_cluster_config(self, upsert):
        patch_result = self.admin.patch_cluster_config(upsert=upsert)
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])

    def setUp(self):
        super().setUp()
        # Set default RF to 5
        # Set minimum Rf to 5
        self._modify_cluster_config({'default_topic_replications': 5})
        self._modify_cluster_config({'minimum_topic_replications': 5})

    @cluster(num_nodes=5)
    def test_schemas_topic(self):
        # Now access the SR, which should result in an RF of 3
        _ = get_subjects(self.redpanda.nodes, self.logger)

        topics = self.rpk.list_topics()
        assert "_schemas" in topics, f'_schemas not in topics {topics}'

        def schemas_topic_ready():
            partitions = list(self.rpk.describe_topic('_schemas'))
            return (len(partitions) > 0, partitions)

        partitions = wait_until_result(
            schemas_topic_ready,
            timeout_sec=30,
            backoff_sec=1,
            err_msg='_schemas topic never became ready')
        config = partitions[0]
        assert len(
            config.replicas
        ) == 3, f'Expected RF of 3 for _schemas but got {len(config.replicas)}'

        self.redpanda.restart_nodes(nodes=self.redpanda.nodes)

        num_found = self.redpanda.count_log_node(
            self.redpanda.nodes[0],
            "Topic {kafka/_schemas} has a replication factor less than specified"
        )
        assert num_found == 0, f'Expected to find 0 messages about _schemas but found {num_found}'

    @cluster(num_nodes=5)
    def test_consumer_offset_topic(self):
        self.rpk.create_topic("test")
        self.rpk.produce("test", key="key1", msg="Hi there")
        self.rpk.consume("test", group="TestGroup", n=1)
        config = list(self.rpk.describe_topic('__consumer_offsets'))[0]
        assert len(
            config.replicas
        ) == 3, f'Expected RF of 3 for __consumer_offsets but got {len(config.replicas)}'

        self.redpanda.restart_nodes(nodes=self.redpanda.nodes)

        num_found = self.redpanda.count_log_node(
            self.redpanda.nodes[0],
            "Topic {kafka/__consumer_offsets} has a replication factor less than specified"
        )
        assert num_found == 0, f'Expected to find 0 messages about _schemas but found {num_found}'
