# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

import time
import re

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.rpk_producer import RpkProducer
from rptest.services.metrics_check import MetricCheck


class TopicDeleteTest(RedpandaTest):
    """
    Verify that topic deletion cleans up storage.
    """
    topics = (TopicSpec(partition_count=3,
                        cleanup_policy=TopicSpec.CLEANUP_COMPACT), )

    def __init__(self, test_context):
        extra_rp_conf = dict(log_segment_size=262144, )

        super(TopicDeleteTest, self).__init__(test_context=test_context,
                                              num_brokers=3,
                                              extra_rp_conf=extra_rp_conf)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def produce_until_partitions(self):
        self.kafka_tools.produce(self.topic, 1024, 1024)
        storage = self.redpanda.storage()
        return len(list(storage.partitions("kafka", self.topic))) == 9

    def dump_storage_listing(self):
        for node in self.redpanda.nodes:
            self.logger.error(f"Storage listing on {node.name}:")
            for line in node.account.ssh_capture(
                    f"find {self.redpanda.DATA_DIR}"):
                self.logger.error(line.strip())

    @cluster(num_nodes=3)
    def topic_delete_test(self):
        wait_until(lambda: self.produce_until_partitions(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Expected partition did not materialize")

        self.kafka_tools.delete_topic(self.topic)

        def topic_storage_purged():
            storage = self.redpanda.storage()
            return all(
                map(lambda n: self.topic not in n.ns["kafka"].topics,
                    storage.nodes))

        try:
            wait_until(lambda: topic_storage_purged(),
                       timeout_sec=30,
                       backoff_sec=2,
                       err_msg="Topic storage was not removed")

        except:
            self.dump_storage_listing()
            raise

    @cluster(num_nodes=3, log_allow_list=[r'filesystem error: remove failed'])
    def topic_delete_orphan_files_test(self):
        wait_until(lambda: self.produce_until_partitions(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Expected partition did not materialize")

        down_node = self.redpanda.nodes[-1]
        try:
            # Make topic directory immutable to prevent deleting
            down_node.account.ssh(
                f"chattr +i {self.redpanda.DATA_DIR}/kafka/{self.topic}")

            self.kafka_tools.delete_topic(self.topic)

            def topic_deleted_on_all_nodes_except_one(redpanda, down_node,
                                                      topic_name):
                storage = redpanda.storage()
                log_not_removed_on_down = topic_name in next(
                    filter(lambda x: x.name == down_node.name,
                           storage.nodes)).ns["kafka"].topics
                logs_removed_on_others = all(
                    map(
                        lambda n: topic_name not in n.ns["kafka"].topics,
                        filter(lambda x: x.name != down_node.name,
                               storage.nodes)))
                return log_not_removed_on_down and logs_removed_on_others

            try:
                wait_until(
                    lambda: topic_deleted_on_all_nodes_except_one(
                        self.redpanda, down_node, self.topic),
                    timeout_sec=30,
                    backoff_sec=2,
                    err_msg=
                    "Topic storage was not removed from running nodes or removed from down node"
                )
            except:
                self.dump_storage_listing()
                raise

            self.redpanda.stop_node(down_node)
        finally:
            down_node.account.ssh(
                f"chattr -i {self.redpanda.DATA_DIR}/kafka/{self.topic}")

        self.redpanda.start_node(down_node)

        def topic_storage_purged():
            storage = self.redpanda.storage()
            return all(
                map(lambda n: self.topic not in n.ns["kafka"].topics,
                    storage.nodes))

        try:
            wait_until(lambda: topic_storage_purged(),
                       timeout_sec=30,
                       backoff_sec=2,
                       err_msg="Topic storage was not removed")
        except:
            self.dump_storage_listing()
            raise


class TopicDeleteStressTest(RedpandaTest):
    """
    The purpose of this test is to execute topic deletion during compaction process.

    The testing strategy is:
        1. Start to produce messaes
        2. Produce until compaction starting
        3. Delete topic
        4. Verify that all data for kafka namespace will be deleted
    """
    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_segment_size=1048576,
            compacted_log_segment_size=1048576,
            log_compaction_interval_ms=300,
            auto_create_topics_enabled=False,
        )

        super(TopicDeleteStressTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=4)
    def stress_test(self):
        for i in range(10):
            spec = TopicSpec(partition_count=2,
                             cleanup_policy=TopicSpec.CLEANUP_COMPACT)
            topic_name = spec.name
            self.client().create_topic(spec)

            producer = RpkProducer(self.test_context, self.redpanda,
                                   topic_name, 1024, 100000)
            producer.start()

            metrics = [
                MetricCheck(self.logger, self.redpanda, n,
                            'vectorized_storage_log_compacted_segment_total',
                            {}, sum) for n in self.redpanda.nodes
            ]

            def check_compaction():
                return all([
                    m.evaluate([
                        ('vectorized_storage_log_compacted_segment_total',
                         lambda a, b: b > 3)
                    ]) for m in metrics
                ])

            wait_until(check_compaction,
                       timeout_sec=120,
                       backoff_sec=5,
                       err_msg="Segments were not compacted")

            self.client().delete_topic(topic_name)

            try:
                producer.stop()
            except:
                # Should ignore exception form rpk
                pass
            producer.free()

            def topic_storage_purged():
                storage = self.redpanda.storage()
                return all(
                    map(lambda n: topic_name not in n.ns["kafka"].topics,
                        storage.nodes))

            try:
                wait_until(lambda: topic_storage_purged(),
                           timeout_sec=60,
                           backoff_sec=2,
                           err_msg="Topic storage was not removed")

            except:
                # On errors, dump listing of the storage location
                for node in self.redpanda.nodes:
                    self.logger.error(f"Storage listing on {node.name}:")
                    for line in node.account.ssh_capture(
                            f"find {self.redpanda.DATA_DIR}"):
                        self.logger.error(line.strip())

                raise
