# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from ducktape.utils.util import wait_until
import time

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.rpk_producer import RpkProducer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import SISettings
from rptest.util import wait_for_segments_removal
from ducktape.mark import parametrize


def topic_storage_purged(redpanda, topic: str):
    storage = redpanda.storage()
    return all(map(lambda n: topic not in n.ns["kafka"].topics, storage.nodes))


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

    @cluster(num_nodes=3)
    def topic_delete_test(self):
        def produce_until_partitions():
            self.kafka_tools.produce(self.topic, 1024, 1024)
            storage = self.redpanda.storage()
            return len(list(storage.partitions("kafka", self.topic))) == 9

        wait_until(lambda: produce_until_partitions(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Expected partition did not materialize")

        self.kafka_tools.delete_topic(self.topic)

        try:
            wait_until(lambda: topic_storage_purged(self.redpanda, self.topic),
                       timeout_sec=30,
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


class TopicDeleteCloudStorageTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        self.si_settings = SISettings(log_segment_size=1024 * 1024)
        super().__init__(
            test_context=test_context,
            # Use all nodes as brokers: enables each test to set num_nodes
            # and get a cluster of that size
            num_brokers=test_context.cluster.available().size(),
            si_settings=self.si_settings)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def _populate_topic(self):
        """
        Get system into state where there is data in both local
        and remote storage for the topic.
        """
        # Set retention to 5MB
        self.kafka_tools.alter_topic_config(
            self.topic, {'retention.local.target.bytes': 5 * 1024 * 1024})

        # Write out 10MB
        self.kafka_tools.produce(self.topic,
                                 record_size=4096,
                                 num_records=2560)

        # Wait for segments evicted from local storage
        for i in range(0, 3):
            wait_for_segments_removal(self.redpanda, self.topic, i, 5)

        # Confirm objects in remote storage
        before_objects = self.s3_client.list_objects(
            self.si_settings.cloud_storage_bucket)
        assert sum(1 for _ in before_objects) > 0

    @cluster(num_nodes=3)
    @parametrize(disable_delete=False)
    @parametrize(disable_delete=True)
    def topic_delete_cloud_storage_test(self, disable_delete):
        if disable_delete:
            # Set remote.delete=False before deleting: objects in
            # S3 should not be removed.
            self.kafka_tools.alter_topic_config(
                self.topic, {'redpanda.remote.delete': 'false'})

        self._populate_topic()

        objects_before = set(
            self.redpanda.s3_client.list_objects(
                self.si_settings.cloud_storage_bucket))

        # Delete topic
        self.kafka_tools.delete_topic(self.topic)

        # Local storage should be purged
        wait_until(lambda: topic_storage_purged(self.redpanda, self.topic),
                   timeout_sec=30,
                   backoff_sec=1)

        def remote_empty():
            """Return true if all objects removed from cloud storage"""
            after_objects = self.s3_client.list_objects(
                self.si_settings.cloud_storage_bucket)
            self.logger.debug("Objects after topic deletion:")
            empty = True
            for i in after_objects:
                self.logger.debug(f"  {i}")
                empty = False

            return empty

        if disable_delete:
            # Unfortunately there is no alternative ot sleeping here:
            # we need to confirm not only that objects aren't deleted
            # instantly, but that they also are not deleted after some
            # delay.
            time.sleep(10)
            objects_after = set(
                self.redpanda.s3_client.list_objects(
                    self.si_settings.cloud_storage_bucket))
            objects_deleted = objects_before - objects_after
            self.logger.debug(
                f"Objects deleted after topic deletion: {objects_deleted}")
            assert len(objects_deleted) == 0
        else:
            # The counter-test that deletion _doesn't_ happen in read replicas
            # is done as part of read_replica_e2e_test
            wait_until(remote_empty, timeout_sec=30, backoff_sec=1)

        # TODO: include transactional data so that we verify that .txrange
        # objects are deleted.

        # TODO: test deleting repeatedly while undergoing write load, to
        # catch the case where there are segments in S3 not reflected in the
        # manifest.

        # TODO: test making the S3 backend unavailable during the topic
        # delete.  The delete action should be acked, but internally
        # redpanda should keep retrying the S3 part until it succeeds.
        # - When we bring the S3 backend back it shoudl succeed
        # - If we restart redpanda before bringing the S3 backend back
        #   it should also succeed.

    @cluster(num_nodes=4)
    def partition_movement_test(self):
        """
        The unwary programmer might do S3 deletion from the
        remove_persistent_state function in Redpanda, but
        they must not!  This function is also used in the case
        when we move a partition and clean up after ourselves.

        This test verifies that partition movement removes local
        data but _not_ cloud data.
        """

        admin = Admin(self.redpanda)

        self._populate_topic()

        objects_before = set(o.Key
                             for o in self.redpanda.s3_client.list_objects(
                                 self.si_settings.cloud_storage_bucket))

        def get_nodes(partition):
            return list(r['node_id'] for r in partition['replicas'])

        nodes_before = get_nodes(admin.get_partitions(self.topic, 0))
        replacement_node = next(
            iter((set([self.redpanda.node_id(n)
                       for n in self.redpanda.nodes]) - set(nodes_before))))
        nodes_after = nodes_before[1:] + [
            replacement_node,
        ]
        new_assignments = list({'core': 0, 'node_id': n} for n in nodes_after)
        admin.set_partition_replicas(self.topic, 0, new_assignments)

        def move_complete():
            p = admin.get_partitions(self.topic, 0)
            return p["status"] == "done" and get_nodes(p) == nodes_after

        wait_until(move_complete, timeout_sec=30, backoff_sec=1)

        # Some additional time in case a buggy deletion path is async
        time.sleep(5)

        objects_after = set(o.Key
                            for o in self.redpanda.s3_client.list_objects(
                                self.si_settings.cloud_storage_bucket))

        deleted = objects_before - objects_after
        self.logger.debug(f"Objects deleted after partition move: {deleted}")
        assert len(deleted) == 0


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
