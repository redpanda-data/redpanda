# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import json
from typing import Optional

from ducktape.utils.util import wait_until
from ducktape.mark import ok_to_fail

from ducktape.mark import matrix, parametrize
from requests.exceptions import HTTPError

from rptest.utils.mode_checks import skip_debug_mode
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.offline_log_viewer import OfflineLogViewer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.rpk_producer import RpkProducer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import CloudStorageType, SISettings, get_cloud_storage_type
from rptest.util import wait_for_local_storage_truncate, firewall_blocked
from rptest.services.admin import Admin
from rptest.tests.partition_movement import PartitionMovementMixin


def get_kvstore_topic_key_counts(redpanda):
    """
    Count the keys in KVStore that relate to Kafka topics: this excludes all
    internal topic items: if no Kafka topics exist, this should be zero for
    all nodes.

    :returns: dict of Node to integer
    """

    viewer = OfflineLogViewer(redpanda)

    # Find the raft group IDs of internal topics
    admin = Admin(redpanda)
    internal_group_ids = set()
    for ntp in [
        ('redpanda', 'controller', 0),
        ('kafka_internal', 'id_allocator', 0),
    ]:
        namespace, topic, partition = ntp
        try:
            p = admin.get_partitions(topic=topic,
                                     namespace=namespace,
                                     partition=partition)
        except HTTPError as e:
            # OK if internal topic doesn't exist (e.g. id_allocator
            # doesn't have to exist)
            if e.response.status_code != 404:
                raise
        else:
            internal_group_ids.add(p['raft_group_id'])

    result = {}
    for n in redpanda.nodes:
        kvstore_data = viewer.read_kvstore(node=n)

        excess_keys = []
        for shard, items in kvstore_data.items():
            keys = [i['key'] for i in items]

            for k in keys:
                if k['keyspace'] == "cluster" or k['keyspace'] == "usage":
                    # Not a per-partition key
                    continue

                if k['data'].get('group', None) in internal_group_ids:
                    # One of the internal topics
                    continue

                if k['data'].get('ntp', {}).get('topic', None) == 'controller':
                    # Controller storage item
                    continue

                excess_keys.append(k)

            redpanda.logger.info(
                f"{n.name}.{shard} Excess Keys {json.dumps(excess_keys,indent=2)}"
            )

        key_count = len(excess_keys)
        result[n] = key_count

    return result


def topic_storage_purged(redpanda, topic_name):
    storage = redpanda.storage()
    logs_removed = all(
        map(lambda n: topic_name not in n.ns["kafka"].topics, storage.nodes))

    if not logs_removed:
        redpanda.logger.info(f"Files remain for topic {topic_name}:")
        for n in storage.nodes:
            ns = n.ns["kafka"]
            for topic_name, topic in ns.topics.items():
                for p_id, p in topic.partitions.items():
                    for f in p.files:
                        redpanda.logger.info(f"  {n.name}: {f}")

        return False

    # Once logs are removed, also do more expensive inspection of
    # kvstore to check that per-partition kvstore contents are
    # gone.  The user doesn't care about this, but it is important
    # to avoid bugs that would cause kvstore to bloat through
    # topic creation/destruction cycles.

    topic_key_counts = get_kvstore_topic_key_counts(redpanda)
    if any([v > 0 for v in topic_key_counts.values()]):
        redpanda.logger.info("Topic keys remain in KVStore")
        for node, count in topic_key_counts.items():
            redpanda.logger.info(f"  {node}: {count}")
        return False

    return True


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
    @parametrize(with_restart=False)
    @parametrize(with_restart=True)
    def topic_delete_test(self, with_restart):
        wait_until(lambda: self.produce_until_partitions(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Expected partition did not materialize")

        if with_restart:
            # Do a restart to encourage writes and flushes, especially to
            # the kvstore.
            self.redpanda.restart_nodes(self.redpanda.nodes)

        # Sanity check the kvstore checks: there should be at least one kvstore entry
        # per partition while the topic exists.
        assert sum(get_kvstore_topic_key_counts(
            self.redpanda).values()) >= self.topics[0].partition_count

        self.kafka_tools.delete_topic(self.topic)

        try:
            wait_until(lambda: topic_storage_purged(self.redpanda, self.topic),
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

        # Sanity check the kvstore checks: there should be at least one kvstore entry
        # per partition while the topic exists.
        assert sum(get_kvstore_topic_key_counts(
            self.redpanda).values()) >= self.topics[0].partition_count

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

        try:
            wait_until(lambda: topic_storage_purged(self.redpanda, self.topic),
                       timeout_sec=10,
                       backoff_sec=2,
                       err_msg="Topic storage was not removed")
        except:
            self.dump_storage_listing()
            raise


class TopicDeleteAfterMovementTest(RedpandaTest):
    """
    Verify that topic deleted after partition movement.
    """
    partition_count = 3
    topics = (TopicSpec(partition_count=partition_count), )

    def __init__(self, test_context):
        super(TopicDeleteAfterMovementTest,
              self).__init__(test_context=test_context, num_brokers=4)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def movement_done(self, partition, assignments):
        results = []
        for n in self.redpanda._started:
            info = self.admin.get_partitions(self.topic, partition, node=n)
            self.logger.info(
                f"current assignments for {self.topic}-{partition}: {info}")
            converged = PartitionMovementMixin._equal_assignments(
                info["replicas"], assignments)
            results.append(converged and info["status"] == "done")
        return all(results)

    def move_topic(self, assignments):
        for partition in range(3):

            def get_nodes(partition):
                return list(r['node_id'] for r in partition['replicas'])

            nodes_before = set(
                get_nodes(self.admin.get_partitions(self.topic, partition)))
            nodes_after = {r['node_id'] for r in assignments}
            if nodes_before == nodes_after:
                continue
            self.admin.set_partition_replicas(self.topic, partition,
                                              assignments)

            wait_until(lambda: self.movement_done(partition, assignments),
                       timeout_sec=60,
                       backoff_sec=2)

    @cluster(num_nodes=4, log_allow_list=[r'filesystem error: remove failed'])
    def topic_delete_orphan_files_after_move_test(self):

        # Write out 10MB per partition
        self.kafka_tools.produce(self.topic,
                                 record_size=4096,
                                 num_records=2560 * self.partition_count)

        self.admin = Admin(self.redpanda)

        # Move every partition to nodes 1,2,3
        assignments = [dict(node_id=n, core=0) for n in [1, 2, 3]]
        self.move_topic(assignments)

        down_node = self.redpanda.nodes[0]
        try:
            # Make topic directory immutable to prevent deleting
            down_node.account.ssh(
                f"chattr +i {self.redpanda.DATA_DIR}/kafka/{self.topic}")

            # Move every partition from node 1 to node 4
            new_assignments = [dict(node_id=n, core=0) for n in [2, 3, 4]]
            self.move_topic(new_assignments)

            def topic_exist_on_every_node(redpanda, topic_name):
                storage = redpanda.storage()
                exist_on_every = all(
                    map(lambda n: topic_name in n.ns["kafka"].topics,
                        storage.nodes))
                return exist_on_every

            wait_until(
                lambda: topic_exist_on_every_node(self.redpanda, self.topic),
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Topic doesn't exist on some node")

            self.redpanda.stop_node(down_node)
        finally:
            down_node.account.ssh(
                f"chattr -i {self.redpanda.DATA_DIR}/kafka/{self.topic}")

        self.redpanda.start_node(down_node)

        def topic_deleted_on_down_node_and_exist_on_others(
                redpanda, down_node, topic_name):
            storage = redpanda.storage()
            log_removed_on_down = topic_name not in next(
                filter(lambda x: x.name == down_node.name,
                       storage.nodes)).ns["kafka"].topics
            logs_not_removed_on_others = all(
                map(lambda n: topic_name in n.ns["kafka"].topics,
                    filter(lambda x: x.name != down_node.name, storage.nodes)))
            return log_removed_on_down and logs_not_removed_on_others

        wait_until(
            lambda: topic_deleted_on_down_node_and_exist_on_others(
                self.redpanda, down_node, self.topic),
            timeout_sec=30,
            backoff_sec=2,
            err_msg=
            "Topic storage was not removed on down node or removed on other")


class TopicDeleteCloudStorageTest(RedpandaTest):
    partition_count = 3
    topics = (TopicSpec(partition_count=partition_count,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        self.si_settings = SISettings(test_context,
                                      log_segment_size=1024 * 1024,
                                      fast_uploads=True)
        super().__init__(
            test_context=test_context,
            # Use all nodes as brokers: enables each test to set num_nodes
            # and get a cluster of that size
            num_brokers=test_context.cluster.available().size(),
            si_settings=self.si_settings)

        self._s3_port = self.si_settings.cloud_storage_api_endpoint_port

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def _populate_topic(self, topic_name, nodes: Optional[list] = None):
        """
        Get system into state where there is data in both local
        and remote storage for the topic.
        """
        # Set retention to 5MB
        local_retention = 5 * 1024 * 1024
        self.kafka_tools.alter_topic_config(
            topic_name, {'retention.local.target.bytes': local_retention})

        # Write out 10MB per partition
        self.kafka_tools.produce(topic_name,
                                 record_size=4096,
                                 num_records=2560 * self.partition_count)

        # Wait for segments evicted from local storage
        for i in range(0, self.partition_count):
            wait_for_local_storage_truncate(self.redpanda,
                                            topic_name,
                                            partition_idx=i,
                                            target_bytes=local_retention,
                                            timeout_sec=30,
                                            nodes=nodes)

        # Confirm objects in remote storage
        objects = self.cloud_storage_client.list_objects(
            self.si_settings.cloud_storage_bucket, topic=topic_name)
        assert sum(1 for _ in objects) > 0

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/8496
    @skip_debug_mode  # Rely on timely uploads during leader transfers
    @cluster(num_nodes=3)
    def topic_delete_installed_snapshots_test(self):
        """
        Test the case where a partition had remote snapshots installed prior
        to deletion: this aims to expose bugs in the snapshot code vs the
        shutdown code.
        """
        victim_node = self.redpanda.nodes[-1]
        other_nodes = self.redpanda.nodes[0:2]

        self.logger.info(f"Stopping victim node {victim_node.name}")
        self.redpanda.stop_node(victim_node)

        # Before populating the topic + waiting for eviction from local
        # disk, stop one node.  This node will later get a snapshot installed
        # when it comes back online
        self.logger.info(
            f"Populating topic and waiting for nodes {[n.name for n in other_nodes]}"
        )
        self._populate_topic(self.topic, nodes=other_nodes)

        self.logger.info(f"Starting victim node {victim_node.name}")
        self.redpanda.start_node(victim_node)

        # TODO wait for victim_node to see hwm catch up
        time.sleep(10)

        # Write more: this should prompt the victim node to do some prefix truncations
        # after having installed a snapshot
        self._populate_topic(self.topic)

        self.kafka_tools.delete_topic(self.topic)
        wait_until(lambda: topic_storage_purged(self.redpanda, self.topic),
                   timeout_sec=30,
                   backoff_sec=1)

        wait_until(lambda: self._topic_remote_deleted(self.topic),
                   timeout_sec=30,
                   backoff_sec=1)

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/9629
    @skip_debug_mode  # Rely on timely uploads during leader transfers
    @cluster(
        num_nodes=3,
        log_allow_list=[
            'exception while executing partition operation: {type: deletion'
        ])
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def topic_delete_unavailable_test(self, cloud_storage_type):
        """
        Test deleting while the S3 backend is unavailable: we should see
        that local deletion proceeds, and remote deletion eventually
        gives up.
        """
        self._populate_topic(self.topic)
        keys_before = set(
            o.key for o in self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket, topic=self.topic))
        assert len(keys_before) > 0

        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            self.kafka_tools.delete_topic(self.topic)

            # From user's point of view, deletion succeeds
            assert self.topic not in self.kafka_tools.list_topics()

            # Local storage deletion should proceed even if remote can't
            wait_until(lambda: topic_storage_purged(self.redpanda, self.topic),
                       timeout_sec=30,
                       backoff_sec=1)

            # Erase timeout is hardcoded 60 seconds, wait long enough
            # for it to give up.
            time.sleep(90)

            # Confirm our firewall block is really working, nothing was deleted
            keys_after = set(
                o.key for o in self.redpanda.cloud_storage_client.list_objects(
                    self.si_settings.cloud_storage_bucket))
            assert len(keys_after) >= len(keys_before)

        # Check that after the controller backend experiences errors trying
        # to execute partition deletion, it is still happily able to execute
        # other operations on unrelated topics, i.e. has not stalled applying.
        next_topic = "next_topic"
        self.kafka_tools.create_topic(
            TopicSpec(name=next_topic,
                      partition_count=self.partition_count,
                      cleanup_policy=TopicSpec.CLEANUP_DELETE))
        self._populate_topic(next_topic)
        after_keys = set(
            o.key for o in self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket, topic=next_topic))
        assert len(after_keys) > 0

        self.kafka_tools.delete_topic(next_topic)
        wait_until(lambda: topic_storage_purged(self.redpanda, next_topic),
                   timeout_sec=30,
                   backoff_sec=1)

        wait_until(lambda: self._topic_remote_deleted(next_topic),
                   timeout_sec=30,
                   backoff_sec=1)

        # The controller gave up on deleting the original topic, objects
        # are left behind in the object store.  This condition can be updated
        # if we ever implement a mechanism for automatically GCing objects after
        # a drop in the object storage backend.
        final_objects = set(
            self.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket, topic=self.topic))
        assert len(final_objects) >= len(keys_before)

    def _topic_remote_deleted(self, topic_name: str):
        """Return true if all objects removed from cloud storage"""
        after_objects = self.cloud_storage_client.list_objects(
            self.si_settings.cloud_storage_bucket, topic=topic_name)
        self.logger.debug(f"Objects after topic {topic_name} deletion:")
        empty = True
        for i in after_objects:
            self.logger.debug(f"  {i}")
            empty = False

        return empty

    @skip_debug_mode  # Rely on timely uploads during leader transfers
    @cluster(num_nodes=3)
    @matrix(disable_delete=[False, True],
            cloud_storage_type=get_cloud_storage_type())
    def topic_delete_cloud_storage_test(self, disable_delete,
                                        cloud_storage_type):
        if disable_delete:
            # Set remote.delete=False before deleting: objects in
            # S3 should not be removed.
            self.kafka_tools.alter_topic_config(
                self.topic, {'redpanda.remote.delete': 'false'})

        self._populate_topic(self.topic)

        keys_before = set(
            o.key for o in self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket))

        # Delete topic
        self.kafka_tools.delete_topic(self.topic)

        # Local storage should be purged
        wait_until(lambda: topic_storage_purged(self.redpanda, self.topic),
                   timeout_sec=30,
                   backoff_sec=1)

        if disable_delete:
            # Unfortunately there is no alternative to sleeping here:
            # we need to confirm not only that objects aren't deleted
            # instantly, but that they also are not deleted after some
            # delay.
            time.sleep(10)
            keys_after = set(
                o.key for o in self.redpanda.cloud_storage_client.list_objects(
                    self.si_settings.cloud_storage_bucket))
            objects_deleted = keys_before - keys_after
            self.logger.debug(
                f"Objects deleted after topic deletion: {objects_deleted}")
            assert len(objects_deleted) == 0
        else:
            # The counter-test that deletion _doesn't_ happen in read replicas
            # is done as part of read_replica_e2e_test
            wait_until(lambda: self._topic_remote_deleted(self.topic),
                       timeout_sec=30,
                       backoff_sec=1)

        # TODO: include transactional data so that we verify that .txrange
        # objects are deleted.

        # TODO: test deleting repeatedly while undergoing write load, to
        # catch the case where there are segments in S3 not reflected in the
        # manifest.

    @skip_debug_mode  # Rely on timely uploads during leader transfers
    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def partition_movement_test(self, cloud_storage_type):
        """
        The unwary programmer might do S3 deletion from the
        remove_persistent_state function in Redpanda, but
        they must not!  This function is also used in the case
        when we move a partition and clean up after ourselves.

        This test verifies that partition movement removes local
        data but _not_ cloud data.
        """

        admin = Admin(self.redpanda)

        self._populate_topic(self.topic)

        keys_before = set(
            o.key for o in self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket))

        def get_nodes(partition):
            return list(r['node_id'] for r in partition['replicas'])

        nodes_before = get_nodes(admin.get_partitions(self.topic, 0))
        replacement_node = next(
            iter((set([self.redpanda.idx(n)
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

        keys_after = set(
            o.key for o in self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket))

        deleted = keys_before - keys_after
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

            try:
                wait_until(
                    lambda: topic_storage_purged(self.redpanda, topic_name),
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
