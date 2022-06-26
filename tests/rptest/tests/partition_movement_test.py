# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time
import requests

from rptest.services.cluster import cluster
from rptest.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from ducktape.mark import ok_to_fail
from rptest.services.rw_workload import RWWorkload

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.services.honey_badger import HoneyBadger
from rptest.services.rpk_producer import RpkProducer
from rptest.services.w_workload import WWorkload
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings

# Errors we should tolerate when moving partitions around
PARTITION_MOVEMENT_LOG_ERRORS = [
    # e.g.  raft - [follower: {id: {1}, revision: {10}}] [group_id:3, {kafka/topic/2}] - recovery_stm.cc:422 - recovery append entries error: raft group does not exists on target broker
    "raft - .*raft group does not exist on target broker",
    # e.g.  raft - [group_id:3, {kafka/topic/2}] consensus.cc:2317 - unable to replicate updated configuration: raft::errc::replicated_entry_truncated
    "raft - .*unable to replicate updated configuration: .*",
    # e.g. recovery_stm.cc:432 - recovery append entries error: rpc::errc::client_request_timeout"
    "raft - .*recovery append entries error.*client_request_timeout"
]


class PartitionMovementTest(PartitionMovementMixin, EndToEndTest):
    """
    Basic partition movement tests. Each test builds a number of topics and then
    performs a series of random replica set changes. After each change a
    verification step is performed.

    TODO
    - Add tests with node failures
    - Add settings for scaling up tests
    - Add tests guarnateeing multiple segments
    """
    def __init__(self, ctx, *args, **kwargs):
        super(PartitionMovementTest, self).__init__(
            ctx,
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False
            },
            **kwargs)
        self._ctx = ctx

    @cluster(num_nodes=3)
    def test_moving_not_fully_initialized_partition(self):
        """
        Move partition before first leader is elected
        """
        self.start_redpanda(num_nodes=3)

        hb = HoneyBadger()
        # if failure injector is not enabled simply skip this test
        if not hb.is_enabled(self.redpanda.nodes[0]):
            return

        for n in self.redpanda.nodes:
            hb.set_exception(n, 'raftgen_service::failure_probes', 'vote')
        topic = "topic-1"
        partition = 0
        self._rpk_client.create_topic(topic, 1, 3)

        # choose a random topic-partition
        self.logger.info(f"selected topic-partition: {topic}-{partition}")

        # get the partition's replica set, including core assignments. the kafka
        # api doesn't expose core information, so we use the redpanda admin api.
        assignments = self._get_assignments(self._admin_client, topic,
                                            partition)
        self.logger.info(f"assignments for {topic}-{partition}: {assignments}")

        brokers = self._admin_client.get_brokers()
        # replace all node cores in assignment
        for assignment in assignments:
            for broker in brokers:
                if broker['node_id'] == assignment['node_id']:
                    assignment['core'] = random.randint(
                        0, broker["num_cores"] - 1)
        self.logger.info(
            f"new assignments for {topic}-{partition}: {assignments}")

        self._admin_client.set_partition_replicas(topic, partition,
                                                  assignments)

        def status_done():
            info = self._admin_client.get_partitions(topic, partition)
            self.logger.info(
                f"current assignments for {topic}-{partition}: {info}")
            converged = self._equal_assignments(info["replicas"], assignments)
            return converged and info["status"] == "done"

        # unset failures
        for n in self.redpanda.nodes:
            hb.unset_failures(n, 'raftgen_service::failure_probes', 'vote')
        # wait until redpanda reports complete
        wait_until(status_done, timeout_sec=60, backoff_sec=2)

        def derived_done():
            info = self._get_current_partitions(self._admin_client, topic,
                                                partition)
            self.logger.info(
                f"derived assignments for {topic}-{partition}: {info}")
            return self._equal_assignments(info, assignments)

        wait_until(derived_done, timeout_sec=60, backoff_sec=2)

    @cluster(num_nodes=3)
    def test_empty(self):
        """
        Move empty partitions.
        """
        self.start_redpanda(num_nodes=3)

        topics = []
        for partition_count in range(1, 5):
            for replication_factor in (1, 3):
                name = f"topic_{partition_count}_{replication_factor}"
                self._rpk_client.create_topic(name, partition_count,
                                              replication_factor)

        for _ in range(25):
            self._move_and_verify()

    @cluster(num_nodes=4, log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS)
    def test_static(self):
        """
        Move partitions with data, but no active producers or consumers.
        """
        self.logger.info(f"Starting redpanda...")
        self.start_redpanda(num_nodes=3)

        topics = []
        for partition_count in range(1, 5):
            for replication_factor in (1, 3):
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        self.logger.info(f"Creating topics...")
        for spec in topics:
            self._rpk_client.create_topic(spec.name, spec.partition_count,
                                          spec.replication_factor)

        num_records = 1000

        w_workload = WWorkload(self.test_context, self.redpanda)
        w_workload.start()
        for spec in topics:
            self.logger.info(f"Producing to {spec}")
            w_workload.remote_start(spec.name, self.redpanda.brokers(),
                                    spec.name, num_records, True)
            w_workload.remote_wait(spec.name)
            self.logger.info(
                f"Finished producing to {spec}, waiting for producer...")

        for _ in range(25):
            self._move_and_verify()

        for spec in topics:
            self.logger.info(f"Verifying records in {spec}")
            w_workload.remote_validate(spec.name, 10)
            self.logger.info(f"Finished verifying records in {spec}")

    def _get_scale_params(self):
        """
        Helper for reducing traffic generation parameters
        when running on a slower debug build of redpanda.
        """
        throughput = 100 if self.debug_mode else 1000
        records = 500 if self.debug_mode else 5000
        moves = 5 if self.debug_mode else 25

        return throughput, records, moves

    @cluster(num_nodes=4, log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS)
    def test_dynamic(self):
        """
        Move partitions with active consumer / producer
        """
        _, records, moves = self._get_scale_params()

        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={"default_topic_replications": 3})
        topic = "topic"
        self._rpk_client.create_topic(topic, 3, 3)

        rw_workload = RWWorkload(self.test_context, self.redpanda)
        rw_workload.start()
        rw_workload.remote_start(self.redpanda.brokers(), topic, 3)
        self.logger.info(f"waiting for 100 r&w operations to warm up")
        rw_workload.ensure_progress(100, 10)

        self.logger.info(f"initiate movement")
        for _ in range(moves):
            self._move_and_verify()

        self.logger.info(f"waiting for 100 r&w operations")
        rw_workload.ensure_progress(100, 10)
        self.logger.info(f"checkin if cleared {records} operations")
        rw_workload.has_cleared(records, 45)

        self.logger.info(f"stopping the workload")
        rw_workload.remote_stop()
        rw_workload.remote_wait()

    @cluster(num_nodes=5, log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS)
    def test_move_consumer_offsets_intranode(self):
        """
        Exercise moving the consumer_offsets/0 partition between shards
        within the same nodes.  This reproduces certain bugs in the special
        handling of this topic.
        """
        throughput, records, moves = self._get_scale_params()

        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={"default_topic_replications": 3})
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup()

        admin = Admin(self.redpanda)
        topic = "__consumer_offsets"
        partition = 0

        for _ in range(moves):
            assignments = self._get_assignments(admin, topic, partition)
            for a in assignments:
                # Bounce between core 0 and 1
                a['core'] = (a['core'] + 1) % 2
            admin.set_partition_replicas(topic, partition, assignments)
            self._wait_post_move(topic, partition, assignments, 360)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)

    @cluster(num_nodes=4,
             log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS +
             RESTART_LOG_ALLOW_LIST)
    def test_bootstrapping_after_move(self):
        """
        Move partitions with active consumer / producer
        """
        self.start_redpanda(num_nodes=3)
        topic = "topic"
        self._rpk_client.create_topic(topic, 3, 3)
        rw_workload = RWWorkload(self.test_context, self.redpanda)
        rw_workload.start()
        rw_workload.remote_start(self.redpanda.brokers(), topic, 3)
        self.logger.info(f"waiting for 100 r&w operations to warm up")
        # execute single move
        self._move_and_verify()
        self.logger.info(f"waiting for 100 r&w operations")
        rw_workload.ensure_progress(100, 10)
        self.logger.info(f"checkin if cleared 1000 operations")
        rw_workload.has_cleared(1000, 45)
        rw_workload.remote_stop()
        rw_workload.remote_wait()

        # snapshot offsets
        rpk = RpkTool(self.redpanda)
        partitions = rpk.describe_topic(topic)
        offset_map = {}
        for p in partitions:
            offset_map[p.id] = p.high_watermark

        # restart all the nodes
        self.redpanda.restart_nodes(self.redpanda.nodes)

        def offsets_are_recovered():
            return all([
                offset_map[p.id] == p.high_watermark
                for p in rpk.describe_topic(topic)
            ])

        wait_until(offsets_are_recovered, 30, 2)

    @cluster(num_nodes=3)
    def test_invalid_destination(self):
        """
        Check that requuests to move to non-existent locations are properly rejected.
        """

        self.start_redpanda(num_nodes=3)
        topic = "topic"
        self._rpk_client.create_topic(topic, 1, 1)
        partition = 0

        admin = Admin(self.redpanda)
        brokers = admin.get_brokers()
        assignments = self._get_assignments(admin, topic, partition)

        # Pick a node id where the topic currently isn't allocated
        valid_dest = list(
            set([b['node_id']
                 for b in brokers]) - set([a['node_id']
                                           for a in assignments]))[0]

        # This test will need updating on far-future hardware when core counts go higher
        invalid_shard = 1000
        invalid_dest = 30

        # A valid node but an invalid core
        assignments = [{"node_id": valid_dest, "core": invalid_shard}]
        try:
            r = admin.set_partition_replicas(topic, partition, assignments)
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == 400
        else:
            raise RuntimeError(f"Expected 400 but got {r.status_code}")

        # An invalid node but a valid core
        assignments = [{"node_id": invalid_dest, "core": 0}]
        try:
            r = admin.set_partition_replicas(topic, partition, assignments)
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == 400
        else:
            raise RuntimeError(f"Expected 400 but got {r.status_code}")

        # An syntactically invalid destination (float instead of int)
        # Reproducer for https://github.com/redpanda-data/redpanda/issues/2286
        assignments = [{"node_id": valid_dest, "core": 3.14}]
        try:
            r = admin.set_partition_replicas(topic, partition, assignments)
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == 400
        else:
            raise RuntimeError(f"Expected 400 but got {r.status_code}")

        assignments = [{"node_id": 3.14, "core": 0}]
        try:
            r = admin.set_partition_replicas(topic, partition, assignments)
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == 400
        else:
            raise RuntimeError(f"Expected 400 but got {r.status_code}")

        # Not unique replicas in replica set
        assignments = [{
            "node_id": valid_dest,
            "core": 0
        }, {
            "node_id": valid_dest,
            "core": 0
        }]
        try:
            r = admin.set_partition_replicas(topic, partition, assignments)
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == 400
        else:
            raise RuntimeError(f"Expected 400 but got {r.status_code}")

        # Finally a valid move
        assignments = [{"node_id": valid_dest, "core": 0}]
        r = admin.set_partition_replicas(topic, partition, assignments)
        assert r.status_code == 200

    @cluster(num_nodes=5)
    def test_overlapping_changes(self):
        """
        Check that while a movement is in flight, rules about
        overlapping operations are properly enforced.
        """

        self.start_redpanda(num_nodes=4)
        node_ids = {1, 2, 3, 4}

        # Create topic with enough data that inter-node movement
        # will take a while.
        name = f"movetest"
        self._rpk_client.create_topic(name, 1, 3)

        self._admin_client.await_stable_leader(name,
                                               partition=0,
                                               replication=3,
                                               timeout_s=10,
                                               backoff_s=0.5)

        msgs = 1024
        w_workload = WWorkload(self.test_context, self.redpanda)
        w_workload.start()
        self.logger.info(f"Producing to {name}")
        t1 = time.time()
        w_workload.remote_start(name, self.redpanda.brokers(), name, msgs,
                                False)
        w_workload.remote_wait(name)
        self.logger.info(
            f"Write complete {msgs} in {time.time() - t1} seconds")

        # - Admin API redirects writes but not reads.  Because we want synchronous
        #   status after submitting operations, send all operations to the controller
        #   leader.  This is not necessary for operations to work, just to simplify
        #   this test by letting it see synchronous status updates.
        # - Because we will later verify that a 503 is sent in response to
        #   a move request to an in_progress topic, set retry_codes=[] to
        #   disable default retries on 503.
        admin_node = self.redpanda.controller()
        admin = Admin(self.redpanda, default_node=admin_node, retry_codes=[])

        # Start an inter-node move, which should take some time
        # to complete because of recovery network traffic
        assignments = self._get_assignments(admin, name, 0)
        new_node = list(node_ids - set([a['node_id'] for a in assignments]))[0]
        self.logger.info(f"old assignments: {assignments}")
        old_assignments = assignments
        assignments = assignments[1:] + [{'node_id': new_node, 'core': 0}]
        self.logger.info(f"new assignments: {assignments}")
        r = admin.set_partition_replicas(name, 0, assignments)
        r.raise_for_status()
        assert admin.get_partitions(name, 0)['status'] == "in_progress"

        # Another move should fail
        assert admin.get_partitions(name, 0)['status'] == "in_progress"
        try:
            r = admin.set_partition_replicas(name, 0, old_assignments)
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == 503
        else:
            raise RuntimeError(f"Expected 503 but got {r.status_code}")

        # An update to partition properties should succeed
        # (issue https://github.com/redpanda-data/redpanda/issues/2300)
        assert admin.get_partitions(name, 0)['status'] == "in_progress"
        self._rpk_client.alter_topic_config(name, "retention.ms", "3600000")

        # A deletion should succeed
        assert name in self._rpk_client.list_topics()
        assert admin.get_partitions(name, 0)['status'] == "in_progress"
        self._rpk_client.delete_topic(name)
        assert name not in self._rpk_client.list_topics()

    @cluster(num_nodes=4)
    def test_deletion_stops_move(self):
        """
        Delete topic which partitions are being moved and check status after 
        topic is created again, old move 
        opeartions should not influcence newly created topic
        """
        self.start_redpanda(num_nodes=3)

        # create a single topic with replication factor of 1
        topic = 'test-topic'
        self._rpk_client.create_topic(topic, 1, 1)
        partition = 0
        num_records = 1000

        self.logger.info(f"Producing to {topic}")
        w_workload = WWorkload(self.test_context, self.redpanda)
        w_workload.start()
        w_workload.remote_start(topic, self.redpanda.brokers(), topic,
                                num_records, False)
        w_workload.remote_wait(topic)
        self.logger.info(f"Producer stop complete.")

        admin = Admin(self.redpanda)
        # get current assignments
        assignments = self._get_assignments(admin, topic, partition)
        assert len(assignments) == 1
        self.logger.info(f"assignments for {topic}-{partition}: {assignments}")
        brokers = admin.get_brokers()
        self.logger.info(f"available brokers: {brokers}")
        candidates = list(
            filter(lambda b: b['node_id'] != assignments[0]['node_id'],
                   brokers))
        replacement = random.choice(candidates)
        target_assignment = [{'node_id': replacement['node_id'], 'core': 0}]
        self.logger.info(
            f"target assignments for {topic}-{partition}: {target_assignment}")
        # shutdown target node to make sure that move will never complete
        node = self.redpanda.get_node(replacement['node_id'])
        self.redpanda.stop_node(node)

        # checking that a controller has leader (just in case
        # the stopped node happened to be previous leader)
        alive_hosts = [
            n.account.hostname for n in self.redpanda.nodes if n != node
        ]
        controller_leader = admin.await_stable_leader(
            topic="controller",
            partition=0,
            namespace="redpanda",
            hosts=alive_hosts,
            check=lambda node_id: node_id != self.redpanda.idx(node),
            timeout_s=30)
        controller_leader = self.redpanda.get_node(controller_leader)

        admin.set_partition_replicas(topic,
                                     partition,
                                     target_assignment,
                                     node=controller_leader)

        # check that the status is in progress

        def get_status():
            partition_info = admin.get_partitions(topic, partition)
            self.logger.info(
                f"current assignments for {topic}-{partition}: {partition_info}"
            )
            return partition_info["status"]

        wait_until(lambda: get_status() == 'in_progress', 10, 1)
        # delete the topic
        self._rpk_client.delete_topic(topic)
        # start the node back up
        self.redpanda.start_node(node)
        # create topic again
        self._rpk_client.create_topic(topic, 1, 1)
        wait_until(lambda: get_status() == 'done', 10, 1)


class SIPartitionMovementTest(PartitionMovementMixin, EndToEndTest):
    """
    Run partition movement tests with shadow indexing enabled
    """
    def __init__(self, ctx, *args, **kwargs):
        # Force shadow indexing to be used by most reads
        # in one test
        si_settings = SISettings(
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=10240,  # 10KiB
        )
        super(SIPartitionMovementTest, self).__init__(
            ctx,
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False,
                'delete_retention_ms': 1000,
            },
            si_settings=si_settings,
            **kwargs)
        self._ctx = ctx

    def _get_scale_params(self):
        """
        Helper for reducing traffic generation parameters
        when running on a slower debug build of redpanda.
        """
        throughput = 100 if self.debug_mode else 1000
        records = 500 if self.debug_mode else 5000
        moves = 5 if self.debug_mode else 25
        partitions = 1 if self.debug_mode else 10
        return throughput, records, moves, partitions

    @cluster(num_nodes=4)
    def test_shadow_indexing(self):
        """
        Test interaction between the shadow indexing and the partition movement.
        Partition movement generate partitions with different revision-ids and the
        archival/shadow-indexing subsystem is using revision to generate unique object
        keys inside the remote storage.
        """
        throughput, records, moves, partitions = self._get_scale_params()

        self.start_redpanda(num_nodes=3)
        self.topic = "topic"
        self._rpk_client.create_topic(self.topic, partitions, 3)

        rw_workload = RWWorkload(self.test_context, self.redpanda)
        rw_workload.start()
        rw_workload.remote_start(self.redpanda.brokers(), self.topic, 3)
        self.logger.info(f"waiting for 100 r&w operations to warm up")
        rw_workload.ensure_progress(100, 10)

        for _ in range(moves):
            self._move_and_verify()

        self.logger.info(f"waiting for 100 r&w operations")
        rw_workload.ensure_progress(100, 10)
        self.logger.info(f"checkin if cleared {records} operations")
        rw_workload.has_cleared(records, 45)
        self.logger.info(f"stopping the workload")
        rw_workload.remote_stop()
        rw_workload.remote_wait()

    @cluster(num_nodes=4)
    def test_cross_shard(self):
        """
        Test interaction between the shadow indexing and the partition movement.
        Move partitions with SI enabled between shards.
        """
        throughput, records, moves, partitions = self._get_scale_params()

        self.start_redpanda(num_nodes=3)
        self.topic = "topic"
        self._rpk_client.create_topic(self.topic, partitions, 3)

        rw_workload = RWWorkload(self.test_context, self.redpanda)
        rw_workload.start()
        rw_workload.remote_start(self.redpanda.brokers(), self.topic, 3)
        self.logger.info(f"waiting for 100 r&w operations to warm up")
        rw_workload.ensure_progress(100, 10)

        admin = Admin(self.redpanda)
        topic = self.topic
        partition = 0

        for _ in range(moves):
            assignments = self._get_assignments(admin, topic, partition)
            for a in assignments:
                # Bounce between core 0 and 1
                a['core'] = (a['core'] + 1) % 2
            admin.set_partition_replicas(topic, partition, assignments)
            self._wait_post_move(topic, partition, assignments, 360)

        self.logger.info(f"waiting for 100 r&w operations")
        rw_workload.ensure_progress(100, 10)
        self.logger.info(f"checkin if cleared {records} operations")
        rw_workload.has_cleared(records, 45)
        self.logger.info(f"stopping the workload")
        rw_workload.remote_stop()
        rw_workload.remote_wait()
