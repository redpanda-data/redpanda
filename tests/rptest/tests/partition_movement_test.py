# Copyright 2020 Redpanda Data, Inc.
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
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.services.honey_badger import HoneyBadger
from rptest.services.rpk_producer import RpkProducer
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST

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
        spec = TopicSpec(name=topic, partition_count=1, replication_factor=3)
        self.client().create_topic(spec)
        admin = Admin(self.redpanda)

        # choose a random topic-partition
        self.logger.info(f"selected topic-partition: {topic}-{partition}")

        # get the partition's replica set, including core assignments. the kafka
        # api doesn't expose core information, so we use the redpanda admin api.
        assignments = self._get_assignments(admin, topic, partition)
        self.logger.info(f"assignments for {topic}-{partition}: {assignments}")

        brokers = admin.get_brokers()
        # replace all node cores in assignment
        for assignment in assignments:
            for broker in brokers:
                if broker['node_id'] == assignment['node_id']:
                    assignment['core'] = random.randint(
                        0, broker["num_cores"] - 1)
        self.logger.info(
            f"new assignments for {topic}-{partition}: {assignments}")

        r = admin.set_partition_replicas(topic, partition, assignments)

        def status_done():
            info = admin.get_partitions(topic, partition)
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
            info = self._get_current_partitions(admin, topic, partition)
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
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)

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
            self.client().create_topic(spec)

        num_records = 1000
        produced = set(
            ((f"key-{i:08d}", f"record-{i:08d}") for i in range(num_records)))

        for spec in topics:
            self.logger.info(f"Producing to {spec}")
            producer = KafProducer(self.test_context, self.redpanda, spec.name,
                                   num_records)
            producer.start()
            self.logger.info(
                f"Finished producing to {spec}, waiting for producer...")
            producer.wait()
            producer.free()
            self.logger.info(f"Producer stop complete.")

        for _ in range(25):
            self._move_and_verify()

        for spec in topics:
            self.logger.info(f"Verifying records in {spec}")

            consumer = RpkConsumer(self.test_context,
                                   self.redpanda,
                                   spec.name,
                                   ignore_errors=False,
                                   retries=0)
            consumer.start()
            timeout = 30
            t1 = time.time()
            consumed = set()
            while consumed != produced:
                if time.time() > t1 + timeout:
                    self.logger.error(
                        f"Validation failed for topic {spec.name}.  Produced {len(produced)}, consumed {len(consumed)}"
                    )
                    self.logger.error(
                        f"Messages consumed but not produced: {sorted(consumed - produced)}"
                    )
                    self.logger.error(
                        f"Messages produced but not consumed: {sorted(produced - consumed)}"
                    )
                    assert set(consumed) == produced
                else:
                    time.sleep(5)
                    for m in consumer.messages:
                        self.logger.info(f"message: {m}")
                    consumed = set([(m['key'], m['value'])
                                    for m in consumer.messages])

            self.logger.info(f"Stopping consumer...")
            consumer.stop()
            self.logger.info(f"Awaiting consumer...")
            consumer.wait()
            self.logger.info(f"Freeing consumer...")
            consumer.free()

            self.logger.info(f"Finished verifying records in {spec}")

    @cluster(num_nodes=5, log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS)
    def test_dynamic(self):
        """
        Move partitions with active consumer / producer
        """
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={"default_topic_replications": 3})
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        for _ in range(25):
            self._move_and_verify()
        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=5, log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS)
    def test_move_consumer_offsets_intranode(self):
        """
        Exercise moving the consumer_offsets/0 partition between shards
        within the same nodes.  This reproduces certain bugs in the special
        handling of this topic.
        """
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={"default_topic_replications": 3})
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        admin = Admin(self.redpanda)
        topic = "__consumer_offsets"
        partition = 0

        for _ in range(25):
            assignments = self._get_assignments(admin, topic, partition)
            for a in assignments:
                # Bounce between core 0 and 1
                a['core'] = (a['core'] + 1) % 2
                admin.set_partition_replicas(topic, partition, assignments)
            self._wait_post_move(topic, partition, assignments, 360)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=5,
             log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS +
             RESTART_LOG_ALLOW_LIST)
    def test_bootstrapping_after_move(self):
        """
        Move partitions with active consumer / producer
        """
        self.start_redpanda(num_nodes=3)
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        # execute single move
        self._move_and_verify()
        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

        # snapshot offsets
        rpk = RpkTool(self.redpanda)
        partitions = rpk.describe_topic(spec.name)
        offset_map = {}
        for p in partitions:
            offset_map[p.id] = p.high_watermark

        # restart all the nodes
        self.redpanda.restart_nodes(self.redpanda.nodes)

        def offsets_are_recovered():

            return all([
                offset_map[p.id] == p.high_watermark
                for p in rpk.describe_topic(spec.name)
            ])

        wait_until(offsets_are_recovered, 30, 2)

    @cluster(num_nodes=3)
    def test_invalid_destination(self):
        """
        Check that requuests to move to non-existent locations are properly rejected.
        """

        self.start_redpanda(num_nodes=3)
        spec = TopicSpec(name="topic", partition_count=1, replication_factor=1)
        self.client().create_topic(spec)
        topic = spec.name
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
        spec = TopicSpec(name=name, partition_count=1, replication_factor=3)
        self.client().create_topic(spec)

        # Wait for the partition to have a leader (`rpk produce` errors
        # out if it tries to write data before this)
        def partition_ready():
            return KafkaCat(self.redpanda).get_partition_leader(
                name, 0)[0] is not None

        wait_until(partition_ready, timeout_sec=10, backoff_sec=0.5)

        # Write a substantial amount of data to the topic
        msg_size = 512 * 1024
        write_bytes = 512 * 1024 * 1024
        producer = RpkProducer(self._ctx,
                               self.redpanda,
                               name,
                               msg_size=msg_size,
                               msg_count=int(write_bytes / msg_size))
        t1 = time.time()
        producer.start()

        # This is an absurdly low expected throughput, but necessarily
        # so to run reliably on current test runners, which share an EBS
        # backend among many parallel tests.  10MB/s has been empirically
        # shown to be too high an expectation.
        expect_bps = 1 * 1024 * 1024
        expect_runtime = write_bytes / expect_bps
        producer.wait(timeout_sec=expect_runtime)

        self.logger.info(
            f"Write complete {write_bytes} in {time.time() - t1} seconds")

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
        rpk = RpkTool(self.redpanda)
        assert admin.get_partitions(name, 0)['status'] == "in_progress"
        rpk.alter_topic_config(name, "retention.ms", "3600000")

        # A deletion should succeed
        assert name in rpk.list_topics()
        assert admin.get_partitions(name, 0)['status'] == "in_progress"
        rpk.delete_topic(name)
        assert name not in rpk.list_topics()

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
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic, 1, 1)
        partition = 0
        num_records = 1000

        self.logger.info(f"Producing to {topic}")
        producer = KafProducer(self.test_context, self.redpanda, topic,
                               num_records)
        producer.start()
        self.logger.info(
            f"Finished producing to {topic}, waiting for producer...")
        producer.wait()
        producer.free()
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
        admin.set_partition_replicas(topic, partition, target_assignment)

        # check that the status is in progress

        def get_status():
            partition_info = admin.get_partitions(topic, partition)
            self.logger.info(
                f"current assignments for {topic}-{partition}: {partition_info}"
            )
            return partition_info["status"]

        wait_until(lambda: get_status() == 'in_progress', 10, 1)
        # delete the topic
        rpk.delete_topic(topic)
        # start the node back up
        self.redpanda.start_node(node)
        # create topic again
        rpk.create_topic(topic, 1, 1)
        wait_until(lambda: get_status() == 'done', 10, 1)
