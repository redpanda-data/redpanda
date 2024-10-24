# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import copy
import random
import time
import signal
import requests

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from ducktape.mark import ignore, matrix

from rptest.utils.mode_checks import skip_debug_mode, skip_fips_mode
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda_installer import InstallOptions, RedpandaInstaller
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.util import wait_until_result
from rptest.services.honey_badger import HoneyBadger
from rptest.services.rpk_producer import RpkProducer
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, PREV_VERSION_LOG_ALLOW_LIST, CloudStorageType, SISettings, get_cloud_storage_type

# Errors we should tolerate when moving partitions around
PARTITION_MOVEMENT_LOG_ERRORS = [
    # e.g.  raft - [follower: {id: {1}, revision: {10}}] [group_id:3, {kafka/topic/2}] - recovery_stm.cc:422 - recovery append entries error: raft group does not exist on target broker
    "raft - .*raft group does not exist on target broker"
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
                'enable_leader_balancer': False,
                'partition_autobalancing_mode': 'off'
            },
            **kwargs)
        self._ctx = ctx

    @cluster(num_nodes=3,
             log_allow_list=PREV_VERSION_LOG_ALLOW_LIST + ["FailureInjector"])
    @matrix(num_to_upgrade=[0, 2])
    def test_moving_not_fully_initialized_partition(self, num_to_upgrade):
        """
        Move partition before first leader is elected
        """
        # NOTE: num_to_upgrade=0 indicates that we're just testing HEAD without
        # any mixed versions.
        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions,
            num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)

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

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        # choose a random topic-partition
        self.logger.info(f"selected topic-partition: {topic}/{partition}")

        # get the partition's replica set, including core assignments. the kafka
        # api doesn't expose core information, so we use the redpanda admin api.
        assignments = self._get_assignments(admin, topic, partition)
        self.logger.info(f"assignments for {topic}/{partition}: {assignments}")

        brokers = admin.get_brokers()
        # replace all node cores in assignment
        for assignment in assignments:
            for broker in brokers:
                if broker['node_id'] == assignment['node_id']:
                    assignment['core'] = random.randint(
                        0, broker["num_cores"] - 1)

        self._set_partition_assignments(
            topic,
            partition,
            assignments,
            admin=admin,
            node_local_core_assignment=node_local_core_assignment)

        def node_assignments_converged():
            info = admin.get_partitions(topic, partition)
            node_assignments = [{
                "node_id": r["node_id"]
            } for r in info["replicas"]]
            self.logger.info(
                f"node assignments for {topic}/{partition}: {node_assignments}, "
                f"partition status: {info['status']}")
            converged = self._equal_assignments(node_assignments, assignments)
            return converged and info["status"] == "done"

        # unset failures
        for n in self.redpanda.nodes:
            hb.unset_failures(n, 'raftgen_service::failure_probes', 'vote')
        # wait until redpanda reports complete
        wait_until(node_assignments_converged, timeout_sec=60, backoff_sec=2)

        def cores_converged():
            info = self._get_current_node_cores(admin, topic, partition)
            self.logger.info(
                f"derived assignments for {topic}/{partition}: {info}")
            return self._equal_assignments(info, assignments)

        wait_until(cores_converged, timeout_sec=60, backoff_sec=2)

    @cluster(num_nodes=3, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_empty(self, num_to_upgrade):
        """
        Move empty partitions.
        """
        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions,
            num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)

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

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        for _ in range(25):
            self._move_and_verify(
                node_local_core_assignment=node_local_core_assignment)

    @cluster(num_nodes=4,
             log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS +
             PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_static(self, num_to_upgrade):
        """
        Move partitions with data, but no active producers or consumers.
        """
        self.logger.info(f"Starting redpanda...")
        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions,
            num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)

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

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        for _ in range(25):
            self._move_and_verify(
                node_local_core_assignment=node_local_core_assignment)

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

    def _get_scale_params(self):
        """
        Helper for reducing traffic generation parameters
        when running on a slower debug build of redpanda.
        """
        throughput = 100 if self.debug_mode else 1000
        records = 500 if self.debug_mode else 5000
        moves = 5 if self.debug_mode else 25

        return throughput, records, moves

    @cluster(num_nodes=5,
             log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS +
             PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_dynamic(self, num_to_upgrade):
        """
        Move partitions with active consumer / producer
        """
        throughput, records, moves = self._get_scale_params()

        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions,
            num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3,
                            install_opts=install_opts,
                            extra_rp_conf={"default_topic_replications": 3})
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup()
        for _ in range(moves):
            self._move_and_verify(
                node_local_core_assignment=node_local_core_assignment)
        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)

    @cluster(num_nodes=5,
             log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS +
             PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_move_consumer_offsets_intranode(self, num_to_upgrade):
        """
        Exercise moving the consumer_offsets/0 partition between shards
        within the same nodes.  This reproduces certain bugs in the special
        handling of this topic.
        """
        throughput, records, moves = self._get_scale_params()

        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions()
        if test_mixed_versions:
            # Start at a version that supports consumer groups.
            install_opts = InstallOptions(install_previous_version=True,
                                          num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3,
                            install_opts=install_opts,
                            extra_rp_conf={"default_topic_replications": 3})

        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup()

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        admin = Admin(self.redpanda)
        topic = "__consumer_offsets"
        partition = 0

        for _ in range(moves):
            assignments = self._get_current_node_cores(admin, topic, partition)
            for a in assignments:
                # Bounce between core 0 and 1
                a['core'] = (a['core'] + 1) % 2
            self._set_partition_assignments(
                topic,
                partition,
                assignments,
                admin=admin,
                node_local_core_assignment=node_local_core_assignment)
            self._wait_post_move(topic, partition, assignments, 360)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)

    @cluster(num_nodes=5,
             log_allow_list=PARTITION_MOVEMENT_LOG_ERRORS +
             RESTART_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_bootstrapping_after_move(self, num_to_upgrade):
        """
        Move partitions with active consumer / producer
        """
        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions,
            num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        # execute single move
        self._move_and_verify(
            node_local_core_assignment=node_local_core_assignment)
        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

        # snapshot offsets
        rpk = RpkTool(self.redpanda)

        def has_offsets_for_all_partitions():
            # NOTE: partitions may not be returned if their fields can't be
            # populated, e.g. during leadership changes.
            partitions = list(rpk.describe_topic(spec.name))
            if len(partitions) == 3:
                return (True, partitions)
            return (False, None)

        partitions = wait_until_result(has_offsets_for_all_partitions, 30, 2)

        offset_map = {}
        for p in partitions:
            offset_map[p.id] = p.high_watermark

        # restart all the nodes
        self.redpanda.restart_nodes(self.redpanda.nodes)

        def offsets_are_recovered():
            partitions_after = list(rpk.describe_topic(spec.name))
            if len(partitions_after) != 3:
                return False
            return all([
                offset_map[p.id] == p.high_watermark for p in partitions_after
            ])

        wait_until(offsets_are_recovered, 30, 2)

    @cluster(num_nodes=3, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_invalid_destination(self, num_to_upgrade):
        """
        Check that requuests to move to non-existent locations are properly rejected.
        """

        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions,
            num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)
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

        def dispatch_and_assert_status_code(assignments, expected_sc):
            try:
                r = admin.set_partition_replicas(topic, partition, assignments)
                status_code = r.status_code
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
            assert status_code == expected_sc, \
                f"Expected {expected_sc} but got {status_code}"

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        # A valid node but an invalid core
        assignments = [{"node_id": valid_dest, "core": invalid_shard}]
        dispatch_and_assert_status_code(
            assignments,
            expected_sc=200 if node_local_core_assignment else 400)

        # An invalid node but a valid core
        assignments = [{"node_id": invalid_dest, "core": 0}]
        dispatch_and_assert_status_code(assignments, expected_sc=400)

        # An syntactically invalid destination (float instead of int)
        # Reproducer for https://github.com/redpanda-data/redpanda/issues/2286
        assignments = [{"node_id": valid_dest, "core": 3.14}]
        dispatch_and_assert_status_code(assignments, expected_sc=400)

        assignments = [{"node_id": 3.14, "core": 0}]
        dispatch_and_assert_status_code(assignments, expected_sc=400)

        # Not unique replicas in replica set
        assignments = [{
            "node_id": valid_dest,
            "core": 0
        }, {
            "node_id": valid_dest,
            "core": 0
        }]
        dispatch_and_assert_status_code(assignments, expected_sc=400)

        # Finally a valid move
        assignments = [{"node_id": valid_dest, "core": 0}]
        dispatch_and_assert_status_code(assignments, expected_sc=200)

    @cluster(num_nodes=5, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_overlapping_changes(self, num_to_upgrade):
        """
        Check that while a movement is in flight, rules about
        overlapping operations are properly enforced.
        """

        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions()
        if test_mixed_versions:
            # Start at a version that supports the RpkProducer workload.
            # TODO: use 'install_previous_version' once it becomes the prior
            # feature version.
            install_opts = InstallOptions(
                install_previous_version=test_mixed_versions)
        self.start_redpanda(num_nodes=4, install_opts=install_opts)

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
        assignments = self._get_assignments(admin, name, 0, with_cores=True)
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

    @cluster(num_nodes=4, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2])
    def test_deletion_stops_move(self, num_to_upgrade):
        """
        Delete topic which partitions are being moved and check status after
        topic is created again, old move
        opeartions should not influcence newly created topic
        """
        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions,
            num_to_upgrade=num_to_upgrade)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)

        # create a single topic with replication factor of 1
        topic = 'test-topic'
        self.rpk_client().create_topic(topic, 1, 1)
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
        self.logger.info(f"assignments for {topic}/{partition}: {assignments}")
        brokers = admin.get_brokers()
        self.logger.info(f"available brokers: {brokers}")
        candidates = list(
            filter(lambda b: b['node_id'] != assignments[0]['node_id'],
                   brokers))
        replacement = random.choice(candidates)
        target_assignment = [{'node_id': replacement['node_id'], 'core': 0}]
        self.logger.info(
            f"target assignments for {topic}/{partition}: {target_assignment}")
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
            try:
                partition_info = admin.get_partitions(topic, partition)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.logger.info(
                        f"topic {topic}/{partition} not found, retrying")
                    return None
                else:
                    raise e
            self.logger.info(
                f"current assignments for {topic}/{partition}: {partition_info}"
            )
            return partition_info["status"]

        wait_until(lambda: get_status() == 'in_progress', 10, 1)
        # delete the topic
        self.rpk_client().delete_topic(topic)

        # start the node back up
        self.redpanda.start_node(node)
        # create topic again
        self.rpk_client().create_topic(topic, 1, 1)
        wait_until(lambda: get_status() == 'done', 10, 1)

    @cluster(num_nodes=5)
    def test_down_replicate(self):
        """
        Test changing replication factor from 3 -> 1
        """
        throughput, records, _ = self._get_scale_params()
        partition_count = 5
        self.start_redpanda(num_nodes=3)
        admin = Admin(self.redpanda)

        spec = TopicSpec(partition_count=partition_count, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup()

        for partition in range(0, partition_count):
            assignments = self._get_assignments(admin, self.topic, partition)
            new_assignment = [assignments[0]]
            self._set_partition_assignments(self.topic, partition,
                                            new_assignment, admin)
            self._wait_post_move(self.topic, partition, new_assignment, 60)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_availability_when_one_node_down(self):
        """
        Test availability during partition reconfiguration.

        The test validates if a partition is available when one of its replicas
        is down during reconfiguration.
        """
        throughput, records, _ = self._get_scale_params()
        partition_count = 1
        self.start_redpanda(num_nodes=4)
        admin = Admin(self.redpanda)
        spec = TopicSpec(partition_count=partition_count, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        partition_id = random.randint(0, partition_count - 1)

        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup()

        assignments = self._get_assignments(admin, self.topic, partition_id)
        self.logger.info(
            f"current assignment for {self.topic}/{partition_id}: {assignments}"
        )
        current_replicas = set()
        for a in assignments:
            current_replicas.add(a['node_id'])
        # replace single replica
        brokers = admin.get_brokers()
        to_select = [
            b for b in brokers if b['node_id'] not in current_replicas
        ]
        selected = random.choice(to_select)
        # replace one of the assignments
        assignments[0] = {'node_id': selected['node_id'], 'core': 0}
        self.logger.info(
            f"new assignment for {self.topic}/{partition_id}: {assignments}")

        to_stop = assignments[1]['node_id']
        # stop one of the not replaced nodes
        self.logger.info(f"stopping node: {to_stop}")
        self.redpanda.stop_node(self.redpanda.get_node(to_stop))

        def new_controller_available():
            controller_id = admin.get_partition_leader(namespace="redpanda",
                                                       topic="controller",
                                                       partition=0)
            self.logger.debug(
                f"current controller: {controller_id}, stopped node: {to_stop}"
            )
            return controller_id != -1 and controller_id != to_stop

        wait_until(new_controller_available, 30, 1)
        # ask partition to move
        self._set_partition_assignments(self.topic, partition_id, assignments,
                                        admin)

        def status_done():
            info = admin.get_partitions(self.topic, partition_id)
            self.logger.info(
                f"current assignments for {self.topic}/{partition_id}: {info}")
            converged = self._equal_assignments(info["replicas"], assignments)
            return converged and info["status"] == "done"

        # wait until redpanda reports complete
        wait_until(status_done, timeout_sec=40, backoff_sec=2)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)

    @cluster(num_nodes=4)
    @matrix(frequent_controller_snapshots=[False, True])
    def test_stale_node(self, frequent_controller_snapshots):
        """
        Test that a stale node rejoining the cluster can correctly restore info about
        in-progress partition movements and finish them.
        """

        partition_count = 1
        self.start_redpanda(num_nodes=4)

        admin = Admin(self.redpanda)

        if frequent_controller_snapshots:
            self.redpanda.set_cluster_config(
                {"controller_snapshot_max_age_sec": 1})

        spec = TopicSpec(partition_count=partition_count, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        partition_id = 0

        assignments = self._get_assignments(admin, self.topic, partition_id)
        self.logger.info(
            f"current assignment for {self.topic}/{partition_id}: {assignments}"
        )
        current_replicas = set()
        for a in assignments:
            current_replicas.add(a['node_id'])
        # replace single replica
        brokers = admin.get_brokers()
        to_select = [
            b for b in brokers if b['node_id'] not in current_replicas
        ]
        selected = random.choice(to_select)
        # replace one of the assignments
        replaced = assignments[0]['node_id']
        assignments[0] = {'node_id': selected['node_id'], 'core': 0}
        self.logger.info(
            f"target assignment for {self.topic}/{partition_id}: {assignments}"
        )

        # stop the replaced node
        self.logger.info(f"stopping node: {replaced}")
        self.redpanda.signal_redpanda(self.redpanda.get_node(replaced),
                                      signal=signal.SIGSTOP)

        def new_controller_available():
            controller_id = admin.get_partition_leader(namespace="redpanda",
                                                       topic="controller",
                                                       partition=0)
            self.logger.debug(
                f"current controller: {controller_id}, stopped node: {replaced}"
            )
            return controller_id != -1 and controller_id != replaced

        wait_until(new_controller_available, 30, 1)

        # ask partition to move
        self._set_partition_assignments(self.topic, partition_id, assignments,
                                        admin)

        def status_done():
            info = admin.get_partitions(self.topic, partition_id)
            self.logger.info(
                f"current assignments for {self.topic}/{partition_id}: {info}")
            converged = self._equal_assignments(info["replicas"], assignments)
            return converged and info["status"] == "done"

        # wait until redpanda reports complete
        wait_until(status_done, timeout_sec=40, backoff_sec=2)

        self.logger.info(
            "first movement done, scheduling second movement back")

        # bring replaced node back
        assignments[0] = {'node_id': replaced, 'core': 0}
        self._set_partition_assignments(self.topic, partition_id, assignments,
                                        admin)

        time.sleep(5)
        self.logger.info(f"unfreezing node: {replaced} again")
        self.redpanda.signal_redpanda(self.redpanda.get_node(replaced),
                                      signal=signal.SIGCONT)

        wait_until(status_done, timeout_sec=40, backoff_sec=2)

    @cluster(num_nodes=7)
    def test_movement_tracking_api(self):
        """
        Test verifying correctness of `/reconfigurations` endpoint
        """
        throughput, records, _ = self._get_scale_params()
        partition_count = 5
        self.start_redpanda(num_nodes=5)
        admin = Admin(self.redpanda)

        spec = TopicSpec(partition_count=partition_count, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup(min_records=throughput * 10, timeout_sec=60)

        self.redpanda.set_feature_active("node_local_core_assignment",
                                         active=True)

        self.redpanda.set_cluster_config({"raft_learner_recovery_rate": 1})
        brokers = admin.get_brokers()
        for partition in range(0, partition_count):
            assignments = self._get_assignments(admin, self.topic, partition)
            prev_assignments = copy.deepcopy(assignments)
            replicas = set([r['node_id'] for r in prev_assignments])
            for b in brokers:
                if b['node_id'] not in replicas:
                    assignments[0] = {"node_id": b['node_id']}
                    break

            self.logger.info(
                f"initial assignments for {self.topic}/{partition}: {prev_assignments}, new assignment: {assignments}"
            )
            self._set_partition_assignments(self.topic,
                                            partition,
                                            assignments,
                                            admin,
                                            node_local_core_assignment=True)

        wait_until(
            lambda: len(admin.list_reconfigurations()) == partition_count, 30)
        reconfigurations = admin.list_reconfigurations()
        for r in reconfigurations:
            assert "previous_replicas" in r
            assert "current_replicas" in r
            assert "bytes_left_to_move" in r
            assert "bytes_moved" in r
            assert "partition_size" in r
            assert "reconciliation_statuses" in r

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)
        self.redpanda.set_cluster_config(
            {"raft_learner_recovery_rate": 500 * (2**20)})
        wait_until(lambda: len(admin.list_reconfigurations()) == 0, 120, 1)


class SIPartitionMovementTest(PartitionMovementMixin, EndToEndTest):
    """
    Run partition movement tests with shadow indexing enabled
    """
    def __init__(self, ctx, *args, **kwargs):
        # Force shadow indexing to be used by most reads
        # in one test
        si_settings = SISettings(
            ctx,
            cloud_storage_max_connections=5,
            log_segment_size=10240,  # 10KiB
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
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

    def _partial_upgrade(self, num_to_upgrade: int):
        nodes = self.redpanda.nodes[0:num_to_upgrade]
        self.logger.info(f"Upgrading nodes: {[node.name for node in nodes]}")

        self.redpanda._installer.install(nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

    def _finish_upgrade(self, num_upgraded_already: int):
        nodes = self.redpanda.nodes[num_upgraded_already:]
        self.logger.info(f"Upgrading nodes: {[node.name for node in nodes]}")

        self.redpanda._installer.install(nodes, RedpandaInstaller.HEAD)
        self.redpanda.rolling_restart_nodes(nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

    # before v24.2, dns query to s3 endpoint do not include the bucketname, which is required for AWS S3 fips endpoints
    @skip_fips_mode
    @cluster(num_nodes=5, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(num_to_upgrade=[0, 2], cloud_storage_type=get_cloud_storage_type())
    @skip_debug_mode  # rolling restarts require more reliable recovery that a slow debug mode cluster can provide
    def test_shadow_indexing(self, num_to_upgrade, cloud_storage_type):
        """
        Test interaction between the shadow indexing and the partition movement.
        Partition movement generate partitions with different revision-ids and the
        archival/shadow-indexing subsystem is using revision to generate unique object
        keys inside the remote storage.
        """
        throughput, records, moves, partitions = self._get_scale_params()

        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions)
        self.start_redpanda(num_nodes=3,
                            install_opts=install_opts,
                            license_required=True)

        spec = TopicSpec(name="topic",
                         partition_count=partitions,
                         replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup()

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        # We will start an upgrade halfway through the test: this ensures
        # that a single-version cluster existed for long enough to actually
        # upload some data to S3, before the upgrade potentially pauses
        # PUTs, as it does in a format-changing step like a 22.2->22.3 upgrade
        upgrade_at_step = moves // 2

        for i in range(moves):
            if i == upgrade_at_step and test_mixed_versions:
                self._partial_upgrade(num_to_upgrade)

            self._move_and_verify(
                node_local_core_assignment=node_local_core_assignment)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)

        self._finish_upgrade(num_to_upgrade)

    @cluster(num_nodes=5, log_allow_list=PREV_VERSION_LOG_ALLOW_LIST)
    # Redpandas before v23.1 did not have support for ABS.
    @matrix(num_to_upgrade=[0, 2], cloud_storage_type=get_cloud_storage_type())
    @skip_debug_mode  # rolling restarts require more reliable recovery that a slow debug mode cluster can provide
    def test_cross_shard(self, num_to_upgrade, cloud_storage_type):
        """
        Test interaction between the shadow indexing and the partition movement.
        Move partitions with SI enabled between shards.
        """

        throughput, records, moves, partitions = self._get_scale_params()

        test_mixed_versions = num_to_upgrade > 0
        install_opts = InstallOptions(
            install_previous_version=test_mixed_versions)
        self.start_redpanda(num_nodes=3,
                            install_opts=install_opts,
                            license_required=True)

        spec = TopicSpec(name="topic",
                         partition_count=partitions,
                         replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1, throughput=throughput)
        self.start_consumer(1)
        self.await_startup()

        if test_mixed_versions:
            node_local_core_assignment = False
        else:
            self.redpanda.set_feature_active("node_local_core_assignment",
                                             active=True)
            node_local_core_assignment = True

        admin = Admin(self.redpanda)
        topic = self.topic
        partition = 0

        # We will start an upgrade halfway through the test: this ensures
        # that a single-version cluster existed for long enough to actually
        # upload some data to S3, before the upgrade potentially pauses
        # PUTs, as it does in a format-changing step like a 22.2->22.3 upgrade
        upgrade_at_step = moves // 2

        for i in range(moves):
            if i == upgrade_at_step and test_mixed_versions:
                self._partial_upgrade(num_to_upgrade)

            assignments = self._get_current_node_cores(admin, topic, partition)
            for a in assignments:
                # Bounce between core 0 and 1
                a['core'] = (a['core'] + 1) % 2
            self._set_partition_assignments(
                topic,
                partition,
                assignments,
                admin=admin,
                node_local_core_assignment=node_local_core_assignment)
            self._wait_post_move(topic, partition, assignments, 360)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=45,
                            min_records=records)

        self._finish_upgrade(num_to_upgrade)
