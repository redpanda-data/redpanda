# Copyright 2020 Vectorized, Inc.
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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
import requests

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.honey_badger import HoneyBadger
from rptest.services.rpk_producer import RpkProducer
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer


class PartitionMovementTest(EndToEndTest):
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

    @staticmethod
    def _random_partition(metadata):
        topic = random.choice(metadata)
        partition = random.choice(topic["partitions"])
        return topic["topic"], partition["partition"]

    @staticmethod
    def _choose_replacement(admin, assignments):
        """
        Does not produce assignments that contain duplicate nodes. This is a
        limitation in redpanda raft implementation.
        """
        replication_factor = len(assignments)
        node_ids = lambda x: set([a["node_id"] for a in x])

        assert replication_factor >= 1
        assert len(node_ids(assignments)) == replication_factor

        # remove random assignment(s). we allow no changes to be made to
        # exercise the code paths responsible for dealing with no-ops.
        num_replacements = random.randint(0, replication_factor)
        selected = random.sample(assignments, num_replacements)
        for assignment in selected:
            assignments.remove(assignment)

        # choose a valid random replacement
        replacements = []
        brokers = admin.get_brokers()
        while len(assignments) != replication_factor:
            broker = random.choice(brokers)
            node_id = broker["node_id"]
            if node_id in node_ids(assignments):
                continue
            core = random.randint(0, broker["num_cores"] - 1)
            replacement = dict(node_id=node_id, core=core)
            assignments.append(replacement)
            replacements.append(replacement)

        return selected, replacements

    @staticmethod
    def _get_assignments(admin, topic, partition):
        res = admin.get_partitions(topic, partition)

        def normalize(a):
            return dict(node_id=a["node_id"], core=a["core"])

        return [normalize(a) for a in res["replicas"]]

    @staticmethod
    def _equal_assignments(r0, r1):
        def to_tuple(a):
            return a["node_id"], a["core"]

        r0 = [to_tuple(a) for a in r0]
        r1 = [to_tuple(a) for a in r1]
        return set(r0) == set(r1)

    def _get_current_partitions(self, admin, topic, partition_id):
        def keep(p):
            return p["ns"] == "kafka" and p["topic"] == topic and p[
                "partition_id"] == partition_id

        result = []
        for node in self.redpanda.nodes:
            node_id = self.redpanda.idx(node)
            partitions = admin.get_partitions(node=node)
            partitions = filter(keep, partitions)
            for partition in partitions:
                result.append(dict(node_id=node_id, core=partition["core"]))
        return result

    def _move_and_verify(self):
        admin = Admin(self.redpanda)

        # choose a random topic-partition
        metadata = self.redpanda.describe_topics()
        topic, partition = self._random_partition(metadata)
        self.logger.info(f"selected topic-partition: {topic}-{partition}")

        # get the partition's replica set, including core assignments. the kafka
        # api doesn't expose core information, so we use the redpanda admin api.
        assignments = self._get_assignments(admin, topic, partition)
        self.logger.info(f"assignments for {topic}-{partition}: {assignments}")

        # build new replica set by replacing a random assignment
        selected, replacements = self._choose_replacement(admin, assignments)
        self.logger.info(
            f"replacement for {topic}-{partition}:{len(selected)}: {selected} -> {replacements}"
        )
        self.logger.info(
            f"new assignments for {topic}-{partition}: {assignments}")

        r = admin.set_partition_replicas(topic, partition, assignments)

        def status_done():
            info = admin.get_partitions(topic, partition)
            self.logger.info(
                f"current assignments for {topic}-{partition}: {info}")
            converged = self._equal_assignments(info["replicas"], assignments)
            return converged and info["status"] == "done"

        # wait until redpanda reports complete
        wait_until(status_done, timeout_sec=90, backoff_sec=2)

        def derived_done():
            info = self._get_current_partitions(admin, topic, partition)
            self.logger.info(
                f"derived assignments for {topic}-{partition}: {info}")
            return self._equal_assignments(info, assignments)

        wait_until(derived_done, timeout_sec=90, backoff_sec=2)

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
        self.redpanda.create_topic(spec)
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
            self.redpanda.create_topic(spec)

        for _ in range(25):
            self._move_and_verify()

    @cluster(num_nodes=4)
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
            self.redpanda.create_topic(spec)

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

    @cluster(num_nodes=5)
    def test_dynamic(self):
        """
        Move partitions with active consumer / producer
        """
        self.start_redpanda(num_nodes=3)
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.redpanda.create_topic(spec)
        self.topic = spec.name
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        for _ in range(25):
            self._move_and_verify()
        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=3)
    def test_invalid_destination(self):
        """
        Check that requuests to move to non-existent locations are properly rejected.
        """

        self.start_redpanda(num_nodes=3)
        spec = TopicSpec(name="topic", partition_count=1, replication_factor=1)
        self.redpanda.create_topic(spec)
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
        # Reproducer for https://github.com/vectorizedio/redpanda/issues/2286
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
        self.redpanda.create_topic(spec)

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

        # Start an inter-node move, which should take some time
        # to complete because of recovery network traffic
        admin = Admin(self.redpanda)
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
            assert e.response.status_code == 400
        else:
            raise RuntimeError(f"Expected 400 but got {r.status_code}")

        # An update to partition properties should succeed
        # (issue https://github.com/vectorizedio/redpanda/issues/2300)
        rpk = RpkTool(self.redpanda)
        assert admin.get_partitions(name, 0)['status'] == "in_progress"
        rpk.alter_topic_config(name, "retention.ms", "3600000")

        # A deletion should succeed
        assert name in rpk.list_topics()
        assert admin.get_partitions(name, 0)['status'] == "in_progress"
        rpk.delete_topic(name)
        assert name not in rpk.list_topics()
