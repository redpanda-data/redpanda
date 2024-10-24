from copyreg import dispatch_table
import os
import random
import signal
from time import sleep
from urllib.error import HTTPError
from numpy import partition

import requests
from rptest.services.cluster import cluster

from ducktape.utils.util import wait_until
from ducktape.mark import matrix, parametrize
from rptest.util import wait_for_recovery_throttle_rate

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.verifiable_producer import TopicPartition
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings, MetricsEndpoint
from rptest.utils.mode_checks import cleanup_on_early_exit, skip_debug_mode

NO_RECOVERY = "no_recovery"
RESTART_RECOVERY = "restart_recovery"


def is_in_replica_set(replicas, id):
    return next((r for r in replicas if r['node_id'] == id), None) is not None


class PartitionMoveInterruption(PartitionMovementMixin, EndToEndTest):
    """
    Basic test verifying partition movement cancellation
    """
    def __init__(self, ctx, *args, **kwargs):
        super(PartitionMoveInterruption, self).__init__(
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
        self.moves = 10
        self.partition_count = 20
        self.consumer_timeout_seconds = 90

        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.throughput = 500
            self.min_records = 5000
        else:
            self.throughput = 10000
            self.min_records = 100000

    def get_moving_to_node_metrics(self, node):
        metrics = self.redpanda.metrics_sample(
            "moving_to_node", [node],
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        return int(sum([metric.value for metric in metrics.samples]))

    def get_moving_from_node_metrics(self, node):
        metrics = self.redpanda.metrics_sample(
            "moving_from_node", [node],
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        return int(sum([metric.value for metric in metrics.samples]))

    def get_cancelling_movements_for_node_metrics(self, node):
        metrics = self.redpanda.metrics_sample(
            "node_cancelling_movements", [node],
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        return int(sum([metric.value for metric in metrics.samples]))

    def metrics_correct(self, prev_assignment, assignmets):
        prev_assignment_nodes = [a["node_id"] for a in prev_assignment]
        assignment_nodes = [a["node_id"] for a in assignmets]
        moving_from_node_sum = 0
        moving_to_node_sum = 0
        self.logger.debug(
            f"Checking metrics for movement from {prev_assignment_nodes} to {assignment_nodes}"
        )
        for node in self.redpanda.nodes:
            node_id = self.redpanda.idx(node)
            moving_to_node_partitions_amount = self.get_moving_to_node_metrics(
                node)
            moving_to_node_sum += moving_to_node_partitions_amount
            moving_from_node_partitions_amount = self.get_moving_from_node_metrics(
                node)
            moving_from_node_sum += moving_from_node_partitions_amount
            cancelling_movements_for_node = self.get_cancelling_movements_for_node_metrics(
                node)

            self.logger.debug(f"Node: {node_id}, \
                moving_to_node_partitions_amount: {moving_to_node_partitions_amount}, \
                moving_from_node_partitions_amount: {moving_from_node_partitions_amount}, \
                cancelling_movements_for_node: {cancelling_movements_for_node}"
                              )

            if node_id in prev_assignment_nodes and node_id not in assignment_nodes:
                if moving_to_node_partitions_amount != 0 or \
                    moving_from_node_partitions_amount != 1:
                    return False
            elif node_id not in prev_assignment_nodes and node_id in assignment_nodes:
                if moving_to_node_partitions_amount != 1 or \
                    moving_from_node_partitions_amount != 0:
                    return False
            else:
                if moving_to_node_partitions_amount != 0 or \
                    moving_from_node_partitions_amount != 0:
                    return False

            if cancelling_movements_for_node != 0:
                return False

        return moving_to_node_sum == moving_from_node_sum

    def check_metrics(self, prev_assignment=[], assignmets=[]):
        return wait_until(
            lambda: self.metrics_correct(prev_assignment, assignmets),
            timeout_sec=10)

    def _random_move_and_cancel(self, unclean_abort):
        metadata = self.client().describe_topics()
        topic, partition = self._random_partition(metadata)
        prev_assignment, assignments = self._dispatch_random_partition_move(
            topic=topic, partition=partition, allow_no_op=False)

        self._wait_for_move_in_progress(topic, partition)
        self.check_metrics(prev_assignment, assignments)
        self._request_move_cancel(unclean_abort=unclean_abort,
                                  topic=topic,
                                  partition=partition,
                                  previous_assignment=prev_assignment)
        self.check_metrics()

    def _throttle_recovery(self, new_rate: int):
        self.redpanda.set_cluster_config(
            {"raft_learner_recovery_rate": str(new_rate)})
        wait_for_recovery_throttle_rate(redpanda=self.redpanda,
                                        new_rate=new_rate)

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(replication_factor=[1, 3],
            unclean_abort=[True, False],
            recovery=[NO_RECOVERY, RESTART_RECOVERY],
            compacted=[False, True])
    def test_cancelling_partition_move(self, replication_factor, unclean_abort,
                                       recovery, compacted):
        """
        Cancel partition moving with active consumer / producer
        """

        self.start_redpanda(num_nodes=5,
                            extra_rp_conf={
                                "default_topic_replications": 3,
                                "compacted_log_segment_size": 1 * (2**20),
                                "controller_snapshot_max_age_sec": 3,
                            })
        # skip compacted topics tests in debug mode
        if compacted and self.debug_mode:
            cleanup_on_early_exit(self)
            return

        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=replication_factor,
                         cleanup_policy=TopicSpec.CLEANUP_COMPACT
                         if compacted else TopicSpec.CLEANUP_DELETE)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1,
                            throughput=self.throughput,
                            repeating_keys=100 if compacted else None)
        if not compacted:
            self.start_consumer(1)
        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)

        for _ in range(self.moves):
            self._random_move_and_cancel(unclean_abort)
            if recovery == RESTART_RECOVERY:
                # restart one of the nodes after each move
                self.redpanda.restart_nodes(
                    [random.choice(self.redpanda.nodes)])
        # start consumer late in the process for the compaction to trigger
        if compacted:
            self.start_consumer(1, verify_offsets=False)
        if unclean_abort:
            # do not run offsets validation as we may experience data loss since partition movement is forcibly cancelling
            wait_until(lambda: self.producer.num_acked > 20000, timeout_sec=60)

            self.producer.stop()
            self.consumer.stop()
        else:
            self.run_validation(
                enable_idempotence=False,
                consumer_timeout_sec=self.consumer_timeout_seconds,
                min_records=self.min_records,
                enable_compaction=compacted)

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(replication_factor=[1, 3],
            unclean_abort=[True, False],
            recovery=[NO_RECOVERY, RESTART_RECOVERY],
            compacted=[False, True])
    def test_cancelling_partition_move_x_core(self, replication_factor,
                                              unclean_abort, recovery,
                                              compacted):
        """
        Cancel partition moving with active consumer / producer
        """

        self.start_redpanda(
            num_nodes=5,
            extra_rp_conf={
                "default_topic_replications": 3,
                # make segments small to ensure that they are compacted during
                # the test (only sealed i.e. not being written segments are compacted)
                "compacted_log_segment_size": 1 * (2**20),
            })

        # skip compacted topics tests in debug mode
        if compacted and self.debug_mode:
            cleanup_on_early_exit(self)
            return

        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=replication_factor,
                         cleanup_policy=TopicSpec.CLEANUP_COMPACT
                         if compacted else TopicSpec.CLEANUP_DELETE)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1,
                            throughput=self.throughput,
                            repeating_keys=100 if compacted else None)

        self.start_consumer(1, verify_offsets=(not compacted))
        self.await_startup()
        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)

        partition = random.randint(0, self.partition_count - 1)
        for i in range(self.moves):
            # move partition between cores first
            x_core = i < self.moves / 2

            prev_assignment, new_assignment = self._dispatch_random_partition_move(
                topic=self.topic, partition=partition, x_core_only=x_core)
            if x_core:
                self._wait_post_move(topic=self.topic,
                                     partition=partition,
                                     assignments=new_assignment,
                                     timeout_sec=60)
            else:
                self._request_move_cancel(unclean_abort=unclean_abort,
                                          topic=self.topic,
                                          partition=partition,
                                          previous_assignment=prev_assignment)
            if recovery == RESTART_RECOVERY:
                # restart one of the nodes after each move
                self.redpanda.restart_nodes(
                    [random.choice(self.redpanda.nodes)])

        if unclean_abort:
            # do not run offsets validation as we may experience data loss since partition movement is forcibly cancelling
            wait_until(lambda: self.producer.num_acked > 20000, timeout_sec=60)

            self.producer.stop()
            self.consumer.stop()
        else:
            self.run_validation(
                enable_idempotence=False,
                consumer_timeout_sec=self.consumer_timeout_seconds,
                min_records=self.min_records,
                enable_compaction=compacted)

    def increase_replication_factor(self, topic, partition,
                                    requested_replication_factor):

        admin = Admin(self.redpanda)
        assignments = self._get_assignments(admin, topic, partition=partition)
        assert requested_replication_factor > len(assignments)

        self.logger.info(
            f"increasing {topic}/{partition} replication factor to {requested_replication_factor}, current assignment: {assignments}"
        )

        while len(assignments) < requested_replication_factor:
            brokers = admin.get_brokers()
            b = random.choice(brokers)
            found = [
                b['node_id'] == replica['node_id'] for replica in assignments
            ]

            if any(found):
                continue
            assignments.append({'node_id': b['node_id'], 'core': 0})

        admin.set_partition_replicas(self.topic, partition, assignments)
        self._wait_post_move(self.topic, partition, assignments, 60)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_forced_cancellation(self):
        partitions = 4
        self.start_redpanda(
            num_nodes=3,
            extra_rp_conf={'default_topic_replications': 3},
        )

        spec = TopicSpec(name='panda-test-topic',
                         partition_count=partitions,
                         replication_factor=3)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=self.throughput)
        self.start_consumer(1)
        self.await_startup()

        admin = Admin(self.redpanda)
        partition = random.randint(0, partitions - 1)

        # get current assignments
        assignment = self._get_assignments(admin,
                                           self.topic,
                                           partition=partition)

        # drop one of the replicas, new quorum requires both of them to be up
        new_assignment = assignment[0:2]

        self.logger.info(
            f"new assignment for {self.topic}/{partition}: {new_assignment}")
        # throttle recovery to prevent partition move from finishing
        # self._throttle_recovery(10)
        # at this point both of the replicas should be voters, stop one of the
        # nodes form new assignment to make sure that new quorum is no
        # longer available
        to_stop = random.choice(new_assignment)['node_id']
        self.redpanda.stop_node(self.redpanda.get_node(to_stop))

        # wait for new controller to be elected
        def new_controller():
            return self.redpanda.idx(self.redpanda.controller()) != to_stop

        wait_until(new_controller, 10, 1)

        # update replica set
        self._set_partition_assignments(self.topic, partition, new_assignment,
                                        admin)

        self._wait_for_move_in_progress(self.topic, partition)

        # abort moving partition
        admin.cancel_partition_move(self.topic, partition=partition)
        admin.force_abort_partition_move(self.topic, partition=partition)

        # # restart the node
        # self.redpanda.start_node(self.redpanda.get_node(to_stop))

        # wait for previous assignment to be set
        def cancelled():
            info = admin.get_partitions(self.topic, partition)

            converged = self._equal_assignments(info["replicas"], assignment)
            return converged and info["status"] == "done"

        # wait until redpanda reports complete
        wait_until(cancelled, timeout_sec=30, backoff_sec=2)

        # do not run offsets validation as we may experience data loss since cancellation is forced
        wait_until(lambda: self.producer.num_acked > 20000, timeout_sec=60)

        self.producer.stop()
        self.consumer.stop()

    @cluster(num_nodes=7)
    def test_cancelling_all_moves_in_cluster(self):
        """
        Cancel all partition moves in the cluster
        """
        self.start_redpanda(num_nodes=5,
                            extra_rp_conf={
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=3)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=self.throughput)
        self.start_consumer(1)
        self.await_startup()
        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)
        current_movements = {}
        partitions = list(range(0, self.partition_count))
        for _ in range(self.partition_count - 1):
            partition = random.choice(partitions)
            partitions.remove(partition)
            current_movements[
                partition] = self._dispatch_random_partition_move(
                    self.topic, partition)

        self.logger.info(f"moving {current_movements}")

        admin = Admin(self.redpanda)
        ongoing = admin.list_reconfigurations()
        assert len(ongoing) > 0

        admin.cancel_all_reconfigurations()

        wait_until(lambda: len(admin.list_reconfigurations()) == 0, 60, 1)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=self.consumer_timeout_seconds,
                            min_records=self.min_records)

    def is_moving_to_node(previous_replicas, current_replicas, id):
        return is_in_replica_set(
            current_replicas,
            id) and not is_in_replica_set(previous_replicas, id)

    def is_moving_from_node(previous_replicas, current_replicas, id):
        return is_in_replica_set(
            previous_replicas,
            id) and not is_in_replica_set(current_replicas, id)

    @cluster(num_nodes=7)
    def test_cancelling_all_moves_from_node(self):
        """
        Cancel all partition moves directed to/from given node
        """
        self.start_redpanda(num_nodes=5,
                            extra_rp_conf={
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=3)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=self.throughput)
        self.start_consumer(1)
        self.await_startup()
        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)
        current_movements = {}
        partitions = list(range(0, self.partition_count))
        for _ in range(self.partition_count - 1):
            partition = random.choice(partitions)
            partitions.remove(partition)
            current_movements[
                partition] = self._dispatch_random_partition_move(
                    self.topic, partition)

        self.logger.info(f"moving {current_movements}")

        admin = Admin(self.redpanda)
        ongoing = admin.list_reconfigurations()
        assert len(ongoing) > 0

        target_node_id = self.redpanda.idx(
            (random.choice(self.redpanda.nodes)))

        self.logger.info(
            "cancelling all node {target_node_id} reconfigurations")

        admin.cancel_all_node_reconfigurations(target_id=target_node_id)

        def is_node_reconfiguration(reconfiguration):
            current_replicas = admin.get_partitions(
                topic=reconfiguration['topic'],
                partition=reconfiguration['partition'])['replicas']
            previous_replicas = reconfiguration['previous_replicas']

            return PartitionMoveInterruption.is_moving_to_node(
                id=target_node_id,
                previous_replicas=previous_replicas,
                current_replicas=current_replicas
            ) or PartitionMoveInterruption.is_moving_from_node(
                id=target_node_id,
                previous_replicas=previous_replicas,
                current_replicas=current_replicas)

        def has_no_node_reconfigurations():
            ongoing = admin.list_reconfigurations()

            return len([o for o in ongoing if is_node_reconfiguration(o)]) == 0

        wait_until(has_no_node_reconfigurations, 60, 1)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=self.consumer_timeout_seconds,
                            min_records=self.min_records)

    def get_node_by_id(self, id):
        for n in self.redpanda.nodes:
            if self.redpanda.node_id(n) == id:
                return n
        return None

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_cancelling_partition_move_node_down(self):
        """
        Cancel partition moving with active consumer / producer
        """

        self.start_redpanda(num_nodes=5,
                            extra_rp_conf={
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=3)

        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(1, throughput=self.throughput)
        self.start_consumer(1)
        self.await_startup(min_records=self.throughput,
                           timeout_sec=self.consumer_timeout_seconds)

        metadata = self.client().describe_topics()
        topic, partition = self._random_partition(metadata)
        admin = Admin(self.redpanda)
        assignments = self._get_assignments(admin, topic, partition)
        prev_assignments = assignments.copy()

        self.logger.info(
            f"initial assignments for {topic}/{partition}: {prev_assignments}")

        replica_ids = [a['node_id'] for a in prev_assignments]
        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)
        to_stop = None
        for n in self.redpanda.nodes:
            id = self.redpanda.node_id(n)
            if id not in replica_ids:
                previous = assignments.pop()
                assignments.append({"node_id": id, "core": 0})
                # stop a node that is going to be removed from current partition assignment
                to_stop = self.get_node_by_id(previous['node_id'])
                self.redpanda.stop_node(to_stop)
                break

        def new_controller():
            leader_id = admin.get_partition_leader(namespace="redpanda",
                                                   topic="controller",
                                                   partition=0)
            return leader_id != -1 and leader_id != self.redpanda.node_id(
                to_stop)

        wait_until(new_controller, 30)

        self.logger.info(
            f"moving {topic}/{partition}: {prev_assignments} -> {assignments}")

        self._set_partition_assignments(topic, partition, assignments, admin)

        self._wait_for_move_in_progress(topic, partition)

        admin.cancel_partition_move(topic, partition)
        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=self.consumer_timeout_seconds,
                            min_records=self.min_records)

        def move_finished():
            for n in self.redpanda.started_nodes():
                partition_info = admin.get_partitions(topic=topic,
                                                      partition=partition,
                                                      node=n)
                if partition_info['status'] != 'done':
                    return False
                if not self._equal_assignments(partition_info['replicas'],
                                               prev_assignments):
                    return False

            return True

        wait_until(move_finished, 30, backoff_sec=1)

    #TODO: investigate slow startups in debug mode
    @skip_debug_mode
    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(replication_factor=[1, 3])
    def test_cancellations_interrupted_with_restarts(self, replication_factor):

        self.start_redpanda(num_nodes=5,
                            extra_rp_conf={
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(name="test-move-topic",
                         partition_count=self.partition_count,
                         replication_factor=replication_factor)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=self.throughput)
        self.start_consumer(1)
        self.await_startup(min_records=self.throughput,
                           timeout_sec=self.consumer_timeout_seconds)

        topic = spec.name
        partition = 0
        admin = Admin(self.redpanda)

        assignments = self._get_assignments(admin, topic, partition)
        prev_assignments = assignments.copy()

        # throttle recovery to prevent partition move from finishing

        for i in range(0, 10):
            self._throttle_recovery(10)
            should_cancel = i % 2
            assignments = self._get_assignments(admin, topic, partition)
            prev_assignments = assignments.copy()
            self.logger.info(
                f"[{i}] current assignments for {topic}/{partition}: {prev_assignments}"
            )
            replica_ids = [a['node_id'] for a in prev_assignments]
            available_ids = [
                self.redpanda.node_id(n) for n in self.redpanda.nodes
            ]
            random.shuffle(available_ids)
            for id in available_ids:
                if id not in replica_ids:
                    assignments.pop()
                    assignments.append({"node_id": id, "core": 0})

            self.logger.info(
                f"[{i}] moving {topic}/{partition}: {prev_assignments} -> {assignments}"
            )

            self._set_partition_assignments(topic, partition, assignments,
                                            admin)

            self._wait_for_move_in_progress(topic, partition)

            if should_cancel:
                try:
                    admin.cancel_partition_move(topic, partition)
                except:
                    pass

            self._throttle_recovery(10000000)
            for n in self.redpanda.nodes:
                self.redpanda.stop_node(n, forced=True)
            for n in self.redpanda.nodes:
                self.redpanda.start_node(n)

            def move_finished():

                for n in self.redpanda.started_nodes():
                    partition_info = admin.get_partitions(topic=topic,
                                                          partition=partition,
                                                          node=n)
                    if partition_info['status'] != 'done':
                        return False

                    replicas = partition_info['replicas']

                    cancelled = self._equal_assignments(
                        replicas, prev_assignments)
                    reverted = self._equal_assignments(replicas, assignments)
                    if not (cancelled or reverted):
                        return False

                return True

            wait_until(move_finished, 80, backoff_sec=1)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=self.consumer_timeout_seconds,
                            min_records=self.min_records)
