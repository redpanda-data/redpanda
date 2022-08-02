from copyreg import dispatch_table
import random
from urllib.error import HTTPError
from numpy import partition

import requests
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.verifiable_producer import TopicPartition
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.tests.partition_movement import PartitionMovementMixin
from ducktape.mark import matrix, parametrize
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings

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
                'enable_leader_balancer': False
            },
            **kwargs)
        self._ctx = ctx
        self.throughput = 10000
        self.moves = 10
        self.min_records = 100000
        self.partition_count = 20
        self.consumer_timeout_seconds = 90

    def replace_replicas(self, admin, assignments, x_core_only=False):
        """
         replaces random number of replicas in `assignments` list of replicas
        
        :param admin: admin api client
        :param assignments: list of dictionaries {"node_id": ...,"core"...} describing partition replica assignments.
        :param x_core_only: when true assignment nodes will not be changed, only cores
        
        :return: a tuple of lists, list of previous assignments and list of replaced assignments
        """
        if x_core_only:
            selected = assignments.copy()
            brokers = admin.get_brokers()
            broker_cores = {}
            for b in brokers:
                broker_cores[b['node_id']] = b["num_cores"]
            for a in assignments:
                a['core'] = random.randint(0, broker_cores[a['node_id']] - 1)
            return selected, assignments

        selected, replacements = self._choose_replacement(admin,
                                                          assignments,
                                                          allow_no_ops=False)
        selected = sorted(selected, key=lambda v: v['node_id'])
        replacements = sorted(replacements, key=lambda v: v['node_id'])

        while selected == replacements:
            selected, replacements = self._choose_replacement(
                admin, assignments, allow_no_ops=False)
            selected = sorted(selected, key=lambda v: v['node_id'])
            replacements = sorted(replacements, key=lambda v: v['node_id'])

        return selected, replacements

    def _dispatch_move(self, partition, x_core_only=False):
        """
        Request partition replicas to be randomly moved

        :param partition: partition id to be moved
        :param x_core_only: when true assignment nodes will not be changed, only cores
        """
        admin = Admin(self.redpanda)

        assignments = self._get_assignments(admin, self.topic, partition)
        prev_assignments = assignments.copy()

        self.logger.info(
            f"assignments for {self.topic}-{partition}: {prev_assignments}")

        # build new replica set by replacing a random assignment, do not allow no ops as we want to have operation to cancel
        selected, replacements = self.replace_replicas(admin, assignments,
                                                       x_core_only)

        self.logger.info(
            f"replacement for {self.topic}-{partition}:{len(selected)}: {selected} -> {replacements}"
        )
        self.logger.info(
            f"new assignments for {self.topic}-{partition}: {assignments}")

        admin.set_partition_replicas(self.topic, partition, assignments)

        return prev_assignments, assignments

    def _cancel_move(self, unclean_abort, partition, previous_assignment):
        """
        Request partition movement to interrupt and validates resulting cancellation against previous assignment
        """
        admin = Admin(self.redpanda)

        def move_in_progress():
            p = admin.get_partitions(self.topic, partition)
            return p['status'] == 'in_progress'

        wait_until(move_in_progress, 10, 1)
        try:
            if unclean_abort:
                admin.force_abort_partition_move(self.topic, partition)
            else:
                admin.cancel_partition_move(self.topic, partition)
        except requests.exceptions.HTTPError as e:
            # we do not throttle cross core moves, it may already be finished
            assert e.response.status_code == 400
            return

        # wait for previous assignment
        self._wait_post_move(self.topic, partition, previous_assignment, 60)

    def _random_move(self, unclean_abort):
        partition = random.randint(0, self.partition_count - 1)
        prev_assignment, _ = self._dispatch_move(partition)

        self._cancel_move(unclean_abort=unclean_abort,
                          partition=partition,
                          previous_assignment=prev_assignment)

    def _throttle_recovery(self, new_value):
        self.redpanda.set_cluster_config(
            {"raft_learner_recovery_rate": str(new_value)})

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(replication_factor=[1, 3],
            unclean_abort=[True, False],
            recovery=[NO_RECOVERY, RESTART_RECOVERY])
    def test_cancelling_partition_move(self, replication_factor, unclean_abort,
                                       recovery):
        """
        Cancel partition moving with active consumer / producer
        """
        self.start_redpanda(num_nodes=5,
                            extra_rp_conf={
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=replication_factor)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=self.throughput)
        self.start_consumer(1)
        self.await_startup()
        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)

        for _ in range(self.moves):
            self._random_move(unclean_abort)
            if recovery == RESTART_RECOVERY:
                # restart one of the nodes after each move
                self.redpanda.restart_nodes(
                    [random.choice(self.redpanda.nodes)])

        if unclean_abort:
            # do not run offsets validation as we may experience data loss since partition movement is forcibly cancelled
            wait_until(lambda: self.producer.num_acked > 20000, timeout_sec=60)

            self.producer.stop()
            self.consumer.stop()
        else:
            self.run_validation(
                enable_idempotence=False,
                consumer_timeout_sec=self.consumer_timeout_seconds,
                min_records=self.min_records)

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(replication_factor=[1, 3],
            unclean_abort=[True, False],
            recovery=[NO_RECOVERY, RESTART_RECOVERY])
    def test_cancelling_partition_move_x_core(self, replication_factor,
                                              unclean_abort, recovery):
        """
        Cancel partition moving with active consumer / producer
        """
        self.start_redpanda(num_nodes=5,
                            extra_rp_conf={
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(partition_count=self.partition_count,
                         replication_factor=replication_factor)

        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=self.throughput)
        self.start_consumer(1)
        self.await_startup()
        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)

        partition = random.randint(0, self.partition_count - 1)
        for i in range(self.moves):
            # move partition between cores first
            x_core = i < self.moves / 2

            prev_assignment, new_assignment = self._dispatch_move(
                partition, x_core_only=x_core)
            if x_core:
                self._wait_post_move(topic=self.topic,
                                     partition=partition,
                                     assignments=new_assignment,
                                     timeout_sec=60)
            else:
                self._cancel_move(unclean_abort=unclean_abort,
                                  partition=partition,
                                  previous_assignment=prev_assignment)
            if recovery == RESTART_RECOVERY:
                # restart one of the nodes after each move
                self.redpanda.restart_nodes(
                    [random.choice(self.redpanda.nodes)])

        if unclean_abort:
            # do not run offsets validation as we may experience data loss since partition movement is forcibly cancelled
            wait_until(lambda: self.producer.num_acked > 20000, timeout_sec=60)

            self.producer.stop()
            self.consumer.stop()
        else:
            self.run_validation(
                enable_idempotence=False,
                consumer_timeout_sec=self.consumer_timeout_seconds,
                min_records=self.min_records)

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
        admin.set_partition_replicas(self.topic, partition, new_assignment)

        def move_in_progress():
            p = admin.get_partitions(self.topic, partition)
            return p['status'] == 'in_progress'

        wait_until(move_in_progress, 10)

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
            current_movements[partition] = self._dispatch_move(partition)

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
            current_movements[partition] = self._dispatch_move(partition)

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
