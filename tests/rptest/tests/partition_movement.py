# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import copy
import random

import requests
from rptest.services.admin import Admin
from rptest.util import wait_until_result
from ducktape.utils.util import wait_until


class PartitionMovementMixin():

    # Use in assignments sent to redpanda when node-local core assignment
    # is enabled, to test that the actual value is ignored.
    INVALID_CORE = 12121212

    @staticmethod
    def _random_partition(metadata):
        topic = random.choice(metadata)
        partition = random.choice(topic.partitions)
        return topic.name, partition.id

    @staticmethod
    def _choose_replacement(admin, assignments, allow_no_ops,
                            node_local_core_assignment):
        """
        Does not produce assignments that contain duplicate nodes. This is a
        limitation in redpanda raft implementation.
        """
        replication_factor = len(assignments)
        node_ids = lambda x: set([a["node_id"] for a in x])
        orig_node_ids = node_ids(assignments)

        assert replication_factor >= 1
        assert len(orig_node_ids) == replication_factor

        # remove random assignment(s). we allow no changes to be made to
        # exercise the code paths responsible for dealing with no-ops.
        num_replacements = random.randint(0 if allow_no_ops else 1,
                                          replication_factor)
        selected = random.sample(assignments, num_replacements)
        for assignment in selected:
            assignments.remove(assignment)

        # choose a valid random replacement
        replacements = []
        while len(assignments) != replication_factor:
            brokers = admin.get_brokers()
            broker = random.choice(brokers)
            node_id = broker["node_id"]
            if node_id in node_ids(assignments):
                continue
            replacement = dict(node_id=node_id)
            if (node_id in orig_node_ids) or (not node_local_core_assignment):
                replacement["core"] = \
                    random.randint(0, broker["num_cores"] - 1)
            if not allow_no_ops and replacement in selected:
                continue
            assignments.append(replacement)
            replacements.append(replacement)

        return selected, replacements

    @staticmethod
    def _get_assignments(admin, topic, partition, with_cores=True):
        def try_get_partitions():
            try:
                res = admin.get_partitions(topic, partition)
                return (True, res)
            except requests.exceptions.HTTPError:
                # Retry HTTP errors, eg. 404 if the receiving node's controller
                # is catching up and doesn't yet know about the partition.
                return (False, None)

        res = wait_until_result(try_get_partitions,
                                timeout_sec=30,
                                backoff_sec=1)

        def normalize(a):
            ret = dict(node_id=a["node_id"])
            if with_cores:
                ret["core"] = a["core"]
            return ret

        return [normalize(a) for a in res["replicas"]]

    @staticmethod
    def _equal_assignments(r0, r1):
        # Core in an assignment can be unknown (None). In this case it is
        # considered equal to any core value in the other assignment.
        def node2core(assignments):
            return dict((a["node_id"], a.get("core")) for a in assignments)

        left_n2c = node2core(r0)
        right_n2c = node2core(r1)

        if set(left_n2c.keys()) != set(right_n2c.keys()):
            return False

        for n, c in left_n2c.items():
            rc = right_n2c[n]
            if c != rc and c is not None and rc is not None:
                return False

        return True

    def _get_current_node_cores(self, admin, topic, partition_id):
        def keep(p):
            return p["ns"] == "kafka" and p["topic"] == topic and p[
                "partition_id"] == partition_id

        result = []
        for node in self.redpanda._started:
            node_id = self.redpanda.idx(node)
            partitions = admin.get_partitions(node=node)
            partitions = filter(keep, partitions)
            for partition in partitions:
                result.append(dict(node_id=node_id, core=partition["core"]))
        return result

    def _wait_post_move(self, topic, partition, assignments, timeout_sec):
        # We need to add retries, becasue of eventual consistency. Metadata will be updated but it can take some time.
        admin = Admin(self.redpanda,
                      retry_codes=[404, 503, 504],
                      retries_amount=10)

        def node_assignments_converged():
            results = []
            for n in self.redpanda._started:
                info = admin.get_partitions(topic, partition, node=n)
                node_assignments = [{
                    "node_id": r["node_id"]
                } for r in info["replicas"]]
                self.logger.info(
                    f"node assignments for {topic}/{partition}: {node_assignments}, "
                    f"partition status: {info['status']}")
                converged = self._equal_assignments(node_assignments,
                                                    assignments)
                results.append(converged and info["status"] == "done")

            return all(results)

        # wait until redpanda reports complete
        wait_until(node_assignments_converged,
                   timeout_sec=timeout_sec,
                   backoff_sec=2)

        def cores_converged():
            info = self._get_current_node_cores(admin, topic, partition)
            self.logger.info(
                f"current core placement for {topic}/{partition}: {info}")
            return self._equal_assignments(info, assignments)

        wait_until(cores_converged, timeout_sec=timeout_sec, backoff_sec=2)

    def _wait_post_cancel(self, topic, partition, prev_assignments,
                          new_assignment, timeout_sec):
        admin = Admin(self.redpanda)

        def cancel_finished():
            results = []
            for n in self.redpanda._started:
                info = admin.get_partitions(topic, partition, node=n)
                results.append(info["status"] == "done")

            return all(results)

        wait_until(cancel_finished, timeout_sec=timeout_sec, backoff_sec=1)

        result_configuration = admin.wait_stable_configuration(
            topic=topic, partition=partition, timeout_s=timeout_sec)
        # don't check core placement as x-core moves can't be cancelled if
        # node-local core assignment is enabled (only assigned anew).
        cur_replicas = [{
            "node_id": r.node_id
        } for r in result_configuration.replicas]

        self.logger.info(
            f"current replicas for {topic}/{partition}: {cur_replicas}")
        movement_cancelled = self._equal_assignments(cur_replicas,
                                                     prev_assignments)

        # Can happen if movement was already in un revertable state
        movement_finished = False
        if new_assignment is not None:
            movement_finished = self._equal_assignments(
                cur_replicas, new_assignment)

        assert movement_cancelled or movement_finished

    def _do_move_and_verify(self,
                            topic,
                            partition,
                            timeout_sec,
                            node_local_core_assignment=False):
        _, new_assignment = self._dispatch_random_partition_move(
            topic=topic,
            partition=partition,
            node_local_core_assignment=node_local_core_assignment)

        self._wait_post_move(topic, partition, new_assignment, timeout_sec)

        return (topic, partition, new_assignment)

    def _move_and_verify(self, node_local_core_assignment):
        # choose a random topic-partition
        metadata = self.client().describe_topics()
        topic, partition = self._random_partition(metadata)
        # timeout for __consumer_offsets topic has to be long enough
        # to wait for compaction to finish. For resource constrained machines
        # and redpanda debug builds it may take a very long time
        timeout_sec = 360 if topic == "__consumer_offsets" else 90

        self.logger.info(f"selected topic-partition: {topic}/{partition}")

        self._do_move_and_verify(
            topic,
            partition,
            timeout_sec,
            node_local_core_assignment=node_local_core_assignment)

    def _replace_replica_set(self, assignments, allow_no_ops, x_core_only,
                             node_local_core_assignment):
        """
         replaces random number of replicas in `assignments` list of replicas
        
        :param admin: admin api client
        :param assignments: list of dictionaries {"node_id": ...,"core"...} describing partition replica assignments.
        :param x_core_only: when true assignment nodes will not be changed, only cores
        
        :return: a tuple of lists, list of previous assignments and list of replaced assignments
        """
        admin = Admin(self.redpanda)
        if x_core_only:
            selected = copy.deepcopy(assignments)
            brokers = admin.get_brokers()
            broker_cores = {}
            for b in brokers:
                broker_cores[b['node_id']] = b["num_cores"]
            for a in assignments:
                a['core'] = random.randint(0, broker_cores[a['node_id']] - 1)
            return selected, assignments

        selected, replacements = self._choose_replacement(
            admin,
            assignments,
            allow_no_ops=allow_no_ops,
            node_local_core_assignment=node_local_core_assignment)

        return selected, replacements

    def _set_partition_assignments(self,
                                   topic,
                                   partition,
                                   assignments,
                                   admin=None,
                                   node_local_core_assignment=False):
        self.logger.info(
            f"setting assignments for {topic}/{partition}: {assignments}")

        if admin is None:
            admin = Admin(self.redpanda)

        if not node_local_core_assignment:
            admin.set_partition_replicas(topic, partition, assignments)
        else:
            admin.set_partition_replicas(topic, partition,
                                         [{
                                             "node_id": a["node_id"],
                                             "core": self.INVALID_CORE,
                                         } for a in assignments])

            for assignment in assignments:
                if "core" in assignment:
                    admin.set_partition_replica_core(topic, partition,
                                                     assignment["node_id"],
                                                     assignment["core"])

    def _dispatch_random_partition_move(self,
                                        topic,
                                        partition,
                                        x_core_only=False,
                                        allow_no_op=True,
                                        node_local_core_assignment=False):
        """
        Request partition replicas to be randomly moved

        :param partition: partition id to be moved
        :param x_core_only: when true assignment nodes will not be changed, only cores
        """
        admin = Admin(self.redpanda)
        assignments = self._get_assignments(
            admin, topic, partition, with_cores=not node_local_core_assignment)
        prev_assignments = assignments.copy()

        self.logger.info(
            f"initial assignments for {topic}/{partition}: {prev_assignments}")

        # build new replica set by replacing a random assignment, do not allow no ops as we want to have operation to cancel
        selected, replacements = self._replace_replica_set(
            assignments,
            x_core_only=x_core_only,
            allow_no_ops=allow_no_op,
            node_local_core_assignment=node_local_core_assignment)

        self.logger.info(
            f"chose {len(selected)} replacements for {topic}/{partition}: {selected} -> {replacements}"
        )

        self._set_partition_assignments(
            topic,
            partition,
            assignments,
            admin=admin,
            node_local_core_assignment=node_local_core_assignment)

        return prev_assignments, assignments

    def _wait_for_move_in_progress(self, topic, partition, timeout=10):
        admin = Admin(self.redpanda)

        def move_in_progress():
            return [
                admin.get_partitions(topic, partition,
                                     node=n)['status'] == 'in_progress'
                for n in self.redpanda._started
            ]

        wait_until(move_in_progress, timeout_sec=timeout)

    def _request_move_cancel(self,
                             topic,
                             partition,
                             previous_assignment,
                             unclean_abort,
                             new_assignment=None,
                             timeout=90):
        """
        Request partition movement to interrupt and validates
        resulting cancellation against previous assignment

        Move can also be already in no return state, then
        result can be equal to initial movement assignment. (PR 8393)
        When request cancellation without throttling recovery rate
        or partition is empty, initial movement assignment must be provided
        """
        self._wait_for_move_in_progress(topic, partition)
        admin = Admin(self.redpanda)
        try:
            if unclean_abort:
                admin.force_abort_partition_move(topic, partition)
            else:
                admin.cancel_partition_move(topic, partition)
        except requests.exceptions.HTTPError as e:
            # we do not throttle cross core moves, it may already be finished
            assert e.response.status_code == 400
            return

        # wait for previous assignment or new assigment if movement cannot be cancelled
        self._wait_post_cancel(topic, partition, previous_assignment,
                               new_assignment, timeout)
