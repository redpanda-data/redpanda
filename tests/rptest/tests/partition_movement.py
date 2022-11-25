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
    @staticmethod
    def _random_partition(metadata):
        topic = random.choice(metadata)
        partition = random.choice(topic.partitions)
        return topic.name, partition.id

    @staticmethod
    def _choose_replacement(admin, assignments, allow_no_ops=True):
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
            core = random.randint(0, broker["num_cores"] - 1)
            replacement = dict(node_id=node_id, core=core)
            if not allow_no_ops and replacement in selected:
                continue
            assignments.append(replacement)
            replacements.append(replacement)

        return selected, replacements

    @staticmethod
    def _get_assignments(admin, topic, partition):
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
        for node in self.redpanda._started:
            node_id = self.redpanda.node_id(node)
            partitions = admin.get_partitions(node=node)
            partitions = filter(keep, partitions)
            for partition in partitions:
                result.append(dict(node_id=node_id, core=partition["core"]))
        return result

    def _wait_post_move(self, topic, partition, assignments, timeout_sec):
        admin = Admin(self.redpanda)

        def status_done():
            results = []
            for n in self.redpanda._started:
                info = admin.get_partitions(topic, partition, node=n)
                self.logger.info(
                    f"current assignments for {topic}-{partition}: {info}")
                converged = self._equal_assignments(info["replicas"],
                                                    assignments)
                results.append(converged and info["status"] == "done")

            return all(results)

        # wait until redpanda reports complete
        wait_until(status_done, timeout_sec=timeout_sec, backoff_sec=2)

        def derived_done():
            info = self._get_current_partitions(admin, topic, partition)
            self.logger.info(
                f"derived assignments for {topic}-{partition}: {info}")
            return self._equal_assignments(info, assignments)

        wait_until(derived_done, timeout_sec=timeout_sec, backoff_sec=2)

    def _do_move_and_verify(self, topic, partition, timeout_sec):
        admin = Admin(self.redpanda)

        _, new_assignment = self._dispatch_random_partition_move(
            topic=topic, partition=partition)

        self._wait_post_move(topic, partition, new_assignment, timeout_sec)

        return (topic, partition, new_assignment)

    def _move_and_verify(self):
        # choose a random topic-partition
        metadata = self.client().describe_topics()
        topic, partition = self._random_partition(metadata)
        # timeout for __consumer_offsets topic has to be long enough
        # to wait for compaction to finish. For resource constrained machines
        # and redpanda debug builds it may take a very long time
        timeout_sec = 360 if topic == "__consumer_offsets" else 90

        self.logger.info(f"selected topic-partition: {topic}-{partition}")

        self._do_move_and_verify(topic, partition, timeout_sec)

    def _replace_replica_set(self,
                             assignments,
                             allow_no_ops=True,
                             x_core_only=False):
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
            admin, assignments, allow_no_ops=allow_no_ops)

        return selected, replacements

    def _dispatch_random_partition_move(self,
                                        topic,
                                        partition,
                                        x_core_only=False,
                                        allow_no_op=True):
        """
        Request partition replicas to be randomly moved

        :param partition: partition id to be moved
        :param x_core_only: when true assignment nodes will not be changed, only cores
        """
        admin = Admin(self.redpanda)
        assignments = self._get_assignments(admin, topic, partition)
        prev_assignments = assignments.copy()

        self.logger.info(
            f"initial assignments for {topic}/{partition}: {prev_assignments}")

        # build new replica set by replacing a random assignment, do not allow no ops as we want to have operation to cancel
        selected, replacements = self._replace_replica_set(
            assignments, x_core_only=x_core_only, allow_no_ops=allow_no_op)

        self.logger.info(
            f"chose {len(selected)} replacements for {topic}/{partition}: {selected} -> {replacements}"
        )
        self.logger.info(
            f"new assignments for {topic}/{partition}: {assignments}")

        admin.set_partition_replicas(topic, partition, assignments)

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
                             timeout=90):
        """
        Request partition movement to interrupt and validates resulting cancellation against previous assignment
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

        # wait for previous assignment
        self._wait_post_move(topic, partition, previous_assignment, timeout)
