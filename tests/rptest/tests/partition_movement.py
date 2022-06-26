# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.util import wait_until


class PartitionMovementMixin():
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

    def _wait_post_move(self, topic, partition, assignments, timeout_sec):
        # wait until all nodes see new assignment
        def derived_done():
            info = self._get_current_partitions(self._admin_client, topic,
                                                partition)
            self.logger.info(
                f"derived assignments for {topic}-{partition}: {info}")
            return self._equal_assignments(info, assignments)

        wait_until(derived_done, timeout_sec=timeout_sec, backoff_sec=1)

        def status_done():
            for node in self.redpanda.nodes:
                info = self._admin_client.get_partitions(topic,
                                                         partition,
                                                         node=node)
                self.logger.info(
                    f"current assignments for {topic}-{partition}: {info}")
                converged = self._equal_assignments(info["replicas"],
                                                    assignments)
                if converged and info["status"] == "done":
                    return True
            return False

        # wait until redpanda reports complete
        wait_until(status_done, timeout_sec=timeout_sec, backoff_sec=1)

    def _do_move_and_verify(self, topic, partition, timeout_sec):
        # get the partition's replica set, including core assignments. the kafka
        # api doesn't expose core information, so we use the redpanda admin api.
        assignments = self._get_assignments(self._admin_client, topic,
                                            partition)
        self.logger.info(f"assignments for {topic}-{partition}: {assignments}")

        # build new replica set by replacing a random assignment
        selected, replacements = self._choose_replacement(
            self._admin_client, assignments)
        self.logger.info(
            f"replacement for {topic}-{partition}:{len(selected)}: {selected} -> {replacements}"
        )
        self.logger.info(
            f"new assignments for {topic}-{partition}: {assignments}")

        self._admin_client.set_partition_replicas(topic, partition,
                                                  assignments)

        self._wait_post_move(topic, partition, assignments, timeout_sec)

        return (topic, partition, assignments)

    def _move_and_verify(self):
        # choose a random topic-partition
        partitions = []
        for p in self._admin_client.get_partitions():
            if p["ns"] != "kafka":
                continue
            partitions.append((p["topic"], p["partition_id"]))
        topic, partition = random.choice(partitions)
        # timeout for __consumer_offsets topic has to be long enough
        # to wait for compaction to finish. For resource constrained machines
        # and redpanda debug builds it may take a very long time
        timeout_sec = 360 if topic == "__consumer_offsets" else 90

        self.logger.info(f"selected topic-partition: {topic}-{partition}")

        self._do_move_and_verify(topic, partition, timeout_sec)
