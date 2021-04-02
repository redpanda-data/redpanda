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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
import requests

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest


class PartitionMovementTest(RedpandaTest):
    """
    Change a partition's replica set.
    """
    topics = (TopicSpec(partition_count=1, replication_factor=1), )

    # TODO: the movement api currently accepts requests for any core and any
    # node so we cannot yet test for invalid target node and partition.
    @cluster(num_nodes=3)
    def test_bad_requests(self):
        controller = self.redpanda.controller()
        topic = self.topics[0].name
        partition = 0

        # verify that we get an error if we attempt to move a partition without
        # specifying a target replica set. this will be removed later when we
        # add functionality that can choose automatically.
        for node in self.redpanda.nodes:
            host = node.account.hostname
            base_url = f"http://{host}:9644/v1/kafka/{topic}/{partition}/move_partition"
            for params in ["", "?", "?target="]:
                url = f"{base_url}{params}"
                res = requests.post(url)
                assert res.status_code == 400
                assert "Partition movement requires target replica set" in res.text

        # check that errors are returned for invalid target formats
        for node in self.redpanda.nodes:
            host = node.account.hostname
            base_url = f"http://{host}:9644/v1/kafka/{topic}/{partition}/move_partition"
            for params in [
                    "1", "a", "1,", "a,", "1,a", "a,1", "1,1,1", "1,1,a",
                    "a,1,1", "a,1,1,", "1,1,1,a"
            ]:
                url = f"{base_url}?target={params}"
                res = requests.post(url)
                assert res.status_code == 400
                assert "Invalid target format" in res.text

        # check for reject request for source partition that does not exist
        for node in self.redpanda.nodes:
            host = node.account.hostname
            url = f"http://{host}:9644/v1/kafka/{topic}/9999/move_partition?target=1,0"
            res = requests.post(url)
            assert res.status_code == 400
            if node == controller:
                assert "Requested partition does not exist" in res.text
            else:
                assert "raft::errc::not_leader" in res.text

        # check for reject request for source topic that does not exist
        for node in self.redpanda.nodes:
            host = node.account.hostname
            url = f"http://{host}:9644/v1/kafka/topic-dne/{partition}/move_partition?target=1,0"
            res = requests.post(url)
            assert res.status_code == 400
            if node == controller:
                assert "Topic does not exist" in res.text
            else:
                assert "raft::errc::not_leader" in res.text

    @cluster(num_nodes=3)
    def test_cycle_simple(self, rounds=4):
        """
        Verifies that an empty partition with one replica can be moved around
        between random brokers in serveral rounds.

        TODO: when picking the set of eligible nodes we filter out the current
        node because move-to-self is not yet supported.
        """
        controller = self.redpanda.controller()
        base_url = f"http://{controller.account.hostname}:9644"
        base_url = f"{base_url}/v1/kafka/{self.topics[0].name}/0/move_partition"

        # shards per node for this cluster
        shards = self.redpanda.shards()

        def get_partition_shard(node):
            metrics = self.redpanda.metrics(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_storage_log_partition_size" and \
                            sample.labels["namespace"] == "kafka" and \
                            sample.labels["topic"] == self.topics[0].name and \
                            sample.labels["partition"] == "0":
                        return int(sample.labels["shard"])
            return None

        def partition_moved(to_node_id, to_shard):
            partitions = self.redpanda.partitions(self.topics[0].name)
            if not partitions:
                return False
            partition = partitions[0]
            node_id = self.redpanda.idx(partition.replicas[0])
            shard = get_partition_shard(partition.replicas[0])
            self.logger.debug(
                f"Waiting for partition {to_node_id}:{to_shard} current {node_id}:{shard}"
            )
            return to_node_id == node_id and to_shard == shard

        def move_partition(to_node_id, to_shard):
            url = f"{base_url}?target={to_node_id},{to_shard}"
            res = requests.post(url)
            self.logger.debug(
                f"Requesting partition move {url}: {res.status_code} {res.text}"
            )
            return res.status_code == 200

        for round in range(rounds):
            partition = self.redpanda.partitions(self.topics[0].name)[0]
            eligible = set(self.redpanda.nodes) - set(partition.replicas)
            target = random.choice(list(eligible))

            from_node_id = self.redpanda.idx(partition.replicas[0])
            to_node_id = self.redpanda.idx(target)
            to_shard = random.randint(0, shards[to_node_id])

            self.logger.debug(
                f"Moving partition round {round}: from node {from_node_id} to {to_node_id}:{to_shard}"
            )

            wait_until(lambda: move_partition(to_node_id, to_shard),
                       timeout_sec=10,
                       backoff_sec=2,
                       err_msg="Previous move haven't finished")

            wait_until(lambda: partition_moved(to_node_id, to_shard),
                       timeout_sec=20,
                       backoff_sec=5,
                       err_msg="Partition move did not complete")
