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

from rptest.tests.redpanda_test import RedpandaTest, TopicSpec


class LeadershipTransferTest(RedpandaTest):
    """
    Transfer leadership from one node to another.
    """
    topics = (TopicSpec(partitions=3, replication_factor=3), )

    @cluster(num_nodes=3)
    def test_controller_recovery(self):
        kc = KafkaCat(self.redpanda)

        # choose a partition and a target node
        partition = self._get_partition(kc)
        target_node_id = next(
            filter(lambda r: r["id"] != partition["leader"],
                   partition["replicas"]))["id"]
        self.logger.debug(
            f"Transfering leader from {partition['leader']} to {target_node_id}"
        )

        # build the transfer url
        meta = kc.metadata()
        brokers = meta["brokers"]
        source_broker = next(
            filter(lambda b: b["id"] == partition["leader"], brokers))
        target_broker = next(
            filter(lambda b: b["id"] == target_node_id, brokers))
        self.logger.debug(f"Source broker {source_broker}")
        self.logger.debug(f"Target broker {target_broker}")
        host = source_broker["name"]
        host = host.split(":")[0]
        partition_id = partition["partition"]
        url = "http://{}:9644/v1/kafka/{}/{}/transfer_leadership?target={}".format(
            host, self.topic, partition["partition"], target_node_id)

        def try_transfer():
            self.logger.debug(url)
            res = requests.post(url)
            self.logger.debug(res.text)
            for _ in range(3):  # just give it a moment
                time.sleep(1)
                meta = kc.metadata()
                partition = next(
                    filter(lambda p: p["partition"] == partition_id,
                           meta["topics"][0]["partitions"]))
                if partition["leader"] == target_node_id:
                    return True
            return False

        wait_until(lambda: try_transfer(),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Transfer did not complete")

    def _get_partition(self, kc):
        partition = [None]

        def get_partition():
            meta = kc.metadata()
            topics = meta["topics"]
            assert len(topics) == 1
            assert topics[0]["topic"] == self.topic
            partition[0] = random.choice(topics[0]["partitions"])
            if partition[0]["leader"] > 0:
                return True
            return False

        wait_until(lambda: get_partition(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="No partition with leader available")

        return partition[0]
