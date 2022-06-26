# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.rw_verifier import RWVerifier
from rptest.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk import RpkException


class RWVerifierTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "id_allocator_replication": 3,
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "enable_auto_rebalance_on_node_add": False
        }

        super(RWVerifierTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=4)
    def test_rw(self):
        rpk = RpkTool(self.redpanda)

        def create_topic():
            try:
                rpk.create_topic("topic1", partitions=2)
                return True
            except RpkException as e:
                if "Kafka replied that the controller broker is -1" in str(e):
                    return False
                raise e

        wait_until(create_topic, 10, 0.1)

        rw_verifier = RWVerifier(self.test_context, self.redpanda)
        rw_verifier.start()

        rw_verifier.remote_start("topic1", self.redpanda.brokers(), "topic1",
                                 2)
        self.logger.info(f"Waiting for 1000 r&w operations")
        rw_verifier.ensure_progress("topic1", 1000, 10)
        self.logger.info(f"Done")
        rw_verifier.remote_stop("topic1")
        rw_verifier.remote_wait("topic1")
