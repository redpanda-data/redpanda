# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.errors import DucktapeError
from time import sleep

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool

import subprocess

# Expected log errors in tests that test misbehaving
# idempotency clients.
TX_ERROR_LOGS = []

NOT_LEADER_FOR_PARTITION = "Tried to send a message to a replica that is not the leader for some partition"


class SaramaProduceTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "id_allocator_replication": 3,
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(SaramaProduceTest, self).__init__(test_context=test_context,
                                                extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3, log_allow_list=TX_ERROR_LOGS)
    def test_produce(self):
        verifier_bin = "/opt/redpanda-tests/go/sarama/produce_test/produce_test"

        self.redpanda.logger.info("creating topics")

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")

        self.redpanda.logger.info("testing sarama produce")
        retries = 5
        for i in range(0, retries):
            try:
                cmd = "{verifier_bin} --brokers {brokers}".format(
                    verifier_bin=verifier_bin, brokers=self.redpanda.brokers())
                subprocess.check_output(["/bin/sh", "-c", cmd],
                                        stderr=subprocess.STDOUT)
                self.redpanda.logger.info("sarama produce test passed")
                break
            except subprocess.CalledProcessError as e:
                error = str(e.output)
                self.redpanda.logger.info("sarama produce failed with " +
                                          error)
                if i + 1 != retries and NOT_LEADER_FOR_PARTITION in error:
                    sleep(5)
                    continue
                raise DucktapeError("sarama produce failed with " + error)
