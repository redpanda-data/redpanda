# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.errors import DucktapeError

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool

import subprocess


class TxVerifierTest(RedpandaTest):
    """
    Verify that segment indices are recovered on startup.
    """
    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "enable_transactions": True,
            "default_topic_replications": 3,
            "default_topic_partitions": 1
        }

        super(TxVerifierTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_tx(self):
        verifier_jar = "/opt/tx-verifier/tx-verifier.jar"

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")
        rpk.create_topic("topic2")

        self.redpanda.logger.info("starting tx verifier")
        try:
            cmd = ("{java} -jar {verifier_jar} {brokers}").format(
                java="java",
                verifier_jar=verifier_jar,
                brokers=self.redpanda.brokers())
            subprocess.check_output(["/bin/sh", "-c", cmd],
                                    stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            raise DucktapeError("tx test failed: " + str(e.output))
