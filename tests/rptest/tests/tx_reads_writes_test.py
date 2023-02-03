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

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool

import subprocess


class TxReadsWritesTest(RedpandaTest):
    """
    Verify that segment indices are recovered on startup.
    """
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 1,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(TxReadsWritesTest, self).__init__(test_context=test_context,
                                                extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_reads_writes(self):
        verifier_jar = "/opt/verifiers/verifiers.jar"

        self.redpanda.logger.info("creating topics")

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1", partitions=1, replicas=1)

        test = "concurrent-reads-writes"

        try:
            cmd = "{java} -cp {verifier_jar} io.vectorized.tx_verifier.Verifier {test} {brokers}".format(
                java="java",
                verifier_jar=verifier_jar,
                test=test,
                brokers=self.redpanda.brokers())
            subprocess.check_output(["/bin/sh", "-c", cmd],
                                    stderr=subprocess.STDOUT)
            self.redpanda.logger.info(
                "txn test \"{test}\" passed".format(test=test))
        except subprocess.CalledProcessError as e:
            self.redpanda.logger.info(
                "txn test \"{test}\" failed".format(test=test))
            errors = ""
            errors += test + "\n"
            errors += str(e.output) + "\n"
            errors += "---------------------------\n"
            raise DucktapeError(errors)
