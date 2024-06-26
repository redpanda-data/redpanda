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

# Expected log errors in tests that test misbehaving
# transactional clients.
TX_ERROR_LOGS = [
    # e.g. tx - [{kafka/topic1/0}] - rm_stm.cc:461 - Can't prepare pid:{producer_identity: id=1, epoch=27} - unknown session
    "tx -.*rm_stm.*unknown session"
]


class TxVerifierTest(RedpandaTest):
    """
    Verify that segment indices are recovered on startup.
    """
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "partition_autobalancing_mode": "off",
        }

        super(TxVerifierTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf)

    def verify(self, tests):
        verifier_jar = "/opt/verifiers/verifiers.jar"

        self.redpanda.logger.info("creating topics")

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")
        rpk.create_topic("topic2")

        errors = ""

        for test in tests:
            self.redpanda.logger.info(
                "testing txn test \"{test}\"".format(test=test))
            try:
                cmd = "{java} -cp {verifier_jar} io.vectorized.tx_verifier.Verifier {test} {brokers}".format(
                    java="java",
                    verifier_jar=verifier_jar,
                    test=test,
                    brokers=self.redpanda.brokers())
                subprocess.check_output(["/bin/sh", "-c", cmd],
                                        stderr=subprocess.STDOUT,
                                        timeout=240)
                self.redpanda.logger.info(
                    "txn test \"{test}\" passed".format(test=test))
            except subprocess.CalledProcessError as e:
                self.redpanda.logger.info(
                    "txn test \"{test}\" failed".format(test=test))
                errors += test + "\n"
                errors += str(e.output) + "\n"
                errors += "---------------------------\n"

        if len(errors) > 0:
            raise DucktapeError(errors)

    @cluster(num_nodes=3, log_allow_list=TX_ERROR_LOGS)
    def test_all_tx_tests(self):
        self.verify([
            "init", "tx", "txes", "abort", "commuting-txes", "conflicting-tx",
            "read-committed-seek", "read-uncommitted-seek",
            "read-committed-tx-seek", "read-uncommitted-tx-seek",
            "fetch-reads-committed-txs", "fetch-skips-aborted-txs",
            "read-committed-seek-waits-ongoing-tx",
            "read-committed-seek-waits-long-hanging-tx",
            "read-committed-seek-reads-short-hanging-tx",
            "read-uncommitted-seek-reads-ongoing-tx", "set-group-start-offset",
            "read-process-write"
        ])
