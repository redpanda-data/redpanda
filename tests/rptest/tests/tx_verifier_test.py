# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.mark import ignore
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
            "transaction_coordinator_replication": 3,
            "id_allocator_replication": 3,
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "enable_auto_rebalance_on_node_add": False
        }

        super(TxVerifierTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf)

    def verify(self, tests):
        verifier_jar = "/opt/tx-verifier/tx-verifier.jar"

        self.redpanda.logger.info("creating topics")

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1")
        rpk.create_topic("topic2")

        errors = ""

        for test in tests:
            self.redpanda.logger.info(
                "testing txn test \"{test}\"".format(test=test))
            try:
                cmd = "{java} -jar {verifier_jar} {test} {brokers}".format(
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
                errors += test + "\n"
                errors += str(e.output) + "\n"
                errors += "---------------------------\n"

        if len(errors) > 0:
            raise DucktapeError(errors)

    @cluster(num_nodes=3)
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
