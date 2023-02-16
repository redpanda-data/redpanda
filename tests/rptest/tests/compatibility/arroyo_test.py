# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import subprocess

from rptest.services.cluster import cluster
from rptest.tests.prealloc_nodes import PreallocNodesTest
from ducktape.cluster.remoteaccount import RemoteCommandError


class ArroyoTest(PreallocNodesTest):
    """
    Run the arroyo test suite against a redpanda cluster in
    a ducktape environment.

    The test suite lives here under tests/ in https://github.com/getsentry/arroyo.
    """
    TEST_SUITE_PATH = "/opt/arroyo"

    def __init__(self, ctx, *args, **kwargs):
        super().__init__(test_context=ctx,
                         node_prealloc_count=1,
                         *args,
                         **kwargs)

    def _find_failed_tests(self, pytest_output: list[str]):
        return [line for line in pytest_output if "FAILED" in line]

    @cluster(num_nodes=4)
    def test_arroyo_test_suite(self):
        test_node = self.preallocated_nodes[0]

        try:
            env_preamble = f"DEFAULT_BROKERS={self.redpanda.brokers()}"
            capture = test_node.account.ssh_capture(
                f"{env_preamble} "
                f"python3 -m pytest {ArroyoTest.TEST_SUITE_PATH} "
                "-k KafkaStreamsTestCase -rf",
                combine_stderr=True)

            pytest_output = list(capture)
            for log_line in pytest_output:
                self.logger.info(log_line)

            failure_reports = self._find_failed_tests(pytest_output)
            if len(failure_reports) > 0:
                assert False, "Arroyo test failures occured. Please check the log file"
        except RemoteCommandError as err:
            if err.exit_status == 2:
                assert False, "Arroyo test suite was interrupted"
            elif err.exit_status == 3:
                assert False, "Internal error durring execution of Arroyo test suite"
            elif err.exit_status == 4:
                assert False, "Pytest command line invocation error"
