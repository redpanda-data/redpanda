# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.prealloc_nodes import PreallocNodesTest


class ArroyoTest(PreallocNodesTest):
    """
    Run the arroyo test suite against a redpanda cluster in
    a ducktape environment.

    The test suite lives here under tests/ in https://github.com/getsentry/arroyo.
    """
    TEST_SUITE_PATH = "/opt/arroyo"

    def __init__(self, ctx, *args, **kwargs):
        super().__init__(
            test_context=ctx,
            node_prealloc_count=1,
            *args,
            extra_rp_conf={
                # Disable leader balancer since arroyo test suite
                # does not refresh group information on reciept of
                # not_coordinator error_code
                "enable_leader_balancer": False
            },
            **kwargs)

    @cluster(num_nodes=4)
    def test_arroyo_test_suite(self):
        test_node = self.preallocated_nodes[0]

        try:
            env_preamble = f"DEFAULT_BROKERS={self.redpanda.brokers()}"

            failed = False
            for line in test_node.account.ssh_capture(
                    f"{env_preamble} "
                    f"python3 -m pytest {ArroyoTest.TEST_SUITE_PATH} "
                    "-k KafkaStreamsTestCase -rf",
                    combine_stderr=True,
                    allow_fail=False,
                    timeout_sec=120):
                self.logger.info(line)
                if 'FAILED' in line:
                    failed = True

            if failed:
                assert False, "Arroyo test failures occurred. Please check the log file"
        finally:
            # Possible reasons to enter this finally block are
            # 1. ssh_capture timeouts
            # 2. assert in source itself
            test_node.account.kill_process('arroyo',
                                           clean_shutdown=False,
                                           allow_fail=True)
