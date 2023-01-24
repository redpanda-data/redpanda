# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService


class CrashLoopChecksTest(RedpandaTest):
    "Checks crash loop detection works as expected."

    CRASH_LOOP_LIMIT = 3

    CRASH_LOOP_LOG = [
        "Crash loop detected. Too many consecutive crashes.*",
        ".*Failure during startup: std::runtime_error \(Crash loop detected, aborting startup.\).*"
    ]

    # main - application.cc:348 - Failure during startup: std::__1::system_error (error C-Ares:4, unreachable_host.com: Not found)
    # main - application.cc:363 - Failure during startup: std::__1::system_error (error C-Ares:11, unreachable_host.com: Connection refused)
    HOSTNAME_ERRORS = [
        ".*Failure during startup: std::__1::system_error \(error C-Ares:4, unreachable_host.com: Not found\)",
        ".*Failure during startup: std::__1::system_error \(error C-Ares:11, unreachable_host.com: Connection refused\)"
    ]

    CRASH_LOOP_TRACKER_FILE = f"{RedpandaService.DATA_DIR}/startup_log"

    def __init__(self, test_context):
        super(CrashLoopChecksTest,
              self).__init__(test_context=test_context,
                             num_brokers=1,
                             extra_node_conf={
                                 "crash_loop_limit":
                                 CrashLoopChecksTest.CRASH_LOOP_LIMIT
                             })

    def remove_crash_loop_tracker_file(self, broker):
        broker.account.ssh(
            f"rm -f {CrashLoopChecksTest.CRASH_LOOP_TRACKER_FILE}")

    def get_broker_to_crash_loop_state(self, broker):
        for _ in range(CrashLoopChecksTest.CRASH_LOOP_LIMIT):
            self.redpanda.signal_redpanda(node=broker)
            self.redpanda.start_node(broker)
        self.redpanda.signal_redpanda(node=broker)
        self.redpanda.start_node(node=broker, expect_fail=True)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG)
    def test_crash_loop_checks_with_tracker_file(self):
        broker = self.redpanda.nodes[0]
        self.get_broker_to_crash_loop_state(broker)
        # Remove the crash loop log and restart, should start up.
        self.remove_crash_loop_tracker_file(broker)
        self.redpanda.start_node(broker)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG)
    def test_crash_loop_checks_with_node_config(self):
        broker = self.redpanda.nodes[0]
        self.get_broker_to_crash_loop_state(broker)
        # Update node configuration file to reset checksum
        update = dict(kafka_api=dict(address="127.0.0.1", port=9099))
        self.redpanda.start_node(broker, override_cfg_params=update)

    @cluster(num_nodes=1, log_allow_list=CRASH_LOOP_LOG + HOSTNAME_ERRORS)
    def test_crash_loop_with_misconfiguration(self):
        broker = self.redpanda.nodes[0]
        self.redpanda.signal_redpanda(broker)

        invalid_conf = dict(
            kafka_api=dict(address="unreachable_host.com", port=9092))
        for _ in range(CrashLoopChecksTest.CRASH_LOOP_LIMIT + 1):
            self.redpanda.start_node(broker,
                                     override_cfg_params=invalid_conf,
                                     expect_fail=True)
        # None of the attempts so far should be considered a crash loop.
        assert not self.redpanda.search_log_node(
            broker, "Too many consecutive crashes")

        # Start again, crash loop should be detected.
        self.redpanda.start_node(broker,
                                 override_cfg_params=invalid_conf,
                                 expect_fail=True)
        assert self.redpanda.search_log_node(broker,
                                             "Too many consecutive crashes")

        # Fix the config and start, crash loop should be reset.
        self.redpanda.start_node(node=broker)
