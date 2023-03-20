# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.services.kgo_repeater_service import KgoRepeaterService
from rptest.services.cluster import cluster
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, PREV_VERSION_LOG_ALLOW_LIST
from rptest.tests.random_node_operations_test import RandomNodeOperationsTestBase, TestSpec


class RandomNodeOperationsScaleTest(RandomNodeOperationsTestBase):
    """RandomNodeOperationsTest customized to run at larger scale, more number of fuzz ops
    and longer time. We use kgo verifier to avoid OOM issues with stock verifiers but
    it does not support compacted topics."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.svc = None

    def start_workload(self):
        self.svc = KgoRepeaterService(context=self.test_context,
                                      redpanda=self.redpanda,
                                      workers=3,
                                      topic=self.topic,
                                      msg_size=16384)
        self.svc.start()
        self.svc.prepare_and_activate()
        self.svc.await_group_ready()

    def end_workload(self):
        if not self.svc:
            return
        try:
            # Validate everything produced so far is consumed
            produced, _ = self.svc.total_messages()

            def all_consumed():
                _, c = self.svc.total_messages()
                self.redpanda.logger.debug(
                    f"waiting for: {produced}, consumed: {c}")
                return c >= produced

            self.redpanda.wait_until(all_consumed,
                                     timeout_sec=180,
                                     backoff_sec=2)
        finally:
            self.svc.stop()

    @cluster(num_nodes=8,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(enable_failures=[True],
            num_to_upgrade=[3],
            compacted_topics=[False])
    def test_node_operations(self, enable_failures, num_to_upgrade,
                             compacted_topics):

        test_spec = TestSpec(num_brokers=7,
                             enable_failures=enable_failures,
                             num_to_upgrade=num_to_upgrade,
                             compacted_topics=compacted_topics,
                             num_topics=10,
                             max_partitions=100,
                             node_fuzz_ops=300,
                             admin_fuzz_ops=200)

        super().run_fuzzer(test_spec=test_spec)
