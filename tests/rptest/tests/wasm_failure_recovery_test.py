# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.clients.types import TopicSpec
from rptest.tests.wasm_identity_test import WasmIdentityTest
from rptest.wasm.topics_result_set import materialized_result_set_compare


class WasmFailureRecoveryTest(WasmIdentityTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmFailureRecoveryTest, self).__init__(test_context,
                                                      extra_rp_conf=None,
                                                      num_records=num_records,
                                                      record_size=record_size)
        self._one_traunch_observed = False

    def records_recieved(self, output_recieved):
        if self._one_traunch_observed is False:
            self.restart_wasm_engine(random.sample(self.redpanda.nodes, 1)[0])
            self._one_traunch_observed = True


class WasmBasicFailureRecoveryTest(WasmFailureRecoveryTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmBasicFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
        return [["sole_output_a"]]

    def verify_results(self):
        return materialized_result_set_compare


class WasmMultiScriptFailureRecoveryTest(WasmFailureRecoveryTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmMultiScriptFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
        return [["aaa"], ["bbb"], ["ccc"]]

    def verify_results(self):
        return materialized_result_set_compare


class WasmMultiInputTopicFailureRecoveryTest(WasmFailureRecoveryTest):
    topics = (
        TopicSpec(partition_count=3,
                  replication_factor=3,
                  cleanup_policy=TopicSpec.CLEANUP_DELETE),
        TopicSpec(partition_count=3,
                  replication_factor=3,
                  cleanup_policy=TopicSpec.CLEANUP_DELETE),
        TopicSpec(partition_count=3,
                  replication_factor=3,
                  cleanup_policy=TopicSpec.CLEANUP_DELETE),
    )

    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmMultiInputTopicFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
        return [["first_topic"], ["second_topic"], ["third_topic"]]

    def verify_results(self):
        return materialized_result_set_compare
