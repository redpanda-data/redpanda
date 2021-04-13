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
from rptest.wasm.topics_result_set import materialized_at_least_once_compare


class WasmRedpandaFailureRecoveryTest(WasmIdentityTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        conf = {'coproc_offset_flush_interval_ms': 1000}
        super(WasmRedpandaFailureRecoveryTest,
              self).__init__(test_context,
                             extra_rp_conf=conf,
                             num_records=num_records,
                             record_size=record_size)
        self._one_traunch_observed = False

    def records_recieved(self, output_recieved):
        if self._one_traunch_observed is False:
            self.restart_redpanda(random.sample(self.redpanda.nodes, 1)[0])
            self._one_traunch_observed = True


class WasmRPBasicFailureRecoveryTest(WasmRedpandaFailureRecoveryTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmRPBasicFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
        return [["sole_output_a"]]

    def verify_results(self):
        return materialized_at_least_once_compare


class WasmRPMultiScriptFailureRecoveryTest(WasmRedpandaFailureRecoveryTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmRPMultiScriptFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
        return [["aaa"], ["bbb"], ["ccc"]]

    def verify_results(self):
        return materialized_at_least_once_compare


class WasmRPMultiInputTopicFailureRecoveryTest(WasmRedpandaFailureRecoveryTest
                                               ):
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
        super(WasmRPMultiInputTopicFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
        return [["first_topic"], ["second_topic"], ["third_topic"]]

    def verify_results(self):
        return materialized_at_least_once_compare


class WasmRPMeshFailureRecoveryTest(WasmRedpandaFailureRecoveryTest):
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
        super(WasmRPMeshFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_xfactor(self):
        return 3

    def wasm_test_outputs(self):
        otopic_a = "output_topic_a"
        otopic_b = "output_topic_b"
        otopic_c = "output_topic_c"
        return [[otopic_a, otopic_b, otopic_c], [otopic_a, otopic_b, otopic_c],
                [otopic_a, otopic_b, otopic_c]]

    def verify_results(self):
        return materialized_at_least_once_compare
