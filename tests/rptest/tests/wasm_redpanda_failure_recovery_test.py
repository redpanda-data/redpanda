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
from rptest.services.cluster import cluster
from rptest.wasm.wasm_script import WasmScript
from rptest.wasm.wasm_build_tool import WasmTemplateRepository


class WasmRedpandaFailureRecoveryTest(WasmIdentityTest):
    topics = (TopicSpec(partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self,
                 test_context,
                 extra_rp_conf=None,
                 num_records=10000,
                 record_size=1024):
        conf = {'coproc_offset_flush_interval_ms': 1000}
        super(WasmRedpandaFailureRecoveryTest,
              self).__init__(test_context,
                             extra_rp_conf=conf,
                             record_size=record_size)
        self._one_traunch_observed = False
        self._num_records = num_records

    def records_recieved(self, output_recieved):
        if self._one_traunch_observed is False:
            self.restart_redpanda(random.sample(self.redpanda.nodes, 1)[0])
            self._one_traunch_observed = True

    def verifier(self):
        return materialized_at_least_once_compare


class WasmRPBasicFailureRecoveryTest(WasmRedpandaFailureRecoveryTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmRPBasicFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_plan(self):
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["sole_output_a"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]


class WasmRPMultiScriptFailureRecoveryTest(WasmRedpandaFailureRecoveryTest):
    def __init__(self, test_context, num_records=10000, record_size=1024):
        super(WasmRPMultiScriptFailureRecoveryTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_plan(self):
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["aaa"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["bbb"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["ccc"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]


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

    def wasm_test_plan(self):
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["first_topic", "second_topic", "third_topic"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]


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

    def wasm_test_plan(self):
        return [
            WasmScript(
                inputs=self.wasm_test_input(),
                outputs=["output_topic_a", "output_topic_b", "output_topic_c"],
                script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(
                inputs=self.wasm_test_input(),
                outputs=["output_topic_a", "output_topic_b", "output_topic_c"],
                script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(
                inputs=self.wasm_test_input(),
                outputs=["output_topic_a", "output_topic_b", "output_topic_c"],
                script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]
