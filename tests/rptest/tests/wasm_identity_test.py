# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.wasm.topic import get_source_topic
from rptest.wasm.topics_result_set import materialized_result_set_compare, group_fan_in_verifier
from rptest.wasm.wasm_test import WasmTest
from rptest.wasm.wasm_script import WasmScript
from rptest.wasm.wasm_build_tool import WasmTemplateRepository
from rptest.services.redpanda import DEFAULT_LOG_ALLOW_LIST

WASM_LOG_ALLOW_LIST = DEFAULT_LOG_ALLOW_LIST + [
    "Wasm engine failed to reply to heartbeat", "Failed to connect wasm engine"
]


class WasmIdentityTest(WasmTest):
    topics = (TopicSpec(partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self,
                 test_context,
                 extra_rp_conf=None,
                 num_records=1024,
                 record_size=1024):
        super(WasmIdentityTest, self).__init__(test_context,
                                               extra_rp_conf=extra_rp_conf
                                               or {},
                                               record_size=record_size)
        self._num_records = num_records

    def wasm_inputs_throughput(self):
        """
        Producer parameters across all input topics
        """
        return {topic: self._num_records for topic in self.wasm_test_input()}

    def wasm_outputs_throughput(self):
        """
        Consumer parameters across all output topics
        """
        return {topic: self._num_records for topic in self.wasm_test_output()}

    @cluster(num_nodes=4, log_allow_list=WASM_LOG_ALLOW_LIST)
    def verify_materialized_topics_test(self):
        self.verify_results(materialized_result_set_compare)


class WasmBasicIdentityTest(WasmIdentityTest):
    def __init__(self, test_context):
        super(WasmBasicIdentityTest, self).__init__(test_context,
                                                    num_records=10240,
                                                    record_size=1024)

    def wasm_test_plan(self):
        """
        The materialized log:
        [
          itopic._script_a_output_,
        ]
        Should exist by tests end and be identical to its respective input log
        """
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["script_a_output"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]


class WasmMultiScriptIdentityTest(WasmIdentityTest):
    """
    In this test spec there is one input topic and three coprocessors.
    Each coprocessor consumes from the same sole input topic and produces
    to one output topic.
    """
    topics = (TopicSpec(partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        super(WasmMultiScriptIdentityTest, self).__init__(test_context,
                                                          num_records=10240,
                                                          record_size=1024)

    def wasm_test_plan(self):
        """
        The materialized logs:
        [
          itopic._script_a_output_,
          itopic._script_b_output_,
          itopic._script_c_output_,
        ]
        """
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["sou_a", "sou_b", "sou_c"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]


class WasmMultiInputTopicIdentityTest(WasmIdentityTest):
    """
    In this test spec there are three input topics and three coprocessors.
    Each coprocessor consumes from the same input topic and produces
    to one output topic, making three materialized topics per script.
    """
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

    def __init__(self, test_context):
        super(WasmMultiInputTopicIdentityTest,
              self).__init__(test_context, num_records=10240, record_size=1024)

    def wasm_test_plan(self):
        """
        The materialized logs:
        [
          itopic[0]._script_a_output_,
          itopic[1]._script_a_output_,
          itopic[2]._script_a_output_,
          itopic[0]._script_b_output_,
          itopic[1]._script_b_output_,
          itopic[2]._script_b_output_,
          itopic[0]._script_c_output_,
          itopic[1]._script_c_output_,
          itopic[2]._script_c_output_,
        ]
        Should exist by tests end and be identical to their respective input logs
        """
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=['script_a_output'],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=['script_b_output'],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=['script_c_output'],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]

    @cluster(num_nodes=6, log_allow_list=WASM_LOG_ALLOW_LIST)
    def verify_materialized_topics_test(self):
        self.verify_results(materialized_result_set_compare)


class WasmAllInputsToAllOutputsIdentityTest(WasmIdentityTest):
    """
    In this test spec there are three input topics and three coprocessors.
    Each coprocessor consumes from the same input topic and produces
    to three output topics, making three materialized topic per script.
    """
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

    def __init__(self, test_context):
        super(WasmAllInputsToAllOutputsIdentityTest,
              self).__init__(test_context, num_records=3024, record_size=1024)

    def wasm_test_plan(self):
        """
        The materialized logs:
        [
          itopic[0]._script_a_output_,
          itopic[1]._script_a_output_,
          itopic[2]._script_a_output_,
          itopic[0]._script_b_output_,
          itopic[1]._script_b_output_,
          itopic[2]._script_b_output_,
          itopic[0]._script_c_output_,
          itopic[1]._script_c_output_,
          itopic[2]._script_c_output_,
        ]
        Should exist by tests end and be identical to their respective input logs.

        This differs from the above because every script is writing to non unique
        output topics. Therefore this tests the output topic mutex within the
        script context.
        """
        opts = ["output_topic_a", "output_topic_b", "output_topic_c"]
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=opts,
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=opts,
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM),
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=opts,
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]

    @cluster(num_nodes=6, log_allow_list=WASM_LOG_ALLOW_LIST)
    def verify_materialized_topics_test(self):
        # Cannot compare topics to topics, can only verify # of records
        self.start_wasm()
        input_results, output_results = self.wait_on_results()
        if not group_fan_in_verifier(self.topics, input_results,
                                     output_results):
            raise Exception(
                "Incorrect number of records observed across topics")
