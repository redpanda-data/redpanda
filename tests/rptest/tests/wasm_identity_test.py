# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.mark import ignore
from rptest.clients.types import TopicSpec
from rptest.wasm.topic import construct_materialized_topic, get_source_topic
from rptest.wasm.topics_result_set import materialized_result_set_compare
from rptest.wasm.wasm_build_tool import WasmTemplateRepository
from rptest.wasm.wasm_test import WasmScript, WasmTest
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST


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
                                               or {})
        self._num_records = num_records
        self._record_size = record_size
        assert len(self.topics) >= 1

    def input_topics(self):
        """
        Default behavior is for all scripts to have all topics as input topics
        """
        return [x.name for x in self.topics]

    def wasm_test_outputs(self):
        raise Exception('Unimplemented method')

    def wasm_test_input(self):
        """
        Topics that will be produced onto, number of records and record_size
        """
        return [(x, self._num_records, self._record_size) for x in self.topics]

    def wasm_test_plan(self):
        """
        List of scripts to deploy, built from the results of wasm_test_outputs().
        By default inputs to all scripts will be self.input_topics()
        """
        itopics = self.input_topics()
        return [
            WasmScript(inputs=itopics,
                       outputs=opts,
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
            for opts in self.wasm_test_outputs()
        ]

    def verify_results(self):
        """ How to interpret PASS/FAIL between two sets of returned results"""
        return materialized_result_set_compare

    @ignore  # https://github.com/vectorizedio/redpanda/issues/2514
    @cluster(num_nodes=3, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def verify_materialized_topics_test(self):
        """
        Entry point for all tests, asynchronously we perform the following:
        1. Scripts are built & deployed
        2. Consumers are set-up listening for expected records on output topics
        3. Producers set-up and begin producing onto input topics
        4. When finished, perform assertions in this method
        """
        self.start(self.wasm_test_input(), self.wasm_test_plan())
        input_results, output_results = self.wait_on_results()
        for script in self.wasm_test_outputs():
            for dest in script:
                outputs = set([
                    construct_materialized_topic(src.name, dest)
                    for src, _, _ in self.wasm_test_input()
                ])
                tresults = output_results.filter(lambda x: x.topic in outputs)
                if not self.verify_results()(input_results, tresults):
                    raise Exception(
                        f"Set {dest} results weren't as expected: {type(self).__name__}"
                    )


class WasmBasicIdentityTest(WasmIdentityTest):
    def __init__(self, test_context, num_records=10240, record_size=1024):
        super(WasmBasicIdentityTest, self).__init__(test_context,
                                                    num_records=num_records,
                                                    record_size=record_size)

    def wasm_test_outputs(self):
        """
        The materialized log:
        [
          itopic._script_a_output_,
        ]
        Should exist by tests end and be identical to its respective input log
        """
        return [["script_a_output"]]


class WasmMultiScriptIdentityTest(WasmIdentityTest):
    """
    In this test spec there is one input topic and three coprocessors.
    Each coprocessor consumes from the same sole input topic and produces
    to one output topic.
    """
    topics = (TopicSpec(partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context, num_records=10240, record_size=1024):
        super(WasmMultiScriptIdentityTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
        """
        The materialized logs:
        [
          itopic._script_a_output_,
          itopic._script_b_output_,
          itopic._script_c_output_,
        ]
        Should exist by tests end and be identical to their respective input logs

        """
        return [["sou_a"], ["sou_b"], ["sou_c"]]


class WasmMultiInputTopicIdentityTest(WasmIdentityTest):
    """
    In this test spec there are three input topics and three coprocessors.
    Each coprocessor consumes from the same input topic and produces
    to one output topic, making three materialized topic per script.
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

    def __init__(self, test_context, num_records=10240, record_size=1024):
        super(WasmMultiInputTopicIdentityTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
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
        return [["script_a_output"], ["script_b_output"], ["script_c_output"]]


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

    def __init__(self, test_context, num_records=3024, record_size=1024):
        super(WasmAllInputsToAllOutputsIdentityTest,
              self).__init__(test_context,
                             num_records=num_records,
                             record_size=record_size)

    def wasm_test_outputs(self):
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
        otopic_a = "output_topic_a"
        otopic_b = "output_topic_b"
        otopic_c = "output_topic_c"
        return [[otopic_a, otopic_b, otopic_c], [otopic_a, otopic_b, otopic_c],
                [otopic_a, otopic_b, otopic_c]]

    @ignore  # https://github.com/vectorizedio/redpanda/issues/2514
    @cluster(num_nodes=3)
    def verify_materialized_topics_test(self):
        # Cannot compare topics to topics, can only verify # of records
        self.start(self.wasm_test_input(), self.wasm_test_plan())
        input_results, output_results = self.wait_on_results()

        def compare(topic):
            iis = input_results.filter(lambda x: x.topic == topic)
            oos = output_results.filter(
                lambda x: get_source_topic(x.topic) == topic)
            return iis.num_records() == oos.num_records()

        if not all(compare(topic) for topic in self.topics):
            raise Exception(
                "Incorrect number of records observed across topics")
