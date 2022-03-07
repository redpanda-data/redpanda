# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import ignore
from rptest.services.cluster import cluster
from rptest.tests.wasm_identity_test import WasmIdentityTest
from rptest.wasm.wasm_script import WasmScript
from rptest.wasm.wasm_build_tool import WasmTemplateRepository


class WasmCreateTopicsTest(WasmIdentityTest):
    def __init__(self, test_context, num_records=10, record_size=1024):
        super(WasmCreateTopicsTest, self).__init__(test_context,
                                                   extra_rp_conf=None,
                                                   num_records=num_records,
                                                   record_size=record_size)

    def wasm_test_plan(self):
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["output_1", "output_2", "output_3"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]

    @ignore  # see: #3858
    @cluster(num_nodes=4)
    def verify_materialized_topics_test(self):
        super().verify_materialized_topics_test()

        def rpk_partition_sort(p):
            return p.id

        # Verify sound input, and grab the input topics config
        test_cfg = self._rpk_tool.describe_topic(self.topic)
        test_cfg = sorted(test_cfg, key=rpk_partition_sort)

        # Ensure all materialized topics created and configs match inputs
        for topic in self.wasm_test_output():
            output = self._rpk_tool.describe_topic(topic)
            output = sorted(output, key=rpk_partition_sort)
            if output != test_cfg:
                raise Exception("Bad config, expected: %s observed: %s" %
                                (test_cfg, output))


class WasmDeleteTopicsTest(WasmIdentityTest):
    def __init__(self, test_context, num_records=10, record_size=1024):
        super(WasmDeleteTopicsTest, self).__init__(test_context,
                                                   extra_rp_conf=None,
                                                   num_records=num_records,
                                                   record_size=record_size)

    def wasm_test_plan(self):
        return [
            WasmScript(inputs=self.wasm_test_input(),
                       outputs=["output_1", "output_2", "output_3"],
                       script=WasmTemplateRepository.IDENTITY_TRANSFORM)
        ]

    @ignore  # https://github.com/redpanda-data/redpanda/issues/3745
    @cluster(num_nodes=4)
    def verify_materialized_topics_test(self):
        super().verify_materialized_topics_test()

        for topic in self.wasm_test_output():
            self._rpk_tool.delete_topic(topic)
        self._rpk_tool.delete_topic(self.topic)

        topics = self._rpk_tool.list_topics()

        for topic in self.wasm_test_output():
            if topic in topics:
                raise Exception(
                    'Failed to delete materialized topic %s - topics: %s' %
                    (topic, topics))

        if self.topic in topics:
            raise Exception('Failed to delete source topic: %s - topics: %s' %
                            (self.topic, topics))
