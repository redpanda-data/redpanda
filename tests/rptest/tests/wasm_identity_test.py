# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster

from rptest.wasm.wasm_test import WasmTest, WasmScript
from rptest.wasm.wasm_build_tool import WasmTemplateRepository
from rptest.wasm.topic import construct_materialized_topic

from rptest.clients.types import TopicSpec


class WasmIdentityTest(WasmTest):
    topics = (TopicSpec(partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        super(WasmIdentityTest, self).__init__(test_context, extra_rp_conf={})

    def wasm_test_plan(self):
        input_topic = self.topics[0].name
        mapped_topic = "myoutputtopic"
        output_topic = construct_materialized_topic(input_topic, mapped_topic)

        num_records = 1024
        record_size = 1024

        # The identity transform produces 1 identital record onto a topic for
        # each input record. The result should be a 1 to 1 mapping between a
        # source and destination topic, they should be identical when compared
        basic_script = WasmScript(
            inputs=[(input_topic, (num_records, record_size))],
            outputs=[(output_topic, num_records)],
            script=WasmTemplateRepository.IDENTITY_TRANSFORM)

        return [basic_script]

    @cluster(num_nodes=3)
    def ensure_identical_output_test(self):
        input_results, output_results = self._start(self.topics,
                                                    self.wasm_test_plan())
        assert input_results.num_records() == 1024
        if input_results != output_results:
            raise Exception(
                "Expected all records across topics to be equivalent")
