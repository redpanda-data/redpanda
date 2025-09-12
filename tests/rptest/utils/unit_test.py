# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from rptest.utils.parallel import execute_in_parallel


class ParallelTest(Test):
    def __init__(self, test_context):
        super(ParallelTest, self).__init__(test_context)
        self.items = list(range(0, 200, 2))
        self.batch_len = 10

    @cluster(num_nodes=0)
    def test_parallel_w_return(self):
        def process_batch(batch):
            # Simulate some processing
            return [x * 2 for x in batch]

        results = execute_in_parallel(self.items, process_batch, self.batch_len)

        # Check that the results are as expected
        expected_results = [x * 2 for x in self.items]
        assert results == expected_results, f"{expected_results} != {results}"

    @cluster(num_nodes=0)
    def test_parallel_wo_return(self):
        seen = []

        def process_batch(batch):
            nonlocal seen
            seen.extend(batch)

        results = execute_in_parallel(self.items, process_batch, self.batch_len)

        assert results is None
        assert sorted(seen) == self.items
