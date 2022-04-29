# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.utils.util import wait_until
from ducktape.tests.test import TestContext


class PreallocNodesTest(RedpandaTest):
    """
    A mixin class that extends RedpandaTest to
    preallocate ducktape nodes from the given test context.
    """
    def __init__(self, test_context: TestContext, node_prealloc_count: int,
                 *args, **kwargs):
        super(PreallocNodesTest, self).__init__(test_context, *args, **kwargs)
        self.node_prealloc_count = node_prealloc_count

        self.preallocated_nodes = test_context.cluster.alloc(
            ClusterSpec.simple_linux(node_prealloc_count))

        for node in self.preallocated_nodes:
            self.logger.debug(f'Allocated node {node.name}')

    def free_nodes(self):
        # Free the normally allocated nodes (e.g. RedpandaService)
        super().free_nodes()

        assert len(self.preallocated_nodes) == self.node_prealloc_count

        # Some tests may open huge numbers of connections, which can interfere
        # with subsequent tests' use of the node. Clear them down first.
        # For example, those tests that use FranzGoVerifiableProducer.
        for node in self.preallocated_nodes:
            wait_until(lambda: self.redpanda.sockets_clear(node),
                       timeout_sec=120,
                       backoff_sec=10)

            # Free the hand-allocated nodes
            self.logger.debug(f"Freeing node {node.name}")
            self.test_context.cluster.free_single(node)
