# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest


class PreallocNodesTest(RedpandaTest):
    """
    Extends RedpandaTest to preallocate ducktape
    nodes from the given test context.

    Having some explicitly allocated nodes is useful for
    re-using those nodes to run multiple services at once, or to
    run services that need to be on the same host (e.g. the KgoVerifier
    producer/consumers that coordinate via a local file)
    """

    _preallocated_nodes: list[ClusterNode]

    def __init__(
        self,
        test_context: TestContext,
        node_prealloc_count: int,
        *args: Any,
        **kwargs: Any,
    ):
        super(PreallocNodesTest, self).__init__(test_context, *args, **kwargs)
        self.node_prealloc_count = node_prealloc_count

        # Nodes are allocated later on first access
        self._preallocated_nodes = []

    @property
    def preallocated_nodes(self):
        if not self._preallocated_nodes:
            self._preallocated_nodes = self.test_context.cluster.alloc(
                ClusterSpec.simple_linux(self.node_prealloc_count)
            )

            for node in self._preallocated_nodes:
                self.logger.debug(f"Allocated node {node.name}")

            # preallocated nodes may be used by any service (e.g. redpanda
            # broker, OMB worker, etc.). However, note below in
            # free_preallocated_nodes that clean-up specific to a redpanda
            # broker is performed (RedpandaService::sockets_clear), which waits
            # for Redpanda broker ports to be cleaned up. We've seen that fail
            # on a pre-allocated node which was using a non-Redpanda broker
            # service, suggesting that a previous user of the node was a
            # Redpanda broker and it failed to clean up, and that most likely
            # that broker was still running and listening on the port. So before
            # we use the preallocated node, clean off the node for leftovers of a
            # previous Redpanda broker service even if it is not the service
            # which will be using this node.
            for node in self._preallocated_nodes:
                self.logger.debug(f"Cleaning node {node.name}")
                self.redpanda.clean_node_state(node)

        return self._preallocated_nodes

    def free_nodes(self):
        # Free the normally allocated nodes (e.g. RedpandaService)
        super().free_nodes()

        self.free_preallocated_nodes()

    def free_preallocated_nodes(self):
        """
        If `preallocated_nodes` has been accessed, free the nodes that
        were allocated there.  If `preallocated_nodes` is used again after
        calling this, then some fresh nodes will be allocated: it is safe
        to do this repeatedly.
        """
        if self._preallocated_nodes:
            assert len(self.preallocated_nodes) == self.node_prealloc_count

            # Some tests may open huge numbers of connections, which can interfere
            # with subsequent tests' use of the node. Clear them down first.
            # For example, those tests that use KgoVerifierProducer.
            for node in self.preallocated_nodes:
                wait_until(
                    lambda: self.redpanda.sockets_clear(node),
                    timeout_sec=120,
                    backoff_sec=10,
                )

                # Free the hand-allocated nodes
                self.logger.debug(f"Freeing node {node.name}")
                self.test_context.cluster.free_single(node)

            self._preallocated_nodes = []
