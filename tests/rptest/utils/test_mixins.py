from logging import Logger
from typing import Any, Callable, cast
from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.tests.test import TestContext, Test
from ducktape.utils.util import wait_until

from rptest.services.redpanda import RedpandaService, RedpandaServiceCloud


class PreallocNodesMixin:
    """
    A test mixin to preallocate some nodes.

    Having some explicitly allocated nodes is useful for
    re-using those nodes to run multiple services at once, or to
    run services that need to be on the same host (e.g. the KgoVerifier
    producer/consumers that coordinate via a local file)
    """

    _preallocated_nodes: list[ClusterNode]

    # these must be provided by the mixin's environment (i.e., other
    # parts of the class hierarchy)
    test_context: TestContext
    logger: Logger

    def __init__(self, node_prealloc_count: int, **kwargs: Any):
        super().__init__(**kwargs)
        self.node_prealloc_count = node_prealloc_count

        # Nodes are allocated later on first access
        self._preallocated_nodes = []

    @property
    def preallocated_nodes(self):
        if not self._preallocated_nodes:
            self._preallocated_nodes = self.test_context.cluster.alloc(
                ClusterSpec.simple_linux(self.node_prealloc_count))

            for node in self._preallocated_nodes:
                self.logger.debug(f'Allocated node {node.name}')

        return self._preallocated_nodes

    def free_nodes(self):
        # Free the normally allocated nodes (e.g. RedpandaService)
        cast(Test, super()).free_nodes()

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
                wait_until(lambda: self.__redpanda.sockets_clear(node),
                           timeout_sec=120,
                           backoff_sec=10)

                # Free the hand-allocated nodes
                self.logger.debug(f"Freeing node {node.name}")
                self.test_context.cluster.free_single(node)

            self._preallocated_nodes = []

    @property
    def __redpanda(self) -> RedpandaService | RedpandaServiceCloud:
        return getattr(self, 'redpanda')
