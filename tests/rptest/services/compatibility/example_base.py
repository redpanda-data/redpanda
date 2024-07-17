# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from abc import ABC, abstractmethod


class ExampleBase(ABC):
    """
    The base class for example
    """
    def __init__(self, redpanda):
        # Instance of redpanda
        self._redpanda = redpanda

        # The result of the internal condiiton.
        # The internal condition is defined in the children.
        self._condition_met = False

    @abstractmethod
    def _condition(self, line: str) -> bool:
        """Subclasses override this to define the condition."""
        ...

    # Calls the internal condition and
    # automatically stores the result
    def condition(self, line):
        if not self._condition_met:
            self._condition_met = self._condition(line)

    # Was the internal condition met?
    def condition_met(self):
        return self._condition_met

    # Set the name of the node assigned to
    # this example.
    def set_node_name(self, node_name):
        # Noop by default since some examples
        # don't need this
        pass
