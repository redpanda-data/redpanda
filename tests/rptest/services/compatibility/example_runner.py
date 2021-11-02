# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
import time
from ducktape.services.background_thread import BackgroundThreadService


class ExampleRunner(BackgroundThreadService):
    """
    The service that runs examples in the background
    """
    def __init__(self, context, example, timeout_sec=10):
        super(ExampleRunner, self).__init__(context, num_nodes=1)

        self._example = example

        self._timeout = timeout_sec

    def _worker(self, idx, node):
        start_time = time.time()

        # Some examples require the hostname of the node
        self._example.set_node_name(node.name)

        # Run the example until the condition is met or timeout occurs
        output_iter = node.account.ssh_capture(self._example.cmd())
        while not self._example.condition_met(
        ) and time.time() < start_time + self._timeout:
            line = next(output_iter)
            line = line.rstrip()
            self.logger.debug(line)

            # Call to example.condition will automatically
            # store result in a boolean variable
            self._example.condition(line)

    # Returns the node name that the example is running on
    def node_name(self):
        return self._example.node_name()

    def stop_node(self, node):
        node.account.kill_process(self._example.process_to_kill())

    def clean_node(self, nodes):
        pass
