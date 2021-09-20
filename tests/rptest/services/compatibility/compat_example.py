# Copyright 2020 Vectorized, Inc.
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
from .compat_helpers import create_helper


class CompatExample(BackgroundThreadService):
    """
    The service that runs examples in the background
    """
    def __init__(self, context, redpanda, topic, extra_conf={}):
        super(CompatExample, self).__init__(context, num_nodes=1)

        # Create the correct helper instance from the test function being run.
        # helper is an object with various methods to help run the example
        self._helper = create_helper(context.function_name, redpanda, topic,
                                     extra_conf)

        # The amount of time to run the example.
        # If user removes timeout key, then use 10 seconds.
        self._timeout = extra_conf.get("timeout") or 10

    def _worker(self, idx, node):
        start_time = time.time()

        # Some examples require the hostname of the node
        self._helper.set_node_name(node.name)

        # Run the example until the condition is met or timeout occurs
        output_iter = node.account.ssh_capture(self._helper.cmd())
        while not self._helper.condition_met(
        ) and time.time() < start_time + self._timeout:
            line = next(output_iter)
            # Call to helper.condition will automatically
            # store result in a boolean variable
            self._helper.condition(line.rstrip())

    # Used to determine if the condition is met
    def ok(self):
        return self._helper.condition_met()

    # Returns the node name that the example is running on
    def node_name(self):
        return self._helper.node_name()

    def stop_node(self, node):
        node.account.kill_process(self._helper.process_to_kill())

    def clean_node(self, nodes):
        pass
