# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError


class ExampleRunner(BackgroundThreadService):
    """
    The service that runs examples in the background
    """
    def __init__(self, context, example, timeout_sec=10):
        super(ExampleRunner, self).__init__(context, num_nodes=1)

        self._example = example

        self._timeout = timeout_sec

        self._pid = None

        self._node = None

    def _worker(self, idx, node):
        self._node = node

        start_time = time.time()

        # Some examples require the hostname of the node
        self._example.set_node_name(node.name)

        # Run the example until the condition is met or timeout occurs
        cmd = "echo $$ ; " + self._example.cmd()
        output_iter = node.account.ssh_capture(cmd)
        while not self._example.condition_met(
        ) and time.time() < start_time + self._timeout:
            line = next(output_iter)
            line = line.strip()
            self.logger.debug(line)

            # Take first line as pid
            if not self._pid:
                self._pid = line
            else:
                # Call to example.condition will automatically
                # store result in a boolean variable
                self._example.condition(line)

    @property
    def node(self):
        assert self._node is not None
        return self._node

    def stop_node(self, node):
        try:
            if self._pid:
                node.account.signal(self._pid, 9, allow_fail=True)
            node.account.kill_process(self._example.process_to_kill(),
                                      clean_shutdown=False)
        except RemoteCommandError as e:
            if b"No such process" in e.msg:
                pass
            else:
                raise

    def clean_node(self, nodes):
        pass
