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

        self._stopped = False

    def _worker(self, idx, node):
        self._node = node

        # Run the example until the condition is met or timeout occurs
        cmd = "echo $$ ; " + self._example.cmd(node.name)
        output_iter = node.account.ssh_capture(cmd)

        self._stopped = False

        # Keep reading input until the example's condition
        # is satisfied.
        while not self._stopped:
            line = next(output_iter)
            line = line.strip()
            self.logger.debug(line)

            # Take first line as pid
            if not self._pid:
                self._pid = line
            else:
                self._stopped = self._example.condition(line)

            time.sleep(0.1)

    @property
    def node(self):
        assert self._node is not None
        return self._node

    def stop_node(self, node):
        self._stopped = True
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
