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

import threading


class ExampleRunner(BackgroundThreadService):
    """
    The service that runs examples in the background
    """
    def __init__(self, context, example, timeout_sec=10):
        super(ExampleRunner, self).__init__(context, num_nodes=1)

        self._example = example

        self._timeout = timeout_sec

        self._pid = None

        self._stopping = threading.Event()

        self._error = None

    @property
    def logger(self):
        return self.context.logger

    def _worker(self, idx, node):
        self._stopping.clear()

        # Some examples require the hostname of the node
        self._example.set_node_name(node.name)

        # Run the example until the condition is met or timeout occurs
        cmd = "echo $$ ; " + self._example.cmd()
        output_iter = node.account.ssh_capture(cmd)

        start_time = time.time()

        while True:
            # Terminate loop on timeout or stop_node
            if time.time() > start_time + self._timeout:
                break
            if self._stopping.is_set():
                break

            try:
                line = next(output_iter)
                line = line.strip()
                self.logger.debug(line)
            except RemoteCommandError as e:
                self.logger.exception(f"Command {cmd} returned an error: {e}")
                self._error = e
                # No retries: thread is complete, test will fail when it calls
                # condition_met and sees the error.
                break

            # Take first line as pid
            if not self._pid:
                self._pid = line
            else:
                # Call to example.condition will automatically
                # store result in a boolean variable
                self._example.condition(line)

    def condition_met(self):
        if self._error:
            raise self._error
        return self._example.condition_met()

    # Returns the node name that the example is running on
    def node_name(self):
        return self._example.node_name()

    def stop_node(self, node):
        self._stopping.set()

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
