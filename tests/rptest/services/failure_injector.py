# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import datetime
import signal
import threading
from ducktape.utils.util import wait_until


class FailureSpec:

    FAILURE_KILL = 0
    FAILURE_TERMINATE = 1
    FAILURE_SUSPEND = 2

    FAILURE_TYPES = [FAILURE_KILL, FAILURE_SUSPEND, FAILURE_TERMINATE]

    def __init__(self, type, node, length=None):
        self.type = type
        self.length = length
        self.node = node

    def __str__(self):
        return f"type: {self.type}, length: {self.length} seconds, node: {self.node}"


class FailureInjector:
    def __init__(self, redpanda):
        self.redpanda = redpanda

    def inject_failure(self, spec):
        self.redpanda.logger.info(f"injecting failure: {spec}")
        self._start_func(spec.type)(spec.node)
        if spec.length is not None:
            if spec.length == 0:
                self._stop_func(spec.type)(spec.node)
            else:
                stop_timer = threading.Timer(function=self._stop_func(
                    spec.type),
                                             args=[spec.node],
                                             interval=spec.length)
                stop_timer.start()

    def _start_func(self, tp):
        if tp == FailureSpec.FAILURE_KILL:
            return self._kill
        elif tp == FailureSpec.FAILURE_SUSPEND:
            return self._suspend
        elif tp == FailureSpec.FAILURE_TERMINATE:
            return self._terminate

    def _stop_func(self, tp):
        if tp == FailureSpec.FAILURE_KILL or tp == FailureSpec.FAILURE_TERMINATE:
            return self._start
        elif tp == FailureSpec.FAILURE_SUSPEND:
            return self._continue

    def _kill(self, node):
        self.redpanda.logger.info(
            f"killing redpanda on { self.redpanda.idx(node)}")
        self.redpanda.signal_redpanda(node, signal=signal.SIGKILL)
        timeout_sec = 10
        wait_until(lambda: self.redpanda.redpanda_pid(node) == None,
                   timeout_sec=timeout_sec,
                   err_msg="Redpanda failed to kill in %d seconds" %
                   timeout_sec)

    def _suspend(self, node):
        self.redpanda.logger.info(
            f"suspending redpanda on { self.redpanda.idx(node)}")
        self.redpanda.signal_redpanda(node, signal=signal.SIGSTOP)

    def _terminate(self, node):
        self.redpanda.logger.info(
            f"terminating redpanda on { self.redpanda.idx(node)}")
        self.redpanda.signal_redpanda(node, signal=signal.SIGTERM)
        timeout_sec = 30
        wait_until(lambda: self.redpanda.redpanda_pid(node) == None,
                   timeout_sec=timeout_sec,
                   err_msg="Redpanda failed to terminate in %d seconds" %
                   timeout_sec)

    def _continue(self, node):
        self.redpanda.logger.info(
            f"continuing execution on { self.redpanda.idx(node)}")
        self.redpanda.signal_redpanda(node, signal=signal.SIGCONT)

    def _start(self, node):
        # make this idempotent
        if self.redpanda.redpanda_pid(node) == None:
            self.redpanda.logger.info(
                f"starting redpanda on {self.redpanda.idx(node)}")
            self.redpanda.start_redpanda(node)
