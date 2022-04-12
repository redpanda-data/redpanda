# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import signal
import threading
from ducktape.utils.util import wait_until


class FailureSpec:

    FAILURE_KILL = 0
    FAILURE_TERMINATE = 1
    FAILURE_SUSPEND = 2
    FAILURE_ISOLATE = 3

    FAILURE_TYPES = [FAILURE_KILL, FAILURE_SUSPEND, FAILURE_TERMINATE]

    def __init__(self, type, node, length=None):
        self.type = type
        self.length = length
        self.node = node

    def __str__(self):
        return f"type: {self.type}, length: {self.length} seconds, node: {self.node.account.hostname}"


class FailureInjector:
    def __init__(self, redpanda):
        self.redpanda = redpanda

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._heal_all()

    def inject_failure(self, spec):
        self.redpanda.logger.info(f"injecting failure: {spec}")
        try:
            self._start_func(spec.type)(spec.node)
        except Exception as e:
            self.redpanda.logger.info(f"injecting failure error: {e}")
        finally:
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
        elif tp == FailureSpec.FAILURE_ISOLATE:
            return self._isolate

    def _stop_func(self, tp):
        if tp == FailureSpec.FAILURE_KILL or tp == FailureSpec.FAILURE_TERMINATE:
            return self._start
        elif tp == FailureSpec.FAILURE_SUSPEND:
            return self._continue
        elif tp == FailureSpec.FAILURE_ISOLATE:
            return self._heal

    def _kill(self, node):
        self.redpanda.logger.info(
            f"killing redpanda on {node.account.hostname}")
        self.redpanda.signal_redpanda(node,
                                      signal=signal.SIGKILL,
                                      idempotent=True)
        timeout_sec = 10
        wait_until(lambda: self.redpanda.redpanda_pid(node) == None,
                   timeout_sec=timeout_sec,
                   err_msg="Redpanda failed to kill in %d seconds" %
                   timeout_sec)

    def _isolate(self, node):
        self.redpanda.logger.info(f"isolating node {node.account.hostname}")

        cmd = "iptables -A OUTPUT -p tcp --destination-port 33145 -j DROP"
        node.account.ssh(cmd)
        cmd = "iptables -A INPUT -p tcp --destination-port 33145 -j DROP"
        node.account.ssh(cmd)

    def _heal(self, node):
        self.redpanda.logger.info(f"healing node {node.account.hostname}")
        cmd = "iptables -D OUTPUT -p tcp --destination-port 33145 -j DROP"
        node.account.ssh(cmd)
        cmd = "iptables -D INPUT -p tcp --destination-port 33145 -j DROP"
        node.account.ssh(cmd)

    def _heal_all(self):
        self.redpanda.logger.info(f"healling all network failures")
        for n in self.redpanda.nodes:
            n.account.ssh("iptables -P INPUT ACCEPT")
            n.account.ssh("iptables -P FORWARD ACCEPT")
            n.account.ssh("iptables -P OUTPUT ACCEPT")
            n.account.ssh("iptables -F")
            n.account.ssh("iptables -X")

    def _suspend(self, node):
        self.redpanda.logger.info(
            f"suspending redpanda on {node.account.hostname}")
        self.redpanda.signal_redpanda(node, signal=signal.SIGSTOP)

    def _terminate(self, node):
        self.redpanda.logger.info(
            f"terminating redpanda on {node.account.hostname}")
        self.redpanda.signal_redpanda(node, signal=signal.SIGTERM)
        timeout_sec = 30
        wait_until(lambda: self.redpanda.redpanda_pid(node) == None,
                   timeout_sec=timeout_sec,
                   err_msg="Redpanda failed to terminate in %d seconds" %
                   timeout_sec)

    def _continue(self, node):
        self.redpanda.logger.info(
            f"continuing execution on {node.account.hostname}")
        self.redpanda.signal_redpanda(node, signal=signal.SIGCONT)

    def _start(self, node):
        # make this idempotent
        if self.redpanda.redpanda_pid(node) == None:
            self.redpanda.logger.info(
                f"starting redpanda on {node.account.hostname}")
            self.redpanda.start_redpanda(node)
