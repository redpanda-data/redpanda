# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import signal
import threading
from ducktape.utils.util import wait_until
from ducktape.errors import TimeoutError

from rptest.services import tc_netem


class FailureSpec:

    FAILURE_KILL = 0
    FAILURE_TERMINATE = 1
    FAILURE_SUSPEND = 2
    FAILURE_ISOLATE = 3
    # network failures
    FAILURE_NETEM_RANDOM_DELAY = 4
    FAILURE_NETEM_PACKET_LOSS = 5
    FAILURE_NETEM_PACKET_CORRUPT = 6
    FAILURE_NETEM_PACKET_DUPLICATE = 7

    FAILURE_TYPES = [
        FAILURE_KILL, FAILURE_SUSPEND, FAILURE_TERMINATE,
        FAILURE_NETEM_RANDOM_DELAY
    ]
    NETEM_FAILURE_TYPES = [
        FAILURE_NETEM_RANDOM_DELAY, FAILURE_NETEM_PACKET_LOSS,
        FAILURE_NETEM_PACKET_CORRUPT, FAILURE_NETEM_PACKET_DUPLICATE
    ]

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
            if spec.type == FailureSpec.FAILURE_TERMINATE and isinstance(
                    e, TimeoutError):
                # A timeout during termination indicates a shutdown hang in redpanda: this
                # is a bug and we should fail the test on it.  Otherwise we'd leave the node
                # in a weird state & get some non-obvious failure later in the test, such
                # as https://github.com/redpanda-data/redpanda/issues/5178
                raise
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
        elif tp == FailureSpec.FAILURE_NETEM_RANDOM_DELAY:
            return self._netem_delay
        elif tp == FailureSpec.FAILURE_NETEM_PACKET_LOSS:
            return self._netem_loss
        elif tp == FailureSpec.FAILURE_NETEM_PACKET_CORRUPT:
            return self._netem_corrupt
        elif tp == FailureSpec.FAILURE_NETEM_PACKET_DUPLICATE:
            return self._netem_duplicate

    def _stop_func(self, tp):
        if tp == FailureSpec.FAILURE_KILL or tp == FailureSpec.FAILURE_TERMINATE:
            return self._start
        elif tp == FailureSpec.FAILURE_SUSPEND:
            return self._continue
        elif tp == FailureSpec.FAILURE_ISOLATE:
            return self._heal
        else:
            return self._delete_netem

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

    def _delete_netem(self, node):
        tc_netem.tc_netem_delete(node)

    def _heal_all(self):
        self.redpanda.logger.info(f"healling all network failures")
        for n in self.redpanda.nodes:
            n.account.ssh("iptables -P INPUT ACCEPT")
            n.account.ssh("iptables -P FORWARD ACCEPT")
            n.account.ssh("iptables -P OUTPUT ACCEPT")
            n.account.ssh("iptables -F")
            n.account.ssh("iptables -X")
            try:
                self._delete_netem(n)
            except:
                # skip error as deleting netem may fail if there are no rules applied
                pass

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

    def _netem(self, node, op):
        self.redpanda.logger.info(
            f"executing: '{op.get_command()}' on '{node.account.hostname}'")
        tc_netem.tc_netem_add(node=node, option=op)

    def _netem_delay(self, node):
        op = tc_netem.NetemDelay(
            random.randint(1000, 5000),
            random.randint(100, 1000),
            correlation_us=None,
            distribution=tc_netem.NetemDelayDistribution.PARETO_NORMAL)

        self._netem(node, op=op)

    def _netem_loss(self, node):
        op = tc_netem.NetemLoss(random.randint(1, 60))
        self._netem(node, op)

    def _netem_corrupt(self, node):
        op = tc_netem.NetemCorrupt(random.randint(1, 60), correlation=None)
        self._netem(node, op=op)

    def _netem_duplicate(self, node):
        op = tc_netem.NetemDuplicate(random.randint(1, 60), correlation=None)
        self._netem(node, op=op)
