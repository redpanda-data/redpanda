# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from contextlib import contextmanager
import random
import sys
import time
import threading

from rptest.services.failure_injector import make_failure_injector, FailureSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale


def scale_dependent_length(scale):
    def get_length(f_type):
        if scale.local:
            return random.randint(1, 5)
        else:
            return random.randint(1, 20)

    return get_length


def const_delay(delay_seconds=10):
    return lambda: delay_seconds


class Finjector:
    # we cannot guarantee start idempotency
    LOG_ALLOW_LIST = ["failed to lock pidfile. already locked"]

    def __init__(self, redpanda, scale, **kwargs):
        self.redpanda = redpanda
        self.enable_manual = False
        self.enable_loop = False
        self.finjector_thread = None
        self.failure_length_provider = scale_dependent_length(scale)
        self.failure_delay_provier = const_delay(10)
        self.allowed_nodes_provider = lambda f_type: self.redpanda.nodes
        self.allowed_failures = FailureSpec.FAILURE_TYPES
        self.custom_failures = []
        self.max_concurrent_failures = sys.maxsize
        self.configure_finjector(**kwargs)

    def add_failure_spec(self, fspec):
        self.custom_failures.append(fspec)

    def configure_finjector(self,
                            allowed_failures=None,
                            length_provider=None,
                            delay_provider=None,
                            max_concurrent_failures=None):
        if allowed_failures:
            allowed_failures = allowed_failures
        if length_provider:
            self.failure_length_provider = length_provider
        if delay_provider:
            self.failure_delay_provier = delay_provider
        if max_concurrent_failures is not None:
            self.max_concurrent_failures = max_concurrent_failures

    @contextmanager
    def finj_thread(self):
        """
        Get a context manager that holds the test in manual failure injection
        mode. Recoverable failures such as suspended process or network issues
        will be repaired on exit.

        :return: void
        """
        try:
            assert not self.enable_manual and not self.enable_loop
            self.enable_loop = True
            f_injector = make_failure_injector(self.redpanda)
            self.finjector_thread = threading.Thread(
                target=self._failure_injector_loop, args=(f_injector))
            self.finjector_thread.start()
            yield
        finally:
            self.enable_loop = False
            if self.finjector_thread:
                self.finjector_thread.join()
            self._cleanup(f_injector)

    @contextmanager
    def finj_manual(self):
        """
        Get a context manager that holds the test in manual failure injection
        mode. Recoverable failures such as suspended process or network issues
        will be repaired on exit. Caller is supposed to make inject_failure()
        calls inside the `with` statement.

        :return: a callable with a single failure spec argument
        """
        f_injector = make_failure_injector(self.redpanda)
        try:
            assert not self.enable_manual and not self.enable_loop
            self.enable_manual = True

            def callable(spec):
                return self.inject_failure(f_injector, spec)

            yield callable
        finally:
            self.enable_manual = False
            self._cleanup(f_injector)

    def random_failure_spec(self):
        f_type = random.choice(self.allowed_failures)
        length = self.failure_length_provider(f_type)
        node = random.choice(self.allowed_nodes_provider(f_type))

        return FailureSpec(node=node, type=f_type, length=length)

    def inject_failure(self, f_injector, spec):
        assert self.enable_manual or self.enable_loop
        f_injector.inject_failure(spec)

    def _next_failure(self):
        if len(self.custom_failures) > 0:
            return self.custom_failures.pop(0)
        else:
            return self.random_failure_spec()

    def _failure_injector_loop(self, f_injector):
        while self.enable_loop:
            failure = self._next_failure()
            f_injector.inject_failure(failure)

            delay = self.failure_delay_provier()
            if f_injector.cnt_in_flight() >= self.max_concurrent_failures:
                delay = max(delay, f_injector.time_till_next_recovery)
            self.redpanda.logger.info(
                f"waiting {delay} seconds before next failure")
            time.sleep(delay)

    def _cleanup(self, f_injector):
        f_injector._heal_all()
        f_injector._continue_all()
        f_injector._undo_all()
