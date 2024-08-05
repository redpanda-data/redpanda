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
    def __init__(self, redpanda, scale):
        self.redpanda = redpanda
        self.enable_manual = False
        self.enable_loop = False
        self.finjector_thread = None
        self.failure_length_provider = scale_dependent_length(scale)
        self.failure_delay_provier = const_delay(10)
        self.allowed_nodes_provider = lambda f_type: self.redpanda.nodes
        self.allowed_failures = FailureSpec.FAILURE_TYPES
        self.custom_failures = []

    def add_failure_spec(self, fspec):
        self.custom_failures.append(fspec)

    def configure_finjector(self,
                            allowed_failures=None,
                            length_provider=None,
                            delay_provider=None):
        if allowed_failures:
            allowed_failures = allowed_failures
        if length_provider:
            self.failure_length_provider = length_provider
        if delay_provider:
            self.failure_delay_provier = delay_provider

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
            self.finjector_thread = threading.Thread(
                target=self._failure_injector_loop, args=())
            self.finjector_thread.start()
            yield
        finally:
            self.enable_loop = False
            if self.finjector_thread:
                self.finjector_thread.join()
            self._cleanup()

    @contextmanager
    def finj_manual(self):
        """
        Get a context manager that holds the test in manual failure injection
        mode. Recoverable failures such as suspended process or network issues
        will be repaired on exit. Caller is supposed to make inject_failure()
        calls inside the `with` statement.

        :return: a callable with a single failure spec argument
        """
        try:
            assert not self.enable_manual and not self.enable_loop
            self.enable_manual = True

            def callable(spec):
                return self.inject_failure(spec)

            yield callable
        finally:
            self.enable_manual = False
            self._cleanup()

    def random_failure_spec(self):
        f_type = random.choice(self.allowed_failures)
        length = self.failure_length_provider(f_type)
        node = random.choice(self.allowed_nodes_provider(f_type))

        return FailureSpec(node=node, type=f_type, length=length)

    def inject_failure(self, spec):
        assert self.enable_manual or self.enable_loop
        f_injector = make_failure_injector(self.redpanda)
        f_injector.inject_failure(spec)

    def _next_failure(self):
        if len(self.custom_failures) > 0:
            return self.custom_failures.pop(0)
        else:
            return self.random_failure_spec()

    def _failure_injector_loop(self):
        while self.enable_loop:
            f_injector = make_failure_injector(self.redpanda)
            f_injector.inject_failure(self._next_failure())

            delay = self.failure_delay_provier()
            self.redpanda.logger.info(
                f"waiting {delay} seconds before next failure")
            time.sleep(delay)

    def _cleanup(self):
        make_failure_injector(self.redpanda)._heal_all()
        make_failure_injector(self.redpanda)._continue_all()


class EndToEndFinjectorTest(EndToEndTest):
    def __init__(self, test_context):
        super(EndToEndFinjectorTest, self).__init__(test_context=test_context)
        self.finjector = Finjector(self.redpanda, test_context)
