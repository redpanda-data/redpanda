# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time
import threading

from rptest.services.failure_injector import FailureInjector, FailureSpec
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


class EndToEndFinjectorTest(EndToEndTest):
    def __init__(self, test_context):
        super(EndToEndFinjectorTest, self).__init__(test_context=test_context)
        self.enable_failures = True
        self.scale = Scale(test_context)
        self.finjector_thread = None
        self.failure_length_provider = scale_dependent_length(self.scale)
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

    def start_finjector(self):
        self.finjector_thread = threading.Thread(
            target=self._failure_injector_loop, args=())
        self.finjector_thread.daemon = True
        self.finjector_thread.start()

    def random_failure_spec(self):
        f_type = random.choice(self.allowed_failures)
        length = self.failure_length_provider(f_type)
        node = random.choice(self.allowed_nodes_provider(f_type))

        return FailureSpec(node=node, type=f_type, length=length)

    def inject_failure(self, spec):
        f_injector = FailureInjector(self.redpanda)
        f_injector.inject_failure(spec)

    def _next_failure(self):
        if len(self.custom_failures) > 0:
            return self.custom_failures.pop(0)
        else:
            return self.random_failure_spec()

    def _failure_injector_loop(self):

        while self.enable_failures:
            f_injector = FailureInjector(self.redpanda)
            f_injector.inject_failure(self._next_failure())

            delay = self.failure_delay_provier()
            self.redpanda.logger.info(
                f"waiting {delay} seconds before next failure")
            time.sleep(delay)

    def teardown(self):
        self.enable_failures = False
        if self.finjector_thread:
            self.finjector_thread.join()
        FailureInjector(self.redpanda)._heal_all()
