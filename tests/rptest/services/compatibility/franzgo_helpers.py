# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from .helpers_base import HelperBase, HelperFactoryBase
import time

# The franz-go root directory
TESTS_DIR = os.path.join("/opt", "franz-go")


class FranzGoBench(HelperBase):
    """
    The common items between helper classes
    for the franz-go bench example.
    """
    def __init__(self, redpanda, topic, extra_conf):
        super(FranzGoBench, self).__init__(redpanda)

        # The kafka topic
        self._topic = topic

        # Number of records produced
        self._recs = 0

        self._extra_conf = extra_conf

    # The internal condition to determine if the
    # example is successful. Returns boolean.
    def _condition(self, line):
        # Multiply by 1k because the number of recs
        # is formated as XXX.XXk records/s
        self._recs += float(line.split()[2][:-1]) * 1000
        return self._recs >= self._extra_conf.get("max_records")

    # Return the process name to kill
    def process_to_kill(self):
        return "bench"


class FranzGoBenchProduceHelper(FranzGoBench):
    """
    The helper class for franz-go's bench example
    using the producer endpoint
    """
    def __init__(self, redpanda, topic, extra_conf):
        super(FranzGoBenchProduceHelper,
              self).__init__(redpanda, topic, extra_conf)

    # Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/bench")
        cmd = f"bench -brokers {self._redpanda.brokers()} -topic {self._topic} -record-bytes 1000"

        auth = self._extra_conf.get("enable_sasl")
        if auth:
            creds = self._redpanda.SUPERUSER_CREDENTIALS
            cmd = cmd + f" -sasl-user {creds[0]} -sasl-pass {creds[1]} -sasl-method {creds[2]}"

        return os.path.join(EXAMPLE_DIR, cmd)


class FranzGoBenchConsumeHelper(FranzGoBench):
    """
    The helper class for franz-go's bench example
    using the consumer endpoint
    """
    def __init__(self, redpanda, topic, extra_conf):
        super(FranzGoBenchConsumeHelper,
              self).__init__(redpanda, topic, extra_conf)

    # Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/bench")
        cmd = f"bench -brokers {self._redpanda.brokers()} -topic {self._topic} -record-bytes 1000 -consume"

        group = self._extra_conf.get("group")
        if group:
            cmd = cmd + f" -group {group}"

        auth = self._extra_conf.get("enable_sasl")
        if auth:
            creds = self._redpanda.SUPERUSER_CREDENTIALS
            cmd = cmd + f" -sasl-user {creds[0]} -sasl-pass {creds[1]} -sasl-method {creds[2]}"

        return os.path.join(EXAMPLE_DIR, cmd)


class FranzGoHelperFactory(HelperFactoryBase):
    """
    The concrete factory for creating FranzGo's
    helper classes.
    """
    def __init__(self, func_name, redpanda, topic, extra_conf):
        super(FranzGoHelperFactory, self).__init__(func_name, redpanda, topic)

        self._extra_conf = extra_conf or dict()

    # The factory method for franz-go
    def create_franzgo_helpers(self):
        # Explictly checking None because "consume" is boolean
        # and False may satisfy this condition
        if not isinstance(self._extra_conf.get("consume"), bool):
            raise RuntimeError(
                "create_franzgo_helpers failed: consume must be bool.")

        if self._extra_conf.get("consume"):
            return FranzGoBenchConsumeHelper(self._redpanda, self._topic,
                                             self._extra_conf)
        else:
            return FranzGoBenchProduceHelper(self._redpanda, self._topic,
                                             self._extra_conf)
