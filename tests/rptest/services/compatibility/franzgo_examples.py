# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from .example_base import ExampleBase

# The franz-go root directory
TESTS_DIR = os.path.join("/opt", "franz-go")


class FranzGoBench(ExampleBase):
    """
    The common items between helper classes
    for the franz-go bench example.
    """
    def __init__(self, redpanda, topic, max_records, enable_sasl):
        super(FranzGoBench, self).__init__(redpanda)

        # The kafka topic
        self._topic = topic

        # Number of records produced
        self._recs = 0

        self._max_records = max_records

        self._enable_sasl = enable_sasl

    # The internal condition to determine if the
    # example is successful. Returns boolean.
    def _condition(self, line):
        # Multiply by 1k because the number of recs
        # is formated as XXX.XXk records/s
        self._recs += float(line.split()[2][:-1]) * 1000
        return self._recs >= self._max_records

    # Return the process name to kill
    def process_to_kill(self):
        return "bench"


class FranzGoBenchProduce(FranzGoBench):
    """
    The helper class for franz-go's bench example
    using the producer endpoint
    """
    def __init__(self, redpanda, topic, max_records, enable_sasl):
        super(FranzGoBenchProduce, self).__init__(redpanda, topic, max_records,
                                                  enable_sasl)

    # Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/bench")
        cmd = f"bench -brokers {self._redpanda.brokers()} -topic {self._topic} -record-bytes 1000"

        if self._enable_sasl:
            creds = self._redpanda.SUPERUSER_CREDENTIALS
            cmd = cmd + f" -sasl-user {creds[0]} -sasl-pass {creds[1]} -sasl-method {creds[2]}"

        return os.path.join(EXAMPLE_DIR, cmd)


class FranzGoBenchConsume(FranzGoBench):
    """
    The helper class for franz-go's bench example
    using the consumer endpoint
    """
    def __init__(self, redpanda, topic, max_records, enable_sasl, group=None):
        super(FranzGoBenchConsume, self).__init__(redpanda, topic, max_records,
                                                  enable_sasl)

        self._group = group

    # Return the command to call in the shell
    def cmd(self):
        EXAMPLE_DIR = os.path.join(TESTS_DIR, "examples/bench")
        cmd = f"bench -brokers {self._redpanda.brokers()} -topic {self._topic} -record-bytes 1000 -consume"

        if self._group:
            cmd = cmd + f" -group {self._group}"

        if self._enable_sasl:
            creds = self._redpanda.SUPERUSER_CREDENTIALS
            cmd = cmd + f" -sasl-user {creds[0]} -sasl-pass {creds[1]} -sasl-method {creds[2]}"

        return os.path.join(EXAMPLE_DIR, cmd)
