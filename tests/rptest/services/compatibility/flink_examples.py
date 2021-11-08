# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from .example_base import ExampleBase

# The flink-examples root directory which is made in the
# Dockerfile
TESTS_DIR = os.path.join("/opt", "flink-kafka-examples")

# The flink dirs
FLINK_DIR = os.path.join("/opt", "flink-1.14.0")
FLINK_BIN = os.path.join(FLINK_DIR, "bin")


class FlinkWithKafka(ExampleBase):
    def __init__(self, redpanda):
        super(FlinkWithKafka, self).__init__(redpanda)

    # The internal condition to determine if the
    # flink job was submitted successfully.
    def _condition(self, line):
        return "Job has been submitted with JobID" in line

    # Return the command to call in the shell
    def cmd(self):
        # This will submit the flink app to the flink cluster
        cmd = f"{FLINK_BIN}/flink run --detached {TESTS_DIR}/target/flink-kafka-examples-1.0.jar {self._redpanda.brokers()}"
        return cmd


class FlinkWordCountJob(FlinkWithKafka):
    def __init__(self, redpanda):
        super(FlinkWordCountJob, self).__init__(redpanda)

    @property
    def name(self):
        return "FlinkWordCountJob"
