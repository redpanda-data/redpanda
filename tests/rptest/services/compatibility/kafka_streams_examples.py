# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re

# The kafka-streams root directory which is made in the
# Dockerfile
TESTS_DIR = os.path.join("/opt", "kafka-streams-examples")

# Below is shared among most of the kafka-streams-examples.
# It is used to execute the programs.
BASE_CMD = f"java -cp {TESTS_DIR}/target/kafka-streams-examples-6.2.0-standalone.jar io.confluent.examples.streams."


class WikipediaExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"WikipediaFeedAvroLambdaExample {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class WikipediaDriver:
    def __init__(self, redpanda):
        self._redpanda = redpanda
        self._users_re = re.compile(
            "(erica|bob|joe|damian|tania|phil|sam|lauren|joseph)")

    def condition(self, line):
        return self._users_re.search(line) is not None

    def cmd(self, host):
        return BASE_CMD + f"WikipediaFeedAvroExampleDriver {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class TopArticlesExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"TopArticlesLambdaExample {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class TopArticlesDriver:
    def __init__(self, redpanda):
        self._redpanda = redpanda
        self._articles_re = re.compile(
            "(engineering|telco|finance|health|science)")

    def condition(self, line):
        return self._articles_re.search(line) is not None

    def cmd(self, host):
        return BASE_CMD + f"TopArticlesExampleDriver {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class SessionWindowExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"SessionWindowsExample {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class SessionWindowDriver:
    def __init__(self, redpanda):
        self._redpanda = redpanda
        self._users_re = re.compile("(sarah|bill|jo)")

    def condition(self, line):
        return self._users_re.search(line) is not None

    def cmd(self, host):
        return BASE_CMD + f"SessionWindowsExampleDriver {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class JsonToAvroExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"JsonToAvroExample {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class JsonToAvroDriver:
    def __init__(self, redpanda):
        self._redpanda = redpanda
        # The driver prints out 3 avro records
        self._count = 3

    def condition(self, line):
        # sub 1 when Converted Avro Record is in the line, otherwise sub 0
        self._count -= "Converted Avro Record" in line
        return self._count <= 0

    def cmd(self, host):
        return BASE_CMD + f"JsonToAvroExampleDriver {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class PageViewExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"PageViewRegionLambdaExample {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class PageViewDriver:
    def __init__(self, redpanda):
        self._redpanda = redpanda
        self._regions_re = re.compile("(europe@|usa@|asia@|africa@)")

    def condition(self, line):
        return self._regions_re.search(line) is not None

    def cmd(self, host):
        return BASE_CMD + f"PageViewRegionExampleDriver {self._redpanda.brokers()} {self._redpanda.schema_reg()}"

    def process_to_kill(self):
        return "java"


class SumLambdaExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"SumLambdaExample {self._redpanda.brokers()}"

    def process_to_kill(self):
        return "java"


class SumLambdaDriver:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Current sum of odd numbers is:" in line

    def cmd(self, host):
        return BASE_CMD + f"SumLambdaExampleDriver {self._redpanda.brokers()}"

    def process_to_kill(self):
        return "java"


class AnomalyDetectionExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"AnomalyDetectionLambdaExample {self._redpanda.brokers()}"

    def process_to_kill(self):
        return "java"


class UserRegionExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"UserRegionLambdaExample {self._redpanda.brokers()}"

    def process_to_kill(self):
        return "java"


class WordCountExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"WordCountLambdaExample {self._redpanda.brokers()}"

    def process_to_kill(self):
        return "java"


class MapFuncExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"MapFunctionLambdaExample {self._redpanda.brokers()}"

    def process_to_kill(self):
        return "java"
