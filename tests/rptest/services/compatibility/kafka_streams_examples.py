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


class InteractiveQueriesExample:
    def __init__(self, redpanda, port):
        self._redpanda = redpanda
        self._port = port

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"interactivequeries.WordCountInteractiveQueriesExample {self._port} {self._redpanda.brokers()} {host}"

    def process_to_kill(self):
        return "java"


class InteractiveQueriesDriver:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Driver completed." in line

    def cmd(self, host):
        return BASE_CMD + f"interactivequeries.WordCountInteractiveQueriesDriver {self._redpanda.brokers()}"

    def process_to_kill(self):
        return "java"


# The interactive queries example effectively computes
# a histogram of any words on the fly. The below dict
# is the expected counts for the data generated by
# interactive queries driver
QUERIES_HIST = {}
QUERIES_HIST["hello"] = 1
QUERIES_HIST["world"] = 2
QUERIES_HIST["all"] = 1
QUERIES_HIST["streams"] = 3
QUERIES_HIST["lead"] = 1
QUERIES_HIST["to"] = 1
QUERIES_HIST["kafka"] = 2
QUERIES_HIST["the"] = 4
QUERIES_HIST["cat"] = 1
QUERIES_HIST["in"] = 1
QUERIES_HIST["hat"] = 1
QUERIES_HIST["green"] = 1
QUERIES_HIST["eggs"] = 1
QUERIES_HIST["and"] = 1
QUERIES_HIST["ham"] = 1
QUERIES_HIST["that"] = 1
QUERIES_HIST["sam"] = 1
QUERIES_HIST["i"] = 1
QUERIES_HIST["am"] = 1
QUERIES_HIST["up"] = 1
QUERIES_HIST["creek"] = 1
QUERIES_HIST["without"] = 1
QUERIES_HIST["a"] = 2
QUERIES_HIST["paddle"] = 1
QUERIES_HIST["run"] = 2
QUERIES_HIST["forest"] = 1
QUERIES_HIST["tank"] = 1
QUERIES_HIST["full"] = 1
QUERIES_HIST["of"] = 2
QUERIES_HIST["gas"] = 1
QUERIES_HIST["eat"] = 1
QUERIES_HIST["sleep"] = 1
QUERIES_HIST["rave"] = 1
QUERIES_HIST["repeat"] = 1
QUERIES_HIST["one"] = 1
QUERIES_HIST["jolly"] = 1
QUERIES_HIST["sailor"] = 1
QUERIES_HIST["king"] = 1


class AppResetExample:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def condition(self, line):
        return "Example started." in line

    def cmd(self, host):
        return BASE_CMD + f"ApplicationResetExample {self._redpanda.brokers()} --reset"

    def process_to_kill(self):
        return "java"


def reset_app_cmd(redpanda, app_id, input_topic, intermediate_topic):
    CONFLUENT_BIN = "/opt/confluent-6.2.1/bin"
    cmd = f"{CONFLUENT_BIN}/kafka-streams-application-reset"
    cmd += f" --bootstrap-servers {redpanda.brokers()}"
    cmd += f" --application-id {app_id}"
    cmd += f" --input-topics {input_topic}"
    cmd += f" --intermediate-topics {intermediate_topic}"
    cmd += " --force"

    return cmd