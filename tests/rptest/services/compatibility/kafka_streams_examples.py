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
from .example_base import ExampleBase

# The kafka-streams root directory which is made in the
# Dockerfile
TESTS_DIR = os.path.join("/opt", "kafka-streams-examples")


class KafkaStreams(ExampleBase):
    def __init__(self, redpanda, is_driver, jar_args):
        super(KafkaStreams, self).__init__(redpanda)

        self._jar_arg = jar_args[0] if is_driver else jar_args[1]
        self._is_driver = is_driver

    # The internal condition to determine if the
    # example is successful. Returns boolean.
    def _condition(self, line):
        if self._is_driver:
            return self.driver_cond(line)
        else:
            return "Example started." in line

    # Return the command to call in the shell
    def cmd(self):
        cmd = f"java -cp {TESTS_DIR}/target/kafka-streams-examples-6.2.0-standalone.jar io.confluent.examples.streams."
        cmd = cmd + self._jar_arg
        return cmd

    # Return the process name to kill
    def process_to_kill(self):
        return "java"

    def driver_cond(self, line):
        raise NotImplementedError("driver condition undefined")


class KafkaStreamsWikipedia(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        schema_reg = redpanda.schema_reg()
        jar_args = (
            f"WikipediaFeedAvroExampleDriver {redpanda.brokers()} {schema_reg}",
            f"WikipediaFeedAvroLambdaExample {redpanda.brokers()} {schema_reg}"
        )

        super(KafkaStreamsWikipedia, self).__init__(redpanda, is_driver,
                                                    jar_args)

        self.users_re = re.compile(
            "(erica|bob|joe|damian|tania|phil|sam|lauren|joseph)")

    def driver_cond(self, line):
        return self.users_re.search(line) is not None


class KafkaStreamsTopArticles(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        schema_reg = redpanda.schema_reg()
        jar_args = (
            f"TopArticlesExampleDriver {redpanda.brokers()} {schema_reg}",
            f"TopArticlesLambdaExample {redpanda.brokers()} {schema_reg}")

        super(KafkaStreamsTopArticles, self).__init__(redpanda, is_driver,
                                                      jar_args)

    def driver_cond(self, line):
        return "engineering" in line or "telco" in line or "finance" in line or "health" in line or "science" in line


class KafkaStreamsSessionWindow(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        schema_reg = redpanda.schema_reg()
        jar_args = (
            f"SessionWindowsExampleDriver {redpanda.brokers()} {schema_reg}",
            f"SessionWindowsExample {redpanda.brokers()} {schema_reg}")

        super(KafkaStreamsSessionWindow,
              self).__init__(redpanda, is_driver, jar_args)

    def driver_cond(self, line):
        return "sarah" in line or "bill" in line or "jo" in line


class KafkaStreamsJsonToAvro(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        schema_reg = redpanda.schema_reg()
        jar_args = (
            f"JsonToAvroExampleDriver {redpanda.brokers()} {schema_reg}",
            f"JsonToAvroExample {redpanda.brokers()} {schema_reg}")

        super(KafkaStreamsJsonToAvro, self).__init__(redpanda, is_driver,
                                                     jar_args)

        # The driver prints out 3 avro records
        self._count = 3

    def driver_cond(self, line):
        # sub 1 when Converted Avro Record is in the line, otherwise sub 0
        self._count -= "Converted Avro Record" in line
        return self._count <= 0


class KafkaStreamsPageView(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        schema_reg = redpanda.schema_reg()
        jar_args = (
            f"PageViewRegionExampleDriver {redpanda.brokers()} {schema_reg}",
            f"PageViewRegionLambdaExample {redpanda.brokers()} {schema_reg}")

        super(KafkaStreamsPageView, self).__init__(redpanda, is_driver,
                                                   jar_args)

    def driver_cond(self, line):
        return "europe@" in line or "usa@" in line or "asia@" in line or "africa@" in line


class KafkaStreamsSumLambda(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        jar_args = (f"SumLambdaExampleDriver {redpanda.brokers()}",
                    f"SumLambdaExample {redpanda.brokers()}")

        super(KafkaStreamsSumLambda, self).__init__(redpanda, is_driver,
                                                    jar_args)

    def driver_cond(self, line):
        return "Current sum of odd numbers is:" in line


class KafkaStreamsAnomalyDetection(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None,
                    f"AnomalyDetectionLambdaExample {redpanda.brokers()}")

        super(KafkaStreamsAnomalyDetection,
              self).__init__(redpanda, is_driver, jar_args)


class KafkaStreamsUserRegion(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None, f"UserRegionLambdaExample {redpanda.brokers()}")

        super(KafkaStreamsUserRegion, self).__init__(redpanda, is_driver,
                                                     jar_args)


class KafkaStreamsWordCount(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None, f"WordCountLambdaExample {redpanda.brokers()}")

        super(KafkaStreamsWordCount, self).__init__(redpanda, is_driver,
                                                    jar_args)


class KafkaStreamsMapFunc(KafkaStreams):
    def __init__(self, redpanda, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None, f"MapFunctionLambdaExample {redpanda.brokers()}")

        super(KafkaStreamsMapFunc, self).__init__(redpanda, is_driver,
                                                  jar_args)
