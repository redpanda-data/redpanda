# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import logging
import os
import random
import string
import sys

from dataclasses import dataclass

from pyflink.table import StreamTableEnvironment, EnvironmentSettings, \
    TableDescriptor, Schema, DataTypes

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment


@dataclass(kw_only=True)
class WorkloadConfig:
    # Default values are set for CDT run inside EC2 instance
    connector_path: str = "file:///opt/flink/connectors/flink-sql-connector-kafka-3.0.1-1.18.jar"
    logger_path: str = "/workloads"
    log_level: str = "DEBUG"
    producer_group: str = "flink_group"
    consumer_group: str = "flink_group"
    topic_name: str = "flink_transactions_topic"
    # Not used
    count: int = 65535
    batch_size: int = 256
    # This should be updated to value from self.redpanda.brokers()
    brokers: str = "localhost:9092"


def setup_logger(logfilepath, level):
    # Simple file logger
    handler = logging.FileHandler(logfilepath)
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    level = logging.getLevelName(level.upper())
    handler.setLevel(level)
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)

    return logger


class FlinkWorkloadProduce:
    def id_to_word(self, word_id):
        return self.words[word_id]

    def __init__(self, config_override):
        # Serialize config
        self.config = WorkloadConfig(**config_override)
        # Create logger
        filename = f"{os.path.basename(__file__).split('.')[0]}.log"
        logfile = os.path.join(self.config.logger_path, filename)
        self.logger = setup_logger(logfile, self.config.log_level)

    def setup(self):
        self.logger.info("Initializing Produce workload")
        # Initialize
        config = Configuration()
        # This is required for ducktape EC2 run
        config.set_string("python.client.executable", "python3")
        config.set_string("python.executable", "python3")

        # Create flink env
        settings = EnvironmentSettings.in_streaming_mode()
        settings.from_configuration(config)
        # Streaming env is user in order to add kafka connector
        env = StreamExecutionEnvironment.get_execution_environment()
        env.add_jars(self.config.connector_path)
        settings = EnvironmentSettings.new_instance() \
            .in_streaming_mode()\
            .build()
        # Create Table API in top of Streaming env
        table_env = StreamTableEnvironment.create(
            stream_execution_environment=env, environment_settings=settings)

        # Alternative way to add connector, just for illustrative purposes
        table_env.get_config().set("pipeline.jars", self.config.connector_path)

        # Prepare data to be sent
        self.logger.info(f"Generating {self.config.count} words")
        self.words = self._generate_words()

        # define the sink
        sink_table_schema = Schema.new_builder() \
            .column('word', DataTypes.STRING()) \
            .build()

        sink_table_desc = TableDescriptor.for_connector('kafka') \
            .schema(sink_table_schema) \
            .option('connector', 'kafka') \
            .option('topic', f'{self.config.topic_name}') \
            .option('properties.bootstrap.servers', f'{self.config.brokers}') \
            .option('properties.group.id', f'{self.config.producer_group}') \
            .option('scan.startup.mode', 'earliest-offset') \
            .option('format', 'csv') \
            .build()

        self.kafka_table = table_env.create_temporary_table(
            'sink', sink_table_desc)

        # Publish it as class var
        self.table_env = table_env

    def _generate_words(self):
        """
            Generates crypto-like bogus array of words
        """
        # Generate words using random word count (10-64)
        # and random word len (4-16)
        return [
            ''.join(
                random.choices(string.ascii_letters, k=random.randint(4, 16)))
            for idx in range(random.randint(10, 64))
        ]

    def run(self):
        """
            Example of a produce task

            Steps:
            - Select from temp table and insert into kafka sink
        """
        idx = 0
        while idx < self.config.count:
            values = [
                f"('{self.id_to_word(random.randint(0, len(self.words)-1))}')"
                for count in range(self.config.batch_size)
            ]
            # Note:
            # One insert operator is a good example of a transaction.
            # Which is two-phase: 'pre-commit/checkpoint' -> 'commit'/'abort'
            # see: https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/

            # batch_size here is basically how much words included in INSERT
            # Using batch_size of 1 results in java.lang.OutOfMemoryError
            # after 403 inserts, i.e. ~400 transactions
            # Using batch_size of 4 -> OutOfMemoryError after 1631 inserts,
            # i.e. once again, ~400 transactions
            # Using batch_size of 256 -> OutOfMemoryError after 104447 inserts,
            # i.e. yet once again, ~400 transactions
            self.table_env.execute_sql("INSERT INTO sink " +
                                       f"VALUES {','.join(values)};")
            idx += self.config.batch_size

    def cleanup(self):
        """
            Cleanup placeholder
        """
        pass


if __name__ == '__main__':
    # Load config if specified
    if len(sys.argv) > 1:
        # Validate arguments in a quick and dirty way.
        # This will assign one argument to filename and generate exception if
        # there is more than one argument
        try:
            [filename] = sys.argv[1:]
        except Exception as e:
            raise RuntimeError("Wrong number of arguments."
                               "Should be one with path to "
                               "flink_workload_conf.json") from e
    else:
        # No config path provided, just use defaults
        filename = "/workloads/flink_workload_config.json"

    # Load configuration
    with open(filename, 'r+t') as f:
        input_config = json.load(f)

    # All messages past this point is intercepted by task manager
    # and will be seen in its log
    workload = FlinkWorkloadProduce(input_config)
    try:
        workload.setup()
        workload.run()
    except Exception as e:
        # Do not re-throw not to cause a commoution
        raise RuntimeError("Workload run failed") from e
    finally:
        workload.cleanup()
