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
from pyflink.table.udf import udf
from pyflink.table.expressions import col

MODE_PRODUCE = 'produce'
MODE_CONSUME = 'consume'


@dataclass(kw_only=True)
class WorkloadConfig:
    # Default values are set for CDT run inside EC2 instance
    connector_path: str = "file:///opt/flink/connectors/flink-sql-connector-kafka-3.0.1-1.18.jar"
    logger_path: str = "/workloads"
    log_level: str = "DEBUG"
    producer_group: str = "flink_group"
    consumer_group: str = "flink_group"
    # path to FOLDER
    data_path: str = "file:///workloads/data",
    topic_name: str = "flink_transactions_topic"
    # options are produce/consume
    mode: str = MODE_PRODUCE
    # maximum size of word in one message; tested values up to 512.
    # Probably will be limited only by str len of Python/Java
    # in the resulting INSERT: (4~word_size) * batch_size
    word_size: int = 16
    # Follow this rule to eliminate possibility of Java's OutOfMemory Exception
    # count / batch_size < 400
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

        # Publish it as class var
        self.table_env = table_env

    def _generate_words(self):
        """
            Generates crypto-like bogus array of words
        """
        # Generate words using random word count (10-64)
        # and random word len (4-word_size)
        return [
            ''.join(
                random.choices(string.ascii_letters,
                               k=random.randint(4, self.config.word_size)))
            for idx in range(random.randint(10, 64))
        ]

    def prepare_produce_mode(self):
        """
            Initialized produce mode

            Steps:
            - Generate words
            - Initialize schema and tables
        """
        # Prepare data to be sent
        self.logger.info(f"Generating {self.config.count} words")
        self.words = self._generate_words()

        # Define the table schema
        sink_table_schema = Schema.new_builder() \
            .column('word', DataTypes.STRING()) \
            .build()

        # Build descriptor with kafka connector and options
        # see: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/kafka/
        sink_table_desc = TableDescriptor.for_connector('kafka') \
            .schema(sink_table_schema) \
            .option('connector', 'kafka') \
            .option('topic', f'{self.config.topic_name}') \
            .option('properties.bootstrap.servers', f'{self.config.brokers}') \
            .option('properties.group.id', f'{self.config.producer_group}') \
            .option('scan.startup.mode', 'earliest-offset') \
            .option('format', 'csv') \
            .build()

        # Create target table
        self.sink_table = self.table_env.create_temporary_table(
            'sink', sink_table_desc)

    def produce_words(self):
        """
            Example of a produce task using Table API

            - initialize sending words in batches. I.e. single INSERT cmd
              aka transaction
            - select words for the next batch from the generated list
            - execute SQL command to insert words in table/topic
        """
        # Do the sending
        idx = 0
        while idx < self.config.count:
            # Prepare list of words that will be included in one INSERT cmd
            # aka batch
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

    def prepare_consume_mode(self):
        """
            Initialize consume mode

            Steps:
            - Init table schema for source
            - Init table schema for sink
        """

        # Define the table schema with metadata
        # See: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/kafka/
        source_table_schema = Schema.new_builder() \
            .column_by_metadata('event_time', DataTypes.TIMESTAMP(3),
                                'timestamp', is_virtual=False) \
            .column_by_metadata('partition', DataTypes.BIGINT(), 'partition',
                                is_virtual=True) \
            .column_by_metadata('offset', DataTypes.BIGINT(), 'offset',
                                is_virtual=True) \
            .column('word', DataTypes.STRING()) \
            .build()

        source_table_desc = TableDescriptor.for_connector('kafka') \
            .schema(source_table_schema) \
            .option('connector', 'kafka') \
            .option('topic', f'{self.config.topic_name}') \
            .option('properties.bootstrap.servers', f'{self.config.brokers}') \
            .option('properties.group.id', f'{self.config.producer_group}') \
            .option('scan.startup.mode', 'earliest-offset') \
            .option('scan.bounded.mode', 'latest-offset') \
            .option('format', 'csv') \
            .build()

        # Create target table
        self.table_env.create_temporary_table('source', source_table_desc)

        # Prepare sink with connector 'print'
        _print_schema = Schema.new_builder() \
            .column('a', DataTypes.BIGINT()) \
            .column('b', DataTypes.TIMESTAMP(3)) \
            .column('c', DataTypes.BIGINT()) \
            .build()
        _print_desc = TableDescriptor.for_connector('filesystem') \
            .schema(_print_schema) \
            .option('path', self.config.data_path) \
            .format('csv') \
            .build()
        self.table_env.create_temporary_table('sink', _print_desc)

    def consume_words(self):
        """
            Example of a consume task usign Table API
        """
        # User Defined function to count len of the string
        @udf(result_type=DataTypes.BIGINT())
        def length(data):
            return len(data)

        # process source
        # wait is critical for 'filesystem' sink connector
        # as the main process would exit before child process is
        # done writing the data
        # In case of remote call, like kafka, wait is not nessesary
        # Example folder structure after output is
        #   data/part-a70f2a9f-7a84-4d89-b493-22d00cc07838-8-0: CSV text
        table = self.table_env.from_path('source')
        # example csv row:
        # 65528,"2023-12-20 13:40:37.461",274
        table.select(col('offset'), col('event_time'),
                     length(col('word'))) \
            .execute_insert('sink') \
            .wait(120000)

        return

    def run(self):
        # Run selected mode
        if self.config.mode == MODE_PRODUCE:
            self.prepare_produce_mode()
            self.produce_words()
        elif self.config.mode == MODE_CONSUME:
            self.prepare_consume_mode()
            self.consume_words()
        else:
            self.logger.info(f"Mode '{self.config.mode}' not supported")

        return

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
