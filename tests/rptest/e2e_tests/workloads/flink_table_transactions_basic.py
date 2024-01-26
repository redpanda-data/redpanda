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
import subprocess

from dataclasses import dataclass

from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, \
    TableDescriptor, Schema, DataTypes

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import udf
from pyflink.table.expressions import col

MODE_PRODUCE = 'produce'
MODE_CONSUME = 'consume'


def shell(cmd) -> list[str]:
    # Run single command
    _cmd = cmd.split(" ")
    p = subprocess.Popen(_cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         text=True)
    p.wait()
    # get stdout, expect no errors
    _out = p.communicate()[0].splitlines()
    # _rcode = p.returncode
    p.kill()
    return _out


@dataclass(kw_only=True)
class WorkloadConfig:
    # Default values are set for CDT run inside EC2 instance
    connector_path: str = "File:///opt/flink/connectors/flink-sql-connector-kafka-3.0.1-1.18.jar"
    python_archive: str = "/opt/flink/flink_venv.tgz"
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
    count: int = 512
    batch_size: int = 64
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
        # Checkpoints settings
        # See: https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.datastream/checkpoint.html
        # and: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/

        # start a checkpoint every 1000 ms
        env.enable_checkpointing(1000)
        # advanced options:
        # set mode to exactly-once (this is the default)
        env.get_checkpoint_config().set_checkpointing_mode(
            CheckpointingMode.EXACTLY_ONCE)
        # make sure 500 ms of progress happen between checkpoints
        env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
        # checkpoints have to complete within one minute, or are discarded
        env.get_checkpoint_config().set_checkpoint_timeout(60000)
        # only two consecutive checkpoint failures are tolerated
        env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)
        # allow only one checkpoint to be in progress at the same time
        env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
        # enable externalized checkpoints which are retained after job cancellation
        env.get_checkpoint_config().enable_externalized_checkpoints(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        # enables the unaligned checkpoints
        env.get_checkpoint_config().enable_unaligned_checkpoints()

        # Configure jars
        env.add_jars(self.config.connector_path)
        settings = EnvironmentSettings.new_instance() \
            .in_streaming_mode()\
            .build()
        # Create Table API in top of Streaming env
        table_env = StreamTableEnvironment.create(
            stream_execution_environment=env, environment_settings=settings)

        # Tune table idle state handling
        # Clear the state if it has not changed
        table_env.get_config().set("table.exec.state.ttl", "60 s")
        # If a source does not received anything after 5 sec mark it as idle
        table_env.get_config().set("table.exec.source.idle-timeout", "5000 ms")

        # Alternative way to add connector, just for illustrative purposes
        table_env.get_config().set("pipeline.jars", self.config.connector_path)
        # This is needed for Scalar functions work
        # See https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/dependency_management/
        lib_path_list = shell("find /opt/flink/opt/ -name flink-python*")
        if len(lib_path_list) < 1:
            raise RuntimeError("Failed to find flink-python-....jar "
                               "at '/opt/flink/opt/")
        elif len(lib_path_list) > 1:
            self.logger.info(f"Found multiple libs: {lib_path_list}")
        self.logger.info(f"Using python classlib at '{lib_path_list[0]}'")
        table_env.get_config().set("pipeline.classpaths",
                                   f"file://{lib_path_list[0]}")

        # Add python venv archive to table_api
        table_env.add_python_archive(self.config.python_archive, "venv")
        table_env.get_config().set_python_executable("venv/bin/python")
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
        self.sink_table_schema = Schema.new_builder() \
            .column('index', DataTypes.BIGINT()) \
            .column('subindex', DataTypes.BIGINT()) \
            .column('word', DataTypes.STRING()) \
            .build()

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
        batch_count = 0
        while idx < self.config.count:
            batch_count += 1
            # Prepare list of words that will be included in one INSERT cmd
            # aka batch
            words_to_go = self.config.count - idx
            if words_to_go == 0:
                break
            size = self.config.batch_size
            size = size if words_to_go > size else words_to_go
            values = []
            for count in range(size):
                word = self.id_to_word(random.randint(0, len(self.words) - 1))
                row = f"({idx+count}, {count}, '{word}')"
                values.append(row)

            sys.stdout.write(f"Initializing batch {batch_count}'\n")
            # Build descriptor with kafka connector and options
            # see: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/kafka/
            sink_table_desc = TableDescriptor.for_connector('kafka') \
                .schema(self.sink_table_schema) \
                .option('connector', 'kafka') \
                .option('topic', f'{self.config.topic_name}') \
                .option('properties.bootstrap.servers', f'{self.config.brokers}') \
                .option('properties.group.id', f'{self.config.producer_group}') \
                .option('properties.transaction.timeout.ms', '15000') \
                .option('scan.startup.mode', 'earliest-offset') \
                .option('sink.delivery-guarantee', 'exactly-once') \
                .option('sink.transactional-id-prefix',
                        f'flink_transaction_test_{batch_count}') \
                .option('format', 'csv') \
                .build()

            _temp_sink_name = f'sink_{batch_count}'
            # Create target table
            self.sink_table = self.table_env.create_temporary_table(
                _temp_sink_name, sink_table_desc)

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
            sys.stdout.write(f"Sending {len(values)} values at '{idx}'\n")
            sys.stdout.flush()
            self.table_env.execute_sql(f"INSERT INTO {_temp_sink_name} "
                                       f"VALUES {','.join(values)};")
            self.table_env.drop_temporary_table(_temp_sink_name)
            idx += size

        return

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
            .column('index', DataTypes.BIGINT()) \
            .column('subindex', DataTypes.BIGINT()) \
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
            .option('properties.transaction.timeout.ms', '15000') \
            .option('properties.auto.offset.reset', 'earliest') \
            .option('properties.isolation.level', 'read_committed') \
            .option('scan.startup.mode', 'earliest-offset') \
            .option('scan.bounded.mode', 'latest-offset') \
            .option('format', 'csv') \
            .build()

        # Create target table
        self.table_env.create_temporary_table('source', source_table_desc)

        # Prepare sink with connector 'print'
        _print_schema = Schema.new_builder() \
            .column('offs', DataTypes.BIGINT()) \
            .column('index', DataTypes.BIGINT()) \
            .column('bindex', DataTypes.BIGINT()) \
            .column('ts', DataTypes.TIMESTAMP(3)) \
            .column('len', DataTypes.BIGINT()) \
            .column('data', DataTypes.STRING()) \
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
        # 542,415,31,"2024-01-09 22:22:43.647",17,TiJJtOuzOkiOsGmws
        table.select(col('offset'), col('index'), col('subindex'),
                     col('event_time'), length(col('word')), col('word')) \
            .execute_insert('sink')

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
