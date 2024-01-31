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
import sys

from dataclasses import dataclass

from pyflink.common import WatermarkStrategy, Types, SimpleStringSchema
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee, \
    KafkaRecordSerializationSchema, KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors import NumberSequenceSource

MODE_PRODUCE = 'produce'
MODE_CONSUME = 'consume'


@dataclass(kw_only=True)
class WorkloadConfig:
    # Default values are set for CDT run inside EC2 instance
    connector_path: str = "File:///opt/flink/connectors/flink-sql-connector-kafka-3.0.1-1.18.jar"
    python_lib_path: str = "File:///opt/flink/opt/flink-python-1.18.0.jar"
    python_archive: str = "/opt/flink/flink_venv.tgz"
    logger_path: str = "/workloads"
    log_level: str = "DEBUG"
    producer_group: str = "flink_group"
    consumer_group: str = "flink_group"
    topic_name: str = "flink_transactions_topic"
    transaction_id_prefix: str = "flink_transaction_prefix"
    # options are produce/consume
    mode: str = MODE_PRODUCE
    # How many events generate
    count: int = 512
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
        config.set_string("pipeline.jars", self.config.connector_path)
        # This is needed for Scalar functions work
        # See https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/dependency_management/
        config.set_string("pipeline.classpaths", self.config.python_lib_path)

        # Create flink env
        # Streaming env is user in order to add kafka connector
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

        # Configure jars, alternatively
        env.add_jars(self.config.connector_path)
        # Add python venv archive to table_api
        env.add_python_archive(self.config.python_archive, "venv")
        env.set_python_executable("venv/bin/python")

        # Checkpoints settings
        # See: https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.datastream/checkpoint.html
        # and: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/

        # start a checkpoint every 1000 ms
        env.enable_checkpointing(1000)
        # advanced options:
        # set mode to exactly-once (this is the default)
        env.get_checkpoint_config().set_checkpointing_mode(
            CheckpointingMode.EXACTLY_ONCE)
        # make sure 1 ms of progress happen between checkpoints
        env.get_checkpoint_config().set_min_pause_between_checkpoints(1)
        # checkpoints have to complete within one minute, or are discarded
        env.get_checkpoint_config().set_checkpoint_timeout(30000)
        # only two consecutive checkpoint failures are tolerated
        env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)
        # allow only one checkpoint to be in progress at the same time
        env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
        # enable externalized checkpoints which are retained
        # after job cancellation
        env.get_checkpoint_config().enable_externalized_checkpoints(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        # enables the unaligned checkpoints
        env.get_checkpoint_config().enable_unaligned_checkpoints()

        # Save env
        self.stream_env = env

    def prepare_produce_mode(self):
        """
            Initialized produce mode

            Steps:
            - Use simple number Sequence as a source
            - Transform it to str
            - Send to brokers
        """

        # NumberSequence produces Types.LONG
        # Cast it to str
        def transform(value):
            yield str(value)

        # Prepare data source
        source = NumberSequenceSource(0, self.config.count)
        # Create DataStream
        ds = self.stream_env.from_source(
            source=source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="number_sequence")
        # Assign transformation
        self.datasource = ds.flat_map(transform, output_type=Types.STRING())
        # Create simple serializer with String schema
        record_serializer = KafkaRecordSerializationSchema.builder() \
            .set_topic(self.config.topic_name) \
            .set_key_serialization_schema(SimpleStringSchema()) \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .build()
        # Build KafkaSource class
        self.sink = KafkaSink.builder() \
            .set_bootstrap_servers(self.config.brokers) \
            .set_record_serializer(record_serializer) \
            .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE) \
            .set_transactional_id_prefix(self.config.transaction_id_prefix) \
            .build()

    def produce_sequence(self):
        # Add previously created KafkaSink class as a sink to DataStream
        self.datasource.sink_to(self.sink)
        # Do the sending
        self.stream_env.execute()

        return

    def prepare_consume_mode(self):
        """
            Initialize consume mode

            Steps:
            - Use KafkaSource
            - Just dump sequence to stdout
        """
        self.logger.info("Creating consumer with target topic of "
                         f"'{self.config.topic_name}'")
        # Create consumer from KafkaSource class
        source = KafkaSource \
            .builder() \
            .set_bootstrap_servers(self.config.brokers) \
            .set_group_id(self.config.consumer_group) \
            .set_topics(self.config.topic_name) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_bounded(KafkaOffsetsInitializer.latest()) \
            .build()

        ds = self.stream_env.from_source(source,
                                         WatermarkStrategy.no_watermarks(),
                                         "Kafka Source")
        # Just print received message
        ds.print()
        self.datasource = ds

    def consume_sequence(self):
        """
            Example of a consume task usign Table API
        """
        # Run
        self.logger.info("Runnig consumer")
        self.stream_env.execute()
        self.logger.info("Done")

        return

    def run(self):
        # Run selected mode
        if self.config.mode == MODE_PRODUCE:
            self.prepare_produce_mode()
            self.produce_sequence()
        elif self.config.mode == MODE_CONSUME:
            self.prepare_consume_mode()
            self.consume_sequence()
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
