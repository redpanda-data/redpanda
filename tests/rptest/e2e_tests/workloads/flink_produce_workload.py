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

from copy import deepcopy
from dataclasses import dataclass

from pyflink.common import Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema


@dataclass(kw_only=True)
class WorkloadConfig:
    # Default values are set for CDT run inside EC2 instance
    connector_path: str = "file:///opt/flink/connectors/flink-sql-connector-kafka-3.0.1-1.18.jar"
    logger_path: str = "/workloads"
    log_level: str = "DEBUG"
    producer_group: str = "flink_group"
    consumer_group: str = "flink_group"
    topic_name: str = "flink_workload_topic"
    msg_size: int = 4096
    count: int = 1000
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
        # Create flink env
        self.env = StreamExecutionEnvironment.get_execution_environment(config)
        # Add Kafka connector to Flink
        self.env.add_jars(self.config.connector_path)
        # Set the broker addresses
        self._basic_properties = {
            'bootstrap.servers': self.config.brokers,
        }
        self.logger.info(f"Brokers set to '{self.config.brokers}'")
        # Announce message holder array
        # This is suitable onlt for simple low-memory workloads
        self.messages = []

    def _generate_message(self):
        return ''.join(
            random.choices(string.ascii_letters + string.digits,
                           k=self.config.msg_size))

    def run(self):
        """
            Example of a produce task

            Steps:
            - generate messages using configured size and random.choices() func
            - create simple serializer
            - create Producer with topic name and group
            - execute producer
        """
        # Prepare data to be sent
        self.logger.info(f"Generating {self.config.count} messages")
        _messages = [(self._generate_message(), )
                     for i in range(self.config.count)]

        type_info = Types.ROW([Types.STRING()])
        ds = self.env.from_collection(_messages, type_info=type_info)

        # Serializer
        serialization_schema = JsonRowSerializationSchema.Builder() \
            .with_type_info(type_info) \
            .build()

        # Producer creation
        _properties = deepcopy(self._basic_properties)
        _properties['group.id'] = self.config.producer_group

        self.logger.info("Creating producer with target topic of "
                         f"'{self.config.topic_name}'")
        kafka_producer = FlinkKafkaProducer(
            topic=self.config.topic_name,
            serialization_schema=serialization_schema,
            producer_config=_properties)

        # Output type of ds must be RowTypeInfo
        ds.add_sink(kafka_producer)
        self.logger.info("About to produce messages")
        self.env.execute()
        self.logger.info("Done")

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
