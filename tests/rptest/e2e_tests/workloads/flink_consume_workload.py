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

from copy import deepcopy
from dataclasses import dataclass

from pyflink.common import Types, Configuration, SimpleStringSchema, \
    WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer


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


class FlinkWorkloadConsume:
    """
        Simple Consume workload
        Goal is to consume all messages from target topic

        This workload requires apache-flink to be available as a python system
        wide dependency and not properly working without it.

        Also, there is an additional work required to stop Consumer
        when messages exhausted
    """
    def __init__(self, config_override):
        # Serialize config
        self.config = WorkloadConfig(**config_override)
        # Create logger
        filename = f"{os.path.basename(__file__).split('.')[0]}.log"
        logfile = os.path.join(self.config.logger_path, filename)
        self.logger = setup_logger(logfile, self.config.log_level)

    def setup(self):
        self.logger.info("Initializing consumer workload")
        # Initialize
        config = Configuration()
        # This is required for ducktape EC2 run
        config.set_string("python.client.executable", "python3")
        config.set_string("python.executable", "python3")

        # Create environment
        self.env = StreamExecutionEnvironment.get_execution_environment(config)
        # Add kafka connector
        self.env.add_jars(self.config.connector_path)
        # Brokers
        self._basic_properties = {
            'bootstrap.servers': self.config.brokers,
        }
        self.logger.info(f"Brokers set to '{self.config.brokers}'")
        self.type_info = Types.ROW([Types.STRING()])

        self.messages = []

    def run(self):
        """
            Example consume task

            Steps:
            - create deserializer and configured properties
            - create consumer
            - set offset to earliest
            - assign source and add callback class
            - execute
        """
        # Consumer creation
        properties = deepcopy(self._basic_properties)
        properties['group.id'] = self.config.consumer_group

        self.logger.info("Creating consumer with target topic of "
                         f"'{self.config.topic_name}'")
        # Create my consumer
        source = KafkaSource \
            .builder() \
            .set_bootstrap_servers(self.config.brokers) \
            .set_group_id(self.config.producer_group) \
            .set_topics(self.config.topic_name) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_bounded(KafkaOffsetsInitializer.latest()) \
            .build()

        ds = self.env.from_source(source, WatermarkStrategy.no_watermarks(),
                                  "Kafka Source")
        # Just print received message
        ds.print()

        # Run
        self.logger.info("Runnig consumer")
        self.env.execute()
        self.logger.info("Done")

    def cleanup(self):
        # Nothing to cleanup as of right now
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
    workload = FlinkWorkloadConsume(input_config)
    try:
        workload.setup()
        workload.run()
    except Exception as e:
        # Do not re-throw not to cause a commoution
        raise RuntimeError("Workload run failed") from e
    finally:
        workload.cleanup()
