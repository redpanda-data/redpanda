import json
import logging
import sys

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, \
    KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.functions import FlatMapFunction
from pyflink.common.typeinfo import Types
from pyflink.common import Row

# Get the absolute path to the JAR file
# jar_path = "file://" + os.path.abspath("../integration-tests/flink-connector-kafka-3.0.1-1.18.jar")


# change the output type as specified
class SumNumbers(FlatMapFunction):
    def flat_map(self, value):
        events = json.loads(value)
        print(f"Source events: {events}")

        # Convert the numbers to integers
        try:
            num1 = events["num_a"]
            num2 = events["num_b"]
        except ValueError:
            # Handle invalid input
            return

        # Calculate the sum of the numbers
        sum = num1 + num2
        logging.info(f"Sum:{sum}")

        # Return the sum as a string
        yield Row(id=events["id"],
                  num_a=str(num1),
                  num_b=str(num2),
                  sink_sum=str(sum))


def flink_job(env):
    # create a RedPanda Source to read data from the input topic
    redpanda_source = KafkaSource \
        .builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_group_id('test_group') \
        .set_topics("test-source-topic") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()

    # Read the data as a stream of strings
    data_stream = env.from_source(redpanda_source,
                                  WatermarkStrategy.no_watermarks(),
                                  "kafka source")
    KafkaSource.builder().set_topics("test-source-topic")

    # specify output type
    sum_stream = data_stream.flat_map(
        SumNumbers(),
        output_type=Types.ROW_NAMED(
            field_names=["id", "num_a", "num_b", "sink_sum"],
            field_types=[
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
            ]),
    )

    # Define the output type for the sum_stream
    output_type = Types.ROW_NAMED(
        field_names=["id", "num_a", "num_b", "sink_sum"],
        field_types=[
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING()
        ])

    # set value serialization schema
    redpanda_sink = (KafkaSink.builder().set_bootstrap_servers(
        "localhost:9092").set_record_serializer(
            KafkaRecordSerializationSchema.builder().set_topic(
                "test-sink-topic").set_value_serialization_schema(
                    JsonRowSerializationSchema.builder().with_type_info(
                        output_type).build()).build()).set_delivery_guarantee(
                            DeliveryGuarantee.AT_LEAST_ONCE).build())

    # Write the transformed data to the output topic
    sum_stream.sink_to(redpanda_sink)

    # Execute the Flink job
    print("Flink job for Sum Numbers")
    env.execute("Flink job for Sum Numbers")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO,
                        format="%(message)s")

    # create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    flink_job(env)
