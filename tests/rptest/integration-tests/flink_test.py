from workloads import Workload

from rptest.services.cluster import cluster
from rptest.clients.default import DefaultClient

from rptest.clients.rpk import RpkTool, RpkException
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import ResourceSettings
from rptest.clients.types import TopicSpec
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.rpk_producer import RpkProducer

# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors.kafka import KafkaSource
# from pyflink.datastream.connectors.python import StreamingFileSink

# Import the Workload classes
from workload import Workload, \
    NumberIncrementalWorkload  # RealtimeWordCountWorkload, StreamAggregationWorkload, GeospatialDataProcessingWorkload

class FlinkTest(RedpandaTest):
    def __init__(self, test_ctx,  *args, **kwargs):
        self._ctx = test_ctx
        self.producer = None
        super(FlinkTest, self).__init__(
            test_ctx,
            num_brokers=3,
            *args,
            **kwargs)

    def create_consumer(self,
                        topic,
                        group,
                        instance_name,
                        instance_id=None,
                        consumer_properties={}):
        return KafkaCliConsumer(
            self.test_context,
            self.redpanda,
            topic=topic,
            group=group,
            from_beginning=True,
            instance_name=instance_name,
            formatter_properties={
                'print.value': 'false',
                'print.key': 'false',
                'print.partition': 'true',
                'print.offset': 'true',
            },
            consumer_properties=FlinkTest.make_consumer_properties(
                consumer_properties, instance_id))
    
    def create_topic(self, p_cnt):
        # create topic
        self.topic_spec = TopicSpec(partition_count=p_cnt,
                                    replication_factor=3)

        self.client().create_topic(specs=self.topic_spec)

    def start_producer(self, msg_cnt=5000):

        # produce some messages to the topic
        self.producer = RpkProducer(self._ctx, self.redpanda,
                                    self.topic_spec.name, 128, msg_cnt, -1)
        self.producer.start()

    @cluster(num_nodes=3)
    def test_flink_integration(self):
        """
        Test validating that end to end flow of redpanda and flink together
        """
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("test_topic")

        redpanda = RedpandaTest()
        redpanda.si_settings()

        # below code will be uncommented gradually after debugging

'''
workload = NumberIncrementalWorkload()  # Replace with the desired workload class
data = workload.generate_data(1000)  # Generate 1000 records by default

# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# Create a KafkaSource to read data from Redpanda
kafka_source = KafkaSource(
    topics=["test-topic"],
    brokers="localhost:9092",
    value_deserializer=SimpleStringSchema()
)

# Create a StreamTransformation to process the data
data_stream = env.add_source(kafka_source)

if isinstance(workload, NumberIncrementalWorkload):
    # Process the data for real-time word count
    word_counts = data_stream.flat_map(lambda sentence: sentence.split()) \
        .map(lambda word: (word, 1)) \
        .key_by(lambda word_count: word_count[0]) \
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    # Print the word counts to the console
    word_counts.print()

elif isinstance(workload, StreamAggregationWorkload):
    # Process the data for stream aggregation
    avg_value = data_stream.map(lambda value: value[1]) \
        .reduce(lambda a, b: (a[0] + 1, a[1] + b))

    # Print the average value to the console
    avg_value.print()

elif isinstance(workload, GeospatialDataProcessingWorkload):
    # Process the data for geospatial data processing
    avg_latitude, avg_longitude = data_stream.map(lambda point: (point[0], point[1])) \
        .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .map(lambda avg_values: (avg_values[0] / 2, avg_values[1] / 2))

    # Print the average latitude and longitude to the console
    avg_latitude.add_sink(StreamingFileSink.for_row_format("avg_latitude.txt", SimpleStringSchema()))
    avg_longitude.add_sink(StreamingFileSink.for_row_format("avg_longitude.txt", SimpleStringSchema()))

# Execute the Flink job
env.execute("Workload Demo")
'''
