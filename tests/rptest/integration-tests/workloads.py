import datetime
import json
import random
import sys
import uuid
from kafka import KafkaProducer, KafkaConsumer


class TestProducer:
    def __init__(self, topic="test-source-topic"):
        """
        Kafka producer for generating test data.

        Parameters:
        - topic (str): Kafka topic name.
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda m: json.dumps(m).encode('ascii'))

    def send_callback(self, num_records, callback):
        """
        Send a specified number of records to the Kafka topic using a callback function.

        Parameters:
        - num_records (int): Number of records to send.
        - callback (function): Callback function to generate data.
        """
        for _ in range(num_records):
            self.producer.send(self.topic, callback())
        return


class TestConsumer:
    def __init__(self, group_id='test-group'):
        """
        Kafka consumer for consuming test data.

        Parameters:
        - group_id (str): Kafka consumer group ID.
        """
        conf = {
            'bootstrap_servers': 'localhost:9092',
            'group_id': group_id,
            'auto_offset_reset': 'earliest',
        }
        self.consumer = KafkaConsumer(
            value_deserializer=lambda m: json.loads(m.decode('utf-8')), **conf)

    def consume_fixed_number_of_messages(self, topics,
                                         num_messages_to_consume):
        """
        Consume a fixed number of messages from Kafka topics.

        Parameters:
        - topics (list): List of Kafka topics.
        - num_messages_to_consume (int): Number of messages to consume.
        """
        messages_consumed = 0  # Counter for consumed messages
        self.consumer.subscribe(topics)

        try:
            while messages_consumed < num_messages_to_consume:
                msg = self.consumer.poll(5.0)

                if msg is None:
                    continue
                else:
                    print(f"Message from {topics} : {msg}")
                messages_consumed += 1  # Increment the message counter

        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

    def consume_from_topic(self, topics):
        """
        Consume messages from Kafka topics.

        Parameters:
        - topics (list): List of Kafka topics.
        """
        try:
            self.consumer.subscribe(topics)

            while True:
                msg = self.consumer.poll(5.0)
                if msg is None:
                    continue
                else:
                    print(f"Message from {topics} : {msg}")
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()


class Workload:
    def __init__(self, name, description):
        """
        Base class for defining workloads.

        Parameters:
        - name (str): Workload name.
        - description (str): Workload description.
        """
        self.name = name
        self.description = description

    def generate_data(self, num_records):
        """
        Generate test data.

        Parameters:
        - num_records (int): Number of records to generate.

        Returns:
        - data (list): List of generated data.
        """
        raise NotImplementedError

    def verify_data(self, data):
        """
        Verify test data.

        Parameters:
        - data: Data to verify.

        Returns:
        - result: Result of the verification.
        """
        raise NotImplementedError


class NumberIncrementalWorkload(Workload):
    def __init__(self):
        super().__init__(
            name="Number Incremental Workload with Timestamp",
            description=
            "Generates a stream of numbers with incremental values and timestamps."
        )

    def generate_data(self):
        num_a = random.randint(1, 1000)
        num_b = random.randint(1, 1000)
        return {
            "id": str(uuid.uuid4()),
            "ts": str(datetime.datetime.now()),
            "num_a": num_a,
            "num_b": num_b,
            "source_sum": num_a + num_b
        }

    def verify_data(self, num_records):
        """
        Verify topics data.

        Parameters:
        - num_records (int): Number of records to verify.
        """
        print("Validate topics data...")

        # Common Kafka consumer settings
        kafka_settings = {
            'bootstrap_servers': 'localhost:9092',
            'group_id': 'test-group',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'value_deserializer': lambda x: x.decode('utf-8'),
            'request_timeout_ms': 100000,
            'max_poll_records': 100
        }

        # Create Kafka consumers for source and sink topics
        source_consumer = KafkaConsumer('test-source-topic', **kafka_settings)
        sink_consumer = KafkaConsumer('test-sink-topic', **kafka_settings)

        print("Reading topics...")
        source_topic_records = read_events_and_store(source_consumer,
                                                     'test-source-topic',
                                                     num_records)
        import time
        time.sleep(5)
        sink_topic_records = read_events_and_store(sink_consumer,
                                                   'test-sink-topic',
                                                   num_records)
        # print(f"source_topic_records : {source_topic_records}")
        # print(f"sink_topic_records : {sink_topic_records}")

        errors = []
        if len(source_topic_records) == len(sink_topic_records):
            for source_key, source_value in source_topic_records.items():
                if source_key in sink_topic_records.keys():
                    if str(source_value["source_sum"]) == str(
                            sink_topic_records[source_key]["sink_sum"]):
                        print(
                            f"Event match: source_sum = {source_value['source_sum']}, sink_sum = {sink_topic_records[source_key]['sink_sum']}"
                        )
                    else:

                        msg = f"Event mismatch: {source_value['source_sum']}, {sink_topic_records[source_key]['sink_sum']}"
                        errors.append(msg)
        if errors:
            raise Exception(errors)


def read_events_and_store(consumer, topic, num_records):
    """
    Read events from a Kafka topic and store them.

    Parameters:
    - consumer: Kafka consumer.
    - topic (str): Kafka topic name.
    - num_records (int): Number of records to read.

    Returns:
    - result: Stored events.
    """
    result = {}  # To store the events

    # Subscribe to the topic
    consumer.subscribe([topic])

    # Read messages from Kafka, print to stdout
    try:
        for _ in range(num_records):
            msg = consumer.poll(100)
            if msg is None:
                continue

            for topic_partition, records in msg.items():
                for record in records:
                    # Access the 'value' attribute of the ConsumerRecord
                    record_value = record.value

                    # Parse the JSON string in the 'value' attribute
                    record_data = json.loads(record_value)
                    # print(record_data)

                    result[record_data['id']] = record_data

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

    return result


if __name__ == '__main__':
    prod = TestProducer()

    num_events = 8
    workload = NumberIncrementalWorkload(
    )  # Replace with the desired workload class

    prod.send_callback(num_events, workload.generate_data)

    workload.verify_data(num_events)

