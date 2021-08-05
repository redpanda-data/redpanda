# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

#A python script that reproduces the error found at
#https://github.com/operatr-io/redpanda-reproducer-1

import sys
import time
from confluent_kafka import SerializingProducer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import KafkaException
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serialization import StringDeserializer
from socket import gethostname


#List all the groups in the cluster
def list_groups(a):
    retries = 5
    groups = ""
    ex = KafkaException()

    #Try list groups 5 times
    #-2 means success!
    #0 means fail.
    while retries > 0:
        try:
            groups = a.list_groups()
            retries = -2
        except Exception as e:
            ex = e
            print(f"Retry list_groups: {retries}")
            time.sleep(1)
            retries -= 1

    if retries == 0:
        raise ex
    else:
        print(groups)


#Create a topic
def create_topic(a, name):
    topic = NewTopic(name, num_partitions=12, replication_factor=3)
    futmap = a.create_topics([topic], operation_timeout=10, request_timeout=10)

    for topic, f in futmap.items():
        try:
            #Wait on result
            f.result()
            print(f"Created {topic}")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


#Produce to topic with given key and value
def produce(producer, topic, k, v):
    producer.produce(topic, key=k, value=v)
    print(f"Produced {k} {v}")


#Consume from a list of topics
#on a 1s poll.
def consume(consumer, topics):
    running = True
    msg_count = 0
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg:
                if msg.error():
                    print(msg.error())
                else:
                    msg_count += 1
                    if msg_count >= 5:
                        consumer.commit(asynchronous=False)
                        running = False
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        print(f"Consumed {msg_count} messages")


#This is a simple script to reproduce the
#coordinator_load_in_progress error that
#causes a time out on some kafka clients
def main(brokers):
    #The below code is nearly identical to
    #https://github.com/operatr-io/redpanda-reproducer-1/blob/main/src/kpow/redpanda.clj
    #but written with confluent_kafka python

    # Create an admin client
    a = AdminClient({'bootstrap.servers': brokers})

    # Create a producer
    prod_conf = {
        'bootstrap.servers': brokers,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': StringSerializer('utf_8')
    }

    producer = SerializingProducer(prod_conf)

    # Create a consumer
    cons_conf = {
        'bootstrap.servers': brokers,
        'group.id': "test-kpow-group",
        'enable.auto.commit': "true",
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': StringDeserializer('utf_8')
    }

    consumer = DeserializingConsumer(cons_conf)
    topic = "kpow_topic"

    list_groups(a)
    create_topic(a, topic)
    list_groups(a)
    produce(producer, topic, "a", "b")
    produce(producer, topic, "b", "c")
    produce(producer, topic, "c", "d")
    produce(producer, topic, "d", "e")
    produce(producer, topic, "e", "f")
    producer.flush()
    list_groups(a)

    consume(consumer, [topic])
    list_groups(a)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <brokers>")
        sys.exit(1)

    print(f"Brokers are: {sys.argv[1]}")
    main(sys.argv[1])
