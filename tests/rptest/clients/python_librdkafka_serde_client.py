# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import argparse
import logging
from collections import OrderedDict
from enum import Enum
from uuid import uuid4

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer

from google.protobuf.descriptor_pb2 import FieldDescriptorProto
from google.protobuf import proto_builder


class AvroPayload(OrderedDict):
    def __init__(self, val: int):
        OrderedDict.__init__(OrderedDict([('val', int)]))
        self['val'] = val

    @property
    def val(self):
        return self['val']


AVRO_SCHEMA = '''{
"type": "record",
"name": "payload",
  "fields": [
    {"name": "val", "type": "int"}
  ]
}
'''

ProtobufPayloadClass = proto_builder.MakeSimpleProtoClass(
    OrderedDict([('val', FieldDescriptorProto.TYPE_INT64)]),
    full_name="example.Payload")


def make_protobuf_payload(val: int):
    p = ProtobufPayloadClass()
    p.val = val
    return p


class SchemaType(Enum):
    AVRO = 1
    PROTOBUF = 2


class SerdeClient:
    """
    SerdeClient produces and consumes payloads in avro or protobuf formats

    The expected offset is stored in the payload and checked.
    """
    def __init__(self,
                 brokers,
                 schema_registry_url,
                 schema_type: SchemaType,
                 *,
                 topic=str(uuid4()),
                 group=str(uuid4()),
                 logger=logging.getLogger("SerdeClient")):
        self.logger = logger
        self.brokers = brokers
        self.sr_client = SchemaRegistryClient({'url': schema_registry_url})
        self.schema_type = schema_type
        self.topic = topic
        self.group = group

        self.produced = 0
        self.acked = 0
        self.consumed = 0

    def _make_serializer(self):
        return {
            SchemaType.AVRO:
            AvroSerializer(self.sr_client, AVRO_SCHEMA),
            SchemaType.PROTOBUF:
            ProtobufSerializer(ProtobufPayloadClass, self.sr_client)
        }[self.schema_type]

    def _make_deserializer(self):
        return {
            SchemaType.AVRO:
            AvroDeserializer(self.sr_client,
                             AVRO_SCHEMA,
                             from_dict=lambda d, _: AvroPayload(d['val'])),
            SchemaType.PROTOBUF:
            ProtobufDeserializer(ProtobufPayloadClass)
        }[self.schema_type]

    def _make_payload(self, val: int):
        return {
            SchemaType.AVRO: AvroPayload(val),
            SchemaType.PROTOBUF: make_protobuf_payload(val)
        }[self.schema_type]

    def produce(self, count: int):
        def increment(err, msg):
            assert err is None
            assert msg is not None
            assert msg.offset() == self.acked
            self.logger.debug("Acked offset %d", msg.offset())
            self.acked += 1

        producer = SerializingProducer({
            'bootstrap.servers':
            self.brokers,
            'key.serializer':
            StringSerializer('utf_8'),
            'value.serializer':
            self._make_serializer()
        })

        self.logger.info("Producing %d %s records to topic %s", count,
                         self.schema_type.name, self.topic)
        for i in range(count):
            # Prevent overflow of buffer
            while len(producer) > 50000:
                # Serve on_delivery callbacks from previous calls to produce()
                producer.poll(0.1)

            producer.produce(topic=self.topic,
                             key=str(uuid4()),
                             value=self._make_payload(i),
                             on_delivery=increment)
            self.produced += 1

        self.logger.info("Flushing records...")
        producer.flush()
        self.logger.info("Records flushed: %d", self.produced)
        while self.acked < count:
            producer.poll(0.01)
        self.logger.info("Records acked: %d", self.acked)

    def consume(self, count: int):
        consumer = DeserializingConsumer({
            'bootstrap.servers':
            self.brokers,
            'key.deserializer':
            StringDeserializer('utf_8'),
            'value.deserializer':
            self._make_deserializer(),
            'group.id':
            self.group,
            'auto.offset.reset':
            "earliest"
        })
        consumer.subscribe([self.topic])

        self.logger.info("Consuming %d %s records from topic %s with group %s",
                         count, self.schema_type.name, self.topic, self.group)
        while self.consumed < count:
            msg = consumer.poll(1)
            if msg is None:
                continue
            payload = msg.value()
            self.logger.debug("Consumed %d at %d", payload.val, msg.offset())
            assert payload.val == self.consumed
            self.consumed += 1

        consumer.close()

    def run(self, count: int):
        self.produce(count)
        assert self.produced == count
        assert self.acked == count
        self.consume(count)
        assert self.consumed == count


def main(args):
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger("SerdeClient")
    logger.addHandler(handler)

    p = SerdeClient(args.bootstrap_servers,
                    args.schema_registry,
                    SchemaType[args.protocol],
                    topic=args.topic,
                    group=args.group,
                    logger=logger)
    p.run(args.count)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SerdeClient")
    parser.add_argument('-b',
                        dest="bootstrap_servers",
                        required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s',
                        dest="schema_registry",
                        required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-p',
                        dest="protocol",
                        default=SchemaType.AVRO.name,
                        choices=SchemaType._member_names_,
                        help="Topic name")
    parser.add_argument('-t',
                        dest="topic",
                        default=str(uuid4()),
                        help="Topic name")
    parser.add_argument('-g',
                        dest="group",
                        default=str(uuid4()),
                        help="Topic name")
    parser.add_argument('-c',
                        dest="count",
                        default=1,
                        type=int,
                        help="Number of messages to send")

    main(parser.parse_args())
