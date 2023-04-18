#!/usr/bin/env python3

# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import argparse
import json
import logging
from collections import OrderedDict
from datetime import datetime
from enum import IntEnum
from typing import Optional
from uuid import uuid4

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer

from payload_pb2 import Payload as ProtobufPayloadClass


class SchemaType(IntEnum):
    AVRO = 1
    PROTOBUF = 2


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


def make_protobuf_payload(val: int):
    p = ProtobufPayloadClass()
    p.val = val
    p.timestamp.FromDatetime(datetime.now())
    return p


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
                 logger=logging.getLogger("SerdeClient"),
                 security_config: dict = None,
                 skip_known_types: Optional[bool] = None):
        self.logger = logger
        self.brokers = brokers
        self.sr_client = SchemaRegistryClient({'url': schema_registry_url})
        self.schema_type = schema_type
        self.topic = topic
        self.group = group

        self.produced = 0
        self.acked = 0
        self.consumed = 0

        self.serde_config = {'use.deprecated.format': False}
        if skip_known_types is not None:
            self.serde_config.update({'skip.known.types': skip_known_types})

        self.security_config = security_config

    def _make_serializer(self):
        return {
            SchemaType.AVRO:
            AvroSerializer(self.sr_client, AVRO_SCHEMA),
            SchemaType.PROTOBUF:
            ProtobufSerializer(ProtobufPayloadClass, self.sr_client,
                               self.serde_config)
        }[self.schema_type]

    def _make_deserializer(self):
        return {
            SchemaType.AVRO:
            AvroDeserializer(self.sr_client,
                             AVRO_SCHEMA,
                             from_dict=lambda d, _: AvroPayload(d['val'])),
            SchemaType.PROTOBUF:
            ProtobufDeserializer(ProtobufPayloadClass,
                                 {'use.deprecated.format': False})
        }[self.schema_type]

    def _make_payload(self, val: int):
        return {
            SchemaType.AVRO: AvroPayload(val),
            SchemaType.PROTOBUF: make_protobuf_payload(val)
        }[self.schema_type]

    def _get_security_options(self):
        if self.security_config is None:
            return {}

        return {
            'sasl.mechanism': self.security_config['sasl_mechanism'],
            'sasl.password': self.security_config['sasl_plain_password'],
            'sasl.username': self.security_config['sasl_plain_username'],
            'security.protocol': self.security_config['security_protocol']
        }

    def produce(self, count: int):
        def increment(err, msg):
            assert err is None
            assert msg is not None
            assert msg.offset() == self.acked
            self.logger.debug("Acked offset %d", msg.offset())
            self.acked += 1

        producer = SerializingProducer(
            {
                'bootstrap.servers': self.brokers,
                'key.serializer': StringSerializer('utf_8'),
                'value.serializer': self._make_serializer()
            } | self._get_security_options())

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
        consumer = DeserializingConsumer(
            {
                'bootstrap.servers': self.brokers,
                'key.deserializer': StringDeserializer('utf_8'),
                'value.deserializer': self._make_deserializer(),
                'group.id': self.group,
                'auto.offset.reset': "earliest"
            } | self._get_security_options())
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

    security_dict = None
    if args.security is not None:
        security_dict = json.loads(args.security)

    p = SerdeClient(args.bootstrap_servers,
                    args.schema_registry,
                    SchemaType[args.protocol],
                    topic=args.topic,
                    group=args.group,
                    logger=logger,
                    security_config=security_dict)
    p.run(args.count)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SerdeClient")
    parser.add_argument('-b',
                        '--brokers',
                        dest="bootstrap_servers",
                        required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s',
                        '--schema-registry',
                        dest="schema_registry",
                        required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-p',
                        '--protocol',
                        dest="protocol",
                        default=SchemaType.AVRO.name,
                        choices=SchemaType._member_names_,
                        help="Topic name")
    parser.add_argument('-t',
                        '--topic',
                        dest="topic",
                        default=str(uuid4()),
                        help="Topic name")
    parser.add_argument('-g',
                        '--consumer-group',
                        dest="group",
                        default=str(uuid4()),
                        help="Topic name")
    parser.add_argument('-c',
                        '--count',
                        dest="count",
                        default=1,
                        type=int,
                        help="Number of messages to send")
    parser.add_argument('--security',
                        dest="security",
                        help="JSON formatted security string")

    parser.add_argument('--skip-known-types',
                        dest="skip_known_types",
                        action='store_true',
                        help="Optional bool")

    main(parser.parse_args())
