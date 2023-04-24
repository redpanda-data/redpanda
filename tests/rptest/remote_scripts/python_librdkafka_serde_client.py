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
import sys
import logging
from collections import OrderedDict
from datetime import datetime
from enum import IntEnum
from typing import Optional
from uuid import uuid4

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.cimpl import KafkaError
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy, record_subject_name_strategy, topic_record_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer

import payload_pb2


class SchemaType(IntEnum):
    AVRO = 1
    PROTOBUF = 2


ProtobuPayloadClasses = {
    "com.redpanda.Payload": payload_pb2.Payload,
    "com.redpanda.A.B.C.D.NestedPayload": payload_pb2.A.B.C.D.NestedPayload
}


class AvroPayload(OrderedDict):
    def __init__(self, val: int):
        OrderedDict.__init__(OrderedDict([('val', int)]))
        self['val'] = val

    @property
    def val(self):
        return self['val']


AVRO_SCHEMA_PAYLOAD = '''{
"type": "record",
"name": "Payload",
"namespace": "com.redpanda",
  "fields": [
    {"name": "val", "type": "int"}
  ]
}
'''

AVRO_SCHEMA_NESTED_PAYLOAD = '''{
"type": "record",
"name": "NestedPayload",
"namespace": "com.redpanda.A.B.C.D",
  "fields": [
    {"name": "val", "type": "int"}
  ]
}
'''

AvroSchemas = {
    "com.redpanda.Payload": AVRO_SCHEMA_PAYLOAD,
    "com.redpanda.A.B.C.D.NestedPayload": AVRO_SCHEMA_NESTED_PAYLOAD
}


def make_protobuf_payload(payload_class, val: int):
    p = payload_class()
    p.val = val
    p.timestamp.FromDatetime(datetime.now())
    return p


subject_name_strategies = {
    "io.confluent.kafka.serializers.subject.TopicNameStrategy":
    topic_subject_name_strategy,
    "io.confluent.kafka.serializers.subject.RecordNameStrategy":
    record_subject_name_strategy,
    "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy":
    topic_record_subject_name_strategy
}


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
                 skip_known_types: Optional[bool] = None,
                 subject_name_strategy: Optional[str] = None,
                 payload_class: str = "com.redpanda.Payload"):
        self.logger = logger
        self.brokers = brokers
        self.sr_client = SchemaRegistryClient({'url': schema_registry_url})
        self.schema_type = schema_type
        self.topic = topic
        self.group = group
        self.protobuf_payload_class = ProtobuPayloadClasses[payload_class]
        self.avro_schema = AvroSchemas[payload_class]

        self.produced = 0
        self.acked = 0
        self.first_ack = None
        self.consumed = 0

        serde_config = {}
        if subject_name_strategy is not None:
            serde_config['subject.name.strategy'] = subject_name_strategies[
                subject_name_strategy]

        self.avro_serde_config = serde_config.copy()

        self.proto_serde_config = serde_config.copy()
        self.proto_serde_config['use.deprecated.format'] = False
        if skip_known_types is not None:
            self.proto_serde_config['skip.known.types'] = skip_known_types

        self.security_config = security_config

    def _make_serializer(self):
        return {
            SchemaType.AVRO:
            AvroSerializer(self.sr_client,
                           self.avro_schema,
                           conf=self.avro_serde_config),
            SchemaType.PROTOBUF:
            ProtobufSerializer(self.protobuf_payload_class, self.sr_client,
                               self.proto_serde_config)
        }[self.schema_type]

    def _make_deserializer(self):
        return {
            SchemaType.AVRO:
            AvroDeserializer(self.sr_client,
                             self.avro_schema,
                             from_dict=lambda d, _: AvroPayload(d['val'])),
            SchemaType.PROTOBUF:
            ProtobufDeserializer(self.protobuf_payload_class,
                                 {'use.deprecated.format': False})
        }[self.schema_type]

    def _make_payload(self, val: int):
        return {
            SchemaType.AVRO:
            AvroPayload(val),
            SchemaType.PROTOBUF:
            make_protobuf_payload(self.protobuf_payload_class, val)
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
        def increment(err: KafkaError, msg):
            if err is not None:
                self.logger.error(f"Produce err: {err}")
                sys.exit(err.code())
            assert msg is not None
            if self.first_ack is None:
                self.first_ack = msg.offset()
            assert msg.offset() == self.first_ack + self.acked
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
                    security_config=security_dict,
                    skip_known_types=args.skip_known_types,
                    subject_name_strategy=args.subject_name_strategy,
                    payload_class=args.payload_class)
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
                        choices=[v.name for v in SchemaType],
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
    parser.add_argument('--payload-class',
                        dest="payload_class",
                        default='com.redpanda.Payload',
                        choices=list(ProtobuPayloadClasses.keys()),
                        help="Which class to use as a payload")
    parser.add_argument(
        '--subject-name-strategy',
        default='io.confluent.kafka.serializers.subject.TopicNameStrategy',
        choices=subject_name_strategies.keys(),
        dest="subject_name_strategy",
        help="Subject Name Strategy")

    main(parser.parse_args())
