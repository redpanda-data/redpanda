/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ProtobufSerializer struct {
	ser   *protobuf.Serializer
	deser *protobuf.Deserializer
}

func NewProtobufSerializer(srClient *schemaregistry.Client) (s *ProtobufSerializer, err error) {
	ser, err := protobuf.NewSerializer(*srClient, serde.ValueSerde, protobuf.NewSerializerConfig())

	if err != nil {
		return nil, err
	}

	deser, err := protobuf.NewDeserializer(*srClient, serde.ValueSerde, protobuf.NewDeserializerConfig())

	if err != nil {
		return nil, err
	}

	deser.ProtoRegistry.RegisterMessage((&Payload{}).ProtoReflect().Type())

	s = new(ProtobufSerializer)

	s.ser = ser
	s.deser = deser
	return s, nil
}

func (s *ProtobufSerializer) CreateSerializedData(msgNum int, topic *string) ([]byte, error) {
	val := Payload{
		Val:       int32(msgNum),
		Timestamp: timestamppb.Now(),
	}

	return s.ser.Serialize(*topic, &val)
}

func (s *ProtobufSerializer) DeserializeAndCheck(msgNum int, topic *string, buf []byte) (valid bool, err error) {
	val := Payload{}
	err = s.deser.DeserializeInto(*topic, buf, &val)

	if err != nil {
		return false, nil
	}

	valid = (int32(msgNum) == val.Val)

	if !valid {
		log.Errorf("Mismatch: %v != %v", msgNum, val.Val)
	}

	return valid, nil
}
