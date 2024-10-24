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
	log "github.com/sirupsen/logrus"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

type AvroSerializer struct {
	ser   *avro.GenericSerializer
	deser *avro.GenericDeserializer
}

type payload struct {
	Value int `json:"val"`
}

func NewAvroSerializer(srClient *schemaregistry.Client) (s *AvroSerializer, err error) {
	ser, err := avro.NewGenericSerializer(*srClient, serde.ValueSerde, avro.NewSerializerConfig())

	if err != nil {
		return nil, err
	}

	deser, err := avro.NewGenericDeserializer(*srClient, serde.ValueSerde, avro.NewDeserializerConfig())

	if err != nil {
		return nil, err
	}

	s = new(AvroSerializer)

	s.ser = ser
	s.deser = deser

	return s, nil
}

func (s *AvroSerializer) CreateSerializedData(msgNum int, topic *string) ([]byte, error) {
	log.Debugf("Creating message with value %v", msgNum)
	val := payload{
		Value: msgNum,
	}

	return s.ser.Serialize(*topic, &val)
}

func (s *AvroSerializer) DeserializeAndCheck(msgNum int, topic *string, buf []byte) (valid bool, err error) {
	val := payload{}
	err = s.deser.DeserializeInto(*topic, buf, &val)

	if err != nil {
		return false, err
	}

	valid = (msgNum == val.Value)

	if !valid {
		log.Errorf("Mismatch: %v != %v", msgNum, val.Value)
	}

	return valid, nil
}
