// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package serde

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// serdeFunc is a encoding/decoding function.
type serdeFunc func([]byte) ([]byte, error)

// Serde is a protobuf or avro serializer/deserializer.
type Serde struct {
	encodeFn serdeFunc
	decodeFn serdeFunc
}

// NewSerde will build a de/serializer based on the schema type. For Protobuf
// schemas, it will use the FQN to find the message type, if no FQN is provided,
// it will only build a deserializer. Each encoded record will contain the
// schema registry wire format bytes (Magic number, Schema ID and Index for
// Proto).
func NewSerde(ctx context.Context, cl *sr.Client, schema *sr.Schema, schemaID int, protoFQN string) (*Serde, error) {
	switch schema.Type {
	case sr.TypeAvro:
		codec, err := generateAvroCodec(ctx, cl, schema)
		if err != nil {
			return nil, fmt.Errorf("unable to parse avro schema: %v", err)
		}
		encFn, err := newAvroEncoder(codec, schemaID)
		if err != nil {
			return nil, fmt.Errorf("unable to build avro encoder: %v", err)
		}
		decFn, err := newAvroDecoder(codec)
		if err != nil {
			return nil, fmt.Errorf("unable to build avro decoder: %v", err)
		}
		return &Serde{encFn, decFn}, nil
	case sr.TypeProtobuf:
		compiled, err := compileSchema(ctx, cl, schema)
		if err != nil {
			return nil, fmt.Errorf("unable to compile proto schema: %v", err)
		}
		// If the proto FQN is not provided, but we only have one message, we
		// use that.
		if protoFQN == "" && compiled.FindFileByPath(inMemFileName).Messages().Len() == 1 {
			protoFQN = string(compiled.FindFileByPath(inMemFileName).Messages().Get(0).FullName())
		}
		var encFn serdeFunc
		// If there is no FQN, most likely we are trying to decode only.
		if protoFQN != "" {
			encFn, err = newProtoEncoder(compiled, protoFQN, schemaID)
			if err != nil {
				return nil, fmt.Errorf("unable to build protobuf encoder: %v", err)
			}
		}
		decFn, err := newProtoDecoder(compiled)
		if err != nil {
			return nil, fmt.Errorf("unable to build protobuf decoder: %v", err)
		}
		return &Serde{encFn, decFn}, nil
	case sr.TypeJSON:
		compiledSchema, err := compileJsonschema(ctx, cl, schema)
		if err != nil {
			return nil, fmt.Errorf("unable to compile jsonschema: %v", err)
		}

		encFn, err := newJsonschemaEncoder(schemaID, compiledSchema)
		if err != nil {
			return nil, fmt.Errorf("unable to build jsonschema encoder: %v", err)
		}

		decFn, err := newJsonschemaDecoder(compiledSchema)
		if err != nil {
			return nil, fmt.Errorf("unable to build jsonschema decoder: %v", err)
		}

		return &Serde{encFn, decFn}, nil
	default:
		return nil, fmt.Errorf("schema with ID %v contains an unsupported schema type %v", schemaID, schema.Type)
	}
}

// EncodeRecord will encode the given record using the internal encodeFn.
func (s *Serde) EncodeRecord(record []byte) ([]byte, error) {
	if s.encodeFn == nil {
		return nil, errors.New("encoder not found; please provide the fully qualified name of the message you are trying to encode")
	}
	return s.encodeFn(record)
}

// DecodeRecord will encode the given record using the internal decodeFn.
func (s *Serde) DecodeRecord(record []byte) ([]byte, error) {
	if s.decodeFn == nil {
		return nil, errors.New("decoder not found")
	}
	return s.decodeFn(record)
}
