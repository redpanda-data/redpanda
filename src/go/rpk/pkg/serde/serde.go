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
}

// NewSerde will build a de/serializer based on the schema type. For Protobuf
// schemas, it will use the FQN to find the message type. Each encoded record
// will contain the schema registry wire format bytes (Magic number, Schema ID
// and Index for Proto).
func NewSerde(ctx context.Context, cl *sr.Client, schema *sr.Schema, schemaID int, protoFQN string) (*Serde, error) {
	switch schema.Type {
	case sr.TypeAvro:
		encFn, err := newAvroEncoder(ctx, cl, schema, schemaID)
		if err != nil {
			return nil, fmt.Errorf("unable to build avro encoder: %v", err)
		}
		return &Serde{encFn}, nil
	case sr.TypeProtobuf:
		encFn, err := newProtoEncoder(ctx, cl, schema, protoFQN, schemaID)
		if err != nil {
			return nil, fmt.Errorf("unable to build protobuf encoder: %v", err)
		}
		return &Serde{encFn}, nil
	default:
		return nil, fmt.Errorf("schema with ID %v contains an unsupported schema type %v", schemaID, schema.Type)
	}
}

// EncodeRecord will encode the given record using the internal encodeFn.
func (s *Serde) EncodeRecord(record []byte) ([]byte, error) {
	if s.encodeFn == nil {
		return nil, errors.New("encoder not found")
	}
	return s.encodeFn(record)
}
