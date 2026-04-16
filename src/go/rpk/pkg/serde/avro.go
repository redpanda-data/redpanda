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
	"fmt"

	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
)

// newAvroEncoder generates a serializer function that can encode the provided
// record using the specified schema. The generated function returns the record
// encoded in the Confluent Schema Registry wire format.
func newAvroEncoder(schema *avro.Schema, schemaID int) (serdeFunc, error) {
	return func(record []byte) ([]byte, error) {
		var native any
		if err := schema.DecodeJSON(record, &native); err != nil {
			return nil, fmt.Errorf("unable to parse record with the provided schema: %v", err)
		}

		binary, err := schema.Encode(native)
		if err != nil {
			return nil, fmt.Errorf("unable to binary encode the record: %v", err)
		}

		// Append the magic byte + the schema ID bytes.
		var serdeHeader sr.ConfluentHeader
		h, err := serdeHeader.AppendEncode(nil, schemaID, nil)
		if err != nil {
			return nil, fmt.Errorf("unable to append header: %v", err)
		}
		return append(h, binary...), nil
	}, nil
}

// newAvroDecoder generates a deserializer function that decodes the given
// avro-encoded record according to the schema. The generated function expects
// the record bytes (without the wire format).
func newAvroDecoder(schema *avro.Schema) (serdeFunc, error) {
	return func(record []byte) ([]byte, error) {
		var native any
		if _, err := schema.Decode(record, &native); err != nil {
			return nil, fmt.Errorf("unable to decode avro-encoded record: %v", err)
		}
		return schema.EncodeJSON(native)
	}, nil
}

// generateAvroSchema parses the schema and its references, returning a
// compiled schema that can be used for encoding and decoding.
func generateAvroSchema(ctx context.Context, cl *sr.Client, schema *sr.Schema) (*avro.Schema, error) {
	if len(schema.References) == 0 {
		s, err := avro.Parse(schema.Schema)
		if err != nil {
			return nil, fmt.Errorf("unable to parse schema: %v", err)
		}
		return s, nil
	}
	cache := &avro.SchemaCache{}
	seen := make(map[string]bool)
	if err := parseAvroReferences(ctx, cl, cache, schema, seen); err != nil {
		return nil, fmt.Errorf("unable to parse references: %v", err)
	}
	s, err := cache.Parse(schema.Schema)
	if err != nil {
		return nil, fmt.Errorf("unable to parse schema: %v", err)
	}
	return s, nil
}

// parseAvroReferences recursively parses all schema references into the cache
// so they are available when parsing the parent schema. The seen map tracks
// already-fetched subject+version pairs to avoid redundant registry requests.
func parseAvroReferences(ctx context.Context, cl *sr.Client, cache *avro.SchemaCache, schema *sr.Schema, seen map[string]bool) error {
	for _, ref := range schema.References {
		key := fmt.Sprintf("%s-%d", ref.Subject, ref.Version)
		if seen[key] {
			continue
		}
		seen[key] = true
		r, err := cl.SchemaByVersion(ctx, ref.Subject, ref.Version)
		if err != nil {
			return fmt.Errorf("unable to get reference schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
		}
		refSchema := r.Schema
		if len(refSchema.References) > 0 {
			if err := parseAvroReferences(ctx, cl, cache, &refSchema, seen); err != nil {
				return fmt.Errorf("unable to parse schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
			}
		}
		if _, err := cache.Parse(refSchema.Schema); err != nil {
			return fmt.Errorf("unable to parse schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
		}
	}
	return nil
}
