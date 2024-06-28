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

	"github.com/hamba/avro/v2"
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"
)

// newAvroEncoder generates a serializer function that can encode the provided
// record using the specified schema. If the schema includes references, it
// retrieves them using the supplied client. The generated function returns the
// record encoded in the protobuf wire format.
func newAvroEncoder(codec *goavro.Codec, schemaID int) (serdeFunc, error) {
	return func(record []byte) ([]byte, error) {
		native, _, err := codec.NativeFromTextual(record)
		if err != nil {
			return nil, fmt.Errorf("unable to parse record with the provided schema: %v", err)
		}

		binary, err := codec.BinaryFromNative(nil, native)
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
func newAvroDecoder(codec *goavro.Codec) (serdeFunc, error) {
	return func(record []byte) ([]byte, error) {
		native, _, err := codec.NativeFromBinary(record)
		if err != nil {
			return nil, fmt.Errorf("unable to decode avro-encoded record: %v", err)
		}

		return codec.TextualFromNative(nil, native)
	}, nil
}

// generateAvroCodec will generate an AVRO codec, parsing the references if
// there are any.
func generateAvroCodec(ctx context.Context, cl *sr.Client, schema *sr.Schema) (*goavro.Codec, error) {
	schemaStr := schema.Schema
	if len(schema.References) > 0 {
		err := parseAvroReferences(ctx, cl, schema)
		if err != nil {
			return nil, fmt.Errorf("unable to parse references: %v", err)
		}

		// We use hamba/avro to for the schema reference resolution.
		refCodec, err := avro.Parse(schema.Schema)
		if err != nil {
			return nil, fmt.Errorf("unable to parse schema: %v", err)
		}
		schemaStr = refCodec.String()
	}
	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("unable to generate codec for the given schema: %v", err)
	}
	return codec, nil
}

// parseAvroReferences uses hamba/avro Parse method to parse every reference. We
// don't need to store the references since the library already cache these
// schemas and use it later for handling references in the parent schema.
func parseAvroReferences(ctx context.Context, cl *sr.Client, schema *sr.Schema) error {
	if len(schema.References) == 0 {
		_, err := avro.Parse(schema.Schema)
		if err != nil {
			return err
		}
		return nil
	}
	for _, ref := range schema.References {
		r, err := cl.SchemaByVersion(ctx, ref.Subject, ref.Version)
		if err != nil {
			return err
		}
		refSchema := r.Schema
		err = parseAvroReferences(ctx, cl, &refSchema)
		if err != nil {
			return fmt.Errorf("unable to parse schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
		}
	}
	_, err := avro.Parse(schema.Schema)
	if err != nil {
		return err
	}
	return nil
}
