// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package serde

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/twmb/franz-go/pkg/sr"
)

func compileJsonschema(ctx context.Context, cl *sr.Client, schema *sr.Schema) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	err := parseJsonschemaReferences(ctx, cl, schema, c)
	if err != nil {
		return nil, fmt.Errorf("unable to parse json schema references: %v", err)
	}
	sch, err := jsonschema.UnmarshalJSON(strings.NewReader(schema.Schema))
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal base schema: %v", err)
	}
	baseFileName := "redpanda_jsonschema.json"
	err = c.AddResource(baseFileName, sch)
	if err != nil {
		return nil, fmt.Errorf("unable to add base json schema to the compiler resource: %v", err)
	}
	return c.Compile(baseFileName)
}

func newJsonschemaEncoder(schemaID int, jSchema *jsonschema.Schema) (serdeFunc, error) {
	return func(record []byte) ([]byte, error) {
		sch, err := jsonschema.UnmarshalJSON(bytes.NewReader(record))
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal record: %v", err)
		}
		err = jSchema.Validate(sch)
		if err != nil {
			return nil, fmt.Errorf("unable to validate json schema: %v", err)
		}
		// Append the magic byte + the schema ID bytes.
		var serdeHeader sr.ConfluentHeader
		h, err := serdeHeader.AppendEncode(nil, schemaID, nil)
		if err != nil {
			return nil, fmt.Errorf("unable to append header: %v", err)
		}
		return append(h, record...), nil
	}, nil
}

func newJsonschemaDecoder(jSchema *jsonschema.Schema) (serdeFunc, error) {
	return func(record []byte) ([]byte, error) {
		var recordDec any
		err := json.Unmarshal(record, &recordDec)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal record: %v", err)
		}
		err = jSchema.Validate(recordDec)
		if err != nil {
			return nil, fmt.Errorf("unable to validate record: %v", err)
		}
		return record, nil
	}, nil
}

func parseJsonschemaReferences(ctx context.Context, cl *sr.Client, schema *sr.Schema, compiler *jsonschema.Compiler) error {
	for _, ref := range schema.References {
		r, err := cl.SchemaByVersion(ctx, ref.Subject, ref.Version)
		if err != nil {
			return fmt.Errorf("unable to get schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
		}
		refSchema := r.Schema

		sch, err := jsonschema.UnmarshalJSON(strings.NewReader(refSchema.Schema))
		if err != nil {
			return fmt.Errorf("unable to unmarshal json schema %q: %v", ref.Name, err)
		}

		err = compiler.AddResource(ref.Name, sch)
		if err != nil {
			return fmt.Errorf("unable to add schema %v: %v", ref.Name, err)
		}

		err = parseJsonschemaReferences(ctx, cl, &refSchema, compiler)
		if err != nil {
			return fmt.Errorf("unable to parse schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
		}
	}
	return nil
}
