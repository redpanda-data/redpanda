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

	"github.com/bufbuild/protocompile"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// newAvroEncoder will generate a serializer function using the specified
// schema. It utilizes the message type identified by protoFQN. If the schema
// includes references, it retrieves them using the supplied client. The
// generated function returns the record encoded in the protobuf wire format.
func newProtoEncoder(ctx context.Context, cl *sr.Client, schema *sr.Schema, protoFQN string, schemaID int) (serdeFunc, error) {
	if protoFQN == "" {
		return nil, fmt.Errorf("please provide the protobuf message name")
	}
	// Compile the Schema Registry proto. //
	const inMemFileName = "tmp.proto"
	accessorMap := make(map[string]string)
	// This is the original schema.
	accessorMap[inMemFileName] = schema.Schema

	// And we add the rest of schemas if we have references.
	if len(schema.References) > 0 {
		err := mapReferences(ctx, cl, schema, accessorMap)
		if err != nil {
			return nil, fmt.Errorf("unable to map references: %v", err)
		}
	}

	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(accessorMap),
		},
		SourceInfoMode: protocompile.SourceInfoStandard,
	}
	compiled, err := compiler.Compile(ctx, inMemFileName)
	if err != nil {
		return nil, fmt.Errorf("unable to compile the given schema: %v", err)
	}

	// Create proto message with runtime information. //
	d := compiled.FindFileByPath(inMemFileName).FindDescriptorByName(protoreflect.FullName(protoFQN))
	if d == nil {
		return nil, fmt.Errorf("unable to find message with name %q", protoFQN)
	}
	msgDescriptor, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unable to process message with name %q", protoFQN)
	}
	message := dynamicpb.NewMessage(msgDescriptor)

	o := protojson.UnmarshalOptions{
		Resolver: compiled.AsResolver(),
	}
	return func(record []byte) ([]byte, error) {
		// Unmarshal the record into the proto message. //
		err = o.Unmarshal(record, message)
		if err != nil {
			return nil, fmt.Errorf("unable to decode the record into the given schema: %v", err)
		}

		// Encode the proto message to protobuf wire format. //
		binary, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("unable to encode the record: %v", err)
		}

		// Add schema registry wire format header. //
		var serdeHeader sr.ConfluentHeader
		h, _ := serdeHeader.AppendEncode(nil, schemaID, messageIndex(msgDescriptor))
		return append(h, binary...), nil
	}, nil
}

// messageIndex traverse the message descriptor to get the list of
// message-indexes which is needed in the wire format.
func messageIndex(d protoreflect.MessageDescriptor) []int {
	idx := []int{d.Index()}
	if parent := d.Parent(); parent != nil {
		pmsg, ok := parent.(protoreflect.MessageDescriptor)
		// If !ok, we have reached the top, and it's a FileDescriptor.
		if !ok {
			return idx
		}
		return append(messageIndex(pmsg), idx...)
	}
	return idx
}

// mapReferences recursively inserts the schema references into the given
// refMap. It uses the client to retrieve the schema references.
func mapReferences(ctx context.Context, cl *sr.Client, schema *sr.Schema, refMap map[string]string) error {
	if len(schema.References) == 0 {
		return nil
	}
	for _, ref := range schema.References {
		r, err := cl.SchemaByVersion(ctx, ref.Subject, ref.Version)
		if err != nil {
			return fmt.Errorf("unable to get reference schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
		}
		refMap[ref.Name] = r.Schema.Schema
		refSchema := r.Schema
		err = mapReferences(ctx, cl, &refSchema, refMap)
		if err != nil {
			return fmt.Errorf("unable to map schema with subject %q and version %v: %v", ref.Subject, ref.Version, err)
		}
	}
	return nil
}
