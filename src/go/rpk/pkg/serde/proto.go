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

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// inMemFileName is an arbitrary name given to the in-memory proto file
// generated while compiling the schema.
const inMemFileName = "tmp.proto"

// newAvroEncoder will generate a serializer function using the specified
// schema. It utilizes the message type identified by protoFQN. If the schema
// includes references, it retrieves them using the supplied client. The
// generated function returns the record encoded in the protobuf wire format.
func newProtoEncoder(compiledFiles linker.Files, protoFQN string, schemaID int) (serdeFunc, error) {
	// Create proto message with runtime information. //
	descriptor := compiledFiles.FindFileByPath(inMemFileName).FindDescriptorByName(protoreflect.FullName(protoFQN))
	if descriptor == nil {
		return nil, fmt.Errorf("unable to find message with name %q", protoFQN)
	}
	msgDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unable to process message with name %q", protoFQN)
	}
	message := dynamicpb.NewMessage(msgDescriptor)

	o := protojson.UnmarshalOptions{
		Resolver: compiledFiles.AsResolver(),
	}
	return func(record []byte) ([]byte, error) {
		// Unmarshal the record into the proto message. //
		err := o.Unmarshal(record, message)
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

// newProtoDecoder will generate a deserializer function that decodes a record
// with the given schema. The generated function expects the record to contain
// the index of the message (schema ID should already be stripped away) in the
// first bytes of the record.
func newProtoDecoder(compiledFiles linker.Files) (serdeFunc, error) {
	descriptors := compiledFiles.FindFileByPath(inMemFileName).Messages()

	return func(record []byte) ([]byte, error) {
		var serdeHeader sr.ConfluentHeader
		protoIndex, toDecode, err := serdeHeader.DecodeIndex(record, 0)
		if err != nil {
			return nil, errors.New("unable to decode index from the protobuf record")
		}
		if len(protoIndex) == 0 {
			return nil, errors.New("encoded message does not contain the protobuf index")
		}
		msgDescriptor := messageDescriptorFromIndex(descriptors, protoIndex)

		message := dynamicpb.NewMessage(msgDescriptor)
		err = proto.Unmarshal(toDecode, message)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall protobuf encoded record into the message with index %v: %v", protoIndex, err)
		}

		// Unmarshal the record into the proto message. //
		o := protojson.MarshalOptions{
			Resolver: compiledFiles.AsResolver(),
		}
		jsonBytes, err := o.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal protobuf message as JSON: %v", err)
		}
		return jsonBytes, nil
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

// messageDescriptorFromIndex recursively finds the inner message descriptor
// based on the protoIndex. Initial protoIndex must be a non-empty slice.
func messageDescriptorFromIndex(msgs protoreflect.MessageDescriptors, protoIndex []int) protoreflect.MessageDescriptor {
	descriptor := msgs.Get(protoIndex[0])
	if len(protoIndex) > 1 {
		descriptor = messageDescriptorFromIndex(descriptor.Messages(), protoIndex[1:])
	}
	return descriptor
}

func compileSchema(ctx context.Context, cl *sr.Client, schema *sr.Schema) (linker.Files, error) {
	// Compile the Schema Registry proto. //
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
	return compiled, nil
}
