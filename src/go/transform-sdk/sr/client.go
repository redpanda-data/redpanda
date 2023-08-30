// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sr

// schemaId is an ID of a schema registered with schema registry
type schemaId int32

// SchemaType is an enum for the different types of schemas that can be stored in schema registry.
type SchemaType int

const (
	TypeAvro SchemaType = iota
	TypeProtobuf
	TypeJSON
)

// SchemaReference is a way for a one schema to reference another. The details
// for how referencing is done are type specific; for example, JSON objects
// that use the key "$ref" can refer to another schema via URL. For more details
// on references, see the following link:
//
//	https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#schema-references
//	https://docs.confluent.io/platform/current/schema-registry/develop/api.html
type Reference struct {
	Name    string
	Subject string
	Version int
}

// Schema is a schema that can be registered within schema registry
type Schema struct {
	Schema     string
	Type       SchemaType
	References []Reference
}

// SchemaSubject is a schema along with the subject, version and ID of the schema
type SubjectSchema struct {
	Schema

	Subject string
	Version int
	ID      int
}
