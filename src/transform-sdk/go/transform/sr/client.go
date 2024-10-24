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

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"unsafe"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/cache"
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/rwbuf"
)

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
//	https://docs.redpanda.com/current/manage/schema-reg/schema-reg-api/#reference-a-schema
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

// SchemaRegistryClient is a client for interacting with the schema registry within Redpanda.
//
// The client provides caching out of the box, which can be configured with options.
type SchemaRegistryClient interface {
	// LookupSchemaById looks up a schema via it's global ID.
	LookupSchemaById(id int) (s *Schema, err error)
	// LookupSchemaByVersion looks up a schema via a subject for a specific version.
	//
	// Use version -1 to get the latest version.
	LookupSchemaByVersion(subject string, version int) (s *SubjectSchema, err error)
	// CreateSchema creates a schema under the given subject, returning the version and ID.
	//
	// If an equivalent schema already exists globally, that schema ID will be reused.
	// If an equivalent schema already exists within that subject, this will be a noop and the
	// existing schema will be returned.
	CreateSchema(subject string, schema Schema) (s *SubjectSchema, err error)
}

type (
	clientOpts struct {
		maxCacheSize int
	}
	// ClientOpt is an option to configure a SchemaRegistryClient
	ClientOpt     interface{ apply(*clientOpts) }
	clientOptFunc func(*clientOpts)
)

func (f clientOptFunc) apply(opts *clientOpts) {
	f(opts)
}

type subjectVersion struct {
	subject string
	version int
}

type (
	clientImpl        struct{}
	cachingClientImpl struct {
		underlying                  SchemaRegistryClient
		schemaByIdCache             cache.Cache[schemaId, *Schema]
		schemaBySubjectVersionCache cache.Cache[subjectVersion, *SubjectSchema]
	}
)

// MaxCacheEntries configures how many entries to cache within the client.
//
// By default the cache is unbounded, use 0 to disable the cache.
func MaxCacheEntries(size int) ClientOpt {
	return clientOptFunc(func(o *clientOpts) {
		o.maxCacheSize = size
	})
}

// NewClient creates a new SchemaRegistryClient with the specified options applied.
func NewClient(opts ...ClientOpt) (c SchemaRegistryClient) {
	o := clientOpts{
		maxCacheSize: math.MaxInt,
	}
	for _, opt := range opts {
		opt.apply(&o)
	}
	checkAbi()
	c = &clientImpl{}
	if o.maxCacheSize <= 0 {
		return c
	}
	return &cachingClientImpl{
		underlying:                  c,
		schemaByIdCache:             cache.New[schemaId, *Schema](o.maxCacheSize),
		schemaBySubjectVersionCache: cache.New[subjectVersion, *SubjectSchema](o.maxCacheSize),
	}
}

func (sr *cachingClientImpl) LookupSchemaById(id int) (s *Schema, err error) {
	cached, ok := sr.schemaByIdCache.Get(schemaId(id))
	if ok {
		return cached, nil
	}
	s, err = sr.underlying.LookupSchemaById(id)
	if err != nil {
		sr.schemaByIdCache.Put(schemaId(id), s)
	}
	return
}

func (sr *clientImpl) LookupSchemaById(id int) (*Schema, error) {
	var length int32
	errno := getSchemaDefinitionLen(schemaId(id), unsafe.Pointer(&length))
	if errno != 0 {
		return nil, errors.New("unable to find a schema definition with id " + strconv.Itoa(int(id)))
	}
	buf := rwbuf.New(int(length))
	result := getSchemaDefinition(
		schemaId(id),
		unsafe.Pointer(buf.WriterBufPtr()),
		int32(buf.WriterLen()),
	)
	if result < 0 {
		return nil, errors.New("unable to find a schema definition with id " + strconv.Itoa(int(id)))
	}
	buf.AdvanceWriter(int(result))
	schema, err := decodeSchemaDef(buf)
	if err != nil {
		return nil, err
	}
	return &schema, nil
}

func (sr *cachingClientImpl) LookupSchemaByVersion(subject string, version int) (s *SubjectSchema, err error) {
	cached, ok := sr.schemaBySubjectVersionCache.Get(subjectVersion{subject, version})
	if ok {
		return cached, nil
	}
	s, err = sr.underlying.LookupSchemaByVersion(subject, version)
	if err != nil {
		sr.schemaBySubjectVersionCache.Put(subjectVersion{subject, version}, s)
	}
	return
}

func (sr *clientImpl) LookupSchemaByVersion(subject string, version int) (s *SubjectSchema, err error) {
	var length int32
	errno := getSchemaSubjectLen(
		unsafe.Pointer(unsafe.StringData(subject)),
		int32(len(subject)),
		int32(version),
		unsafe.Pointer(&length),
	)
	if errno != 0 {
		return nil, errors.New("unable to find a schema " + subject + " with version " + strconv.Itoa(int(version)))
	}
	buf := rwbuf.New(int(length))
	result := getSchemaSubject(
		unsafe.Pointer(unsafe.StringData(subject)),
		int32(len(subject)),
		int32(version),
		unsafe.Pointer(buf.WriterBufPtr()),
		int32(buf.WriterLen()),
	)
	if result < 0 {
		return nil, errors.New("unable to find a schema " + subject + " and version " + strconv.Itoa(version))
	}
	buf.AdvanceWriter(int(result))
	schema, err := decodeSchema(subject, buf)
	if err != nil {
		return nil, err
	}
	return &schema, nil
}

func (sr *cachingClientImpl) CreateSchema(subject string, schema Schema) (s *SubjectSchema, err error) {
	s, err = sr.underlying.CreateSchema(subject, schema)
	if err == nil {
		sr.schemaBySubjectVersionCache.Put(subjectVersion{subject, s.Version}, s)
	}
	return
}

func (sr *clientImpl) CreateSchema(subject string, schema Schema) (s *SubjectSchema, err error) {
	buf := rwbuf.New(binary.MaxVarintLen32 + len(schema.Schema) + binary.MaxVarintLen32)
	encodeSchemaDef(buf, schema)
	b := buf.ReadAll()
	var id schemaId
	errno := createSubjectSchema(
		unsafe.Pointer(unsafe.StringData(subject)),
		int32(len(subject)),
		unsafe.Pointer(&b[0]),
		int32(len(b)),
		unsafe.Pointer(&id),
	)
	if errno != 0 {
		return nil, errors.New("unable to create new schema")
	}
	return &SubjectSchema{
		Subject: subject,
		ID:      int(id),
		Schema:  schema,
	}, nil
}
