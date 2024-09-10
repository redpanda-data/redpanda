// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"os"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata/schema-registry/avro"
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/sr"
)

var (
	c         sr.SchemaRegistryClient
	s         sr.Serde[*avro.Example]
	topicName = "demo-topic"
)

func main() {
	c = sr.NewClient()
	e := avro.Example{}
	_, err := c.CreateSchema(topicName+"-value", sr.Schema{
		Type:   sr.TypeAvro,
		Schema: e.Schema(),
	})
	if err != nil {
		println("unable to register schema ", err)
	}
	transform.OnRecordWritten(avroToJsonTransform)
}

// This is an example transform that converts avro->json using the avro schema specified in schema registry.
func avroToJsonTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	v := e.Record().Value
	ex := avro.Example{}
	// Attempt to decode the value, if it's from a schema we don't know about then
	// look it up and then try to decode.
	err := s.Decode(v, &ex)
	if err == sr.ErrNotRegistered {
		id, err := sr.ExtractID(v)
		if err != nil {
			return err
		}
		schema := sr.Schema{
			Type:   sr.TypeAvro,
			Schema: ex.Schema(),
		}
		// Register the new schema
		s.Register(id, sr.DecodeFn[*avro.Example](func(b []byte, e *avro.Example) error {
			ex, err := avro.DeserializeExampleFromSchema(
				bytes.NewReader(b),
				schema.Schema,
			)
			*e = ex
			return err
		}), sr.ValueSubjectTopicName[*avro.Example](os.Getenv("REDPANDA_INPUT_TOPIC"), func(sn string) error {
			_, err = c.CreateSchema(sn, schema)
			return err
		}))
		// Now try and decode the value now that we've looked it up.
		if err = s.Decode(v, &ex); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	// Output the record as JSON.
	j, err := ex.MarshalJSON()
	if err != nil {
		return err
	}
	return w.Write(transform.Record{
		Key:   e.Record().Key,
		Value: j,
	})
}
