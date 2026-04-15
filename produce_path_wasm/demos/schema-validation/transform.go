package main

import (
	"bytes"
	"fmt"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/sr"
	"schema-validator/avro"
)

var (
	client sr.SchemaRegistryClient
	serde  sr.Serde[*avro.Example]
)

func main() {
	client = sr.NewClient()
	transform.OnRecordWritten(validateSchema)
}

// validateSchema extracts the schema ID from the Confluent wire
// format, looks up the schema from the registry, and tries to
// deserialize the record value as an Avro Example. If deserialization
// fails, the produce is rejected -- the record is never written.
func validateSchema(e transform.WriteEvent, w transform.RecordWriter) error {
	value := e.Record().Value

	// Try to decode with known schemas first.
	ex := avro.Example{}
	err := serde.Decode(value, &ex)
	if err == sr.ErrNotRegistered {
		// Unknown schema ID -- look it up and register the decoder.
		id, extractErr := sr.ExtractID(value)
		if extractErr != nil {
			return fmt.Errorf("invalid record: %v", extractErr)
		}
		schema, lookupErr := client.LookupSchemaById(id)
		if lookupErr != nil {
			return fmt.Errorf("unknown schema id %d: %v", id, lookupErr)
		}
		serde.Register(
			id,
			sr.DecodeFn[*avro.Example](func(b []byte, e *avro.Example) error {
				decoded, err := avro.DeserializeExampleFromSchema(
					bytes.NewReader(b), schema.Schema)
				*e = decoded
				return err
			}),
		)
		// Retry with the newly registered decoder.
		err = serde.Decode(value, &ex)
	}
	if err != nil {
		return fmt.Errorf("schema validation failed: %v", err)
	}

	// Valid record -- pass through unchanged.
	return w.Write(e.Record())
}
