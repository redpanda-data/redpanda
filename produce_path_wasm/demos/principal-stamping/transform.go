package main

import (
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

func main() {
	transform.OnRecordWritten(metadataStamper)
}

// metadataStamper reads the principal name from batch metadata and
// stamps it as a record header before writing the record through.
func metadataStamper(e transform.WriteEvent, w transform.RecordWriter) error {
	rec := e.Record()
	principal := e.Metadata("principal_name")
	rec.Headers = append(rec.Headers, transform.RecordHeader{
		Key:   []byte("principal"),
		Value: []byte(principal),
	})
	return w.Write(rec)
}
