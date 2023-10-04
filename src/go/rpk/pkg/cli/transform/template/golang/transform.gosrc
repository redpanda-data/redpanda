package main

import (
	"github.com/redpanda-data/redpanda/src/go/transform-sdk"
)

func main() {
        // Register your transform function. 
        // This is a good place to perform other setup too.
	redpanda.OnRecordWritten(doTransform)
}

// doTransform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
func doTransform(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	return []redpanda.Record{e.Record()}, nil
}
