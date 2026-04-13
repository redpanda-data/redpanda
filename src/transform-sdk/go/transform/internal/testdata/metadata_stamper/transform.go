// Copyright 2026 Redpanda Data, Inc.
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
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

func main() {
	transform.OnRecordWritten(metadataStamper)
}

// metadataStamper reads the principal name from batch metadata and
// stamps it as a record header.
func metadataStamper(e transform.WriteEvent, w transform.RecordWriter) error {
	rec := e.Record()
	principal := e.Metadata("principal_name")
	rec.Headers = append(rec.Headers, transform.RecordHeader{
		Key:   []byte("principal"),
		Value: []byte(principal),
	})
	return w.Write(rec)
}
