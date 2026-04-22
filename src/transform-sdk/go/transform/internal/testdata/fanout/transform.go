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
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

func main() {
	transform.OnRecordWritten(fanoutTransform)
}

// fanoutTransform writes each record to the default output (input topic
// in produce-path mode) AND to all declared output topics.
func fanoutTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	rec := e.Record()
	// Write to default (input topic in produce-path mode)
	if err := w.Write(rec); err != nil {
		return err
	}
	// Also write to each declared output topic
	for i := 0; i < 8; i++ {
		topic, ok := os.LookupEnv(fmt.Sprintf("REDPANDA_OUTPUT_TOPIC_%d", i))
		if !ok {
			break
		}
		if err := w.Write(rec, transform.ToTopic(topic)); err != nil {
			return err
		}
	}
	return nil
}
