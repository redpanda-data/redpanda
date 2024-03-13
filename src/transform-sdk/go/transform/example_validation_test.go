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

package transform_test

import (
	"encoding/json"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

// This example shows the basic usage of the package:
// This is a transform that validates the data is valid JSON,
// and outputs invalid JSON to a dead letter queue.
func Example_validation() {
	transform.OnRecordWritten(jsonValidate)
}

// This will be called for each record in the source topic.
func jsonValidate(e transform.WriteEvent, w transform.RecordWriter) error {
	if json.Valid(e.Record().Value) {
		// Write the valid records to the "default" output topic, this is the
		// first output topic specified in the configuration.
		return w.Write(e.Record())
	}
	// If a record does not contain valid JSON then route it to another topic for
	// triage and debugging.
	return w.Write(e.Record(), transform.ToTopic("invalid_json"))
}
