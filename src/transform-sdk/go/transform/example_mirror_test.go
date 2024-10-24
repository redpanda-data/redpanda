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
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

// This example shows the basic usage of the package:
// This is a "transform" that does nothing but copies the same data to an new
// topic.
func Example_identityTransform() {
	// Other setup can happen here, such as setting up lookup tables,
	// initializing reusable buffers, reading environment variables, etc.

	// Make sure to register your callback so Redpanda knows which
	// function to invoke when records are written
	transform.OnRecordWritten(mirrorTransform)
}

// This will be called for each record in the source topic.
//
// The records written to w be output to the destination topic.
func mirrorTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	return w.Write(e.Record())
}
