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
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

// This example shows a transform that converts CSV into JSON.
func Example_transcoding() {
	transform.OnRecordWritten(csvToJsonTransform)
}

type Foo struct {
	A string `json:"a"`
	B int    `json:"b"`
}

func csvToJsonTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	// The input data is a CSV (without a header row) that is the structure of:
	// key, a, b
	// This transform emits each row in that CSV as JSON.
	reader := csv.NewReader(bytes.NewReader(e.Record().Value))
	// Improve performance by reusing the result slice.
	reader.ReuseRecord = true
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if len(row) != 3 {
			return errors.New("unexpected number of rows")
		}
		// Convert the last column into an int
		b, err := strconv.Atoi(row[2])
		if err != nil {
			return err
		}
		// Marshal our JSON value
		f := Foo{
			A: row[1],
			B: b,
		}
		v, err := json.Marshal(&f)
		if err != nil {
			return err
		}
		// Add our output record using the first column as the key.
		r := transform.Record{
			Key:   []byte(row[0]),
			Value: v,
		}
		if err := w.Write(r); err != nil {
			return err
		}
	}
	return nil
}
