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

package redpanda_test

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/transform-sdk"
)

// This example shows a transform that converts CSV into JSON.
func Example_transcoding() {
	redpanda.OnRecordWritten(csvToJsonTransform)
}

type Foo struct {
	A string `json:"a"`
	B int    `json:"b"`
}

func csvToJsonTransform(e redpanda.WriteEvent) ([]redpanda.Record, error) {
	// The input data is a CSV (without a header row) that is the structure of:
	// key, a, b
	// This transform emits each row in that CSV as JSON.
	reader := csv.NewReader(bytes.NewReader(e.Record().Value))
	// Improve performance by reusing the result slice.
	reader.ReuseRecord = true
	output := []redpanda.Record{}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if len(row) != 3 {
			return nil, errors.New("unexpected number of rows")
		}
		// Convert the last column into an int
		b, err := strconv.Atoi(row[2])
		if err != nil {
			return nil, err
		}
		// Marshal our JSON value
		f := Foo{
			A: row[1],
			B: b,
		}
		v, err := json.Marshal(&f)
		if err != nil {
			return nil, err
		}
		// Add our output record using the first column as the key.
		output = append(output, redpanda.Record{
			Key:   []byte(row[0]),
			Value: v,
		})

	}
	return output, nil
}
