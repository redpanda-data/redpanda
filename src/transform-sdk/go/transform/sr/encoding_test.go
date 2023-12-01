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

package sr

import (
	"reflect"
	"testing"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/rwbuf"
)

func TestSchemaRoundtrip(t *testing.T) {
	s := Schema{
		Type: TypeAvro,
		Schema: `
		{
                  "type": "record",
                  "namespace": "com.example.school",
                  "name": "Student",
                  "fields": [
                    {
                      "name": "Name",
                      "type": "string"
                    },
                    {
                      "name": "Age",
                      "type": "int"
                    },
                    {
                      "name": "Address",
                      "type": "com.example.school.Address"
                    }
                  ]
                }`,
		References: []Reference{
			{
				Name:    "com.example.school.Address",
				Subject: "Address",
				Version: 1,
			},
		},
	}
	b := rwbuf.New(0)
	encodeSchemaDef(b, s)
	roundtripped, err := decodeSchemaDef(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundtripped, s) {
		t.Errorf("%#v != %#v", roundtripped, s)
	}
}
