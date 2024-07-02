// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package serde

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
)

const allTypesSchema = `{
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "id": { "type": ["string", "number"] },
    "telephone": { "type": "integer" },
    "member": { "type": "boolean" },
    "apartments": { "type": "array" },
    "tags": { "type": "object" },
    "friends": { "type": "null" }
  },
  "required": ["name"]
}`

const nestedSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/product.schema.json",
  "title": "Product",
  "description": "A product from Acme's catalog",
  "type": "object",
  "properties": {
    "productId": {
      "description": "The unique identifier for a product",
      "type": "integer"
    },
    "productName": {
      "description": "Name of the product",
      "type": "string"
    },
    "price": {
      "description": "The price of the product",
      "type": "number",
      "exclusiveMinimum": 0
    },
    "tags": {
      "description": "Tags for the product",
      "type": "array",
      "items": {
        "type": "string"
      },
      "minItems": 1,
      "uniqueItems": true
    },
    "dimensions": {
      "type": "object",
      "properties": {
        "length": {
          "type": "number"
        },
        "width": {
          "type": "number"
        },
        "height": {
          "type": "number"
        }
      },
      "required": [ "length", "width", "height" ]
    }
  },
  "required": [ "productId", "productName", "price" ]
}`

func Test_encodeDecodeJsonSchemaNoReferences(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		schemaID  int
		record    string
		expErr    bool // error building the Serde.
		expEncErr bool // error encoding the record.
	}{
		{
			name:     "Valid schema, record with all fields",
			schema:   allTypesSchema,
			schemaID: 2206,
			record: `{
  "name":"foo",
  "id": 20.20,
  "telephone": 12322212,
  "member": true,
  "apartments": [906, 2206, "test"],
  "tags": { "random": "value" }
}`,
		},
		{
			name:     "Valid schema, record with minimum fields",
			schema:   allTypesSchema,
			schemaID: 94,
			record:   `{"name":"foo"}`,
		},
		{
			name:     "Valid schema, record with extra fields",
			schema:   allTypesSchema,
			schemaID: 11,
			record:   `{"name":"foo", "extra":"yes"}`,
		},
		{
			name:     "Nested schema", // Example from jsonschema docs
			schema:   nestedSchema,
			schemaID: 123,
			record: `{
  "productId": 12345,
  "productName": "Acme Widget",
  "price": 13.23,
  "tags": ["electronics", "gadgets", "widgets"],
  "dimensions": {
    "length": 10.5,
    "width": 5.0,
    "height": 3.25
  }
}`,
		},
		{
			name:     "Valid schema, empty json",
			schema:   `{"type": "object","properties":{"name":{"type":"string"}}}`,
			schemaID: 12,
			record:   `{}`,
		},
		{
			name: "Valid schema, with no additionalProperties",
			schema: `{
  "type": "object",
  "properties": {
    "name": { "type": "string" }
  },
  "additionalProperties": false
}`,
			schemaID:  8,
			record:    `{"name":"foo", "bar":"baz"}`,
			expEncErr: true,
		},
		{
			name:     "Error in schema",
			schema:   `{"type": "object","properties":{"name":{"tpe":"string"}}`, // missing }, typo.
			schemaID: 12,
			record:   `{"name":"foo"}`,
			expErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noopCl, _ := sr.NewClient()
			schema := sr.Schema{
				Schema: tt.schema,
				Type:   sr.TypeJSON,
			}
			serde, err := NewSerde(context.Background(), noopCl, &schema, tt.schemaID, "")
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			got, err := serde.EncodeRecord([]byte(tt.record))
			if tt.expEncErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Validate Magic Byte.
			require.Equal(t, uint8(0), got[0])

			var serdeHeader sr.ConfluentHeader
			id, encRecord, err := serdeHeader.DecodeID(got)
			require.NoError(t, err)
			require.Equal(t, tt.schemaID, id)
			if len(got) == 5 {
				return // It's an empty record.
			}

			// Validate encoded record. Decode and compare to original:
			gotDecoded, err := serde.DecodeRecord(encRecord)
			if err != nil {
				return
			}
			require.NoError(t, err)

			// To avoid any mismatch in the decode order of the elements, we
			// better unmarshal and compare the unmarshaled records.
			var gotU, expU map[string]any
			err = json.Unmarshal(gotDecoded, &gotU)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(tt.record), &expU)
			require.NoError(t, err)

			require.Equal(t, expU, gotU)
		})
	}
}

func Test_encodeDecodeJsonsRecordWithReferences(t *testing.T) {
	tests := []struct {
		name     string
		schema   *sr.Schema
		schemaID int
		record   string
	}{
		{
			name:     "single reference + nested",
			schemaID: 123,
			record: `{
  "productId": 123,
  "productName": "redpanda plush", 
  "tags": ["foo", "bar"],
  "dimensions": {
    "length": 10.5,
    "width": 5.0,
    "height": 3.25
  },
  "warehouseLocation": {
    "latitude": 37.2795481,
    "longitude": 127.047077
  }
}`,
			schema: &sr.Schema{
				Schema: `{
   "type":"object",
   "properties":{
      "productId":{
         "type":"integer"
      },
      "productName":{
         "type":"string"
      },
      "tags":{
         "type":"array",
         "items":{
            "type":"string"
         }
      },
      "dimensions":{
         "type":"object",
         "properties":{
            "length":{
               "type":"number"
            },
            "width":{
               "type":"number"
            },
            "height":{
               "type":"number"
            }
         }
      },
      "warehouseLocation":{
         "$ref":"https://example.com/geographical-location.schema.json"
      }
   }
}`,
				References: []sr.SchemaReference{
					{
						Name:    "https://example.com/geographical-location.schema.json",
						Subject: "single",
						Version: 0,
					},
				},
			},
		},
		{
			name:     "multiple single references",
			schemaID: 123,
			record: `{
  "productId": 123,
  "productName": "redpanda plush", 
  "warehouseLocation": {
    "latitude": 37.2795481,
    "longitude": 127.047077
  },
  "colors": {
    "available": ["red", "orange"]
  }
}`,
			schema: &sr.Schema{
				Schema: `{
   "type":"object",
   "properties":{
      "productId":{
         "type":"integer"
      },
      "productName":{
         "type":"string"
      },
      "warehouseLocation":{
         "$ref":"https://example.com/geographical-location.schema.json"
      },
      "colors":{
          "$ref":"file:///tmp/colors.json"
      }
   }
}`,
				References: []sr.SchemaReference{
					{
						Name:    "https://example.com/geographical-location.schema.json",
						Subject: "single",
						Version: 0,
					},
					{
						Name:    "file:///tmp/colors.json",
						Subject: "single",
						Version: 1,
					},
				},
			},
		},
		{
			name:     "multiple single references",
			schemaID: 123,
			record: `{
  "productId": 123,
  "productName": "redpanda plush", 
  "warehouseLocation": {
    "coordinates": {
      "latitude": 37.2795481,
      "longitude": 127.047077
    }
  }
}`,
			schema: &sr.Schema{
				Schema: `{
   "type":"object",
   "properties":{
      "productId":{
         "type":"integer"
      },
      "productName":{
         "type":"string"
      },
      "warehouseLocation":{
         "$ref":"nested.json"
      }
   }
}`,
				References: []sr.SchemaReference{
					{
						Name:    "nested.json",
						Subject: "nested",
						Version: 2,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(jsonReferenceHandler())
			defer ts.Close()

			fs := afero.NewMemMapFs()
			params := &config.Params{ConfigFlag: "/some/path/redpanda.yaml"}
			p, err := params.LoadVirtualProfile(fs)
			require.NoError(t, err)

			p.SR.Addresses = []string{ts.URL}

			client, err := schemaregistry.NewClient(fs, p)
			require.NoError(t, err)

			tt.schema.Type = sr.TypeJSON
			serde, err := NewSerde(context.Background(), client, tt.schema, tt.schemaID, "")
			require.NoError(t, err)

			got, err := serde.EncodeRecord([]byte(tt.record))
			require.NoError(t, err)

			// Validate Magic Byte.
			require.Equal(t, uint8(0), got[0])

			var serdeHeader sr.ConfluentHeader
			id, encRecord, err := serdeHeader.DecodeID(got)
			require.NoError(t, err)
			require.Equal(t, tt.schemaID, id)
			if len(got) == 5 {
				return // It's an empty record.
			}

			// Validate encoded record. Decode and compare to original:
			gotDecoded, err := serde.DecodeRecord(encRecord)
			require.NoError(t, err)

			// To avoid any mismatch in the decode order of the elements, we
			// better unmarshal and compare the unmarshaled records.
			var gotU, expU map[string]any
			err = json.Unmarshal(gotDecoded, &gotU)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(tt.record), &expU)
			require.NoError(t, err)

			require.Equal(t, expU, gotU)
		})
	}
}

var jsonschemaReferenceMap = map[string]string{
	"single-0": `{"schema":"{\"type\":\"object\",\"properties\":{\"latitude\":{\"type\":\"number\"},\"longitude\":{\"type\":\"number\"}}}"}`,
	"single-1": `{"schema":"{\"type\":\"object\",\"properties\":{\"available\":{\"type\":\"array\"}}}"}`,
	"nested-2": `{"references":[{"name":"https://example.com/geographical-location.schema.json","subject":"single","version":0}],"schema":"{\"type\":\"object\",\"properties\":{\"coordinates\":{\"$ref\":\"https://example.com/geographical-location.schema.json\"}}}"}`,
}

func jsonReferenceHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Split the URL path by '/' to get individual path segments
		segments := strings.Split(r.URL.Path, "/")

		// /subjects/{subject}/versions/{version}"
		if len(segments) >= 4 && segments[1] == "subjects" && segments[3] == "versions" {
			subject := segments[2]
			version := segments[4]
			id := subject + "-" + version

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(jsonschemaReferenceMap[id]))
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}
