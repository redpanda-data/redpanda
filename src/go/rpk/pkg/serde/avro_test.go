// Copyright 2023 Redpanda Data, Inc.
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

func Test_encodeDecodeAvroRecordNoReferences(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		schemaID  int
		record    string
		expRecord string
		expErr    bool // error building the Serde.
		expEncErr bool // error encoding the record.
	}{
		{
			name: "Valid record and schema",
			schema: `
{
  "type":"record",
  "name":"test",
  "fields":
    [{
      "name":"name",
      "type":"string"
    }]
}`,
			schemaID:  906,
			record:    `{"name":"redpanda"}`,
			expRecord: `{"name":"redpanda"}`,
		}, {
			name: "Valid nested record and schema",
			schema: `
{
   "type":"record",
   "name":"test",
   "fields":[
      {
         "name":"name",
         "type":"string"
      },
      {
         "name":"complex",
         "type":{
            "type":"record",
            "name":"nestedSchemaName",
            "fields":[
               {
                  "name":"list",
                  "type":{"type":"array", "items":"int"}
               }
            ]
         }
      }
   ]
}`,
			schemaID:  906,
			record:    `{"name":"redpanda","complex":{"list":[1,2,3,4]}}`,
			expRecord: `{"name":"redpanda","complex":{"list":[1,2,3,4]}}`,
		}, {
			name: "Valid empty record with default null in schema",
			schema: `
{
  "type":"record",
  "name":"test",
  "fields":
    [{
      "name" :"name",
      "type" :["null"],
      "default":null
    }]
}`,
			schemaID:  1,
			record:    "{}",
			expRecord: `{"name":null}`,
		}, {
			name: "Invalid record for a valid schema",
			schema: `
{
  "type":"record",
  "name":"test",
  "fields":
    [{
      "name":"name",
      "type":"string"
    }]
}`,
			schemaID:  2,
			record:    `{"notValid":123}`,
			expEncErr: true,
		}, {
			name: "Invalid schema",
			schema: `
{
  "typezz":"record",
  "name":"test",
  "fields":
    [{
      "name":"name"
    }]
}`,
			schemaID: 2,
			expErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noopCl, _ := sr.NewClient()
			schema := sr.Schema{
				Schema: tt.schema,
				Type:   sr.TypeAvro,
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
			err = json.Unmarshal([]byte(tt.expRecord), &expU)
			require.NoError(t, err)

			require.Equal(t, expU, gotU)
		})
	}
}

func Test_encodeDecodeAvroRecordWithReferences(t *testing.T) {
	tests := []struct {
		name     string
		schema   *sr.Schema
		schemaID int
		record   string
	}{
		{
			name:     "single reference",
			schemaID: 123,
			record:   `{"name":"redpanda","telephone":{"number":12341234,"identifier":"home"}}`,
			schema: &sr.Schema{
				Schema: `{
   "type":"record",
   "name":"test",
   "fields":[
      {
         "name":"name",
         "type":"string"
      },
      {
         "name":"telephone",
         "type":"telephone"
      }
   ]
}`,
				References: []sr.SchemaReference{
					{
						Name:    "telephone",
						Subject: "single",
						Version: 0,
					},
				},
			},
		}, {
			name:     "multiple single reference",
			schemaID: 123,
			record:   `{"name":"redpanda","telephone":{"number":12341234,"identifier":"home"},"coordinates":{"longitude":12547930,"latitude":-81.716652}}`,
			schema: &sr.Schema{
				Schema: `{
   "type":"record",
   "name":"test",
   "fields":[
      {
         "name":"name",
         "type":"string"
      },
      {
         "name":"telephone",
         "type":"telephone"
      },
      {
         "name":"coordinates",
         "type":"coordinates"
      }
   ]
}`,
				References: []sr.SchemaReference{
					{
						Name:    "telephone",
						Subject: "single",
						Version: 0,
					},
					{
						Name:    "coordinates",
						Subject: "single",
						Version: 1,
					},
				},
			},
		}, {
			name:     "nested references",
			schemaID: 123,
			record:   `{"name":"redpanda","telephone":{"number":12341234,"identifier":"home","owner":{"lastname":"panda"}}}`,
			schema: &sr.Schema{
				Schema: `{
   "type":"record",
   "name":"test",
   "fields":[
      {
         "name":"name",
         "type":"string"
      },
      {
         "name":"telephone",
         "type":"telephoneOwner"
      }
   ]
}`,
				References: []sr.SchemaReference{
					{
						Name:    "telephoneOwner",
						Subject: "nested",
						Version: 1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(avroReferenceHandler())
			defer ts.Close()

			fs := afero.NewMemMapFs()
			params := &config.Params{ConfigFlag: "/some/path/redpanda.yaml"}
			p, err := params.LoadVirtualProfile(fs)
			require.NoError(t, err)

			p.SR.Addresses = []string{ts.URL}

			client, err := schemaregistry.NewClient(fs, p)
			require.NoError(t, err)

			tt.schema.Type = sr.TypeAvro
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

var avroReferenceMap = map[string]string{
	"single-0": `{"schema":"{\"type\":\"record\",\"name\":\"telephone\",\"fields\":[{\"name\":\"number\",\"type\":\"int\"},{\"name\":\"identifier\",\"type\":\"string\"}]}"}`,
	"single-1": `{"schema":"{\"type\":\"record\",\"name\":\"coordinates\",\"fields\":[{\"name\":\"longitude\",\"type\":\"long\"},{\"name\":\"latitude\",\"type\":\"double\"}]}"}`,
	"nested-1": `{"references":[{"name":"owner","subject":"nested","version":2}],"schema":"{\"type\":\"record\",\"name\":\"telephoneOwner\",\"fields\":[{\"name\":\"number\",\"type\":\"int\"},{\"name\":\"identifier\",\"type\":\"string\"},{\"name\":\"owner\",\"type\":\"owner\"}]}"}`,
	"nested-2": `{"schema":"{\"type\":\"record\",\"name\":\"owner\",\"fields\":[{\"name\":\"lastname\",\"type\":\"string\"}]}"}`,
}

func avroReferenceHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Split the URL path by '/' to get individual path segments of
		segments := strings.Split(r.URL.Path, "/")

		// /subjects/{subject}/versions/{version}"
		if len(segments) >= 4 && segments[1] == "subjects" && segments[3] == "versions" {
			subject := segments[2]
			version := segments[4]
			id := subject + "-" + version

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(avroReferenceMap[id]))
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}
