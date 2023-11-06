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
	"fmt"
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

// For now, it just tests that it successfully encodes the message and that the
// first bytes match the expected wire format. When we add decoding, we should
// add the test for the record.
func Test_encodeDecodeProtoRecordNoReferences(t *testing.T) {
	const testSimpleSchema = `syntax = "proto3";

message Person {
  string name = 1;
  int32 id = 2;
  bool isAdmin = 3;
  optional string email = 4;
}`
	const testComplexSchema = `syntax = "proto3";

message Person {
  message PhoneNumber {
    string number = 1;
    .Person.PhoneType type = 2;
  }
  enum PhoneType {
    PHONE_TYPE_UNSPECIFIED = 0;
    PHONE_TYPE_MOBILE = 1;
    PHONE_TYPE_HOME = 2;
    PHONE_TYPE_WORK = 3;
  }
  string name = 1;
  int32 id = 2;
  string email = 3;
  repeated .Person.PhoneNumber phones = 4;
}

message AddressBook {
  repeated .Person people = 1;
}`
	const testPackageSchema = `syntax = "proto3";
package foo.bar;

message Person {
  message PhoneNumber {
    string number = 1;
    foo.bar.Person.PhoneType type = 2;
  }
  enum PhoneType {
    PHONE_TYPE_UNSPECIFIED = 0;
    PHONE_TYPE_MOBILE = 1;
    PHONE_TYPE_HOME = 2;
    PHONE_TYPE_WORK = 3;
  }
  string name = 1;
  int32 id = 2;
  string email = 3;
  repeated foo.bar.Person.PhoneNumber phones = 4;
}`

	const testNestedSchema = `syntax = "proto3";

message Person {
   message Unused {
       string foo = 1;
  }

  message PhoneNumber {
    string number = 1;
    .Person.PhoneNumber.PhoneBrand brand = 2;
	optional .Person.PhoneNumber.PhoneCondition condition = 3;

	message PhoneCondition {
       bool damaged = 1;
    }

	message PhoneBrand {
       string brand = 1;
       int32 year = 2;
    }
  }

  string name = 1;
  .Person.PhoneNumber phone = 2;
}`

	tests := []struct {
		name      string
		schema    string
		msgType   string
		record    string
		expRecord string
		schemaID  int
		expIdx    []int
		expErr    bool // error building the Serde.
		expEncErr bool // error encoding the record.
	}{
		{
			name:      "simple - complete record",
			schema:    testSimpleSchema,
			msgType:   "Person",
			schemaID:  1,
			record:    `{"name":"igor","id":123,"isAdmin":true,"email":"test@redpanda.com"}`,
			expRecord: `{"name":"igor","id":123,"isAdmin":true,"email":"test@redpanda.com"}`,
			expIdx:    []int{0},
		}, {
			name:      "simple - complete record, no FQN",
			schema:    testSimpleSchema,
			msgType:   "",
			schemaID:  1,
			record:    `{"name":"igor","id":123,"isAdmin":true,"email":"test@redpanda.com"}`,
			expRecord: `{"name":"igor","id":123,"isAdmin":true,"email":"test@redpanda.com"}`,
			expIdx:    []int{0},
		}, {
			name:      "simple - without optional field",
			schema:    testSimpleSchema,
			msgType:   "Person",
			schemaID:  2,
			record:    `{"name":"igor","id":123,"isAdmin":true}`,
			expRecord: `{"name":"igor","id":123,"isAdmin":true}`,
			expIdx:    []int{0},
		}, {
			name:      "simple - bad record",
			schema:    testSimpleSchema,
			msgType:   "Person",
			record:    `{"thisIsNotValid":"igor","id":123,"isAdmin":true}`,
			expEncErr: true,
			expRecord: `{"thisIsNotValid":"igor","id":123,"isAdmin":true}`,
		}, {
			name:      "simple - msg type not found",
			schema:    testSimpleSchema,
			msgType:   "NotFoo",
			record:    `{"name":"igor","id":123,"isAdmin":true}`,
			expRecord: `{"name":"igor","id":123,"isAdmin":true}`,
			expErr:    true,
		}, {
			name:      "complex - complete record, using index 0 message",
			schema:    testComplexSchema,
			msgType:   "Person",
			schemaID:  3,
			record:    `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111","type":0},{"number":"2222","type":1},{"number":"33333","type":2}]}`,
			expRecord: `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111"},{"number":"2222","type":"PHONE_TYPE_MOBILE"},{"number":"33333","type":"PHONE_TYPE_HOME"}]}`,
			expIdx:    []int{0},
		}, {
			name:      "complex - complete record, no FQN should fail",
			schema:    testComplexSchema,
			schemaID:  3,
			record:    `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111","type":0},{"number":"2222","type":1},{"number":"33333","type":2}]}`,
			expEncErr: true,
		}, {
			name:      "complex - complete record, using index 1 message",
			schema:    testComplexSchema,
			msgType:   "AddressBook",
			schemaID:  4,
			record:    `{"people":[{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"111","type":2}]},{"name":"igor","id":321,"email":"igor@redpanda.com","phones":[{"number":"1231231","type":0},{"number":"2222333","type":1}]}]}`,
			expRecord: `{"people":[{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"111","type":"PHONE_TYPE_HOME"}]},{"name":"igor","id":321,"email":"igor@redpanda.com","phones":[{"number":"1231231"},{"number":"2222333","type":"PHONE_TYPE_MOBILE"}]}]}`,
			expIdx:    []int{1},
		}, {
			name:      "complex - nested record",
			schema:    testComplexSchema,
			msgType:   "Person.PhoneNumber",
			schemaID:  5,
			record:    `{"number":"111","type":2}`,
			expRecord: `{"number":"111","type":"PHONE_TYPE_HOME"}`,
			expIdx:    []int{0, 0},
		}, {
			name:      "package - complete record",
			schema:    testPackageSchema,
			msgType:   "foo.bar.Person",
			schemaID:  6,
			record:    `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111","type":0},{"number":"2222","type":1},{"number":"33333","type":2}]}`,
			expRecord: `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111"},{"number":"2222","type":"PHONE_TYPE_MOBILE"},{"number":"33333","type":"PHONE_TYPE_HOME"}]}`,
			expIdx:    []int{0},
		}, {
			name:      "package - complete record with fqn message type",
			schema:    testPackageSchema,
			msgType:   "foo.bar.Person",
			schemaID:  7,
			record:    `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111","type":0},{"number":"2222","type":1},{"number":"33333","type":2}]}`,
			expRecord: `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111"},{"number":"2222","type":"PHONE_TYPE_MOBILE"},{"number":"33333","type":"PHONE_TYPE_HOME"}]}`,
			expIdx:    []int{0},
		}, {
			name:      "package - nested record with fqn message type",
			schema:    testPackageSchema,
			msgType:   "foo.bar.Person",
			schemaID:  8,
			record:    `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111","type":0},{"number":"2222","type":1},{"number":"33333","type":2}]}`,
			expRecord: `{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"1111"},{"number":"2222","type":"PHONE_TYPE_MOBILE"},{"number":"33333","type":"PHONE_TYPE_HOME"}]}`,
			expIdx:    []int{0},
		}, {
			name:      "nested - complete record",
			schema:    testNestedSchema,
			msgType:   "Person",
			schemaID:  9,
			record:    `{"name":"foo","phone":{"number":"123","brand":{"brand":"pandaPhone","year":2023},"condition":{"damaged":true}}}`,
			expRecord: `{"name":"foo","phone":{"number":"123","brand":{"brand":"pandaPhone","year":2023},"condition":{"damaged":true}}}`,
			expIdx:    []int{0},
		}, {
			name:      "nested - nested record",
			schema:    testNestedSchema,
			msgType:   "Person.PhoneNumber.PhoneBrand",
			schemaID:  10,
			record:    `{"brand":"pandaPhone","year":2023}`,
			expRecord: `{"brand":"pandaPhone","year":2023}`,
			expIdx:    []int{0, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noopCl, _ := sr.NewClient()
			schema := sr.Schema{
				Schema: tt.schema,
				Type:   sr.TypeProtobuf,
			}
			serde, err := NewSerde(context.Background(), noopCl, &schema, tt.schemaID, tt.msgType)
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

			// Validate magic byte.
			require.Equal(t, uint8(0), got[0])

			// Validate schema registry wire format.
			var serdeHeader sr.ConfluentHeader
			id, toDecode, err := serdeHeader.DecodeID(got)
			require.NoError(t, err)
			require.Equal(t, tt.schemaID, id)

			index, _, err := serdeHeader.DecodeIndex(toDecode, 3) // 3 is the most nested element in this test.
			require.NoError(t, err)
			require.Equal(t, tt.expIdx, index)

			gotDecoded, err := serde.DecodeRecord(toDecode)
			require.NoError(t, err)

			// To avoid any mismatch in the decode order of the elements, we
			// unmarshal and compare the unmarshaled records.
			var gotU, expU map[string]any
			err = json.Unmarshal(gotDecoded, &gotU)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(tt.expRecord), &expU)
			require.NoError(t, err)

			require.Equal(t, expU, gotU)
		})
	}
}

const testSingleReference = `syntax = "proto3";

import "person.proto";

message AddressBook {
  repeated .Person people = 1;
}`

const testNestedReference = `syntax = "proto3";

import "nestedPerson.proto";

message AddressBook {
  repeated .Person people = 1;
}`

const testPersonReference = `syntax = "proto3";

message Person {
  message PhoneNumber {
    string number = 1;
    .Person.PhoneType type = 2;
  }
  enum PhoneType {
    PHONE_TYPE_UNSPECIFIED = 0;
    PHONE_TYPE_MOBILE = 1;
    PHONE_TYPE_HOME = 2;
    PHONE_TYPE_WORK = 3;
  }
  string name = 1;
  int32 id = 2;
  string email = 3;
  repeated .Person.PhoneNumber phones = 4;
}`

const testPersonNested = `syntax = "proto3";

import "phoneNumber.proto";

message Person {
  string name = 1;
  int32 id = 2;
  string email = 3;
  repeated PhoneNumber phones = 4;
}`

const testPhoneNumberReference = `syntax = "proto3";

message PhoneNumber {
  string number = 1;
}`

func Test_encodeProtoRecordWithReferences(t *testing.T) {
	tests := []struct {
		name      string
		schema    *sr.Schema
		schemaID  int
		msgType   string
		record    string
		expRecord string
		expIdx    []int
		expErr    bool
	}{
		{
			name:      "single reference",
			schemaID:  906,
			msgType:   "AddressBook",
			expIdx:    []int{0},
			record:    `{"people":[{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"111","type":2}]},{"name":"igor","id":321,"email":"igor@redpanda.com","phones":[{"number":"1231231","type":0},{"number":"2222333","type":1}]}]}`,
			expRecord: `{"people":[{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"111","type":"PHONE_TYPE_HOME"}]},{"name":"igor","id":321,"email":"igor@redpanda.com","phones":[{"number":"1231231"},{"number":"2222333","type":"PHONE_TYPE_MOBILE"}]}]}`,
			schema: &sr.Schema{
				Schema: testSingleReference,
				References: []sr.SchemaReference{
					{
						Name:    "person.proto",
						Subject: "phone",
						Version: 0,
					},
				},
			},
		}, {
			name:      "nested reference",
			schemaID:  906,
			msgType:   "AddressBook",
			expIdx:    []int{0},
			record:    `{"people":[{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"111"}]},{"name":"igor","id":321,"email":"igor@redpanda.com","phones":[{"number":"1231231"},{"number":"2222333"}]}]}`,
			expRecord: `{"people":[{"name":"rogger","id":123,"email":"test@redpanda.com","phones":[{"number":"111"}]},{"name":"igor","id":321,"email":"igor@redpanda.com","phones":[{"number":"1231231"},{"number":"2222333"}]}]}`,
			schema: &sr.Schema{
				Schema: testNestedReference,
				References: []sr.SchemaReference{
					{
						Name:    "nestedPerson.proto",
						Subject: "phone",
						Version: 1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(protoReferenceHandler())
			defer ts.Close()

			fs := afero.NewMemMapFs()
			params := &config.Params{ConfigFlag: "/some/path/redpanda.yaml"}
			p, err := params.LoadVirtualProfile(fs)
			require.NoError(t, err)

			p.SR.Addresses = []string{ts.URL}

			cl, err := schemaregistry.NewClient(fs, p)
			require.NoError(t, err)

			tt.schema.Type = sr.TypeProtobuf
			serde, err := NewSerde(context.Background(), cl, tt.schema, tt.schemaID, tt.msgType)
			require.NoError(t, err)

			got, err := serde.EncodeRecord([]byte(tt.record))
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Validate magic byte.
			require.Equal(t, uint8(0), got[0])

			// Validate schema registry wire format.
			var serdeHeader sr.ConfluentHeader
			id, toDecode, err := serdeHeader.DecodeID(got)
			require.NoError(t, err)
			require.Equal(t, tt.schemaID, id)

			index, _, err := serdeHeader.DecodeIndex(toDecode, 3) // 3 is the most nested element in this test.
			require.NoError(t, err)
			require.Equal(t, tt.expIdx, index)

			gotDecoded, err := serde.DecodeRecord(toDecode)
			require.NoError(t, err)

			// To avoid any mismatch in the decode order of the elements, we
			// unmarshal and compare the unmarshaled records.
			var gotU, expU map[string]any
			err = json.Unmarshal(gotDecoded, &gotU)
			require.NoError(t, err)
			err = json.Unmarshal([]byte(tt.expRecord), &expU)
			require.NoError(t, err)

			require.Equal(t, expU, gotU)
		})
	}
}

var protoReferenceMap = map[string]string{
	"phone-0":  fmt.Sprintf(`{"schema":%q}`, testPersonReference),
	"phone-1":  fmt.Sprintf(`{"schema":%q,"references":[{"name":"phoneNumber.proto","subject":"number","version":0}]}`, testPersonNested),
	"number-0": fmt.Sprintf(`{"schema":%q}`, testPhoneNumberReference),
}

func protoReferenceHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Split the URL path by '/' to get individual path segments of
		segments := strings.Split(r.URL.Path, "/")

		// /subjects/{subject}/versions/{version}"
		if len(segments) >= 4 && segments[1] == "subjects" && segments[3] == "versions" {
			subject := segments[2]
			version := segments[4]
			id := subject + "-" + version

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(protoReferenceMap[id]))
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}
