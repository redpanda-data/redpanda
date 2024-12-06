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

	// https://github.com/redpanda-data/redpanda/blob/dev/src/v/pandaproxy/schema_registry/test/compatibility_protobuf.cc
	const testWellKnownSchema = `syntax = "proto3";
package test;
import "google/protobuf/any.proto";
import "google/protobuf/api.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/field_mask.proto";
import "google/protobuf/source_context.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/type.proto";
import "google/protobuf/wrappers.proto";
import "google/type/calendar_period.proto";
import "google/type/color.proto";
import "google/type/date.proto";
import "google/type/datetime.proto";
import "google/type/dayofweek.proto";
import "google/type/decimal.proto";
import "google/type/expr.proto";
import "google/type/fraction.proto";
import "google/type/interval.proto";
import "google/type/latlng.proto";
import "google/type/localized_text.proto";
import "google/type/money.proto";
import "google/type/month.proto";
import "google/type/phone_number.proto";
import "google/type/postal_address.proto";
import "google/type/quaternion.proto";
import "google/type/timeofday.proto";
import "confluent/meta.proto";
import "confluent/types/decimal.proto";

message well_known_types {
  google.protobuf.Any any = 1;
  google.protobuf.Api api = 2;
  google.protobuf.BoolValue bool_value = 3;
  google.protobuf.BytesValue bytes_value = 4;
  google.protobuf.DoubleValue double_value = 5;
  google.protobuf.Duration duration = 6;
  google.protobuf.Empty empty = 7;
  google.protobuf.Enum enum = 8;
  google.protobuf.EnumValue enum_value = 9;
  google.protobuf.Field field = 10;
  google.protobuf.FieldMask field_mask = 11;
  google.protobuf.FloatValue float_value = 12;
  google.protobuf.Int32Value int32_value = 13;
  google.protobuf.Int64Value int64_value = 14;
  google.protobuf.ListValue list_value = 15;
  google.protobuf.Method method = 16;
  google.protobuf.Mixin mixin = 17;
  google.protobuf.NullValue null_value = 18;
  google.protobuf.Option option = 19;
  google.protobuf.SourceContext source_context = 20;
  google.protobuf.StringValue string_value = 21;
  google.protobuf.Struct struct = 22;
  google.protobuf.Syntax syntax = 23;
  google.protobuf.Timestamp timestamp = 24;
  google.protobuf.Type type = 25;
  google.protobuf.UInt32Value uint32_value = 26;
  google.protobuf.UInt64Value uint64_value = 27;
  google.protobuf.Value value = 28;
  google.type.CalendarPeriod calendar_period = 29;
  google.type.Color color = 30;
  google.type.Date date = 31;
  google.type.DateTime date_time = 32;
  google.type.DayOfWeek day_of_week = 33;
  google.type.Decimal decimal = 34;
  google.type.Expr expr = 35;
  google.type.Fraction fraction = 36;
  google.type.Interval interval = 37;
  google.type.LatLng lat_lng = 39;
  google.type.LocalizedText localized_text = 40;
  google.type.Money money = 41;
  google.type.Month month = 42;
  google.type.PhoneNumber phone_number = 43;
  google.type.PostalAddress postal_address = 44;
  google.type.Quaternion quaternion = 45;
  google.type.TimeOfDay time_of_day = 46;
  confluent.Meta c_meta = 47;
  confluent.type.Decimal c_decimal = 48;
}

message Person {
  string first_name = 1;
  string last_name = 2;
}
`

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
		}, {
			name:      "wellknown - All Fields",
			schema:    testWellKnownSchema,
			msgType:   "test.well_known_types",
			schemaID:  11,
			record:    messageAllWellKnown,
			expRecord: messageAllWellKnown,
			expIdx:    []int{0},
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

// Long message, including all well-known types baked in rpk that we can
// encode/decode.
const messageAllWellKnown = `{
  "any": {
    "@type": "type.googleapis.com/test.Person",
    "firstName": "foo",
    "lastName": "bar"
  },
  "api": {
    "version": "v1",
    "methods": [
      {
        "name": "GetMethod",
        "requestTypeUrl": "type.googleapis.com/google.protobuf.Empty",
        "responseTypeUrl": "type.googleapis.com/google.protobuf.StringValue"
      }
    ]
  },
  "boolValue": true,
  "bytesValue": "bGVasG6gd11ybGQ=",
  "doubleValue": 3.14159,
  "duration": "3600s",
  "empty": {},
  "enum": {
    "name": "ZERO"
  },
  "enumValue": {
    "name": "MY_ENUM_VALUE",
    "number": 1
  },
  "field": {
    "name": "fieldName"
  },
  "fieldMask": "field",
  "floatValue": 1.23,
  "int32Value": 42,
  "int64Value": "123456789",
  "listValue": [
      { "stringValue": "Item 1" },
      { "int32Value": 100 }
  ],
  "method": {
    "name": "MethodName",
    "requestTypeUrl": "type.googleapis.com/google.protobuf.StringValue",
    "responseTypeUrl": "type.googleapis.com/google.protobuf.StringValue"
  },
  "mixin": {
    "name": "MixinName",
    "root": "rootPath"
  },
  "option": {
    "name": "optionName",
    "value": { 
      "@type": "type.googleapis.com/test.Person",
      "firstName": "foo",
      "lastName": "bar"
    }
  },
  "sourceContext": {
    "fileName": "fileName.proto"
  },
  "stringValue": "This is a string",
  "struct": {
    "fields": {
      "field1": { "stringValue": "value1" }
    }
  },
  "syntax": "SYNTAX_PROTO3",
  "timestamp": "2020-05-22T20:32:05Z",
  "type": {
    "name": "TypeName",
    "fields": [
      { "name": "field1", "typeUrl": "type.googleapis.com/google.protobuf.StringValue" }
    ]
  },
  "uint32Value": 9876521,
  "uint64Value": "9876543210",
  "value": {
    "stringValue": "A value example"
  },
  "calendarPeriod": "DAY",
  "color": {
    "red": 255,
    "green": 100,
    "blue": 50,
    "alpha": 0.8
  },
  "date": {
    "year": 2024,
    "month": 12,
    "day": 5
  },
  "dateTime": {
    "year": 2024,
    "month": 12,
    "day": 5,
    "hours": 14,
    "minutes": 30,
    "seconds": 15,
    "nanos": 123456789,
    "utcOffset": "3600s"
  },
  "dayOfWeek": "WEDNESDAY",
  "decimal": {
    "value": "123.456"
  },
  "expr": {
    "expression": "a + b",
    "title": "Sample Expression"
  },
  "fraction": {
    "numerator": "3",
    "denominator": "4"
  },
  "interval": {
    "startTime": "2020-05-22T20:32:05Z",
    "endTime": "2023-01-01T20:32:05Z"
  },
  "latLng": {
    "latitude": 37.7749,
    "longitude": -122.4194
  },
  "localizedText": {
    "text": "Hello",
    "languageCode": "en-US"
  },
  "money": {
    "currencyCode": "USD",
    "units": "100",
    "nanos": 99
  },
  "month": "DECEMBER",
  "phoneNumber": {
    "e164Number": "+15552220123",
    "extension": "1234"
  },
  "postalAddress": {
    "regionCode": "US",
    "postalCode": "94105",
    "locality": "San Francisco",
    "addressLines": ["123 Main St", "Suite 456"]
  },
  "quaternion": {
    "x": 1.0,
    "y": 1.0,
    "z": 1.0,
    "w": 1.0
  },
  "timeOfDay": {
    "hours": 14,
    "minutes": 30,
    "seconds": 1,
    "nanos": 2
  },
  "cMeta": {
    "doc": "v2"
  },
  "cDecimal": {
    "value": "AQAAAG9wI1Xh",
    "precision": 10,
    "scale": 2
  }
}
`
