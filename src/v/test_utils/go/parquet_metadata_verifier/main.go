// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"

	"github.com/kr/pretty"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/encoding/thrift"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func ptr[T any](t T) *T {
	return &t
}

var testCases = []any{
	&format.PageHeader{
		UncompressedPageSize: 42,
		CompressedPageSize:   22,
		CRC:                  0xBEEF,
		Type:                 format.IndexPage,
		IndexPageHeader:      &format.IndexPageHeader{},
	},
	&format.PageHeader{
		UncompressedPageSize: 2,
		CompressedPageSize:   1,
		CRC:                  0xDEAD,
		Type:                 format.DataPageV2,
		DataPageHeaderV2: &format.DataPageHeaderV2{
			NumValues:                  22,
			NumNulls:                   0,
			NumRows:                    1,
			Encoding:                   format.Plain,
			DefinitionLevelsByteLength: 21,
			RepetitionLevelsByteLength: 1,
			IsCompressed:               ptr(false),
			Statistics: format.Statistics{
				NullCount: 42,
				MinValue:  []byte{0xDE, 0xAD, 0xBE, 0xE0},
				MaxValue:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
			},
		},
	},
	&format.PageHeader{
		UncompressedPageSize: 99999,
		CompressedPageSize:   0,
		CRC:                  0xEEEE,
		Type:                 format.DictionaryPage,
		DictionaryPageHeader: &format.DictionaryPageHeader{
			NumValues: 44,
			Encoding:  format.RLE,
			IsSorted:  true,
		},
	},
	&format.FileMetaData{
		Version: 2,
		Schema: []format.SchemaElement{
			{
				Name:        "root",
				NumChildren: 3,
			},
			{
				Type:           ptr(format.Int32),
				RepetitionType: ptr(format.Optional),
				Name:           "foo",
				FieldID:        1,
				LogicalType: &format.LogicalType{
					Time: &format.TimeType{
						IsAdjustedToUTC: true,
						Unit: format.TimeUnit{
							Millis: &format.MilliSeconds{},
						},
					},
				},
				ConvertedType: ptr(deprecated.TimeMillis),
			},
			{
				Type:           ptr(format.FixedLenByteArray),
				TypeLength:     ptr[int32](16),
				RepetitionType: ptr(format.Required),
				Name:           "bar",
				FieldID:        2,
				LogicalType: &format.LogicalType{
					UUID: &format.UUIDType{},
				},
			},
			{
				Type:           ptr(format.Boolean),
				RepetitionType: ptr(format.Repeated),
				Name:           "baz",
				FieldID:        3,
			},
		},
		RowGroups: []format.RowGroup{
			{
				Columns: []format.ColumnChunk{
					{
						MetaData: format.ColumnMetaData{
							Type: format.Boolean,
							Encoding: []format.Encoding{
								format.Plain, format.RLE,
							},
							PathInSchema:          []string{"foo", "baz"},
							Codec:                 format.Uncompressed,
							NumValues:             888,
							TotalUncompressedSize: 9999,
							TotalCompressedSize:   9,
							KeyValueMetadata: []format.KeyValue{
								{Key: "qux", Value: "thid"},
							},
							DataPageOffset:       2,
							IndexPageOffset:      5,
							DictionaryPageOffset: 9,
							Statistics: format.Statistics{
								NullCount: 9,
								MinValue:  []byte{0x00},
								MaxValue:  []byte{0xFF},
							},
						},
					},
				},
				TotalByteSize:       321,
				NumRows:             1,
				FileOffset:          1234,
				TotalCompressedSize: 231,
				Ordinal:             1,
			},
			{
				TotalByteSize: 99999,
				NumRows:       38,
				Columns:       []format.ColumnChunk{},
				SortingColumns: []format.SortingColumn{
					{
						ColumnIdx:  1,
						Descending: false,
						NullsFirst: true,
					},
					{
						ColumnIdx:  2,
						Descending: true,
						NullsFirst: false,
					},
				},
				FileOffset:          4123,
				TotalCompressedSize: 8888,
				Ordinal:             2,
			},
		},
		NumRows: math.MaxInt64,
		KeyValueMetadata: []format.KeyValue{
			{Key: "foo", Value: "bar"},
			{Key: "baz", Value: "qux"},
			{Key: "nice", Value: ""},
		},
		CreatedBy: "The best Message Broker in the West",
		ColumnOrders: []format.ColumnOrder{
			{TypeOrder: &format.TypeDefinedOrder{}},
			{TypeOrder: &format.TypeDefinedOrder{}},
			{TypeOrder: &format.TypeDefinedOrder{}},
		},
	},
	&format.FileMetaData{
		Version: 2,
		Schema: []format.SchemaElement{
			{
				Name:        "root",
				NumChildren: 16,
			},
			{
				Type:           ptr(format.Boolean),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_bool",
			},
			{
				Type:           ptr(format.Int32),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_i32",
				LogicalType: &format.LogicalType{
					Integer: &format.IntType{BitWidth: 32, IsSigned: true},
				},
				ConvertedType: ptr(deprecated.Int32),
			},
			{
				Type:           ptr(format.Int64),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_i64",
				LogicalType: &format.LogicalType{
					Integer: &format.IntType{BitWidth: 64, IsSigned: true},
				},
				ConvertedType: ptr(deprecated.Int64),
			},
			{
				Type:           ptr(format.Float),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_f32",
			},
			{
				Type:           ptr(format.Double),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_f64",
			},
			{
				Type:           ptr(format.FixedLenByteArray),
				TypeLength:     ptr[int32](16),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_decimal",
				Scale:          ptr[int32](8),
				Precision:      ptr[int32](38),
				LogicalType: &format.LogicalType{
					Decimal: &format.DecimalType{
						Scale: 8, Precision: 38,
					},
				},
				ConvertedType: ptr(deprecated.Decimal),
			},
			{
				Type:           ptr(format.Int32),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_date",
				LogicalType: &format.LogicalType{
					Date: &format.DateType{},
				},
				ConvertedType: ptr(deprecated.Date),
			},
			{
				Type:           ptr(format.Int64),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_time",
				LogicalType: &format.LogicalType{
					Time: &format.TimeType{IsAdjustedToUTC: false, Unit: format.TimeUnit{Micros: &format.MicroSeconds{}}},
				},
				ConvertedType: ptr(deprecated.TimeMicros),
			},
			{
				Type:           ptr(format.Int64),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_timestamp",
				LogicalType: &format.LogicalType{
					Timestamp: &format.TimestampType{IsAdjustedToUTC: false, Unit: format.TimeUnit{Micros: &format.MicroSeconds{}}},
				},
				ConvertedType: ptr(deprecated.TimestampMicros),
			},
			{
				Type:           ptr(format.Int64),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_timestamptz",
				LogicalType: &format.LogicalType{
					Timestamp: &format.TimestampType{IsAdjustedToUTC: true, Unit: format.TimeUnit{Micros: &format.MicroSeconds{}}},
				},
				ConvertedType: ptr(deprecated.TimestampMicros),
			},
			{
				Type:           ptr(format.ByteArray),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_string",
				LogicalType: &format.LogicalType{
					UTF8: &format.StringType{},
				},
				ConvertedType: ptr(deprecated.UTF8),
			},
			{
				Type:           ptr(format.FixedLenByteArray),
				TypeLength:     ptr[int32](16),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_uuid",
				LogicalType: &format.LogicalType{
					UUID: &format.UUIDType{},
				},
			},
			{
				Type:           ptr(format.FixedLenByteArray),
				TypeLength:     ptr[int32](103),
				RepetitionType: ptr(format.Required),
				Name:           "iceberg_fixed",
			},
			{
				Type:           ptr(format.ByteArray),
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_binary",
			},
			{
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_string_int_map",
				NumChildren:    1,
				LogicalType:    &format.LogicalType{Map: &format.MapType{}},
				ConvertedType:  ptr(deprecated.Map),
			},
			{
				RepetitionType: ptr(format.Repeated),
				Name:           "map",
				NumChildren:    2,
			},
			{
				Type:           ptr(format.ByteArray),
				RepetitionType: ptr(format.Required),
				Name:           "key",
				LogicalType: &format.LogicalType{
					UTF8: &format.StringType{},
				},
				ConvertedType: ptr(deprecated.UTF8),
			},
			{
				Type:           ptr(format.Int32),
				RepetitionType: ptr(format.Optional),
				Name:           "value",
			},
			{
				RepetitionType: ptr(format.Optional),
				Name:           "iceberg_int_list",
				NumChildren:    1,
				LogicalType:    &format.LogicalType{List: &format.ListType{}},
				ConvertedType:  ptr(deprecated.List),
			},
			{
				RepetitionType: ptr(format.Repeated),
				Name:           "list",
				NumChildren:    1,
			},
			{
				Type:           ptr(format.Int32),
				RepetitionType: ptr(format.Optional),
				Name:           "element",
			},
		},
		RowGroups:        []format.RowGroup{},
		KeyValueMetadata: []format.KeyValue{},
	},
}

func main() {
	var file string
	var testCase int
	flag.StringVar(&file, "input-file", "", "The input file to read the serialized Apache Thrift data from (required)")
	flag.IntVar(&testCase, "test-case", -1, "The test case to verify that the input file matches (required)")
	flag.Parse()
	if file == "" {
		fmt.Fprintln(os.Stderr, "❌ missing -input-file flag")
		os.Exit(1)
	}
	if testCase < 0 {
		fmt.Fprintln(os.Stderr, "❌ missing -test-case flag")
		os.Exit(1)
	}
	if testCase >= len(testCases) {
		fmt.Fprintln(os.Stderr, "❌ invalid -test-case flag")
		os.Exit(1)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌ unable to read file:", err)
		os.Exit(1)
	}
	expected := testCases[testCase]
	value := reflect.New(reflect.TypeOf(expected).Elem())
	actual := value.Interface()
	err = thrift.Unmarshal(new(thrift.CompactProtocol), b, actual)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌ invalid thrift input:", err)
		os.Exit(1)
	}
	if !reflect.DeepEqual(expected, actual) {
		fmt.Fprintf(os.Stderr, "❌ structs not equals!\n\n\tgot: %#v\n\twant: %#v\n=== DIFF ===\n", actual, expected)
		pretty.Fdiff(os.Stderr, expected, actual)
		diff := diffmatchpatch.New()
		fmt.Fprintln(os.Stderr, "\n=== RAW DIFF ===\n",
			diff.DiffPrettyText(diff.DiffMain(
				fmt.Sprintf("%#v", actual),
				fmt.Sprintf("%#v", expected),
				false,
			)),
		)
		os.Exit(1)
	}
	fmt.Println("✅ test case successful")
}
