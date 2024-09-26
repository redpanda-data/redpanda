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
						},
						OffsetIndexOffset: 23,
						OffsetIndexLength: 43,
						ColumnIndexOffset: 999,
						ColumnIndexLength: 876,
					},
				},
				TotalByteSize: 321,
				NumRows:       1,
				FileOffset:    1234,
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
				FileOffset: 4123,
			},
		},
		NumRows: math.MaxInt64,
		KeyValueMetadata: []format.KeyValue{
			{Key: "foo", Value: "bar"},
			{Key: "baz", Value: "qux"},
			{Key: "nice", Value: ""},
		},
		CreatedBy: "The best Message Broker in the West",
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
		fmt.Fprintln(os.Stderr)
		os.Exit(1)
	}
	fmt.Println("✅ test case successful")
}
