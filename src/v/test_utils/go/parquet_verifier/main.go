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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/kr/pretty"
	"github.com/parquet-go/parquet-go"
)

type TestCase struct {
	File []byte `json:"file"`
	Rows []any  `json:"rows"`
}

func main() {
	var file string
	var testCase TestCase
	flag.StringVar(&file, "input-file", "", "The input file to read the serialized JSON testcase from (required)")
	flag.Parse()
	if file == "" {
		log.Fatalln("❌ missing -input-file flag")
	}
	log.Println("reading file:", file)
	b, err := os.ReadFile(file)
	if err != nil {
		log.Fatalln("❌ unable to read file:", err)
	}
	dec := json.NewDecoder(bytes.NewBuffer(b))
	dec.UseNumber()
	if err := dec.Decode(&testCase); err != nil {
		log.Fatalln("❌ unable to deserialize file:", err)
	}
	log.Println("reading parquet file")
	rdr := parquet.NewGenericReader[any](bytes.NewReader(testCase.File))
	if int(rdr.NumRows()) != len(testCase.Rows) {
		log.Fatalf("❌ structs not equals!\n\n\tgot: %#v\n\twant: %#v\n", rdr.NumRows(), len(testCase.Rows))
	}
	log.Println("validating parquet data")
	for _, expected := range testCase.Rows {
		rows := make([]any, 1)
		n, err := rdr.Read(rows)
		if n != 1 {
			log.Fatalf("❌ unable to read one row from reader, got %d rows: %v\n", n, err)
		}
		// normalize the parquet data so it has the same types as the JSON data
		actual := normalize(rows[0])
		if !reflect.DeepEqual(expected, actual) {
			desc := pretty.Diff(expected, actual)
			log.Fatalf("❌ structs not equals!\n\n\tgot: %#v\n\twant: %#v\n=== DIFF ===\n%s\n", actual, expected, strings.Join(desc, "\n"))
		}
	}
	if err := rdr.Close(); err != nil {
		log.Fatalln("❌ unable to close reader:", err)
	}
	log.Println("validating parquet metadata")
	originalFile, err := parquet.OpenFile(bytes.NewReader(testCase.File), int64(len(testCase.File)))
	if err != nil {
		log.Fatalln("❌ unable to open parquet file:", err)
	}

	for i, rg := range originalFile.RowGroups() {
		rows := make([]parquet.Row, 128)
		rowCount := 0
		rowRdr := rg.Rows()
		for {
			n, err := rowRdr.ReadRows(rows)
			rowCount += n
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				log.Fatalln("❌ unable to read parquet rows:", err)
			} else if n == 0 {
				break
			}
		}
		if err := rowRdr.Close(); err != nil {
			log.Fatalln("❌ unable to close parquet reader:", err)
		}
		if rowCount != int(rg.NumRows()) {
			log.Fatalln("❌ row count mismatch:", rowCount, "vs", rg.NumRows())
		}
		log.Printf("read row group %d, num rows: %d", i, rowCount)
		// A big validation loop where we ensure that columns and pages all have consistent stats values.
		for j, col := range rg.ColumnChunks() {
			col := col.(*parquet.FileColumnChunk)
			var colMin parquet.Value
			var colMax parquet.Value
			minCmp := parquet.CompareNullsLast(col.Type().Compare)
			maxCmp := parquet.CompareNullsFirst(col.Type().Compare)
			pages := col.Pages()
			for {
				page, err := pages.ReadPage()
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					log.Fatalln("❌ unable to read parquet page:", err)
				}
				colValues := make([]parquet.Value, page.NumValues())
				n, err := page.Values().ReadValues(colValues)
				if n != len(colValues) || (err != nil && !errors.Is(err, io.EOF)) {
					log.Fatalf(
						"❌ : unable to read all page values for row group %d column %d: %v\n",
						i, j, err,
					)
				}
				var actualMin parquet.Value
				var actualMax parquet.Value
				if len(colValues) > 0 {
					actualMin = slices.MinFunc(colValues, minCmp)
					actualMax = slices.MaxFunc(colValues, maxCmp)
				}
				pmin, pmax, ok := page.Bounds()
				if ok != (page.NumValues() != page.NumNulls()) {
					log.Fatalf(
						"❌ : missing page bounds for row group %d column %d (%d, %d)\n",
						i, j, page.NumValues(), page.NumNulls(),
					)
				}
				if minCmp(pmin, colMin) < 0 {
					colMin = pmin
				}
				if !parquet.Equal(actualMin, pmin) {
					log.Fatalf(
						"❌ : invalid page min value for row group %d column %d (%v != %v)\n",
						i, j, actualMin, pmin,
					)
				}
				if maxCmp(pmax, colMax) > 0 {
					colMax = pmax
				}
				if !parquet.Equal(actualMax, pmax) {
					log.Fatalf(
						"❌ : invalid page max value for row group %d column %d (%v != %v)\n",
						i, j, actualMax, pmax,
					)
				}
			}
			cmin, cmax, ok := col.Bounds()
			if ok != !(colMin.IsNull() && colMax.IsNull()) {
				log.Fatalf(
					"❌ : missing column bounds for row group %d column %d (%d, %d)\n",
					i, j, col.NumValues(), col.NullCount(),
				)
			}
			if len(cmin.Bytes()) > 64 || len(cmax.Bytes()) > 64 {
				log.Fatalf(
					"❌ : invalid length for cmin %v(len %d) or cmax %v(len %d) needs to be <= 64 bytes\n",
					cmin, len(cmin.Bytes()), cmax, len(cmax.Bytes()),
				)
			}

			switch cmin.Kind() {
			case parquet.ByteArray, parquet.FixedLenByteArray:
				if parquet.ByteArrayType.Compare(cmin, colMin) > 0 {
					log.Fatalf(
						"❌ : invalid min value for page in row group %d, column %d: %v > %v\n",
						i, j, cmin, colMin,
					)
				}
			default:
				if !parquet.Equal(cmin, colMin) {
					log.Fatalf(
						"❌ : invalid min value for page in row group %d, column %d: %v != %v\n",
						i, j, cmin, colMin,
					)
				}
			}

			switch cmax.Kind() {
			case parquet.ByteArray, parquet.FixedLenByteArray:
				if parquet.ByteArrayType.Compare(cmax, colMax) < 0 {
					log.Fatalf(
						"❌ : invalid max value for page in row group %d, column %d: %v < %v\n",
						i, j, cmax, colMax,
					)
				}
			default:
				if !parquet.Equal(cmax, colMax) {
					log.Fatalf(
						"❌ : invalid max value for page in row group %d, column %d: %v != %v\n",
						i, j, cmax, colMax,
					)
				}
			}

			_ = pages.Close()
		}
	}
	log.Println("rewriting parquet file to validate more metadata")
	// Ensure if we rewrite the same data the stats don't change between row groups
	// (the page boundaries could change, so we can't verify if they are the same).
	rewrittenFile, err := rewriteParquetFile(originalFile)
	if err != nil {
		log.Fatalln("❌ unable to rewrite parquet file:", err)
	}

	if len(rewrittenFile.RowGroups()) != len(originalFile.RowGroups()) {
		log.Fatalf(
			"❌ rewriting parquet file resulted in a different number of row groups: %d != %d\n",
			len(rewrittenFile.RowGroups()),
			len(originalFile.RowGroups()),
		)
	}

	for i := range len(originalFile.RowGroups()) {
		originalRowGroup := originalFile.RowGroups()[i]
		rewrittenRowGroup := rewrittenFile.RowGroups()[i]
		for j := range len(originalRowGroup.ColumnChunks()) {
			originalChunk := originalRowGroup.ColumnChunks()[j].(*parquet.FileColumnChunk)
			rewrittenChunk := rewrittenRowGroup.ColumnChunks()[j].(*parquet.FileColumnChunk)

			if originalChunk.NullCount() != rewrittenChunk.NullCount() {
				log.Fatalf(
					"❌ row group %d, column %d max null count mismatch: %v != %v\n",
					i, j, originalChunk.NullCount(), rewrittenChunk.NullCount(),
				)
			}

			if originalChunk.NumValues() != rewrittenChunk.NumValues() {
				log.Fatalf(
					"❌ row group %d, column %d num values count mismatch: %v != %v\n",
					i, j, originalChunk.NumValues(), rewrittenChunk.NumValues(),
				)
			}

			originalAllNulls := originalChunk.NumValues() == originalChunk.NullCount()
			omin, omax, ook := originalChunk.Bounds()
			if ook != !originalAllNulls {
				log.Fatalf(
					"❌ : missing original bounds for row group %d column %d (%d, %d)\n",
					i, j, originalChunk.NumValues(), originalChunk.NullCount(),
				)
			}

			rewrittenAllNulls := rewrittenChunk.NumValues() == rewrittenChunk.NullCount()
			rmin, rmax, rok := rewrittenChunk.Bounds()
			if rok != !rewrittenAllNulls {
				log.Fatalf(
					"❌ : missing rewritten bounds for row group %d column %d\n",
					i, j,
				)
			}

			// Note that parquet-go currently does not truncate bounds in page stats.
			// Hence the bounds will not be equal in the rewritten file.
			switch omin.Kind() {
			case parquet.ByteArray, parquet.FixedLenByteArray:
				if parquet.ByteArrayType.Compare(omin, rmin) > 0 {
					log.Fatalf(
						"❌ row group %d, column %d min value mismatch: %v > %v\n",
						i, j, omin, rmin,
					)
				}
			default:
				if !parquet.Equal(omin, rmin) {
					log.Fatalf(
						"❌ row group %d, column %d min value mismatch: %v != %v\n",
						i, j, omin, rmin,
					)
				}
			}

			switch omax.Kind() {
			case parquet.ByteArray, parquet.FixedLenByteArray:
				if parquet.ByteArrayType.Compare(omax, rmax) < 0 {
					log.Fatalf(
						"❌ row group %d, column %d max value mismatch: %v < %v\n",
						i, j, omax, rmax,
					)
				}
			default:
				if !parquet.Equal(omax, rmax) {
					log.Fatalf(
						"❌ row group %d, column %d max value mismatch: %v != %v\n",
						i, j, omax, rmin,
					)
				}
			}
		}
	}

	log.Println("✅ test case successful")
}

func rewriteParquetFile(f *parquet.File) (*parquet.File, error) {
	out := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[any](out, f.Schema(), parquet.DataPageStatistics(true))
	for _, rg := range f.RowGroups() {
		_, err := w.WriteRowGroup(rg)
		if err != nil {
			return nil, fmt.Errorf("unable to copy row group: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("unable to close parquet writer: %w", err)
	}
	return parquet.OpenFile(bytes.NewReader(out.Bytes()), int64(out.Len()))
}

func normalize(val any) any {
	switch v := val.(type) {
	case map[string]any:
		mapped := make(map[string]any, len(v))
		for k, v := range v {
			mapped[k] = normalize(v)
		}
		return mapped
	case []any:
		mapped := make([]any, len(v))
		for i, v := range v {
			mapped[i] = normalize(v)
		}
		return mapped
	case string:
		return base64.StdEncoding.EncodeToString([]byte(v))
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case int32:
		return json.Number(strconv.Itoa(int(v)))
	case int64:
		return json.Number(strconv.Itoa(int(v)))
	case int:
		return json.Number(strconv.Itoa(int(v)))
	case float32:
		// Use a fixed precision between the verifier and generator
		return json.Number(strconv.FormatFloat(float64(v), 'f', 8, 32))
	case float64:
		// Use a fixed precision between the verifier and generator
		return json.Number(strconv.FormatFloat(v, 'f', 8, 64))
	default:
		return v
	}
}
