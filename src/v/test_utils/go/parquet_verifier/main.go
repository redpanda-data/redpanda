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
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"

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
		fmt.Fprintln(os.Stderr, "❌ missing -input-file flag")
		os.Exit(1)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌ unable to read file:", err)
		os.Exit(1)
	}
	dec := json.NewDecoder(bytes.NewBuffer(b))
	dec.UseNumber()
	if err := dec.Decode(&testCase); err != nil {
		fmt.Fprintln(os.Stderr, "❌ unable to deserialize file:", err)
		os.Exit(1)
	}
	rdr := parquet.NewGenericReader[any](bytes.NewReader(testCase.File))
	if int(rdr.NumRows()) != len(testCase.Rows) {
		fmt.Fprintf(os.Stderr, "❌ structs not equals!\n\n\tgot: %#v\n\twant: %#v\n", rdr.NumRows(), len(testCase.Rows))
		os.Exit(1)
	}
	for _, expected := range testCase.Rows {
		rows := make([]any, 1)
		if n, err := rdr.Read(rows); err != nil || n != 1 {
			fmt.Fprintln(os.Stderr, "❌ unable to read from reader:", err)
			os.Exit(1)
		}
		// normalize the parquet data so it has the same types as the JSON data
		actual := normalize(rows[0])
		if !reflect.DeepEqual(expected, actual) {
			fmt.Fprintf(os.Stderr, "❌ structs not equals!\n\n\tgot: %#v\n\twant: %#v\n=== DIFF ===\n", actual, expected)
			pretty.Fdiff(os.Stderr, expected, actual)
			fmt.Fprintln(os.Stderr)
			os.Exit(1)
		}
	}
	if err := rdr.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "❌ unable to close reader:", err)
		os.Exit(1)
	}
	fmt.Println("✅ test case successful")
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
