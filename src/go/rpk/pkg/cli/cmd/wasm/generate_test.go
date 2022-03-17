// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/wasm/template"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs"
)

func TestGetWasmApiVersion(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		output string
	}{
		{
			name: "should get correct vectorized WASM API version from npm search",
			input: `[{"name":"@vectorizedio/wasm-api",
		    "scope":"vectorizedio",
		    "version":"21.10.1-si-beta13",
		    "description":"wasm api helps to define wasm function",
		    "date":"2021-10-27T17:00:30.090Z",
		    "links":{"npm":"https://www.npmjs.com/package/%40vectorizedio%2Fwasm-api"},
		    "publisher":{"username":"vectorizedio","email":"billing@vectorized.io"},
		    "maintainers":[{"username":"vectorizedio","email":"billing@vectorized.io"}]}
		    ]`,
			output: "21.10.1-si-beta13",
		},
		{
			name:   "should get default vectorized WASM API version if npm search returns random string",
			input:  "Random string\n",
			output: defApiVersion,
		},
		{
			name:   "should get default vectorized WASM API version if npm search returns null string",
			input:  "",
			output: defApiVersion,
		},
		{
			name:   "should get default vectorized WASM API version if npm search returns null JSON array",
			input:  "[]",
			output: defApiVersion,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			version := getWasmApiVersion(test.input)
			require.Exactly(t, version, test.output)
		})
	}
}

func TestWasmCommand(t *testing.T) {
	tests := []struct {
		name string
		path string

		pre    map[string]testfs.Fmode
		post   map[string]testfs.Fmode
		expNot []string
		expErr bool
	}{
		{
			name: "should create an npm template in new directory with executable webpack",
			path: "new_folder/new_sub_folder/wasm",
			pre:  nil,
			post: map[string]testfs.Fmode{
				"new_folder/new_sub_folder/wasm/src/main.js":       {0o600, template.WasmJs()},
				"new_folder/new_sub_folder/wasm/test/main.test.js": {0o600, template.WasmTestJs()},
				"new_folder/new_sub_folder/wasm/package.json":      {0o600, template.PackageJson(defApiVersion)},
				"new_folder/new_sub_folder/wasm/webpack.js":        {0o766, template.Webpack()},
			},
		},
		{
			name:   "should fail if the given dir contains files created by this command*",
			path:   "wasm",
			pre:    map[string]testfs.Fmode{"wasm/package.json": {0o300, "foo"}},
			post:   map[string]testfs.Fmode{"wasm/package.json": {0o300, "foo"}},
			expNot: []string{"wasm/src/main.js", "wasm/test/main.test.js", "wasm/webpack.js"},
			expErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := testfs.FromMap(test.pre)
			err := executeGenerate(fs, test.path, true)
			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("got err %v (%v) != exp err? %v", gotErr, err, test.expErr)
			}
			testfs.Expect(t, fs, test.post)
			testfs.ExpectNot(t, fs, test.expNot...)
		})
	}
}
