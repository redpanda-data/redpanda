// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package out

import (
	"reflect"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs"
)

func TestParseFileArray(t *testing.T) {
	fs := testfs.FromMap(map[string]testfs.Fmode{
		"/t1": {
			Mode: 0o666,
			Contents: `first 3 fields true 12 0

second 4 str false 4.4 2
// comment line
third 5 s2 true 3 0
`,
		},

		"/t2.txt": {
			Mode:     0o666,
			Contents: `text	0	file	false	3.2	3`,
		},

		"/t3.json": {
			Mode: 0o666,
			Contents: `[{
	"f1": "json",
	"f3": "file",
	"f6": 1,
	"f4": true
}, {
	"f2": 3
}]`,
		},

		"/t4.yaml": {
			Mode: 0o666,
			Contents: `
- f1: yaml
  f3: easier
- f5: 3.2
  f3: yamly
  f6: 9
 `,
		},

		"/t5": {
			Mode:     0o666,
			Contents: "short line\n",
		},
		"/t6": {
			Mode:     0o666,
			Contents: "nonbool 4 str FAIL 4.4 2\n",
		},
		"/t7": {
			Mode:     0o666,
			Contents: "nonint FAIL str false 4.4 2\n",
		},
		"/t8": {
			Mode:     0o666,
			Contents: "nonuint 3 str false 4.4 FAIL\n",
		},
		"/t9": {
			Mode:     0o666,
			Contents: "nonfloat 3 str false FAIL 3\n",
		},
	})

	type S struct {
		F1 string  `json:"f1" yaml:"f1"`
		F2 int     `json:"f2" yaml:"f2"`
		F3 string  `json:"f3" yaml:"f3"`
		F4 bool    `json:"f4" yaml:"f4"`
		F5 float32 `json:"f5" yaml:"f5"`
		F6 uint32  `json:"f6" yaml:"f6"`
	}

	for _, test := range []struct {
		file   string
		exp    []S
		expErr bool
	}{
		{
			file: "/t1",
			exp: []S{
				{"first", 3, "fields", true, 12, 0},
				{"second", 4, "str", false, 4.4, 2},
				{"third", 5, "s2", true, 3, 0},
			},
		},
		{
			file: "/t2.txt",
			exp: []S{
				{"text", 0, "file", false, 3.2, 3},
			},
		},
		{
			file: "/t3.json",
			exp: []S{
				{"json", 0, "file", true, 0, 1},
				{"", 3, "", false, 0, 0},
			},
		},
		{
			file: "/t4.yaml",
			exp: []S{
				{"yaml", 0, "easier", false, 0, 0},
				{"", 0, "yamly", false, 3.2, 9},
			},
		},

		{file: "/t5", expErr: true},
		{file: "/t6", expErr: true},
		{file: "/t7", expErr: true},
		{file: "/t8", expErr: true},
		{file: "/t9", expErr: true},
	} {
		t.Run(test.file, func(t *testing.T) {
			got, err := ParseFileArray[S](fs, test.file)
			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("got err? %v, exp err? %v", gotErr, test.expErr)
			}
			if gotErr || test.expErr {
				return
			}
			if !reflect.DeepEqual(got, test.exp) {
				t.Errorf("got %#v != exp %#v", got, test.exp)
			}
		})
	}
}
