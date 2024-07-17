// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schema

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
)

func Test_parseReferenceFlag(t *testing.T) {
	for _, tt := range []struct {
		name          string
		referenceFlag string
		file          string
		exp           []sr.SchemaReference
		expErr        bool
	}{
		{
			name:          "single reference",
			referenceFlag: "name:subject:1",
			exp:           []sr.SchemaReference{{Name: "name", Subject: "subject", Version: 1}},
		}, {
			name:          "multiple references",
			referenceFlag: "name:subject:1,foo:bar:2",
			exp:           []sr.SchemaReference{{Name: "name", Subject: "subject", Version: 1}, {Name: "foo", Subject: "bar", Version: 2}},
		}, {
			name:          "empty references",
			referenceFlag: "",
			exp:           []sr.SchemaReference{},
		}, {
			name:          "just a colon",
			referenceFlag: ":",
			expErr:        true,
		}, {
			name:          "empty values",
			referenceFlag: "::",
			expErr:        true,
		}, {
			name:          "missing name",
			referenceFlag: ":subject:3",
			expErr:        true,
		}, {
			name:          "missing subject",
			referenceFlag: "name::3",
			expErr:        true,
		}, {
			name:          "missing version",
			referenceFlag: "name:subject:",
			expErr:        true,
		}, {
			name:          "single row, column file",
			referenceFlag: "/tmp/foo",
			file:          "name subject 1",
			exp:           []sr.SchemaReference{{Name: "name", Subject: "subject", Version: 1}},
		}, {
			name:          "multiple rows, column file",
			referenceFlag: "/tmp/foo",
			file: `name subject 1
foo bar 2`,
			exp: []sr.SchemaReference{{Name: "name", Subject: "subject", Version: 1}, {Name: "foo", Subject: "bar", Version: 2}},
		}, {
			name:          "json file",
			referenceFlag: "/tmp/foo.json",
			file:          `[{"name":"foo","subject":"bar","version":1},{"name":"foo","subject":"bar","version":2}]`,
			exp:           []sr.SchemaReference{{Name: "foo", Subject: "bar", Version: 1}, {Name: "foo", Subject: "bar", Version: 2}},
		}, {
			name:          "yaml file",
			referenceFlag: "/tmp/foo.yaml",
			file: `- name: foo
  subject: bar
  version: 1
- name: foo
  subject: bar
  version: 2`,
			exp: []sr.SchemaReference{{Name: "foo", Subject: "bar", Version: 1}, {Name: "foo", Subject: "bar", Version: 2}},
		}, {
			name:          "single URL reference",
			referenceFlag: "http://example.com/foo/referenced.json:ref_test:1",
			exp:           []sr.SchemaReference{{Name: "http://example.com/foo/referenced.json", Subject: "ref_test", Version: 1}},
		}, {
			name:          "single URL reference with port",
			referenceFlag: "http://example.com:8080/referenced.json:ref_test:1",
			exp:           []sr.SchemaReference{{Name: "http://example.com:8080/referenced.json", Subject: "ref_test", Version: 1}},
		}, {
			name:          "multiple URL reference + normal references",
			referenceFlag: "http://example.com/asdf/referenced.json:ref_test:1,foo:bar:2,file:///tmp/foo.json:ref_test:3",
			exp: []sr.SchemaReference{
				{Name: "http://example.com/asdf/referenced.json", Subject: "ref_test", Version: 1},
				{Name: "foo", Subject: "bar", Version: 2},
				{Name: "file:///tmp/foo.json", Subject: "ref_test", Version: 3},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.file != "" {
				err := afero.WriteFile(fs, tt.referenceFlag, []byte(tt.file), 0o777)
				require.NoError(t, err)
			}
			got, err := parseReferenceFlag(fs, tt.referenceFlag)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}
