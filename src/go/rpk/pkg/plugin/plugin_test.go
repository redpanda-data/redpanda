// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs"
	"github.com/stretchr/testify/require"
)

func TestListPlugins(t *testing.T) {
	t.Parallel()

	fs := testfs.FromMap(map[string]testfs.Fmode{
		"/usr/local/sbin/.rpk-non_executable":    {Mode: 0o666, Contents: ""},
		"/usr/local/sbin/.rpk-barely_executable": {Mode: 0o100, Contents: ""},
		"/bin/.rpk-barely_executable":            {Mode: 0o100, Contents: "shadowed!"},
		"/bin/.rpk.ac-barely_executable":         {Mode: 0o100, Contents: "also shadowed!"},
		"/bin/.rpk.ac-auto_completed":            {Mode: 0o777, Contents: ""},
		"/bin/.rpk.managed-man2_nested":          {Mode: 0o777, Contents: ""},
		"/bin/has/dir/":                          {Mode: 0o777, Contents: ""},
		"/bin/.rpk.ac-":                          {Mode: 0o777, Contents: "empty name ignored"},
		"/bin/.rpk-":                             {Mode: 0o777, Contents: "empty name ignored"},
		"/bin/.rpkunrelated":                     {Mode: 0o777, Contents: ""},
		"/bin/rpk-nodot":                         {Mode: 0o777, Contents: ""},
		"/unsearched/.rpk-valid_unused":          {Mode: 0o777, Contents: ""},
	})

	got := ListPlugins(fs, []string{
		"   /usr/local/sbin   ", // space trimmed
		"",                      // empty path, ignored
		"/usr/local/sbin",       // dup ignored
		"/bin",
	})

	exp := Plugins{
		{
			Name:      "auto",
			Path:      "/bin/.rpk.ac-auto_completed",
			Arguments: []string{"auto", "completed"},
		},
		{
			Name:      "barely",
			Path:      "/usr/local/sbin/.rpk-barely_executable",
			Arguments: []string{"barely", "executable"},
			ShadowedPaths: []string{
				"/bin/.rpk-barely_executable",
				"/bin/.rpk.ac-barely_executable",
			},
		},
		{
			Name:      "man2",
			Path:      "/bin/.rpk.managed-man2_nested",
			Arguments: []string{"man2", "nested"},
			Managed:   true,
		},
	}

	require.Equal(t, exp, got, "got plugins != expected")
}

func TestUniqueTrimmedStrs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		in   []string
		exp  []string
	}{
		{
			"empty",
			[]string{},
			[]string{},
		},

		{
			"duplicated",
			[]string{"foo", "bar", "foo", "biz", "baz"},
			[]string{"foo", "bar", "biz", "baz"},
		},

		{
			"trimmed and empty dropped",
			[]string{"", "    bar ", "foo", "biz", "baz", "foo   "},
			[]string{"bar", "foo", "biz", "baz"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := uniqueTrimmedStrs(test.in)
			require.Equal(t, test.exp, got, "got unique trimmed strs != expected")
		})
	}
}

func TestWriteBinary(t *testing.T) {
	t.Parallel()

	fs := testfs.FromMap(map[string]testfs.Fmode{
		"/bin/":     {},
		"/usr/bin/": {},
	})

	{
		dst, err := WriteBinary(fs, "autocomplete", "/bin/", []byte("ac"), true, false)
		require.NoError(t, err, "creating an autocomplete binary failed")
		require.Equal(t, "/bin/.rpk.ac-autocomplete", dst, "binary path not as expected")
	}
	{
		dst, err := WriteBinary(fs, "non-auto", "/usr/bin", []byte("nonac"), false, false)
		require.NoError(t, err, "creating a non-autocomplete binary failed")
		require.Equal(t, "/usr/bin/.rpk-non-auto", dst, "binary path not as expected")
	}
	{
		dst, err := WriteBinary(fs, "managed", "/usr/bin", []byte("m"), false, true)
		require.NoError(t, err, "creating a managed binary failed")
		require.Equal(t, "/usr/bin/.rpk.managed-managed", dst, "binary path not as expected")
	}

	testfs.Expect(t, fs, map[string]testfs.Fmode{
		"/bin/.rpk.ac-autocomplete":     {Mode: 0o755, Contents: "ac"},
		"/usr/bin/.rpk-non-auto":        {Mode: 0o755, Contents: "nonac"},
		"/usr/bin/.rpk.managed-managed": {Mode: 0o755, Contents: "m"},
	})
}
