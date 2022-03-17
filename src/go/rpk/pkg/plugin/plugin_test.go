package plugin

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs"
)

func TestListPlugins(t *testing.T) {
	t.Parallel()

	fs := testfs.FromMap(map[string]testfs.Fmode{
		"/usr/local/sbin/.rpk-non_executable":    {0666, ""},
		"/usr/local/sbin/.rpk-barely_executable": {0100, ""},
		"/bin/.rpk-barely_executable":            {0100, "shadowed!"},
		"/bin/.rpk.ac-barely_executable":         {0100, "also shadowed!"},
		"/bin/.rpk.ac-auto_completed":            {0777, ""},
		"/bin/has/dir/":                          {0777, ""},
		"/bin/.rpk.ac-":                          {0777, "empty name ignored"},
		"/bin/.rpk-":                             {0777, "empty name ignored"},
		"/bin/.rpkunrelated":                     {0777, ""},
		"/bin/rpk-nodot":                         {0777, ""},
		"/unsearched/.rpk-valid_unused":          {0777, ""},
	})

	got := ListPlugins(fs, []string{
		"   /usr/local/sbin   ", // space trimmed
		"",                      // empty path, ignored
		"/usr/local/sbin",       // dup ignored
		"/bin",
	})

	exp := Plugins{
		{
			Path:      "/usr/local/sbin/.rpk-barely_executable",
			Arguments: []string{"barely", "executable"},
			ShadowedPaths: []string{
				"/bin/.rpk-barely_executable",
				"/bin/.rpk.ac-barely_executable",
			},
		},
		{
			Path:      "/bin/.rpk.ac-auto_completed",
			Arguments: []string{"auto", "completed"},
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
		dst, err := WriteBinary(fs, "autocomplete", "/bin/", []byte("ac"), true)
		require.NoError(t, err, "creating an autocomplete binary failed")
		require.Equal(t, "/bin/.rpk.ac-autocomplete", dst, "binary path not as expected")
	}
	{
		dst, err := WriteBinary(fs, "non-auto", "/usr/bin", []byte("nonac"), false)
		require.NoError(t, err, "creating a non-autocomplete binary failed")
		require.Equal(t, "/usr/bin/.rpk-non-auto", dst, "binary path not as expected")
	}

	testfs.Expect(t, fs, map[string]testfs.Fmode{
		"/bin/.rpk.ac-autocomplete": {0o755, "ac"},
		"/usr/bin/.rpk-non-auto":    {0o755, "nonac"},
	})
}
