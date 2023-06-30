package topic

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

func Test_parseOffsetFile(t *testing.T) {
	o0 := kadm.Offset{
		Topic:     "foo",
		Partition: 0,
		At:        1,
	}
	o2 := kadm.Offset{
		Topic:     "foo-2",
		Partition: 2,
		At:        3,
	}
	expBoth := kadm.Offsets{
		"foo":   map[int32]kadm.Offset{0: o0},
		"foo-2": map[int32]kadm.Offset{2: o2},
	}
	for _, tt := range []struct {
		name     string
		fileName string
		content  string
		args     []string
		exp      kadm.Offsets
		expErr   bool
	}{
		{
			name:     "parse text file - no extension",
			fileName: "/tmp/noExtension",
			content:  "foo 0 1\nfoo-2 2 3",
			exp:      expBoth,
		},
		{
			name:     "parse text file - extension",
			fileName: "/tmp/noExtension.txt",
			content:  "foo 0 1\nfoo-2 2 3",
			exp:      expBoth,
		},
		{
			name:     "parse 2-column text file",
			fileName: "/tmp/noExtension",
			args:     []string{"foo"},
			content:  "0 1\n2 3",
			exp: kadm.Offsets{
				"foo": map[int32]kadm.Offset{0: o0, 2: {Topic: "foo", Partition: 2, At: 3}},
			},
		},
		{
			name:     "parse json file",
			fileName: "/tmp/in.json",
			content:  `[{"topic":"foo","partition":0,"offset":1},{ "topic":"foo-2","partition":2,"offset":3}]`,
			exp:      expBoth,
		},
		{
			name:     "parse yaml file",
			fileName: "/tmp/in.yaml",
			content: `
- topic: foo
  partition: 0
  offset: 1
- topic: foo-2
  partition: 2
  offset: 3
`,
			exp: expBoth,
		},
		{
			name:     "invalid types",
			fileName: "/tmp/noExtension",
			content:  "1 zero 1\nfoo-2 two 3",
			expErr:   true,
		},
		{
			name:     "err 3-column and mismatch arg",
			fileName: "/tmp/noExtension",
			args:     []string{"foo"},
			content:  "foo 0 1\nfoo-2 2 3",
			expErr:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			err := afero.WriteFile(fs, tt.fileName, []byte(tt.content), 0o777)
			require.NoError(t, err)

			got, err := parseOffsetFile(fs, tt.fileName, tt.args)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}
