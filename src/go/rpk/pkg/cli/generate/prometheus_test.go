package generate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels []string
		expInt map[string]string
		expPub map[string]string
		expErr bool
	}{
		{
			name:   "parse single label",
			labels: []string{"hello=world"},
			expInt: map[string]string{"hello": "world"},
			expPub: map[string]string{"hello": "world"},
		},
		{
			name:   "parse 2 labels with metric target",
			labels: []string{"internal:foo=bar", "public:hello=world"},
			expInt: map[string]string{"foo": "bar"},
			expPub: map[string]string{"hello": "world"},
		},
		{
			name:   "parse 1 label with metric target",
			labels: []string{"internal:foo=bar"},
			expInt: map[string]string{"foo": "bar"},
			expPub: map[string]string{},
		},
		{
			name:   "incorrect label",
			labels: []string{"what"},
			expErr: true,
		},
		{
			name:   "incorrect metric target",
			labels: []string{"mid:foo=bar"},
			expErr: true,
		},
		{
			name:   "err if more than 1 label per metric",
			labels: []string{"internal:foo=bar", "public:hello=world", "public:color=blue"},
			expErr: true,
		},
		{
			name:   "err if more than 1 label per metric - no target",
			labels: []string{"foo:bar", "hello:world"},
			expErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInt, gotPub, err := parseLabels(tt.labels)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expInt, gotInt)
			require.Equal(t, tt.expPub, gotPub)
		})
	}
}
