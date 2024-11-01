package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		exp   string
	}{
		{
			name:  "No forbidden characters",
			input: "validName",
			exp:   "validName",
		},
		{
			name:  "Single forbidden character",
			input: "invalid:8083",
			exp:   "invalid-8083",
		},
		{
			name:  "Multiple forbidden characters",
			input: "name/with|forbidden?chars",
			exp:   "name-with-forbidden-chars",
		},
		{
			name:  "Only forbidden characters",
			input: `<>:\"/\\|?*`,
			exp:   "-----------",
		},
		{
			name:  "Empty string",
			input: "",
			exp:   "",
		},
		{
			name:  "No change with already sanitized name",
			input: "cleanName123",
			exp:   "cleanName123",
		},
		{
			name:  "Name with numbers and special characters",
			input: "name123|test*<>",
			exp:   "name123-test---",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := SanitizeName(tt.input)
			require.Equal(t, tt.exp, actual)
		})
	}
}
