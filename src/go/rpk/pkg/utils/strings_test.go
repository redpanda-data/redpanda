package utils_test

import (
	"testing"
	"vectorized/pkg/utils"

	"github.com/stretchr/testify/require"
)

func TestStringInSlice(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		slice    []string
		expected bool
	}{
		{
			name:     "it should return true if the slice contains the string",
			str:      "best",
			slice:    []string{"redpanda", "is", "the", "best", "there", "is"},
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			actual := utils.StringInSlice(tt.str, tt.slice)
			require.Equal(st, tt.expected, actual)
		})
	}
}
