package tuners

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTuneResult(t *testing.T) {
	tests := []struct {
		name           string
		rebootRequired bool
		want           TuneResult
	}{
		{
			name:           "Shall indicate that reboot is required when passed true",
			rebootRequired: true,
			want:           &tuneResult{rebootRequired: true},
		},
		{
			name:           "Shall indicate that reboot is required when passed true",
			rebootRequired: false,
			want:           &tuneResult{rebootRequired: false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewTuneResult(tt.rebootRequired)
			require.Exactly(t, tt.want, got)
		})
	}
}
