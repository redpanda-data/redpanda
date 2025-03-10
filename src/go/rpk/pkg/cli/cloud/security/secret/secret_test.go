package secret

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSecretName(t *testing.T) {
	tests := []struct {
		name        string
		secretName  string
		expectedErr bool
	}{
		{"ValidName", "SECRET_123", false},
		{"EmptyName", "", true},
		{"TooLongName", "A" + string(make([]byte, 255)), true},
		{"InvalidCharacters", "secret_123", true},
		{"InvalidCharacters", "SECRET-123", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSecretName(tt.secretName)
			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
