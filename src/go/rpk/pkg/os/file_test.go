// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package os

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeStrictYAML(t *testing.T) {
	type testType struct {
		Enabled bool `yaml:"enabled"`
	}
	tests := []struct {
		name     string
		input    string
		expError bool
	}{
		{
			name:     "valid YAML",
			input:    `enabled: true`,
			expError: false,
		},
		{
			name:     "invalid YAML",
			input:    `:`,
			expError: true,
		},
		{
			name:     "unknown field",
			input:    `unknown: value`,
			expError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result testType
			err := decodeStrictYAML([]byte(tt.input), &result)
			if tt.expError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
