// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
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
