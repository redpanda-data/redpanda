// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompress(t *testing.T) {
	tests := []struct {
		name		string
		ints		[]int
		expected	[]string
	}{
		{
			name:		"test 1",
			ints:		[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expected:	[]string{"1-20"},
		},
		{
			name:		"test 2",
			ints:		[]int{0, 2, 3, 4, 5, 7, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expected:	[]string{"0", "2-5", "7", "9", "10", "12-20"},
		},
		{
			name:		"test 3",
			ints:		[]int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20},
			expected:	[]string{"0", "2", "4", "6", "8", "10", "12", "14", "16", "18", "20"},
		},
		{
			name:		"test 4",
			ints:		[]int{},
			expected:	[]string{},
		},
		{
			name:		"test 4",
			ints:		[]int{1},
			expected:	[]string{"1"},
		},
		{
			name:		"test 4",
			ints:		[]int{1, 2},
			expected:	[]string{"1", "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			require.Equal(st, tt.expected, compress(tt.ints))
		})
	}
}
