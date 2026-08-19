// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package metastore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePartitionList(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		count   uint32
		want    []uint32
		wantErr string
	}{
		{name: "all", input: "all", count: 3, want: []uint32{0, 1, 2}},
		{name: "empty means all", input: "", count: 2, want: []uint32{0, 1}},
		{name: "single", input: "0", count: 4, want: []uint32{0}},
		{name: "list", input: "0,3", count: 4, want: []uint32{0, 3}},
		{name: "range", input: "1-4", count: 8, want: []uint32{1, 2, 3, 4}},
		{name: "mixed", input: "0,2-3,5", count: 8, want: []uint32{0, 2, 3, 5}},
		{name: "dedup and sort", input: "3,1,1,2", count: 8, want: []uint32{1, 2, 3}},
		{name: "whitespace tolerated", input: " 0 , 1 ", count: 4, want: []uint32{0, 1}},
		{name: "range reversed", input: "4-1", count: 8, wantErr: "invalid partition range \"4-1\": start > end"},
		{name: "out of range single", input: "5", count: 3, wantErr: "partition 5 out of range [0, 3)"},
		{name: "out of range upper", input: "0-5", count: 3, wantErr: "partition 5 out of range [0, 3)"},
		{name: "bad token", input: "abc", count: 3, wantErr: "invalid partition token \"abc\""},
		{name: "bad range token", input: "0-x", count: 3, wantErr: "invalid partition range \"0-x\""},
		{name: "empty token", input: ",1", count: 3, wantErr: "empty partition token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePartitionList(tc.input, tc.count)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseLevelList(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    map[int32]bool
		wantErr string
	}{
		{name: "all", input: "all", want: nil}, // nil set means "no filter"
		{name: "empty means all", input: "", want: nil},
		{name: "single", input: "0", want: map[int32]bool{0: true}},
		{name: "list", input: "0,1", want: map[int32]bool{0: true, 1: true}},
		{name: "range", input: "0-2", want: map[int32]bool{0: true, 1: true, 2: true}},
		{name: "bad token", input: "L0", wantErr: "invalid level token \"L0\""},
		{name: "negative", input: "-1", wantErr: "invalid level token \"-1\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLevelList(tc.input)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
