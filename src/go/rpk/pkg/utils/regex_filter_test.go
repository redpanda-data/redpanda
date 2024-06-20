// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegexListedItems(t *testing.T) {
	tests := []struct {
		name    string
		list    []string
		exprs   []string
		want    []string
		wantErr bool
	}{
		{
			name: "no list, no expressions: no error",
		},

		{
			name: "list, no expressions: no change",
			list: []string{"foo", "bar"},
		},

		{
			name:  "matching ^f",
			list:  []string{"foo", "bar", "fdsa"},
			exprs: []string{"^f.*"},
			want:  []string{"foo", "fdsa"},
		},

		{
			name:  "matching three",
			list:  []string{"foo", "bar", "biz", "baz", "buzz"},
			exprs: []string{".a.", "^f.."},
			want:  []string{"bar", "baz", "foo"},
		},

		{
			name:  "dot matches nothing by default, because we anchor with ^ and $",
			list:  []string{"foo", "bar", "biz", "baz", "buzz"},
			exprs: []string{"."},
		},

		{
			name:  ".* matches everything",
			list:  []string{"foo", "bar", "biz", "baz", "buzz"},
			exprs: []string{".*"},
			want:  []string{"foo", "bar", "biz", "baz", "buzz"},
		},

		{
			name:  "* matches everything",
			list:  []string{"foo", "bar", "biz", "baz", "buzz"},
			exprs: []string{".*"},
			want:  []string{"foo", "bar", "biz", "baz", "buzz"},
		},

		{
			name:    "no list",
			exprs:   []string{"as[df"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RegexListedItems(tt.list, tt.exprs)
			gotErr := err != nil
			if gotErr != tt.wantErr {
				t.Errorf("got err? %v, exp? %v", gotErr, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			require.Equal(t, tt.want, got, "got topics != expected")
		})
	}
}
