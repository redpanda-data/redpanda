// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseKVs(t *testing.T) {
	for _, test := range []struct {
		name string
		in   []string

		exp    map[string]string
		expErr bool
	}{
		{
			name: "empty is ok",
			in:   nil,
			exp:  make(map[string]string),
		},

		{
			name: "both colon and equal are supported with spaces wherever",
			in:   []string{" key : value ", "k2= v2"},
			exp: map[string]string{
				"key": "value",
				"k2":  "v2",
			},
		},

		{
			name: "value can contain colon or equal",
			in:   []string{"key:v:a=l", "k=v:2"},
			exp: map[string]string{
				"key": "v:a=l",
				"k":   "v:2",
			},
		},

		{
			name:   "no delim fails",
			in:     []string{"no delim"},
			expErr: true,
		},

		{
			name:   "empty key fails",
			in:     []string{"=val"},
			expErr: true,
		},

		{
			name:   "empty val fails",
			in:     []string{"key="},
			expErr: true,
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseKVs(test.in)

			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("got err? %v, exp? %v", gotErr, test.expErr)
			}
			if test.expErr {
				return
			}
			require.Equal(t, test.exp, got, "got keyvals != expected")
		})
	}
}

func TestRegexListedTopics(t *testing.T) {
	for _, test := range []struct {
		topics []string
		exprs  []string
		exp    []string
		expErr bool
	}{
		{}, // no topics, no expressions: no error

		{ // topic, no expressions: no change
			topics: []string{"foo", "bar"},
		},

		{
			topics: []string{"foo", "bar", "biz", "baz", "buzz"},
			exprs:  []string{".a.", "^f.."},
			exp:    []string{"bar", "baz", "foo"},
		},

		{ // dot matches nothing by default, because we anchor with ^ and $
			topics: []string{"foo", "bar", "biz", "baz", "buzz"},
			exprs:  []string{"."},
		},

		{ // .* matches everything
			topics: []string{"foo", "bar", "biz", "baz", "buzz"},
			exprs:  []string{".*"},
			exp:    []string{"foo", "bar", "biz", "baz", "buzz"},
		},

		{
			exprs:  []string{"as[df"},
			expErr: true,
		},

		//
	} {
		got, err := regexListedTopics(test.topics, test.exprs)

		gotErr := err != nil
		if gotErr != test.expErr {
			t.Errorf("got err? %v, exp? %v", gotErr, test.expErr)
		}
		if test.expErr {
			return
		}
		require.Equal(t, test.exp, got, "got topics != expected")
	}
}
