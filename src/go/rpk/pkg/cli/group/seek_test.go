// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestParseSeekFile(t *testing.T) {
	keep := map[string]bool{"foo": true}
	o0 := kadm.Offset{
		Topic:       "foo",
		Partition:   0,
		At:          1,
		LeaderEpoch: -1,
	}
	o2 := kadm.Offset{
		Topic:       "foo",
		Partition:   2,
		At:          3,
		LeaderEpoch: -1,
	}
	expBoth := kadm.Offsets{"foo": map[int32]kadm.Offset{0: o0, 2: o2}}

	for _, test := range []struct {
		name     string
		contents string

		exp    kadm.Offsets
		expErr bool
	}{
		{
			name:     "standard",
			contents: "foo 0 1\nfoo 2 3\n",
			exp:      expBoth,
		},

		{
			name:     "empty_line",
			contents: "\nfoo 0 1\nfoo 2 3\n",
			exp:      expBoth,
		},

		{
			name:     "bar_skipped",
			contents: "\nfoo 0 1\nfoo 2 3\nbar 0 2\n",
			exp:      expBoth,
		},

		{
			name:     "tabs",
			contents: "foo\t0\t1\nfoo\t2\t3\n",
			exp:      expBoth,
		},

		{
			name:     "tab_space_mix",
			contents: "foo 0 1\nfoo\t2\t3\n",
			exp:      expBoth,
		},

		{
			name:     "nothing_is_fine",
			contents: "",
			exp:      make(kadm.Offsets),
		},

		{
			name:     "bad_part_num",
			contents: "foo asdf 1\n",
			expErr:   true,
		},

		{
			name:     "bad_offset_num",
			contents: "foo 0 asdf\n",
			expErr:   true,
		},

		{
			name:     "too_many_fields",
			contents: "foo 0 1 2\n",
			expErr:   true,
		},

		{
			name:     "part_too_large",
			contents: "foo 99999999999999999999999 1\n",
			expErr:   true,
		},

		{
			name:     "offset_too_large",
			contents: "foo 1 99999999999999999999999n",
			expErr:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := testfs.FromMap(map[string]testfs.Fmode{
				test.name: {Mode: 0o444, Contents: test.contents},
			})

			got, err := parseSeekFile(fs, test.name, keep)
			gotErr := err != nil

			if gotErr != test.expErr {
				t.Errorf("got err? %v, exp err? %v", gotErr, test.expErr)
			}
			if gotErr || test.expErr {
				return
			}
			require.Equal(t, got, test.exp)
		})
	}
}
