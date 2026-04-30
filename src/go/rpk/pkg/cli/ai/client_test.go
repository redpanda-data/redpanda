// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package ai

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateVersion(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"latest", false},
		{"0.1.2", false},
		{"v0.1.2", false},
		{"1.2.3-rc1", false}, // suffix after patch is allowed by the regex
		{"0.1", true},
		{"foo", true},
		{"", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			err := validateVersion(c.in)
			if c.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
